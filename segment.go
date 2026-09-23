package wal

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// A segment file is a 16-byte header followed by records. The header holds a
// magic, the format version and the index of the first record, which also
// names the file, so a renamed segment is detected.
const (
	segmentExt        = ".wal"
	segmentMagic      = "WALS"
	segmentVersion    = 2
	segmentHeaderSize = 16
	segmentNameDigits = 20

	// sparseInterval bounds the bytes a read scans from the nearest sparse
	// index entry to the record it wants.
	sparseInterval = 4 << 10
)

// Failures that make records impossible to locate, besides record decode ones.
var (
	errShortSegment     = errors.New("file is shorter than a segment header")
	errBadSegmentHeader = errors.New("bad segment header")
	errMissingRecords   = errors.New("segment ends before its last record")
)

// openSegment opens a closed segment file for reading.
var openSegment = os.Open

// indexEntry maps a record index to its offset in the segment file.
type indexEntry struct {
	index  uint64
	offset int64
}

// segment is one file of consecutive records.
type segment struct {
	first       uint64
	path        string
	count       uint64
	size        int64        // header and records; preallocation makes the file longer
	sparseIndex []indexEntry // one entry per sparseInterval bytes or so

	// file is open while the segment is in the log. Only the active segment's
	// is written; a closed segment's is read-only when Open opened it.
	file *os.File

	// Bytes up to trustedEnd hold the records below trustedNext, which Open
	// accepted without the tail scan. Verification locates them up to
	// readableEnd; lost says why it could not locate the rest, if any.
	trustedEnd  int64
	trustedNext uint64
	readableEnd int64
	lost        error
}

// verification is what scanTrusted finds: the index entries of the trusted
// records it located, where they end and why it could not locate the rest.
type verification struct {
	entries []indexEntry
	end     int64
	lost    error
}

// newSegment returns an empty segment starting at first.
func newSegment(dir string, first uint64) *segment {
	return &segment{
		first:       first,
		path:        filepath.Join(dir, segmentName(first)),
		size:        segmentHeaderSize,
		trustedEnd:  segmentHeaderSize,
		trustedNext: first,
		readableEnd: segmentHeaderSize,
	}
}

// createSegment makes an empty segment through a temp file and a rename, so a
// crash never leaves a segment without a header. The file stays open.
func createSegment(dir string, first uint64, prealloc int64) (*segment, error) {
	seg := newSegment(dir, first)
	tmp := seg.path + tempExt

	// An index left by an earlier segment of this name would not describe it.
	if err := removeFile(seg.indexPath()); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, wrap(err)
	}

	if err := initSegment(file, first, prealloc); err != nil {
		return nil, errors.Join(err, file.Close(), removeFile(tmp))
	}

	if err := os.Rename(tmp, seg.path); err != nil {
		return nil, errors.Join(wrap(err), file.Close(), removeFile(tmp))
	}

	if err := syncDir(dir); err != nil {
		return nil, errors.Join(err, file.Close())
	}

	seg.file = file

	return seg, nil
}

// trust accepts the bytes up to end as the records below next, to be verified.
func (s *segment) trust(end int64, next uint64) {
	s.size, s.count = end, next-s.first
	s.trustedEnd, s.trustedNext = end, next
}

// verify checks the header and locates the trusted records, from the index
// file when it matches them and otherwise by scanning, and reports whether the
// index was used. It keeps damage for reads to report and fails only on I/O.
func (s *segment) verify() (bool, error) {
	if s.trustedNext == s.first {
		return false, nil
	}

	if err := s.checkHeader(s.file); err != nil {
		if !isDecodeError(err) {
			return false, err
		}

		s.readableEnd, s.lost = segmentHeaderSize, err

		return false, nil
	}

	// Any failure to use the index, even an I/O one, leaves the scan to decide.
	if entries, err := readIndex(s.indexPath(), s.first, s.trustedEnd, s.trustedNext); err == nil {
		s.sparseIndex = append(entries, s.sparseIndex...)
		s.readableEnd = s.trustedEnd

		return true, nil
	}

	found, err := s.scanTrusted()
	if err != nil {
		return false, err
	}

	s.sparseIndex = append(found.entries, s.sparseIndex...)
	s.readableEnd, s.lost = found.end, found.lost

	return false, nil
}

// scanTrusted checks every trusted record, passing over records with bad data.
// It stops at worse damage without resyncing, since a record's data may itself
// look like records.
func (s *segment) scanTrusted() (verification, error) {
	prefix := newSegment(filepath.Dir(s.path), s.first)
	reader := recordReader{src: s.file, offset: prefix.size, end: s.trustedEnd}

	for prefix.nextIndex() < s.trustedNext {
		start := reader.offset

		_, err := reader.next(prefix.nextIndex())
		if errors.Is(err, errBadData) {
			err = reader.skip()
		}

		if errors.Is(err, io.EOF) {
			err = errMissingRecords
		}

		if isDecodeError(err) {
			return verification{entries: prefix.sparseIndex, end: prefix.size, lost: err}, nil
		}

		if err != nil {
			return verification{}, err
		}

		prefix.addRecord(reader.offset - start)
	}

	return verification{entries: prefix.sparseIndex, end: prefix.size}, nil
}

// nextIndex returns the index the next appended record gets.
func (s *segment) nextIndex() uint64 {
	return s.first + s.count
}

// addRecord accounts for a record of size bytes at the end of the segment. The
// first record past the trusted bytes is always indexed, so reads of it never
// start in the trusted records.
func (s *segment) addRecord(size int64) {
	n := len(s.sparseIndex)
	if n == 0 || s.size == s.trustedEnd || s.size-s.sparseIndex[n-1].offset >= sparseInterval {
		s.sparseIndex = append(s.sparseIndex, indexEntry{index: s.nextIndex(), offset: s.size})
	}

	s.count++
	s.size += size
}

// position returns the nearest indexed record at or before index and its offset.
func (s *segment) position(index uint64) (uint64, int64) {
	i, found := slices.BinarySearchFunc(s.sparseIndex, index,
		func(entry indexEntry, target uint64) int {
			return cmp.Compare(entry.index, target)
		})
	if !found {
		i--
	}

	if i < 0 {
		return s.first, segmentHeaderSize
	}

	return s.sparseIndex[i].index, s.sparseIndex[i].offset
}

// nextIndexed returns the first indexed record after index, where a read can
// start without scanning what precedes it, or the segment's next index.
func (s *segment) nextIndexed(index uint64) uint64 {
	return s.indexedAfter(index).index
}

// indexedAfter returns the first index entry after index, or the segment's next
// index and size. Record index ends at or before its offset.
func (s *segment) indexedAfter(index uint64) indexEntry {
	i, _ := slices.BinarySearchFunc(s.sparseIndex, index+1,
		func(entry indexEntry, target uint64) int {
			return cmp.Compare(entry.index, target)
		})
	if i < len(s.sparseIndex) {
		return s.sparseIndex[i]
	}

	return indexEntry{index: s.nextIndex(), offset: s.size}
}

// read appends records from index from to out, up to limit or the segment end,
// or returns what it has plus a *CorruptError at damage. The caller holds the
// log's read lock.
func (s *segment) read(from uint64, limit int, out []Record) ([]Record, error) {
	index, offset := s.position(from)

	// The trusted records and the ones after them are read as separate stretches,
	// so a read never runs from unlocated trusted records into later ones.
	for len(out) < limit && index < s.nextIndex() {
		end, next := s.size, s.nextIndex()
		if index < s.trustedNext {
			end, next = s.readableEnd, s.trustedNext
		}

		// Fetch no further than the index entry after the last wanted record,
		// so a short read does not pull in bytes it never decodes.
		want, last := max(index, from), next-1
		if n := uint64(limit - len(out)); n < next-want {
			last = want + n - 1
		}

		stop := min(end, s.indexedAfter(last).offset)
		reader := recordReader{src: s.file, offset: offset, end: end, stop: stop}

		for ; len(out) < limit && index < next; index++ {
			at := reader.offset

			data, err := reader.next(index)
			if errors.Is(err, errBadData) && index < from {
				err = reader.skip()
			}

			if err != nil {
				return out, s.damage(index, at, err)
			}

			if index >= from {
				out = append(out, Record{Index: index, Data: data})
			}
		}

		offset = s.trustedEnd
	}

	return out, nil
}

// damage turns a failure to read record index at offset into a *CorruptError
// and passes I/O errors through. Bad data loses one record; other damage loses
// every record up to the next sparse index entry.
func (s *segment) damage(index uint64, offset int64, err error) error {
	last := index

	switch {
	case errors.Is(err, errBadData):
	case errors.Is(err, io.EOF):
		// Only a trusted stretch that verification could not locate ends early.
		err = cmp.Or(s.lost, errMissingRecords)
		last = s.nextIndexed(index) - 1
	case isDecodeError(err):
		last = s.nextIndexed(index) - 1
	default:
		return err
	}

	return &CorruptError{Path: s.path, Offset: offset, First: index, Last: last, Err: err}
}

// scan rebuilds count, size and the sparse index from file up to end. It stops
// at the first record that fails to decode and returns that error; size is
// then the offset of that record.
func (s *segment) scan(file *os.File, end int64) error {
	reader := recordReader{src: file, offset: s.size, end: end}

	for {
		data, err := reader.next(s.nextIndex())
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		s.addRecord(recordHeaderSize + int64(len(data)))
	}
}

// checkHeader fails with errShortSegment or errBadSegmentHeader on damage.
func (s *segment) checkHeader(file *os.File) error {
	var header [segmentHeaderSize]byte

	if _, err := file.ReadAt(header[:], 0); err != nil {
		if errors.Is(err, io.EOF) {
			return errShortSegment
		}

		return wrap(err)
	}

	if header != segmentHeader(s.first) {
		return errBadSegmentHeader
	}

	return nil
}

// seal cuts preallocated space and makes the segment durable.
func (s *segment) seal() error {
	if err := s.file.Truncate(s.size); err != nil {
		return wrap(err)
	}

	if err := s.file.Sync(); err != nil {
		return wrap(err)
	}

	return nil
}

// saveIndex writes the index file of the segment, which is sealed, so the next
// Open need not scan it. It skips a segment without records or with trusted
// records that verification did not locate up to the trusted end.
func (s *segment) saveIndex() {
	if s.count == 0 || s.lost != nil || s.readableEnd != s.trustedEnd {
		return
	}

	buf := encodeIndex(s.first, s.size, s.nextIndex(), s.sparseIndex)
	_ = writeIndex(s.indexPath(), buf) // a cache: a failure costs only a scan
}

// indexPath returns the path of the segment's index file.
func (s *segment) indexPath() string {
	return filepath.Join(filepath.Dir(s.path), indexName(s.first))
}

// close closes the file, if open.
func (s *segment) close() error {
	if s.file == nil {
		return nil
	}

	err := s.file.Close()
	s.file = nil

	if err != nil {
		return wrap(err)
	}

	return nil
}

// recordError turns a decode failure at offset into ErrCorrupt and passes other
// errors through.
func (s *segment) recordError(offset int64, err error) error {
	if isDecodeError(err) {
		return fmt.Errorf("%w: %s at offset %d: %w", ErrCorrupt, s.path, offset, err)
	}

	return err
}

// initSegment writes the header, preallocates and syncs a new segment file.
func initSegment(file *os.File, first uint64, prealloc int64) error {
	header := segmentHeader(first)

	if _, err := file.Write(header[:]); err != nil {
		return wrap(err)
	}

	if err := preallocate(file, prealloc); err != nil {
		return wrap(err)
	}

	if err := file.Sync(); err != nil {
		return wrap(err)
	}

	return nil
}

// segmentHeader encodes the header of the segment starting at first.
func segmentHeader(first uint64) [segmentHeaderSize]byte {
	var header [segmentHeaderSize]byte

	copy(header[0:4], segmentMagic)
	binary.LittleEndian.PutUint32(header[4:8], segmentVersion)
	binary.LittleEndian.PutUint64(header[8:16], first)

	return header
}

// segmentName returns the file name of the segment starting at first.
func segmentName(first uint64) string {
	return fmt.Sprintf("%0*d%s", segmentNameDigits, first, segmentExt)
}

// indexName returns the file name of the index of the segment starting at first.
func indexName(first uint64) string {
	return fmt.Sprintf("%0*d%s", segmentNameDigits, first, indexExt)
}

// parseSegmentName returns the first index a segment file name encodes.
// Indexes start at 1, so a zero name is not a segment.
func parseSegmentName(name string) (uint64, bool) {
	return parseName(name, segmentExt)
}

// parseIndexName returns the first index an index file name encodes.
func parseIndexName(name string) (uint64, bool) {
	return parseName(name, indexExt)
}

// parseName returns the first index encoded by name, a segment or index file
// name with extension ext.
func parseName(name, ext string) (uint64, bool) {
	digits, ok := strings.CutSuffix(name, ext)
	if !ok || len(digits) != segmentNameDigits {
		return 0, false
	}

	for _, char := range digits {
		if char < '0' || char > '9' {
			return 0, false
		}
	}

	first, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || first == 0 {
		return 0, false
	}

	return first, true
}
