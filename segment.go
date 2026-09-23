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
	"sync"
)

// A segment file is a 16-byte header followed by records. The header holds a
// magic, the format version and the index of the first record, which also
// names the file, so a renamed segment is detected.
const (
	segmentExt        = ".wal"
	segmentMagic      = "WALS"
	segmentVersion    = 1
	segmentHeaderSize = 16
	segmentNameDigits = 20

	// sparseInterval bounds the bytes a read scans from the nearest sparse
	// index entry to the record it wants.
	sparseInterval = 4 << 10
)

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
	file        *os.File     // open while the segment is active

	// Open trusts the bytes up to trustedEnd to hold the records below
	// trustedNext without reading them. The first read verifies and indexes
	// them; until then sparseIndex covers only what follows.
	trustedEnd  int64
	trustedNext uint64
	verified    bool
	verifyMu    sync.Mutex // serializes verification and guards corrupt
	corrupt     error      // damage verification found
}

func newSegment(dir string, first uint64) *segment {
	return &segment{
		first:    first,
		path:     filepath.Join(dir, segmentName(first)),
		size:     segmentHeaderSize,
		verified: true,
	}
}

// trust accepts the bytes up to end as the records below next, unread.
func (s *segment) trust(end int64, next uint64) {
	s.size, s.count = end, next-s.first
	s.trustedEnd, s.trustedNext = end, next
	s.verified = false
}

// scanTrusted reads the trusted bytes, checking the header, every record and
// that they hold exactly the trusted records, and returns their index entries.
func (s *segment) scanTrusted() ([]indexEntry, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return nil, wrap(err)
	}
	defer func() { _ = file.Close() }() // read only, nothing to lose

	if err := s.checkHeader(file); err != nil {
		return nil, err
	}

	prefix := newSegment(filepath.Dir(s.path), s.first)
	if err := prefix.scan(file, s.trustedEnd); err != nil {
		return nil, s.recordError(prefix.size, err)
	}

	if prefix.nextIndex() != s.trustedNext {
		return nil, fmt.Errorf("%w: %s ends at index %d but should end at %d",
			ErrCorrupt, s.path, prefix.nextIndex()-1, s.trustedNext-1)
	}

	return prefix.sparseIndex, nil
}

// createSegment makes an empty segment through a temp file and a rename, so a
// crash never leaves a segment without a header. The file stays open.
func createSegment(dir string, first uint64, prealloc int64) (*segment, error) {
	seg := newSegment(dir, first)
	tmp := seg.path + tempExt

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

// nextIndex returns the index the next appended record gets.
func (s *segment) nextIndex() uint64 {
	return s.first + s.count
}

// addRecord accounts for a record of size bytes at the end of the segment.
func (s *segment) addRecord(size int64) {
	if n := len(s.sparseIndex); n == 0 || s.size-s.sparseIndex[n-1].offset >= sparseInterval {
		s.sparseIndex = append(s.sparseIndex, indexEntry{index: s.nextIndex(), offset: s.size})
	}

	s.count++
	s.size += size
}

// position returns the nearest indexed record at or before index and its offset.
func (s *segment) position(index uint64) (uint64, int64) {
	i, found := slices.BinarySearchFunc(s.sparseIndex, index, func(entry indexEntry, target uint64) int {
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

// read appends records with index >= from to out until out holds limit records
// or the segment ends. The segment must be verified. Callers hold the log's
// read lock, so nothing is published meanwhile.
func (s *segment) read(from uint64, limit int, out []Record) ([]Record, error) {
	file := s.file
	if file == nil {
		closed, err := os.Open(s.path)
		if err != nil {
			return nil, wrap(err)
		}
		defer func() { _ = closed.Close() }() // read only, nothing to lose

		file = closed
	}

	index, offset := s.position(from)
	reader := recordReader{src: file, offset: offset, end: s.size}

	for ; len(out) < limit; index++ {
		data, err := reader.next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, s.recordError(reader.offset, err)
		}

		if index >= from {
			out = append(out, Record{Index: index, Data: data})
		}
	}

	return out, nil
}

// scan rebuilds count, size and the sparse index from file up to end. It stops
// at the first record that fails to decode and returns that error; size is
// then the offset of that record.
func (s *segment) scan(file *os.File, end int64) error {
	reader := recordReader{src: file, offset: s.size, end: end}

	for {
		data, err := reader.next()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		s.addRecord(recordHeaderSize + int64(len(data)))
	}
}

func (s *segment) checkHeader(file *os.File) error {
	var header [segmentHeaderSize]byte

	if _, err := file.ReadAt(header[:], 0); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: %s is shorter than a segment header", ErrCorrupt, s.path)
		}

		return wrap(err)
	}

	if header != segmentHeader(s.first) {
		return fmt.Errorf("%w: %s has a bad header", ErrCorrupt, s.path)
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
	if isRecordError(err) {
		return fmt.Errorf("%w: %s at offset %d: %w", ErrCorrupt, s.path, offset, err)
	}

	return err
}

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

func segmentHeader(first uint64) [segmentHeaderSize]byte {
	var header [segmentHeaderSize]byte

	copy(header[0:4], segmentMagic)
	binary.LittleEndian.PutUint32(header[4:8], segmentVersion)
	binary.LittleEndian.PutUint64(header[8:16], first)

	return header
}

func segmentName(first uint64) string {
	return fmt.Sprintf("%0*d%s", segmentNameDigits, first, segmentExt)
}

// parseSegmentName returns the first index a segment file name encodes.
// Indexes start at 1, so a zero name is not a segment.
func parseSegmentName(name string) (uint64, bool) {
	digits, ok := strings.CutSuffix(name, segmentExt)
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
