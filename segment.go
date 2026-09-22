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

// A segment file starts with a magic and a version, then records back to
// back. Its name is the index of its first record.
const (
	segmentExt        = ".wal"
	segmentMagic      = "WAL1"
	segmentVersion    = 1
	segmentHeaderSize = 8
	segmentNameDigits = 20

	// indexInterval bounds the bytes scanned to reach a record from the
	// nearest sparse index entry.
	indexInterval = 4 << 10
	chunkSize     = 256 << 10
)

type indexEntry struct {
	index  uint64
	offset int64
}

// segment is one file of consecutive records.
type segment struct {
	first uint64
	path  string
	count uint64
	size  int64        // bytes of header and intact records
	index []indexEntry // sparse, about one entry per indexInterval bytes
	file  *os.File     // open only for the active segment
}

func segmentFileName(first uint64) string {
	return fmt.Sprintf("%0*d%s", segmentNameDigits, first, segmentExt)
}

func parseSegmentFileName(name string) (uint64, bool) {
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
	if err != nil {
		return 0, false
	}

	return first, true
}

// next returns the index the next record gets.
func (s *segment) next() uint64 {
	return s.first + s.count
}

// advance accounts for one record of size bytes at the end of the segment.
func (s *segment) advance(size int64) {
	if len(s.index) == 0 || s.size-s.index[len(s.index)-1].offset >= indexInterval {
		s.index = append(s.index, indexEntry{index: s.next(), offset: s.size})
	}

	s.count++
	s.size += size
}

// locate returns the nearest indexed record at or before index.
func (s *segment) locate(index uint64) (uint64, int64) {
	pos, found := slices.BinarySearchFunc(s.index, index, func(entry indexEntry, target uint64) int {
		return cmp.Compare(entry.index, target)
	})
	if !found {
		pos--
	}

	if pos < 0 {
		return s.first, segmentHeaderSize
	}

	return s.index[pos].index, s.index[pos].offset
}

// read appends up to limit records with index >= from to out.
func (s *segment) read(from uint64, limit int, out []Record) ([]Record, error) {
	if s.file != nil {
		return s.decode(s.file, from, limit, out)
	}

	file, err := os.Open(s.path)
	if err != nil {
		return nil, wrap(err)
	}

	out, err = s.decode(file, from, limit, out)
	if err := errors.Join(err, file.Close()); err != nil {
		return nil, err
	}

	return out, nil
}

func (s *segment) decode(src io.ReaderAt, from uint64, limit int, out []Record) ([]Record, error) {
	index, offset := s.locate(from)
	reader := newChunkReader(src, offset, s.size)

	for len(out) < limit {
		data, err := reader.next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}

		if errors.Is(err, errTorn) {
			return nil, fmt.Errorf("%w: %s at offset %d", ErrCorrupt, s.path, reader.offset())
		}

		if err != nil {
			return nil, err
		}

		if index >= from {
			out = append(out, Record{Index: index, Data: data})
		}

		index++
	}

	return out, nil
}

// createSegment writes a new segment through a temp file, so a crash never
// leaves a segment without a header. The file stays open for appends.
func createSegment(dir string, first uint64, size int64) (*segment, error) {
	seg := &segment{
		first: first,
		path:  filepath.Join(dir, segmentFileName(first)),
		size:  segmentHeaderSize,
	}
	tmp := seg.path + tmpExt

	file, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, wrap(err)
	}

	if err := initSegmentFile(file, size); err != nil {
		return nil, errors.Join(err, file.Close(), os.Remove(tmp))
	}

	if err := os.Rename(tmp, seg.path); err != nil {
		return nil, errors.Join(wrap(err), file.Close(), os.Remove(tmp))
	}

	if err := syncDir(dir); err != nil {
		return nil, errors.Join(err, file.Close())
	}

	seg.file = file

	return seg, nil
}

func initSegmentFile(file *os.File, size int64) error {
	var header [segmentHeaderSize]byte

	copy(header[:], segmentMagic)
	binary.LittleEndian.PutUint32(header[4:], segmentVersion)

	if _, err := file.Write(header[:]); err != nil {
		return wrap(err)
	}

	if err := preallocate(file, size); err != nil {
		return wrap(err)
	}

	if err := file.Sync(); err != nil {
		return wrap(err)
	}

	return nil
}

// finishSegment makes a segment durable, cuts preallocated space and closes it.
func finishSegment(seg *segment) error {
	file := seg.file
	seg.file = nil

	if err := file.Truncate(seg.size); err != nil {
		return errors.Join(wrap(err), file.Close())
	}

	if err := file.Sync(); err != nil {
		return errors.Join(wrap(err), file.Close())
	}

	if err := file.Close(); err != nil {
		return wrap(err)
	}

	return nil
}

// loadSegment reads and validates a segment file. The active segment may end
// in a torn record, which is cut off. A closed one may only be followed by
// zeros: a truncate lost in a crash.
func loadSegment(dir string, first uint64, active bool) (*segment, error) {
	seg := &segment{
		first: first,
		path:  filepath.Join(dir, segmentFileName(first)),
		size:  segmentHeaderSize,
	}

	file, err := os.Open(seg.path)
	if err != nil {
		return nil, wrap(err)
	}

	fileSize, err := scanSegment(file, seg)
	if err == nil && !active && seg.size < fileSize {
		err = checkZeroTail(file, seg, fileSize)
	}

	if err := errors.Join(err, file.Close()); err != nil {
		return nil, err
	}

	if active && seg.size < fileSize {
		if err := os.Truncate(seg.path, seg.size); err != nil {
			return nil, wrap(err)
		}
	}

	return seg, nil
}

// scanSegment walks the records of a segment, filling count, size and the
// sparse index up to the last intact record. It returns the file size.
func scanSegment(file *os.File, seg *segment) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, wrap(err)
	}

	if err := checkHeader(file, seg.path); err != nil {
		return 0, err
	}

	reader := newChunkReader(file, segmentHeaderSize, info.Size())

	for {
		start := reader.offset()

		if _, err := reader.next(); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, errTorn) {
				return info.Size(), nil
			}

			return 0, err
		}

		seg.advance(reader.offset() - start)
	}
}

func checkHeader(file *os.File, path string) error {
	var header [segmentHeaderSize]byte

	if _, err := file.ReadAt(header[:], 0); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: %s is shorter than a header", ErrCorrupt, path)
		}

		return wrap(err)
	}

	if string(header[:4]) != segmentMagic || binary.LittleEndian.Uint32(header[4:]) != segmentVersion {
		return fmt.Errorf("%w: %s has a bad header", ErrCorrupt, path)
	}

	return nil
}

func checkZeroTail(file *os.File, seg *segment, fileSize int64) error {
	buf := make([]byte, min(chunkSize, fileSize-seg.size))

	for offset := seg.size; offset < fileSize; offset += int64(len(buf)) {
		chunk := buf[:min(int64(len(buf)), fileSize-offset)]

		if _, err := file.ReadAt(chunk, offset); err != nil {
			return wrap(err)
		}

		for _, b := range chunk {
			if b != 0 {
				return fmt.Errorf("%w: %s has data after its last record", ErrCorrupt, seg.path)
			}
		}
	}

	return nil
}

// chunkReader decodes consecutive records between two offsets, reading in
// chunks. Returned data aliases a chunk buffer that is never reused, so it
// stays valid after the next call.
type chunkReader struct {
	src io.ReaderAt
	off int64 // file offset of buf[pos]
	end int64
	buf []byte
	pos int
}

func newChunkReader(src io.ReaderAt, offset, end int64) *chunkReader {
	return &chunkReader{src: src, off: offset, end: end}
}

func (c *chunkReader) offset() int64 {
	return c.off
}

func (c *chunkReader) next() ([]byte, error) {
	if c.off >= c.end {
		return nil, io.EOF
	}

	if c.end-c.off < recordHeaderSize {
		return nil, errTorn
	}

	if err := c.fill(recordHeaderSize); err != nil {
		return nil, err
	}

	need := recordHeaderSize + int64(binary.LittleEndian.Uint32(c.buf[c.pos:]))
	if need > c.end-c.off {
		return nil, errTorn
	}

	if err := c.fill(need); err != nil {
		return nil, err
	}

	data, size, err := decodeRecord(c.buf[c.pos : c.pos+int(need)])
	if err != nil {
		return nil, err
	}

	c.pos += size
	c.off += int64(size)

	return data, nil
}

// fill makes at least need bytes available at buf[pos:], reading a fresh
// chunk from the current offset when the buffered rest is too short.
func (c *chunkReader) fill(need int64) error {
	if int64(len(c.buf)-c.pos) >= need {
		return nil
	}

	buf := make([]byte, min(max(need, chunkSize), c.end-c.off))

	if _, err := c.src.ReadAt(buf, c.off); err != nil {
		return wrap(err)
	}

	c.buf, c.pos = buf, 0

	return nil
}
