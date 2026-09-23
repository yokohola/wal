package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
)

// A record is a 12-byte header and its data. The header holds the data length,
// a CRC32C of the data and a CRC32C of those first 8 bytes, so a damaged length
// is caught before it is trusted. Zero bytes never form a valid header.
const (
	recordHeaderSize   = 12
	formatMaxRecordLen = math.MaxUint32

	// readChunkSize is how many bytes a recordReader fetches at a time.
	readChunkSize = 256 << 10
)

// Decode failures. errShortRecord means the input ends inside the record.
var (
	errShortRecord = errors.New("record runs past the end of the data")
	errBadHeader   = errors.New("record header checksum mismatch")
	errBadData     = errors.New("record data checksum mismatch")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// recordReader decodes consecutive records from src between offset and end.
// Every fetch allocates a new chunk, so returned data stays valid.
type recordReader struct {
	src    io.ReaderAt
	offset int64 // file offset of chunk[0]
	end    int64
	chunk  []byte
}

// next returns the data of the next record or io.EOF at end. On a decode error
// offset stays at the failing record; after errBadData, skip passes over it.
func (r *recordReader) next() ([]byte, error) {
	size, err := r.locate()
	if err != nil {
		return nil, err
	}

	data, err := decodeRecord(r.chunk[:size])
	if err != nil {
		return nil, err
	}

	r.advance(size)

	return data, nil
}

// skip passes over the next record without checking its data. Its header gives
// its size, so this works on a record whose data alone is damaged.
func (r *recordReader) skip() error {
	size, err := r.locate()
	if err != nil {
		return err
	}

	r.advance(size)

	return nil
}

// locate buffers the next record whole and returns its size.
func (r *recordReader) locate() (int64, error) {
	if r.offset >= r.end {
		return 0, io.EOF
	}

	if err := r.fill(recordHeaderSize); err != nil {
		return 0, err
	}

	length, err := decodeHeader(r.chunk)
	if err != nil {
		return 0, err
	}

	size := recordHeaderSize + length
	if err := r.fill(size); err != nil {
		return 0, err
	}

	return size, nil
}

func (r *recordReader) advance(size int64) {
	r.chunk = r.chunk[size:]
	r.offset += size
}

// fill makes at least n bytes available in chunk, fetching from offset when
// fewer are buffered.
func (r *recordReader) fill(n int64) error {
	if int64(len(r.chunk)) >= n {
		return nil
	}

	if r.end-r.offset < n {
		return errShortRecord
	}

	chunk := make([]byte, min(max(n, readChunkSize), r.end-r.offset))

	read, err := r.src.ReadAt(chunk, r.offset)
	if read < len(chunk) {
		if err == nil || errors.Is(err, io.EOF) {
			return errShortRecord
		}

		return wrap(err)
	}

	r.chunk = chunk

	return nil
}

// appendRecord frames data and appends it to dst. The caller ensures that
// len(data) <= formatMaxRecordLen.
func appendRecord(dst, data []byte) []byte {
	start := len(dst)

	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(data)))
	dst = binary.LittleEndian.AppendUint32(dst, crc32.Checksum(data, crcTable))
	dst = binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:start+8], crcTable))

	return append(dst, data...)
}

// decodeHeader returns the data length of the record starting buf.
func decodeHeader(buf []byte) (int64, error) {
	if len(buf) < recordHeaderSize {
		return 0, errShortRecord
	}

	if crc32.Checksum(buf[:8], crcTable) != binary.LittleEndian.Uint32(buf[8:12]) {
		return 0, errBadHeader
	}

	return int64(binary.LittleEndian.Uint32(buf[0:4])), nil
}

// decodeRecord returns the data of the record starting buf. The result aliases
// buf with its capacity capped, so appending to it never touches what follows.
func decodeRecord(buf []byte) ([]byte, error) {
	length, err := decodeHeader(buf)
	if err != nil {
		return nil, err
	}

	end := recordHeaderSize + length
	if end > int64(len(buf)) {
		return nil, errShortRecord
	}

	data := buf[recordHeaderSize:end:end]
	if crc32.Checksum(data, crcTable) != binary.LittleEndian.Uint32(buf[4:8]) {
		return nil, errBadData
	}

	return data, nil
}

// isDecodeError reports whether err is a failure to decode a record or segment
// header rather than an I/O one.
func isDecodeError(err error) bool {
	for _, target := range []error{
		errShortRecord, errBadHeader, errBadData, errShortSegment, errBadSegmentHeader, errMissingRecords,
	} {
		if errors.Is(err, target) {
			return true
		}
	}

	return false
}
