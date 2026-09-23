package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
)

// A record is a 12-byte header (data length, CRC32C of index and data, CRC32C
// of those 8 bytes) and the data. The index is not stored, so a record at the
// wrong position fails its checksum.
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

var (
	crcTable      = crc32.MakeTable(crc32.Castagnoli)
	indexCRCTable = makeIndexCRCTable()
)

// recordReader decodes consecutive records from src between offset and end,
// fetching at most readChunkSize bytes and not past stop. Every fetch allocates
// a new chunk, so returned data stays valid.
type recordReader struct {
	src    io.ReaderAt
	offset int64 // file offset of chunk[0]
	end    int64
	stop   int64 // where the wanted records end; zero means end
	chunk  []byte
}

// next returns the data of the next record, which must have index, or io.EOF at
// end. On a decode error offset stays at the failing record; after errBadData,
// skip passes over it.
func (r *recordReader) next(index uint64) ([]byte, error) {
	size, err := r.locate()
	if err != nil {
		return nil, err
	}

	data, err := decodeRecord(r.chunk[:size], index)
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

// advance moves past a buffered record of size bytes.
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

	size := int64(readChunkSize)
	if r.stop > 0 {
		size = min(size, r.stop-r.offset)
	}

	chunk := make([]byte, min(max(n, size), r.end-r.offset))

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

// appendRecord frames data as the record with index and appends it to dst. The
// caller ensures that len(data) <= formatMaxRecordLen.
func appendRecord(dst []byte, index uint64, data []byte) []byte {
	start := len(dst)

	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(data)))
	dst = binary.LittleEndian.AppendUint32(dst, dataChecksum(index, data))
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

// decodeRecord returns the data of the record starting buf, which must have
// index. The result aliases buf with its capacity capped, so appending to it
// never touches what follows.
func decodeRecord(buf []byte, index uint64) ([]byte, error) {
	length, err := decodeHeader(buf)
	if err != nil {
		return nil, err
	}

	end := recordHeaderSize + length
	if end > int64(len(buf)) {
		return nil, errShortRecord
	}

	data := buf[recordHeaderSize:end:end]
	if dataChecksum(index, data) != binary.LittleEndian.Uint32(buf[4:8]) {
		return nil, errBadData
	}

	return data, nil
}

// dataChecksum is the CRC32C of index, as 8 little-endian bytes, and data.
// The index goes through indexCRCTable, since a slice of its bytes would escape
// to the heap through crc32.
func dataChecksum(index uint64, data []byte) uint32 {
	crc := ^uint32(index)
	hi := index >> 32
	crc = indexCRCTable[0][byte(hi>>24)] ^ indexCRCTable[1][byte(hi>>16)] ^
		indexCRCTable[2][byte(hi>>8)] ^ indexCRCTable[3][byte(hi)] ^
		indexCRCTable[4][byte(crc>>24)] ^ indexCRCTable[5][byte(crc>>16)] ^
		indexCRCTable[6][byte(crc>>8)] ^ indexCRCTable[7][byte(crc)]

	return crc32.Update(^crc, crcTable, data)
}

// makeIndexCRCTable builds slicing-by-8 tables for crcTable: entry [k][b] is
// the CRC contribution of byte b followed by k zero bytes, so the 8 bytes of an
// index take 8 independent lookups.
func makeIndexCRCTable() *[8]crc32.Table {
	var tab [8]crc32.Table

	tab[0] = *crcTable
	for k := 1; k < 8; k++ {
		for b := range 256 {
			prev := tab[k-1][b]
			tab[k][b] = prev>>8 ^ crcTable[byte(prev)]
		}
	}

	return &tab
}

// isDecodeError reports whether err is a failure to decode a record or segment
// header rather than an I/O one.
func isDecodeError(err error) bool {
	for _, target := range []error{
		errShortRecord, errBadHeader, errBadData,
		errShortSegment, errBadSegmentHeader, errMissingRecords,
	} {
		if errors.Is(err, target) {
			return true
		}
	}

	return false
}
