package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
)

// A record on disk is a little-endian length, a CRC32C over that length and
// the data, then the data. Covering the length means a zero-filled tail never
// parses as a record, so preallocated space is harmless.
const recordHeaderSize = 8

var (
	errTooLarge = errors.New("wal: record too large")
	errTorn     = errors.New("torn record")
)

func checksum(header, data []byte) uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	crc := crc32.Update(0, table, header)

	return crc32.Update(crc, table, data)
}

// appendRecord frames data and appends it to dst. Data must fit a 32-bit
// length.
func appendRecord(dst, data []byte) ([]byte, error) {
	size := len(data)
	if uint64(size) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: %d bytes", errTooLarge, size)
	}

	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(size))
	dst = binary.LittleEndian.AppendUint32(dst, checksum(dst[start:start+4], data))

	return append(dst, data...), nil
}

// decodeRecord parses the record at the start of buf. The returned data
// aliases buf. It fails with errTorn unless buf starts with a complete,
// intact record.
func decodeRecord(buf []byte) ([]byte, int, error) {
	if len(buf) < recordHeaderSize {
		return nil, 0, errTorn
	}

	end := recordHeaderSize + int64(binary.LittleEndian.Uint32(buf[:4]))
	if end > int64(len(buf)) {
		return nil, 0, errTorn
	}

	data := buf[recordHeaderSize:end]
	if checksum(buf[:4], data) != binary.LittleEndian.Uint32(buf[4:8]) {
		return nil, 0, errTorn
	}

	return data, int(end), nil
}
