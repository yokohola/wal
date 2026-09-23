package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// An index file caches a sealed segment's sparse index so Open need not scan it:
// a header (magic, version, first index, end, next index), entries and CRC32C.
// It is written without fsync and used only when it matches the segment; a
// wrong entry fails reads and never returns a record under a wrong index.
const (
	indexExt        = ".idx"
	indexMagic      = "WALI"
	indexVersion    = 1
	indexHeaderSize = 32
	indexEntrySize  = 16
	indexCRCSize    = 4
)

// errBadIndex means an index file does not describe the segment's records.
var errBadIndex = errors.New("bad index file")

// encodeIndex encodes the sparse index of the segment starting at first whose
// records below next end at offset end.
func encodeIndex(first uint64, end int64, next uint64, entries []indexEntry) []byte {
	buf := make([]byte, 0, indexHeaderSize+len(entries)*indexEntrySize+indexCRCSize)

	buf = append(buf, indexMagic...)
	buf = binary.LittleEndian.AppendUint32(buf, indexVersion)
	buf = binary.LittleEndian.AppendUint64(buf, first)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(end))
	buf = binary.LittleEndian.AppendUint64(buf, next)

	for _, entry := range entries {
		buf = binary.LittleEndian.AppendUint64(buf, entry.index)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(entry.offset))
	}

	return binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, crcTable))
}

// decodeIndex returns the entries of buf, an index file, when it is intact and
// describes the segment starting at first whose records below next end at end.
// The first entry must be the first record, and every entry must leave at least
// a record header for each record before the next entry and before end.
func decodeIndex(buf []byte, first uint64, end int64, next uint64) ([]indexEntry, error) {
	size := len(buf) - indexHeaderSize - indexCRCSize
	if size < indexEntrySize || size%indexEntrySize != 0 {
		return nil, fmt.Errorf("%w: %d bytes", errBadIndex, len(buf))
	}

	body := buf[:len(buf)-indexCRCSize]
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(buf[len(body):]) {
		return nil, fmt.Errorf("%w: checksum mismatch", errBadIndex)
	}

	if string(body[0:4]) != indexMagic || binary.LittleEndian.Uint32(body[4:8]) != indexVersion {
		return nil, fmt.Errorf("%w: bad header", errBadIndex)
	}

	gotFirst := binary.LittleEndian.Uint64(body[8:16])
	gotEnd := int64(binary.LittleEndian.Uint64(body[16:24]))
	gotNext := binary.LittleEndian.Uint64(body[24:32])

	if gotFirst != first || gotEnd != end || gotNext != next {
		return nil, fmt.Errorf("%w: records %d to %d ending at %d, want %d to %d ending at %d",
			errBadIndex, gotFirst, gotNext, gotEnd, first, next, end)
	}

	entries := make([]indexEntry, size/indexEntrySize)

	for i := range entries {
		raw := body[indexHeaderSize+i*indexEntrySize:][:indexEntrySize]
		entries[i] = indexEntry{
			index:  binary.LittleEndian.Uint64(raw[0:8]),
			offset: int64(binary.LittleEndian.Uint64(raw[8:16])),
		}
	}

	if entries[0] != (indexEntry{index: first, offset: segmentHeaderSize}) {
		return nil, fmt.Errorf("%w: first entry %d at %d",
			errBadIndex, entries[0].index, entries[0].offset)
	}

	for i := 1; i < len(entries); i++ {
		if prev, entry := entries[i-1], entries[i]; !holdsRecords(prev, entry) {
			return nil, fmt.Errorf("%w: entry %d at %d after %d at %d",
				errBadIndex, entry.index, entry.offset, prev.index, prev.offset)
		}
	}

	if last := entries[len(entries)-1]; !holdsRecords(last, indexEntry{index: next, offset: end}) {
		return nil, fmt.Errorf("%w: last entry %d at %d", errBadIndex, last.index, last.offset)
	}

	return entries, nil
}

// holdsRecords reports whether the records from a up to b fit between their
// offsets: b follows a and each record takes at least its header. The offset of
// a is at least a segment header, so the subtraction cannot overflow.
func holdsRecords(a, b indexEntry) bool {
	return b.index > a.index && b.offset > a.offset &&
		uint64(b.offset-a.offset)/recordHeaderSize >= b.index-a.index
}

// readIndex reads and decodes the index file at path; see decodeIndex. A file
// with more entries than record headers fit before end is rejected unread.
func readIndex(path string, first uint64, end int64, next uint64) ([]indexEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, wrap(err)
	}
	defer func() { _ = file.Close() }() // read only, nothing to lose

	info, err := file.Stat()
	if err != nil {
		return nil, wrap(err)
	}

	size := info.Size()
	entries := (size - indexHeaderSize - indexCRCSize) / indexEntrySize
	if entries > (end-segmentHeaderSize)/recordHeaderSize {
		return nil, fmt.Errorf("%w: %s has %d bytes", errBadIndex, path, size)
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(file, buf); err != nil {
		return nil, wrap(err)
	}

	return decodeIndex(buf, first, end, next)
}

// writeIndex replaces the index file at path with buf through a temp file and a
// rename, so a reader never sees it half written. Nothing is synced: after a
// crash the file may be stale, torn or missing, which costs only a scan.
func writeIndex(path string, buf []byte) error {
	tmp := path + tempExt

	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return errors.Join(wrap(err), removeFile(tmp))
	}

	if err := os.Rename(tmp, path); err != nil {
		return errors.Join(wrap(err), removeFile(tmp))
	}

	return nil
}
