package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// The checkpoint file holds the committed index, the active segment's durable
// end and their CRC32C. A temp file and rename make replacing it atomic.
const (
	checkpointName = "checkpoint"
	checkpointSize = 36
)

// checkpoint is the durable state Open starts from. The bytes of segment up to
// end hold the records below next and were synced before the checkpoint was
// written.
type checkpoint struct {
	committed uint64
	segment   uint64
	end       int64
	next      uint64
}

// readCheckpoint returns the checkpoint, or false when none was ever written.
func readCheckpoint(dir string) (checkpoint, bool, error) {
	path := filepath.Join(dir, checkpointName)

	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return checkpoint{}, false, nil
	}

	if err != nil {
		return checkpoint{}, false, wrap(err)
	}
	defer func() { _ = file.Close() }() // read only, nothing to lose

	buf, err := io.ReadAll(io.LimitReader(file, checkpointSize+1))
	if err != nil {
		return checkpoint{}, false, wrap(err)
	}

	if len(buf) != checkpointSize {
		return checkpoint{}, false, fmt.Errorf("%w: %s is not %d bytes", ErrCorrupt, path, checkpointSize)
	}

	if crc32.Checksum(buf[:32], crcTable) != binary.LittleEndian.Uint32(buf[32:]) {
		return checkpoint{}, false, fmt.Errorf("%w: %s checksum mismatch", ErrCorrupt, path)
	}

	cp := checkpoint{
		committed: binary.LittleEndian.Uint64(buf[0:8]),
		segment:   binary.LittleEndian.Uint64(buf[8:16]),
		end:       int64(binary.LittleEndian.Uint64(buf[16:24])),
		next:      binary.LittleEndian.Uint64(buf[24:32]),
	}

	return cp, true, nil
}

// writeCheckpoint durably replaces the checkpoint with cp.
func writeCheckpoint(dir string, cp checkpoint) error {
	var buf [checkpointSize]byte

	binary.LittleEndian.PutUint64(buf[0:8], cp.committed)
	binary.LittleEndian.PutUint64(buf[8:16], cp.segment)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(cp.end))
	binary.LittleEndian.PutUint64(buf[24:32], cp.next)
	binary.LittleEndian.PutUint32(buf[32:], crc32.Checksum(buf[:32], crcTable))

	return writeFileAtomic(filepath.Join(dir, checkpointName), buf[:])
}
