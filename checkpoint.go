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

// The checkpoint file holds the committed index and a CRC32C of it. It is
// replaced through a temp file and a rename, so it is the old or the new one.
const (
	checkpointName = "checkpoint"
	checkpointSize = 12
)

// readCheckpoint returns the committed index, or false when no checkpoint was
// ever written.
func readCheckpoint(dir string) (uint64, bool, error) {
	path := filepath.Join(dir, checkpointName)

	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, wrap(err)
	}
	defer func() { _ = file.Close() }() // read only, nothing to lose

	buf, err := io.ReadAll(io.LimitReader(file, checkpointSize+1))
	if err != nil {
		return 0, false, wrap(err)
	}

	if len(buf) != checkpointSize {
		return 0, false, fmt.Errorf("%w: %s is not %d bytes", ErrCorrupt, path, checkpointSize)
	}

	if crc32.Checksum(buf[:8], crcTable) != binary.LittleEndian.Uint32(buf[8:]) {
		return 0, false, fmt.Errorf("%w: %s checksum mismatch", ErrCorrupt, path)
	}

	return binary.LittleEndian.Uint64(buf[:8]), true, nil
}

func writeCheckpoint(dir string, index uint64) error {
	var buf [checkpointSize]byte

	binary.LittleEndian.PutUint64(buf[:8], index)
	binary.LittleEndian.PutUint32(buf[8:], crc32.Checksum(buf[:8], crcTable))

	return writeFileAtomic(filepath.Join(dir, checkpointName), buf[:])
}
