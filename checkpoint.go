package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// The checkpoint file holds the committed index and a CRC32C of it. It is
// replaced atomically, so it is either intact or absent.
const (
	checkpointFile = "checkpoint"
	checkpointSize = 12
	tmpExt         = ".tmp"
)

func readCheckpoint(dir string) (uint64, bool, error) {
	path := filepath.Join(dir, checkpointFile)

	buf, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, wrap(err)
	}

	if len(buf) != checkpointSize {
		return 0, false, fmt.Errorf("%w: %s has %d bytes", ErrCorrupt, path, len(buf))
	}

	if checksum(buf[:8], nil) != binary.LittleEndian.Uint32(buf[8:]) {
		return 0, false, fmt.Errorf("%w: %s checksum mismatch", ErrCorrupt, path)
	}

	return binary.LittleEndian.Uint64(buf[:8]), true, nil
}

func writeCheckpoint(dir string, index uint64) error {
	var buf [checkpointSize]byte

	binary.LittleEndian.PutUint64(buf[:8], index)
	binary.LittleEndian.PutUint32(buf[8:], checksum(buf[:8], nil))

	return writeFileAtomic(filepath.Join(dir, checkpointFile), buf[:])
}

// writeFileAtomic replaces path with data through a synced temp file and
// rename, then syncs the directory so the rename is durable too.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + tmpExt

	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return wrap(err)
	}

	if err := writeAndSync(file, data); err != nil {
		return errors.Join(err, file.Close(), os.Remove(tmp))
	}

	if err := file.Close(); err != nil {
		return wrap(err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return wrap(err)
	}

	return syncDir(filepath.Dir(path))
}

func writeAndSync(file *os.File, data []byte) error {
	if _, err := file.Write(data); err != nil {
		return wrap(err)
	}

	if err := file.Sync(); err != nil {
		return wrap(err)
	}

	return nil
}

func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return wrap(err)
	}

	if err := handle.Sync(); err != nil {
		return errors.Join(wrap(err), handle.Close())
	}

	if err := handle.Close(); err != nil {
		return wrap(err)
	}

	return nil
}
