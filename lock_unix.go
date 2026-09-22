//go:build unix

package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockDir takes an exclusive flock on the lock file. The lock lives as long
// as the returned file is open, so a crashed process never leaves it behind.
func lockDir(dir string) (*os.File, error) {
	path := filepath.Join(dir, lockFile)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, wrap(err)
	}

	fd, err := descriptor(file)
	if err != nil {
		return nil, errors.Join(wrap(err), file.Close())
	}

	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.Join(ErrLocked, file.Close())
		}

		return nil, errors.Join(fmt.Errorf("wal: lock %s: %w", path, err), file.Close())
	}

	return file, nil
}
