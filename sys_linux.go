//go:build linux

package wal

import (
	"errors"
	"os"
	"syscall"
)

// fdatasync skips the inode update fsync would journal on every append.
func fdatasync(file *os.File) error {
	fd, err := descriptor(file)
	if err != nil {
		return err
	}

	if err := syscall.Fdatasync(fd); err != nil {
		return &os.PathError{Op: "fdatasync", Path: file.Name(), Err: err}
	}

	return nil
}

// preallocate reserves size bytes so appends do not allocate extents one by
// one. Filesystems without fallocate are left alone.
func preallocate(file *os.File, size int64) error {
	fd, err := descriptor(file)
	if err != nil {
		return err
	}

	err = syscall.Fallocate(fd, 0, 0, size)
	if err == nil || errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.ENOSYS) {
		return nil
	}

	return &os.PathError{Op: "fallocate", Path: file.Name(), Err: err}
}
