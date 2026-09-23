package wal

import (
	"errors"
	"os"
	"syscall"
)

// fdatasync flushes data and the metadata needed to read it back, skipping
// timestamps. Preallocation keeps the file size fixed, so appends usually
// need no metadata flush at all.
func fdatasync(file *os.File) error {
	if err := withFD(file, syscall.Fdatasync); err != nil {
		return &os.PathError{Op: "fdatasync", Path: file.Name(), Err: err}
	}

	return nil
}

// preallocate reserves size bytes so appends do not allocate blocks one by one.
// Filesystems without fallocate are left alone.
func preallocate(file *os.File, size int64) error {
	err := withFD(file, func(fd int) error {
		return syscall.Fallocate(fd, 0, 0, size)
	})
	if err == nil || errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.ENOSYS) {
		return nil
	}

	return &os.PathError{Op: "fallocate", Path: file.Name(), Err: err}
}
