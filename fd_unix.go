//go:build unix

package wal

import (
	"math"
	"os"
)

// descriptor returns the file's descriptor for raw system calls. A closed
// file reports an invalid one.
func descriptor(file *os.File) (int, error) {
	fd := file.Fd()
	if fd > math.MaxInt {
		return 0, &os.PathError{Op: "descriptor", Path: file.Name(), Err: os.ErrClosed}
	}

	return int(fd), nil
}
