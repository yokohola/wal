//go:build unix

package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const lockName = "LOCK"

// lockDir takes an exclusive flock on the lock file in dir. The lock lasts
// while the returned file is open, so a crashed process never leaves it held.
func lockDir(dir string) (*os.File, error) {
	path := filepath.Join(dir, lockName)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, wrap(err)
	}

	err = control(file, func(fd int) error {
		return syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
	})
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return nil, errors.Join(ErrLocked, file.Close())
	}

	if err != nil {
		return nil, errors.Join(fmt.Errorf("wal: lock %s: %w", path, err), file.Close())
	}

	return file, nil
}

// control runs fn with the descriptor of file, which stays open meanwhile.
func control(file *os.File, fn func(fd int) error) error {
	conn, err := file.SyscallConn()
	if err != nil {
		return err
	}

	var fnErr error

	if err := conn.Control(func(fd uintptr) { fnErr = fn(int(fd)) }); err != nil {
		return err
	}

	return fnErr
}
