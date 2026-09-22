//go:build !linux

package wal

import "os"

func fdatasync(file *os.File) error {
	return file.Sync()
}

func preallocate(*os.File, int64) error {
	return nil
}
