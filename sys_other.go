//go:build !linux

package wal

import "os"

// fdatasync falls back to a full fsync, which on darwin is F_FULLFSYNC.
func fdatasync(file *os.File) error {
	return file.Sync()
}

func preallocate(*os.File, int64) error {
	return nil
}
