package wal

import "os"

// fdatasync falls back to a full fsync, F_FULLFSYNC on macOS. macOS is a
// development target; production runs on Linux.
func fdatasync(file *os.File) error {
	return file.Sync()
}

// preallocate is a no-op; appends grow the file instead.
func preallocate(*os.File, int64) error {
	return nil
}
