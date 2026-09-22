package wal

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// tempExt marks a file written under a temp name and renamed once complete.
const tempExt = ".tmp"

// writeFileAtomic replaces path with data through a synced temp file and a
// rename, then syncs the directory so the rename is durable too.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + tempExt

	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return wrap(err)
	}

	if _, err := file.Write(data); err != nil {
		return errors.Join(wrap(err), file.Close(), removeFile(tmp))
	}

	if err := file.Sync(); err != nil {
		return errors.Join(wrap(err), file.Close(), removeFile(tmp))
	}

	if err := file.Close(); err != nil {
		return errors.Join(wrap(err), removeFile(tmp))
	}

	if err := os.Rename(tmp, path); err != nil {
		return errors.Join(wrap(err), removeFile(tmp))
	}

	return syncDir(filepath.Dir(path))
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

// removeFile deletes path. A file that is already gone counts as deleted.
func removeFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return wrap(err)
	}

	return nil
}
