//go:build !unix

package wal

import (
	"errors"
	"fmt"
	"os"
)

func lockDir(string) (*os.File, error) {
	return nil, fmt.Errorf("wal: directory lock: %w", errors.ErrUnsupported)
}
