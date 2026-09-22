package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// load rebuilds the in-memory state from the directory.
func (l *Log) load() error {
	segments, err := loadSegments(l.dir)
	if err != nil {
		return err
	}

	if len(segments) == 0 {
		seg, err := createSegment(l.dir, 1, l.opts.SegmentSize)
		if err != nil {
			return err
		}

		segments = append(segments, seg)
	}

	l.segments = segments
	for _, seg := range segments {
		l.size += seg.size
	}

	if err := l.applyCheckpoint(); err != nil {
		return err
	}

	if err := l.openActive(); err != nil {
		return err
	}

	return l.reclaim()
}

// loadSegments validates every segment file and their continuity. Stale temp
// files from an interrupted segment creation or checkpoint are removed.
func loadSegments(dir string) ([]*segment, error) {
	firsts, err := listSegments(dir)
	if err != nil {
		return nil, err
	}

	segments := make([]*segment, 0, len(firsts))

	for pos, first := range firsts {
		seg, err := loadSegment(dir, first, pos == len(firsts)-1)
		if err != nil {
			return nil, err
		}

		if pos > 0 && segments[pos-1].next() != first {
			return nil, fmt.Errorf("%w: %s does not continue %s", ErrCorrupt, seg.path, segments[pos-1].path)
		}

		segments = append(segments, seg)
	}

	return segments, nil
}

func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, wrap(err)
	}

	var firsts []uint64

	for _, entry := range entries {
		name := entry.Name()

		if strings.HasSuffix(name, tmpExt) {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, wrap(err)
			}

			continue
		}

		if first, ok := parseSegmentFileName(name); ok {
			firsts = append(firsts, first)
		}
	}

	slices.Sort(firsts)

	return firsts, nil
}

// applyCheckpoint restores the committed index. A checkpoint past the last
// record means committed records were lost with an unsynced tail; the log
// continues after the checkpoint so no index is ever reused.
func (l *Log) applyCheckpoint() error {
	index, found, err := readCheckpoint(l.dir)
	if err != nil {
		return err
	}

	l.committed = l.segments[0].first
	if found {
		l.committed = max(index, l.committed)
	}

	if l.committed <= l.active().next() {
		return nil
	}

	seg, err := createSegment(l.dir, l.committed, l.opts.SegmentSize)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, seg)
	l.size += segmentHeaderSize

	return nil
}

func (l *Log) openActive() error {
	tail := l.active()
	if tail.file != nil {
		return nil
	}

	file, err := os.OpenFile(tail.path, os.O_RDWR, 0)
	if err != nil {
		return wrap(err)
	}

	if err := preallocate(file, l.opts.SegmentSize); err != nil {
		return errors.Join(wrap(err), file.Close())
	}

	tail.file = file

	return nil
}

// reclaim removes closed segments whose records are all committed.
func (l *Log) reclaim() error {
	for len(l.segments) > 1 && l.segments[0].next() <= l.committed {
		seg := l.segments[0]

		if err := os.Remove(seg.path); err != nil {
			return wrap(err)
		}

		l.size -= seg.size
		l.segments = slices.Delete(l.segments, 0, 1)
	}

	return nil
}
