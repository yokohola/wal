package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// sectorSize is the unit a disk writes atomically. A write cut short by a crash
// leaves whole sectors unwritten, and those read back as zeros.
const sectorSize = 512

var zeroSector [sectorSize]byte

// listSegments returns the first indexes of the segments in dir, ascending. It
// deletes the temp files a crash leaves while a segment or the checkpoint is
// written and leaves every other file alone.
func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, wrap(err)
	}

	var firsts []uint64

	for _, entry := range entries {
		name := entry.Name()

		if isTempName(name) {
			if err := removeFile(filepath.Join(dir, name)); err != nil {
				return nil, err
			}

			continue
		}

		if first, ok := parseSegmentName(name); ok {
			firsts = append(firsts, first)
		}
	}

	slices.Sort(firsts)

	return firsts, nil
}

// loadSegments validates segments against the checkpoint and opens the last.
// Fully committed ones, gaps included, are deleted once the rest is valid.
func loadSegments(dir string, firsts []uint64, cp checkpoint, prealloc int64) ([]*segment, error) {
	live := firsts
	for len(live) > 1 && live[1] <= cp.committed+1 {
		live = live[1:]
	}

	if cp.committed+1 < live[0] {
		return nil, fmt.Errorf("%w: checkpoint %d but the first segment starts at %d",
			ErrCorrupt,
			cp.committed,
			live[0])
	}

	if cp.segment >= live[0] && !slices.Contains(live, cp.segment) {
		return nil, fmt.Errorf("%w: checkpoint names segment %d, which is missing",
			ErrCorrupt, cp.segment)
	}

	segments, err := openSegments(dir, live, cp, prealloc)
	if err != nil {
		return nil, err
	}

	active := segments[len(segments)-1]
	if cp.committed >= active.nextIndex() {
		return nil, errors.Join(
			fmt.Errorf("%w: checkpoint %d is past the last record %d",
				ErrCorrupt, cp.committed, active.nextIndex()-1),
			active.close())
	}

	for _, first := range firsts[:len(firsts)-len(live)] {
		if err := removeFile(filepath.Join(dir, segmentName(first))); err != nil {
			return nil, errors.Join(err, active.close())
		}
	}

	return segments, nil
}

// openSegments loads contiguous segments and opens the last for appending.
func openSegments(dir string, firsts []uint64, cp checkpoint, prealloc int64) ([]*segment, error) {
	last := len(firsts) - 1
	segments := make([]*segment, 0, len(firsts))

	for i, first := range firsts[:last] {
		seg, err := trustClosedSegment(dir, first, firsts[i+1])
		if err != nil {
			return nil, err
		}

		segments = append(segments, seg)
	}

	active, err := recoverActiveSegment(dir, firsts[last], cp, prealloc)
	if err != nil {
		return nil, err
	}

	return append(segments, active), nil
}

// trustClosedSegment accepts a segment unread: it was sealed before the next
// one was created, so a crash cannot have torn it.
func trustClosedSegment(dir string, first, next uint64) (*segment, error) {
	seg := newSegment(dir, first)

	info, err := os.Stat(seg.path)
	if err != nil {
		return nil, wrap(err)
	}

	seg.trust(info.Size(), next)

	return seg, nil
}

// recoverActiveSegment opens the last segment for appending, scanning only past
// the checkpoint. A torn tail is truncated and synced before any append.
func recoverActiveSegment(
	dir string, first uint64, cp checkpoint, prealloc int64,
) (*segment, error) {
	seg := newSegment(dir, first)

	file, err := os.OpenFile(seg.path, os.O_RDWR, 0)
	if err != nil {
		return nil, wrap(err)
	}

	if err := repairActiveSegment(seg, file, cp, prealloc); err != nil {
		return nil, errors.Join(err, file.Close())
	}

	seg.file = file

	return seg, nil
}

func repairActiveSegment(seg *segment, file *os.File, cp checkpoint, prealloc int64) error {
	info, err := file.Stat()
	if err != nil {
		return wrap(err)
	}

	if err := seg.checkHeader(file); err != nil {
		return err
	}

	if cp.segment == seg.first {
		empty := cp.end == segmentHeaderSize
		if cp.end < segmentHeaderSize || cp.end > info.Size() || cp.next < seg.first ||
			cp.next <= cp.committed || empty != (cp.next == seg.first) ||
			cp.next-seg.first > uint64(cp.end-segmentHeaderSize)/recordHeaderSize {
			return fmt.Errorf("%w: checkpoint end %d at index %d, committed %d, does not fit %s of %d bytes",
				ErrCorrupt, cp.end, cp.next, cp.committed, seg.path, info.Size())
		}

		seg.trust(cp.end, cp.next)
	}

	if scanErr := seg.scan(file, info.Size()); scanErr != nil {
		torn, err := isTornRecord(file, seg.size, scanErr)
		if err != nil {
			return err
		}

		if !torn {
			return seg.recordError(seg.size, scanErr)
		}
	}

	if seg.size < info.Size() {
		if err := file.Truncate(seg.size); err != nil {
			return wrap(err)
		}
	}

	if err := file.Sync(); err != nil {
		return wrap(err)
	}

	if err := preallocate(file, prealloc); err != nil {
		return wrap(err)
	}

	return nil
}

// isTornRecord reports whether a record that failed to decode was cut short by a
// crash: it runs past the file end or has a sector that reads as zeros.
func isTornRecord(file *os.File, offset int64, decodeErr error) (bool, error) {
	if errors.Is(decodeErr, errShortRecord) {
		return true, nil
	}

	if !errors.Is(decodeErr, errBadHeader) && !errors.Is(decodeErr, errBadData) {
		return false, decodeErr
	}

	record := make([]byte, recordHeaderSize)
	if _, err := file.ReadAt(record, offset); err != nil {
		return false, wrap(err)
	}

	if errors.Is(decodeErr, errBadData) {
		length, err := decodeHeader(record)
		if err != nil {
			return false, err
		}

		record = make([]byte, recordHeaderSize+length)
		if _, err := file.ReadAt(record, offset); err != nil {
			return false, wrap(err)
		}
	}

	return hasZeroSector(record, offset), nil
}

// hasZeroSector reports whether the part of buf inside some disk sector is all
// zeros. buf starts at file offset offset.
func hasZeroSector(buf []byte, offset int64) bool {
	for len(buf) > 0 {
		n := min(int64(len(buf)), sectorSize-offset%sectorSize)
		if bytes.Equal(buf[:n], zeroSector[:n]) {
			return true
		}

		buf = buf[n:]
		offset += n
	}

	return false
}

// isTempName reports whether name is a segment or checkpoint temp file that
// this package writes before a rename.
func isTempName(name string) bool {
	base, ok := strings.CutSuffix(name, tempExt)
	if !ok {
		return false
	}

	_, isSegment := parseSegmentName(base)

	return isSegment || base == checkpointName
}
