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

// load rebuilds the log from its directory. It deletes temp files and committed
// segments a crash left behind and cuts a torn tail off the active segment. Any
// other inconsistency it reads is ErrCorrupt. Only what follows the checkpoint
// is read; the rest is verified by the first read that touches it.
func (l *Log) load() error { // todo: log point in different file, needed poolishing and refactoring for the whole project
	firsts, err := listSegments(l.dir)
	if err != nil {
		return err
	}

	cp, found, err := readCheckpoint(l.dir)
	if err != nil {
		return err
	}

	var segments []*segment

	switch {
	case len(firsts) == 0 && found:
		return fmt.Errorf("%w: checkpoint %d but no segments", ErrCorrupt, cp.committed)
	case len(firsts) == 0:
		seg, err := createSegment(l.dir, 1, l.cfg.SegmentSize)
		if err != nil {
			return err
		}

		segments = []*segment{seg}
	default:
		if !found && firsts[0] != 1 {
			return fmt.Errorf("%w: no checkpoint but the first segment starts at %d", ErrCorrupt, firsts[0])
		}

		segments, err = loadSegments(l.dir, firsts, cp, l.cfg.SegmentSize)
		if err != nil {
			return err
		}
	}

	// Recovery synced the active segment, so every record found is durable.
	l.segments = segments
	l.committed = cp.committed
	l.saved = cp
	l.markSynced()

	for _, seg := range segments {
		l.size += seg.size
	}

	return nil
}

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

// loadSegments validates the segments named by firsts against the checkpoint
// and opens the last one for appending. Segments holding only committed records
// are skipped and deleted once the rest is valid: a crash after the checkpoint
// is written can leave any of them, so gaps there are no damage.
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
		return nil, fmt.Errorf("%w: checkpoint names segment %d, which is missing", ErrCorrupt, cp.segment)
	}

	segments, err := openSegments(dir, live, cp, prealloc)
	if err != nil {
		return nil, err
	}

	active := segments[len(segments)-1]
	if cp.committed >= active.nextIndex() {
		return nil, errors.Join(
			fmt.Errorf("%w: checkpoint %d is past the last record %d", ErrCorrupt, cp.committed, active.nextIndex()-1),
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

// trustClosedSegment accepts a segment unread. It was sealed before the next
// one was created, so a crash cannot have torn it, and its records end where
// the next segment starts.
func trustClosedSegment(dir string, first, next uint64) (*segment, error) {
	seg := newSegment(dir, first)

	info, err := os.Stat(seg.path)
	if err != nil {
		return nil, wrap(err)
	}

	seg.trust(info.Size(), next)

	return seg, nil
}

// recoverActiveSegment opens the last segment for appending. It trusts what the
// checkpoint marks durable and scans the rest. A torn tail is truncated and the
// file synced before any append, so new records never land in front of stale
// bytes that a later crash could expose.
func recoverActiveSegment(dir string, first uint64, cp checkpoint, prealloc int64) (*segment, error) {
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

// isTornRecord reports whether the record at offset, which failed to decode
// with decodeErr, is a write cut short by a crash rather than damaged data. A
// cut write runs past the end of the file or has a sector that reads as zeros.
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
