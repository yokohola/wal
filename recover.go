package wal

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// truncateFile cuts a file to size.
var truncateFile = (*os.File).Truncate

// listSegments returns the first indexes of the segments in dir, ascending. It
// deletes this package's temp files and orphaned index files, nothing else.
func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, wrap(err)
	}

	var firsts, indexed []uint64

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
		} else if first, ok := parseIndexName(name); ok {
			indexed = append(indexed, first)
		}
	}

	slices.Sort(firsts)

	for _, first := range indexed {
		if _, found := slices.BinarySearch(firsts, first); !found {
			if err := removeFile(filepath.Join(dir, indexName(first))); err != nil {
				return nil, err
			}
		}
	}

	return firsts, nil
}

// removeSegment deletes the segment starting at first and its index. The index
// goes first, so a crash in between never leaves an index without its segment.
func removeSegment(dir string, first uint64) error {
	if err := removeFile(filepath.Join(dir, indexName(first))); err != nil {
		return err
	}

	return removeFile(filepath.Join(dir, segmentName(first)))
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
			closeSegments(segments))
	}

	for _, first := range firsts[:len(firsts)-len(live)] {
		if err := removeSegment(dir, first); err != nil {
			return nil, errors.Join(err, closeSegments(segments))
		}
	}

	return segments, nil
}

// openSegments loads contiguous segments, verified, and opens the last for
// appending. On failure it closes what it opened.
func openSegments(dir string, firsts []uint64, cp checkpoint, prealloc int64) ([]*segment, error) {
	last := len(firsts) - 1
	segments := make([]*segment, 0, len(firsts))

	for i, first := range firsts[:last] {
		seg, err := loadClosedSegment(dir, first, firsts[i+1])
		if err != nil {
			return nil, errors.Join(err, closeSegments(segments))
		}

		segments = append(segments, seg)
	}

	active, err := recoverActiveSegment(dir, firsts[last], cp, prealloc)
	if err != nil {
		return nil, errors.Join(err, closeSegments(segments))
	}

	return append(segments, active), nil
}

// closeSegments closes the files of segments.
func closeSegments(segments []*segment) error {
	var err error

	for _, seg := range segments {
		err = errors.Join(err, seg.close())
	}

	return err
}

// loadClosedSegment opens a closed segment for reading and verifies it, which a
// crash cannot have torn. A segment verified without its index gets one.
func loadClosedSegment(dir string, first, next uint64) (*segment, error) {
	seg := newSegment(dir, first)

	file, err := openSegment(seg.path)
	if err != nil {
		return nil, wrap(err)
	}

	seg.file = file

	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(wrap(err), seg.close())
	}

	seg.trust(info.Size(), next)

	indexed, err := seg.verify()
	if err != nil {
		return nil, errors.Join(err, seg.close())
	}

	if !indexed {
		seg.saveIndex()
	}

	return seg, nil
}

// recoverActiveSegment opens the last segment for appending, verifying what the
// checkpoint covers and scanning past it; the log ends at the first bad record.
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

	if _, err := seg.verify(); err != nil {
		return nil, errors.Join(err, seg.close())
	}

	return seg, nil
}

// repairActiveSegment scans the active segment past the checkpoint, cuts it at
// the first record that fails to decode, syncs and preallocates the file.
func repairActiveSegment(seg *segment, file *os.File, cp checkpoint, prealloc int64) error {
	info, err := file.Stat()
	if err != nil {
		return wrap(err)
	}

	if err := seg.checkHeader(file); err != nil {
		return seg.recordError(0, err)
	}

	if cp.segment == seg.first {
		empty := cp.end == segmentHeaderSize
		if cp.end < segmentHeaderSize || cp.end > info.Size() || cp.next < seg.first ||
			cp.next <= cp.committed || empty != (cp.next == seg.first) ||
			cp.next-seg.first > uint64(cp.end-segmentHeaderSize)/recordHeaderSize {
			return fmt.Errorf(
				"%w: checkpoint end %d at index %d, committed %d, does not fit %s of %d bytes",
				ErrCorrupt, cp.end, cp.next, cp.committed, seg.path, info.Size())
		}

		seg.trust(cp.end, cp.next)
	}

	// Nothing proves these records durable, so, as in PostgreSQL crash recovery,
	// the first bad one ends the log along with everything after it. A read
	// error fails Open instead, since the records may be intact.
	if err := seg.scan(file, info.Size()); err != nil && !isDecodeError(err) {
		return err
	}

	if seg.size < info.Size() {
		if err := removeStaleIndex(seg); err != nil {
			return err
		}

		if err := truncateFile(file, seg.size); err != nil {
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

// removeStaleIndex deletes the index a Close left for the active segment before
// Open cuts it: the cut records may later be replaced by others that match the
// index's end and count at different offsets.
func removeStaleIndex(seg *segment) error {
	err := os.Remove(seg.indexPath())
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	if err != nil {
		return wrap(err)
	}

	return syncDir(filepath.Dir(seg.path))
}

// isTempName reports whether name is a segment, index or checkpoint temp file
// that this package writes before a rename.
func isTempName(name string) bool {
	base, ok := strings.CutSuffix(name, tempExt)
	if !ok {
		return false
	}

	_, isSegment := parseSegmentName(base)
	_, isIndex := parseIndexName(base)

	return isSegment || isIndex || base == checkpointName
}
