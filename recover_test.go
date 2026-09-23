package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReopen_RestoresState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4)}

	l := openLog(t, dir, cfg)
	appendN(t, l, 10)
	require.NoError(t, l.Commit(3))
	size := l.Size()
	require.NoError(t, l.Close())

	l = openLog(t, dir, cfg)
	require.Equal(t, uint64(1), l.FirstIndex())
	require.Equal(t, uint64(10), l.LastIndex())
	require.Equal(t, uint64(3), l.Committed())
	require.Equal(t, size, l.Size())
	requireRecords(t, readAll(t, l), 1, 10)

	appendN(t, l, 2)
	requireRecords(t, readAll(t, l), 1, 12)
}

func TestReopen_CutsTornTail(t *testing.T) {
	t.Parallel()

	valid := appendRecord(nil, payload(4))

	cases := map[string][]byte{
		"short header":           valid[:recordHeaderSize-3],
		"short data":             valid[:len(valid)-1],
		"header only":            valid[:recordHeaderSize],
		"zero fill":              make([]byte, 4096),
		"zero header then bytes": append(make([]byte, recordHeaderSize), valid...),
	}

	for name, tail := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			l := openLog(t, dir, Config{})
			appendN(t, l, 3)
			require.NoError(t, l.Close())

			appendBytes(t, filepath.Join(dir, segmentName(1)), tail)

			l = openLog(t, dir, Config{})
			require.Equal(t, uint64(3), l.LastIndex())
			require.Equal(t, segmentOf(3), l.Size())

			appendN(t, l, 2)
			require.NoError(t, l.Close())

			l = openLog(t, dir, Config{})
			requireRecords(t, readAll(t, l), 1, 5)
		})
	}
}

// A crash can persist later pages of an unsynced write but not earlier ones.
// Unwritten sectors read as zeros, which marks the record as torn even when
// intact records follow it.
func TestReopen_CutsRecordWithUnwrittenSector(t *testing.T) {
	t.Parallel()

	// Records of 1012 bytes start at offsets 16, 1028 and 2040.
	data := bytes.Repeat([]byte("x"), 1000)
	second := int64(segmentHeaderSize + recordHeaderSize + len(data))

	cases := map[string]struct {
		from, to int64
	}{
		"data sector":   {from: 1536, to: second + recordHeaderSize + int64(len(data))},
		"header sector": {from: second, to: 1536},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := writeSegment(t, dir, 1, data, data, data)
			zeroRange(t, path, tc.from, tc.to)

			l := openLog(t, dir, Config{})
			require.Equal(t, uint64(1), l.LastIndex())

			appendN(t, l, 1)
			requireRecords(t, readAll(t, l)[1:], 2, 1)
		})
	}
}

func TestReopen_EveryCutKeepsAPrefix(t *testing.T) {
	t.Parallel()

	const n = 5

	dir := t.TempDir()
	payloads := make([][]byte, n)

	for i := range payloads {
		payloads[i] = payload(uint64(i + 1))
	}

	full, err := os.ReadFile(writeSegment(t, dir, 1, payloads...))
	require.NoError(t, err)

	for cut := segmentHeaderSize; cut <= len(full); cut++ {
		cutDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(cutDir, segmentName(1)), full[:cut], 0o644))

		l, err := Open(cutDir, Config{})
		require.NoError(t, err, "cut at %d", cut)

		kept := (cut - segmentHeaderSize) / testRecordSize
		require.Equal(t, uint64(kept), l.LastIndex(), "cut at %d", cut)
		requireRecords(t, readAll(t, l), 1, kept)
		require.NoError(t, l.Close())
	}
}

func TestReopen_EveryFlippedByteInActiveSegmentIsCorrupt(t *testing.T) {
	t.Parallel()

	const n = 5

	dir := t.TempDir()
	payloads := make([][]byte, n)

	for i := range payloads {
		payloads[i] = payload(uint64(i + 1))
	}

	full, err := os.ReadFile(writeSegment(t, dir, 1, payloads...))
	require.NoError(t, err)

	for offset := 0; offset < len(full); offset++ {
		flipDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(flipDir, segmentName(1)), flipped(full, offset), 0o644))

		_, err := Open(flipDir, Config{})
		require.ErrorIs(t, err, ErrCorrupt, "flip at %d", offset)
	}
}

func TestReopen_DetectsCorruption(t *testing.T) {
	t.Parallel()

	// The log holds segments 1, 5 and 9 with records 1 to 10 and checkpoint 2.
	path := func(dir string, first uint64) string {
		return filepath.Join(dir, segmentName(first))
	}

	cases := map[string]struct {
		damage func(t *testing.T, dir string)
		reason string
	}{
		"flipped record in closed segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				flipByte(t, path(dir, 1), segmentHeaderSize+recordHeaderSize+2)
			},
			reason: "data checksum mismatch",
		},
		"flipped record in active segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				flipByte(t, path(dir, 9), segmentHeaderSize+recordHeaderSize+2)
			},
			reason: "data checksum mismatch",
		},
		"bad magic in closed segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				flipByte(t, path(dir, 1), 0)
			},
			reason: "bad header",
		},
		"bad magic in active segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				flipByte(t, path(dir, 9), 1)
			},
			reason: "bad header",
		},
		"segment under another name": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				data, err := os.ReadFile(path(dir, 5))
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path(dir, 9), data, 0o644))
			},
			reason: "bad header",
		},
		"active segment shorter than header": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Truncate(path(dir, 9), segmentHeaderSize-1))
			},
			reason: "shorter than a segment header",
		},
		"bytes after closed segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				appendBytes(t, path(dir, 1), []byte("garbage"))
			},
			reason: "runs past the end",
		},
		"zeros after closed segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				appendBytes(t, path(dir, 1), make([]byte, 100))
			},
			reason: "header checksum mismatch",
		},
		"truncated closed segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Truncate(path(dir, 1), segmentOf(4)-3))
			},
			reason: "runs past the end",
		},
		"missing middle segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Remove(path(dir, 5)))
			},
			reason: "ends at index 4 but the next segment starts at 9",
		},
		"corrupt checkpoint": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				flipByte(t, filepath.Join(dir, checkpointName), 0)
			},
			reason: "checksum mismatch",
		},
		"checkpoint below first segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Remove(path(dir, 1)))
			},
			reason: "checkpoint 2 is below the first segment 5",
		},
		"checkpoint past last record": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, writeCheckpoint(dir, 12))
			},
			reason: "checkpoint 12 is past the last record 10",
		},
		"no checkpoint after reclaim": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Remove(filepath.Join(dir, checkpointName)))
				require.NoError(t, os.Remove(path(dir, 1)))
			},
			reason: "no checkpoint but the first segment starts at 5",
		},
		"checkpoint without segments": {
			damage: func(t *testing.T, dir string) {
				t.Helper()

				for _, first := range []uint64{1, 5, 9} {
					require.NoError(t, os.Remove(path(dir, first)))
				}
			},
			reason: "checkpoint 2 but no segments",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			cfg := Config{SegmentSize: segmentOf(4)}
			l := openLog(t, dir, cfg)
			appendN(t, l, 10)
			require.NoError(t, l.Commit(2))
			require.NoError(t, l.Close())

			tc.damage(t, dir)

			_, err := Open(dir, cfg)
			require.ErrorIs(t, err, ErrCorrupt)
			require.ErrorContains(t, err, tc.reason)
		})
	}
}

func TestReopen_FailureDeletesNothing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4)}
	l := openLog(t, dir, cfg)
	appendN(t, l, 10)
	require.NoError(t, l.Close())

	// This checkpoint covers every segment but claims records that never existed.
	require.NoError(t, writeCheckpoint(dir, 50))

	_, err := Open(dir, cfg)
	require.ErrorIs(t, err, ErrCorrupt)
	require.Equal(t, []string{segmentName(1), segmentName(5), segmentName(9)}, segmentFiles(t, dir))
}

// A crash between writing the checkpoint and deleting segments leaves any subset
// of the committed segments, since deletions may reach the disk out of order.
func TestReopen_DeletesSegmentsLeftBelowCheckpoint(t *testing.T) {
	t.Parallel()

	cases := map[string][]uint64{
		"all left":    nil,
		"middle gone": {5},
		"first gone":  {1},
	}

	for name, removed := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			cfg := Config{SegmentSize: segmentOf(4)}
			l := openLog(t, dir, cfg)
			appendN(t, l, 10)
			require.NoError(t, l.Close())

			require.NoError(t, writeCheckpoint(dir, 9))

			for _, first := range removed {
				require.NoError(t, os.Remove(filepath.Join(dir, segmentName(first))))
			}

			l = openLog(t, dir, cfg)
			require.Equal(t, uint64(9), l.Committed())
			require.Equal(t, uint64(9), l.FirstIndex())
			require.Equal(t, []string{segmentName(9)}, segmentFiles(t, dir))
			requireRecords(t, readAll(t, l), 9, 2)
		})
	}
}

// Rolling away a fully committed active segment creates the new segment before
// deleting the old one; a crash in between leaves both.
func TestReopen_DeletesCommittedSegmentBeforeEmptyActive(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeSegment(t, dir, 1, payload(1), payload(2), payload(3))
	writeSegment(t, dir, 4)
	require.NoError(t, writeCheckpoint(dir, 4))

	l := openLog(t, dir, Config{})
	require.Equal(t, uint64(4), l.FirstIndex())
	require.Equal(t, uint64(3), l.LastIndex())
	require.Equal(t, []string{segmentName(4)}, segmentFiles(t, dir))

	appendN(t, l, 1)
	requireRecords(t, readAll(t, l), 4, 1)
}

func TestReopen_RemovesOnlyOwnTempFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Config{})
	appendN(t, l, 1)
	require.NoError(t, l.Close())

	own := []string{segmentName(9) + tempExt, checkpointName + tempExt}
	foreign := []string{"notes.tmp", "9.wal.tmp"}

	for _, name := range append(own, foreign...) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("partial"), 0o644))
	}

	l = openLog(t, dir, Config{})
	requireRecords(t, readAll(t, l), 1, 1)

	for _, name := range own {
		require.NoFileExists(t, filepath.Join(dir, name))
	}

	for _, name := range foreign {
		require.FileExists(t, filepath.Join(dir, name))
	}
}

// writeSegment writes a segment file holding payloads and returns its path.
func writeSegment(t *testing.T, dir string, first uint64, payloads ...[]byte) string {
	t.Helper()

	header := segmentHeader(first)
	buf := header[:]

	for _, data := range payloads {
		buf = appendRecord(buf, data)
	}

	path := filepath.Join(dir, segmentName(first))
	require.NoError(t, os.WriteFile(path, buf, 0o644))

	return path
}

func zeroRange(t *testing.T, path string, from, to int64) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)

	_, err = file.WriteAt(make([]byte, to-from), from)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}
