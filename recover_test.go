package wal

import (
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReopen_RestoresState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}

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

	valid := appendRecord(nil, 4, payload(4))

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

	// Damage in data the checkpoint trusts surfaces on Read instead.
	cases := map[string]struct {
		damage func(t *testing.T, dir string)
		reason string
	}{
		"bad magic in active segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				flipByte(t, path(dir, 9), 1)
			},
			reason: "bad segment header",
		},
		"segment under another name": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				data, err := os.ReadFile(path(dir, 5))
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path(dir, 9), data, 0o644))
			},
			reason: "bad segment header",
		},
		"active segment shorter than header": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Truncate(path(dir, 9), segmentHeaderSize-1))
			},
			reason: "shorter than a segment header",
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
			reason: "checkpoint 2 but the first segment starts at 5",
		},
		"checkpoint one short of first segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Remove(path(dir, 1)))
				require.NoError(t, writeCheckpoint(dir, checkpoint{committed: 3}))
			},
			reason: "checkpoint 3 but the first segment starts at 5",
		},
		"active segment shorter than its durable end": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Truncate(path(dir, 9), segmentOf(1)))
			},
			reason: "does not fit",
		},
		"checkpoint names missing segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, writeCheckpoint(dir, checkpoint{committed: 2, segment: 7}))
			},
			reason: "checkpoint names segment 7, which is missing",
		},
		"checkpoint not past committed": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, writeCheckpoint(dir, checkpoint{committed: 10, segment: 9, end: segmentOf(2), next: 10}))
			},
			reason: "does not fit",
		},
		"checkpoint end without records": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, writeCheckpoint(dir, checkpoint{committed: 2, segment: 9, end: segmentHeaderSize, next: 11}))
			},
			reason: "does not fit",
		},
		"checkpoint claims more records than fit": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, writeCheckpoint(dir, checkpoint{committed: 2, segment: 9, end: segmentOf(2), next: 15}))
			},
			reason: "does not fit",
		},
		"checkpoint past last record": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, writeCheckpoint(dir, checkpoint{committed: 11}))
			},
			reason: "checkpoint 11 is past the last record 10",
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
			cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
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
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 10)
	require.NoError(t, l.Close())

	// This checkpoint covers every segment but claims records that never existed.
	require.NoError(t, writeCheckpoint(dir, checkpoint{committed: 50}))

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
			cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
			l := openLog(t, dir, cfg)
			appendN(t, l, 10)
			require.NoError(t, l.Close())

			require.NoError(t, writeCheckpoint(dir, checkpoint{committed: 8}))

			for _, first := range removed {
				require.NoError(t, os.Remove(filepath.Join(dir, segmentName(first))))
			}

			l = openLog(t, dir, cfg)
			require.Equal(t, uint64(8), l.Committed())
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
	require.NoError(t, writeCheckpoint(dir, checkpoint{committed: 3}))

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

// A crash after Commit leaves records the checkpoint does not cover. Open
// trusts what it covers and scans only the rest.
func TestReopen_ScansOnlyPastCheckpoint(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(8), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 3)
	require.NoError(t, l.Commit(2))
	appendN(t, l, 1)
	require.NoError(t, l.Sync())

	active := segmentName(1)
	first, fourth := int64(segmentHeaderSize+recordHeaderSize), segmentOf(3)+recordHeaderSize

	crashed := snapshot(t, dir)
	appendBytes(t, filepath.Join(crashed, active), appendRecord(nil, 5, payload(5))[:10])
	l = openLog(t, crashed, cfg)
	require.Equal(t, uint64(4), l.LastIndex(), "the torn tail is cut")
	requireRecords(t, readAll(t, l), 1, 4)

	trustedDamage := snapshot(t, dir)
	flipByte(t, filepath.Join(trustedDamage, active), first)
	l = openLog(t, trustedDamage, cfg)
	require.Equal(t, uint64(4), l.LastIndex(), "trusted records are not read by Open")

	_, err := l.Read(1, 1)
	require.ErrorIs(t, err, ErrCorrupt)

	scannedDamage := snapshot(t, dir)
	flipByte(t, filepath.Join(scannedDamage, active), fourth)
	_, err = Open(scannedDamage, cfg)
	require.ErrorIs(t, err, ErrCorrupt, "records past the checkpoint are scanned")
}

// When the log rolled after the last Commit, the new active segment is scanned
// from its start and the closed ones are trusted.
func TestReopen_ScansActiveFromStartAfterRoll(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 2)
	require.NoError(t, l.Commit(1))
	appendN(t, l, 4)
	require.NoError(t, l.Sync())
	require.Equal(t, []string{segmentName(1), segmentName(5)}, segmentFiles(t, dir))

	closedDamage := snapshot(t, dir)
	flipByte(t, filepath.Join(closedDamage, segmentName(1)), segmentHeaderSize+recordHeaderSize)
	l = openLog(t, closedDamage, cfg)
	require.Equal(t, uint64(6), l.LastIndex())

	activeDamage := snapshot(t, dir)
	flipByte(t, filepath.Join(activeDamage, segmentName(5)), segmentHeaderSize+recordHeaderSize)
	_, err := Open(activeDamage, cfg)
	require.ErrorIs(t, err, ErrCorrupt)

	l = openLog(t, snapshot(t, dir), cfg)
	requireRecords(t, readAll(t, l), 1, 6)
}

// Trusted records are verified and indexed by concurrent first reads while
// appends extend the same segment.
func TestRead_VerifiesTrustedRecordsOnFirstRead(t *testing.T) {
	t.Parallel()

	const n = 1200

	// Each segment spans several sparse index intervals.
	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(400), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, n)
	require.NoError(t, l.Close())

	l = openLog(t, dir, cfg)
	require.Greater(t, len(l.segments), 2)

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			for from := uint64(1); from <= n; from += 37 {
				recs, err := l.Read(from, 5)
				if err != nil {
					t.Error(err)

					return
				}

				for i, rec := range recs {
					if rec.Index != from+uint64(i) || !bytes.Equal(rec.Data, payload(rec.Index)) {
						t.Errorf("read %d: got index %d data %q", from, rec.Index, rec.Data)
					}
				}
			}
		})
	}

	wg.Go(func() {
		for range 50 {
			if _, err := l.Append(payload(l.LastIndex() + 1)); err != nil {
				t.Error(err)

				return
			}
		}
	})

	wg.Wait()

	for from := uint64(1); from <= l.LastIndex(); from++ {
		recs, err := l.Read(from, 1)
		require.NoError(t, err)
		requireRecords(t, recs, from, 1)
	}
}

// Verification runs without the log's lock, so others go on meanwhile.
func TestRead_VerificationDoesNotBlockOthers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 10)
	require.NoError(t, l.Close())

	l = openLog(t, dir, cfg)
	requireRecords(t, readAll(t, l)[8:], 9, 2)

	// Holding verifyMu stands in for a long verification of the first segment.
	first := l.segments[0]
	first.verifyMu.Lock()

	type result struct {
		recs []Record
		err  error
	}

	read := make(chan result, 1)
	go func() {
		recs, err := l.Read(1, 4)
		read <- result{recs: recs, err: err}
	}()

	appendN(t, l, 1)
	require.Equal(t, uint64(0), l.Committed())

	recs, err := l.Read(5, 2)
	require.NoError(t, err, "the second segment is verified independently")
	requireRecords(t, recs, 5, 2)

	first.verifyMu.Unlock()

	got := <-read
	require.NoError(t, got.err)
	requireRecords(t, got.recs, 1, 4)
}

func TestRead_SegmentReclaimedDuringVerification(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 10)
	require.NoError(t, l.Close())

	l = openLog(t, dir, cfg)
	first := l.segments[0]
	first.verifyMu.Lock()

	read := make(chan error, 1)
	go func() {
		_, err := l.Read(1, 4)
		read <- err
	}()

	require.NoError(t, l.Commit(4))
	require.NoFileExists(t, first.path)

	first.verifyMu.Unlock()
	require.ErrorIs(t, <-read, ErrOutOfRange)
}

// Damage in trusted records fails only the records it makes unreadable. Records
// before it come back as a short result and records after it read normally.
func TestRead_ReportsLostTrustedRecords(t *testing.T) {
	t.Parallel()

	// The log holds segments 1, 5 and 9 with records 1 to 10, all trusted.
	path := func(dir string, first uint64) string {
		return filepath.Join(dir, segmentName(first))
	}

	// recordAt is the offset of record index in its segment.
	recordAt := func(index, first uint64) int64 {
		return segmentOf(int(index - first))
	}

	cases := map[string]struct {
		damage      func(t *testing.T, dir string)
		segment     uint64
		offset      int64
		first, last uint64
		reason      string
	}{
		"bad data in closed segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				flipByte(t, path(dir, 1), recordAt(2, 1)+recordHeaderSize+2)
			},
			segment: 1, offset: recordAt(2, 1), first: 2, last: 2,
			reason: "data checksum mismatch",
		},
		"bad data in active segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				flipByte(t, path(dir, 9), recordAt(9, 9)+recordHeaderSize)
			},
			segment: 9, offset: recordAt(9, 9), first: 9, last: 9,
			reason: "data checksum mismatch",
		},
		"bad record header in closed segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				flipByte(t, path(dir, 5), recordAt(6, 5))
			},
			segment: 5, offset: recordAt(6, 5), first: 6, last: 8,
			reason: "header checksum mismatch",
		},
		"bad record header in active segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				flipByte(t, path(dir, 9), recordAt(9, 9)+recordHeaderSize-1)
			},
			segment: 9, offset: recordAt(9, 9), first: 9, last: 10,
			reason: "header checksum mismatch",
		},
		"bad magic in closed segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				flipByte(t, path(dir, 5), 0)
			},
			segment: 5, offset: segmentHeaderSize, first: 5, last: 8,
			reason: "bad segment header",
		},
		"closed segment shorter than header": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Truncate(path(dir, 5), segmentHeaderSize-1))
			},
			segment: 5, offset: segmentHeaderSize, first: 5, last: 8,
			reason: "shorter than a segment header",
		},
		"closed segment cut inside a record": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Truncate(path(dir, 1), segmentOf(4)-3))
			},
			segment: 1, offset: recordAt(4, 1), first: 4, last: 4,
			reason: "runs past the end",
		},
		"closed segment cut between records": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Truncate(path(dir, 1), segmentOf(2)))
			},
			segment: 1, offset: recordAt(3, 1), first: 3, last: 4,
			reason: "segment ends before its last record",
		},
		"missing middle segment": {
			damage: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Remove(path(dir, 5)))
			},
			segment: 1, offset: segmentOf(4), first: 5, last: 8,
			reason: "segment ends before its last record",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
			l := openLog(t, dir, cfg)
			appendN(t, l, 10)
			require.NoError(t, l.Close())

			tc.damage(t, dir)

			l = openLog(t, dir, cfg)

			recs, err := l.Read(1, 100)
			if tc.first == 1 {
				require.ErrorIs(t, err, ErrCorrupt)
			} else {
				require.NoError(t, err)
				requireRecords(t, recs, 1, int(tc.first-1))
			}

			for from := tc.first; from <= tc.last; from++ {
				_, err := l.Read(from, 100)
				require.ErrorIs(t, err, ErrCorrupt)
				require.ErrorContains(t, err, tc.reason)

				var lost *CorruptError
				require.ErrorAs(t, err, &lost)
				require.Equal(t, CorruptError{
					Path:   path(dir, tc.segment),
					Offset: tc.offset,
					First:  tc.first,
					Last:   tc.last,
					Err:    lost.Err,
				}, *lost)
			}

			recs, err = l.Read(tc.last+1, 100)
			require.NoError(t, err)
			requireRecords(t, recs, tc.last+1, int(10-tc.last))

			appendN(t, l, 1)
			recs, err = l.Read(tc.last+1, 100)
			require.NoError(t, err)
			requireRecords(t, recs, tc.last+1, int(11-tc.last))
		})
	}
}

// Flipping any byte of a trusted segment loses exactly the records it covers:
// one record for its data, the rest of the segment for a header. No Read ever
// returns a record under a wrong index. Open checks the active segment's header
// itself.
func TestRead_EveryFlippedTrustedByteLosesItsRange(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 10)
	require.NoError(t, l.Close())

	for _, first := range []uint64{5, 9} {
		last := min(first+3, 10)

		for offset := range segmentOf(int(last - first + 1)) {
			damaged := snapshot(t, dir)
			flipByte(t, filepath.Join(damaged, segmentName(first)), offset)

			active := first == 9
			if active && offset < segmentHeaderSize {
				_, err := Open(damaged, cfg)
				require.ErrorContains(t, err, "bad segment header")

				continue
			}

			l := openLog(t, damaged, cfg)
			appendN(t, l, 1)

			want := lostRange(first, last)
			if offset >= segmentHeaderSize {
				index := first + uint64((offset-segmentHeaderSize)/testRecordSize)
				want = lostRange(index, last)

				if (offset-segmentHeaderSize)%testRecordSize >= recordHeaderSize {
					want = lostRange(index, index)
				}
			}

			require.Equal(t, want, lostIndexes(t, l), "segment %d, flip at %d", first, offset)
			require.NoError(t, l.Close())
		}
	}
}

// Records this process wrote are checked on every Read. A bad header loses the
// records up to the next one the sparse index locates.
func TestRead_ReportsLostWrittenRecords(t *testing.T) {
	t.Parallel()

	const n = 600

	l := openLog(t, t.TempDir(), Config{})
	appendN(t, l, n)

	seg := l.segments[0]
	require.Greater(t, len(seg.sparseIndex), 3)
	entry := seg.sparseIndex[2]

	cases := map[string]struct {
		index       uint64
		offset      int64 // within the record
		first, last uint64
	}{
		"bad data":                     {index: 50, offset: recordHeaderSize + 1, first: 50, last: 50},
		"bad data at indexed record":   {index: entry.index, offset: recordHeaderSize, first: entry.index, last: entry.index},
		"bad header":                   {index: 50, offset: 0, first: 50, last: seg.sparseIndex[1].index - 1},
		"bad header at indexed record": {index: entry.index, offset: 4, first: entry.index, last: seg.sparseIndex[3].index - 1},
		"bad header before indexed record": {
			index: entry.index - 1, offset: 8, first: entry.index - 1, last: entry.index - 1,
		},
		"bad header of last record": {index: n, offset: 0, first: n, last: n},
	}

	for name, tc := range cases {
		offset := segmentOf(int(tc.index-1)) + tc.offset
		flipByte(t, seg.path, offset)

		want := lostRange(tc.first, tc.last)
		require.Equal(t, want, lostIndexes(t, l), name)

		_, err := l.Read(tc.first, 1)

		var lost *CorruptError
		require.ErrorAs(t, err, &lost, name)
		require.Equal(t, segmentOf(int(tc.first-1)), lost.Offset, name)
		require.Equal(t, seg.path, lost.Path, name)

		flipByte(t, seg.path, offset)
		require.Empty(t, lostIndexes(t, l), "%s: restored", name)
	}
}

// A record's checksum covers its index, so intact records moved to another
// position read as damage rather than as data at the wrong index.
func TestRead_ReportsSwappedRecords(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 10)
	require.NoError(t, l.Close())

	swapRecords(t, filepath.Join(dir, segmentName(1)), 2, 3)

	l = openLog(t, dir, cfg)
	require.Equal(t, []uint64{2, 3}, lostIndexes(t, l))
}

func TestReopen_RejectsSwappedRecordsPastCheckpoint(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Config{})
	appendN(t, l, 3)
	require.NoError(t, l.Sync())

	crashed := snapshot(t, dir)
	swapRecords(t, filepath.Join(crashed, segmentName(1)), 2, 3)

	_, err := Open(crashed, Config{})
	require.ErrorIs(t, err, ErrCorrupt)
	require.ErrorIs(t, err, errBadData)
}

// Verification can find fewer or more trusted records than the checkpoint
// claims. Reads keep the records past the trusted bytes apart from them, so
// neither shifts their indexes.
func TestRead_TrustedRecordsDoNotShiftLaterOnes(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		claimed int // trusted records the checkpoint claims; 4 are on disk
		lost    []uint64
	}{
		"fewer on disk": {claimed: 5, lost: []uint64{5}},
		"more on disk":  {claimed: 3},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			writeSegment(t, dir, 1, payload(1), payload(2), payload(3), payload(4))
			require.NoError(t, writeCheckpoint(dir, checkpoint{
				segment: 1, end: segmentOf(4), next: uint64(tc.claimed) + 1,
			}))

			l := openLog(t, dir, Config{})
			require.Equal(t, uint64(tc.claimed), l.LastIndex())

			// The first Read verifies the segment before anything follows the trusted
			// bytes; the appended records must still be found.
			require.Equal(t, tc.lost, lostIndexes(t, l))

			for range 200 {
				_, err := l.Append(payload(l.LastIndex() + 1))
				require.NoError(t, err)
			}

			require.Equal(t, tc.lost, lostIndexes(t, l))
		})
	}
}

// Verification remembers where it stopped locating records, even once the
// damage is gone. Bad data is found by each Read, so fixing it takes effect.
func TestRead_RemembersUnlocatedRecords(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 10)
	require.NoError(t, l.Close())

	path := filepath.Join(dir, segmentName(1))
	header := int64(segmentHeaderSize)
	data := segmentOf(2) + recordHeaderSize

	flipByte(t, path, header)
	flipByte(t, path, data)

	l = openLog(t, dir, cfg)
	require.Equal(t, []uint64{1, 2, 3, 4}, lostIndexes(t, l))

	flipByte(t, path, header)
	require.Equal(t, []uint64{1, 2, 3, 4}, lostIndexes(t, l), "unlocated records stay lost")

	require.NoError(t, l.Close())
	l = openLog(t, dir, cfg)
	require.Equal(t, []uint64{3}, lostIndexes(t, l), "a new verification locates them")

	flipByte(t, path, data)
	require.Empty(t, lostIndexes(t, l))
}

// Bytes past the last trusted record of a closed segment hold no record.
func TestRead_IgnoresBytesAfterTrustedRecords(t *testing.T) {
	t.Parallel()

	for name, extra := range map[string][]byte{
		"garbage": []byte("garbage"),
		"zeros":   make([]byte, 100),
		"record":  appendRecord(nil, 5, payload(5)),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
			l := openLog(t, dir, cfg)
			appendN(t, l, 10)
			require.NoError(t, l.Close())

			appendBytes(t, filepath.Join(dir, segmentName(1)), extra)

			l = openLog(t, dir, cfg)
			require.Empty(t, lostIndexes(t, l))
		})
	}
}

// lostIndexes reads from every retained index and returns those Read fails on.
// It checks that every record returned has its index and data, that a short
// result stops right before a lost record and that the range a failure reports
// holds the index read from.
func lostIndexes(t *testing.T, l *Log) []uint64 {
	t.Helper()

	var lost []uint64

	for from := l.FirstIndex(); from <= l.LastIndex(); from++ {
		recs, err := l.Read(from, math.MaxInt)

		var damage *CorruptError
		if errors.As(err, &damage) {
			require.LessOrEqual(t, damage.First, from, err)
			require.GreaterOrEqual(t, damage.Last, from, err)

			lost = append(lost, from)

			continue
		}

		require.NoError(t, err)
		require.NotEmpty(t, recs)

		for i, rec := range recs {
			require.Equal(t, from+uint64(i), rec.Index)
			require.Equal(t, payload(rec.Index), rec.Data)
		}

		if next := recs[len(recs)-1].Index + 1; next <= l.LastIndex() {
			_, err := l.Read(next, 1)
			require.ErrorIs(t, err, ErrCorrupt, "short result from %d", from)
		}
	}

	return lost
}

// lostRange returns the indexes from first to last.
func lostRange(first, last uint64) []uint64 {
	var out []uint64
	for i := first; i <= last; i++ {
		out = append(out, i)
	}

	return out
}

// snapshot copies the files of dir, as a crash would leave them, to a new
// directory.
func snapshot(t *testing.T, dir string) string {
	t.Helper()

	out := t.TempDir()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(out, entry.Name()), data, 0o644))
	}

	return out
}

// writeSegment writes a segment file holding payloads and returns its path.
func writeSegment(t *testing.T, dir string, first uint64, payloads ...[]byte) string {
	t.Helper()

	header := segmentHeader(first)
	buf := header[:]

	for i, data := range payloads {
		buf = appendRecord(buf, first+uint64(i), data)
	}

	path := filepath.Join(dir, segmentName(first))
	require.NoError(t, os.WriteFile(path, buf, 0o644))

	return path
}

// swapRecords swaps records a and b of the segment at path, which starts at
// index 1 and holds only test payloads.
func swapRecords(t *testing.T, path string, a, b uint64) {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	at := func(index uint64) []byte {
		offset := segmentOf(int(index - 1))

		return data[offset : offset+testRecordSize]
	}

	first := bytes.Clone(at(a))
	copy(at(a), at(b))
	copy(at(b), first)

	require.NoError(t, os.WriteFile(path, data, 0o644))
}

func zeroRange(t *testing.T, path string, from, to int64) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)

	_, err = file.WriteAt(make([]byte, to-from), from)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}
