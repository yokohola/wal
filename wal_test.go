package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// payloadSize keeps every test record 8+12 bytes on disk, so segment
// boundaries in tests are predictable.
const (
	payloadSize    = 12
	testRecordSize = recordHeaderSize + payloadSize
)

func payload(index uint64) []byte {
	return fmt.Appendf(nil, "record-%05d", index)
}

func openLog(t *testing.T, dir string, opts Options) *Log {
	t.Helper()

	l, err := Open(dir, opts)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := l.Close(); err != nil && !errors.Is(err, ErrClosed) {
			t.Errorf("close: %v", err)
		}
	})

	return l
}

// appendN appends n records one by one; each payload encodes its own index.
func appendN(t *testing.T, l *Log, n int) {
	t.Helper()

	for range n {
		want := l.LastIndex() + 1

		got, err := l.Append(payload(want))
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func readAll(t *testing.T, l *Log) []Record {
	t.Helper()

	var out []Record

	for from := l.FirstIndex(); from <= l.LastIndex(); {
		recs, err := l.Read(from, 7)
		require.NoError(t, err)
		require.NotEmpty(t, recs)

		out = append(out, recs...)
		from = recs[len(recs)-1].Index + 1
	}

	return out
}

func requireRecords(t *testing.T, recs []Record, first uint64, n int) {
	t.Helper()
	require.Len(t, recs, n)

	for i, rec := range recs {
		require.Equal(t, first+uint64(i), rec.Index)
		require.Equal(t, payload(rec.Index), rec.Data)
	}
}

func segmentFiles(t *testing.T, dir string) []string {
	t.Helper()

	names, err := filepath.Glob(filepath.Join(dir, "*"+segmentExt))
	require.NoError(t, err)

	for i, name := range names {
		names[i] = filepath.Base(name)
	}

	return names
}

func TestOpen_FreshDirectory(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "nested", "wal")
	l := openLog(t, dir, Options{})

	require.Equal(t, uint64(1), l.FirstIndex())
	require.Equal(t, uint64(0), l.LastIndex())
	require.Equal(t, uint64(1), l.Committed())
	require.Equal(t, int64(segmentHeaderSize), l.Size())
	require.Equal(t, []string{"00000000000000000001.wal"}, segmentFiles(t, dir))
}

func TestOpen_RejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	cases := map[string]Options{
		"negative segment size": {SegmentSize: -1},
		"negative max size":     {MaxSize: -1},
		"max size below two segments": {
			SegmentSize: 1024,
			MaxSize:     2047,
		},
		"negative sync interval": {SyncInterval: -time.Second},
	}

	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := Open(t.TempDir(), opts)
			require.ErrorIs(t, err, ErrInvalidOptions)
		})
	}
}

func TestOpen_LocksDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Options{})

	_, err := Open(dir, Options{})
	require.ErrorIs(t, err, ErrLocked)

	require.NoError(t, l.Close())

	again, err := Open(dir, Options{})
	require.NoError(t, err)
	require.NoError(t, again.Close())
}

func TestAppend_AssignsConsecutiveIndexes(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Options{})

	last, err := l.Append(payload(1))
	require.NoError(t, err)
	require.Equal(t, uint64(1), last)

	last, err = l.Append(payload(2), payload(3))
	require.NoError(t, err)
	require.Equal(t, uint64(3), last)

	last, err = l.Append()
	require.NoError(t, err)
	require.Equal(t, uint64(3), last)

	require.Equal(t, uint64(1), l.FirstIndex())
	require.Equal(t, uint64(3), l.LastIndex())
	require.Equal(t, int64(segmentHeaderSize+3*testRecordSize), l.Size())
}

func TestAppend_CopiesInput(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Options{})

	data := []byte("original")
	_, err := l.Append(data)
	require.NoError(t, err)

	copy(data, "mutated!")

	recs, err := l.Read(1, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("original"), recs[0].Data)
}

func TestAppend_AllowsEmptyRecords(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Options{})

	_, err := l.Append(nil, []byte{}, []byte("x"))
	require.NoError(t, err)

	recs, err := l.Read(1, 3)
	require.NoError(t, err)
	require.Len(t, recs, 3)
	require.Empty(t, recs[0].Data)
	require.Empty(t, recs[1].Data)
	require.Equal(t, []byte("x"), recs[2].Data)
}

func TestAppend_RollsSegments(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Options{SegmentSize: segmentHeaderSize + 4*testRecordSize})

	appendN(t, l, 10)

	require.Equal(t, []string{
		"00000000000000000001.wal",
		"00000000000000000005.wal",
		"00000000000000000009.wal",
	}, segmentFiles(t, dir))

	info, err := os.Stat(filepath.Join(dir, "00000000000000000001.wal"))
	require.NoError(t, err)
	require.Equal(t, int64(segmentHeaderSize+4*testRecordSize), info.Size())

	requireRecords(t, readAll(t, l), 1, 10)
}

func TestAppend_LargeRecordGetsOwnSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Options{SegmentSize: 64})

	big := make([]byte, 200)
	for i := range big {
		big[i] = byte(i)
	}

	_, err := l.Append(big)
	require.NoError(t, err)

	_, err = l.Append([]byte("small"))
	require.NoError(t, err)

	require.Len(t, segmentFiles(t, dir), 2)

	recs, err := l.Read(1, 2)
	require.NoError(t, err)
	require.Equal(t, big, recs[0].Data)
	require.Equal(t, []byte("small"), recs[1].Data)
}

func TestRead_Ranges(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Options{SegmentSize: segmentHeaderSize + 3*testRecordSize})
	appendN(t, l, 10)

	cases := []struct {
		name  string
		from  uint64
		limit int
		want  int
		err   error
	}{
		{name: "first records", from: 1, limit: 2, want: 2},
		{name: "across segments", from: 3, limit: 4, want: 4},
		{name: "limit past end", from: 8, limit: 100, want: 3},
		{name: "everything", from: 1, limit: 10, want: 10},
		{name: "at end is empty", from: 11, limit: 5, want: 0},
		{name: "zero limit", from: 1, limit: 0, want: 0},
		{name: "negative limit", from: 1, limit: -1, want: 0},
		{name: "below first", from: 0, limit: 1, err: ErrOutOfRange},
		{name: "past end", from: 12, limit: 1, err: ErrOutOfRange},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recs, err := l.Read(tc.from, tc.limit)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, recs)
			requireRecords(t, recs, tc.from, tc.want)
		})
	}
}

func TestRead_ReturnsCallerOwnedData(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Options{})
	appendN(t, l, 2)

	recs, err := l.Read(1, 2)
	require.NoError(t, err)

	copy(recs[0].Data, "xxxxxxxxxxxx")
	copy(recs[1].Data, "yyyyyyyyyyyy")

	requireRecords(t, readAll(t, l), 1, 2)
}

func TestCommit_PersistsAndReclaims(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	opts := Options{SegmentSize: segmentHeaderSize + 4*testRecordSize}
	l := openLog(t, dir, opts)
	appendN(t, l, 10)

	require.NoError(t, l.Commit(6))
	require.Equal(t, uint64(6), l.Committed())
	require.Equal(t, uint64(5), l.FirstIndex())
	require.Equal(t, []string{"00000000000000000005.wal", "00000000000000000009.wal"}, segmentFiles(t, dir))
	require.Equal(t, int64(2*segmentHeaderSize+6*testRecordSize), l.Size())

	require.NoError(t, l.Commit(3), "lower commit is a no-op")
	require.Equal(t, uint64(6), l.Committed())

	require.ErrorIs(t, l.Commit(12), ErrOutOfRange)

	require.NoError(t, l.Commit(11))
	require.Equal(t, []string{"00000000000000000009.wal"}, segmentFiles(t, dir))
	require.Equal(t, uint64(9), l.FirstIndex())
	require.Equal(t, uint64(10), l.LastIndex())

	require.NoError(t, l.Close())

	l = openLog(t, dir, opts)
	require.Equal(t, uint64(11), l.Committed())
	require.Equal(t, uint64(9), l.FirstIndex())
	require.Equal(t, uint64(10), l.LastIndex())
}

func TestCommit_KeepsActiveSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Options{})
	appendN(t, l, 3)

	require.NoError(t, l.Commit(4))
	require.Equal(t, []string{"00000000000000000001.wal"}, segmentFiles(t, dir))
	require.Equal(t, uint64(1), l.FirstIndex())
	require.Equal(t, uint64(3), l.LastIndex())
	require.Equal(t, uint64(4), l.Committed())
}

func TestReopen_RestoresState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	opts := Options{SegmentSize: segmentHeaderSize + 4*testRecordSize}

	l := openLog(t, dir, opts)
	appendN(t, l, 10)
	require.NoError(t, l.Commit(3))
	size := l.Size()
	require.NoError(t, l.Close())

	l = openLog(t, dir, opts)
	require.Equal(t, uint64(1), l.FirstIndex())
	require.Equal(t, uint64(10), l.LastIndex())
	require.Equal(t, uint64(3), l.Committed())
	require.Equal(t, size, l.Size())
	requireRecords(t, readAll(t, l), 1, 10)

	appendN(t, l, 2)
	requireRecords(t, readAll(t, l), 1, 12)
}

func TestReopen_TrimsTornTail(t *testing.T) {
	t.Parallel()

	valid := frame(t, payload(4))

	cases := map[string][]byte{
		"short header": valid[:recordHeaderSize-3],
		"short body":   valid[:len(valid)-1],
		"bad crc":      append(valid[:4:4], append([]byte{0xff, 0xff, 0xff, 0xff}, valid[8:]...)...),
		"zero fill":    make([]byte, 64),
	}

	for name, tail := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			l := openLog(t, dir, Options{})
			appendN(t, l, 3)
			require.NoError(t, l.Close())

			path := filepath.Join(dir, "00000000000000000001.wal")
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			require.NoError(t, err)
			_, err = f.Write(tail)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			l = openLog(t, dir, Options{})
			require.Equal(t, uint64(3), l.LastIndex())
			require.Equal(t, int64(segmentHeaderSize+3*testRecordSize), l.Size())

			appendN(t, l, 2)
			requireRecords(t, readAll(t, l), 1, 5)
			require.NoError(t, l.Close())

			l = openLog(t, dir, Options{})
			requireRecords(t, readAll(t, l), 1, 5)
		})
	}
}

func TestReopen_DetectsCorruption(t *testing.T) {
	t.Parallel()

	first := "00000000000000000001.wal"
	second := "00000000000000000005.wal"

	cases := map[string]func(t *testing.T, dir string){
		"flipped byte in closed segment": func(t *testing.T, dir string) {
			t.Helper()
			flipByte(t, filepath.Join(dir, first), segmentHeaderSize+recordHeaderSize+2)
		},
		"bad magic in closed segment": func(t *testing.T, dir string) {
			t.Helper()
			flipByte(t, filepath.Join(dir, first), 0)
		},
		"bad magic in active segment": func(t *testing.T, dir string) {
			t.Helper()
			flipByte(t, filepath.Join(dir, second), 1)
		},
		"garbage after closed segment": func(t *testing.T, dir string) {
			t.Helper()
			appendBytes(t, filepath.Join(dir, first), []byte("garbage"))
		},
		"missing segment": func(t *testing.T, dir string) {
			t.Helper()
			require.NoError(t, os.Remove(filepath.Join(dir, second)))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "00000000000000000006.wal"), nil, 0o644))
		},
		"corrupt checkpoint": func(t *testing.T, dir string) {
			t.Helper()
			flipByte(t, filepath.Join(dir, checkpointFile), 0)
		},
	}

	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			opts := Options{SegmentSize: segmentHeaderSize + 4*testRecordSize}
			l := openLog(t, dir, opts)
			appendN(t, l, 6)
			require.NoError(t, l.Commit(2))
			require.NoError(t, l.Close())

			corrupt(t, dir)

			_, err := Open(dir, opts)
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func TestReopen_ToleratesZeroTailOnClosedSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	opts := Options{SegmentSize: segmentHeaderSize + 4*testRecordSize}
	l := openLog(t, dir, opts)
	appendN(t, l, 6)
	require.NoError(t, l.Close())

	appendBytes(t, filepath.Join(dir, "00000000000000000001.wal"), make([]byte, 100))

	l = openLog(t, dir, opts)
	requireRecords(t, readAll(t, l), 1, 6)
	require.Equal(t, int64(2*segmentHeaderSize+6*testRecordSize), l.Size())
}

func TestReopen_CheckpointAheadOfData(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Options{})
	appendN(t, l, 10)
	require.NoError(t, l.Commit(11))
	require.NoError(t, l.Close())

	// Lose records 8..10 as an unsynced tail would be lost in a power cut.
	path := filepath.Join(dir, "00000000000000000001.wal")
	require.NoError(t, os.Truncate(path, segmentHeaderSize+7*testRecordSize))

	l = openLog(t, dir, Options{})
	require.Equal(t, uint64(11), l.Committed())
	require.Equal(t, uint64(11), l.FirstIndex())
	require.Equal(t, uint64(10), l.LastIndex())
	require.Equal(t, []string{"00000000000000000011.wal"}, segmentFiles(t, dir))

	last, err := l.Append(payload(11))
	require.NoError(t, err)
	require.Equal(t, uint64(11), last)
	requireRecords(t, readAll(t, l), 11, 1)
}

func TestReopen_MissingCheckpointStartsAtFirstIndex(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Options{})
	appendN(t, l, 5)
	require.NoError(t, l.Commit(4))
	require.NoError(t, l.Close())

	require.NoError(t, os.Remove(filepath.Join(dir, checkpointFile)))

	l = openLog(t, dir, Options{})
	require.Equal(t, uint64(1), l.Committed())
}

func TestReopen_ReclaimsAfterCrashBetweenCheckpointAndUnlink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	opts := Options{SegmentSize: segmentHeaderSize + 4*testRecordSize}
	l := openLog(t, dir, opts)
	appendN(t, l, 10)
	require.NoError(t, l.Close())

	require.NoError(t, writeCheckpoint(dir, 9))

	l = openLog(t, dir, opts)
	require.Equal(t, uint64(9), l.Committed())
	require.Equal(t, uint64(9), l.FirstIndex())
	require.Equal(t, []string{"00000000000000000009.wal"}, segmentFiles(t, dir))
}

func TestReopen_RemovesStaleTempFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Options{})
	require.NoError(t, l.Close())

	stale := filepath.Join(dir, "00000000000000000009.wal.tmp")
	require.NoError(t, os.WriteFile(stale, []byte("partial"), 0o644))

	l = openLog(t, dir, Options{})
	require.NoFileExists(t, stale)
	require.Equal(t, uint64(0), l.LastIndex())
}

func TestMaxSize_AppliesBackpressure(t *testing.T) {
	t.Parallel()

	segment := int64(segmentHeaderSize + 4*testRecordSize)
	l := openLog(t, t.TempDir(), Options{SegmentSize: segment, MaxSize: 2 * segment})

	appendN(t, l, 7)

	_, err := l.Append(payload(8), payload(9))
	require.ErrorIs(t, err, ErrFull)
	require.Equal(t, uint64(7), l.LastIndex(), "nothing is written on ErrFull")

	require.NoError(t, l.Commit(5))
	appendN(t, l, 2)
	requireRecords(t, readAll(t, l), 5, 5)
}

func TestSync_OnAppendAndOnDemand(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Options{SyncOnAppend: true})
	appendN(t, l, 3)
	require.NoError(t, l.Sync())
	requireRecords(t, readAll(t, l), 1, 3)
}

func TestSync_BackgroundStopsOnClose(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Options{SyncInterval: time.Millisecond})
	appendN(t, l, 3)
	time.Sleep(5 * time.Millisecond)

	require.NoError(t, l.Close())

	select {
	case <-l.syncDone:
	case <-time.After(time.Second):
		t.Fatal("background sync did not stop")
	}
}

func TestClose_RejectsFurtherUse(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Options{})
	appendN(t, l, 2)
	require.NoError(t, l.Close())

	_, err := l.Append(payload(3))
	require.ErrorIs(t, err, ErrClosed)

	_, err = l.Read(1, 1)
	require.ErrorIs(t, err, ErrClosed)

	require.ErrorIs(t, l.Commit(2), ErrClosed)
	require.ErrorIs(t, l.Sync(), ErrClosed)
	require.ErrorIs(t, l.Close(), ErrClosed)

	require.Equal(t, uint64(1), l.FirstIndex())
	require.Equal(t, uint64(2), l.LastIndex())
	require.Equal(t, uint64(1), l.Committed())
}

func TestFailure_IsSticky(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Options{SegmentSize: segmentHeaderSize + 2*testRecordSize})
	appendN(t, l, 3)

	// Break the active segment's descriptor so the next fsync fails.
	require.NoError(t, l.active().file.Close())

	first := l.Sync()
	require.Error(t, first)

	_, err := l.Append(payload(4))
	require.ErrorIs(t, err, first)
	require.ErrorIs(t, l.Commit(2), first)
	require.ErrorIs(t, l.Sync(), first)

	recs, err := l.Read(1, 2)
	require.NoError(t, err, "reads of intact segments keep working")
	requireRecords(t, recs, 1, 2)

	require.ErrorIs(t, l.Close(), first)
}

func TestConcurrent_AppendReadCommit(t *testing.T) {
	t.Parallel()

	const (
		writers = 8
		perW    = 300
		total   = writers * perW
	)

	l := openLog(t, t.TempDir(), Options{SegmentSize: 4096})

	var wg sync.WaitGroup

	for w := range writers {
		wg.Go(func() {
			for i := range perW {
				if _, err := l.Append(fmt.Appendf(nil, "%d:%d", w, i)); err != nil {
					t.Error(err)

					return
				}
			}
		})
	}

	wg.Go(func() {
		for range 1000 {
			_ = l.FirstIndex()
			_ = l.LastIndex()
			_ = l.Size()
		}
	})

	seen := make(map[int]int, writers)
	from := l.Committed()
	committed := from

	for count := 0; count < total; {
		recs, err := l.Read(from, 64)
		require.NoError(t, err)

		if len(recs) == 0 {
			time.Sleep(100 * time.Microsecond)

			continue
		}

		for _, rec := range recs {
			var w, i int

			_, err := fmt.Sscanf(string(rec.Data), "%d:%d", &w, &i)
			require.NoError(t, err)
			require.Equal(t, seen[w], i, "writer %d out of order", w)

			seen[w]++
		}

		count += len(recs)
		from = recs[len(recs)-1].Index + 1

		if from-committed >= 256 {
			require.NoError(t, l.Commit(from))

			committed = from
		}
	}

	wg.Wait()

	require.NoError(t, l.Commit(from))
	require.Equal(t, uint64(total+1), l.Committed())
	require.Equal(t, uint64(total), l.LastIndex())
	require.Less(t, l.Size(), int64(8192), "committed segments were reclaimed")
}

func flipByte(t *testing.T, path string, offset int64) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)

	b := make([]byte, 1)
	_, err = f.ReadAt(b, offset)
	require.NoError(t, err)

	b[0] ^= 0xff
	_, err = f.WriteAt(b, offset)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func appendBytes(t *testing.T, path string, b []byte) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)

	_, err = f.Write(b)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func BenchmarkAppend(b *testing.B) {
	for _, batch := range []int{1, 100} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			l := openBench(b)

			data := make([][]byte, batch)
			for i := range data {
				data[i] = make([]byte, 160)
			}

			b.SetBytes(int64(batch * 160))
			b.ReportAllocs()

			for b.Loop() {
				if _, err := l.Append(data...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func openBench(b *testing.B) *Log {
	b.Helper()

	l, err := Open(b.TempDir(), Options{})
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		if err := l.Close(); err != nil {
			b.Error(err)
		}
	})

	return l
}

func BenchmarkRead(b *testing.B) {
	l := openBench(b)

	const n = 100_000

	data := make([][]byte, 1000)
	for i := range data {
		data[i] = make([]byte, 160)
	}

	for range n / len(data) {
		if _, err := l.Append(data...); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()

	from := uint64(1)

	for b.Loop() {
		recs, err := l.Read(from, 1024)
		if err != nil {
			b.Fatal(err)
		}

		from = recs[len(recs)-1].Index + 1
		if from > n-1024 {
			from = 1
		}
	}
}
