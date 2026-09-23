package wal

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Every test payload is 12 bytes, so a test record takes a fixed 24 bytes on
// disk and segment boundaries are predictable.
const (
	payloadSize    = 12
	testRecordSize = recordHeaderSize + payloadSize
)

func TestOpen_FreshDirectory(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "nested", "wal")
	l := openLog(t, dir, Config{})

	require.Equal(t, uint64(1), l.FirstIndex())
	require.Equal(t, uint64(0), l.LastIndex())
	require.Equal(t, uint64(0), l.Committed())
	require.Equal(t, int64(segmentHeaderSize), l.Size())
	require.Equal(t, []string{segmentName(1)}, segmentFiles(t, dir))
}

func TestOpen_RejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	cases := map[string]Config{
		"negative segment size":           {SegmentSize: -1},
		"negative max wal size":           {MaxWALSize: -1},
		"negative max record size":        {MaxRecordSize: -1},
		"record size above format":        {MaxRecordSize: math.MaxUint32 + 1},
		"negative sync interval":          {SyncInterval: -time.Second},
		"max wal size below segment size": {SegmentSize: 1024, MaxRecordSize: 100, MaxWALSize: 1023},
		"max wal size below default":      {MaxWALSize: DefaultSegmentSize - 1},
		"record above segment":            {SegmentSize: 1024, MaxRecordSize: 1025},
		"default record above segment":    {SegmentSize: 1024},
	}

	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := Open(t.TempDir(), cfg)
			require.ErrorIs(t, err, ErrInvalidConfig)
		})
	}
}

func TestOpen_LocksDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Config{})

	_, err := Open(dir, Config{})
	require.ErrorIs(t, err, ErrLocked)

	require.NoError(t, l.Close())

	again, err := Open(dir, Config{})
	require.NoError(t, err)
	require.NoError(t, again.Close())
}

func TestAppend_AssignsConsecutiveIndexes(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{})

	last, err := l.Append(payload(1))
	require.NoError(t, err)
	require.Equal(t, uint64(1), last)

	last, err = l.Append(payload(2))
	require.NoError(t, err)
	require.Equal(t, uint64(2), last)

	last, err = l.Append(payload(3))
	require.NoError(t, err)
	require.Equal(t, uint64(3), last)

	require.Equal(t, uint64(1), l.FirstIndex())
	require.Equal(t, uint64(3), l.LastIndex())
	require.Equal(t, int64(segmentHeaderSize+3*testRecordSize), l.Size())
}

func TestAppend_CopiesInput(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{})

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

	dir := t.TempDir()
	l := openLog(t, dir, Config{})

	for _, data := range [][]byte{nil, {}, []byte("x")} {
		_, err := l.Append(data)
		require.NoError(t, err)
	}

	require.NoError(t, l.Close())

	l = openLog(t, dir, Config{})

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
	l := openLog(t, dir, Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize})

	appendN(t, l, 10)

	require.Equal(t, []string{segmentName(1), segmentName(5), segmentName(9)}, segmentFiles(t, dir))

	for _, first := range []uint64{1, 5} {
		info, err := os.Stat(filepath.Join(dir, segmentName(first)))
		require.NoError(t, err)
		require.Equal(t, segmentOf(4), info.Size(), "a closed segment has no preallocated tail")
	}

	requireRecords(t, readAll(t, l), 1, 10)
}

func TestAppend_RecordOfSegmentSizeGetsOwnSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: 1024, MaxRecordSize: 1024}
	l := openLog(t, dir, cfg)

	_, err := l.Append(make([]byte, cfg.MaxRecordSize))
	require.NoError(t, err)

	last, err := l.Append(make([]byte, cfg.MaxRecordSize))
	require.NoError(t, err)
	require.Equal(t, uint64(2), last)
	require.Equal(t, []string{segmentName(1), segmentName(2)}, segmentFiles(t, dir))
	require.NoError(t, l.Close())

	for _, first := range []uint64{1, 2} {
		info, err := os.Stat(filepath.Join(dir, segmentName(first)))
		require.NoError(t, err)
		require.Equal(t, segmentHeaderSize+recordHeaderSize+cfg.SegmentSize, info.Size(), "only the framing exceeds SegmentSize")
	}
}

func TestAppend_BoundsRecordSize(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Config{MaxRecordSize: 100})

	_, err := l.Append(make([]byte, 100))
	require.NoError(t, err)

	_, err = l.Append(make([]byte, 101))
	require.ErrorIs(t, err, ErrTooLarge)
	require.Equal(t, uint64(1), l.LastIndex(), "a rejected record is not written")
	require.Equal(t, segmentHeaderSize+recordHeaderSize+int64(100), l.Size())
	require.NoError(t, l.Close())

	l = openLog(t, dir, Config{MaxRecordSize: 10})

	recs, err := l.Read(1, 1)
	require.NoError(t, err)
	require.Len(t, recs[0].Data, 100, "a lower limit keeps existing records readable")
}

func TestAppend_DefaultMaxRecordSize(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{})

	_, err := l.Append(make([]byte, DefaultMaxRecordSize))
	require.NoError(t, err)

	_, err = l.Append(make([]byte, DefaultMaxRecordSize+1))
	require.ErrorIs(t, err, ErrTooLarge)
}

func TestRead_Ranges(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{SegmentSize: segmentOf(3), MaxRecordSize: payloadSize})
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
		{name: "huge limit", from: 1, limit: math.MaxInt, want: 10},
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

func TestRead_FromAnyIndex(t *testing.T) {
	t.Parallel()

	const n = 600

	dir := t.TempDir()
	cfg := Config{SegmentSize: 64 << 10, MaxRecordSize: 700}
	l := openLog(t, dir, cfg)

	want := make([][]byte, n+1)
	for i := 1; i <= n; i++ {
		want[i] = bytes.Repeat([]byte{byte(i)}, i*37%700)

		_, err := l.Append(want[i])
		require.NoError(t, err)
	}

	check := func(l *Log) {
		require.Greater(t, len(segmentFiles(t, dir)), 2)

		for from := uint64(1); from <= n; from++ {
			recs, err := l.Read(from, 3)
			require.NoError(t, err)
			require.Len(t, recs, int(min(3, n-from+1)))

			for i, rec := range recs {
				require.Equal(t, from+uint64(i), rec.Index)
				require.Equal(t, want[rec.Index], rec.Data)
			}
		}
	}

	check(l)
	require.NoError(t, l.Close())
	check(openLog(t, dir, cfg))
}

func TestRead_ReturnsCallerOwnedData(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{})
	appendN(t, l, 2)

	recs, err := l.Read(1, 2)
	require.NoError(t, err)

	copy(recs[0].Data, "xxxxxxxxxxxx")
	copy(recs[1].Data, "yyyyyyyyyyyy")

	requireRecords(t, readAll(t, l), 1, 2)
}

func TestRead_RecordsDoNotShareCapacity(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{})
	appendN(t, l, 2)

	recs, err := l.Read(1, 2)
	require.NoError(t, err)

	// Small enough to fit a capacity that leaked into the next record.
	_ = append(recs[0].Data, bytes.Repeat([]byte("X"), testRecordSize)...)

	require.Equal(t, payload(2), recs[1].Data)
}

func TestRead_DoesNotWaitForSync(t *testing.T) {
	var blocking atomic.Bool

	entered := make(chan struct{})
	release := make(chan struct{})

	setSyncData(t, func(file *os.File) error {
		if blocking.Load() {
			entered <- struct{}{}
			<-release
		}

		return file.Sync()
	})

	l := openLog(t, t.TempDir(), Config{SyncOnAppend: true})
	appendN(t, l, 2)

	// Registered after openLog, so it runs first and Close never waits on a
	// blocked fsync.
	var unblock sync.Once

	t.Cleanup(func() { unblock.Do(func() { close(release) }) })

	blocking.Store(true)

	appended := make(chan error, 1)
	go func() {
		_, err := l.Append(payload(3))
		appended <- err
	}()

	<-entered
	blocking.Store(false)

	type result struct {
		recs []Record
		err  error
	}

	read := make(chan result, 1)
	go func() {
		recs, err := l.Read(1, 10)
		read <- result{recs: recs, err: err}
	}()

	select {
	case got := <-read:
		require.NoError(t, got.err)
		requireRecords(t, got.recs, 1, 2)
	case <-time.After(5 * time.Second):
		t.Fatal("Read waited for an fsync")
	}

	unblock.Do(func() { close(release) })
	require.NoError(t, <-appended)
	require.Equal(t, uint64(3), l.LastIndex())
}

func TestCommit_PersistsAndReclaims(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)

	require.NoError(t, l.Commit(0), "nothing to commit")
	require.ErrorIs(t, l.Commit(1), ErrOutOfRange)

	appendN(t, l, 10)

	require.NoError(t, l.Commit(5))
	require.Equal(t, uint64(5), l.Committed())
	require.Equal(t, uint64(5), l.FirstIndex())
	require.Equal(t, []string{segmentName(5), segmentName(9)}, segmentFiles(t, dir))
	require.Equal(t, 2*segmentHeaderSize+6*int64(testRecordSize), l.Size())

	require.NoError(t, l.Commit(3), "a lower index is a no-op")
	require.Equal(t, uint64(5), l.Committed())

	require.ErrorIs(t, l.Commit(11), ErrOutOfRange)

	require.NoError(t, l.Commit(10))
	require.Equal(t, []string{segmentName(9)}, segmentFiles(t, dir), "the active segment stays")
	require.Equal(t, uint64(9), l.FirstIndex())
	require.Equal(t, uint64(10), l.LastIndex())

	require.NoError(t, l.Close())

	l = openLog(t, dir, cfg)
	require.Equal(t, uint64(10), l.Committed())
	require.Equal(t, uint64(9), l.FirstIndex())
	require.Equal(t, uint64(10), l.LastIndex())
}

func TestCommit_SyncsRecordsBeforeCheckpoint(t *testing.T) {
	dir := t.TempDir()

	// Each sync records the checkpoint on disk at that moment.
	var seen []uint64

	setSyncData(t, func(file *os.File) error {
		cp, _, err := readCheckpoint(dir)
		require.NoError(t, err)

		seen = append(seen, cp.committed)

		return file.Sync()
	})

	l := openLog(t, dir, Config{})

	appendN(t, l, 3)
	require.NoError(t, l.Commit(2))
	require.Equal(t, []uint64{0}, seen, "records were synced before the first checkpoint")

	require.NoError(t, l.Commit(3))
	require.Len(t, seen, 1, "records already durable are not synced again")

	appendN(t, l, 1)
	require.NoError(t, l.Commit(4))
	require.Equal(t, []uint64{0, 3}, seen)
}

func TestCommit_RetryFinishesReclaim(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize})
	appendN(t, l, 10)

	// A non-empty directory in place of the first segment makes its removal fail.
	blocked := filepath.Join(dir, segmentName(1))
	require.NoError(t, os.Remove(blocked))
	require.NoError(t, os.Mkdir(blocked, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(blocked, "file"), nil, 0o644))

	size := l.Size()

	require.Error(t, l.Commit(8))
	require.Equal(t, uint64(8), l.Committed(), "the checkpoint is durable")
	require.Equal(t, uint64(9), l.FirstIndex(), "reclaimed segments leave the log at once")
	require.Equal(t, size, l.Size(), "undeleted files still count")
	require.FileExists(t, filepath.Join(dir, segmentName(5)))
	requireRecords(t, readAll(t, l), 9, 2)

	require.NoError(t, os.Remove(filepath.Join(blocked, "file")))

	require.NoError(t, l.Commit(8))
	require.Equal(t, uint64(9), l.FirstIndex())
	require.Equal(t, []string{segmentName(9)}, segmentFiles(t, dir))
	require.Equal(t, segmentHeaderSize+2*int64(testRecordSize), l.Size())
}

func TestMaxSize_AppliesBackpressure(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize, MaxWALSize: 2 * segmentOf(4)})
	appendN(t, l, 8)

	_, err := l.Append(payload(9))
	require.ErrorIs(t, err, ErrFull)
	require.Equal(t, uint64(8), l.LastIndex(), "nothing is written on ErrFull")

	require.NoError(t, l.Commit(4))
	appendN(t, l, 2)
	requireRecords(t, readAll(t, l), 5, 6)
}

func TestMaxSize_RollsAwayCommittedActiveSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	l := openLog(t, dir, Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize, MaxWALSize: segmentOf(4)})
	appendN(t, l, 4)

	_, err := l.Append(payload(5))
	require.ErrorIs(t, err, ErrFull, "the full active segment leaves no room")

	require.NoError(t, l.Commit(2))
	_, err = l.Append(payload(5))
	require.ErrorIs(t, err, ErrFull, "uncommitted records keep the segment")

	require.NoError(t, l.Commit(4))
	appendN(t, l, 4)

	require.Equal(t, []string{segmentName(5)}, segmentFiles(t, dir))
	require.Equal(t, segmentOf(4), l.Size())
	requireRecords(t, readAll(t, l), 5, 4)
}

func TestSync_OnAppend(t *testing.T) {
	var calls atomic.Int64

	setSyncData(t, countingSync(&calls))

	l := openLog(t, t.TempDir(), Config{SyncOnAppend: true})

	appendN(t, l, 3)
	require.Equal(t, int64(3), calls.Load())

	require.NoError(t, l.Sync())
	require.Equal(t, int64(3), calls.Load(), "nothing left to sync")
}

func TestSync_ConcurrentAppendsShareOneFsync(t *testing.T) {
	const queued = 10

	var (
		calls    atomic.Int64
		blocking atomic.Bool
	)

	entered := make(chan struct{})
	release := make(chan struct{})

	dir := t.TempDir()
	setSyncData(t, func(file *os.File) error {
		calls.Add(1)

		if blocking.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}

		return file.Sync()
	})

	l := openLog(t, dir, Config{SyncOnAppend: true})

	blocking.Store(true)

	var wg sync.WaitGroup

	lasts := make(chan uint64, queued+1)
	appendOne := func(data []byte) {
		last, err := l.Append(data)
		if err != nil {
			t.Error(err)

			return
		}

		lasts <- last
	}

	wg.Go(func() { appendOne(payload(0)) })
	<-entered

	for i := range queued {
		wg.Go(func() { appendOne(payload(uint64(i + 1))) })
	}

	require.Eventually(t, func() bool {
		l.queueMu.Lock()
		defer l.queueMu.Unlock()

		return len(l.queue) == queued
	}, 5*time.Second, time.Millisecond)

	require.Equal(t, uint64(0), l.LastIndex(), "records are invisible until their fsync")

	close(release)
	wg.Wait()
	close(lasts)

	require.Equal(t, int64(2), calls.Load(), "the queued appends share one fsync")

	seen := map[uint64]bool{}
	for last := range lasts {
		seen[last] = true
	}

	require.Len(t, seen, queued+1)
	require.Equal(t, uint64(queued+1), l.LastIndex())
	require.NoError(t, l.Close())

	l = openLog(t, dir, Config{})
	recs := readAll(t, l)
	require.Len(t, recs, queued+1)

	for _, rec := range recs {
		require.True(t, seen[rec.Index])
	}
}

func TestAppend_GroupKeepsPerRecordOutcome(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{
		SegmentSize:   segmentOf(2),
		MaxRecordSize: segmentOf(2) - segmentHeaderSize - recordHeaderSize,
		MaxWALSize:    2 * segmentOf(2),
	}
	l := openLog(t, dir, cfg)
	appendN(t, l, 1)

	request := func(data []byte) *appendRequest {
		return &appendRequest{data: data, size: recordHeaderSize + int64(len(data)), done: make(chan struct{})}
	}

	fits := request(payload(2))
	rolls := request(payload(3))
	full := request(make([]byte, cfg.MaxRecordSize)) // needs a whole segment
	fitsAfter := request(payload(4))

	l.writeMu.Lock()
	l.writeGroup([]*appendRequest{fits, rolls, full, fitsAfter})
	l.writeMu.Unlock()

	for _, req := range []*appendRequest{fits, rolls, full, fitsAfter} {
		select {
		case <-req.done:
		default:
			t.Fatal("request not completed")
		}
	}

	require.NoError(t, fits.err)
	require.Equal(t, uint64(2), fits.index)
	require.NoError(t, rolls.err)
	require.Equal(t, uint64(3), rolls.index)
	require.ErrorIs(t, full.err, ErrFull)
	require.NoError(t, fitsAfter.err)
	require.Equal(t, uint64(4), fitsAfter.index)

	require.Equal(t, []string{segmentName(1), segmentName(3)}, segmentFiles(t, dir))
	requireRecords(t, readAll(t, l), 1, 4)
	require.NoError(t, l.Close())

	l = openLog(t, dir, cfg)
	requireRecords(t, readAll(t, l), 1, 4)
}

func TestSync_OnDemand(t *testing.T) {
	var calls atomic.Int64

	setSyncData(t, countingSync(&calls))

	l := openLog(t, t.TempDir(), Config{})

	appendN(t, l, 3)
	require.Zero(t, calls.Load())

	require.NoError(t, l.Sync())
	require.NoError(t, l.Sync())
	require.Equal(t, int64(1), calls.Load())

	appendN(t, l, 1)
	require.NoError(t, l.Sync())
	require.Equal(t, int64(2), calls.Load())
}

func TestSync_InBackground(t *testing.T) {
	var calls atomic.Int64

	setSyncData(t, countingSync(&calls))

	l := openLog(t, t.TempDir(), Config{SyncInterval: time.Millisecond})
	appendN(t, l, 3)

	require.Eventually(t, func() bool { return calls.Load() > 0 }, 5*time.Second, time.Millisecond)

	require.NoError(t, l.Close())

	select {
	case <-l.syncDone:
	default:
		t.Fatal("background sync still running after Close")
	}
}

func TestClose_RejectsFurtherUse(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{})
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
	require.Equal(t, uint64(0), l.Committed())
}

func TestFailure_IsSticky(t *testing.T) {
	var failing atomic.Bool

	injected := errors.New("injected fsync failure")
	dir := t.TempDir()
	cfg := Config{SyncOnAppend: true, SegmentSize: segmentOf(2), MaxRecordSize: payloadSize}

	setSyncData(t, func(file *os.File) error {
		if failing.Load() {
			return injected
		}

		return file.Sync()
	})

	l := openLog(t, dir, cfg)
	appendN(t, l, 3)

	failing.Store(true)

	_, err := l.Append(payload(4))
	require.ErrorIs(t, err, injected)
	require.Equal(t, uint64(3), l.LastIndex(), "a failed append is not visible")

	failing.Store(false)

	_, err = l.Append(payload(4))
	require.ErrorIs(t, err, injected)
	require.ErrorIs(t, l.Commit(2), injected)
	require.ErrorIs(t, l.Sync(), injected)

	requireRecords(t, readAll(t, l), 1, 3)
	require.ErrorIs(t, l.Close(), injected)

	// The write reached the file before the fsync failed, so it is found again.
	l = openLog(t, dir, cfg)
	requireRecords(t, readAll(t, l), 1, 4)
}

func TestConcurrent_AppendReadCommit(t *testing.T) {
	t.Parallel()

	const (
		writers   = 8
		perWriter = 300
		total     = writers * perWriter
	)

	l := openLog(t, t.TempDir(), Config{SegmentSize: 4096, MaxRecordSize: 64, SyncInterval: time.Millisecond})

	var wg sync.WaitGroup

	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
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
	deadline := time.Now().Add(30 * time.Second)
	committed := l.Committed()
	from := committed + 1

	for count := 0; count < total; {
		require.True(t, time.Now().Before(deadline), "read %d of %d records before the deadline", count, total)

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

		if last := from - 1; last-committed >= 256 {
			require.NoError(t, l.Commit(last))

			committed = last
		}
	}

	wg.Wait()

	require.NoError(t, l.Commit(from-1))

	require.Equal(t, uint64(total), l.Committed())
	require.Equal(t, uint64(total), l.LastIndex())
	require.Less(t, l.Size(), int64(2*4096), "committed segments were reclaimed")
}

func BenchmarkAppend(b *testing.B) {
	l := openBench(b, Config{})
	data := make([]byte, 160)

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()

	for b.Loop() {
		if _, err := l.Append(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_ConcurrentSync(b *testing.B) {
	l := openBench(b, Config{SyncOnAppend: true})
	data := make([]byte, 160)

	b.SetParallelism(8)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := l.Append(data); err != nil {
				b.Error(err)

				return
			}
		}
	})
}

func BenchmarkRead(b *testing.B) {
	const n = 100_000

	l := openBench(b, Config{})

	data := make([]byte, 160)

	for range n {
		if _, err := l.Append(data); err != nil {
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

func payload(index uint64) []byte {
	return fmt.Appendf(nil, "record-%05d", index)
}

// segmentOf returns the size of a segment holding exactly n test records.
func segmentOf(n int) int64 {
	return segmentHeaderSize + int64(n)*testRecordSize
}

func openLog(t *testing.T, dir string, cfg Config) *Log {
	t.Helper()

	l, err := Open(dir, cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := l.Close(); err != nil && !errors.Is(err, ErrClosed) {
			t.Errorf("close: %v", err)
		}
	})

	return l
}

// setSyncData replaces syncData until the test ends. Callers must not run in
// parallel, and must call it before opening a log so the log closes first.
func setSyncData(t *testing.T, fn func(*os.File) error) {
	t.Helper()

	prev := syncData
	syncData = fn

	t.Cleanup(func() { syncData = prev })
}

func openBench(b *testing.B, cfg Config) *Log {
	b.Helper()

	l, err := Open(b.TempDir(), cfg)
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

func countingSync(calls *atomic.Int64) func(*os.File) error {
	return func(file *os.File) error {
		calls.Add(1)

		return file.Sync()
	}
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

func flipByte(t *testing.T, path string, offset int64) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)

	b := make([]byte, 1)
	_, err = file.ReadAt(b, offset)
	require.NoError(t, err)

	b[0] ^= 0xff
	_, err = file.WriteAt(b, offset)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func appendBytes(t *testing.T, path string, b []byte) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)

	_, err = file.Write(b)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}
