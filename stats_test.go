package wal

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStats_FreshLog(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{})

	require.Equal(t, Stats{FirstIndex: 1, Size: segmentHeaderSize, Segments: 1}, l.Stats())
}

func TestStats_TracksLifecycle(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{SegmentSize: segmentOf(2), MaxRecordSize: payloadSize})

	// Segments [1,2] [3,4] [5]; sealing on roll is not a data fsync.
	appendN(t, l, 5)

	s := l.Stats()
	require.Equal(t, uint64(5), s.Appends)
	require.Equal(t, uint64(5*testRecordSize), s.AppendedBytes)
	require.Equal(t, uint64(5), s.Writes)
	require.Equal(t, uint64(2), s.Rolls)
	require.Equal(t, 3, s.Segments)
	require.Equal(t, uint64(5), s.LastIndex)
	require.Equal(t, l.Size(), s.Size)
	require.Zero(t, s.Syncs)

	// Record 3 is durable from the roll, so this Commit needs no fsync.
	require.NoError(t, l.Commit(3))
	require.NoError(t, l.Commit(5))
	require.NoError(t, l.Commit(5), "a Commit that does not advance is not counted")

	s = l.Stats()
	require.Equal(t, uint64(2), s.Commits)
	require.Equal(t, uint64(1), s.Syncs)
	require.Positive(t, s.SyncTime)
	require.Equal(t, uint64(2), s.Reclaimed)
	require.Equal(t, 1, s.Segments)
	require.Equal(t, uint64(5), s.FirstIndex)
	require.Equal(t, uint64(5), s.Committed)
	require.Equal(t, segmentOf(1), s.Size)
}

func TestStats_SyncOnAppendCountsEachFsync(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{SyncOnAppend: true})
	appendN(t, l, 3)
	require.NoError(t, l.Sync())

	require.Equal(t, uint64(3), l.Stats().Syncs, "Sync had nothing left to sync")
}

func TestStats_GroupIsOneWrite(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{SyncOnAppend: true})
	writeGroup(t, l, 1, 2, 3)

	s := l.Stats()
	require.Equal(t, uint64(3), s.Appends)
	require.Equal(t, uint64(1), s.Writes)
	require.Equal(t, uint64(1), s.Syncs)
}

func TestStats_ReportsFailure(t *testing.T) {
	failing := failSyncs(t)

	l := openLog(t, t.TempDir(), Config{SyncOnAppend: true})
	appendN(t, l, 1)

	failing.Store(true)

	_, err := l.Append(payload(2))
	require.ErrorIs(t, err, ErrPermanent)

	s := l.Stats()
	require.True(t, s.Failed)
	require.Equal(t, uint64(1), s.Appends, "a failed append is not counted")
	require.Equal(t, uint64(2), s.Syncs, "the failed fsync is counted")

	require.ErrorIs(t, l.Close(), ErrPermanent)
	require.Equal(t, s, l.Stats(), "Stats works after Close")
}

func TestStats_ConcurrentWithWrites(t *testing.T) {
	t.Parallel()

	const (
		writers   = 4
		perWriter = 200
	)

	l := openLog(t, t.TempDir(), Config{SegmentSize: segmentOf(16), MaxRecordSize: payloadSize, SyncOnAppend: true})

	var wg sync.WaitGroup

	for range writers {
		wg.Go(func() {
			for i := range perWriter {
				if _, err := l.Append(payload(uint64(i))); err != nil {
					t.Error(err)

					return
				}
			}
		})
	}

	wg.Go(func() {
		for l.Stats().Appends < writers*perWriter {
			if err := l.Commit(l.LastIndex()); err != nil {
				t.Error(err)

				return
			}
		}
	})

	wg.Wait()

	s := l.Stats()
	require.Equal(t, uint64(writers*perWriter), s.Appends)
	require.Equal(t, uint64(writers*perWriter), s.LastIndex)
	require.LessOrEqual(t, s.Writes, s.Appends)
	require.Positive(t, s.Rolls)
}
