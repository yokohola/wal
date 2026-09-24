package wal

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// errInjected is the fsync failure failSyncs injects.
var errInjected = errors.New("injected fsync failure")

// permanentFault drives the log into ErrPermanent. The log starts with records
// 1 to 3 durable and 2 committed; trigger returns the highest index a later Open
// may find and the error the failing call got.
type permanentFault struct {
	cfg     Config
	trigger func(t *testing.T, l *Log, dir string, failing *atomic.Bool) (uint64, error)
}

// Each failure that leaves the log in ErrPermanent, recovered once by Close and
// Open and once by a crash and Open.
func TestFailure_Permanent(t *testing.T) {
	onDemand := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	onAppend := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize, SyncOnAppend: true}

	appendFails := func(t *testing.T, l *Log, _ string, _ *atomic.Bool) (uint64, error) {
		readOnlyActive(t, l)

		_, err := l.Append(payload(4))
		require.Equal(t, uint64(3), l.LastIndex(), "the failed append is not visible")

		return 3, err
	}

	// Record 4 fills the active segment, so the group rolls first.
	rollFails := func(block func(t *testing.T, l *Log, dir string) func()) func(
		*testing.T, *Log, string, *atomic.Bool) (uint64, error) {
		return func(t *testing.T, l *Log, dir string, _ *atomic.Bool) (uint64, error) {
			appendN(t, l, 1)

			unblock := block(t, l, dir)
			reqs := writeGroup(t, l, 5, 6)
			unblock()

			require.ErrorIs(t, reqs[1].err, ErrPermanent, "the rest of the group")
			require.NoFileExists(t, filepath.Join(dir, segmentName(5)))
			require.Equal(t, uint64(4), l.LastIndex(), "the failed group is not visible")

			return 4, reqs[0].err
		}
	}

	cases := map[string]permanentFault{
		"append write fails, on-demand sync": {cfg: onDemand, trigger: appendFails},
		"append write fails, sync on append": {cfg: onAppend, trigger: appendFails},
		"append fsync fails, sync on append": {
			cfg: onAppend,
			trigger: func(t *testing.T, l *Log, _ string, failing *atomic.Bool) (uint64, error) {
				failing.Store(true)

				reqs := writeGroup(t, l, 4)
				require.Equal(t, uint64(3), l.LastIndex(), "the failed append is not visible")

				return 4, reqs[0].err // written before the fsync failed
			},
		},
		"append fsync fails, whole batch": {
			cfg: onAppend,
			trigger: func(t *testing.T, l *Log, _ string, failing *atomic.Bool) (uint64, error) {
				failing.Store(true)

				reqs := writeGroup(t, l, 4, 5, 6)
				require.Equal(t, uint64(3), l.LastIndex(), "the failed batch is not visible")
				for _, req := range reqs {
					require.ErrorIs(t, req.err, ErrPermanent)
				}

				return 6, reqs[0].err
			},
		},
		"sync fails": {
			cfg: onDemand,
			trigger: func(t *testing.T, l *Log, _ string, failing *atomic.Bool) (uint64, error) {
				appendN(t, l, 2)
				failing.Store(true)

				return 5, l.Sync()
			},
		},
		"commit fsync fails": {
			cfg: onDemand,
			trigger: func(t *testing.T, l *Log, _ string, failing *atomic.Bool) (uint64, error) {
				appendN(t, l, 1)
				failing.Store(true)

				err := l.Commit(4)
				require.Equal(t, uint64(2), l.Committed())

				return 4, err
			},
		},
		"roll seal fails": {
			cfg: onDemand,
			trigger: rollFails(func(t *testing.T, l *Log, _ string) func() {
				readOnlyActive(t, l)

				return func() {}
			}),
		},
		"roll segment creation fails, stale index removal": {
			cfg: onDemand,
			trigger: rollFails(func(t *testing.T, _ *Log, dir string) func() {
				return blockPath(t, filepath.Join(dir, indexName(5)))
			}),
		},
		"roll segment creation fails, temp file": {
			cfg: onDemand,
			trigger: rollFails(func(t *testing.T, _ *Log, dir string) func() {
				return blockPath(t, filepath.Join(dir, segmentName(5)+tempExt))
			}),
		},
		"roll segment creation fails, rename": {
			cfg: onDemand,
			trigger: rollFails(func(t *testing.T, _ *Log, dir string) func() {
				return blockPath(t, filepath.Join(dir, segmentName(5)))
			}),
		},
	}

	for name, fault := range cases {
		for _, recovery := range []string{"close", "crash"} {
			t.Run(name+", "+recovery, func(t *testing.T) {
				testPermanentFault(t, fault, recovery == "crash")
			})
		}
	}
}

func testPermanentFault(t *testing.T, fault permanentFault, crash bool) {
	failing := failSyncs(t)
	dir := t.TempDir()
	l := openLog(t, dir, fault.cfg)

	appendN(t, l, 3)
	require.NoError(t, l.Commit(2))
	require.NoError(t, l.Sync())

	require.NoError(t, l.Err())

	attempted, err := fault.trigger(t, l, dir, failing)
	require.ErrorIs(t, err, ErrPermanent)
	require.Equal(t, err, l.Err())
	require.ErrorContains(t, err, "Close and Open")

	failing.Store(false)
	requirePermanent(t, l)

	if crash {
		crashLog(t, l)
	} else {
		cp, _, _ := readCheckpoint(dir)

		require.ErrorIs(t, l.Close(), ErrPermanent)
		require.ErrorIs(t, l.Close(), ErrClosed)

		after, _, _ := readCheckpoint(dir)
		require.Equal(t, cp, after, "Close after ErrPermanent writes nothing")
	}

	l = openLog(t, dir, fault.cfg)
	require.Equal(t, uint64(2), l.Committed())
	require.GreaterOrEqual(t, l.LastIndex(), uint64(3), "durable records survive")
	require.LessOrEqual(t, l.LastIndex(), attempted)
	requireRecords(t, readAll(t, l), 1, int(l.LastIndex()))
	require.NoError(t, l.Err())

	next := l.LastIndex() + 1
	index, err := l.Append(payload(next))
	require.NoError(t, err)
	require.Equal(t, next, index)
	require.NoError(t, l.Commit(next))
	require.NoError(t, l.Close())
}

func TestFailure_CloseSealFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 3)

	readOnlyActive(t, l)

	err := l.Close()
	require.ErrorIs(t, err, ErrPermanent)
	require.ErrorIs(t, l.Err(), ErrPermanent)
	require.ErrorIs(t, l.Close(), ErrClosed)

	l = openLog(t, dir, cfg)
	requireRecords(t, readAll(t, l), 1, 3)
}

// A checkpoint write failure leaves the old or the new checkpoint on disk, both
// valid, so Commit fails with a plain error and a retry writes it again.
func TestFailure_CommitCheckpointIsRetryable(t *testing.T) {
	t.Parallel()

	for name, blocked := range map[string]string{
		"temp file": checkpointName + tempExt,
		"rename":    checkpointName,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
			l := openLog(t, dir, cfg)
			appendN(t, l, 3)

			unblock := blockPath(t, filepath.Join(dir, blocked))

			err := l.Commit(2)
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrPermanent)
			require.Equal(t, uint64(0), l.Committed())

			appendN(t, l, 1)
			require.NoError(t, l.Sync())

			unblock()

			require.NoError(t, l.Commit(2))
			require.Equal(t, uint64(2), l.Committed())
			require.NoError(t, l.Close())

			l = openLog(t, dir, cfg)
			require.Equal(t, uint64(2), l.Committed())
			requireRecords(t, readAll(t, l), 1, 4)
		})
	}
}

func TestFailure_CloseCheckpointIsPlain(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 3)
	require.NoError(t, l.Commit(1))
	appendN(t, l, 1) // so Close has a new checkpoint to write

	unblock := blockPath(t, filepath.Join(dir, checkpointName+tempExt))

	err := l.Close()
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrPermanent)
	require.ErrorIs(t, l.Close(), ErrClosed)

	unblock()

	l = openLog(t, dir, cfg)
	require.Equal(t, uint64(1), l.Committed())
	requireRecords(t, readAll(t, l), 1, 4)
}

// A roll that cannot delete the committed segment it leaves behind fails the
// Append with a plain error and writes nothing; the next Append succeeds.
func TestFailure_ReclaimAfterRollIsPlain(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{SegmentSize: segmentOf(4), MaxRecordSize: payloadSize}
	l := openLog(t, dir, cfg)
	appendN(t, l, 4)
	require.NoError(t, l.Commit(4))

	// The seal's index write fails silently, then removal of the index fails.
	unblock := blockPath(t, filepath.Join(dir, indexName(1)))

	_, err := l.Append(payload(5))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrPermanent)
	require.Equal(t, uint64(4), l.LastIndex(), "the record is not written")
	require.FileExists(t, filepath.Join(dir, segmentName(1)))

	index, err := l.Append(payload(5))
	require.NoError(t, err)
	require.Equal(t, uint64(5), index)

	unblock()

	require.NoError(t, l.Commit(4), "Commit retries the deletion")
	require.Equal(t, []string{segmentName(5)}, segmentFiles(t, dir))
	requireRecords(t, readAll(t, l), 5, 1)
}

// Err reads the failure without waiting for a write in progress.
func TestFailure_ErrDoesNotWaitOnWrites(t *testing.T) {
	t.Parallel()

	l := openLog(t, t.TempDir(), Config{})

	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	done := make(chan error)

	go func() { done <- l.Err() }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Err waited on writeMu")
	}
}

// requirePermanent checks that every mutation reports ErrPermanent and reads
// still return the visible records.
func requirePermanent(t *testing.T, l *Log) {
	t.Helper()

	visible := l.LastIndex()

	_, err := l.Append(payload(visible + 1))
	require.ErrorIs(t, err, ErrPermanent)
	require.ErrorIs(t, l.Sync(), ErrPermanent)
	require.ErrorIs(t, l.Commit(l.Committed()), ErrPermanent)

	require.Equal(t, visible, l.LastIndex(), "failed appends are not visible")
	requireRecords(t, readAll(t, l), 1, int(visible))
}

// failSyncs makes syncData fail while the returned flag is set.
func failSyncs(t *testing.T) *atomic.Bool {
	t.Helper()

	var failing atomic.Bool

	setSyncData(t, func(file *os.File) error {
		if failing.Load() {
			return errInjected
		}

		return file.Sync()
	})

	return &failing
}

// readOnlyActive swaps the active segment's file for a read-only one, so writes
// and truncation fail while reads and fsync work.
func readOnlyActive(t *testing.T, l *Log) {
	t.Helper()

	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	tail := l.active()

	file, err := os.Open(tail.path)
	require.NoError(t, err)

	l.mu.Lock()
	orig := tail.file
	tail.file = file
	l.mu.Unlock()

	t.Cleanup(func() { _ = orig.Close() })
}

// blockPath puts a non-empty directory at path, so creating, replacing or
// removing a file there fails. The returned func removes it.
func blockPath(t *testing.T, path string) func() {
	t.Helper()

	require.NoError(t, os.Mkdir(path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(path, "file"), nil, 0o644))

	return func() { require.NoError(t, os.RemoveAll(path)) }
}

// writeGroup appends payload(i) for each i as one group and returns the
// completed requests.
func writeGroup(t *testing.T, l *Log, indexes ...uint64) []*appendRequest {
	t.Helper()

	reqs := make([]*appendRequest, len(indexes))
	for i, index := range indexes {
		data := payload(index)
		reqs[i] = &appendRequest{data: data, size: recordHeaderSize + int64(len(data)), done: make(chan struct{})}
	}

	l.writeMu.Lock()
	l.writeGroup(reqs)
	l.writeMu.Unlock()

	for _, req := range reqs {
		<-req.done
	}

	return reqs
}

// crashLog releases the directory without syncing or writing a checkpoint, as
// a crashed process would.
func crashLog(t *testing.T, l *Log) {
	t.Helper()

	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()

	require.NoError(t, l.release())
}
