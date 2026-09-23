// Package wal is an append-only log of byte records on local disk.
//
// Records get dense, monotonic indexes starting at 1. A consumer reads from any
// retained index, keeps its own position and calls Commit once it is done with
// a prefix of the log. Segments holding only committed records are deleted.
package wal

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"
)

// DefaultSegmentSize applies when Config.SegmentSize is zero.
const DefaultSegmentSize = 64 << 20

const (
	// maxKeptBuffer bounds the encode buffer a Log keeps between appends.
	maxKeptBuffer = 8 << 20

	// maxReadPrealloc bounds the result capacity Read allocates up front.
	maxReadPrealloc = 1024
)

// Errors reported by a Log, to be matched with errors.Is.
var (
	ErrClosed        = errors.New("wal: closed")
	ErrCorrupt       = errors.New("wal: corrupt")
	ErrFull          = errors.New("wal: full")
	ErrInvalidConfig = errors.New("wal: invalid config")
	ErrLocked        = errors.New("wal: directory locked by another process")
	ErrOutOfRange    = errors.New("wal: index out of range")
	ErrTooLarge      = errors.New("wal: batch too large")
)

// Config configures a Log. The zero value gives 64 MiB segments, no size cap
// and fsync only where durability requires it: segment roll, Commit and Close.
type Config struct {
	// SegmentSize is the size at which the active segment is closed and a new
	// one started. A batch is never split, so a segment may exceed it by one
	// batch.
	SegmentSize int64

	// MaxSize caps Size. Append fails with ErrFull while a batch does not fit
	// and with ErrTooLarge when it never can. Zero means no cap; otherwise it
	// must be at least SegmentSize.
	MaxSize int64

	// SyncOnAppend makes every Append durable before it returns. Batch records
	// into one Append to amortize the fsync.
	SyncOnAppend bool

	// SyncInterval makes appended records durable in the background at this
	// period. Zero disables it.
	SyncInterval time.Duration
}

// Record is one entry of the log.
type Record struct {
	Index uint64
	Data  []byte
}

// Log is an append-only log in one directory, used by one process at a time.
// Create it with Open and do not copy it.
//
// Methods are safe for concurrent use. Append, Commit, Sync and Close run one
// at a time. Read and the accessors run alongside them and wait only while new
// state is published, never during a write or fsync.
//
// Invariants: segments hold contiguous indexes and only the last, the active
// segment, takes appends; FirstIndex <= Committed <= LastIndex+1; records below
// Committed are durable.
type Log struct {
	dir      string
	cfg      Config
	lock     *os.File
	syncData func(*os.File) error // fdatasync; tests substitute it

	// writeMu serializes mutations and guards the fields below it.
	writeMu   sync.Mutex
	encodeBuf []byte
	synced    uint64 // records below this index are durable
	failed    error  // first write or fsync failure

	// mu guards the fields below and the segments' state. They change only
	// under writeMu and mu together, so holding either one is enough to read.
	mu        sync.RWMutex
	segments  []*segment
	committed uint64
	size      int64
	closed    bool

	syncStop chan struct{}
	syncDone chan struct{}
}

// Open opens or creates the log in dir. It repairs what a crash can leave
// behind and fails with ErrCorrupt on any other damage, or with ErrLocked when
// another process holds dir. Open reads every retained record to verify it.
func Open(dir string, cfg Config) (*Log, error) {
	return open(dir, cfg, fdatasync)
}

func open(dir string, cfg Config, syncData func(*os.File) error) (*Log, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, wrap(err)
	}

	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}

	l := &Log{dir: dir, cfg: cfg, lock: lock, syncData: syncData}

	if err := l.load(); err != nil {
		return nil, errors.Join(err, l.release())
	}

	l.startSyncer()

	return l, nil
}

func (o Config) withDefaults() (Config, error) {
	if o.SegmentSize < 0 || o.MaxSize < 0 || o.SyncInterval < 0 {
		return o, fmt.Errorf("%w: negative value", ErrInvalidConfig)
	}

	if o.SegmentSize == 0 {
		o.SegmentSize = DefaultSegmentSize
	}

	if o.MaxSize > 0 && o.MaxSize < o.SegmentSize {
		return o, fmt.Errorf("%w: MaxSize %d is below SegmentSize %d", ErrInvalidConfig, o.MaxSize, o.SegmentSize)
	}

	return o, nil
}

// Append writes data as consecutive records and returns the index of the last
// one. Records become visible to Read when Append succeeds. A crash can keep
// any prefix of the batch, and a batch whose Append failed may still be found
// after reopening. A write or fsync failure is sticky, see Log.
func (l *Log) Append(data ...[]byte) (uint64, error) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	if err := l.checkWritable(); err != nil {
		return 0, err
	}

	if len(data) == 0 {
		return l.active().nextIndex() - 1, nil
	}

	batch, err := l.batchSize(data)
	if err != nil {
		return 0, err
	}

	if err := l.makeRoom(batch); err != nil {
		return 0, err
	}

	return l.write(data)
}

// Read returns up to limit records starting at from, fewer at the end of the
// log. from must lie in [FirstIndex, LastIndex+1]; at LastIndex+1 the result is
// empty. The caller owns the returned data. Records of one call may share a
// backing array, but no slice's capacity reaches into another's bytes.
func (l *Log) Read(from uint64, limit int) ([]Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrClosed
	}

	first, next := l.segments[0].first, l.active().nextIndex()
	if from < first || from > next {
		return nil, fmt.Errorf("%w: %d outside [%d, %d]", ErrOutOfRange, from, first, next)
	}

	if limit <= 0 || from == next {
		return []Record{}, nil
	}

	out := make([]Record, 0, min(uint64(limit), next-from, maxReadPrealloc))

	for i := l.segmentFor(from); i < len(l.segments) && len(out) < limit; i++ {
		var err error

		out, err = l.segments[i].read(from, limit, out)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// Commit marks every record below index as processed. It makes those records
// and the new checkpoint durable, then deletes segments holding only committed
// records. An index at or below Committed changes nothing but retries deletions
// that failed before. It fails with ErrOutOfRange above LastIndex+1.
func (l *Log) Commit(index uint64) error {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	if err := l.checkWritable(); err != nil {
		return err
	}

	if next := l.active().nextIndex(); index > next {
		return fmt.Errorf("%w: %d above %d", ErrOutOfRange, index, next)
	}

	if index > l.committed {
		if index > l.synced {
			if err := l.syncActive(); err != nil {
				return err
			}
		}

		if err := writeCheckpoint(l.dir, index); err != nil {
			return err
		}

		l.mu.Lock()
		l.committed = index
		l.mu.Unlock()
	}

	return l.reclaim()
}

// Committed returns the checkpoint: the first index not yet committed, where a
// consumer resumes after Open.
func (l *Log) Committed() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.committed
}

// FirstIndex returns the index of the oldest retained record.
func (l *Log) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].first
}

// LastIndex returns the index of the newest record, or FirstIndex-1 when the
// log holds none.
func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.active().nextIndex() - 1
}

// Size returns the bytes of all retained segments. Preallocation can make the
// active segment take up to SegmentSize on disk before it fills.
func (l *Log) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.size
}

// Sync makes every appended record durable.
func (l *Log) Sync() error {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	if err := l.checkWritable(); err != nil {
		return err
	}

	return l.syncActive()
}

// Close makes appended records durable, stops the background sync and releases
// the directory. After a failure it releases without syncing and returns that
// failure.
func (l *Log) Close() error {
	l.writeMu.Lock()

	if l.closed {
		l.writeMu.Unlock()

		return ErrClosed
	}

	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	l.writeMu.Unlock()

	// The syncer may be waiting for writeMu, so it is stopped without holding it.
	l.stopSyncer()

	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	if l.failed != nil {
		return errors.Join(l.failed, l.release())
	}

	return errors.Join(l.active().seal(), l.release())
}

func (l *Log) active() *segment {
	if len(l.segments) == 0 {
		return nil
	}

	return l.segments[len(l.segments)-1]
}

// segmentFor returns the position of the segment holding index.
func (l *Log) segmentFor(index uint64) int {
	i, found := slices.BinarySearchFunc(l.segments, index, func(seg *segment, target uint64) int {
		return cmp.Compare(seg.first, target)
	})
	if !found {
		i--
	}

	return i
}

func (l *Log) checkWritable() error {
	if l.closed {
		return ErrClosed
	}

	return l.failed
}

// fail records the first write or fsync failure. Afterwards what reached the
// disk is unknown, so every later mutation reports it until the log is reopened.
func (l *Log) fail(err error) error {
	if l.failed == nil {
		l.failed = err
	}

	return l.failed
}

// batchSize returns the encoded size of data. It fails with ErrTooLarge when a
// record exceeds the format limit or the batch can never fit MaxSize.
func (l *Log) batchSize(data [][]byte) (int64, error) {
	var total int64

	for i, rec := range data {
		if int64(len(rec)) > maxRecordSize {
			return 0, fmt.Errorf("%w: record %d has %d bytes, limit %d", ErrTooLarge, i, len(rec), int64(maxRecordSize))
		}

		total += recordHeaderSize + int64(len(rec))
	}

	if l.cfg.MaxSize > 0 && segmentHeaderSize+total > l.cfg.MaxSize {
		return 0, fmt.Errorf("%w: %d bytes never fit MaxSize %d", ErrTooLarge, total, l.cfg.MaxSize)
	}

	return total, nil
}

// makeRoom rolls to a new segment when the batch overflows the active one, or
// when the active segment is fully committed and rolling it away frees the
// space the batch needs. It fails with ErrFull when the batch does not fit.
func (l *Log) makeRoom(batch int64) error {
	tail := l.active()
	roll := tail.count > 0 && tail.size+batch > l.cfg.SegmentSize

	if l.cfg.MaxSize > 0 {
		after := l.size + batch
		if roll {
			after += segmentHeaderSize
		}

		if after > l.cfg.MaxSize && tail.count > 0 && tail.nextIndex() == l.committed {
			roll = true
			after = l.size - tail.size + segmentHeaderSize + batch
		}

		if after > l.cfg.MaxSize {
			return ErrFull
		}
	}

	if !roll {
		return nil
	}

	if err := l.roll(); err != nil {
		return err
	}

	return l.reclaim()
}

// write appends the encoded batch to the active segment and publishes it.
func (l *Log) write(data [][]byte) (uint64, error) {
	tail := l.active()

	buf := l.encodeBuf[:0]
	for _, rec := range data {
		buf = appendRecord(buf, rec)
	}

	if cap(buf) <= maxKeptBuffer {
		l.encodeBuf = buf
	}

	if _, err := tail.file.WriteAt(buf, tail.size); err != nil {
		return 0, l.fail(wrap(err))
	}

	if l.cfg.SyncOnAppend {
		if err := l.syncData(tail.file); err != nil {
			return 0, l.fail(wrap(err))
		}
	}

	l.mu.Lock()

	for _, rec := range data {
		tail.addRecord(recordHeaderSize + int64(len(rec)))
	}

	l.size += int64(len(buf))
	l.mu.Unlock()

	if l.cfg.SyncOnAppend {
		l.synced = tail.nextIndex()
	}

	return tail.nextIndex() - 1, nil
}

// roll seals the active segment and starts the next one. Sealing comes first,
// so every segment but the last is complete and durable.
func (l *Log) roll() error {
	tail := l.active()

	if err := tail.seal(); err != nil {
		return l.fail(err)
	}

	next, err := createSegment(l.dir, tail.nextIndex(), l.cfg.SegmentSize)
	if err != nil {
		return l.fail(err)
	}

	l.mu.Lock()
	closeErr := tail.close()
	l.segments = append(l.segments, next)
	l.size += segmentHeaderSize
	l.mu.Unlock()

	l.synced = next.first

	if closeErr != nil {
		return l.fail(closeErr)
	}

	return nil
}

// reclaim deletes closed segments whose records are all committed. Readers hold
// mu while they read, so a segment is never deleted under one.
func (l *Log) reclaim() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var (
		n   int
		err error
	)

	for ; n < len(l.segments)-1 && l.segments[n].nextIndex() <= l.committed; n++ {
		if err = removeFile(l.segments[n].path); err != nil {
			break
		}

		l.size -= l.segments[n].size
	}

	l.segments = slices.Delete(l.segments, 0, n)

	return err
}

func (l *Log) syncActive() error {
	tail := l.active()
	if l.synced == tail.nextIndex() {
		return nil
	}

	if err := l.syncData(tail.file); err != nil {
		return l.fail(wrap(err))
	}

	l.synced = tail.nextIndex()

	return nil
}

// release closes the active segment without syncing it and unlocks the
// directory.
func (l *Log) release() error {
	var err error

	if tail := l.active(); tail != nil {
		err = tail.close()
	}

	return errors.Join(err, l.lock.Close())
}

func (l *Log) startSyncer() {
	if l.cfg.SyncInterval == 0 {
		return
	}

	l.syncStop = make(chan struct{})
	l.syncDone = make(chan struct{})

	go l.runSyncer()
}

// runSyncer syncs at the configured interval until stopped or until a sync
// fails, which leaves the failure for the next mutation to report.
func (l *Log) runSyncer() {
	defer close(l.syncDone)

	ticker := time.NewTicker(l.cfg.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.syncStop:
			return
		case <-ticker.C:
			if err := l.Sync(); err != nil {
				return
			}
		}
	}
}

func (l *Log) stopSyncer() {
	if l.syncStop == nil {
		return
	}

	close(l.syncStop)
	<-l.syncDone
}

// wrap tags an operating system error, which already names the operation and
// the path.
func wrap(err error) error {
	return fmt.Errorf("wal: %w", err)
}
