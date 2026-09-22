// Package wal is an append-only log of byte records on local disk.
//
// Records get dense, monotonic indexes starting at 1. A consumer reads from
// any retained index, keeps its own position, and calls Commit to say what it
// is done with. Space below the checkpoint is reclaimed segment by segment.
// One Log per directory, one process at a time.
//
// Byte slices passed to Append are copied. Slices returned by Read belong to
// the caller.
package wal

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sync"
	"time"
)

// DefaultSegmentSize applies when Options.SegmentSize is zero.
const DefaultSegmentSize = 64 << 20

const (
	lockFile      = "LOCK"
	maxKeptBuffer = 8 << 20
)

// Errors a Log reports, to be matched with errors.Is.
var (
	ErrClosed         = errors.New("wal: closed")
	ErrCorrupt        = errors.New("wal: corrupt")
	ErrFull           = errors.New("wal: full")
	ErrInvalidOptions = errors.New("wal: invalid options")
	ErrLocked         = errors.New("wal: directory locked by another process")
	ErrOutOfRange     = errors.New("wal: index out of range")
)

// wrap tags an operating system error, which already names the operation
// and the path.
func wrap(err error) error {
	return fmt.Errorf("wal: %w", err)
}

// Options configure a Log. The zero value works: 64 MiB segments, no size
// cap, fsync only at segment roll, Commit and Close.
type Options struct {
	// SegmentSize is the size at which the active segment is closed and a
	// new one started. A record larger than this gets a segment of its own.
	SegmentSize int64

	// MaxSize caps bytes on disk; Append fails with ErrFull beyond it.
	// Must be at least 2*SegmentSize so a full segment can always be
	// reclaimed. 0 means no cap.
	MaxSize int64

	// SyncOnAppend fsyncs after every Append. Durable, but bound by fsync
	// latency; batch payloads into one Append to amortize it.
	SyncOnAppend bool

	// SyncInterval fsyncs in the background at this period. 0 disables it.
	SyncInterval time.Duration
}

func (o Options) normalize() (Options, error) {
	if o.SegmentSize < 0 || o.MaxSize < 0 || o.SyncInterval < 0 {
		return o, fmt.Errorf("%w: negative value", ErrInvalidOptions)
	}

	if o.SegmentSize == 0 {
		o.SegmentSize = DefaultSegmentSize
	}

	if o.MaxSize > 0 && o.MaxSize < 2*o.SegmentSize {
		return o, fmt.Errorf("%w: MaxSize below 2*SegmentSize", ErrInvalidOptions)
	}

	return o, nil
}

// Record is one entry of the log.
type Record struct {
	Index uint64
	Data  []byte
}

// Log is an append-only log in one directory.
//
// All methods are safe for concurrent use. Reads run in parallel with each
// other and with Sync; Append and Commit are exclusive. After a failed
// write or fsync the log refuses further Append, Sync and Commit calls
// with that error; reopen to recover.
//
// Invariant: FirstIndex <= Committed <= LastIndex+1.
type Log struct {
	dir  string
	opts Options
	lock *os.File

	mu        sync.RWMutex
	segments  []*segment // ascending by first index; the last one is active
	committed uint64
	size      int64
	buf       []byte
	failed    error
	closed    bool

	syncStop chan struct{}
	syncDone chan struct{}
}

// Open opens or creates the log in dir. It trims a torn tail left by a
// crash and fails with ErrCorrupt on any other damage, or ErrLocked when
// another process holds the directory.
func Open(dir string, opts Options) (*Log, error) {
	opts, err := opts.normalize()
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

	log := &Log{dir: dir, opts: opts, lock: lock}

	if err := log.load(); err != nil {
		return nil, errors.Join(err, log.release())
	}

	log.startSyncer()

	return log, nil
}

// Append writes data as consecutive records and returns the index of the
// last one. Either all records are written or none. Fails with ErrFull when
// MaxSize would be exceeded.
func (l *Log) Append(data ...[]byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.writable(); err != nil {
		return 0, err
	}

	tail := l.active()
	if len(data) == 0 {
		return tail.next() - 1, nil
	}

	batch := batchSize(data)
	roll := tail.count > 0 && tail.size+batch > l.opts.SegmentSize

	grow := batch
	if roll {
		grow += segmentHeaderSize
	}

	if l.opts.MaxSize > 0 && l.size+grow > l.opts.MaxSize {
		return 0, ErrFull
	}

	if roll {
		if err := l.roll(); err != nil {
			return 0, err
		}

		tail = l.active()
	}

	return l.write(tail, data)
}

// Read returns up to limit records starting at from, fewer at the end of
// the log. from must be in [FirstIndex, LastIndex+1]; reading at
// LastIndex+1 returns an empty slice.
func (l *Log) Read(from uint64, limit int) ([]Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrClosed
	}

	first, next := l.segments[0].first, l.active().next()
	if from < first || from > next {
		return nil, fmt.Errorf("%w: %d outside [%d, %d]", ErrOutOfRange, from, first, next)
	}

	out := make([]Record, 0, readCapacity(limit, next-from))
	if limit <= 0 || from == next {
		return out, nil
	}

	for i := l.segmentAt(from); i < len(l.segments) && len(out) < limit; i++ {
		var err error

		out, err = l.segments[i].read(from, limit, out)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// Commit marks records before index as processed, persists that, and
// removes segments holding only committed records. Monotonic and
// idempotent: an index at or below Committed is a no-op. Fails with
// ErrOutOfRange above LastIndex+1.
func (l *Log) Commit(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.writable(); err != nil {
		return err
	}

	if next := l.active().next(); index > next {
		return fmt.Errorf("%w: %d above %d", ErrOutOfRange, index, next)
	}

	if index <= l.committed {
		return nil
	}

	if err := writeCheckpoint(l.dir, index); err != nil {
		return l.fail(err)
	}

	l.committed = index

	return l.reclaim()
}

// Committed returns the persisted checkpoint: the first index not yet
// committed. Resume reading here after Open.
func (l *Log) Committed() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.committed
}

// FirstIndex returns the oldest index still on disk.
func (l *Log) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].first
}

// LastIndex returns the newest index on disk, or FirstIndex-1 when the log
// is empty.
func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.active().next() - 1
}

// Size returns bytes on disk, including committed records not yet
// reclaimed.
func (l *Log) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.size
}

// Sync flushes appended records to stable storage.
func (l *Log) Sync() error {
	l.mu.RLock()

	err := l.writable()
	if err != nil {
		l.mu.RUnlock()

		return err
	}

	err = fdatasync(l.active().file)
	l.mu.RUnlock()

	if err == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	return l.fail(wrap(err))
}

// Close syncs, stops background work and releases the directory.
func (l *Log) Close() error {
	l.mu.Lock()

	if l.closed {
		l.mu.Unlock()

		return ErrClosed
	}

	l.closed = true
	l.mu.Unlock()

	l.stopSyncer()

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.failed != nil {
		return errors.Join(l.failed, l.release())
	}

	return errors.Join(finishSegment(l.active()), l.lock.Close())
}

// release closes the active segment without syncing it, then the lock.
func (l *Log) release() error {
	var err error

	if tail := l.active(); tail != nil && tail.file != nil {
		err = tail.file.Close()
		tail.file = nil
	}

	return errors.Join(err, l.lock.Close())
}

func (l *Log) active() *segment {
	if len(l.segments) == 0 {
		return nil
	}

	return l.segments[len(l.segments)-1]
}

// segmentAt returns the position of the segment holding index.
func (l *Log) segmentAt(index uint64) int {
	pos, found := slices.BinarySearchFunc(l.segments, index, func(seg *segment, target uint64) int {
		return cmp.Compare(seg.first, target)
	})
	if !found {
		pos--
	}

	return pos
}

func (l *Log) writable() error {
	if l.closed {
		return ErrClosed
	}

	return l.failed
}

// fail records the first disk failure; every later write reports it.
func (l *Log) fail(err error) error {
	if l.failed == nil {
		l.failed = err
	}

	return l.failed
}

func (l *Log) write(tail *segment, data [][]byte) (uint64, error) {
	l.buf = l.buf[:0]

	for _, rec := range data {
		var err error

		l.buf, err = appendRecord(l.buf, rec)
		if err != nil {
			return 0, err
		}
	}

	if _, err := tail.file.WriteAt(l.buf, tail.size); err != nil {
		return 0, l.fail(wrap(err))
	}

	for _, rec := range data {
		tail.advance(recordHeaderSize + int64(len(rec)))
	}

	l.size += int64(len(l.buf))

	if cap(l.buf) > maxKeptBuffer {
		l.buf = nil
	}

	if l.opts.SyncOnAppend {
		if err := fdatasync(tail.file); err != nil {
			return 0, l.fail(wrap(err))
		}
	}

	return tail.next() - 1, nil
}

// roll closes the active segment and starts the next one.
func (l *Log) roll() error {
	tail := l.active()

	if err := finishSegment(tail); err != nil {
		return l.fail(err)
	}

	next, err := createSegment(l.dir, tail.next(), l.opts.SegmentSize)
	if err != nil {
		return l.fail(err)
	}

	l.segments = append(l.segments, next)
	l.size += segmentHeaderSize

	return nil
}

func (l *Log) startSyncer() {
	if l.opts.SyncInterval <= 0 {
		return
	}

	l.syncStop = make(chan struct{})
	l.syncDone = make(chan struct{})

	go l.runSyncer()
}

// runSyncer fsyncs at the configured interval until stopped or the first
// failure, which Sync keeps for the caller.
func (l *Log) runSyncer() {
	defer close(l.syncDone)

	ticker := time.NewTicker(l.opts.SyncInterval)
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

func batchSize(data [][]byte) int64 {
	var total int64

	for _, rec := range data {
		total += recordHeaderSize + int64(len(rec))
	}

	return total
}

// readCapacity sizes a Read result: limit, or fewer when the log ends first.
func readCapacity(limit int, pending uint64) int {
	if limit <= 0 {
		return 0
	}

	if pending > math.MaxInt {
		return limit
	}

	return min(limit, int(pending))
}
