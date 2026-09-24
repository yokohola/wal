// Package wal is an append-only log of byte records on local disk.
//
// Records get dense indexes starting at 1. A consumer reads from any retained
// index and commits the prefix it is done with, and fully committed segments
// are deleted.
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

// Defaults applied to zero Config fields.
const (
	DefaultSegmentSize   = 64 << 20
	DefaultMaxRecordSize = 1 << 20
)

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
	ErrTooLarge      = errors.New("wal: too large")

	// ErrPermanent wraps the first failed segment write, fsync or segment
	// creation; Append, Sync, Commit and Close return it from then on, and Read
	// still works. Close and Open the log again: records not synced before it
	// may be lost on power failure, and a failed Append may still be found.
	ErrPermanent = errors.New("wal: permanent failure, Close and Open the log again")
)

// syncData makes a file's written data durable.
var syncData = fdatasync

// CorruptError is the ErrCorrupt Read returns when the segment file Path is
// damaged at byte Offset and records First to Last cannot be read. A consumer
// that gives them up reads on from Last+1; the log itself never skips them.
type CorruptError struct {
	Path   string
	Offset int64
	First  uint64
	Last   uint64
	Err    error // what is wrong at Offset
}

// Config configures a Log. The zero value gives 64 MiB segments, 1 MiB records,
// no size cap and fsync only where durability requires it: segment roll, Commit
// and Close.
type Config struct {
	// SegmentSize is the size at which the active segment is closed and a new
	// one started. A record is never split, so a segment holding one record of
	// SegmentSize bytes exceeds it by that record's framing.
	SegmentSize int64

	// MaxWALSize caps Size; Append fails with ErrFull while a record does not
	// fit. Zero means no cap, otherwise it must be at least SegmentSize.
	MaxWALSize int64

	// MaxRecordSize caps one record's data, at most SegmentSize; Append rejects a
	// larger one with ErrTooLarge. Lowering it keeps existing records readable.
	MaxRecordSize int64

	// SyncOnAppend makes every Append durable before it returns, with concurrent
	// Appends sharing one fsync. Without it, call Sync or Commit.
	SyncOnAppend bool
}

// Record is one entry of the log.
type Record struct {
	Index uint64
	Data  []byte
}

// Log is an append-only log in one directory, created with Open and safe for
// concurrent use; reads never wait on writes. Segments hold contiguous indexes,
// only the last one takes appends, and committed records are always durable.
type Log struct {
	dir  string
	cfg  Config
	lock *os.File

	// queueMu guards the Appends waiting for the next group write.
	queueMu sync.Mutex
	queue   []*appendRequest

	// writeMu serializes mutations and guards the fields below it.
	writeMu   sync.Mutex
	encodeBuf []byte
	synced    uint64     // records below this index are durable
	syncedEnd int64      // offset of synced in the active segment
	saved     checkpoint // last checkpoint written or loaded
	doomed    []*segment // reclaimed from segments, not yet deleted
	counters  counters

	// mu guards the fields below and the segments' state. They change only
	// under writeMu and mu together, so holding either one is enough to read.
	mu        sync.RWMutex
	segments  []*segment
	committed uint64
	size      int64
	closed    bool
	failed    error // ErrPermanent wrapping the first write or fsync failure
}

// appendRequest is a record waiting in the queue. The leader of its group sets
// index and err, then closes done.
type appendRequest struct {
	data []byte
	size int64 // encoded size

	done  chan struct{}
	index uint64
	err   error
}

// Open opens or creates the log in dir. Records appended since the last Commit
// or Close end at the first damaged one, as a crash leaves them; other damage
// fails Open or Read with ErrCorrupt, and a held dir fails with ErrLocked.
func Open(dir string, cfg Config) (*Log, error) {
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

	l := &Log{dir: dir, cfg: cfg, lock: lock}

	if err := l.load(); err != nil {
		return nil, errors.Join(err, l.release())
	}

	return l, nil
}

// Error describes the damage and the records it makes unreadable.
func (e *CorruptError) Error() string {
	return fmt.Sprintf("%v: %s at offset %d, records %d to %d: %v",
		ErrCorrupt, e.Path, e.Offset, e.First, e.Last, e.Err)
}

// Is makes a CorruptError match ErrCorrupt.
func (e *CorruptError) Is(target error) bool {
	return target == ErrCorrupt
}

// Unwrap returns the decode failure behind the damage.
func (e *CorruptError) Unwrap() error {
	return e.Err
}

// withDefaults fills zero fields with defaults and validates the result.
func (c Config) withDefaults() (Config, error) {
	if c.SegmentSize < 0 || c.MaxWALSize < 0 || c.MaxRecordSize < 0 {
		return c, fmt.Errorf("%w: negative value", ErrInvalidConfig)
	}

	if c.SegmentSize == 0 {
		c.SegmentSize = DefaultSegmentSize
	}

	if c.MaxRecordSize == 0 {
		c.MaxRecordSize = DefaultMaxRecordSize
	}

	if c.MaxRecordSize > formatMaxRecordLen {
		return c, fmt.Errorf("%w: MaxRecordSize %d exceeds the format limit %d",
			ErrInvalidConfig, c.MaxRecordSize, int64(formatMaxRecordLen))
	}

	if c.MaxRecordSize > c.SegmentSize {
		return c, fmt.Errorf("%w: MaxRecordSize %d exceeds SegmentSize %d",
			ErrInvalidConfig, c.MaxRecordSize, c.SegmentSize)
	}

	if c.MaxWALSize > 0 && c.MaxWALSize < c.SegmentSize {
		return c, fmt.Errorf("%w: MaxWALSize %d is below SegmentSize %d",
			ErrInvalidConfig, c.MaxWALSize, c.SegmentSize)
	}

	return c, nil
}

// Append writes data as one record and returns its index. After a crash the
// record is whole or absent, and one whose Append failed may still survive.
func (l *Log) Append(data []byte) (uint64, error) {
	size, err := l.recordSize(data)
	if err != nil {
		return 0, err
	}

	req := &appendRequest{data: data, size: size, done: make(chan struct{})}

	// The request that finds the queue empty leads: it writes everything queued
	// by the time it holds writeMu. The others wait for their leader.
	l.queueMu.Lock()
	lead := len(l.queue) == 0
	l.queue = append(l.queue, req)
	l.queueMu.Unlock()

	if lead {
		l.writeMu.Lock()

		l.queueMu.Lock()
		group := l.queue
		l.queue = nil
		l.queueMu.Unlock()

		l.writeGroup(group)
		l.writeMu.Unlock()
	}

	<-req.done

	return req.index, req.err
}

// Read returns up to limit caller-owned records from index from, which must lie
// in [FirstIndex, LastIndex+1]. Damage ends the result early, and a Read that
// starts at damage fails with a *CorruptError naming the records lost.
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
		if errors.Is(err, ErrCorrupt) && len(out) > 0 {
			return out, nil
		}

		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// Commit durably marks records up to and including index, at most LastIndex,
// as processed and deletes fully committed segments. An index at or below
// Committed only retries failed deletions.
func (l *Log) Commit(index uint64) error {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	if err := l.checkWritable(); err != nil {
		return err
	}

	if last := l.active().nextIndex() - 1; index > last {
		return fmt.Errorf("%w: %d above %d", ErrOutOfRange, index, last)
	}

	if index > l.committed {
		if index >= l.synced {
			if err := l.syncActive(); err != nil {
				return err
			}
		}

		tail := l.active()
		cp := checkpoint{committed: index, segment: tail.first, end: l.syncedEnd, next: l.synced}
		if err := l.saveCheckpoint(cp); err != nil {
			return err
		}

		l.mu.Lock()
		l.committed = index
		l.mu.Unlock()

		l.counters.commits.Add(1)
	}

	return l.reclaim()
}

// Committed returns the index of the last committed record, or 0 when none. A
// consumer resumes at Committed()+1 after Open.
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

// Size returns the bytes of all segment files, including reclaimed ones whose
// deletion failed. Index files are not counted.
func (l *Log) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.size
}

// Err returns the ErrPermanent the log has failed with, or nil while it works.
// It never waits on writes, so it suits health checks.
func (l *Log) Err() error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.failed
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

// Close makes appended records durable, so the next Open scans nothing, and
// releases the directory. After ErrPermanent it returns that error and releases
// without syncing.
func (l *Log) Close() error {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	if l.closed {
		return ErrClosed
	}

	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()

	if l.failed != nil {
		return errors.Join(l.failed, l.release())
	}

	tail := l.active()
	if err := tail.seal(); err != nil {
		return errors.Join(l.fail(err), l.release())
	}

	tail.saveIndex()

	cp := checkpoint{
		committed: l.committed,
		segment:   tail.first,
		end:       tail.size,
		next:      tail.nextIndex(),
	}

	return errors.Join(l.saveCheckpoint(cp), l.release())
}

// load rebuilds the log from its directory, reading only what follows the
// checkpoint, and cuts those records at the first damaged one.
func (l *Log) load() error {
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
			return fmt.Errorf("%w: no checkpoint but the first segment starts at %d",
				ErrCorrupt, firsts[0])
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

// active returns the segment taking appends, or nil before load.
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

// checkWritable returns the error a mutation fails with, if any.
func (l *Log) checkWritable() error {
	if l.closed {
		return ErrClosed
	}

	return l.failed
}

// fail records the first write or fsync failure as ErrPermanent. Afterwards
// what reached the disk is unknown, so every later mutation reports it until the
// log is closed and opened again.
func (l *Log) fail(err error) error {
	if l.failed == nil {
		l.mu.Lock()
		l.failed = fmt.Errorf("%w: %w", ErrPermanent, err)
		l.mu.Unlock()
	}

	return l.failed
}

// recordSize returns the encoded size of data. It fails with ErrTooLarge when
// data exceeds MaxRecordSize.
func (l *Log) recordSize(data []byte) (int64, error) {
	if int64(len(data)) > l.cfg.MaxRecordSize {
		return 0, fmt.Errorf("%w: record has %d bytes, MaxRecordSize %d",
			ErrTooLarge, len(data), l.cfg.MaxRecordSize)
	}

	return recordHeaderSize + int64(len(data)), nil
}

// makeRoom rolls to a new segment when the record overflows the active one, or
// when the active segment is fully committed and rolling it away frees the
// space the record needs. It fails with ErrFull when the record does not fit.
func (l *Log) makeRoom(size int64) error {
	tail := l.active()
	roll := tail.count > 0 && tail.size+size > l.cfg.SegmentSize

	if l.cfg.MaxWALSize > 0 {
		after := l.size + size
		if roll {
			after += segmentHeaderSize
		}

		if after > l.cfg.MaxWALSize && tail.count > 0 && tail.nextIndex() == l.committed+1 {
			roll = true
			after = l.size - tail.size + segmentHeaderSize + size
		}

		if after > l.cfg.MaxWALSize {
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

// writeGroup writes the queued records in order. Each record gets the result it
// would get if appended alone, and records that fit the active segment together
// share one write and fsync.
func (l *Log) writeGroup(group []*appendRequest) {
	var (
		pending []*appendRequest
		buf     = l.encodeBuf[:0]
	)

	for _, req := range group {
		if len(pending) > 0 && !l.fitsBehind(int64(len(buf)), req.size) {
			l.flush(pending, buf)
			pending, buf = pending[:0], buf[:0]
		}

		if req.err = l.checkWritable(); req.err != nil {
			close(req.done)

			continue
		}

		if len(pending) == 0 {
			if req.err = l.makeRoom(req.size); req.err != nil {
				close(req.done)

				continue
			}
		}

		// Pending records are not yet counted in the active segment.
		index := l.active().nextIndex() + uint64(len(pending))
		buf = appendRecord(buf, index, req.data)
		pending = append(pending, req)
	}

	l.flush(pending, buf)

	if cap(buf) <= maxKeptBuffer {
		l.encodeBuf = buf[:0]
	}
}

// fitsBehind reports whether a record fits in the active segment and MaxWALSize
// right after the pending bytes. Only then can it join the pending write, since
// makeRoom would not roll for it.
func (l *Log) fitsBehind(pending, size int64) bool {
	if l.active().size+pending+size > l.cfg.SegmentSize {
		return false
	}

	return l.cfg.MaxWALSize == 0 || l.size+pending+size <= l.cfg.MaxWALSize
}

// flush writes buf, the encoded records of pending, to the active segment,
// makes them visible to reads and completes their requests.
func (l *Log) flush(pending []*appendRequest, buf []byte) {
	if len(pending) == 0 {
		return
	}

	tail := l.active()

	if err := l.writeActive(buf); err != nil {
		for _, req := range pending {
			req.err = err
			close(req.done)
		}

		return
	}

	l.mu.Lock()

	for _, req := range pending {
		tail.addRecord(req.size)
		req.index = tail.nextIndex() - 1
	}

	l.size += int64(len(buf))
	l.mu.Unlock()

	l.counters.appends.Add(uint64(len(pending)))
	l.counters.appendedBytes.Add(uint64(len(buf)))
	l.counters.writes.Add(1)

	if l.cfg.SyncOnAppend {
		l.markSynced()
	}

	for _, req := range pending {
		close(req.done)
	}
}

// writeActive writes buf at the end of the active segment and syncs it when
// SyncOnAppend is set.
func (l *Log) writeActive(buf []byte) error {
	tail := l.active()

	if _, err := tail.file.WriteAt(buf, tail.size); err != nil {
		return l.fail(wrap(err))
	}

	if l.cfg.SyncOnAppend {
		return l.syncTail()
	}

	return nil
}

// roll seals the active segment and starts the next one. Sealing comes first,
// so every segment but the last is complete and durable.
func (l *Log) roll() error {
	tail := l.active()

	if err := tail.seal(); err != nil {
		return l.fail(err)
	}

	tail.saveIndex()

	next, err := createSegment(l.dir, tail.nextIndex(), l.cfg.SegmentSize)
	if err != nil {
		return l.fail(err)
	}

	l.mu.Lock()
	l.segments = append(l.segments, next)
	l.size += segmentHeaderSize
	l.mu.Unlock()

	l.counters.rolls.Add(1)

	l.markSynced()

	return nil
}

// reclaim deletes closed segments whose records are all committed. They are
// unlisted under mu and unlinked after it is released.
func (l *Log) reclaim() error {
	l.mu.Lock()

	n := 0
	for n < len(l.segments)-1 && l.segments[n].nextIndex() <= l.committed+1 {
		n++
	}

	l.doomed = append(l.doomed, l.segments[:n]...)
	l.segments = slices.Delete(l.segments, 0, n)
	l.mu.Unlock()

	// Reads find segments only under mu, so none can use these files now.
	for _, seg := range l.doomed {
		_ = seg.close() // sealed and synced, nothing to lose
	}

	var (
		removed int
		freed   int64
		err     error
	)

	for _, seg := range l.doomed {
		if err = removeSegment(l.dir, seg.first); err != nil {
			break
		}

		removed++
		freed += seg.size
	}

	l.doomed = slices.Delete(l.doomed, 0, removed)

	l.mu.Lock()
	l.size -= freed
	l.mu.Unlock()

	l.counters.reclaimed.Add(uint64(removed))

	return err
}

// syncActive makes the active segment durable unless it already is.
func (l *Log) syncActive() error {
	tail := l.active()
	if l.synced == tail.nextIndex() {
		return nil
	}

	if err := l.syncTail(); err != nil {
		return err
	}

	l.markSynced()

	return nil
}

// syncTail fsyncs the active segment's data and counts it in Stats.
func (l *Log) syncTail() error {
	start := time.Now()
	err := syncData(l.active().file)
	l.counters.syncNanos.Add(int64(time.Since(start)))
	l.counters.syncs.Add(1)

	if err != nil {
		return l.fail(wrap(err))
	}

	return nil
}

// markSynced records that the active segment is durable up to its end.
func (l *Log) markSynced() {
	tail := l.active()
	l.synced, l.syncedEnd = tail.nextIndex(), tail.size
}

// saveCheckpoint writes cp unless it is the checkpoint already on disk.
func (l *Log) saveCheckpoint(cp checkpoint) error {
	if cp == l.saved {
		return nil
	}

	if err := writeCheckpoint(l.dir, cp); err != nil {
		return err
	}

	l.saved = cp

	return nil
}

// release closes the segments, the active one without syncing it, and unlocks
// the directory.
func (l *Log) release() error {
	return errors.Join(closeSegments(l.segments), l.lock.Close())
}

// wrap tags an operating system error, which already names the operation and
// the path.
func wrap(err error) error {
	return fmt.Errorf("wal: %w", err)
}
