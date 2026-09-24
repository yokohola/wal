package wal

import (
	"sync/atomic"
	"time"
)

// Stats is a snapshot of a Log for metrics. Counters are cumulative since Open;
// export them as counters and compute rates and ratios in the metrics system.
type Stats struct {
	FirstIndex uint64
	LastIndex  uint64
	Committed  uint64 // LastIndex-Committed records await the consumer
	Size       int64  // as Size reports
	Segments   int
	Failed     bool // Err would return ErrPermanent

	Appends       uint64        // records appended
	AppendedBytes uint64        // bytes written by Append, framing included
	Writes        uint64        // group writes; Appends/Writes is the batching ratio
	Syncs         uint64        // fsyncs of appended records, by SyncOnAppend, Sync or Commit
	SyncTime      time.Duration // spent in those fsyncs; SyncTime/Syncs is their mean latency
	Commits       uint64        // Commits that advanced Committed
	Rolls         uint64        // segments sealed to start a new one
	Reclaimed     uint64        // committed segments deleted
}

// counters are the cumulative Stats. Only writers holding writeMu update them,
// so the atomics are uncontended and Stats reads them without waiting.
type counters struct {
	appends       atomic.Uint64
	appendedBytes atomic.Uint64
	writes        atomic.Uint64
	syncs         atomic.Uint64
	syncNanos     atomic.Int64
	commits       atomic.Uint64
	rolls         atomic.Uint64
	reclaimed     atomic.Uint64
}

// Stats returns a snapshot of the log. It never waits on writes, so it suits
// frequent scrapes; counters and the other fields are each current but may
// straddle a concurrent write. After Close it returns the final values.
func (l *Log) Stats() Stats {
	l.mu.RLock()
	s := Stats{
		FirstIndex: l.segments[0].first,
		LastIndex:  l.active().nextIndex() - 1,
		Committed:  l.committed,
		Size:       l.size,
		Segments:   len(l.segments),
		Failed:     l.failed != nil,
	}
	l.mu.RUnlock()

	c := &l.counters
	s.Appends = c.appends.Load()
	s.AppendedBytes = c.appendedBytes.Load()
	s.Writes = c.writes.Load()
	s.Syncs = c.syncs.Load()
	s.SyncTime = time.Duration(c.syncNanos.Load())
	s.Commits = c.commits.Load()
	s.Rolls = c.rolls.Load()
	s.Reclaimed = c.reclaimed.Load()

	return s
}
