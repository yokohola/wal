# wal

[![Go Reference](https://pkg.go.dev/badge/github.com/yokohola/wal.svg)](https://pkg.go.dev/github.com/yokohola/wal)
[![Go](https://img.shields.io/badge/go-1.25%2B-00ADD8)](go.mod)
[![Dependencies](https://img.shields.io/badge/dependencies-stdlib%20only-brightgreen)](go.mod)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

A crash-safe write-ahead log for Go, built for high-load applications.

> [!NOTE]
> `wal` targets Linux for production and supports macOS for development.
> Windows is not supported.

> [!TIP]
> Keep records small (1 MiB max by default). Store large payloads in separate
> files and append a reference to them.

## Features

- **Crash-safe.** Committed records always survive a crash. On restart, `Open`
  reconciles the segments with the checkpoint.
- **Built for high load.** Concurrent appends share one write and fsync;
  parallel readers never wait on writers or disk I/O.
- **Detects corruption.** Every record is checked with CRC32C. `Read` stops
    before a damaged record and returns a `*CorruptError`.
- **Automatic cleanup.** Segments are deleted once all their records are
  committed. With `MaxWALSize`, producers get `ErrFull` instead of filling the disk.
- **Linux-tuned.** Uses `fdatasync` and preallocated segments, so most appends
  need no metadata flush.
- **No dependencies.** Standard library only. `flock` stops two processes
  from opening the same directory.

## Quick start

```go
func run() (err error) {
	l, err := wal.Open("/var/lib/myapp/wal", wal.Config{SyncOnAppend: true})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, l.Close()) }()

	if _, err := l.Append([]byte("set a")); err != nil {
		return err
	}

	recs, err := l.Read(l.Committed()+1, 1024)
	if err != nil {
		return err
	}

	for _, rec := range recs {
		apply(rec.Index, rec.Data)
	}

	return l.Commit(recs[len(recs)-1].Index)
}
```

### Consumer loop

```go
func consume(ctx context.Context, l *wal.Log) error {
	for next := l.Committed() + 1; ctx.Err() == nil; {
		recs, err := l.Read(next, 1024)
		if err != nil {
			return err
		}

		if len(recs) == 0 {
			time.Sleep(10 * time.Millisecond) // caught up
			continue
		}

		for _, rec := range recs {
			apply(rec.Index, rec.Data)
		}

		last := recs[len(recs)-1].Index
		next = last + 1

		if err := l.Commit(last); err != nil {
			return err
		}
	}

	return ctx.Err()
}
```

### Sync loop

Without `SyncOnAppend`, records are durable only after `Sync` or `Commit`:

```go
func syncLoop(ctx context.Context, l *wal.Log) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := l.Sync(); err != nil {
				return err
			}
		}
	}
}
```

### Reopen

After `ErrPermanent` the log must be closed and opened again. Records not yet
synced may be lost, and a failed `Append` may still be found.

```go
// reopen replaces a log that failed with ErrPermanent. Stop every caller of the
// old log first and switch them to the new one.
func reopen(l *wal.Log) (*wal.Log, error) {
	log.Printf("wal failed, reopening: %v", l.Err())

	_ = l.Close() // returns the same error; the directory is released anyway

	return wal.Open(dir, cfg)
}
```

### Metrics

Export `Stats` to Prometheus once; every scrape reads the current log, so the
metrics follow a reopen:

```go
// registerMetrics exports Stats of the log current returns as Prometheus
// metrics.
func registerMetrics(current func() *wal.Log) {
	gauge := func(name, help string, value func(wal.Stats) float64) {
		prometheus.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{Name: name, Help: help},
			func() float64 { return value(current().Stats()) }))
	}

	gauge("wal_lag", "Records not yet committed.",
		func(s wal.Stats) float64 { return float64(s.LastIndex - s.Committed) })
	gauge("wal_committed_index", "Index of the last committed record.",
		func(s wal.Stats) float64 { return float64(s.Committed) })
	gauge("wal_last_index", "Index of the newest record.",
		func(s wal.Stats) float64 { return float64(s.LastIndex) })

	prometheus.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{Name: "wal_appends_total", Help: "Records appended."},
		func() float64 { return float64(current().Stats().Appends) }))
}
```

## Configuration

| Field | Default | Meaning |
|---|---|---|
| `SegmentSize` | 64 MiB | Roll to a new segment at this size. |
| `MaxWALSize` | no cap | Cap on total segment size (`.idx` files excluded); `Append` returns `ErrFull` until more is committed. |
| `MaxRecordSize` | 1 MiB | Larger records are rejected with `ErrTooLarge`. Must not exceed `SegmentSize`. |
| `SyncOnAppend` | `false` | fsync before every `Append` returns; concurrent appends share one. |

Segment roll, `Commit`, `Close` and `Sync` always fsync.

## Errors

| Error | When |
|---|---|
| `ErrFull` | The log is at `MaxWALSize`; commit, then retry. |
| `ErrTooLarge` | The record exceeds `MaxRecordSize`. |
| `ErrCorrupt` | Damage a crash cannot cause. `Read` returns the records before it, then a `*CorruptError` naming the lost range; read from `Last+1` to skip it. |
| `ErrOutOfRange` | Index outside `[FirstIndex, LastIndex+1]`. |
| `ErrLocked` | Another process holds the directory. |
| `ErrInvalidConfig` | `Open` got an invalid `Config`. |
| `ErrClosed` | The log is closed. |
| `ErrPermanent` | A segment write or fsync failed; `Close` and `Open` again. `Err` reports it. |

## Testing

```sh
go test -race ./...
go test -run '^$' -bench . -benchmem ./...
```
