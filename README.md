# wal

[![Go Reference](https://pkg.go.dev/badge/github.com/yokohola/wal.svg)](https://pkg.go.dev/github.com/yokohola/wal)
[![Go](https://img.shields.io/badge/go-1.25%2B-00ADD8)](go.mod)
[![Dependencies](https://img.shields.io/badge/dependencies-stdlib%20only-brightgreen)](go.mod)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

Fast, crash-consistent, append-only write-ahead log for Go.

> [!NOTE]
> `wal` targets Linux for production and supports macOS for development.
> Windows is not supported.

> [!TIP]
> Keep records small (1 MiB max by default). Store large payloads in separate
> files and append a reference to them.

## Features

- **Crash-safe.** Committed records always survive a crash. Each `Append`
  writes one record, which a crash keeps whole or not at all. `Open` repairs a
  half-written tail by itself and never returns damaged data.
- **Group commit.** Concurrent `Append` calls are grouped into a single write
  and fsync, so many writers pay for one fsync instead of one each.
- **Reads never block.** Readers never wait on writes or fsyncs, and any
  number of them can read the log at the same time.
- **Automatic cleanup.** Segments are deleted once all their records are
  committed. With `MaxWALSize`, producers get `ErrFull` instead of filling the disk.
- **Linux-tuned.** Uses `fdatasync` and preallocated segments, so most appends
  need no metadata flush.
- **No dependencies.** Standard library only. `flock` stops two processes
  from opening the same directory.

## Quick start


```go
l, err := wal.Open("/var/lib/myapp/wal", wal.Config{SyncOnAppend: true})
if err != nil {
	return err
}
defer l.Close()

last, err := l.Append([]byte("set a"))
if err != nil {
	return err
}

recs, err := l.Read(l.Committed()+1, 1024) // resume where the last run stopped
if err != nil {
	return err
}
// ... process recs ...

return l.Commit(last)
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

## Configuration

| Field | Default | Meaning |
|---|---|---|
| `SegmentSize` | 64 MiB | Roll to a new segment at this size. |
| `MaxWALSize` | no cap | Total size cap; `Append` returns `ErrFull` until more is committed. |
| `MaxRecordSize` | 1 MiB | Larger records are rejected with `ErrTooLarge`. Must not exceed `SegmentSize`. |
| `SyncOnAppend` | `false` | fsync before every `Append` returns; concurrent appends share one. |
| `SyncInterval` | off | fsync in the background at this period. |

Segment roll, `Commit`, `Close` and `Sync` always fsync.

## High load

- **Many writers:** set `SyncOnAppend`. Concurrent appends share one fsync.
- **Throughput over per-call durability:** use `SyncInterval` instead of
  `SyncOnAppend`. A power loss can lose the last interval; a process crash
  cannot.
- **Reads:** read in batches (`Read(next, 1024)`). A 128-record read costs
  about six single-record reads.
- **Disk:** set `MaxWALSize` and treat `ErrFull` as backpressure.

## Performance

Apple M3 Pro, macOS, Go 1.26, 256-byte records, 64 MiB segments. Medians of
three runs; reads use a warmed 100,000-record dataset.

| Operation | wal | tidwall | RoseDB | HashiCorp |
|---|---|---|---|---|
| Single append, no sync | 1.43 µs | 1.40 µs | 1.39 µs | n/a |
| Single durable append | 2.58 ms | 2.61 ms | 2.65 ms | 1.70 ms |
| Durable appends, 16 writers | **2,993 rec/s** | 368 rec/s | 331 rec/s | 484 rec/s |
| Random single-record read | 1.45 µs | 0.098 µs | 1.74 µs | 3.35 µs |
| Sequential read, 128 records | 9.22 µs | 3.52 µs | 111 µs | 231 µs |

Group commit gives 6 to 9× the durable throughput with concurrent writers.
tidwall serves reads from an in-memory cache without checksum validation;
`wal` reads the segment files and validates every record.

## Errors

| Error | When |
|---|---|
| `ErrFull` | The log is at `MaxWALSize`; commit, then retry. |
| `ErrTooLarge` | The record exceeds `MaxRecordSize`. |
| `ErrCorrupt` | Damage a crash cannot cause. `Read` returns the records before it, then a `*CorruptError` naming the lost range; read from `Last+1` to skip it. |
| `ErrOutOfRange` | Index outside `[FirstIndex, LastIndex+1]`. |
| `ErrLocked` | Another process holds the directory. |
| `ErrClosed` | The log is closed. |

After a failed write or fsync, `Append`, `Commit` and `Sync` keep returning
that error; reopen the log to recover.

## Testing

```sh
go test -race ./...
go test -run '^$' -bench . -benchmem ./...
```
