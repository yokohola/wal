# wal

Append-only log of byte records on local disk. Records get dense, monotonic
indexes starting at 1. A consumer reads from any retained index, keeps its own
position and commits what it is done with; segments holding only committed
records are deleted.

Standard library only. Unix only: the directory lock uses `flock`.

## Usage

```go
l, err := wal.Open("/data/shard-1/wal", wal.Options{})
if err != nil {
	return err
}
defer l.Close()

last, err := l.Append(payloadA, payloadB) // indexes last-1 and last

recs, err := l.Read(l.Committed(), 1024) // resume where the last run stopped
// ... process recs ...
err = l.Commit(recs[len(recs)-1].Index + 1) // first index not yet processed
```

## Semantics

- `Append` writes a batch as consecutive records, visible to `Read` once it
  returns. A crash can keep any prefix of the batch. `ErrFull` means the batch
  does not fit `MaxSize` until more is committed; `ErrTooLarge` means it never
  will.
- `Read(from, limit)` is stateless. `from` must lie in
  `[FirstIndex, LastIndex+1]`; at `LastIndex+1` the result is empty.
- `Commit(index)` marks records below `index` as processed. It makes them and
  the checkpoint durable, then deletes closed segments below it. When the
  active segment is fully committed and space is needed, `Append` rolls it away.
  `FirstIndex <= Committed <= LastIndex+1` always holds.
- Durability: `SyncOnAppend` fsyncs every append, `SyncInterval` in the
  background, `Sync` on demand. Segment roll, `Commit` and `Close` always sync.
- After a failed write or fsync the log refuses `Append`, `Commit` and `Sync`
  with that error. Reads keep working. Reopen to recover; the failed batch may
  then be present.
- `Size` and `MaxSize` count retained bytes. On Linux the active segment is
  preallocated, so it can take up to `SegmentSize` on disk while it fills.
- One process per directory, enforced with `flock`.

## Recovery

`Open` reads every retained record. It repairs what a crash can leave and
reports anything else as `ErrCorrupt`:

- A torn tail of the active segment is truncated and the file synced. A record
  counts as torn when it runs past the end of the file or has a 512-byte sector
  that reads as zeros, which is how an unwritten sector looks after a crash.
  Any other bad record, anywhere, is corruption.
- Temp files of an interrupted segment or checkpoint write are deleted.
- Segments wholly below the checkpoint, left by a crash during reclamation, are
  deleted, gaps among them included.
- A checkpoint below the first segment or past the last record, a gap between
  segments, a renamed segment and bytes after a closed segment are corruption.

This assumes a crash leaves unwritten file ranges reading as zeros, as ext4
with `data=ordered`, xfs and apfs do. A bad record whose data is legitimately
zero across a whole sector is indistinguishable from a torn one.

## Layout

```
LOCK                       flock target
checkpoint                 committed index and CRC32C, replaced atomically
00000000000000000001.wal   segment named by its first index
00000000000000524289.wal
```

A segment is a 16-byte header (`WALS`, version, first index) followed by
records. A record is a 12-byte header (data length, CRC32C of the data, CRC32C
of those 8 bytes) and the data. The header checksum lets recovery trust a
length before reading past it.
