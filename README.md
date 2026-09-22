# wal

Append-only log of byte records on local disk. Records get dense, monotonic
indexes. A consumer reads from any retained index, keeps its own position and
commits what it is done with; committed segments are deleted.

Zero dependencies outside the standard library. Unix only: the directory
lock uses `flock`.

## Usage

```go
l, err := wal.Open("/data/shard-1/wal", wal.Options{})
if err != nil {
	return err
}
defer l.Close()

last, err := l.Append(payloadA, payloadB) // indexes last-1 and last

recs, err := l.Read(l.Committed(), 1024)  // resume where the last run stopped
// ... process recs ...
err = l.Commit(recs[len(recs)-1].Index + 1) // durable; frees segments below
```

## Semantics

- `Append` writes a batch as consecutive records, all or nothing. It returns
  `ErrFull` when `Options.MaxSize` would be exceeded, so the caller decides
  whether to block, drop or degrade.
- `Read(from, limit)` is stateless. `from` must lie in
  `[FirstIndex, LastIndex+1]`; reading at `LastIndex+1` returns an empty slice.
- `Commit(index)` persists the checkpoint first, then unlinks closed segments
  whose records are all below it. The active segment is never unlinked.
  `FirstIndex <= Committed <= LastIndex+1` always holds.
- Durability is opt-in: `SyncOnAppend` fsyncs every append, `SyncInterval`
  fsyncs in the background, `Sync` does it on demand. Closed segments, the
  checkpoint and `Close` are always fsynced.
- `Open` cuts a torn record at the end of the active segment, which a crash
  can leave. Any other damage is `ErrCorrupt`. A checkpoint past the last
  record, which an unsynced tail can leave behind, is honored: the log
  continues after it and no index is reused.
- After a failed write or fsync the log refuses further `Append`, `Sync` and
  `Commit` with that error. Reads keep working. Reopen to recover.
- One process per directory, enforced with `flock`.

## Layout

```
LOCK                       flock target
checkpoint                 committed index, CRC32C; replaced atomically
00000000000000000001.wal   segment named by its first index
00000000000000524289.wal
```

A segment is an 8-byte header (`WAL1`, version) followed by records. A record
is a little-endian length, a CRC32C over that length and the data, and the
data. The CRC covers the length so preallocated zeros never parse as a record.
