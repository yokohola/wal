# Future work

## Fast Open: scan only what follows the last commit

Problem: Open reads and verifies every retained record, closed segments and
the whole active segment, so startup time grows with the backlog.

Fix: trust everything up to the last Commit and scan only what follows.

- Closed segments: trusted, not read. Each is truncated and fsynced before the
  next one is created, so a crash cannot tear it. Its record count comes from
  file names (next segment's first index minus this one's).
- Active segment: Commit and Close also write its durable end (offset and next
  index) into the checkpoint. Open trusts the segment up to that point.
- Open scans only past it: nothing after a clean Close, the records appended
  since the last Commit after a crash. The scan keeps valid records, cuts a
  torn tail and finds the next index and append offset. If the log rolled
  since the last Commit, the new active segment is scanned from its start.
- Sparse indexes of trusted parts are built lazily, on first read.

Left behind: bit rot in trusted data is no longer caught by Open. It surfaces
as ErrCorrupt on the first Read that touches it, since reads verify CRCs.

## Bound record size

Problem: `Log.write` copies the whole batch into `encodeBuf`, and `Read` and
recovery hold whole records in memory. A large record costs its size several
times over.

Fix: add `Config.MaxRecordSize`, default 1 MiB, and reject larger records with
ErrTooLarge before touching disk. Large payloads live outside the WAL, with a
reference record inside it.

## Group commit

Problem: `Append` holds `writeMu` through `WriteAt` and fsync, so with
SyncOnAppend concurrent writers pay one fsync each, one after another.

Fix: writers queue their batches; one leader writes everything queued, fsyncs
once and wakes all of them. N concurrent appends cost one fsync.

## No streaming reads

Problem: `Read` returns `[]byte`, so one large record is allocated whole.

Fix: covered by MaxRecordSize. Add an `io.Reader` based read only if records
above the limit are ever required.

## Oversized segments

Problem: a batch is never split, so one large batch makes its segment far
larger than SegmentSize, and `reclaim` frees it only when fully committed.

Fix: MaxRecordSize bounds the overshoot. Optionally split a batch at a segment
roll; a crash already keeps only a prefix of a batch, so the contract holds.

## Reclaim without blocking readers

Problem: `reclaim` unlinks committed segments while holding `mu`, so a Commit
that frees many segments stalls every Read for the whole deletion.

Fix: under `mu`, only take the segments out of the list. Unlink them after
releasing `mu`, still under `writeMu`. Readers no longer wait; appends still
wait for the unlink, which keeps the change small.

## Inclusive Commit

Problem: `Commit(index)` commits records below index, so after processing 1..3
the caller must pass 4, an index that may not exist yet.

Fix: `Commit(index)` commits everything up to and including index.
`Committed()` returns the last committed index, 0 when none; a consumer resumes
at `Committed()+1`. Checkpoint, recovery rules, docs and tests follow.

## Decisions

- CRC32C stays mandatory. It is part of the format and recovery depends on it.
- Not planned: retryable ENOSPC at segment roll, a default MaxSize or retention
  independent of Commit, incremental CRC during recovery.
- No blocking Wait for consumers. Read returns empty at the end; the consumer
  decides how to wait.
