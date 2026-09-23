# Future work

## Done

- Fast Open: Open trusts closed segments and the active segment up to the
  durable end stored in the checkpoint, and scans only past it. The first Read
  of a segment verifies its trusted part outside the log's lock.
- Bound record size: `Config.MaxRecordSize`, default 1 MiB, ErrTooLarge.
- Group commit: queued Appends share one write and one fsync.
- Reclaim without blocking readers: unlink happens outside `mu`; failed
  deletions are retried.
- Inclusive Commit: `Commit(index)` includes index; `Committed()` is the last
  committed index, 0 when none.
- Oversized segments: opt-in `RejectBatchOnSegmentSize` rejects a batch that
  does not fit an empty segment. Without it a batch still overshoots.

## Open

### ErrCorrupt granularity in trusted data (needs research)

Problem: when bytes of a trusted segment are damaged after they became durable
(bit rot, bad sector, manual edit), every Read of that segment fails with
ErrCorrupt until reopen, including intact records before and after the damage
and records appended to the active segment after Open.

To research: what a Read should return in this case.

## Decisions

- CRC32C stays mandatory. It is part of the format and recovery depends on it.
- Not planned: retryable ENOSPC at segment roll, a default MaxWALSize or
  retention independent of Commit, incremental CRC during recovery, streaming
  reads.
- No blocking Wait for consumers. Read returns empty at the end; the consumer
  decides how to wait.
