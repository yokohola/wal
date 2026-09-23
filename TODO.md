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

## Open

### ErrCorrupt granularity in trusted data (needs research)

Problem: when bytes of a trusted segment are damaged after they became durable
(bit rot, bad sector, manual edit), every Read of that segment fails with
ErrCorrupt until reopen, including intact records before and after the damage
and records appended to the active segment after Open.

To research: what a Read should return in this case.

### No streaming reads

Problem: `Read` returns `[]byte`, so one large record is allocated whole.

Note: MaxRecordSize bounds one record, not a Read. `Read(from, limit)` can
still hold `limit` × MaxRecordSize.

### Oversized segments

Problem: a batch is never split, so one large batch makes its segment far
larger than SegmentSize, and `reclaim` frees it only when fully committed.

Note: MaxRecordSize does not bound the overshoot; only MaxSize bounds a batch.
Splitting a batch at a roll would publish part of a batch before Append
succeeds, unless new segments are staged unpublished.

## Decisions

- CRC32C stays mandatory. It is part of the format and recovery depends on it.
- Not planned: retryable ENOSPC at segment roll, a default MaxSize or retention
  independent of Commit, incremental CRC during recovery.
- No blocking Wait for consumers. Read returns empty at the end; the consumer
  decides how to wait.
