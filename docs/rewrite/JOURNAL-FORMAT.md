# The journal, on disk

The system of record. Rust writes it; Go reads it. Because two languages parse
these bytes, the format is specified here rather than inferred from whichever
implementation someone opened first, and both sides are pinned to the same
golden file.

Everything is little-endian. There is no padding and no alignment requirement:
frames are contiguous, and a reader that has consumed `len` bytes of payload is
positioned exactly at the next frame header.

## Files

```
/data/journal/
  0000000000000001.jrnl     segment; the name is base_seq in 16 hex digits
  0000000000104729.jrnl
  LOCK                      held open and flocked by the writer
  cursors/
    gatewayd                last seq this consumer has processed
    projectord
```

Segments roll at 64 MiB. The name carries the sequence of the first record in
the file, so finding the segment holding a given sequence is a directory listing
and a binary search, not a scan.

`LOCK` is the single-writer proof. One machine, one journal, one `flock` — a
second process cannot claim it, and the kernel releases it when the holder dies
whether it died cleanly or not. This is deliberately **not** a lock in a
database: the previous system halted for five days because a Postgres advisory
lock was being used to answer a question that was never distributed, and the
zombie session holding it was its own.

## Segment header — 32 bytes, at offset 0

| offset | size | field | |
|---|---|---|---|
| 0 | 8 | `magic` | `53 45 4e 54 4a 52 4e 01` — `"SENTJRN"` and a format byte |
| 8 | 4 | `version` | `1` |
| 12 | 4 | `reserved` | `0` |
| 16 | 8 | `base_seq` | sequence of the first record in this segment |
| 24 | 8 | `created_nanos` | wall clock at creation — the one clock read in the whole write path, and it is metadata, never input |

## Record frame — 36-byte header, then payload

| offset | size | field | |
|---|---|---|---|
| 0 | 4 | `len` | payload bytes; at most 1 MiB |
| 4 | 4 | `crc32c` | Castagnoli over bytes `[8, 36+len)` — the whole frame after the checksum itself |
| 8 | 8 | `seq` | monotonic from 1, gap-free across the whole journal |
| 16 | 8 | `epoch` | which run of the writer produced this |
| 24 | 8 | `nanos` | when it was appended |
| 32 | 2 | `kind` | record kind; never 0 |
| 34 | 2 | `flags` | reserved, 0 |
| 36 | `len` | `payload` | kind-specific |

The checksum covers the header as well as the payload. A frame whose `len` was
half-written is caught by the length check; a frame whose `seq` was corrupted in
place is caught by the CRC.

## Record kinds

| kind | name | payload |
|---|---|---|
| 1 | `EpochClaimed` | the writer took the journal; carries the new epoch and the sequence it resumed from |
| 2 | `IntentPersisted` | an authorised order, durable before the venue sees it (R1.1) |
| 3 | `OrderEvent` | a lifecycle transition |
| 4 | `Decision` | why something happened, or did not |
| 5 | `Mark` | a price observation |
| 6 | `Balance` | an account balance snapshot |
| 7 | `Ticked` | time entering the engine |

Kind `0` is not valid, so a frame whose kind byte was never written is refused
rather than decoded as whichever record happened to be listed first.

## Torn tails

A crash mid-append leaves a partial frame. On claim, the writer scans forward
from the last segment header and stops at the first frame that is:

- shorter than 36 bytes,
- claiming a `len` that runs past the end of the file, or
- failing its CRC.

Everything from that offset is truncated. This is the only case in which the
journal is ever shortened, and it can only remove records that were never
flushed — so no record that was acknowledged is lost.

A reader that hits the same condition stops and reports a torn tail. It never
skips forward hunting for the next plausible frame: a log with a hole in it is
not a log, and continuing past one silently is how a replay diverges from the
run it is supposed to reproduce.

## Visibility

A record is visible to readers only after `flush`, because `append` writes to a
buffer and `flush` is what reaches the disk. That ordering is the guarantee:

> No order is acknowledged before its events are durable.

The engine drains everything queued, applies each command to memory, and flushes
the whole group with one `fsync`. Cost per record therefore falls as load rises,
which is the property the previous system did not have — it paid a Postgres
round trip per state transition and got slower under exactly the conditions
where it mattered.

## Cursors

One file per consumer under `cursors/`, holding the last sequence that consumer
has finished with, as ASCII decimal with a trailing newline. Written by rename,
so a cursor file is never half-updated.

A consumer that has lost its cursor starts from the beginning. That is correct
rather than merely tolerable: every consumer of this log is building a
projection, and a projection is a fold that can always be re-run.

## Version 2: `applied`

Version 1 carried `from_state` and `to_state` and claimed that `to == from`
meant "recorded as evidence, applied to nothing". That claim was false, and the
case it was false for is the one that matters: a fill booked while an order is
reconciling moves the position and leaves the state at `Reconciling`, and an
adopted position is booked exactly that way. A consumer reading the states
under-reported exposure — silently, in a display, about money — while the engine
had it right.

Version 2 adds one byte after `to_state`: `applied`, `1` or `0`, written by the
side that knows. `to_state` is still there and still checked, but it is no
longer asked a question it cannot answer.

There is no migration. A version 1 segment is refused rather than guessed at,
because the one thing that cannot be recovered from a v1 record is precisely the
bit this adds.
