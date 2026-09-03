# The projection

The journal is the source of truth and it is a binary log. Nothing can run SQL
over it, so "what did I trade on Tuesday" meant replaying a quarter of a million
records. `projectord` copies the log into Postgres so the question has an
answer.

**Nothing on the order path reads it.** It can be down for an hour and the
engine will not notice. That is the whole of "no Postgres in the hot path" —
Postgres was never slow; in v1 it was load bearing, which is a different
problem.

## Where it writes

`DATABASE_URL` projects into Postgres, which a Fly deployment already has.
`SUPABASE_URL` with `SUPABASE_SERVICE_KEY` uses the REST path instead. Neither
set means no projector, which is a deployment choice rather than a fault — but
an unreachable database *is* a fault and is fatal at startup, rather than a
warning every five seconds that nobody reads.

Apply the schema by hand:

```bash
projectord schema | psql "$DATABASE_URL"
```

Printed rather than applied, because creating tables from the process that also
writes to them is how a bad migration reaches production at three in the
morning.

## Delivery

The cursor moves only *after* a batch is durable at the far end, so a crash
re-sends rather than skips. Re-sending is safe because both sinks ignore
duplicate sequences: at-least-once into an idempotent sink is exactly-once, and
the alternative is a two-phase commit with a service that does not offer one.

Batches are 500 rows with a 5-second linger, armed on the batch's *first* row
rather than refreshed by each — an account trickling one record a second would
otherwise never reach the deadline.

## The shape

The columns that get asked about are real columns; everything else stays as
`jsonb`. A schema mirroring every record variant would need migrating each time
the log learns a new one, and the log is allowed to learn.

The payload is written for a person. Marshalling the decoded record directly
produces `{"Kind":5,"To":8,"From":3}` — the whole record, and unusable, because
finding the fills first requires knowing that five is a fill and eight is
filled. So enums go in by name, the event or decision is flattened to the top
level instead of nested under whichever variant carried it, and empty fields are
dropped so a query can tell absent from zero.

```sql
-- every fill
select to_timestamp(nanos/1e9), payload->>'client_order_id',
       payload->>'qty', payload->>'price'
from journal_records
where payload->>'event' = 'FILL_APPLIED'
order by seq desc;

-- why did it stop trading
select seq, payload->>'actor', payload->>'note'
from journal_records
where payload->>'decision' = 'HALTED';

-- what the strategy wanted, bar by bar
select to_timestamp(nanos/1e9), payload->>'note'
from journal_records
where payload->>'decision' = 'STANCE_DECLARED'
order by seq desc;
```

Note `FILL_APPLIED`, not `FILL_RECEIVED`: the record kind is `FillReceived` and
its wire name is `FILL_APPLIED`. The wire name is what lands in the payload.

## Reprojecting

The journal is authoritative, so throwing the table away costs nothing but
time. Stop the projector first — a running one rewrites its cursor within the
linger and undoes the reset:

```bash
# on the machine: kill projectord, then
rm -f /data/<journal>/cursors/projectord
# on the database:
psql "$DATABASE_URL" -c 'truncate journal_records'
# then restart the machine
```
