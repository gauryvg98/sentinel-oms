# Alerting

On 2026-09-03 the account halted on a false positive at 05:40 and nobody knew
for ten hours. The reason had aged out of the gateway's two-hundred-decision
ring, the host's logs did not reach back a day, and nothing was watching. A
supervisor that stops trading and waits to be noticed is not supervising
anything.

`alertd` reads the same journal as everything else and holds no state of its
own, so it can be killed and restarted freely. It is **not** one of the two
processes the container depends on: a dead alerter must not stop a working
supervisor. It does mean nobody is watching, so it says so on the way out
rather than exiting quietly.

## What pages

| condition | why it is worth waking someone |
|---|---|
| **halted**, with the reason | the account has stopped trading and will not start again by itself |
| **journal stalled** (2m) | the writer ticks about once a second. Neither trading nor halted is the worst of the three states, because no decision is written to notice it |
| **strategy silent** (15m) | a stance is declared every bar even when it wants what it already holds — which is exactly what makes silence detectable. The writer can look perfectly healthy while nothing is deciding |
| **resumed** | "it is fixed" is news, and it clears the halt so two halts half an hour apart are two events rather than one long one |

## What does not page

Replayed history. Catching up with a halt that was cleared last week must not
wake anyone, so nothing fires until the tailer has reported "nothing further"
at least once.

The exception is a halt that is *still true* at that moment: starting into an
already-halted account pages immediately, because that is precisely the case
where the first alert was missed or nobody was listening.

A condition that stays true pages once per `SENTINEL_ALERT_REPEAT` (default an
hour) rather than every time it is checked — the stall check runs every second.

## Configuration

| variable | default | meaning |
|---|---|---|
| `SENTINEL_ALERT_WEBHOOK` | unset | **set this or nothing is reported.** Any Discord or Slack incoming webhook |
| `SENTINEL_ALERT_NAME` | `sentinel` | which deployment is speaking |
| `SENTINEL_ALERT_STALL_AFTER` | `2m` | silence from the writer that counts as stopped |
| `SENTINEL_ALERT_STRATEGY_SILENT_AFTER` | `15m` | silence from the strategy that counts as dead |
| `SENTINEL_ALERT_REPEAT` | `1h` | how long a standing condition stays quiet |

The payload is `{"content": "🛑 Sentinel: ..."}` — the shape v1 sent, so an
existing webhook needs no reconfiguration.

Delivery failure only logs. The thing that tells you something broke must not
be a thing that breaks.
