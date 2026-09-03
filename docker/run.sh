#!/bin/sh
# Start the three processes, writer first.
#
# sentineld claims the journal and opens its control socket only after the book
# is rebuilt and sound, so the other two start into a world that is already
# consistent. If either the writer or the gateway dies, the container dies with
# it — a gateway serving a projection nothing is updating is a screen that has
# quietly stopped being true, and a writer with nothing watching it is a
# deployment nobody can see.
#
# POSIX sh throughout, and no `wait -n`: this image's /bin/sh is dash, where
# that is an "Illegal option" and the script exits the moment it is reached.
set -eu

JOURNAL="${SENTINEL_JOURNAL:-/data/journal}"
mkdir -p "$JOURNAL"

/app/sentineld &
WRITER=$!

# Wait for the control socket, which is the writer saying it is ready. Bounded:
# a writer that has not appeared in thirty seconds is not going to, and hanging
# here would show up as a container that never becomes healthy and never says
# why.
i=0
while [ ! -S "$JOURNAL/control.sock" ]; do
  if ! kill -0 "$WRITER" 2>/dev/null; then
    echo "run.sh: the writer exited before it was ready" >&2
    exit 1
  fi
  i=$((i + 1))
  if [ "$i" -gt 300 ]; then
    echo "run.sh: the writer never opened its control socket" >&2
    kill "$WRITER" 2>/dev/null || true
    exit 1
  fi
  sleep 0.1
done

/app/gatewayd &
GATEWAY=$!

# The projector is optional: with no database configured it would exit
# immediately, and that is a deployment choice rather than a fault. It is also
# not one of the two that matter — the journal is the source of truth, and a
# projection falling behind is a reporting problem, not a trading one.
if [ -n "${DATABASE_URL:-}" ] || [ -n "${SUPABASE_URL:-}" ]; then
  /app/projectord &
fi

# The strategy is optional too, and is not one of the two that matter: if it
# dies the container keeps running, because a supervisor that has lost its
# opinion about what to trade is still the thing holding the position and still
# has to be reachable. Absent SENTINEL_STRATEGY it is simply not started.
if [ -n "${SENTINEL_STRATEGY:-}" ]; then
  /app/strategyd &
fi

# Stop when either of the two that matter stops. Polled rather than `wait -n`,
# which dash does not have.
term() {
  kill "$WRITER" "$GATEWAY" 2>/dev/null || true
  exit 0
}
trap term TERM INT

while kill -0 "$WRITER" 2>/dev/null && kill -0 "$GATEWAY" 2>/dev/null; do
  sleep 1
done

echo "run.sh: a required process exited; stopping the rest" >&2
kill "$WRITER" "$GATEWAY" 2>/dev/null || true
wait "$WRITER" 2>/dev/null || true
exit 1
