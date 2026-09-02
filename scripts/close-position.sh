#!/usr/bin/env bash
# Close an open position through the running sentineld.
#
#   scripts/close-position.sh BTCUSDT
#
# Shows what is held, asks, places a protective exit for exactly that size, and
# then watches until the book agrees it is flat. The order is authority=exit, so
# it runs through evaluate_exit and is bounded by the reconciled position — it
# cannot close more than is actually held, whatever this script passes.
set -euo pipefail

INSTRUMENT="${1:-BTCUSDT}"
HOST="${SENTINEL_HOST:-https://sentinel-v2.fly.dev}"

if [ -z "${SENTINEL_TOKEN:-}" ]; then
  echo "set SENTINEL_TOKEN (the SENTINEL_ADMIN_TOKEN secret on the app)" >&2
  exit 1
fi

state() { curl -fsS -m 20 "$HOST/api/state"; }

read -r QTY MARK <<<"$(state | python3 -c '
import json,sys
s = json.load(sys.stdin)
want = sys.argv[1]
qty = next((p["qty"] for p in (s.get("positions") or []) if p["instrument"] == want), "0")
print(qty, (s.get("marks") or {}).get(want, "?"))
' "$INSTRUMENT")"

case "$QTY" in
  0|0.0|"") echo "$INSTRUMENT is already flat"; exit 0 ;;
esac

# Buy back a short, sell off a long.
case "$QTY" in
  -*) SIDE=buy;  SIZE="${QTY#-}" ;;
   *) SIDE=sell; SIZE="$QTY" ;;
esac

echo "$INSTRUMENT holds $QTY at mark $MARK"
echo "this will place a MARKET $SIDE of $SIZE to close it, on $HOST"
printf 'type the instrument name to confirm: '
read -r CONFIRM
[ "$CONFIRM" = "$INSTRUMENT" ] || { echo "not confirmed"; exit 1; }

COID="CLOSE-$(date +%s)"
curl -fsS -m 30 -X POST "$HOST/api/command" \
  -H "X-Sentinel-Token: $SENTINEL_TOKEN" \
  -d "{\"op\":\"place\",\"client_order_id\":\"$COID\",\"instrument\":\"$INSTRUMENT\",\"side\":\"$SIDE\",\"qty\":\"$SIZE\",\"authority\":\"exit\",\"trace_id\":$(date +%s)}"
echo " <- $COID"

# Watch until the position is gone, rather than declaring success from the ack.
for _ in $(seq 1 30); do
  sleep 2
  state | python3 -c '
import json,sys
s = json.load(sys.stdin)
want, coid = sys.argv[1], sys.argv[2]
pos = next((p for p in (s.get("positions") or []) if p["instrument"] == want), None)
order = next((o for o in (s.get("orders") or []) if o["client_order_id"] == coid), None)
print("  %-9s position %-10s realized %s" % (
    order["state"] if order else "?",
    pos["qty"] if pos else "flat",
    pos["realized"] if pos else s["realized"]))
sys.exit(0 if pos is None or pos["qty"] in ("0", "0.0") else 1)
' "$INSTRUMENT" "$COID" && { echo "closed"; exit 0; }
done

echo "still not flat after 60s — check $HOST/api/state" >&2
exit 1
