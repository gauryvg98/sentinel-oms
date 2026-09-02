#!/usr/bin/env bash
# End to end, against the simulator: sentineld claims a journal, takes an order
# over the control socket, and gatewayd and the adversary read the same log.
#
# No venue, no keys, no money. What it proves is that the three processes agree
# about a file — which is the only thing they share.
set -euo pipefail
cd "$(dirname "$0")/.."

JOURNAL="${TMPDIR:-/tmp}/sentinel-smoke-$$"
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$JOURNAL"' EXIT
mkdir -p "$JOURNAL"

echo "── build ───────────────────────────────────────────────"
(cd rust && cargo build --release -q -p sentineld)
(cd go && go build -o "$JOURNAL/bin-gatewayd" ./cmd/gatewayd)
(cd go && go build -o "$JOURNAL/bin-adversary" ./cmd/adversary)

echo "── sentineld ───────────────────────────────────────────"
SENTINEL_JOURNAL="$JOURNAL" SENTINEL_INSTRUMENTS=BTCUSDT \
  rust/target/release/sentineld &
for _ in $(seq 1 50); do [ -S "$JOURNAL/control.sock" ] && break; sleep 0.1; done
[ -S "$JOURNAL/control.sock" ] || { echo "control socket never appeared"; exit 1; }

echo "── gatewayd ────────────────────────────────────────────"
SENTINEL_JOURNAL="$JOURNAL" SENTINEL_GATEWAY_ADDR=127.0.0.1:8099 \
  SENTINEL_ADMIN_TOKEN=smoke "$JOURNAL/bin-gatewayd" &
for _ in $(seq 1 50); do
  curl -fsS http://127.0.0.1:8099/health >/dev/null 2>&1 && break; sleep 0.1
done

echo "── place an order through the gateway ──────────────────"
curl -fsS -X POST http://127.0.0.1:8099/api/command \
  -H 'X-Sentinel-Token: smoke' \
  -d '{"op":"place","client_order_id":"SMOKE-1","instrument":"BTCUSDT","side":"buy","qty":"0.003","trace_id":1}'
echo

sleep 1

echo "── what the gateway sees ───────────────────────────────"
curl -fsS http://127.0.0.1:8099/api/state |
  python3 -c 'import json,sys; s=json.load(sys.stdin); print(json.dumps({"epoch":s["epoch"],"last_seq":s["last_seq"],"halted":s["halted"],"orders":[{k:o[k] for k in ("client_order_id","state","qty","filled_qty")} for o in s["orders"]]}, indent=1))'

echo "── the adversary, from outside ─────────────────────────"
"$JOURNAL/bin-adversary" "$JOURNAL"

echo "── halt, and confirm it is durable ─────────────────────"
curl -fsS -X POST http://127.0.0.1:8099/api/command \
  -H 'X-Sentinel-Token: smoke' -d '{"op":"halt","note":"smoke test"}'
echo
sleep 0.5
curl -fsS http://127.0.0.1:8099/health
echo

echo "── a second writer is refused ──────────────────────────"
# Captured first, then matched. Under `set -o pipefail` the pipeline would
# report sentineld's non-zero exit — which is the *correct* behaviour being
# tested — and the grep's success would never be seen.
second=$(SENTINEL_JOURNAL="$JOURNAL" rust/target/release/sentineld 2>&1 || true)
case "$second" in
  *"held by another writer"*) echo "refused, as it must be" ;;
  *) echo "A SECOND WRITER GOT IN:"; echo "$second"; exit 1 ;;
esac

echo
echo "all good"
