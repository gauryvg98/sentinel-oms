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
(cd go && go build -o "$JOURNAL/bin-strategyd" ./cmd/strategyd)
(cd go && go build -o "$JOURNAL/bin-alertd" ./cmd/alertd)

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

echo "── strategyd reads the same log ────────────────────────"
# Dry run, and a bar interval short enough that the marks already in the log
# close bars immediately. What this proves is that it can read the journal the
# writer produced and reach a decision — not that the rule makes money.
# Backgrounded and killed rather than `timeout`, which macOS does not have.
SENTINEL_JOURNAL="$JOURNAL" SENTINEL_STRATEGY_INSTRUMENT=BTCUSDT \
  SENTINEL_STRATEGY_INTERVAL=1s SENTINEL_STRATEGY_FAST=2 SENTINEL_STRATEGY_SLOW=3 \
  "$JOURNAL/bin-strategyd" > "$JOURNAL/strategy.log" 2>&1 &
STRATEGY=$!
sleep 3
kill "$STRATEGY" 2>/dev/null || true
head -3 "$JOURNAL/strategy.log"
grep -q "dry run" "$JOURNAL/strategy.log" || { echo "FAILED: strategyd did not default to a dry run"; exit 1; }
grep -q "caught up with the journal" "$JOURNAL/strategy.log" || {
  echo "FAILED: strategyd never reported catching up — without that gate it"
  echo "        places a live order for every bar in the replayed history"
  exit 1
}

echo "── strategyd refuses to trade without being told ───────"
out=$(SENTINEL_JOURNAL="$JOURNAL" SENTINEL_STRATEGY_INSTRUMENT="BTCUSDT,BTCUSDC" \
  "$JOURNAL/bin-strategyd" 2>&1 || true)
case "$out" in
  *"must name one instrument"*) echo "refused a list, as it must" ;;
  *) echo "FAILED: a two-instrument list was accepted: $out"; exit 1 ;;
esac

echo "── the adversary, from outside ─────────────────────────"
"$JOURNAL/bin-adversary" "$JOURNAL"

echo "── halt, and confirm it is durable ─────────────────────"
curl -fsS -X POST http://127.0.0.1:8099/api/command \
  -H 'X-Sentinel-Token: smoke' -d '{"op":"halt","note":"smoke test"}'
echo
sleep 0.5
curl -fsS http://127.0.0.1:8099/health
echo

echo "── SIGTERM actually stops the writer ───────────────────"
# The drain is the whole reason SIGTERM is caught, and for a while it never
# ran: signal_hook sets its flag to true, the runtime reads true as "running",
# so the signal set the value it already had and the wait spun for ever. On Fly
# that means every deploy waits out the kill timeout and is SIGKILLed instead.
SENTINEL_JOURNAL="$JOURNAL/term" SENTINEL_INSTRUMENTS=BTCUSDT \
  rust/target/release/sentineld > "$JOURNAL/term.log" 2>&1 &
TERMPID=$!
for _ in $(seq 1 50); do [ -S "$JOURNAL/term/control.sock" ] && break; sleep 0.1; done
kill -TERM "$TERMPID" 2>/dev/null || true
stopped=no
for _ in $(seq 1 50); do
  kill -0 "$TERMPID" 2>/dev/null || { stopped=yes; break; }
  sleep 0.1
done
if [ "$stopped" != yes ]; then
  kill -9 "$TERMPID" 2>/dev/null || true
  echo "FAILED: sentineld ignored SIGTERM"
  exit 1
fi
grep -q "stopped" "$JOURNAL/term.log" && echo "drained and stopped, as it must"

echo "── a halt reaches the webhook ──────────────────────────"
# The account was halted two steps ago and is still halted, so an alerter
# starting now must say so rather than assume somebody saw the first one.
python3 - "$JOURNAL" <<'PYEOF' &
import sys, json, http.server
d = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        open(d + "/alerts.txt", "a").write(json.loads(body)["content"] + "\n")
        self.send_response(204); self.end_headers()
    def log_message(self, *a): pass
http.server.HTTPServer(("127.0.0.1", 8798), H).serve_forever()
PYEOF
HOOK=$!
sleep 1
SENTINEL_JOURNAL="$JOURNAL" SENTINEL_ALERT_WEBHOOK=http://127.0.0.1:8798/hook \
  SENTINEL_ALERT_NAME=smoke "$JOURNAL/bin-alertd" > "$JOURNAL/alertd.log" 2>&1 &
ALERTER=$!
sleep 3
kill "$ALERTER" "$HOOK" 2>/dev/null || true
if grep -q "HALTED" "$JOURNAL/alerts.txt" 2>/dev/null; then
  echo "paged: $(head -1 "$JOURNAL/alerts.txt")"
else
  echo "FAILED: a halted account did not reach the webhook"
  exit 1
fi

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
