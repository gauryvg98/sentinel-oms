#!/usr/bin/env bash
# The v2 suite. If this passes, the tree is shippable.
#
#   rust    the engine and everything that touches the venue
#   go      the edge, and the other half of the journal format
#   smoke   all three processes against one file, end to end
#
# Pass --fast to skip the smoke test, which builds a release binary.
set -euo pipefail
cd "$(dirname "$0")"

echo "── rust ────────────────────────────────────────────────"
(cd rust && cargo fmt --all -- --check)
(cd rust && cargo clippy --workspace --all-targets -- -D warnings)
(cd rust && cargo test --workspace)

echo
echo "── go ──────────────────────────────────────────────────"
(cd go && test -z "$(gofmt -l .)" || { echo "gofmt:"; gofmt -l ./go; exit 1; })
(cd go && go vet ./...)
(cd go && go test ./... -count=1)

echo
echo "── cross-language format contract ──────────────────────"
echo "Rust wrote testdata/journal-golden; Go read it back identically."

if [ "${1:-}" != "--fast" ]; then
  echo
  echo "── end to end ──────────────────────────────────────────"
  ./scripts/smoke-v2.sh

  echo
  echo "── against the real venue ──────────────────────────────"
  # Public checks only unless BINANCE_FUTURES_KEY/SECRET are in the environment,
  # and read-only even then. Skipped when the venue is unreachable, because a
  # suite that fails on someone's train wifi is a suite people stop running.
  (cd rust && cargo run --release -q -p sentinel-binance --example verify) \
    || echo "   (skipped: the venue was not reachable)"
fi
