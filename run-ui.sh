#!/usr/bin/env bash
# Sentinel paper terminal: postgres via compose, Binance testnet, :8000.
set -euo pipefail
cd "$(dirname "$0")"
docker compose up -d postgres
until docker compose exec -T postgres pg_isready -U sentinel >/dev/null 2>&1; do
  sleep 0.5
done
exec ./.venv/bin/python -m sentinel.ui
