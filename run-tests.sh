#!/usr/bin/env bash
# One command from a clean machine: venv, pinned deps, full suite.
# Requirements: Python 3.11+, Docker running (for the Postgres-backed tests;
# without Docker those tests skip cleanly and the pure suites still run).
set -euo pipefail
cd "$(dirname "$0")"

PY="${PYTHON:-python3}"
if [ ! -d .venv ]; then
  "$PY" -m venv .venv
fi
./.venv/bin/pip install --quiet --upgrade pip
if [ -f requirements-lock.txt ]; then
  ./.venv/bin/pip install --quiet -r requirements-lock.txt
fi
./.venv/bin/pip install --quiet -e ".[dev]"
exec ./.venv/bin/python -m pytest "$@"
