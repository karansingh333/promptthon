#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  echo "Creating Python virtual environment (.venv)..."
  python3 -m venv .venv
fi

PYTHON_BIN=".venv/bin/python"
PIP_BIN=".venv/bin/pip"

if ! "$PYTHON_BIN" -c "import fastapi, uvicorn, httpx, multipart" >/dev/null 2>&1; then
  echo "Installing dependencies from requirements.txt..."
  "$PIP_BIN" install -r requirements.txt
fi

PIDS=()

cleanup() {
  echo ""
  echo "Shutting down Vault cluster..."
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait 2>/dev/null || true
  echo "All Vault processes stopped."
}

trap cleanup EXIT INT TERM

echo "Starting Vault Storage Nodes on ports 8001, 8002, 8003..."
"$PYTHON_BIN" node.py 8001 &
PIDS+=($!)

"$PYTHON_BIN" node.py 8002 &
PIDS+=($!)

"$PYTHON_BIN" node.py 8003 &
PIDS+=($!)

sleep 1

echo "Starting Vault Coordinator on http://127.0.0.1:8000 ..."
"$PYTHON_BIN" coordinator.py &
PIDS+=($!)

echo "Vault cluster is running! Open http://127.0.0.1:8000 in your browser."
echo "Press Ctrl+C to stop all services."

wait
