#!/usr/bin/env bash
# starts backend (uvicorn :8001) and frontend (next :3000) together.
# ctrl-c stops both.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
BACKEND_PORT="${BACKEND_PORT:-8001}"

cleanup() {
  kill "${BACKEND_PID:-}" "${FRONTEND_PID:-}" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

(
  cd "$ROOT/backend"
  exec uv run uvicorn app:app --app-dir src --port "$BACKEND_PORT" --reload
) &
BACKEND_PID=$!

(
  cd "$ROOT/frontend"
  exec npm run dev
) &
FRONTEND_PID=$!

echo
echo "  backend  → http://localhost:$BACKEND_PORT  (docs: /docs)"
echo "  frontend → http://localhost:3000"
echo "  ctrl-c stops both"
echo

wait
