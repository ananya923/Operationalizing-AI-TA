#!/usr/bin/env bash
# Run NYC Cab Analytics — starts FastAPI backend + Vite dev server
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP="$ROOT/app"

# ── Backend ───────────────────────────────────────────────────
echo "[nyc-cab-analytics] Starting FastAPI on http://localhost:8000"
cd "$APP/backend"
"$ROOT/.venv/bin/uvicorn" main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!

# ── Frontend ──────────────────────────────────────────────────
echo "[nyc-cab-analytics] Starting Vite on http://localhost:5173"
cd "$APP/frontend"
npm run dev &
FRONTEND_PID=$!

# ── Cleanup on exit ───────────────────────────────────────────
trap "echo '[nyc-cab-analytics] Stopping...'; kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit 0" INT TERM

echo ""
echo "  Fleet view → http://localhost:5173"
echo "  Driver view → http://localhost:5173 (toggle in topbar)"
echo "  API docs   → http://localhost:8000/docs"
echo ""
echo "  Press Ctrl+C to stop both servers."
echo ""

wait
