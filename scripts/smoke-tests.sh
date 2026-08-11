#!/bin/bash
# Post-deploy smoke tests. Checks the routes this app actually exposes
# (flight_tracker/server.py) — not a generic template:
#   - /health           aggregated liveness (Phase J), status: healthy|degraded
#   - /api/flights/{id} cache-aside lookup; 404 for an unknown id is a
#                       correct response, not a failure, so both 200 and
#                       404 count as "route reachable"
#   - /ws/{airport}     WebSocket; server sends a SNAPSHOT message
#                       immediately on connect (Phase G protocol)
set -euo pipefail

APP_URL=${1:-http://localhost:3000}
BACKEND_URL=${2:-http://localhost:8000}
AIRPORT_CODE=${3:-KJFK}

fail() { echo "✗ $1"; exit 1; }

echo "Testing frontend ($APP_URL)..."
code=$(curl -s -o /dev/null -w '%{http_code}' -m 30 "$APP_URL") || fail "frontend unreachable"
[ "$code" = "200" ] && echo "✓ frontend reachable" || fail "frontend returned HTTP $code"

echo "Testing backend health ($BACKEND_URL/health)..."
response=$(curl -s -m 30 -w '\n%{http_code}' "$BACKEND_URL/health") || fail "backend unreachable"
code=$(echo "$response" | tail -n1)
payload=$(echo "$response" | sed '$d')
[ "$code" = "200" ] || fail "/health returned HTTP $code"
status=$(echo "$payload" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status","unknown"))' 2>/dev/null || echo "unparseable")
case "$status" in
  healthy) echo "✓ backend healthy" ;;
  degraded) echo "⚠ backend degraded — check $BACKEND_URL/health/db and $BACKEND_URL/health/dlq" ;;
  *) fail "/health returned unexpected status: $status" ;;
esac

echo "Testing GET /api/flights/{flight_id}..."
code=$(curl -s -o /dev/null -w '%{http_code}' -m 30 "$BACKEND_URL/api/flights/SMOKE_TEST_PROBE") || fail "/api/flights/{id} unreachable"
case "$code" in
  200|404) echo "✓ /api/flights/{id} route reachable (HTTP $code)" ;;
  *) fail "/api/flights/{id} returned HTTP $code" ;;
esac

echo "Testing WebSocket /ws/${AIRPORT_CODE}..."
WS_URL="$(echo "$BACKEND_URL" | sed -e 's#^http://#ws://#' -e 's#^https://#wss://#')/ws/${AIRPORT_CODE}"
python3 - "$WS_URL" <<'PYEOF'
import asyncio
import json
import sys

import websockets


async def main(url: str) -> None:
    async with websockets.connect(url, open_timeout=10) as ws:
        msg = await asyncio.wait_for(ws.recv(), timeout=10)
        data = json.loads(msg)
        if data.get("type") != "SNAPSHOT":
            print(f"✗ expected SNAPSHOT as first message, got {data.get('type')}")
            sys.exit(1)
        print("✓ websocket connected and received SNAPSHOT")


try:
    asyncio.run(main(sys.argv[1]))
except Exception as e:
    print(f"✗ websocket check failed: {e!r}")
    sys.exit(1)
PYEOF

echo ""
echo "✓ All smoke tests passed"
