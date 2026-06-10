#!/usr/bin/env bash
# mesh-session-end.sh — Claude Code SessionEnd hook: leave the mesh when the
# session ends. The graceful leave makes the coordinator release every claim
# this session still holds (reclaim-on-leave, P1), so locks never dangle
# until the TTL backstop after a session closes normally.
#
# Identity is re-derived from the session_id in the hook JSON, exactly as
# mesh-session-start.sh derived it — no environment handoff between hooks.
# If the derived socket does not exist, this session never joined: no-op.
#
# Fail-open: any missing prerequisite or mesh failure exits 0. If this hook
# never runs (crash, kill -9), the presence lease expires and the
# coordinator reclaims the claims anyway — this hook just makes the common
# case prompt.
set -euo pipefail

command -v python3 >/dev/null 2>&1 || exit 0
command -v mesh >/dev/null 2>&1 || exit 0

read -r -d '' PARSER <<'PY' || true
import json, re, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
sid = data.get("session_id") if isinstance(data, dict) else None
if not isinstance(sid, str):
    sys.exit(0)
short = re.sub(r"[^A-Za-z0-9]", "", sid)[:8]
if short:
    print("cc-" + short)
PY

name="$(python3 -c "$PARSER" 2>/dev/null)" || exit 0
name="${name//$'\r'/}"
[ -n "$name" ] || exit 0

sock="${MESH_DIR:-$HOME/.mesh}/agents/$name.sock"
[ -S "$sock" ] || exit 0

MESH_SOCKET="$sock" mesh leave --reason "claude-code session ended" >/dev/null 2>&1 || true
exit 0
