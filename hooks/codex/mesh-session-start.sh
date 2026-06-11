#!/usr/bin/env bash
# mesh-session-start.sh - Codex SessionStart hook: join the mesh with a
# Codex-scoped identity and inject compact mesh context.
#
# Codex includes a stable session_id in hook JSON. Hooks run as separate
# processes, so each hook re-derives "cx-<sid8>" from stdin instead of relying
# on environment handoff. Explicit MESH_SOCKET is still honored by the other
# hooks for wrapper-driven sessions.
set -euo pipefail

command -v mesh >/dev/null 2>&1 || exit 0

if [ -n "${MESH_SOCKET:-}" ] && [ -S "$MESH_SOCKET" ]; then
    name="$(basename "$MESH_SOCKET" .sock)"
    sock="$MESH_SOCKET"
else
    command -v python3 >/dev/null 2>&1 || exit 0

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
    print("cx-" + short)
PY

    name="$(python3 -c "$PARSER" 2>/dev/null)" || exit 0
    name="${name//$'\r'/}"
    [ -n "$name" ] || exit 0

    if [ -n "${MESH_REPO:-}" ]; then
        mesh join --name "$name" --role "${MESH_ROLE:-builder}" --repo "$MESH_REPO" --json >/dev/null 2>&1 || exit 0
    else
        mesh join --name "$name" --role "${MESH_ROLE:-builder}" --json >/dev/null 2>&1 || exit 0
    fi
    sock="${MESH_DIR:-$HOME/.mesh}/agents/$name.sock"
fi

if [ -n "${MESH_REPO:-}" ]; then
    MESH_SOCKET="$sock" mesh announce "codex session started" --repo "$MESH_REPO" >/dev/null 2>&1 || true
else
    MESH_SOCKET="$sock" mesh announce "codex session started" >/dev/null 2>&1 || true
fi

echo "mesh: this Codex session is agent \"$name\" (role ${MESH_ROLE:-builder}) on the local agent mesh."
echo "mesh: apply_patch edits are claimed automatically; if a patch is blocked, another agent holds that file - coordinate before retrying."
echo "mesh: useful commands: mesh who · mesh announce \"<intent>\" · mesh note \"<decision>\" · mesh ask --role <role> \"<q>\" · mesh poll <ticket>"
roster="$(MESH_SOCKET="$sock" mesh who 2>/dev/null)" || exit 0
if [ -n "$roster" ]; then
    echo "mesh: current roster:"
    printf '%s\n' "$roster" | sed 's/^/  /'
fi
exit 0
