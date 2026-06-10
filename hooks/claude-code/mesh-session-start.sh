#!/usr/bin/env bash
# mesh-session-start.sh — Claude Code SessionStart hook: join the mesh
# automatically with a session-scoped identity, so a Claude Code session
# participates in coordination with zero manual setup (no `mesh join`, no
# `export MESH_SOCKET`).
#
# Identity: the agent name is derived from Claude Code's session_id —
# "cc-" + the first 8 alphanumerics — so every session gets its own agent
# and the sidecar socket lands at a path every other hook can re-derive
# from its own stdin ($MESH_DIR/agents/cc-<sid8>.sock). Hooks run as
# separate processes, so an env var exported here could never reach the
# claim guard; the session_id in each hook's JSON is the one shared fact.
#
# Output: stdout from a SessionStart hook is added to the model's context.
# On a successful join this prints a compact block naming the session's
# mesh identity, the basic verbs, and the current roster — so the model
# knows it has teammates and how to talk to them.
#
# Knobs: MESH_ROLE (default "builder"), MESH_REPO (forwarded to join so
# claims land in the right repo namespace).
#
# Fail-open everywhere: no python3, no mesh, unparseable stdin, join
# refused, bus down → exit 0 silently. A coordination aid must never
# break session startup on a machine where the mesh is absent or broken.
set -euo pipefail

command -v python3 >/dev/null 2>&1 || exit 0
command -v mesh >/dev/null 2>&1 || exit 0

# Parse session_id from the SessionStart JSON; print the derived agent
# name. Anything unparseable prints nothing.
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

# Join (idempotent: rejoin refreshes the existing registration; the
# sidecar is autostarted when absent). Forward MESH_REPO when pinned.
rc=0
if [ -n "${MESH_REPO:-}" ]; then
    mesh join --name "$name" --role "${MESH_ROLE:-builder}" --repo "$MESH_REPO" --json >/dev/null 2>&1 || rc=$?
else
    mesh join --name "$name" --role "${MESH_ROLE:-builder}" --json >/dev/null 2>&1 || rc=$?
fi
[ "$rc" -eq 0 ] || exit 0

sock="${MESH_DIR:-$HOME/.mesh}/agents/$name.sock"

# Broadcast that this session is live. This is advisory only; if announce
# fails, startup still succeeds because join is the real participation step.
if [ -n "${MESH_REPO:-}" ]; then
    MESH_SOCKET="$sock" mesh announce "claude-code session started" --repo "$MESH_REPO" >/dev/null 2>&1 || true
else
    MESH_SOCKET="$sock" mesh announce "claude-code session started" >/dev/null 2>&1 || true
fi

# Context for the model: who am I on the mesh, and who else is here.
# Keep it compact — this lands in the session context every startup.
echo "mesh: this session is agent \"$name\" (role ${MESH_ROLE:-builder}) on the local agent mesh."
echo "mesh: claims on files you edit are taken automatically; if an edit is blocked, another agent holds that file — coordinate (mesh who / mesh ask) instead of retrying."
echo "mesh: useful commands: mesh who · mesh announce \"<intent>\" · mesh note \"<decision>\" · mesh ask --role <role> \"<q>\" · mesh poll <ticket>"
roster="$(MESH_SOCKET="$sock" mesh who 2>/dev/null)" || exit 0
if [ -n "$roster" ]; then
    echo "mesh: current roster:"
    printf '%s\n' "$roster" | sed 's/^/  /'
fi
exit 0
