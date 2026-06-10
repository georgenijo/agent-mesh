#!/usr/bin/env bash
# codex-mesh.sh - optional Codex launcher that guarantees prompt leave on exit.
#
# Codex documents SessionStart and Stop hooks, but no SessionEnd hook. Project
# hooks still handle join/claim/inbox without this wrapper; use this launcher
# when you want a process-scoped mesh identity that leaves as soon as Codex
# exits instead of waiting for the presence/claim TTL backstops.
set -euo pipefail

command -v codex >/dev/null 2>&1 || {
    echo "codex-mesh: codex not found on PATH" >&2
    exit 127
}
command -v mesh >/dev/null 2>&1 || exec codex "$@"

short="$(python3 - <<'PY' 2>/dev/null || true
import os, re, uuid
seed = os.environ.get("CODEX_MESH_AGENT") or str(uuid.uuid4())
print(re.sub(r"[^A-Za-z0-9]", "", seed)[:8])
PY
)"
short="${short//$'\r'/}"
[ -n "$short" ] || short="manual"
name="${CODEX_MESH_AGENT:-cx-$short}"

if [ -n "${MESH_REPO:-}" ]; then
    mesh join --name "$name" --role "${MESH_ROLE:-builder}" --repo "$MESH_REPO" --json >/dev/null 2>&1 || exec codex "$@"
else
    mesh join --name "$name" --role "${MESH_ROLE:-builder}" --json >/dev/null 2>&1 || exec codex "$@"
fi

export MESH_SOCKET="${MESH_DIR:-$HOME/.mesh}/agents/$name.sock"
cleanup() {
    mesh leave --reason "codex session ended" >/dev/null 2>&1 || true
}
trap cleanup EXIT

codex "$@"
rc=$?
exit "$rc"
