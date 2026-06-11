#!/usr/bin/env bash
# mesh-claim-guard.sh - Codex PreToolUse hook for apply_patch.
#
# Codex reports file edits through the canonical apply_patch tool and passes
# the patch as tool_input.command. This guard extracts target paths from the
# patch grammar, takes mesh claims before the patch runs, and blocks only when
# another agent already owns a target. Everything else fails open.
set -euo pipefail

command -v python3 >/dev/null 2>&1 || exit 0
command -v mesh >/dev/null 2>&1 || exit 0

read -r -d '' PARSER <<'PY' || true
import json, os, posixpath, re, sys

mode = sys.argv[1] if len(sys.argv) > 1 else "pre"
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
if not isinstance(data, dict):
    sys.exit(0)

def join_path(cwd, path):
    if os.path.isabs(path) or not isinstance(cwd, str) or not cwd:
        return path
    if "\\" in cwd or re.match(r"^[A-Za-z]:[\\/]", cwd):
        return os.path.join(cwd, path)
    return posixpath.join(cwd, path)

if mode == "pre":
    if data.get("tool_name") != "apply_patch":
        sys.exit(0)
    tool_input = data.get("tool_input")
    if not isinstance(tool_input, dict):
        sys.exit(0)
    command = tool_input.get("command")
    if not isinstance(command, str):
        sys.exit(0)
    cwd = data.get("cwd")
    paths = []
    for line in command.splitlines():
        for prefix in ("*** Add File: ", "*** Update File: ", "*** Delete File: ", "*** Move to: "):
            if line.startswith(prefix):
                path = line[len(prefix):].strip()
                if path and path not in paths:
                    paths.append(path)
    if not paths:
        sys.exit(0)
    sid = data.get("session_id")
    short = re.sub(r"[^A-Za-z0-9]", "", sid)[:8] if isinstance(sid, str) else ""
    print("cx-" + short if short else "")
    for path in paths:
        print(join_path(cwd, path))
else:
    owner = data.get("owner") or "unknown"
    since = data.get("since") or "unknown"
    print("%s\t%s" % (owner, since))
PY

parsed="$(python3 -c "$PARSER" pre 2>/dev/null)" || exit 0
parsed="${parsed//$'\r'/}"
[ -n "$parsed" ] || exit 0
agent="${parsed%%$'\n'*}"
paths="${parsed#*$'\n'}"
[ -n "$paths" ] || exit 0

if [ -z "${MESH_SOCKET:-}" ]; then
    [ -n "$agent" ] || exit 0
    derived="${MESH_DIR:-$HOME/.mesh}/agents/$agent.sock"
    [ -S "$derived" ] || exit 0
    export MESH_SOCKET="$derived"
fi

claimed=""
while IFS= read -r path; do
    [ -n "$path" ] || continue
    rc=0
    if [ -n "${MESH_REPO:-}" ]; then
        out="$(mesh claim "$path" --repo "$MESH_REPO" --json 2>/dev/null)" || rc=$?
    else
        out="$(mesh claim "$path" --json 2>/dev/null)" || rc=$?
    fi
    case "$rc" in
    0)
        claimed="${claimed}${path}"$'\n'
        ;;
    6)
        owner="unknown"
        since="unknown"
        if parsed_lost="$(printf '%s' "$out" | python3 -c "$PARSER" lost 2>/dev/null)" && [ -n "$parsed_lost" ]; then
            parsed_lost="${parsed_lost//$'\r'/}"
            owner="${parsed_lost%%$'\t'*}"
            since="${parsed_lost#*$'\t'}"
        fi
        while IFS= read -r held; do
            [ -n "$held" ] || continue
            if [ -n "${MESH_REPO:-}" ]; then
                mesh release "$held" --repo "$MESH_REPO" >/dev/null 2>&1 || true
            else
                mesh release "$held" >/dev/null 2>&1 || true
            fi
        done <<<"$claimed"
        printf 'mesh: %s is claimed by %s since %s - coordinate before editing (mesh who / mesh ask)\n' \
            "$path" "$owner" "$since" >&2
        exit 2
        ;;
    *)
        exit 0
        ;;
    esac
done <<<"$paths"

exit 0
