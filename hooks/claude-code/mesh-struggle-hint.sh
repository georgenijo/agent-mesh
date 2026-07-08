#!/usr/bin/env bash
# mesh-struggle-hint.sh — Claude Code PostToolUse hook: when a worker's
# struggle observer has written expert guidance to $MESH_EXPERT_HINT_FILE,
# feed it to the model (decision:block) and clear the file so the same hint
# is not re-injected on every subsequent tool call.
#
# Protocol: PostToolUse can emit {"decision":"block","reason":"..."} on
# stdout; Claude Code feeds the reason back to the model before the next
# tool. Fail-open everywhere: missing env/file/python3 → exit 0 silently.
#
# This hook is optional — the worker also injects guidance via buildPrompt
# when the hint file already exists at Run start. The hook covers the
# mid-run case (hint lands while the CLI child is still streaming).
set -euo pipefail

command -v python3 >/dev/null 2>&1 || exit 0

hint="${MESH_EXPERT_HINT_FILE:-}"
[ -n "$hint" ] || exit 0
[ -f "$hint" ] || exit 0
[ -s "$hint" ] || exit 0

# Parse the hint JSON → one reason line; truncate the file after a successful
# emit so the next PostToolUse is a no-op until a new answer lands.
read -r -d '' RENDER <<'PY' || true
import json, os, sys

path = sys.argv[1]
try:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read().strip()
except OSError:
    sys.exit(0)
if not raw:
    sys.exit(0)
try:
    data = json.loads(raw)
except Exception:
    sys.exit(0)
if not isinstance(data, dict):
    sys.exit(0)
answer = data.get("answer")
if not isinstance(answer, str) or not answer.strip():
    sys.exit(0)
signal = data.get("signal") or "struggle"
by = data.get("answeredBy") or "expert"
ticket = data.get("ticket") or ""
parts = [f"Expert guidance ({signal} from {by}"]
if ticket:
    parts[0] += f", ticket {ticket}"
parts[0] += "):"
parts.append(answer.strip())
parts.append("Apply this guidance before continuing; do not repeat the same failing approach.")
reason = "\n".join(parts)
print(json.dumps({"decision": "block", "reason": reason}))
try:
    open(path, "w", encoding="utf-8").close()
except OSError:
    pass
PY

python3 -c "$RENDER" "$hint" 2>/dev/null || exit 0
exit 0
