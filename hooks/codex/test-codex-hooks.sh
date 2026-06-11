#!/usr/bin/env bash
# test-codex-hooks.sh - self-contained checks for Codex mesh hooks.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
START="$HERE/mesh-session-start.sh"
GUARD="$HERE/mesh-claim-guard.sh"
DRAIN="$HERE/mesh-inbox-drain.sh"
BASH_BIN="$(command -v bash)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

STUB="$TMP/bin"
mkdir -p "$STUB"
export MESH_STUB_LOG="$TMP/mesh-args.log"

cat >"$STUB/mesh" <<'SH'
#!/usr/bin/env bash
printf '%s | socket=%s\n' "$*" "${MESH_SOCKET:-}" >>"${MESH_STUB_LOG:-/dev/null}"
case "$1" in
join)
    exit "${MESH_STUB_JOIN:-0}" ;;
announce|who|release|leave)
    if [ "$1" = "who" ]; then
        echo "NAME   ROLE     STATE"
        echo "alpha  builder  live"
    fi
    exit 0 ;;
claim)
    path="$2"
    case "${MESH_STUB_MODE:-claimed}" in
    lost-second)
        if [ "$path" = "/tmp/demo/src/two.go" ]; then
            echo '{"result":"lost","path":"src/two.go","repo":"default","owner":"agent-bob","since":"2026-06-05T11:58:07Z"}'
            exit 6
        fi
        echo '{"result":"claimed","path":"src/one.go","repo":"default","owner":"agent-me","since":"2026-06-05T12:00:00Z"}'
        exit 0 ;;
    lost)
        echo '{"result":"lost","path":"src/one.go","repo":"default","owner":"agent-bob","since":"2026-06-05T11:58:07Z"}'
        exit 6 ;;
    buserror)
        echo '{"ok":false,"code":"unavailable","message":"bus unavailable"}'
        exit 1 ;;
    *)
        echo '{"result":"claimed","path":"src/one.go","repo":"default","owner":"agent-me","since":"2026-06-05T12:00:00Z"}'
        exit 0 ;;
    esac ;;
inbox)
    if [ -n "${MESH_STUB_INBOX+x}" ]; then
        printf '%s' "$MESH_STUB_INBOX"
    else
        printf '%s' '{"items":[{"ticket":"t-1","from":"alpha","question":"is auth fixed?","context":"PR 41"}]}'
    fi
    exit "${MESH_STUB_INBOX_RC:-0}" ;;
*)
    exit 0 ;;
esac
SH
chmod +x "$STUB/mesh"

SID="abcd1234-ef56-7890-abcd-ef1234567890"
START_JSON='{"session_id":"'"$SID"'","cwd":"/tmp/demo","hook_event_name":"SessionStart","source":"startup"}'
STOP_JSON='{"session_id":"'"$SID"'","cwd":"/tmp/demo","hook_event_name":"Stop","stop_hook_active":false}'
STOP_ACTIVE_JSON='{"session_id":"'"$SID"'","cwd":"/tmp/demo","hook_event_name":"Stop","stop_hook_active":true}'
PATCH_ONE='*** Begin Patch
*** Update File: src/one.go
@@
-old
+new
*** End Patch'
PATCH_TWO='*** Begin Patch
*** Update File: src/one.go
@@
-old
+new
*** Update File: src/two.go
@@
-old
+new
*** End Patch'
PATCH_JSON='{"session_id":"'"$SID"'","cwd":"/tmp/demo","hook_event_name":"PreToolUse","tool_name":"apply_patch","tool_input":{"command":'"$(printf '%s' "$PATCH_ONE" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')"'}}'
PATCH_TWO_JSON='{"session_id":"'"$SID"'","cwd":"/tmp/demo","hook_event_name":"PreToolUse","tool_name":"apply_patch","tool_input":{"command":'"$(printf '%s' "$PATCH_TWO" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')"'}}'
BASH_JSON='{"session_id":"'"$SID"'","cwd":"/tmp/demo","hook_event_name":"PreToolUse","tool_name":"Bash","tool_input":{"command":"echo hi"}}'

pass=0
fail=0

assert_eq() {
    if [ "$2" = "$3" ]; then
        echo "PASS  $1"; pass=$((pass + 1))
    else
        echo "FAIL  $1 (want: $2, got: $3)"; fail=$((fail + 1))
    fi
}

assert_contains() {
    if grep -qF -- "$2" "$3" 2>/dev/null; then
        echo "PASS  $1"; pass=$((pass + 1))
    else
        echo "FAIL  $1 (missing: $2 in: $(cat "$3" 2>/dev/null))"; fail=$((fail + 1))
    fi
}

assert_empty_file() {
    if [ ! -s "$2" ]; then
        echo "PASS  $1"; pass=$((pass + 1))
    else
        echo "FAIL  $1 (expected empty, got: $(cat "$2"))"; fail=$((fail + 1))
    fi
}

run() {
    local hook="$1" json="$2"; shift 2
    rc=0
    printf '%s' "$json" | env -u MESH_SOCKET PATH="$STUB:$PATH" "$@" \
        "$BASH_BIN" "$hook" >"$TMP/out" 2>"$TMP/err" || rc=$?
}

mksock() {
    mkdir -p "$(dirname "$1")"
    perl -MIO::Socket::UNIX -e 'IO::Socket::UNIX->new(Local => $ARGV[0], Listen => 1) or die $!' "$1"
}

MESH_DIR_T="$TMP/meshdir"
mksock "$MESH_DIR_T/agents/cx-abcd1234.sock"

: >"$MESH_STUB_LOG"
run "$START" "$START_JSON" MESH_DIR="$MESH_DIR_T"
assert_eq "start: exit 0" 0 "$rc"
assert_contains "start: joins as cx-<sid8>" "join --name cx-abcd1234 --role builder" "$MESH_STUB_LOG"
assert_contains "start: announces startup" "announce codex session started" "$MESH_STUB_LOG"
assert_contains "start: context names agent" 'agent "cx-abcd1234"' "$TMP/out"

: >"$MESH_STUB_LOG"
run "$START" "$START_JSON" MESH_DIR="$MESH_DIR_T" MESH_SOCKET="$MESH_DIR_T/agents/cx-abcd1234.sock"
assert_eq "start: explicit socket exit 0" 0 "$rc"
assert_contains "start: explicit socket skips join" "announce codex session started" "$MESH_STUB_LOG"
if grep -qF -- "join --name" "$MESH_STUB_LOG" 2>/dev/null; then
    echo "FAIL  start: explicit socket should not join twice"; fail=$((fail + 1))
else
    echo "PASS  start: explicit socket should not join twice"; pass=$((pass + 1))
fi

: >"$MESH_STUB_LOG"
run "$GUARD" "$PATCH_JSON" MESH_DIR="$MESH_DIR_T"
assert_eq "guard: claimed exit 0" 0 "$rc"
assert_contains "guard: claims patch target" "claim /tmp/demo/src/one.go" "$MESH_STUB_LOG"
assert_contains "guard: uses session socket" "socket=$MESH_DIR_T/agents/cx-abcd1234.sock" "$MESH_STUB_LOG"

rc=0
run "$GUARD" "$PATCH_JSON" MESH_DIR="$MESH_DIR_T" MESH_STUB_MODE=lost
assert_eq "guard: lost exit 2" 2 "$rc"
assert_contains "guard: lost names owner" "agent-bob" "$TMP/err"

: >"$MESH_STUB_LOG"
run "$GUARD" "$PATCH_TWO_JSON" MESH_DIR="$MESH_DIR_T" MESH_STUB_MODE=lost-second
assert_eq "guard: second lost exit 2" 2 "$rc"
assert_contains "guard: releases prior claim" "release /tmp/demo/src/one.go" "$MESH_STUB_LOG"

: >"$MESH_STUB_LOG"
run "$GUARD" "$BASH_JSON" MESH_DIR="$MESH_DIR_T"
assert_eq "guard: non-apply_patch exit 0" 0 "$rc"
assert_empty_file "guard: non-apply_patch no mesh" "$MESH_STUB_LOG"

run "$GUARD" "$PATCH_JSON" MESH_DIR="$MESH_DIR_T" MESH_STUB_MODE=buserror
assert_eq "guard: bus error fail-open" 0 "$rc"

run "$DRAIN" "$STOP_JSON" MESH_DIR="$MESH_DIR_T"
assert_eq "drain: exit 0" 0 "$rc"
assert_contains "drain: emits decision block" '"decision": "block"' "$TMP/out"
assert_contains "drain: reason carries ticket" "t-1 from alpha" "$TMP/out"

run "$DRAIN" "$STOP_ACTIVE_JSON" MESH_DIR="$MESH_DIR_T"
assert_eq "drain: active exit 0" 0 "$rc"
assert_empty_file "drain: active is silent JSON no-op" "$TMP/out"

run "$DRAIN" "$STOP_JSON" MESH_DIR="$MESH_DIR_T" MESH_STUB_INBOX='{"items":[]}'
assert_eq "drain: empty inbox exit 0" 0 "$rc"
assert_empty_file "drain: empty inbox silent" "$TMP/out"

echo
echo "$pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
    echo "FAIL"
    exit 1
fi
echo "PASS"
exit 0
