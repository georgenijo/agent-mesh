#!/usr/bin/env bash
# test-claim-guard.sh — self-contained checks for mesh-claim-guard.sh.
#
# Stubs a fake `mesh` on PATH (canned --json output + exit code, driven by
# $MESH_STUB_MODE; argv logged so tests can assert what the hook ran), pipes
# sample PreToolUse JSON through the hook, and asserts the exit-code
# contract. Plain bash asserts, no framework; exits non-zero on any failure.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
HOOK="$HERE/mesh-claim-guard.sh"
# Resolve bash before tests restrict PATH (the missing-python3 case).
BASH_BIN="$(command -v bash)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

STUB="$TMP/bin"
mkdir -p "$STUB"
export MESH_STUB_LOG="$TMP/mesh-args.log"

cat >"$STUB/mesh" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${MESH_STUB_LOG:-/dev/null}"
case "${MESH_STUB_MODE:-claimed}" in
claimed)
    echo '{"result":"claimed","path":"/tmp/demo/main.go","repo":"default","owner":"agent-me","since":"2026-06-05T12:00:00Z"}'
    exit 0 ;;
lost)
    echo '{"result":"lost","path":"/tmp/demo/main.go","repo":"default","owner":"agent-bob","since":"2026-06-05T11:58:07Z"}'
    exit 6 ;;
notjoined)
    echo '{"ok":false,"code":"not_joined","message":"not joined: no sidecar socket"}'
    exit 5 ;;
buserror)
    echo '{"ok":false,"code":"unavailable","message":"bus unavailable"}'
    exit 1 ;;
esac
SH
chmod +x "$STUB/mesh"

EDIT_JSON='{"tool_name":"Edit","tool_input":{"file_path":"/tmp/demo/main.go","old_string":"a","new_string":"b"},"cwd":"/tmp/demo"}'
WRITE_JSON='{"tool_name":"Write","tool_input":{"file_path":"/tmp/demo/new.go","content":"package demo"},"cwd":"/tmp/demo"}'
NOTEBOOK_JSON='{"tool_name":"NotebookEdit","tool_input":{"notebook_path":"/tmp/demo/nb.ipynb","new_source":"x"},"cwd":"/tmp/demo"}'
REL_JSON='{"tool_name":"Edit","tool_input":{"file_path":"sub/rel.go","old_string":"a","new_string":"b"},"cwd":"/tmp/demo"}'
BASH_JSON='{"tool_name":"Bash","tool_input":{"command":"ls"},"cwd":"/tmp/demo"}'

pass=0
fail=0

assert_eq() { # <name> <want> <got>
    if [ "$2" = "$3" ]; then
        echo "PASS  $1"
        pass=$((pass + 1))
    else
        echo "FAIL  $1 (want: $2, got: $3)"
        fail=$((fail + 1))
    fi
}

assert_contains() { # <name> <needle> <file>
    if grep -q -- "$2" "$3" 2>/dev/null; then
        echo "PASS  $1"
        pass=$((pass + 1))
    else
        echo "FAIL  $1 (missing: $2 in: $(cat "$3" 2>/dev/null))"
        fail=$((fail + 1))
    fi
}

# run <stub-mode> <json>: hook with stub mesh + real python3; sets $rc,
# stderr in $TMP/err, stdout in $TMP/out. MESH_SOCKET is set so the hook
# identifies its agent (the no-socket no-op is exercised separately below).
run() {
    rc=0
    printf '%s' "$2" | env PATH="$STUB:$PATH" MESH_STUB_MODE="$1" MESH_SOCKET="$TMP/agent.sock" \
        "$BASH_BIN" "$HOOK" >"$TMP/out" 2>"$TMP/err" || rc=$?
}

# --- claimed → allow ---------------------------------------------------------
run claimed "$EDIT_JSON"
assert_eq "claimed -> exit 0" 0 "$rc"
assert_eq "claimed -> silent stderr" "" "$(cat "$TMP/err")"

run claimed "$WRITE_JSON"
assert_eq "Write claimed -> exit 0" 0 "$rc"

# --- lost → block with owner message -----------------------------------------
run lost "$EDIT_JSON"
assert_eq "lost -> exit 2" 2 "$rc"
assert_contains "lost -> names owner" "agent-bob" "$TMP/err"
assert_contains "lost -> names path" "/tmp/demo/main.go" "$TMP/err"
assert_contains "lost -> names since" "2026-06-05T11:58:07Z" "$TMP/err"
assert_contains "lost -> coordination hint" "mesh who / mesh release" "$TMP/err"

# --- NotebookEdit claims notebook_path ----------------------------------------
run lost "$NOTEBOOK_JSON"
assert_eq "NotebookEdit lost -> exit 2" 2 "$rc"
assert_contains "NotebookEdit -> uses notebook_path" "/tmp/demo/nb.ipynb" "$TMP/err"

# --- not joined → silent no-op ------------------------------------------------
run notjoined "$EDIT_JSON"
assert_eq "not-joined -> exit 0" 0 "$rc"
assert_eq "not-joined -> silent stderr" "" "$(cat "$TMP/err")"

# --- other mesh failure → fail-open -------------------------------------------
run buserror "$EDIT_JSON"
assert_eq "bus error -> exit 0 (fail-open)" 0 "$rc"

# --- non-edit tool → no-op, mesh never invoked --------------------------------
: >"$MESH_STUB_LOG"
run claimed "$BASH_JSON"
assert_eq "non-edit tool -> exit 0" 0 "$rc"
assert_eq "non-edit tool -> mesh not invoked" "" "$(cat "$MESH_STUB_LOG")"

# --- relative path resolved against cwd ---------------------------------------
: >"$MESH_STUB_LOG"
run claimed "$REL_JSON"
assert_eq "relative path -> exit 0" 0 "$rc"
assert_contains "relative path -> joined with cwd" "/tmp/demo/sub/rel.go" "$MESH_STUB_LOG"

# --- MESH_REPO forwarded as --repo ---------------------------------------------
: >"$MESH_STUB_LOG"
rc=0
printf '%s' "$EDIT_JSON" | env PATH="$STUB:$PATH" MESH_STUB_MODE=claimed MESH_REPO=myrepo MESH_SOCKET="$TMP/agent.sock" \
    "$BASH_BIN" "$HOOK" >"$TMP/out" 2>"$TMP/err" || rc=$?
assert_eq "MESH_REPO -> exit 0" 0 "$rc"
assert_contains "MESH_REPO -> --repo forwarded" "--repo myrepo" "$MESH_STUB_LOG"

# --- no MESH_SOCKET → no-op, mesh never invoked (cannot identify the agent) ----
: >"$MESH_STUB_LOG"
rc=0
printf '%s' "$EDIT_JSON" | env -u MESH_SOCKET PATH="$STUB:$PATH" MESH_STUB_MODE=lost \
    "$BASH_BIN" "$HOOK" >"$TMP/out" 2>"$TMP/err" || rc=$?
assert_eq "no MESH_SOCKET -> exit 0" 0 "$rc"
assert_eq "no MESH_SOCKET -> mesh not invoked" "" "$(cat "$MESH_STUB_LOG")"

# --- garbage stdin → fail-open --------------------------------------------------
rc=0
printf 'not json at all' | env PATH="$STUB:$PATH" MESH_STUB_MODE=claimed \
    "$BASH_BIN" "$HOOK" >"$TMP/out" 2>"$TMP/err" || rc=$?
assert_eq "garbage stdin -> exit 0" 0 "$rc"

# --- missing python3 (PATH holds only the mesh stub) → fail-open ----------------
: >"$MESH_STUB_LOG"
rc=0
printf '%s' "$EDIT_JSON" | env PATH="$STUB" \
    "$BASH_BIN" "$HOOK" >"$TMP/out" 2>"$TMP/err" || rc=$?
assert_eq "missing python3 -> exit 0" 0 "$rc"
assert_eq "missing python3 -> mesh not invoked" "" "$(cat "$MESH_STUB_LOG")"

# --- summary --------------------------------------------------------------------
echo
echo "$pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
    echo "FAIL"
    exit 1
fi
echo "PASS"
exit 0
