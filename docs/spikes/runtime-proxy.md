# Runtime proxy spike findings

Resolves GitHub issue #32. Scope is the runtime primitive only: a resident local
Claude Code process over stream-json, plus a tiny local API shim. This is not
production Agent Mesh.

Run date: 2026-06-05. Environment: macOS, `claude` 2.1.165, subscription/OAuth
auth (`apiKeySource: "none"`), default model `claude-opus-4-8[1m]`.

Harness: `docs/spikes/runtime_proxy_spike.py`.

---

## Verdict: GO

One resident `claude -p --input-format stream-json --output-format stream-json
--verbose` process can:

- keep stdin open after a first result;
- accept a second user message more than 30 seconds later;
- emit structured JSON events/results for both turns;
- preserve the same Claude session id across both turns;
- retain conversation state across the gap;
- surface `SIGKILL` as a typed runtime error instead of hanging.

This is enough to proceed with #26/#27 using a resident stream-json process as
the warm expert/session primitive. Recovery should still be built around
`--resume <session-id>` plus blackboard replay/checkpoint summaries, because a
crashed process loses RAM-only context since the last durable checkpoint.

---

## Command contract

Started command:

```sh
claude -p --input-format stream-json --output-format stream-json --verbose
```

Accepted stdin line shape:

```json
{"type":"user","message":{"role":"user","content":"<prompt text>"}}
```

Observed invalid shapes:

- `{"type":"user","content":"..."}` fails with a parse error because
  `message.role` is missing.
- `{"type":"user","message":{"content":"..."}}` fails because the message role
  is missing.

Adapter rule: write exactly one JSON object per line, flush after each line, and
use `result` events as turn-completion markers. Do not infer success from
assistant text alone.

---

## Proof run

Raw proof output was written to `/tmp/agent-mesh-runtime-proxy-proof.json`.

| item | observed |
|---|---|
| local session id | `local-372a3edc-9558-402f-8e87-10dd7625af69` |
| child pid | `18889` |
| Claude session id | `4f88f203-78f2-4dd1-be69-361f1745f7ea` |
| started | `2026-06-05T21:03:28Z` |
| turn 1 result | `2026-06-05T21:03:32Z` |
| turn 2 sent | `2026-06-05T21:04:02Z` |
| measured delay | `30.005s` |
| turn 2 result | `2026-06-05T21:04:09Z` |
| same OS process | yes, pid `18889` |
| same Claude session | yes, `4f88f203-78f2-4dd1-be69-361f1745f7ea` |
| state retained | yes, turn 2 repeated the nonce from turn 1 |
| turn 2 cache read | `19054` input tokens |

Turn 1 response:

```text
Nonce locked: runtime-proxy-e66e9bf3-7e90-4117-91e5-7a8a1a162818.
```

Turn 2 response after the 30 second hold:

```text
Nonce: runtime-proxy-e66e9bf3-7e90-4117-91e5-7a8a1a162818. Retained from same conversation, turn 1.
```

Important nuance: `result.num_turns` was `1` for both streamed user messages.
Do not use `num_turns` as the resident-session reuse proof. The reliable proof
is same child pid, same `session_id`, successful turn-2 cache read, and recalled
conversation state.

---

## Observed event shapes

The stream emitted newline-delimited JSON objects. These types were observed:

- `system/hook_started` and `system/hook_response`: startup hooks/plugins.
- `system/init`: includes `session_id`, `cwd`, `tools`, `model`,
  `apiKeySource`, `permissionMode`, and `claude_code_version`. An `init` event
  appeared at process/session startup and again before turn 2.
- `assistant`: assistant message payload with content blocks.
- `rate_limit_event`: status was `allowed`; includes reset/rate-limit metadata.
- `result/success`: terminal turn result with `result`, `session_id`,
  `duration_ms`, `duration_api_ms`, `usage`, `modelUsage`, `api_error_status`,
  `permission_denials`, `stop_reason`, and `terminal_reason`.

The adapter should treat any of these as typed events and preserve unknown
fields. Map `is_error: true`, `subtype != "success"`, or
`api_error_status != null` to a typed error result.

---

## Local API shape

The spike exposes the issue #32 API shape via the standard-library HTTP shim:

```sh
python3 docs/spikes/runtime_proxy_spike.py serve --host 127.0.0.1 --port 8766
```

Endpoints:

- `POST /sessions` starts a resident child process and returns metadata:
  local session id, child pid, Claude session id once seen, timestamps, status,
  last error, checkpoint summary, command, and cwd.
- `POST /sessions/:id/messages` writes one stream-json user message to held
  stdin and returns `202` plus a local turn id.
- `GET /sessions/:id/events?since=N` returns cached runtime/Claude events.
- `DELETE /sessions/:id` terminates the child and returns final metadata.

The shim was smoke-tested with a fake stream-json child:

```text
create 201
message 202
events 200 [runtime:session_started, runtime:message_sent, system:init, result:success]
delete 200
```

This is intentionally in-memory. Production needs durable event append, auth,
bounded history, backpressure, and a real async turn queue.

---

## Live UI

The same shim serves a visual console at `/`:

```sh
python3 docs/spikes/runtime_proxy_spike.py serve --host 127.0.0.1 --port 8767
open http://127.0.0.1:8767/
```

The UI can start the resident Claude process, send a custom message, run the
30-second two-turn proof, tail structured events, and trigger the typed
`SIGKILL` crash path. The proof timeline intentionally leaves the child running
after turn 2 so the resident process can be inspected before crash testing.

---

## Crash detection

After turn 2, the harness sent `SIGKILL` to pid `18889`.

Typed crash event:

```json
{
  "type": "runtime_error",
  "subtype": "child_exit",
  "code": "child_crashed",
  "pid": 18889,
  "exit_code": -9,
  "signal": "SIGKILL"
}
```

A post-crash send failed immediately with a second typed error:

```json
{
  "type": "runtime_error",
  "subtype": "send_failed",
  "code": "process_not_running",
  "exit_code": -9
}
```

No hung request was observed.

---

## Failure modes and recovery recommendation

Failure modes to handle next:

- child exits before a `result` event;
- stdout closes while stdin is still writable from the parent perspective;
- stdin write raises `BrokenPipeError`;
- Claude emits structured non-success results;
- startup hooks/plugins inject noisy `system` events;
- long-running sessions need bounded event history and periodic checkpoints;
- a resident process can keep RAM context, but crash recovery still depends on
  persisted session metadata and durable blackboard context.

Recommended recovery path:

1. Persist local session metadata: local id, pid, command, cwd, Claude
   `session_id`, last result/checkpoint summary, last durable blackboard offset.
2. On crash, mark the current session `crashed` with the typed child-exit event.
3. Spawn a replacement process with `--resume <claude_session_id>` when usable.
4. Inject the latest blackboard/context summary before accepting new work.
5. Re-open any in-flight ask/ticket as retryable unless its final answer was
   already durably committed.

---

## Where this should live next

Next implementation should be a small runtime supervisor layer, not the full
mesh core. Suggested shape:

- code organization: `internal/agentruntime` for the process/session adapter
  and typed event model;
- process boundary: start embedded in `meshd` for the first local MVP if that
  keeps delivery small, but keep the API/adapter narrow enough to extract into
  a `meshrun` helper binary;
- prior-art harvest: borrow Hangar-style lifecycle tests and driver seams, but
  do not depend on Hangar REST or PTY/TUI scraping for the primary contract.

The runtime proxy owns process supervision, stdin/stdout JSON framing, crash
classification, event buffering, and resume metadata. Agent Mesh proper should
consume this as a local runtime capability and keep coordination state in the
blackboard/ticket layer.
