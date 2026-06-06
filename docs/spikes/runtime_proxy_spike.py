#!/usr/bin/env python3
"""Runtime proxy spike for a resident Claude Code stream-json process.

This is intentionally not production Agent Mesh code. It proves the local
process/session primitive and sketches the HTTP API shape from issue #32.
"""

from __future__ import annotations

import argparse
import datetime as dt
import http.server
import json
import os
import queue
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlparse


BASE_COMMAND = [
    "claude",
    "-p",
    "--input-format",
    "stream-json",
    "--output-format",
    "stream-json",
    "--verbose",
]
UI_PATH = os.path.join(os.path.dirname(__file__), "runtime-proxy-ui.html")


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


def json_response(handler: http.server.BaseHTTPRequestHandler, status: int, body: dict[str, Any]) -> None:
    payload = json.dumps(body, indent=2, sort_keys=True).encode("utf-8")
    handler.send_response(status)
    handler.send_header("content-type", "application/json")
    handler.send_header("content-length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def file_response(handler: http.server.BaseHTTPRequestHandler, path: str, content_type: str) -> None:
    with open(path, "rb") as handle:
        payload = handle.read()
    handler.send_response(200)
    handler.send_header("content-type", content_type)
    handler.send_header("content-length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def read_json_body(handler: http.server.BaseHTTPRequestHandler) -> dict[str, Any]:
    length = int(handler.headers.get("content-length") or "0")
    if length == 0:
        return {}
    raw = handler.rfile.read(length)
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


@dataclass
class ResidentClaudeSession:
    cwd: str
    command: list[str] = field(default_factory=lambda: list(BASE_COMMAND))
    local_session_id: str = field(default_factory=lambda: f"local-{uuid.uuid4()}")
    started_at: str = field(default_factory=utc_now)
    status: str = "created"
    last_seen: str | None = None
    last_error: dict[str, Any] | None = None
    checkpoint_summary: str = "no turns completed"
    process: subprocess.Popen[str] | None = None
    claude_session_id: str | None = None
    child_pid: int | None = None
    turn_count: int = 0
    events: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._lock = threading.RLock()
        self._result_events: queue.Queue[dict[str, Any]] = queue.Queue()
        self._exit_events: queue.Queue[dict[str, Any]] = queue.Queue()
        self._stop_requested = False

    def start(self) -> None:
        os.makedirs(self.cwd, exist_ok=True)
        self.process = subprocess.Popen(
            self.command,
            cwd=self.cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.child_pid = self.process.pid
        self.status = "running"
        self._record_runtime_event(
            "session_started",
            command=self.command,
            cwd=self.cwd,
            pid=self.child_pid,
        )
        threading.Thread(target=self._read_stdout, name=f"{self.local_session_id}-stdout", daemon=True).start()
        threading.Thread(target=self._read_stderr, name=f"{self.local_session_id}-stderr", daemon=True).start()
        threading.Thread(target=self._watch_exit, name=f"{self.local_session_id}-wait", daemon=True).start()

    def metadata(self) -> dict[str, Any]:
        with self._lock:
            return {
                "id": self.local_session_id,
                "child_pid": self.child_pid,
                "claude_session_id": self.claude_session_id,
                "started_at": self.started_at,
                "last_seen": self.last_seen,
                "status": self.status,
                "last_error": self.last_error,
                "checkpoint_summary": self.checkpoint_summary,
                "command": self.command,
                "cwd": self.cwd,
            }

    def submit_user_message(self, content: str) -> str:
        with self._lock:
            process = self.process
            if process is None or process.poll() is not None or process.stdin is None:
                error = self._runtime_error(
                    "send_failed",
                    code="process_not_running",
                    message="cannot send message because child process is not running",
                    pid=self.child_pid,
                    exit_code=process.poll() if process else None,
                )
                raise RuntimeError(json.dumps(error, sort_keys=True))

            self.turn_count += 1
            turn_id = f"{self.local_session_id}-turn-{self.turn_count}"
            line = {"type": "user", "message": {"role": "user", "content": content}}
            try:
                process.stdin.write(json.dumps(line, separators=(",", ":")) + "\n")
                process.stdin.flush()
            except BrokenPipeError as exc:
                error = self._runtime_error(
                    "send_failed",
                    code="broken_pipe",
                    message=str(exc),
                    pid=self.child_pid,
                    turn_id=turn_id,
                )
                raise RuntimeError(json.dumps(error, sort_keys=True)) from exc

            self._record_runtime_event(
                "message_sent",
                turn_id=turn_id,
                stdin_shape={"type": "user", "message": {"role": "user", "content": "<string>"}},
            )
            return turn_id

    def ask(self, content: str, timeout_s: float = 240.0) -> tuple[str, dict[str, Any]]:
        turn_id = self.submit_user_message(content)
        return turn_id, self.wait_for_result(timeout_s)

    def wait_for_result(self, timeout_s: float = 240.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_s
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("timed out waiting for Claude result event")
            try:
                return self._result_events.get(timeout=min(1.0, remaining))
            except queue.Empty:
                process = self.process
                if process is not None and process.poll() is not None:
                    error = self._runtime_error(
                        "wait_failed",
                        code="process_exited_before_result",
                        message="child process exited before a result event arrived",
                        pid=self.child_pid,
                        exit_code=process.poll(),
                    )
                    raise RuntimeError(json.dumps(error, sort_keys=True))

    def crash_child(self, sig: signal.Signals = signal.SIGKILL, timeout_s: float = 10.0) -> dict[str, Any]:
        process = self.process
        if process is None or process.poll() is not None:
            return self._runtime_error(
                "crash_failed",
                code="process_not_running",
                message="child process was already stopped",
                pid=self.child_pid,
                exit_code=process.poll() if process else None,
            )

        self._record_runtime_event("kill_sent", pid=process.pid, signal=sig.name)
        os.kill(process.pid, sig)
        try:
            return self._exit_events.get(timeout=timeout_s)
        except queue.Empty:
            return self._runtime_error(
                "crash_detection_timeout",
                code="process_exit_timeout",
                message="child did not report exit before timeout",
                pid=process.pid,
                signal=sig.name,
            )

    def stop(self, timeout_s: float = 5.0) -> dict[str, Any]:
        process = self.process
        if process is None or process.poll() is not None:
            return self._record_runtime_event("session_already_stopped", pid=self.child_pid)

        self._stop_requested = True
        self._record_runtime_event("stop_sent", pid=process.pid, signal="SIGTERM")
        process.terminate()
        try:
            process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self._record_runtime_event("stop_escalated", pid=process.pid, signal="SIGKILL")
            process.kill()
            process.wait(timeout=timeout_s)
        return self._record_runtime_event("session_stopped", pid=process.pid, exit_code=process.returncode)

    def _read_stdout(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        for raw_line in self.process.stdout:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                self._record_runtime_event("stdout_non_json", line=line)
                continue
            with self._lock:
                event["_proxy_event_id"] = len(self.events)
                event["_proxy_seen_at"] = utc_now()
                self.events.append(event)
                self.last_seen = event["_proxy_seen_at"]
                if event.get("session_id"):
                    self.claude_session_id = event["session_id"]
                if event.get("type") == "result":
                    if event.get("subtype") == "success" and not event.get("is_error"):
                        self.checkpoint_summary = f"last result: num_turns={event.get('num_turns')} session_id={event.get('session_id')}"
                    else:
                        self.last_error = event
                    self._result_events.put(event)
        self._record_runtime_event("stdout_eof", pid=self.child_pid)

    def _read_stderr(self) -> None:
        assert self.process is not None and self.process.stderr is not None
        for raw_line in self.process.stderr:
            line = raw_line.rstrip("\n")
            if line:
                self._record_runtime_event("stderr", line=line)

    def _watch_exit(self) -> None:
        assert self.process is not None
        exit_code = self.process.wait()
        signal_name = None
        if exit_code < 0:
            try:
                signal_name = signal.Signals(-exit_code).name
            except ValueError:
                signal_name = f"signal_{-exit_code}"

        if self._stop_requested:
            event = self._record_runtime_event("child_exit", pid=self.child_pid, exit_code=exit_code, status="stopped")
            self.status = "stopped"
            self._exit_events.put(event)
            return

        if exit_code == 0:
            event = self._record_runtime_event("child_exit", pid=self.child_pid, exit_code=exit_code, status="exited")
            self.status = "exited"
        else:
            event = self._runtime_error(
                "child_exit",
                code="child_crashed",
                message="child process exited before DELETE/session stop",
                pid=self.child_pid,
                exit_code=exit_code,
                signal=signal_name,
            )
            self.status = "crashed"
        self._exit_events.put(event)

    def _record_runtime_event(self, subtype: str, **fields: Any) -> dict[str, Any]:
        with self._lock:
            event = {
                "type": "runtime",
                "subtype": subtype,
                "id": len(self.events),
                "time": utc_now(),
                "local_session_id": self.local_session_id,
                **fields,
            }
            self.events.append(event)
            self.last_seen = event["time"]
            return event

    def _runtime_error(self, subtype: str, **fields: Any) -> dict[str, Any]:
        with self._lock:
            event = {
                "type": "runtime_error",
                "subtype": subtype,
                "id": len(self.events),
                "time": utc_now(),
                "local_session_id": self.local_session_id,
                **fields,
            }
            self.events.append(event)
            self.last_seen = event["time"]
            self.last_error = event
            return event


class SessionManager:
    def __init__(self, command: list[str], cwd: str) -> None:
        self.command = command
        self.cwd = cwd
        self.sessions: dict[str, ResidentClaudeSession] = {}
        self._lock = threading.RLock()

    def create(self, cwd: str | None = None, command: list[str] | None = None) -> ResidentClaudeSession:
        session = ResidentClaudeSession(cwd=cwd or self.cwd, command=command or list(self.command))
        session.start()
        with self._lock:
            self.sessions[session.local_session_id] = session
        return session

    def get(self, session_id: str) -> ResidentClaudeSession | None:
        with self._lock:
            return self.sessions.get(session_id)

    def list_metadata(self) -> list[dict[str, Any]]:
        with self._lock:
            return [session.metadata() for session in self.sessions.values()]


def make_handler(manager: SessionManager) -> type[http.server.BaseHTTPRequestHandler]:
    class RuntimeProxyHandler(http.server.BaseHTTPRequestHandler):
        server_version = "agent-mesh-runtime-proxy-spike/0"

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/sessions":
                body = read_json_body(self)
                session = manager.create(cwd=body.get("cwd"), command=body.get("command"))
                json_response(self, 201, {"session": session.metadata()})
                return

            match = re.fullmatch(r"/sessions/([^/]+)/messages", parsed.path)
            if match:
                session = manager.get(match.group(1))
                if session is None:
                    json_response(self, 404, {"error": {"code": "no_such_session"}})
                    return
                body = read_json_body(self)
                content = body.get("content") or body.get("message")
                if not isinstance(content, str) or not content:
                    json_response(self, 400, {"error": {"code": "empty_message"}})
                    return
                try:
                    turn_id = session.submit_user_message(content)
                except RuntimeError as exc:
                    json_response(self, 409, {"error": json.loads(str(exc))})
                    return
                json_response(self, 202, {"turn_id": turn_id, "session": session.metadata()})
                return

            match = re.fullmatch(r"/sessions/([^/]+)/kill", parsed.path)
            if match:
                session = manager.get(match.group(1))
                if session is None:
                    json_response(self, 404, {"error": {"code": "no_such_session"}})
                    return
                event = session.crash_child()
                json_response(self, 200, {"session": session.metadata(), "event": event})
                return

            json_response(self, 404, {"error": {"code": "not_found"}})

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path in ("/", "/runtime-proxy-ui.html"):
                file_response(self, UI_PATH, "text/html; charset=utf-8")
                return

            if parsed.path == "/sessions":
                json_response(self, 200, {"sessions": manager.list_metadata()})
                return

            match = re.fullmatch(r"/sessions/([^/]+)/events", parsed.path)
            if not match:
                json_response(self, 404, {"error": {"code": "not_found"}})
                return
            session = manager.get(match.group(1))
            if session is None:
                json_response(self, 404, {"error": {"code": "no_such_session"}})
                return
            since_raw = parse_qs(parsed.query).get("since", ["0"])[0]
            try:
                since = int(since_raw)
            except ValueError:
                json_response(self, 400, {"error": {"code": "bad_since"}})
                return
            json_response(self, 200, {"session": session.metadata(), "events": session.events[since:]})

        def do_DELETE(self) -> None:
            parsed = urlparse(self.path)
            match = re.fullmatch(r"/sessions/([^/]+)", parsed.path)
            if not match:
                json_response(self, 404, {"error": {"code": "not_found"}})
                return
            session = manager.get(match.group(1))
            if session is None:
                json_response(self, 404, {"error": {"code": "no_such_session"}})
                return
            event = session.stop()
            json_response(self, 200, {"session": session.metadata(), "event": event})

        def log_message(self, fmt: str, *args: Any) -> None:
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    return RuntimeProxyHandler


def run_proof(args: argparse.Namespace) -> int:
    command = list(BASE_COMMAND) + list(args.claude_arg)
    manager = SessionManager(command=command, cwd=args.cwd)
    session = manager.create()
    nonce = f"runtime-proxy-{uuid.uuid4()}"
    started_pid = session.child_pid

    prompt_1 = (
        "Runtime proxy spike turn 1. Do not use tools. "
        f"Remember this exact nonce for the next turn: {nonce}. "
        "Reply with one short sentence containing the nonce."
    )
    turn_1, result_1 = session.ask(prompt_1, timeout_s=args.timeout)

    wait_started = time.monotonic()
    time.sleep(args.delay)
    waited_s = time.monotonic() - wait_started

    prompt_2 = (
        "Runtime proxy spike turn 2. Do not use tools. "
        "Repeat the exact nonce I gave you in turn 1, and say whether you retained it "
        "from this same conversation."
    )
    turn_2, result_2 = session.ask(prompt_2, timeout_s=args.timeout)
    still_same_pid = session.child_pid == started_pid and session.process is not None and session.process.poll() is None
    same_claude_session = result_1.get("session_id") == result_2.get("session_id")
    nonce_recalled = nonce in (result_2.get("result") or "")

    crash_event = session.crash_child()
    try:
        session.submit_user_message("this should fail after crash")
    except RuntimeError as exc:
        post_crash_send = json.loads(str(exc))
    else:
        post_crash_send = {"type": "runtime_error", "subtype": "expected_send_failure_missing"}

    report = {
        "spike": "runtime-proxy",
        "ran_at": utc_now(),
        "command": command,
        "cwd": args.cwd,
        "local_session_id": session.local_session_id,
        "started_pid": started_pid,
        "final_pid": session.child_pid,
        "waited_seconds_between_messages": round(waited_s, 3),
        "turns": [
            {"turn_id": turn_1, "result": result_1},
            {"turn_id": turn_2, "result": result_2},
        ],
        "proof": {
            "same_os_process_for_both_turns": still_same_pid,
            "same_claude_session_id_for_both_turns": same_claude_session,
            "nonce_recalled_on_turn_2": nonce_recalled,
            "turn_2_num_turns": result_2.get("num_turns"),
        },
        "crash": {
            "kill_event": crash_event,
            "post_crash_send": post_crash_send,
        },
        "final_metadata": session.metadata(),
        "events_observed": session.events,
    }

    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)
            handle.write("\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def run_server(args: argparse.Namespace) -> int:
    command = list(BASE_COMMAND) + list(args.claude_arg)
    manager = SessionManager(command=command, cwd=args.cwd)
    handler = make_handler(manager)
    server = http.server.ThreadingHTTPServer((args.host, args.port), handler)
    print(f"runtime proxy spike listening on http://{args.host}:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resident Claude stream-json runtime proxy spike.")
    parser.add_argument("--cwd", default=os.path.join(tempfile.gettempdir(), "agent-mesh-runtime-proxy-spike"))
    parser.add_argument("--claude-arg", action="append", default=[], help="Extra Claude arg for experiments, e.g. --claude-arg=--model --claude-arg=haiku")
    sub = parser.add_subparsers(dest="command", required=True)

    proof = sub.add_parser("proof", help="Run the issue #32 two-message proof and crash check.")
    proof.add_argument("--delay", type=float, default=30.0)
    proof.add_argument("--timeout", type=float, default=240.0)
    proof.add_argument("--output")
    proof.set_defaults(func=run_proof)

    serve = sub.add_parser("serve", help="Run the local API shim from issue #32.")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=0)
    serve.set_defaults(func=run_server)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
