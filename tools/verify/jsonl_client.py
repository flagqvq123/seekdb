#!/usr/bin/env python3
"""Cross-platform (Windows-first) JSONL client for MiniVectorDB.

The official protocol_check driver uses Unix-only facilities: selectors on
pipes, os.killpg and start_new_session. Windows cannot select on anonymous
pipes, so this client uses a dedicated reader thread plus a queue for bounded,
per-response timeouts. It is pure standard library and runs on Windows, Linux
and macOS.

Features
--------
* spawn the database binary with --data-dir and VECTORDB_DATA_DIR;
* wait for the exact {"type":"ready"} handshake with a timeout;
* send execute / shutdown requests and strictly validate responses:
  single-line UTF-8 JSON object, boolean ok, echoed id with the exact same
  JSON type (int vs str vs bool), no unsolicited output;
* capture the child's stderr into a log file and expose its tail;
* clean shutdown: response ok:true, no extra stdout, exit code 0;
* restart(): normal shutdown then start again on the same data directory;
* crash(): kill/terminate the process without shutdown to simulate a crash;
* strict JSON parsing rejects NaN/Infinity and duplicate object keys.

Library use::

    from jsonl_client import Session, ProtocolError

    s = Session(["path/to/minivectordb"], data_dir="/tmp/db", stderr_log="/tmp/db.err")
    s.start()
    print(s.execute("CREATE TABLE t (id BIGINT PRIMARY KEY);"))
    s.shutdown()

CLI use (each line is one SQL statement, or a driver directive)::

    python tools/verify/jsonl_client.py --binary path/to/minivectordb \
        --file requests.sql [--mode ann] [--keep-data] [--crash-at-end]

Supported directives inside --file (same meaning as the official driver):
--restart and --mode exact|ann.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

DEFAULT_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
EOF = object()


class ProtocolError(RuntimeError):
    """The peer violated the JSON Lines protocol or died unexpectedly."""


class ClientTimeout(TimeoutError):
    """A startup/read/write operation exceeded its deadline."""


def strict_json(text):
    """Parse JSON but reject non-finite numbers and duplicate keys.

    Mirrors the judge's _transport.strict_json so local tests cannot accept
    responses the official checker would reject.
    """

    def invalid_constant(value):
        raise ValueError(f"non-finite JSON number: {value}")

    def reject_duplicates(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(text, parse_constant=invalid_constant, object_pairs_hook=reject_duplicates)


class Session:
    """One database process with strict request/response handling."""

    def __init__(
        self,
        argv,
        data_dir,
        stderr_log,
        *,
        startup_timeout=60.0,
        request_timeout=60.0,
        max_response_bytes=DEFAULT_MAX_RESPONSE_BYTES,
        cwd=None,
        env=None,
        recorder=None,
        name=None,
    ):
        if not argv or not all(isinstance(part, str) for part in argv):
            raise ValueError("argv must be a non-empty list of strings")
        if data_dir is None:
            raise ValueError("data_dir is required")
        self.argv = list(argv)
        self.data_dir = os.path.abspath(str(data_dir))
        self.stderr_log = os.path.abspath(str(stderr_log))
        self.startup_timeout = float(startup_timeout)
        self.request_timeout = float(request_timeout)
        self.max_response_bytes = int(max_response_bytes)
        self.cwd = str(cwd) if cwd else None
        self.extra_env = dict(env) if env else {}
        self.recorder = recorder
        self.name = name or os.path.basename(self.argv[0])
        self.proc = None
        self._reader = None
        self._queue = None
        self._log = None
        self.sequence = 0
        self.transcript = []

    # -- lifecycle ---------------------------------------------------------

    def start(self):
        """Spawn the process and block until an exact ready handshake."""
        if self.proc is not None:
            raise ProtocolError("session is already started")
        self._spawn_process()
        message = self._read_message(self.startup_timeout, "startup handshake")
        if message != {"type": "ready"}:
            raise ProtocolError(f"expected ready, got {message!r}; {self._stderr_hint()}")

    def spawn(self):
        """Spawn the process WITHOUT waiting for the ready handshake.

        Used to simulate a crash during startup/recovery: write committed data,
        crash, spawn a new process (which starts replaying the log) and kill it
        before or right after it reports ready. Call start() only on a session
        that has not been spawned yet.
        """
        if self.proc is not None:
            raise ProtocolError("session is already started")
        self._spawn_process()

    def _spawn_process(self):
        log_dir = os.path.dirname(self.stderr_log)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        if self._log is None:
            self._log = open(self.stderr_log, "ab", buffering=0)

        env = dict(os.environ)
        env["VECTORDB_DATA_DIR"] = self.data_dir
        env.update(self.extra_env)
        argv = [*self.argv, "--data-dir", self.data_dir]
        self._queue = queue.Queue()
        try:
            self.proc = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self._log,
                cwd=self.cwd,
                env=env,
            )
        except OSError as exc:
            raise ProtocolError(f"cannot start {argv[0]}: {exc}") from exc
        proc, q = self.proc, self._queue
        self._reader = threading.Thread(
            target=self._reader_loop,
            args=(proc, q),
            name=f"jsonl-reader-{self.name}",
            daemon=True,
        )
        self._reader.start()

    def restart(self):
        """Normal shutdown, then start again on the same data directory."""
        self.shutdown()
        self.start()

    def shutdown(self):
        """Send shutdown, verify a clean exit and close the process."""
        if self.proc is None:
            raise ProtocolError("session is not running")
        response = self.request(op="shutdown")
        if response.get("ok") is not True:
            raise ProtocolError(f"shutdown returned ok:false: {response!r}")
        proc = self.proc
        try:
            proc.stdin.close()
        except (OSError, ValueError):
            pass
        try:
            proc.wait(timeout=self.request_timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
            self._finish_process(proc)
            raise ProtocolError(
                f"database did not exit within {self.request_timeout}s after shutdown"
            )
        self._join_reader(10.0)
        extra = self._drain_extra_lines()
        returncode = proc.returncode
        self._finish_process(proc)
        if extra:
            raise ProtocolError(
                "extra stdout after shutdown response: "
                + "; ".join(repr(line[:200]) for line in extra[:5])
            )
        if returncode != 0:
            raise ProtocolError(
                f"nonzero exit code {returncode} after shutdown; {self._stderr_hint()}"
            )
        return response

    def crash(self, method="kill"):
        """Terminate the process without shutdown to simulate a crash.

        method='kill' sends an immediate kill (Windows TerminateProcess,
        POSIX SIGKILL); method='terminate' asks for a graceful termination
        first and escalates to kill after a short grace period. Returns the
        process exit code when available.
        """
        if self.proc is None:
            return None
        proc = self.proc
        try:
            if method == "terminate":
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
            else:
                proc.kill()
        except OSError:
            pass
        try:
            proc.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            pass
        returncode = proc.returncode
        self._finish_process(proc)
        return returncode

    def close(self):
        """Best-effort cleanup; safe to call more than once."""
        if self.proc is not None:
            try:
                if self.proc.poll() is None:
                    self.proc.kill()
                self.proc.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                pass
            self._finish_process(self.proc)
        self._join_reader(2.0)
        if self._log is not None:
            try:
                self._log.close()
            except OSError:
                pass
            self._log = None

    # -- requests ----------------------------------------------------------

    def execute(self, sql, mode="exact", ident=None, wait=True):
        """Send one execute request and return the validated response."""
        return self.request(sql=sql, mode=mode, op="execute", ident=ident, wait=wait)

    def request(self, sql=None, mode=None, op="execute", ident=None, wait=True):
        """Send one request; validate echoed id type/value and boolean ok."""
        if not isinstance(op, str) or not op:
            raise ValueError("op must be a non-empty string")
        if sql is not None and not isinstance(sql, str):
            raise ValueError("sql must be a string")
        if self.proc is None:
            raise ProtocolError("session is not running")

        self.sequence += 1
        request_id = self.sequence if ident is None else ident
        request = {"id": request_id, "op": op}
        if sql is not None:
            request["sql"] = sql
            request["search_mode"] = mode or "exact"
        elif mode is not None:
            request["search_mode"] = mode
        self.send(request)
        if not wait:
            return None
        response = self._read_message(
            self.request_timeout, f"response to id={request_id!r}"
        )
        actual_id = response.get("id")
        if type(actual_id) is not type(request_id) or actual_id != request_id:
            raise ProtocolError(
                f"response id differs from request: sent {request_id!r} "
                f"({type(request_id).__name__}), got {actual_id!r} "
                f"({type(actual_id).__name__}); response={response!r}"
            )
        if type(response.get("ok")) is not bool:
            raise ProtocolError(f"missing boolean ok in response: {response!r}")
        self._record(request, response)
        return response

    def send(self, request):
        """Serialize and write one request, checking for unsolicited output."""
        if self.proc is None:
            raise ProtocolError("session is not running")
        if self.proc.poll() is not None:
            raise ProtocolError(
                f"database exited (code {self.proc.returncode}) before request; "
                + self._stderr_hint()
            )
        try:
            kind, payload = self._queue.get_nowait()
        except queue.Empty:
            kind, payload = None, None
        if kind == "line":
            raise ProtocolError(f"unsolicited or duplicate response: {payload[:240]!r}")
        if kind == "eof":
            raise ProtocolError(
                f"database exited (code {self.proc.poll()}) before request; "
                + self._stderr_hint()
            )
        if kind == "error":
            raise ProtocolError(str(payload))
        try:
            data = (
                json.dumps(request, ensure_ascii=False, allow_nan=False) + "\n"
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ProtocolError(f"cannot serialize request: {exc}") from exc
        try:
            self.proc.stdin.write(data)
            self.proc.stdin.flush()
        except (OSError, ValueError) as exc:
            raise ProtocolError(
                f"cannot write request to database: {exc}; {self._stderr_hint()}"
            ) from exc

    # -- internals ---------------------------------------------------------

    def _reader_loop(self, proc, out_queue):
        limit = self.max_response_bytes
        try:
            while True:
                try:
                    line = proc.stdout.readline(limit + 1)
                except (OSError, ValueError):
                    break
                if not line:
                    break
                if len(line) > limit:
                    out_queue.put(
                        ("error", f"response exceeds byte limit ({limit} bytes)")
                    )
                    break
                if not line.endswith(b"\n"):
                    out_queue.put(
                        (
                            "error",
                            "response line is not newline-terminated: "
                            f"{line[:240]!r}",
                        )
                    )
                    break
                out_queue.put(("line", line))
        finally:
            out_queue.put(("eof", None))

    def _read_message(self, timeout, context):
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ClientTimeout(
                    f"{context} timed out after {timeout:.3f}s; {self._stderr_hint()}"
                )
            try:
                kind, payload = self._queue.get(timeout=remaining)
            except queue.Empty:
                raise ClientTimeout(
                    f"{context} timed out after {timeout:.3f}s; {self._stderr_hint()}"
                ) from None
            if kind == "line":
                return self._parse_line(payload)
            if kind == "eof":
                code = self.proc.poll() if self.proc is not None else None
                raise ProtocolError(
                    f"database exited before response (exit code {code}); "
                    + self._stderr_hint()
                )
            if kind == "error":
                raise ProtocolError(str(payload))
            raise ProtocolError(f"internal reader state: {kind!r}")

    def _parse_line(self, raw):
        try:
            text = raw[:-1].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError(
                f"response is not valid UTF-8: {exc}; raw={raw[:240]!r}"
            ) from exc
        if not text.strip():
            raise ProtocolError("empty line on stdout")
        try:
            value = strict_json(text)
        except ValueError as exc:
            raise ProtocolError(
                f"invalid JSON response: {exc}; stdout line (first 240 chars)={text[:240]!r}"
            ) from exc
        if not isinstance(value, dict):
            raise ProtocolError(
                f"response must be a JSON object, got {type(value).__name__}"
            )
        return value

    def _join_reader(self, timeout):
        reader = self._reader
        if reader is not None and reader.is_alive():
            reader.join(timeout)
        if reader is not None and reader.is_alive():
            raise ProtocolError("stdout reader thread did not finish")

    def _drain_extra_lines(self):
        extra = []
        while True:
            try:
                kind, payload = self._queue.get_nowait()
            except queue.Empty:
                break
            if kind == "line":
                extra.append(payload)
            elif kind == "error":
                raise ProtocolError(str(payload))
        if self.proc is not None:
            try:
                leftover = self.proc.stdout.read()
            except (OSError, ValueError):
                leftover = b""
            if leftover:
                extra.append(leftover)
        return extra

    def _finish_process(self, proc):
        for stream in (proc.stdin, proc.stdout):
            if stream is not None and not stream.closed:
                try:
                    stream.close()
                except (OSError, ValueError):
                    pass
        self.proc = None
        self._reader = None
        self._queue = None

    def _record(self, request, response):
        entry = {
            "session": self.name,
            "request": request,
            "response": response,
        }
        self.transcript.append(entry)
        if self.recorder is not None:
            self.recorder(self, request, response)

    def _stderr_hint(self):
        tail = self.stderr_tail(1000)
        if not tail:
            return f"stderr log: {self.stderr_log} (empty)"
        return f"stderr tail: {tail!r}"

    def stderr_tail(self, limit=2000):
        """Return the last limit bytes of the captured stderr log."""
        try:
            data = Path(self.stderr_log).read_bytes()
        except OSError:
            return ""
        return data[-limit:].decode("utf-8", "replace")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.shutdown()
        finally:
            self.close()
        return False


# -- CLI -------------------------------------------------------------------


def _default_binary():
    here = Path(__file__).resolve()
    root = here.parents[2]  # seekdb/
    names = ["minivectordb.exe", "minivectordb"] if os.name == "nt" else ["minivectordb"]
    for profile in ("debug", "release"):
        for name in names:
            candidate = root / "minivectordb" / "target" / profile / name
            if candidate.exists():
                return str(candidate)
    return None


def _load_statements(args):
    statements = list(args.sql or [])
    if args.file:
        text = Path(args.file).read_text(encoding="utf-8-sig")
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            statements.append(stripped)
    return statements


def _cli(argv=None):
    # Windows consoles often default to a legacy code page (GBK); protocol output
    # contains UTF-8/unicode, so force UTF-8 with replacement on the CLI streams.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
        except (AttributeError, ValueError):
            pass
    parser = argparse.ArgumentParser(
        description="Drive a MiniVectorDB binary over the JSON Lines protocol."
    )
    parser.add_argument("--binary", help="path to the database binary")
    parser.add_argument("--data-dir", help="data directory (default: fresh temp dir)")
    parser.add_argument("--sql", action="append", help="SQL statement; repeatable")
    parser.add_argument("--file", help="file with one SQL statement/directive per line")
    parser.add_argument("--mode", choices=("exact", "ann"), default="exact")
    parser.add_argument("--startup-timeout", type=float, default=120.0)
    parser.add_argument("--request-timeout", type=float, default=60.0)
    parser.add_argument("--handshake-only", action="store_true")
    parser.add_argument(
        "--crash-at-end",
        action="store_true",
        help="kill the process instead of sending shutdown (crash simulation)",
    )
    parser.add_argument(
        "--keep-data", action="store_true", help="keep the temporary data directory"
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    binary = args.binary or _default_binary()
    if not binary:
        parser.error("--binary is required (default debug binary was not found)")
    if not Path(binary).exists():
        parser.error(f"binary does not exist: {binary}")

    root = Path(tempfile.mkdtemp(prefix="mvdb-client-"))
    data_dir = Path(args.data_dir) if args.data_dir else root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    stderr_log = root / "stderr.log"

    session = Session(
        [str(Path(binary).resolve())],
        data_dir=data_dir,
        stderr_log=stderr_log,
        startup_timeout=args.startup_timeout,
        request_timeout=args.request_timeout,
    )
    mode = args.mode
    try:
        session.start()
        print('{"type":"ready"}', file=sys.stderr)
        statements = [] if args.handshake_only else _load_statements(args)
        for statement in statements:
            if statement == "--restart":
                session.restart()
                print('{"type":"ready"}  # after restart', file=sys.stderr)
                continue
            if statement.startswith("--mode "):
                candidate = statement.split()[1]
                if candidate not in ("exact", "ann"):
                    raise SystemExit(f"invalid mode directive: {statement}")
                mode = candidate
                continue
            response = session.execute(statement, mode=mode)
            print(json.dumps(response, ensure_ascii=False), flush=True)
        if args.crash_at_end:
            session.crash()
            print(f"crashed (data kept in {data_dir})", file=sys.stderr)
        else:
            session.shutdown()
    except (ProtocolError, ClientTimeout) as exc:
        print(f"PROTOCOL ERROR: {exc}", file=sys.stderr)
        if args.verbose:
            for entry in session.transcript[-5:]:
                print(f"  {entry['request']!r} -> {entry['response']!r}", file=sys.stderr)
        return 1
    finally:
        session.close()
    if args.keep_data or args.data_dir:
        print(f"data directory: {data_dir}", file=sys.stderr)
        print(f"stderr log: {stderr_log}", file=sys.stderr)
    else:
        import shutil

        shutil.rmtree(root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
