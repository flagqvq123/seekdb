#!/usr/bin/env python3
"""Cross-platform harness for the MiniVectorDB public protocol checks.

The official driver is protocol_check/run.py.  Its transport relies on
POSIX-only APIs (selectors on pipes, os.set_blocking, os.killpg), so it cannot
watch a child process on Windows.  This wrapper keeps one interface:

  --binary PATH   run a locally built executable through --command mode
  --image TAG     run the built Docker image exactly like the judge

On Linux/WSL it delegates to protocol_check/run.py verbatim, i.e. the same
transport and timing semantics as the judge.  On Windows it runs a portable
re-implementation of the same steps (a reader thread instead of selectors) and
reuses protocol_check's manifest, cases, config and assertion module, so the
assertions are identical.

Each run creates a timestamped directory under protocol-results/ containing
per-case stderr logs, cases.json, summary.json and summary.md, and prints a
PASS/FAIL table.  This is a local diagnostic only: no scores, no hidden cases.

Usage:
  python tools/verify/run_public.py --binary minivectordb/target/release/minivectordb.exe
  python tools/verify/run_public.py --image minivectordb:round2
  python tools/verify/run_public.py --image minivectordb:round2 --handshake-only
  python tools/verify/run_public.py --binary minivectordb/target/release/minivectordb.exe \
      --case sql_crud --case storage_restart

Exit codes: 0 = every selected case passed, 1 = failures, 2 = usage/setup error.
"""
import argparse
import datetime
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[2]
PROTOCOL_CHECK = ROOT / "protocol_check"
PACKAGE = ROOT / "mysqltest"
CONFIG_PATH = PROTOCOL_CHECK / "config.json"
RESULTS_ROOT = ROOT / "protocol-results"
SCRATCH_ROOT = RESULTS_ROOT / "tmp"
STATUS_ORDER = ("PASS", "FAIL", "TIMEOUT", "ERROR", "SKIPPED")
FENCE = "```"

try:  # keep unusual SQL/Unicode out of console-encoding crashes on Windows
    sys.stdout.reconfigure(errors="backslashreplace")
    sys.stderr.reconfigure(errors="backslashreplace")
except (AttributeError, ValueError):
    pass


# --------------------------------------------------------------------------- #
# Shared helpers (mirroring protocol_check where it matters)
# --------------------------------------------------------------------------- #
def strict_json(text):
    """JSON parser that rejects NaN/Infinity and duplicate keys, like the judge."""

    def invalid(value):
        raise ValueError("non-finite JSON number: %s" % value)

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key: %s" % key)
            result[key] = value
        return result

    return json.loads(text, parse_constant=invalid, object_pairs_hook=pairs)


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load %s" % path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def within(root, relative):
    path = (root / relative).resolve(strict=True)
    path.relative_to(root.resolve())
    return path


def scratch_dir():
    """Preferred data-directory root for spawned processes.

    A repo-local scratch root is used so verification does not depend on the
    system temp directory: on some Windows hosts the WAL storage gets
    access-denied when creating files under the user %TEMP% directory.
    """
    try:
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        probe = SCRATCH_ROOT / ".write-probe"
        probe.write_bytes(b"")
        probe.unlink()
        return str(SCRATCH_ROOT)
    except OSError:
        return None


def timestamp():
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


def read_tail(path, max_lines=25, max_chars=4000):
    try:
        text = Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return "(no stderr log: %s)" % exc
    text = text.strip()
    if not text:
        return "(stderr empty)"
    lines = text.splitlines()[-max_lines:]
    tail = "\n".join(lines)
    if len(tail) > max_chars:
        tail = tail[-max_chars:]
    return tail


class ProtocolError(RuntimeError):
    pass


# --------------------------------------------------------------------------- #
# Portable stdin/stdout transport (Windows + POSIX)
# --------------------------------------------------------------------------- #
class Session:
    """One database process/container, speaking the JSONL protocol."""

    def __init__(self, config, log_path, image=None, command=None, bind_dir=None):
        self.config = config
        self.image = image
        self.command = command
        self.bind_dir = bind_dir
        self.log_path = Path(log_path)
        self.name = "mvdb-verify-" + uuid.uuid4().hex[:12]
        self.volume = self.name + "-data"
        self.volume_created = False
        self.temp = (tempfile.TemporaryDirectory(prefix="mvdb-verify-", dir=scratch_dir())
                     if command else None)
        self.data_dir = self.temp.name if self.temp is not None else None
        self.proc = None
        self.log = None
        self.sequence = 0
        self._buf = b""
        self._cond = threading.Condition()
        self._reader = None
        self._eof = False
        self._read_error = None
        self._deadline = time.monotonic() + float(config.get("case_timeout", 600))
        try:
            self.log = open(self.log_path, "ab", buffering=0)
        except OSError as exc:
            raise ProtocolError("cannot open stderr log %s: %s" % (self.log_path, exc)) from exc

    # -- process / container management ------------------------------------ #
    def _docker_quiet(self, *args):
        try:
            subprocess.run(["docker", *args], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=120, check=False)
        except (OSError, subprocess.SubprocessError):
            pass

    def _docker_checked(self, *args):
        try:
            done = subprocess.run(["docker", *args], stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, timeout=120, check=False)
        except (OSError, subprocess.SubprocessError) as exc:
            raise ProtocolError("docker %s failed: %s" % (" ".join(args), exc)) from exc
        if done.returncode != 0:
            raise ProtocolError("docker %s failed (%d): %s" % (
                " ".join(args), done.returncode,
                done.stderr.decode("utf-8", "replace").strip()[:500]))

    def start(self):
        if self.proc is not None:
            raise ProtocolError("session already running")
        if self.log is None or self.log.closed:
            self.log = open(self.log_path, "ab", buffering=0)
        if self.image:
            if self.bind_dir:
                mount = "type=bind,src=%s,dst=/data" % self.bind_dir
            else:
                if not self.volume_created:
                    self._docker_checked("volume", "create", self.volume)
                    self.volume_created = True
                mount = "type=volume,src=%s,dst=/data" % self.volume
            argv = ["docker", "run", "--name", self.name, "--interactive",
                    "--network=none",
                    "--cpus=" + str(self.config["cpus"]),
                    "--memory=" + str(self.config["memory"]),
                    "--memory-swap=" + str(self.config["memory"]),
                    "--pids-limit=" + str(self.config["pids_limit"]),
                    "--cap-drop=ALL", "--security-opt=no-new-privileges",
                    "--read-only",
                    "--mount", mount,
                    self.image]
            env = None
        else:
            if not self.command:
                raise ProtocolError("no command given")
            argv = [part.replace("{data_dir}", self.data_dir) for part in self.command]
            env = dict(os.environ, VECTORDB_DATA_DIR=self.data_dir)
        with self._cond:
            self._buf = b""
            self._eof = False
            self._read_error = None
        try:
            self.proc = subprocess.Popen(argv, stdin=subprocess.PIPE,
                                         stdout=subprocess.PIPE, stderr=self.log,
                                         bufsize=0, env=env)
        except OSError as exc:
            raise ProtocolError("cannot start %r: %s" % (argv[0], exc)) from exc
        self._reader = threading.Thread(target=self._read_loop, name="jsonl-reader",
                                        daemon=True)
        self._reader.start()
        message = self.read(float(self.config["startup_timeout"]))
        if message != {"type": "ready"}:
            raise ProtocolError("expected ready, got %r" % (message,))

    def _read_loop(self):
        try:
            fd = self.proc.stdout.fileno()
            while True:
                chunk = os.read(fd, 65536)
                if not chunk:
                    break
                with self._cond:
                    self._buf += chunk
                    self._cond.notify_all()
        except (OSError, ValueError) as exc:
            with self._cond:
                self._read_error = exc
                self._eof = True
                self._cond.notify_all()
            return
        with self._cond:
            self._eof = True
            self._cond.notify_all()

    # -- protocol ----------------------------------------------------------- #
    def read(self, timeout):
        limit = int(self.config.get("max_response_bytes", 16 * 1024 * 1024))
        deadline = time.monotonic() + float(timeout)
        with self._cond:
            while b"\n" not in self._buf:
                left = min(deadline, self._deadline) - time.monotonic()
                if left <= 0:
                    raise TimeoutError("JSON response timeout")
                if len(self._buf) > limit:
                    raise ProtocolError("response exceeds byte limit")
                self._cond.wait(left)
                if self._read_error is not None and b"\n" not in self._buf:
                    raise ProtocolError("cannot read stdout: %s" % self._read_error)
                if self._eof and b"\n" not in self._buf:
                    raise ProtocolError("database exited before response")
            line, self._buf = self._buf.split(b"\n", 1)
        try:
            value = strict_json(line.decode("utf-8"))
        except (ValueError, UnicodeError) as exc:
            raise ProtocolError(
                "invalid JSON response: %s; stdout line (first 240 bytes)=%r"
                % (exc, line[:240])) from exc
        if not isinstance(value, dict):
            raise ProtocolError("response must be an object")
        return value

    def send(self, request):
        if self._buf:
            raise ProtocolError("unsolicited or duplicate response")
        data = (json.dumps(request, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
        fd = self.proc.stdin.fileno()
        view = memoryview(data)
        deadline = time.monotonic() + float(self.config["request_timeout"])
        while view:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError("request write timeout")
            try:
                written = os.write(fd, view)
            except BlockingIOError:
                time.sleep(0.001)
                continue
            except OSError as exc:
                raise ProtocolError("cannot write request: %s" % exc) from exc
            if written <= 0:
                raise ProtocolError("database stdin closed")
            view = view[written:]

    def request(self, sql=None, mode="exact", op="execute", wait=True, ident=None):
        self.sequence += 1
        request = {"id": self.sequence if ident is None else ident, "op": op}
        if sql is not None:
            request.update(sql=sql, search_mode=mode)
        self.send(request)
        if not wait:
            return None
        response = self.read(float(self.config["request_timeout"]))
        if type(response.get("id")) is not type(request["id"]) or response["id"] != request["id"]:
            raise ProtocolError("response id differs from request")
        if type(response.get("ok")) is not bool:
            raise ProtocolError("missing boolean ok")
        return response

    def shutdown(self):
        response = self.request(op="shutdown")
        if response["ok"] is not True:
            raise ProtocolError("shutdown failed")
        try:
            self.proc.stdin.close()
        except OSError:
            pass
        try:
            returncode = self.proc.wait(timeout=float(self.config["request_timeout"]))
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError("database did not exit after shutdown") from exc
        if self._reader is not None:
            self._reader.join(timeout=10)
        with self._cond:
            extra = self._buf
        if extra:
            raise ProtocolError("extra stdout after shutdown: %r" % extra[:240])
        if returncode:
            raise ProtocolError("database exited with code %s after shutdown" % returncode)
        self.stop()

    # -- cleanup ------------------------------------------------------------ #
    def stop(self):
        if self.image and self.proc is not None:
            # Killing the docker CLI alone would leave the container alive.
            self._docker_quiet("rm", "--force", "--volumes", self.name)
        proc = self.proc
        if proc is not None:
            if proc.poll() is None:
                try:
                    proc.kill()
                except OSError:
                    pass
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
            for stream in (proc.stdin, proc.stdout):
                try:
                    if stream is not None and not stream.closed:
                        stream.close()
                except OSError:
                    pass
            self.proc = None
        if self._reader is not None:
            self._reader.join(timeout=2)
            self._reader = None
        if self.log is not None and not self.log.closed:
            try:
                self.log.close()
            except OSError:
                pass

    def close(self):
        try:
            self.stop()
        finally:
            if self.volume_created:
                self._docker_quiet("volume", "rm", "--force", self.volume)
                self.volume_created = False
            if self.temp is not None:
                self.temp.cleanup()
                self.temp = None


# --------------------------------------------------------------------------- #
# Case execution (same statements/assertion order as protocol_check/run.py)
# --------------------------------------------------------------------------- #
def check_case(session, case, package, config, assertions):
    expectations = strict_json(within(package, case["result"]).read_text(encoding="utf-8"))
    statements = within(package, case["test"]).read_text(encoding="utf-8").splitlines()
    index, mode = 0, "exact"
    for line_number, text in enumerate(statements, 1):
        sql = text.strip()
        if not sql or sql.startswith("#"):
            continue
        if sql == "--restart":
            session.shutdown()
            session.start()
        elif sql in ("--mode exact", "--mode ann"):
            mode = sql.split()[1]
        elif sql.startswith("--"):
            raise ValueError("unsupported public directive: %s" % sql)
        else:
            if index >= len(expectations):
                raise ValueError("missing expected result")
            response = session.request(sql, mode)
            try:
                assertions.assert_response(response, expectations[index], config)
            except AssertionError as exc:
                raise AssertionError("line %d: %s: %s" % (line_number, sql, exc)) from exc
            index += 1
    if index != len(expectations):
        raise ValueError("SQL/result count mismatch")


def build_command(args):
    if not args.binary:
        return None
    binary = Path(args.binary).expanduser().resolve()
    if not binary.is_file():
        raise SystemExit("error: --binary %s is not a file" % binary)
    return [str(binary), "--data-dir", "{data_dir}", *args.binary_arg]


def run_portable(cases, config, args, out_dir, assertions):
    command = build_command(args)
    results, aborted = [], None
    for case in cases:
        if aborted:
            results.append(dict(id=case["id"], group=case.get("group", ""),
                                status="SKIPPED", message=aborted, duration_ms=0))
            continue
        before = time.monotonic()
        row = dict(id=case["id"], group=case.get("group", ""), status="PASS",
                   message="", duration_ms=0)
        session = None
        try:
            session = Session(config, out_dir / (case["id"] + ".stderr.log"),
                              image=args.image, command=command)
            session.start()
            if not args.handshake_only:
                check_case(session, case, PACKAGE, config, assertions)
            session.shutdown()
        except KeyboardInterrupt:
            row.update(status="ERROR", message="interrupted by operator")
            aborted = "not run after interruption"
        except (TimeoutError, subprocess.TimeoutExpired) as exc:
            row.update(status="TIMEOUT", message=str(exc))
            aborted = "not run after first timeout"
        except Exception as exc:  # noqa: BLE001 - report any failure, keep going
            row.update(status="FAIL", message=str(exc))
        finally:
            if session is not None:
                try:
                    session.close()
                except Exception as exc:  # noqa: BLE001
                    row.update(status="ERROR",
                               message=str(row.get("message", "")) + "; cleanup failed: " + str(exc))
                    aborted = "not run after cleanup failure"
        row["duration_ms"] = round((time.monotonic() - before) * 1000)
        results.append(row)
        print("%s: %s %s" % (row["id"], row["status"], row["message"]), flush=True)
        write_json(out_dir / "cases.json", results)
    return results


def run_via_protocol_check(cases, args, out_dir):
    """Delegate to the official driver where its POSIX transport works."""
    sub_out = out_dir / "protocol-check"
    command = build_command(args)
    argv = [sys.executable, str(PROTOCOL_CHECK / "run.py"), "--output", str(sub_out),
            "--package", str(PACKAGE), "--config", str(CONFIG_PATH)]
    if args.image:
        argv += ["--image", args.image]
    else:
        argv += ["--command", json.dumps(command)]
    if args.handshake_only:
        argv += ["--handshake-only"]
    for case_id in args.case:
        argv += ["--case", case_id]
    print("delegating to protocol_check/run.py:", " ".join(argv), flush=True)
    completed = subprocess.run(argv, cwd=str(ROOT), check=False)
    report_path = sub_out / "result.json"
    if not report_path.is_file():
        raise RuntimeError("protocol_check/run.py produced no result.json (exit %s)"
                           % completed.returncode)
    report = strict_json(report_path.read_text(encoding="utf-8"))
    results = []
    for row in report["cases"]:
        case = next((item for item in cases if item["id"] == row["id"]), None)
        results.append(dict(
            id=row["id"],
            group=(case or {}).get("group", ""),
            status=row.get("status", "ERROR"),
            message=row.get("message", ""),
            duration_ms=row.get("duration_ms", 0),
            stderr_log=str(sub_out / (row["id"] + ".stderr.log")),
        ))
    return results


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n",
                          encoding="utf-8")


def attach_stderr_tails(results, out_dir):
    for row in results:
        if row["status"] == "PASS":
            continue
        log = row.get("stderr_log") or str(out_dir / (row["id"] + ".stderr.log"))
        row["stderr_log"] = log
        row["stderr_tail"] = read_tail(log)


def print_table(results):
    if not results:
        return
    headers = ("CASE", "GROUP", "STATUS", "MS", "DETAIL")
    ids = [str(r["id"]) for r in results]
    id_width = max(4, min(34, max(len(x) for x in ids)))
    group_width = max(5, min(16, max(len(str(r.get("group", ""))) for r in results)))
    print("")
    print("%-*s  %-*s  %-8s  %8s  %s" % (id_width, headers[0], group_width, headers[1],
                                         headers[2], headers[3], headers[4]))
    print("-" * (id_width + group_width + 30))
    for row in results:
        detail = " ".join(str(row.get("message", "")).split())
        if len(detail) > 100:
            detail = detail[:97] + "..."
        print("%-*s  %-*s  %-8s  %8s  %s" % (id_width, row["id"], group_width,
                                             row.get("group", ""), row["status"],
                                             row.get("duration_ms", 0), detail))


def build_summary(args, config, cases, results, out_dir, runner):
    counts = {status: sum(1 for row in results if row["status"] == status)
              for status in STATUS_ORDER}
    passed = bool(results) and all(row["status"] == "PASS" for row in results)
    return {
        "generated_at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "scope": "public-protocol-check (local diagnostic only)",
        "verdict": "PASS" if passed else "FAIL",
        "runner": runner,
        "target": {
            "kind": "image" if args.image else "binary",
            "value": args.image if args.image else args.binary,
        },
        "command": build_command(args) if args.binary else None,
        "package": str(PACKAGE),
        "manifest_version": json.loads((PACKAGE / "manifest.json").read_text(encoding="utf-8")
                                       ).get("version"),
        "config": config,
        "selection": {
            "handshake_only": args.handshake_only,
            "cases": args.case,
            "selected": [case["id"] for case in cases],
        },
        "totals": dict(total=len(results), **counts),
        "passed": passed,
        "output_dir": str(out_dir),
        "cases": results,
    }


def write_summary_markdown(summary, out_dir):
    lines = [
        "# MiniVectorDB public protocol check",
        "",
        "- generated: %s" % summary["generated_at"],
        "- target: %s '%s'" % (summary["target"]["kind"], summary["target"]["value"]),
        "- runner: %s" % summary["runner"],
        "- package: '%s' (%s)" % (summary["package"], summary["manifest_version"]),
        "- verdict: **%s**" % summary["verdict"],
        "- totals: %s" % ", ".join("%s=%d" % (k, v) for k, v in summary["totals"].items()),
        "",
        "| case | group | status | ms | detail |",
        "| --- | --- | --- | ---: | --- |",
    ]
    for row in summary["cases"]:
        detail = " ".join(str(row.get("message", "")).split())
        detail = detail.replace("|", "\\|")
        if len(detail) > 160:
            detail = detail[:157] + "..."
        lines.append("| %s | %s | %s | %s | %s |" % (
            row["id"], row.get("group", ""), row["status"],
            row.get("duration_ms", 0), detail))
    failures = [row for row in summary["cases"] if row["status"] != "PASS"]
    if failures:
        lines += ["", "## Failed case stderr (up to 5)", ""]
        for row in failures[:5]:
            lines += ["### %s (%s)" % (row["id"], row["status"]), ""]
            if row.get("message"):
                message = str(row["message"]).replace("|", "\\|")
                lines += ["message: " + message, ""]
            lines += [FENCE, row.get("stderr_tail", "(no stderr captured)"), FENCE, ""]
    (out_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def print_failure_details(results):
    failures = [row for row in results if row["status"] != "PASS"]
    if not failures:
        return
    print("")
    print("FAILED CASES (%d) -- stderr tail" % len(failures))
    for row in failures[:5]:
        print("")
        print("--- %s [%s] ---" % (row["id"], row["status"]))
        if row.get("message"):
            print("message: " + " ".join(str(row["message"]).split())[:500])
        tail = row.get("stderr_tail") or read_tail(row.get("stderr_log", ""))
        for line in tail.splitlines()[-12:]:
            print("  " + line)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Run the MiniVectorDB public protocol cases against a binary or image.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary", help="path to a built minivectordb executable")
    target.add_argument("--image", help="built Docker image tag (judge-style container)")
    parser.add_argument("--handshake-only", action="store_true",
                        help="only check ready/shutdown, no SQL cases")
    parser.add_argument("--case", action="append", default=[], metavar="ID",
                        help="public case id (repeatable); default: all public cases")
    parser.add_argument("--runner", choices=["auto", "protocol-check", "portable"],
                        default="auto",
                        help="transport to use; auto = protocol_check on POSIX, portable on Windows")
    parser.add_argument("--binary-arg", action="append", default=[], metavar="ARG",
                        help="extra argv element appended to the binary command (repeatable)")
    parser.add_argument("--output", type=Path,
                        help="explicit new output directory (default: timestamped under protocol-results/)")
    args = parser.parse_args(argv)
    if args.handshake_only and args.case:
        parser.error("--handshake-only and --case cannot be combined")
    return args


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    for path, what in ((PROTOCOL_CHECK / "run.py", "protocol_check/run.py"),
                       (PACKAGE / "manifest.json", "mysqltest/manifest.json"),
                       (CONFIG_PATH, "protocol_check/config.json")):
        if not path.is_file():
            print("error: %s not found (run from the competition repository)" % what,
                  file=sys.stderr)
            return 2
    config = strict_json(CONFIG_PATH.read_text(encoding="utf-8"))
    manifest = strict_json((PACKAGE / "manifest.json").read_text(encoding="utf-8"))
    known = [case["id"] for case in manifest["cases"]]
    if args.case:
        unknown = sorted(set(args.case) - set(known))
        if unknown:
            print("error: unknown public case(s): %s" % ", ".join(unknown), file=sys.stderr)
            print("available: %s" % ", ".join(known), file=sys.stderr)
            return 2
    if args.handshake_only:
        cases = [{"id": "handshake", "group": "protocol"}]
    elif args.case:
        cases = [case for case in manifest["cases"] if case["id"] in args.case]
    else:
        cases = list(manifest["cases"])

    if args.output is not None:
        out_dir = args.output.expanduser().resolve()
        if out_dir.exists():
            print("error: output directory already exists: %s" % out_dir, file=sys.stderr)
            return 2
    else:
        prefix = "handshake" if args.handshake_only else ("selected" if args.case else "public")
        kind = "image" if args.image else "binary"
        out_dir = RESULTS_ROOT / ("%s-%s-%s" % (prefix, kind, timestamp()))
    out_dir.mkdir(parents=True, exist_ok=False)

    runner = args.runner
    if runner == "auto":
        runner = "protocol-check" if os.name == "posix" else "portable"
    print("MiniVectorDB public protocol check")
    print("  target : %s %s" % ("image" if args.image else "binary",
                                args.image if args.image else args.binary))
    print("  runner : %s" % runner)
    print("  output : %s" % out_dir)
    print("  cases  : %s" % ("handshake" if args.handshake_only
                             else ", ".join(c["id"] for c in cases)))
    started = time.monotonic()

    try:
        if runner == "protocol-check":
            results = run_via_protocol_check(cases, args, out_dir)
        else:
            assertions = load_module("mvdb_protocol_assertions", PROTOCOL_CHECK / "_assertions.py")
            results = run_portable(cases, config, args, out_dir, assertions)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130

    for row in results:
        log = row.get("stderr_log")
        row["stderr_log"] = str(Path(log)) if log and Path(log).is_file() \
            else str(out_dir / (row["id"] + ".stderr.log"))
    attach_stderr_tails(results, out_dir)
    summary = build_summary(args, config, cases, results, out_dir, runner)
    summary["elapsed_seconds"] = round(time.monotonic() - started, 2)
    write_json(out_dir / "summary.json", summary)
    write_summary_markdown(summary, out_dir)
    print_table(results)
    print_failure_details(results)
    totals = summary["totals"]
    print("")
    print("RESULT: %s  (%d/%d passed, %d failed, %d timeout, %d error, %d skipped, %.1fs)"
          % (summary["verdict"], totals["PASS"], totals["total"], totals["FAIL"],
             totals["TIMEOUT"], totals["ERROR"], totals["SKIPPED"],
             summary["elapsed_seconds"]))
    print("summary: %s" % (out_dir / "summary.json"))
    print("markdown: %s" % (out_dir / "summary.md"))
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
