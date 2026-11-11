#!/usr/bin/env python3
"""One-command regression entry for MiniVectorDB (Windows and Linux).

Runs, against one native binary and in one process invocation:

1. protocol handshake: exact {"type":"ready"} then a clean shutdown;
2. all public mysqltest cases from mysqltest/manifest.json, using the
   cross-platform JSONL client (no Unix-only selectors, so it works on
   Windows where the official protocol_check/run.py cannot run);
3. the hidden-case hardening suite in tools/verify/edge_cases.py (invalid
   vectors, atomicity, crash durability, BIGINT, unicode, filter-first TopK,
   index maintenance, unsupported statements, VARCHAR length, large data).

Every public case and every edge case gets an isolated data directory and its
own process. stderr for each process is captured to a log file and shown on
failure.

Usage:
    python tools/verify/run_all.py --binary minivectordb/target/debug/minivectordb.exe
    python tools/verify/run_all.py --binary ... --xfail-group durability
    python tools/verify/run_all.py --binary ... --skip-edge --case sql_crud
    python tools/verify/run_all.py --binary ... --report results/run.json
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]  # seekdb/
PACKAGE = ROOT / "mysqltest"
CONFIG_PATH = ROOT / "protocol_check" / "config.json"
sys.path.insert(0, str(HERE))
from jsonl_client import ClientTimeout, ProtocolError, Session, strict_json  # noqa: E402

DEFAULT_STARTUP_TIMEOUT = 120.0
DEFAULT_REQUEST_TIMEOUT = 60.0

# Public cases whose only expected failure today is the stubbed DiskStorage:
# every one of them restarts the process and then reads previously written data.
KNOWN_PERSISTENCE_PUBLIC_CASES = {
    "storage_restart",
    "storage_mutations",
    "storage_multiple_tables",
    "vector_index_maintenance",
    "transaction_failed_insert",
    "transaction_autocommit_atomic",
    "transaction_shutdown_rollback",
}


# -- official assertion semantics -----------------------------------------


def same(actual, expected, abs_tol, rel_tol):
    if isinstance(expected, float):
        return (
            type(actual) in (int, float)
            and math.isfinite(actual)
            and math.isclose(actual, expected, abs_tol=abs_tol, rel_tol=rel_tol)
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(same(a, b, abs_tol, rel_tol) for a, b in zip(actual, expected))
        )
    return type(actual) is type(expected) and actual == expected


def assert_response(response, expected, config):
    if "error" in expected:
        assert response.get("ok") is False, "statement should fail"
        code = response.get("error", {}).get("code")
        assert isinstance(code, str) and code, "missing error.code"
        required = config.get("error_codes", {}).get(expected["error"])
        if required is not None:
            assert code == required, f"error.code {code!r} != {required!r}"
        return
    assert response.get("ok") is True, f"statement failed: {response}"
    for key in ("affected_rows", "columns"):
        if key in expected:
            assert same(
                response.get(key), expected[key], config["abs_tol"], config["rel_tol"]
            ), f"{key}: {response.get(key)!r} != {expected[key]!r}"
    if "rows" not in expected:
        return
    actual, wanted = response.get("rows"), expected["rows"]
    assert isinstance(actual, list), "missing rows"
    if expected.get("unordered"):
        remaining = list(actual)
        for row in wanted:
            index = next(
                (
                    i
                    for i, candidate in enumerate(remaining)
                    if same(candidate, row, config["abs_tol"], config["rel_tol"])
                ),
                None,
            )
            assert index is not None, f"missing row: {row!r}"
            remaining.pop(index)
        assert not remaining, f"unexpected rows: {remaining[:5]!r}"
    else:
        assert same(
            actual, wanted, config["abs_tol"], config["rel_tol"]
        ), f"rows: {actual!r} != {wanted!r}"


def within(root, relative):
    path = (root / relative).resolve(strict=True)
    path.relative_to(root.resolve())
    return path


# -- helpers ---------------------------------------------------------------


def configure_streams():
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(
                encoding="utf-8", errors="backslashreplace", line_buffering=True
            )
        except (AttributeError, ValueError):
            pass


def default_binary():
    names = ["minivectordb.exe", "minivectordb"] if os.name == "nt" else ["minivectordb"]
    for profile in ("debug", "release"):
        for name in names:
            candidate = ROOT / "minivectordb" / "target" / profile / name
            if candidate.exists():
                return str(candidate)
    return None


def session_stderr_suffix(session):
    tail = session.stderr_tail(1500)
    if not tail.strip():
        return ""
    return "\n  stderr tail:\n    " + "\n    ".join(tail.splitlines()[-8:])


# -- phases ----------------------------------------------------------------


def run_handshake(binary, args):
    work = tempfile.mkdtemp(prefix="mvdb-handshake-")
    data = os.path.join(work, "data")
    os.makedirs(data)
    session = None
    error = None
    started = time.monotonic()
    try:
        session = Session(
            [binary],
            data,
            os.path.join(work, "stderr.log"),
            startup_timeout=args.startup_timeout,
            request_timeout=args.request_timeout,
            name="handshake",
        )
        session.start()
        session.shutdown()
    except (ProtocolError, ClientTimeout) as exc:
        error = f"{type(exc).__name__}: {exc}"
    except Exception as exc:  # noqa: BLE001
        error = f"unexpected {type(exc).__name__}: {exc}"
    finally:
        if session is not None:
            if error is None:
                pass
            else:
                error = (error or "") + session_stderr_suffix(session)
            session.close()
        if not (error and args.keep_data):
            shutil.rmtree(work, ignore_errors=True)
    return {
        "id": "handshake",
        "status": "PASS" if error is None else "FAIL",
        "message": error or "",
        "duration_ms": round((time.monotonic() - started) * 1000),
    }


def run_public_case(case, config, binary, args):
    work = tempfile.mkdtemp(prefix="mvdb-public-")
    data = os.path.join(work, "data")
    os.makedirs(data)
    session = None
    error = None
    started = time.monotonic()
    try:
        expectations = strict_json(
            within(PACKAGE, case["result"]).read_text(encoding="utf-8")
        )
        statements = within(PACKAGE, case["test"]).read_text(encoding="utf-8").splitlines()
        session = Session(
            [binary],
            data,
            os.path.join(work, "stderr.log"),
            startup_timeout=args.startup_timeout,
            request_timeout=args.request_timeout,
            name=case["id"],
        )
        session.start()
        index, mode = 0, "exact"
        for line_number, text in enumerate(statements, 1):
            sql = text.strip()
            if not sql or sql.startswith("#"):
                continue
            if sql == "--restart":
                session.shutdown()
                session.start()
                continue
            if sql in ("--mode exact", "--mode ann"):
                mode = sql.split()[1]
                continue
            if sql.startswith("--"):
                raise AssertionError(f"line {line_number}: unsupported directive {sql!r}")
            if index >= len(expectations):
                raise AssertionError(f"line {line_number}: missing expected result for {sql!r}")
            response = session.execute(sql, mode=mode)
            try:
                assert_response(response, expectations[index], config)
            except AssertionError as exc:
                raise AssertionError(f"line {line_number}: {sql}: {exc}") from exc
            index += 1
        if index != len(expectations):
            raise AssertionError(
                f"SQL/result count mismatch: executed {index}, expected {len(expectations)}"
            )
        session.shutdown()
    except AssertionError as exc:
        error = str(exc)
    except (ProtocolError, ClientTimeout) as exc:
        error = f"{type(exc).__name__}: {exc}"
    except Exception as exc:  # noqa: BLE001
        error = f"unexpected {type(exc).__name__}: {exc}"
    finally:
        if session is not None:
            if error is not None:
                error += session_stderr_suffix(session)
            session.close()
        if not (error and args.keep_data):
            shutil.rmtree(work, ignore_errors=True)
    return {
        "id": case["id"],
        "group": case.get("group", ""),
        "status": "PASS" if error is None else "FAIL",
        "message": error or "",
        "duration_ms": round((time.monotonic() - started) * 1000),
    }


def run_edge_suite(binary, args):
    report_path = Path(tempfile.mkdtemp(prefix="mvdb-edge-report-")) / "edge.json"
    command = [
        sys.executable,
        str(HERE / "edge_cases.py"),
        "--binary",
        binary,
        "--report",
        str(report_path),
        "--large-rows",
        str(args.large_rows),
        "--startup-timeout",
        str(args.startup_timeout),
        "--request-timeout",
        str(args.request_timeout),
    ]
    for group in args.group or []:
        command += ["--group", group]
    for group in args.xfail_group or []:
        command += ["--xfail-group", group]
    if args.keep_data:
        command.append("--keep-data")
    sys.stdout.flush()
    sys.stderr.flush()
    started = time.monotonic()
    returncode = subprocess.run(command).returncode
    payload = None
    if report_path.exists():
        try:
            payload = strict_json(report_path.read_text(encoding="utf-8"))
        except ValueError:
            payload = None
        shutil.rmtree(report_path.parent, ignore_errors=True)
    return {
        "returncode": returncode,
        "duration_ms": round((time.monotonic() - started) * 1000),
        "report": payload,
    }


def main(argv=None):
    configure_streams()
    parser = argparse.ArgumentParser(
        description="Run handshake + public mysqltest cases + hidden edge cases."
    )
    parser.add_argument("--binary", help="database binary (default: debug build)")
    parser.add_argument("--case", action="append", help="public case id; repeatable")
    parser.add_argument(
        "--group",
        action="append",
        help="only run these edge-case groups; repeatable",
    )
    parser.add_argument(
        "--xfail-group",
        action="append",
        default=[],
        help="edge groups expected to fail (for example durability while storage is a stub)",
    )
    parser.add_argument(
        "--xfail-case",
        action="append",
        default=[],
        help="public case id expected to fail; repeatable (temporary known gaps)",
    )
    parser.add_argument(
        "--xfail-public-persistence",
        action="store_true",
        help="temporarily treat the 7 restart-dependent public cases as expected failures",
    )
    parser.add_argument("--skip-handshake", action="store_true")
    parser.add_argument("--skip-public", action="store_true")
    parser.add_argument("--skip-edge", action="store_true")
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument("--large-rows", type=int, default=20000)
    parser.add_argument("--startup-timeout", type=float, default=DEFAULT_STARTUP_TIMEOUT)
    parser.add_argument("--request-timeout", type=float, default=DEFAULT_REQUEST_TIMEOUT)
    parser.add_argument("--report", type=Path, help="write a combined JSON report")
    args = parser.parse_args(argv)

    binary = args.binary or default_binary()
    if not binary:
        parser.error("--binary is required (default debug binary not found)")
    binary = os.path.abspath(binary)
    if not os.path.exists(binary):
        parser.error(f"binary does not exist: {binary}")

    config = strict_json(CONFIG_PATH.read_text(encoding="utf-8"))
    manifest = strict_json((PACKAGE / "manifest.json").read_text(encoding="utf-8"))
    public_cases = manifest["cases"]
    if args.case:
        known = {case["id"] for case in public_cases}
        unknown = set(args.case) - known
        if unknown:
            parser.error("unknown public case: " + ", ".join(sorted(unknown)))
        public_cases = [case for case in public_cases if case["id"] in set(args.case)]

    started = time.monotonic()
    print("MiniVectorDB one-command regression")
    print(f"binary: {binary}")
    print("")

    handshake = None
    if not args.skip_handshake:
        print("=== Phase 1: protocol handshake ===")
        handshake = run_handshake(binary, args)
        print(f"{handshake['status']} handshake ({handshake['duration_ms']} ms)")
        if handshake["message"]:
            print("  " + handshake["message"].replace("\n", "\n  "))
        print("")

    xfail_ids = set(args.xfail_case)
    if args.xfail_public_persistence:
        xfail_ids |= KNOWN_PERSISTENCE_PUBLIC_CASES
    unknown_xfail = xfail_ids - {case["id"] for case in manifest["cases"]}
    if unknown_xfail:
        parser.error("unknown --xfail-case: " + ", ".join(sorted(unknown_xfail)))

    public_results = []
    if not args.skip_public:
        print(f"=== Phase 2: public mysqltest cases ({len(public_cases)}) ===")
        for case in public_cases:
            result = run_public_case(case, config, binary, args)
            result["xfail"] = result["status"] == "FAIL" and result["id"] in xfail_ids
            public_results.append(result)
            status = "XFAIL" if result.get("xfail") else result["status"]
            print(f"{status} {case['id']} ({result['duration_ms']} ms)")
            if result["message"]:
                print("  " + result["message"].replace("\n", "\n  "))
        passed = sum(1 for result in public_results if result["status"] == "PASS")
        xfailed = sum(1 for result in public_results if result.get("xfail"))
        suffix = f" ({xfailed} xfail)" if xfailed else ""
        print(f"public: {passed}/{len(public_results)} passed{suffix}")
        print("")

    edge = None
    if not args.skip_edge:
        print("=== Phase 3: hidden-case hardening suite ===")
        edge = run_edge_suite(binary, args)
        print("")

    public_failures = [
        r for r in public_results if r["status"] != "PASS" and not r.get("xfail")
    ]
    handshake_failed = handshake is not None and handshake["status"] != "PASS"
    edge_failed = edge is not None and edge["returncode"] != 0
    overall = not (handshake_failed or public_failures or edge_failed)

    print("=== Combined summary ===")
    if handshake is not None:
        print(f"handshake: {handshake['status']}")
    if not args.skip_public:
        print(
            f"public: {len(public_results) - len(public_failures)}/{len(public_results)} passed"
        )
        for result in public_results:
            if result["status"] == "PASS" or result.get("xfail"):
                continue
            print(f"  FAIL {result['id']}: {result['message'].splitlines()[0]}")
        for result in public_results:
            if result.get("xfail"):
                print(f"  XFAIL {result['id']}: {result['message'].splitlines()[0]}")
    if edge is not None:
        report = edge.get("report") or {}
        summary = report.get("cases")
        if summary:
            edge_passed = sum(1 for case in summary if case.get("status") == "PASS")
            print(f"edge: {edge_passed}/{len(summary)} passed, exit code {edge['returncode']}")
        else:
            print(f"edge: exit code {edge['returncode']}")
    print(f"overall: {'PASS' if overall else 'FAIL'} ({(time.monotonic() - started):.2f}s)")

    if args.report:
        payload = {
            "binary": binary,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(started)),
            "duration_ms": round((time.monotonic() - started) * 1000),
            "handshake": handshake,
            "public": public_results,
            "edge": edge,
            "overall": "PASS" if overall else "FAIL",
        }
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"Report: {args.report}")
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
