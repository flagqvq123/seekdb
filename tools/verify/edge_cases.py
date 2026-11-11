#!/usr/bin/env python3
"""Hidden-case hardening suite for MiniVectorDB (stdlib only, no pytest).

Derived from the official requirements in mysqltest/README.md that are not
fully covered by the 20 public samples:

* protocol strictness: exact ready handshake, id/type echo, boolean ok,
  single-line JSON, clean shutdown, errors do not kill the connection;
* invalid vectors: NaN/Infinity, dimension mismatch, float32 overflow, empty
  vector, non-numeric components -- always ok:false with non-empty error.code;
* statement atomicity: multi-row INSERT / UPDATE failures must not leave
  partial writes, in autocommit and explicit transaction modes;
* crash durability: committed data survives a hard kill, uncommitted
  transactions do not, repeated crash/recovery cycles keep working;
* BIGINT extremes (9223372036854775807 and its negation) with exact JSON
  integer round-trip and no float precision loss;
* unicode/escaping, ORDER BY ties broken by primary key, LIMIT 0, empty
  tables, multiple tables, duplicate primary key followed by continued use,
  empty SELECT results still returning columns.

Every case gets an isolated data directory and its own process(es). Output is a
PASS/FAIL line per case, a per-group summary and failure details (reason, last
request/response exchanges, stderr tail). Exit code is 0 only when all selected
cases pass; use --xfail-group GROUP to treat a group's failures as expected
(for example while the durable DiskStorage engine is still a stub).

Usage:
    python tools/verify/edge_cases.py --binary path/to/minivectordb
    python tools/verify/edge_cases.py --binary ... --group durability
    python tools/verify/edge_cases.py --binary ... --xfail-group durability
    python tools/verify/edge_cases.py --list
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
import tempfile
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jsonl_client import (  # noqa: E402
    ClientTimeout,
    ProtocolError,
    Session,
    make_workdir,
    test_root,
)

ABS_TOL = 1e-5
REL_TOL = 1e-5
MAX_BIGINT = 9223372036854775807
NEG_BIGINT = -9223372036854775807


class CaseFailure(AssertionError):
    """Raised when a case detects a product bug."""


def fail(message):
    raise CaseFailure(message)


def brief(value, limit=400):
    try:
        text = json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        text = repr(value)
    return text if len(text) <= limit else text[:limit] + "..."


def same(actual, expected, abs_tol=ABS_TOL, rel_tol=REL_TOL):
    """Official assertion semantics: exact types for ints/strings, tolerance for floats."""
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


def require_ok(response, what):
    if not isinstance(response, dict) or response.get("ok") is not True:
        fail(f"{what}: expected ok:true, got {brief(response)}")


def require_error(response, what, code=None):
    if not isinstance(response, dict) or response.get("ok") is not False:
        fail(f"{what}: expected ok:false, got {brief(response)}")
    error = response.get("error")
    if not isinstance(error, dict):
        fail(f"{what}: expected an error object, got {brief(response)}")
    actual_code = error.get("code")
    if not isinstance(actual_code, str) or not actual_code:
        fail(f"{what}: error.code must be a non-empty string, got {brief(response)}")
    if code is not None and actual_code != code:
        fail(f"{what}: expected error.code {code!r}, got {actual_code!r}")
    return actual_code


def check_affected(response, expected, what):
    require_ok(response, what)
    actual = response.get("affected_rows")
    if type(actual) is not int or actual != expected:
        fail(f"{what}: affected_rows {actual!r} != {expected!r}; response={brief(response)}")


def check_rows(response, columns, rows, what):
    require_ok(response, what)
    actual_columns = response.get("columns")
    if actual_columns != columns:
        fail(
            f"{what}: columns {brief(actual_columns)} != {brief(columns)}; "
            f"response={brief(response)}"
        )
    actual_rows = response.get("rows")
    if not isinstance(actual_rows, list):
        fail(f"{what}: rows must be an array; response={brief(response)}")
    if len(actual_rows) != len(rows):
        fail(
            f"{what}: row count {len(actual_rows)} != {len(rows)}; "
            f"rows={brief(actual_rows)} expected={brief(rows)}"
        )
    for index, (actual, expected) in enumerate(zip(actual_rows, rows)):
        if not isinstance(actual, list) or not same(actual, expected):
            fail(f"{what}: row {index} {brief(actual)} != {brief(expected)}")


def count_rows(session, table, what):
    response = session.execute(f"SELECT id FROM {table} ORDER BY id;")
    require_ok(response, what)
    rows = response.get("rows")
    if not isinstance(rows, list):
        fail(f"{what}: rows must be an array; response={brief(response)}")
    return rows


def ids(rows):
    return [row[0] for row in rows]


# ---------------------------------------------------------------------------
# Case registry and per-case environment
# ---------------------------------------------------------------------------


class Case:
    def __init__(self, group, name, func):
        self.group = group
        self.name = name
        self.func = func
        self.id = f"{group}.{name}"


CASES = []
KNOWN_GROUPS = (
    "protocol",
    "invalid_vector",
    "atomicity",
    "sql",
    "vector",
    "spec",
    "durability",
    "large_data",
)


def case(group, name):
    if group not in KNOWN_GROUPS:
        raise ValueError(f"unknown case group {group!r}")

    def decorator(func):
        CASES.append(Case(group, name, func))
        return func

    return decorator


class CaseEnv:
    """Owns one data directory and every database process opened for a case."""

    def __init__(self, binary, work_dir, startup_timeout, request_timeout, large_rows):
        self.binary = str(binary)
        self.work_dir = str(work_dir)
        self.data_dir = os.path.join(self.work_dir, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.startup_timeout = startup_timeout
        self.request_timeout = request_timeout
        self.large_rows = max(int(large_rows), 100)
        self.sessions = []
        self.exchanges = []
        self._count = 0

    def open(self):
        self._count += 1
        name = f"s{self._count}"
        log_path = os.path.join(self.work_dir, f"{name}.stderr.log")
        session = Session(
            [self.binary],
            self.data_dir,
            log_path,
            startup_timeout=self.startup_timeout,
            request_timeout=self.request_timeout,
            recorder=self._record,
            name=name,
        )
        self.sessions.append(session)
        session.start()
        return session

    def open_unready(self):
        """Spawn a process without waiting for ready (crash-during-recovery tests)."""
        self._count += 1
        name = f"s{self._count}"
        log_path = os.path.join(self.work_dir, f"{name}.stderr.log")
        session = Session(
            [self.binary],
            self.data_dir,
            log_path,
            startup_timeout=self.startup_timeout,
            request_timeout=self.request_timeout,
            recorder=self._record,
            name=name,
        )
        self.sessions.append(session)
        session.spawn()
        return session

    def _record(self, session, request, response):
        self.exchanges.append(
            {"session": session.name, "request": request, "response": response}
        )

    def close_all(self):
        for session in reversed(self.sessions):
            try:
                session.close()
            except Exception:  # noqa: BLE001 - cleanup must never mask the failure
                pass

    def stderr_tails(self, limit=1500):
        tails = []
        for session in self.sessions:
            tail = session.stderr_tail(limit)
            if tail.strip():
                tails.append((session.name, tail))
        return tails


class CaseResult:
    def __init__(self, case, error, duration, exchanges, tails, kept_dir=None):
        self.case = case
        self.error = error
        self.duration = duration
        self.exchanges = exchanges
        self.tails = tails
        self.kept_dir = kept_dir


def run_case(case, binary, args):
    work_dir = make_workdir("mvdb-edge-")
    env = CaseEnv(
        binary, work_dir, args.startup_timeout, args.request_timeout, args.large_rows
    )
    started = time.monotonic()
    error = None
    try:
        case.func(env)
    except CaseFailure as exc:
        error = str(exc) or "case failed"
    except (ProtocolError, ClientTimeout) as exc:
        error = f"{type(exc).__name__}: {exc}"
    except Exception:  # noqa: BLE001 - report unexpected harness/problem exceptions
        error = "unexpected exception:\n" + traceback.format_exc()
    duration = time.monotonic() - started
    env.close_all()
    tails = env.stderr_tails() if error else []
    exchanges = list(env.exchanges)
    kept_dir = None
    if error and args.keep_data:
        kept_dir = work_dir
    else:
        shutil.rmtree(work_dir, ignore_errors=True)
    return CaseResult(case, error, duration, exchanges, tails, kept_dir)


def print_failure(result):
    print(f"  reason: {result.error}")
    recent = result.exchanges[-8:]
    if recent:
        print("  last exchanges:")
        for exchange in recent:
            print(
                f"    [{exchange['session']}] -> {brief(exchange['request'], 300)}"
            )
            print(
                f"    [{exchange['session']}] <- {brief(exchange['response'], 300)}"
            )
    for name, tail in result.tails:
        print(f"  stderr[{name}] tail:")
        for line in tail.splitlines()[-12:]:
            print(f"    {line}")
    if result.kept_dir:
        print(f"  kept data directory: {result.kept_dir}")


# ---------------------------------------------------------------------------
# Group: protocol
# ---------------------------------------------------------------------------


@case("protocol", "handshake_execute_shutdown")
def protocol_handshake_execute_shutdown(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_affected(session.execute("INSERT INTO t VALUES (1, 'a');"), 1, "insert")
    check_rows(
        session.execute("SELECT id, v FROM t;"), ["id", "v"], [[1, "a"]], "select"
    )
    response = session.shutdown()
    if response.get("ok") is not True:
        fail(f"shutdown returned non-true ok: {brief(response)}")
    # shutdown() itself verifies: no extra stdout, exit code 0.


@case("protocol", "response_id_echo_type")
def protocol_response_id_echo_type(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY);"), "create table"
    )
    identifiers = [
        9007199254740993,  # beyond JS safe integer; must not lose precision
        0,
        -17,
        "req-中文-1",
        "",
        True,
    ]
    for ident in identifiers:
        response = session.execute("SELECT id FROM t;", ident=ident)
        if type(response.get("id")) is not type(ident) or response["id"] != ident:
            fail(
                f"id echo mismatch: sent {ident!r} ({type(ident).__name__}), "
                f"got {response.get('id')!r} ({type(response.get('id')).__name__})"
            )
        require_ok(response, f"select with id {ident!r}")

    for ident in (9223372036854775807, "err-中文-2"):
        response = session.execute("SELECT id FROM missing_table;", ident=ident)
        require_error(response, f"missing table with id {ident!r}")
        if type(response.get("id")) is not type(ident) or response["id"] != ident:
            fail(
                f"error-response id echo mismatch: sent {ident!r}, "
                f"got {response.get('id')!r}"
            )
    session.shutdown()


@case("protocol", "errors_continue_connection")
def protocol_errors_continue_connection(env):
    session = env.open()
    require_error(
        session.execute("SELECT id FROM missing_table;"), "select from missing table"
    )
    require_error(session.execute("SELEC 1;"), "syntax error")
    require_error(
        session.execute("CREATE TABLE bad (id BIGINT);"),
        "create table without primary key",
    )
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_affected(session.execute("INSERT INTO t VALUES (1, 'a');"), 1, "insert")
    require_error(
        session.execute("INSERT INTO t VALUES (1, 'dup');"), "duplicate primary key"
    )
    require_error(
        session.execute("ALTER TABLE t ADD x BIGINT;"), "unsupported ALTER TABLE"
    )
    require_error(
        session.execute("UPDATE t SET missing = 1;"), "update unknown column"
    )
    require_error(session.execute("INSERT INTO t VALUES (2);"), "column count mismatch")
    check_affected(session.execute("INSERT INTO t VALUES (2, 'b');"), 1, "insert after errors")
    check_rows(
        session.execute("SELECT id, v FROM t ORDER BY id;"),
        ["id", "v"],
        [[1, "a"], [2, "b"]],
        "connection still usable after errors",
    )
    session.shutdown()


@case("protocol", "response_matches_request_count")
def protocol_response_matches_request_count(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY);"), "create table"
    )
    for index in range(5):
        check_affected(
            session.execute(f"INSERT INTO t VALUES ({index});"), 1, f"insert {index}"
        )
    check_rows(
        session.execute("SELECT id FROM t ORDER BY id;"),
        ["id"],
        [[0], [1], [2], [3], [4]],
        "select",
    )
    # shutdown() fails on any unsolicited/extra stdout after the last response.
    session.shutdown()


# ---------------------------------------------------------------------------
# Group: invalid_vector
# ---------------------------------------------------------------------------


def create_vector_table(session, name="vecs", dim=2):
    require_ok(
        session.execute(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, embedding VECTOR({dim}));"
        ),
        f"create vector table {name}",
    )


def insert_vector(session, table, pk, literal, what):
    return session.execute(f"INSERT INTO {table} VALUES ({pk}, '{literal}');")


@case("invalid_vector", "nan_rejected")
def invalid_vector_nan_rejected(env):
    session = env.open()
    create_vector_table(session)
    for index, literal in enumerate(("[NaN,1]", "[nan,1]", "[-NaN,1]"), start=10):
        require_error(insert_vector(session, "vecs", index, literal, "NaN"), f"insert {literal}")
    if count_rows(session, "vecs", "after NaN inserts"):
        fail("an INSERT with a NaN vector left a row behind")
    check_affected(
        session.execute("INSERT INTO vecs VALUES (1, '[0,0]');"), 1, "insert valid vector"
    )
    require_error(
        session.execute("SELECT L2_DISTANCE(embedding, '[NaN,1]') AS d FROM vecs;"),
        "L2_DISTANCE query with NaN",
    )
    check_rows(
        session.execute("SELECT id FROM vecs ORDER BY id;"),
        ["id"],
        [[1]],
        "connection usable after NaN errors",
    )
    session.shutdown()


@case("invalid_vector", "infinity_rejected")
def invalid_vector_infinity_rejected(env):
    session = env.open()
    create_vector_table(session)
    literals = ("[Infinity,1]", "[-Infinity,1]", "[inf,0]", "[1e400,0]")
    for index, literal in enumerate(literals, start=10):
        require_error(insert_vector(session, "vecs", index, literal, "Infinity"), f"insert {literal}")
    if count_rows(session, "vecs", "after Infinity inserts"):
        fail("an INSERT with an Infinity vector left a row behind")
    check_affected(
        session.execute("INSERT INTO vecs VALUES (1, '[0,0]');"), 1, "insert valid vector"
    )
    require_error(
        session.execute("SELECT L2_DISTANCE(embedding, '[Infinity,1]') AS d FROM vecs;"),
        "L2_DISTANCE query with Infinity",
    )
    session.shutdown()


@case("invalid_vector", "float32_overflow_rejected")
def invalid_vector_float32_overflow_rejected(env):
    session = env.open()
    create_vector_table(session)
    literals = ("[3.4028236e38,0]", "[1e39,0]", "[-1e39,0]")
    for index, literal in enumerate(literals, start=10):
        require_error(insert_vector(session, "vecs", index, literal, "overflow"), f"insert {literal}")
    if count_rows(session, "vecs", "after overflow inserts"):
        fail("an INSERT with a float32-overflow vector left a row behind")
    check_affected(
        session.execute("INSERT INTO vecs VALUES (1, '[3.5e38,0]');"),
        0,
        "3.5e38 also overflows float32",
    ) if False else None
    # Valid maximum-magnitude float32 values must still be accepted.
    check_affected(
        session.execute("INSERT INTO vecs VALUES (2, '[3.4028235e38,0]');"),
        1,
        "max float32 value accepted",
    )
    session.shutdown()


@case("invalid_vector", "dimension_mismatch_rejected")
def invalid_vector_dimension_mismatch_rejected(env):
    session = env.open()
    create_vector_table(session)
    for index, literal in enumerate(("[1]", "[1,2,3]", "[]", "[ ]", "[,]", "[1,]"), start=10):
        require_error(insert_vector(session, "vecs", index, literal, "dimension"), f"insert {literal}")
    if count_rows(session, "vecs", "after dimension inserts"):
        fail("an INSERT with a wrong-dimension vector left a row behind")
    check_affected(
        session.execute("INSERT INTO vecs VALUES (1, '[3,4]');"), 1, "insert valid vector"
    )
    for literal in ("[0]", "[0,0,0]", "[]"):
        require_error(
            session.execute(
                f"SELECT L2_DISTANCE(embedding, '{literal}') AS d FROM vecs;"
            ),
            f"L2_DISTANCE query with dimension mismatch {literal}",
        )
    check_rows(
        session.execute("SELECT id FROM vecs ORDER BY id;"),
        ["id"],
        [[1]],
        "connection usable after dimension errors",
    )
    session.shutdown()


@case("invalid_vector", "empty_vector_rejected")
def invalid_vector_empty_rejected(env):
    session = env.open()
    create_vector_table(session)
    for index, literal in enumerate(("[]", "[ ]", "[  ,  ]"), start=10):
        require_error(insert_vector(session, "vecs", index, literal, "empty"), f"insert {literal}")
    if count_rows(session, "vecs", "after empty inserts"):
        fail("an INSERT with an empty vector left a row behind")
    session.shutdown()


@case("invalid_vector", "non_numeric_component_rejected")
def invalid_vector_non_numeric_rejected(env):
    session = env.open()
    create_vector_table(session)
    literals = ("[abc,1]", "[1,xyz]", "1,2", "[1;2]", "[]x", "[1 2]")
    for index, literal in enumerate(literals, start=10):
        require_error(insert_vector(session, "vecs", index, literal, "non-numeric"), f"insert {literal}")
    if count_rows(session, "vecs", "after non-numeric inserts"):
        fail("an INSERT with a non-numeric vector left a row behind")
    check_affected(
        session.execute("INSERT INTO vecs VALUES (1, '[1.5,-2.25]');"),
        1,
        "valid fractional vector accepted",
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Group: atomicity
# ---------------------------------------------------------------------------


@case("atomicity", "autocommit_multirow_insert")
def atomicity_autocommit_multirow_insert(env):
    session = env.open()
    create_vector_table(session)
    check_affected(
        session.execute("INSERT INTO vecs VALUES (1, '[0,0]');"), 1, "seed row"
    )
    # Second row fails coercion; the first row of the same statement must vanish.
    require_error(
        session.execute("INSERT INTO vecs VALUES (2, '[1,1]'), (3, '[1]');"),
        "multi-row insert with a bad second vector",
    )
    check_rows(
        session.execute("SELECT id FROM vecs ORDER BY id;"),
        ["id"],
        [[1]],
        "no partial rows after failed autocommit INSERT",
    )
    check_affected(
        session.execute("INSERT INTO vecs VALUES (2, '[1,1]'), (3, '[2,2]');"),
        2,
        "retry succeeds",
    )
    check_rows(
        session.execute("SELECT id FROM vecs ORDER BY id;"),
        ["id"],
        [[1], [2], [3]],
        "all rows present after successful retry",
    )
    session.shutdown()


@case("atomicity", "autocommit_update_failure")
def atomicity_autocommit_update_failure(env):
    session = env.open()
    create_vector_table(session)
    check_affected(
        session.execute(
            "INSERT INTO vecs VALUES (1, '[3,4]'), (2, '[0,1]');"
        ),
        2,
        "seed rows",
    )
    require_error(
        session.execute("UPDATE vecs SET embedding = '[NaN,1]' WHERE id >= 1;"),
        "update with invalid vector",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM vecs ORDER BY id;"
        ),
        ["id", "d"],
        [[1, 5.0], [2, 1.0]],
        "no row partially updated",
    )
    require_error(
        session.execute("UPDATE vecs SET embedding = '[1]' WHERE id = 2;"),
        "update with wrong dimension",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM vecs ORDER BY id;"
        ),
        ["id", "d"],
        [[1, 5.0], [2, 1.0]],
        "rows unchanged after second failed update",
    )
    check_affected(
        session.execute("UPDATE vecs SET embedding = '[6,8]' WHERE id = 1;"),
        1,
        "valid update",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM vecs ORDER BY id;"
        ),
        ["id", "d"],
        [[1, 10.0], [2, 1.0]],
        "valid update applied",
    )
    session.shutdown()


@case("atomicity", "transaction_failed_insert")
def atomicity_transaction_failed_insert(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, content VARCHAR(16), embedding VECTOR(2));"
        ),
        "create docs",
    )
    check_affected(
        session.execute("INSERT INTO docs VALUES (1, '初始', '[0,0]');"),
        1,
        "seed row",
    )
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(
        session.execute("UPDATE docs SET content = '保留' WHERE id = 1;"),
        1,
        "update inside transaction",
    )
    require_error(
        session.execute(
            "INSERT INTO docs VALUES (2, '新', '[1,1]'), (1, '重复', '[0,0]');"
        ),
        "multi-row insert with duplicate primary key inside transaction",
    )
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "保留"]],
        "failed statement rolled back only itself",
    )
    check_affected(
        session.execute("INSERT INTO docs VALUES (2, '新', '[1,1]');"),
        1,
        "successful insert after failed statement",
    )
    require_ok(session.execute("COMMIT;"), "commit")
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "保留"], [2, "新"]],
        "prior update and later insert committed together",
    )
    session.shutdown()


@case("atomicity", "transaction_failed_then_rollback")
def atomicity_transaction_failed_then_rollback(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_affected(session.execute("INSERT INTO t VALUES (1, 'a');"), 1, "seed row")
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("INSERT INTO t VALUES (2, 'b');"), 1, "insert in txn")
    require_error(
        session.execute("INSERT INTO t VALUES (3, 'c'), (2, 'dup');"),
        "failed multi-row insert inside transaction",
    )
    require_ok(session.execute("ROLLBACK;"), "rollback")
    check_rows(
        session.execute("SELECT id, v FROM t ORDER BY id;"),
        ["id", "v"],
        [[1, "a"]],
        "rollback discarded the whole transaction, including the earlier valid insert",
    )
    check_affected(session.execute("INSERT INTO t VALUES (4, 'd');"), 1, "insert after rollback")
    session.shutdown()


@case("atomicity", "transaction_commit_rollback_visibility")
def atomicity_transaction_commit_rollback_visibility(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_affected(session.execute("INSERT INTO t VALUES (1, 'orig');"), 1, "seed row")
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("UPDATE t SET v = 'temp' WHERE id = 1;"), 1, "update")
    check_rows(
        session.execute("SELECT v FROM t WHERE id = 1;"),
        ["v"],
        [["temp"]],
        "transaction sees its own writes",
    )
    require_ok(session.execute("ROLLBACK;"), "rollback")
    check_rows(
        session.execute("SELECT v FROM t WHERE id = 1;"),
        ["v"],
        [["orig"]],
        "rollback restores the original value",
    )
    require_ok(session.execute("BEGIN;"), "begin again")
    check_affected(session.execute("UPDATE t SET v = 'final' WHERE id = 1;"), 1, "update")
    require_ok(session.execute("COMMIT;"), "commit")
    check_rows(
        session.execute("SELECT v FROM t WHERE id = 1;"),
        ["v"],
        [["final"]],
        "commit persists the new value",
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Group: sql
# ---------------------------------------------------------------------------


@case("sql", "bigint_extremes")
def sql_bigint_extremes(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE b (id BIGINT PRIMARY KEY, value BIGINT);"
        ),
        "create bigint table",
    )
    check_affected(
        session.execute(
            f"INSERT INTO b VALUES ({MAX_BIGINT}, {MAX_BIGINT}), "
            f"({NEG_BIGINT}, {NEG_BIGINT}), (0, 0);"
        ),
        3,
        "insert BIGINT extremes",
    )
    check_rows(
        session.execute("SELECT id, value FROM b ORDER BY id;"),
        ["id", "value"],
        [[NEG_BIGINT, NEG_BIGINT], [0, 0], [MAX_BIGINT, MAX_BIGINT]],
        "exact BIGINT round-trip",
    )
    response = session.execute(f"SELECT value FROM b WHERE id = {MAX_BIGINT};")
    check_rows(response, ["value"], [[MAX_BIGINT]], "select max BIGINT")
    if type(response["rows"][0][0]) is not int:
        fail("MAX_BIGINT came back as a non-integer JSON value")
    response = session.execute(f"SELECT id FROM b WHERE value = {NEG_BIGINT};")
    check_rows(response, ["id"], [[NEG_BIGINT]], "select by negative BIGINT")
    if type(response["rows"][0][0]) is not int:
        fail("negative BIGINT came back as a non-integer JSON value")
    require_error(
        session.execute("INSERT INTO b VALUES (9223372036854775808, 1);"),
        "integer literal one above BIGINT max",
    )
    check_rows(
        session.execute("SELECT id FROM b ORDER BY id;"),
        ["id"],
        [[NEG_BIGINT], [0], [MAX_BIGINT]],
        "out-of-range literal did not wrap or insert",
    )
    session.shutdown()


@case("sql", "unicode_and_escaping")
def sql_unicode_and_escaping(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE u (id BIGINT PRIMARY KEY, content VARCHAR(64));"
        ),
        "create unicode table",
    )
    values = [
        (1, "中文"),
        (2, "你好，世界！"),
        (3, "😀🚀"),
        (4, "O'Brien"),
        (5, "a" + chr(92) + "b"),
        (6, "line1" + chr(10) + "line2"),
    ]
    for pk, value in values:
        escaped = value.replace("'", "''")
        check_affected(
            session.execute(f"INSERT INTO u VALUES ({pk}, '{escaped}');"),
            1,
            f"insert unicode row {pk}",
        )
    check_rows(
        session.execute("SELECT id, content FROM u ORDER BY id;"),
        ["id", "content"],
        [[pk, value] for pk, value in values],
        "unicode/escape round-trip",
    )
    check_rows(
        session.execute("SELECT content FROM u WHERE id = 1;"),
        ["content"],
        [["中文"]],
        "filter by unicode value",
    )
    session.shutdown()


@case("sql", "order_by_ties_by_primary_key")
def sql_order_by_ties_by_primary_key(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, value BIGINT);"),
        "create table",
    )
    check_affected(
        session.execute("INSERT INTO t VALUES (3, 10), (1, 10), (2, 10), (5, 5);"),
        4,
        "insert tied rows",
    )
    check_rows(
        session.execute("SELECT id FROM t ORDER BY value;"),
        ["id"],
        [[5], [1], [2], [3]],
        "ascending ties broken by primary key",
    )
    create_vector_table(session, name="v")
    check_affected(
        session.execute(
            "INSERT INTO v VALUES (9, '[1,0]'), (5, '[-1,0]'), (2, '[0,1]'), (7, '[3,4]');"
        ),
        4,
        "insert vector ties",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM v ORDER BY d LIMIT 3;"
        ),
        ["id", "d"],
        [[2, 1.0], [5, 1.0], [9, 1.0]],
        "equal distances ordered by primary key",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM v ORDER BY d LIMIT 2;"
        ),
        ["id", "d"],
        [[2, 1.0], [5, 1.0]],
        "TopK with ties picks lowest primary keys",
    )
    session.shutdown()


@case("sql", "limit_zero")
def sql_limit_zero(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_affected(
        session.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');"),
        3,
        "insert rows",
    )
    check_rows(
        session.execute("SELECT id FROM t LIMIT 0;"), ["id"], [], "LIMIT 0"
    )
    check_rows(
        session.execute("SELECT id FROM t ORDER BY id LIMIT 0;"),
        ["id"],
        [],
        "ORDER BY ... LIMIT 0",
    )
    check_rows(
        session.execute("SELECT id, v FROM t ORDER BY id DESC LIMIT 0;"),
        ["id", "v"],
        [],
        "ORDER BY DESC LIMIT 0 keeps columns",
    )
    check_rows(
        session.execute("SELECT id FROM t ORDER BY id LIMIT 1;"),
        ["id"],
        [[1]],
        "LIMIT 1 still works",
    )
    session.shutdown()


@case("sql", "empty_table")
def sql_empty_table(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_rows(
        session.execute("SELECT id, v FROM t;"),
        ["id", "v"],
        [],
        "empty table select returns columns",
    )
    check_rows(
        session.execute("SELECT * FROM t;"),
        ["id", "v"],
        [],
        "SELECT * on empty table returns columns",
    )
    check_rows(
        session.execute("SELECT id FROM t WHERE id = 1;"),
        ["id"],
        [],
        "empty table filtered select returns columns",
    )
    check_affected(session.execute("DELETE FROM t;"), 0, "delete from empty table")
    check_affected(session.execute("UPDATE t SET v = 'x';"), 0, "update empty table")
    check_rows(session.execute("SELECT id FROM t;"), ["id"], [], "still empty")
    session.shutdown()


@case("sql", "select_empty_result_columns")
def sql_select_empty_result_columns(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, content VARCHAR(16), embedding VECTOR(2));"
        ),
        "create docs",
    )
    check_affected(
        session.execute(
            "INSERT INTO docs VALUES (1, 'a', '[0,0]'), (2, 'b', '[1,1]');"
        ),
        2,
        "insert rows",
    )
    check_rows(
        session.execute("SELECT id, content FROM docs WHERE id = 999;"),
        ["id", "content"],
        [],
        "filtered empty result keeps columns",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d "
            "FROM docs WHERE content = 'missing' ORDER BY d LIMIT 5;"
        ),
        ["id", "d"],
        [],
        "empty vector result keeps expression columns",
    )
    session.shutdown()


@case("sql", "multiple_tables")
def sql_multiple_tables(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table a",
    )
    require_ok(
        session.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, value BIGINT);"),
        "create table b",
    )
    require_ok(
        session.execute("CREATE TABLE c (id BIGINT PRIMARY KEY, note VARCHAR(64));"),
        "create table c",
    )
    check_affected(session.execute("INSERT INTO a VALUES (1, 'a1');"), 1, "insert a")
    check_affected(session.execute("INSERT INTO b VALUES (1, 100);"), 1, "insert b")
    check_affected(session.execute("INSERT INTO c VALUES (1, 'c1');"), 1, "insert c")
    check_rows(session.execute("SELECT id, v FROM a;"), ["id", "v"], [[1, "a1"]], "select a")
    check_rows(
        session.execute("SELECT id, value FROM b;"), ["id", "value"], [[1, 100]], "select b"
    )
    check_rows(
        session.execute("SELECT id, note FROM c;"), ["id", "note"], [[1, "c1"]], "select c"
    )
    check_affected(
        session.execute("UPDATE b SET value = 200 WHERE id = 1;"), 1, "update b"
    )
    check_rows(
        session.execute("SELECT value FROM b WHERE id = 1;"),
        ["value"],
        [[200]],
        "b updated",
    )
    check_rows(
        session.execute("SELECT v FROM a WHERE id = 1;"),
        ["v"],
        [["a1"]],
        "a unaffected",
    )
    check_affected(session.execute("DELETE FROM c WHERE id = 1;"), 1, "delete c")
    check_rows(session.execute("SELECT id FROM c;"), ["id"], [], "c emptied")
    session.shutdown()


@case("sql", "duplicate_key_then_continue")
def sql_duplicate_key_then_continue(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE docs (id BIGINT PRIMARY KEY, content VARCHAR(16));"),
        "create docs",
    )
    check_affected(
        session.execute("INSERT INTO docs VALUES (1, 'orig');"), 1, "seed row"
    )
    require_error(
        session.execute("INSERT INTO docs VALUES (1, 'dup');"),
        "duplicate primary key",
    )
    check_rows(
        session.execute("SELECT id, content FROM docs;"),
        ["id", "content"],
        [[1, "orig"]],
        "duplicate insert did not overwrite",
    )
    check_affected(
        session.execute("INSERT INTO docs VALUES (2, 'two');"), 1, "insert after duplicate"
    )
    require_error(
        session.execute("INSERT INTO docs VALUES (3, 'a'), (3, 'b');"),
        "duplicate primary key within one multi-row statement",
    )
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "orig"], [2, "two"]],
        "self-duplicate statement wrote nothing",
    )
    check_affected(
        session.execute("UPDATE docs SET content = 'updated' WHERE id = 2;"),
        1,
        "update after duplicate",
    )
    check_affected(session.execute("DELETE FROM docs WHERE id = 2;"), 1, "delete after duplicate")
    check_rows(
        session.execute("SELECT id, content FROM docs;"),
        ["id", "content"],
        [[1, "orig"]],
        "connection usable after duplicate-key errors",
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Group: durability (expected to fail while DiskStorage is still a stub)
# ---------------------------------------------------------------------------


@case("durability", "restart_autocommit_persists")
def durability_restart_autocommit_persists(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE docs (id BIGINT PRIMARY KEY, content VARCHAR(16));"),
        "create docs",
    )
    check_affected(
        session.execute("INSERT INTO docs VALUES (1, '初始');"), 1, "insert"
    )
    session.shutdown()
    session = env.open()
    check_rows(
        session.execute("SELECT id, content FROM docs;"),
        ["id", "content"],
        [[1, "初始"]],
        "autocommit row survives normal restart",
    )
    session.shutdown()


@case("durability", "restart_accumulates")
def durability_restart_accumulates(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY);"), "create table"
    )
    check_affected(session.execute("INSERT INTO t VALUES (1);"), 1, "insert 1")
    session.shutdown()
    session = env.open()
    check_affected(session.execute("INSERT INTO t VALUES (2);"), 1, "insert 2")
    session.shutdown()
    session = env.open()
    check_affected(session.execute("INSERT INTO t VALUES (3);"), 1, "insert 3")
    session.shutdown()
    session = env.open()
    check_rows(
        session.execute("SELECT id FROM t ORDER BY id;"),
        ["id"],
        [[1], [2], [3]],
        "all commits survive repeated restarts",
    )
    session.shutdown()


@case("durability", "restart_preserves_mutations")
def durability_restart_preserves_mutations(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, content VARCHAR(16), embedding VECTOR(2));"
        ),
        "create docs",
    )
    check_affected(
        session.execute(
            "INSERT INTO docs VALUES (1, 'a', '[3,4]'), (2, 'b', '[0,1]');"
        ),
        2,
        "insert rows",
    )
    check_affected(
        session.execute(
            "UPDATE docs SET content = 'a2', embedding = '[6,8]' WHERE id = 1;"
        ),
        1,
        "update",
    )
    check_affected(session.execute("DELETE FROM docs WHERE id = 2;"), 1, "delete")
    session.shutdown()
    session = env.open()
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "a2"]],
        "update/delete persisted",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs ORDER BY id;"
        ),
        ["id", "d"],
        [[1, 10.0]],
        "updated vector persisted",
    )
    session.shutdown()


@case("durability", "crash_committed_survives")
def durability_crash_committed_survives(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_affected(session.execute("INSERT INTO t VALUES (1, 'kept');"), 1, "insert")
    session.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id, v FROM t;"),
        ["id", "v"],
        [[1, "kept"]],
        "committed row survives hard kill",
    )
    session.shutdown()


@case("durability", "crash_deleted_stays_deleted")
def durability_crash_deleted_stays_deleted(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_affected(
        session.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b');"), 2, "insert"
    )
    check_affected(session.execute("DELETE FROM t WHERE id = 2;"), 1, "delete")
    session.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id, v FROM t;"),
        ["id", "v"],
        [[1, "a"]],
        "delete survives hard kill",
    )
    session.shutdown()


@case("durability", "crash_uncommitted_txn_absent")
def durability_crash_uncommitted_txn_absent(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_affected(session.execute("INSERT INTO t VALUES (1, 'committed');"), 1, "committed insert")
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("INSERT INTO t VALUES (2, 'uncommitted');"), 1, "uncommitted insert")
    check_rows(
        session.execute("SELECT id FROM t ORDER BY id;"),
        ["id"],
        [[1], [2]],
        "transaction sees its own insert",
    )
    session.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id, v FROM t;"),
        ["id", "v"],
        [[1, "committed"]],
        "uncommitted transaction is not persisted",
    )
    session.shutdown()


@case("durability", "crash_multiple_recoveries")
def durability_crash_multiple_recoveries(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY);"), "create table"
    )
    check_affected(session.execute("INSERT INTO t VALUES (1);"), 1, "insert 1")
    session.crash()
    session = env.open()
    check_affected(session.execute("INSERT INTO t VALUES (2);"), 1, "insert 2")
    session.crash()
    session = env.open()
    check_affected(session.execute("INSERT INTO t VALUES (3);"), 1, "insert 3")
    session.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id FROM t ORDER BY id;"),
        ["id"],
        [[1], [2], [3]],
        "all commits survive repeated crash recovery",
    )
    session.shutdown()


@case("durability", "crash_immediately_after_ready")
def durability_crash_immediately_after_ready(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_affected(session.execute("INSERT INTO t VALUES (1, 'kept');"), 1, "insert")
    session.shutdown()
    # Startup replays the log; kill right after ready, before any request.
    session = env.open()
    session.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id, v FROM t;"),
        ["id", "v"],
        [[1, "kept"]],
        "data survives a crash right after startup recovery",
    )
    session.shutdown()


@case("durability", "shutdown_rolls_back_uncommitted")
def durability_shutdown_rolls_back_uncommitted(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_affected(session.execute("INSERT INTO t VALUES (1, 'committed');"), 1, "committed insert")
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("INSERT INTO t VALUES (2, 'uncommitted');"), 1, "uncommitted insert")
    session.shutdown()
    session = env.open()
    check_rows(
        session.execute("SELECT id, v FROM t;"),
        ["id", "v"],
        [[1, "committed"]],
        "normal shutdown rolled back the open transaction",
    )
    session.shutdown()


@case("durability", "restart_multiple_tables")
def durability_restart_multiple_tables(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE a (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table a",
    )
    require_ok(
        session.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, value BIGINT);"),
        "create table b",
    )
    check_affected(session.execute("INSERT INTO a VALUES (1, 'a1');"), 1, "insert a")
    check_affected(session.execute("INSERT INTO b VALUES (1, 100);"), 1, "insert b")
    session.shutdown()
    session = env.open()
    check_rows(session.execute("SELECT id, v FROM a;"), ["id", "v"], [[1, "a1"]], "table a persisted")
    check_rows(
        session.execute("SELECT id, value FROM b;"), ["id", "value"], [[1, 100]], "table b persisted"
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Group: vector (float32 storage semantics, filtered TopK, index maintenance)
# ---------------------------------------------------------------------------


@case("vector", "distance_uses_stored_float32")
def vector_distance_uses_stored_float32(env):
    session = env.open()
    create_vector_table(session, name="v")
    check_affected(
        session.execute(
            "INSERT INTO v VALUES (1, '[16777217,0]'), (2, '[16777216,0]'), "
            "(3, '[0.1,0]'), (4, '[3,4]');"
        ),
        4,
        "insert vectors that stress float32 rounding",
    )
    # 16777217 is not representable in f32; it must be stored as 16777216.
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[16777216,0]') AS d "
            "FROM v WHERE id <= 2 ORDER BY id;"
        ),
        ["id", "d"],
        [[1, 0.0], [2, 0.0]],
        "stored vector distance uses float32 rounding",
    )
    # Query literals are also converted to f32 before the distance is computed.
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[16777217,0]') AS d "
            "FROM v WHERE id <= 2 ORDER BY id;"
        ),
        ["id", "d"],
        [[1, 0.0], [2, 0.0]],
        "query literal rounded to float32",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0.1,0]') AS d FROM v WHERE id = 3;"
        ),
        ["id", "d"],
        [[3, 0.0]],
        "0.1 stored and queried as f32",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM v WHERE id = 4;"
        ),
        ["id", "d"],
        [[4, 5.0]],
        "plain Euclidean distance",
    )
    check_affected(
        session.execute("UPDATE v SET embedding = '[16777217,0]' WHERE id = 2;"),
        1,
        "update with rounding vector",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[16777216,0]') AS d FROM v WHERE id = 2;"
        ),
        ["id", "d"],
        [[2, 0.0]],
        "updated vector is rounded to float32 too",
    )
    session.shutdown()


@case("vector", "filtered_topk_is_filter_first")
def vector_filtered_topk_is_filter_first(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE ft (id BIGINT PRIMARY KEY, category VARCHAR(64), embedding VECTOR(2));"
        ),
        "create table",
    )
    check_affected(
        session.execute(
            "INSERT INTO ft VALUES (1, 'skip', '[0,0]'), (2, 'skip', '[0,0]'), "
            "(3, 'keep', '[5,0]'), (4, 'keep', '[3,0]'), (5, 'other', '[1,0]');"
        ),
        5,
        "insert rows",
    )
    filtered = (
        "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM ft "
        "WHERE category = 'keep' ORDER BY d LIMIT "
    )
    # Global top-1 would be a 'skip' row at distance 0; TopK must be computed
    # over the filtered set only.
    check_rows(
        session.execute(filtered + "1;"), ["id", "d"], [[4, 3.0]], "filter-first TopK"
    )
    check_rows(
        session.execute(filtered + "5;"),
        ["id", "d"],
        [[4, 3.0], [3, 5.0]],
        "fewer than K matches returns all of them",
    )
    check_rows(
        session.execute(filtered.replace("'keep'", "'absent'") + "5;"),
        ["id", "d"],
        [],
        "no matching rows returns empty result with columns",
    )
    for limit in (1, 5):
        check_rows(
            session.execute(filtered + f"{limit};", mode="ann"),
            ["id", "d"],
            [[4, 3.0], [3, 5.0]][:limit],
            f"ann mode filtered TopK limit {limit}",
        )
    # Equal distances are broken by ascending primary key.
    require_ok(
        session.execute(
            "CREATE TABLE ties (id BIGINT PRIMARY KEY, category VARCHAR(64), embedding VECTOR(2));"
        ),
        "create ties table",
    )
    check_affected(
        session.execute(
            "INSERT INTO ties VALUES (10, 'keep', '[1,0]'), (7, 'keep', '[-1,0]'), "
            "(9, 'keep', '[0,1]');"
        ),
        3,
        "insert tied rows",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM ties "
            "WHERE category = 'keep' ORDER BY d LIMIT 3;"
        ),
        ["id", "d"],
        [[7, 1.0], [9, 1.0], [10, 1.0]],
        "tied distances ordered by primary key",
    )
    session.shutdown()


@case("vector", "index_maintenance_live")
def vector_index_maintenance_live(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, category VARCHAR(64), embedding VECTOR(2));"
        ),
        "create docs",
    )
    check_affected(
        session.execute(
            "INSERT INTO docs VALUES (3, '甲', '[0,2]'), (1, '甲', '[0,0]'), (2, '甲', '[1,0]');"
        ),
        3,
        "insert rows",
    )
    require_ok(
        session.execute("CREATE VECTOR INDEX docs_vec ON docs(embedding);"),
        "create vector index",
    )
    check_affected(
        session.execute("UPDATE docs SET embedding = '[3,4]' WHERE id = 1;"),
        1,
        "update vector after index",
    )
    check_affected(session.execute("DELETE FROM docs WHERE id = 2;"), 1, "delete after index")
    check_affected(
        session.execute("INSERT INTO docs VALUES (4, '甲', '[0,0]'), (5, '乙', '[9,9]');"),
        2,
        "insert after index",
    )
    expected_keep = [[4, 0.0], [3, 2.0], [1, 5.0]]
    sql_keep = (
        "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs "
        "WHERE category = '甲' ORDER BY d LIMIT 10;"
    )
    check_rows(session.execute(sql_keep), ["id", "d"], expected_keep, "exact filtered TopK")
    check_rows(
        session.execute(sql_keep, mode="ann"),
        ["id", "d"],
        expected_keep,
        "ann filtered TopK after index maintenance",
    )
    sql_all = (
        "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs "
        "ORDER BY d LIMIT 10;"
    )
    expected_all = [[4, 0.0], [3, 2.0], [1, 5.0], [5, math.sqrt(162.0)]]
    check_rows(session.execute(sql_all), ["id", "d"], expected_all, "exact TopK after index maintenance")
    check_rows(
        session.execute(sql_all, mode="ann"),
        ["id", "d"],
        expected_all,
        "ann TopK after index maintenance",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs "
            "WHERE category = 'absent' ORDER BY d LIMIT 10;"
        ),
        ["id", "d"],
        [],
        "empty filtered result has columns",
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Group: spec (formal requirements: unsupported statements, VARCHAR, ordering)
# ---------------------------------------------------------------------------


@case("spec", "unsupported_statements_error")
def spec_unsupported_statements_error(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(16), embedding VECTOR(2));"
        ),
        "create table",
    )
    check_affected(
        session.execute("INSERT INTO t VALUES (1, 'x', '[0,0]');"), 1, "seed row"
    )
    unsupported = [
        ("JOIN", "SELECT t.id FROM t JOIN t2 ON t.id = t2.id;"),
        ("GROUP BY", "SELECT v FROM t GROUP BY v;"),
        ("aggregate COUNT", "SELECT COUNT(*) FROM t;"),
        ("subquery", "SELECT id FROM (SELECT id FROM t) AS x;"),
        ("ALTER TABLE", "ALTER TABLE t ADD extra BIGINT;"),
        (
            "foreign key",
            "CREATE TABLE fk (id BIGINT PRIMARY KEY, ref BIGINT, "
            "FOREIGN KEY (ref) REFERENCES t(id));",
        ),
        ("UPDATE primary key", "UPDATE t SET id = 2 WHERE id = 1;"),
        ("IS NULL", "SELECT id FROM t WHERE v IS NULL;"),
        ("UNION", "SELECT id FROM t UNION SELECT id FROM t;"),
        ("DROP TABLE", "DROP TABLE t;"),
        ("CREATE INDEX", "CREATE INDEX idx ON t(v);"),
        ("NULL primary key", "INSERT INTO t VALUES (NULL, 'n', '[0,0]');"),
        ("NULL non-key column", "INSERT INTO t VALUES (2, NULL, '[0,0]');"),
    ]
    for label, sql in unsupported:
        require_error(session.execute(sql), f"unsupported statement: {label}")
    # Nested transaction must error instead of silently starting a second one.
    require_ok(session.execute("BEGIN;"), "begin")
    require_error(session.execute("BEGIN;"), "nested BEGIN")
    require_ok(session.execute("ROLLBACK;"), "rollback after nested BEGIN error")
    # Nothing above may have silently mutated or dropped the table.
    check_rows(
        session.execute("SELECT id, v FROM t ORDER BY id;"),
        ["id", "v"],
        [[1, "x"]],
        "unsupported statements left data intact",
    )
    session.shutdown()


@case("spec", "varchar_length_unicode_chars")
def spec_varchar_length_unicode_chars(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, s VARCHAR(16));"),
        "create table",
    )
    sixteen_ascii = "a" * 16
    sixteen_cjk = "中" * 16
    sixteen_emoji = "😀" * 16
    check_affected(
        session.execute(f"INSERT INTO t VALUES (1, '{sixteen_ascii}');"),
        1,
        "16 ASCII characters accepted",
    )
    check_affected(
        session.execute(f"INSERT INTO t VALUES (2, '{sixteen_cjk}');"),
        1,
        "16 Unicode characters (Chinese) accepted",
    )
    check_affected(
        session.execute(f"INSERT INTO t VALUES (3, '{sixteen_emoji}');"),
        1,
        "16 Unicode characters (4-byte emoji) accepted",
    )
    require_error(
        session.execute("INSERT INTO t VALUES (10, '" + "a" * 17 + "');"),
        "17 ASCII characters rejected",
    )
    require_error(
        session.execute("INSERT INTO t VALUES (11, '" + "中" * 17 + "');"),
        "17 Chinese characters rejected (character count, not bytes)",
    )
    require_error(
        session.execute("INSERT INTO t VALUES (12, '" + "😀" * 17 + "');"),
        "17 emoji rejected (4-byte UTF-8 still counts as one character)",
    )
    check_rows(
        session.execute("SELECT id, s FROM t ORDER BY id;"),
        ["id", "s"],
        [[1, sixteen_ascii], [2, sixteen_cjk], [3, sixteen_emoji]],
        "over-length inserts left no rows",
    )
    require_error(
        session.execute("UPDATE t SET s = '" + "b" * 17 + "' WHERE id = 1;"),
        "UPDATE to 17 characters rejected",
    )
    check_rows(
        session.execute("SELECT s FROM t WHERE id = 1;"),
        ["s"],
        [[sixteen_ascii]],
        "failed over-length UPDATE left the old value",
    )
    check_affected(
        session.execute("UPDATE t SET s = '" + "b" * 16 + "' WHERE id = 1;"),
        1,
        "UPDATE to exactly 16 characters accepted",
    )
    session.shutdown()


@case("spec", "string_compare_by_code_point")
def spec_string_compare_by_code_point(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, s VARCHAR(32));"),
        "create table",
    )
    values = [(1, "A"), (2, "a"), (3, "中"), (4, "😀"), (5, "Z"), (6, "中a")]
    for pk, value in values:
        check_affected(
            session.execute(f"INSERT INTO t VALUES ({pk}, '{value}');"),
            1,
            f"insert {value!r}",
        )
    # Unicode code point order: A(65) < Z(90) < a(97) < 中(20013) < 中a < 😀(128512).
    check_rows(
        session.execute("SELECT id FROM t ORDER BY s;"),
        ["id"],
        [[1], [5], [2], [3], [6], [4]],
        "ORDER BY uses Unicode code point order",
    )
    check_rows(
        session.execute("SELECT id FROM t WHERE s >= 'a' ORDER BY s;"),
        ["id"],
        [[2], [3], [6], [4]],
        "comparison uses Unicode code point order",
    )
    check_rows(
        session.execute("SELECT id FROM t WHERE s = '中';"),
        ["id"],
        [[3]],
        "equality with a multi-byte character",
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Group: durability -- commit acknowledgement and repeated recovery
# ---------------------------------------------------------------------------


@case("durability", "commit_ack_then_crash")
def durability_commit_ack_then_crash(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("INSERT INTO t VALUES (1, 'committed');"), 1, "insert")
    # The COMMIT response is the confirmation boundary: once received, a kill
    # immediately afterwards must not lose the transaction.
    require_ok(session.execute("COMMIT;"), "commit")
    session.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id, v FROM t;"),
        ["id", "v"],
        [[1, "committed"]],
        "transaction confirmed by COMMIT response survives kill -9",
    )
    session.shutdown()


@case("durability", "autocommit_ack_then_crash")
def durability_autocommit_ack_then_crash(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(64));"),
        "create table",
    )
    check_affected(
        session.execute("INSERT INTO t VALUES (1, 'auto-committed');"),
        1,
        "autocommit insert",
    )
    # Response already received: the commit must be durable at this point.
    session.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id, v FROM t;"),
        ["id", "v"],
        [[1, "auto-committed"]],
        "autocommit statement confirmed by its response survives kill -9",
    )
    session.shutdown()


@case("durability", "double_kill_during_recovery")
def durability_double_kill_during_recovery(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(16));"),
        "create table",
    )
    # Enough committed rows that startup replay is not instantaneous.
    total = 500
    for start in range(1, total + 1, 50):
        batch = ", ".join(
            f"({pk}, 'value-{pk}')" for pk in range(start, min(start + 50, total + 1))
        )
        count = min(start + 50, total + 1) - start
        check_affected(
            session.execute(f"INSERT INTO t VALUES {batch};"), count, f"batch {start}"
        )
    session.crash()
    # Kill the recovering process twice, before/while it reports ready.
    for _ in range(2):
        recovering = env.open_unready()
        recovering.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id, v FROM t WHERE id = 1 OR id = 250 OR id = 500 ORDER BY id;"),
        ["id", "v"],
        [[1, "value-1"], [250, "value-250"], [500, "value-500"]],
        "data recoverable after repeated kills during recovery",
    )
    check_rows(
        session.execute("SELECT id FROM t WHERE id >= 498 ORDER BY id;"),
        ["id"],
        [[498], [499], [500]],
        "tail rows intact after repeated recovery kills",
    )
    session.shutdown()


@case("durability", "index_maintenance_restart_ann")
def durability_index_maintenance_restart_ann(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, category VARCHAR(64), embedding VECTOR(2));"
        ),
        "create docs",
    )
    check_affected(
        session.execute(
            "INSERT INTO docs VALUES (3, '甲', '[0,2]'), (1, '甲', '[0,0]'), (2, '甲', '[1,0]');"
        ),
        3,
        "insert rows",
    )
    require_ok(
        session.execute("CREATE VECTOR INDEX docs_vec ON docs(embedding);"),
        "create vector index",
    )
    check_affected(
        session.execute("UPDATE docs SET embedding = '[3,4]' WHERE id = 1;"), 1, "update"
    )
    check_affected(session.execute("DELETE FROM docs WHERE id = 2;"), 1, "delete")
    check_affected(
        session.execute("INSERT INTO docs VALUES (4, '甲', '[0,0]');"), 1, "insert"
    )
    session.shutdown()
    session = env.open()
    expected = [[4, 0.0], [3, 2.0], [1, 5.0]]
    sql = (
        "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs "
        "ORDER BY d LIMIT 10;"
    )
    check_rows(session.execute(sql), ["id", "d"], expected, "exact TopK after restart")
    check_rows(
        session.execute(sql, mode="ann"),
        ["id", "d"],
        expected,
        "ann TopK matches current visible records after restart",
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Group: large_data (scale / memory pressure, 128-dim vectors)
# ---------------------------------------------------------------------------


LARGE_DIM = 128


def large_vector_text(pk, dim=LARGE_DIM):
    first = pk % 977
    second = (pk * 7) % 503
    parts = [str(first), str(second)]
    parts.extend(["0"] * (dim - 2))
    return "[" + ",".join(parts) + "]"


def large_distance(pk):
    return math.hypot(pk % 977, (pk * 7) % 503)


def large_bucket(pk):
    return "even" if pk % 2 == 0 else "odd"


def large_expected(rows, bucket=None, limit=10):
    entries = []
    for pk in range(1, rows + 1):
        if bucket is not None and large_bucket(pk) != bucket:
            continue
        entries.append((large_distance(pk), pk))
    entries.sort()
    return [[pk, distance] for distance, pk in entries[:limit]]


def load_large_dataset(session, rows):
    require_ok(
        session.execute(
            "CREATE TABLE big (id BIGINT PRIMARY KEY, bucket VARCHAR(4), "
            f"embedding VECTOR({LARGE_DIM}));"
        ),
        "create large table",
    )
    batch_size = 100
    for start in range(1, rows + 1, batch_size):
        stop = min(start + batch_size, rows + 1)
        values = ", ".join(
            f"({pk}, '{large_bucket(pk)}', '{large_vector_text(pk)}')"
            for pk in range(start, stop)
        )
        check_affected(
            session.execute(f"INSERT INTO big VALUES {values};"),
            stop - start,
            f"bulk insert rows {start}..{stop - 1}",
        )
        if session.proc is None or session.proc.poll() is not None:
            fail(f"database died during bulk load around row {start}")


def check_large_topk(session, rows, mode, label):
    sql = (
        "SELECT id, L2_DISTANCE(embedding, '["
        + ",".join(["0"] * LARGE_DIM)
        + "]') AS d FROM big ORDER BY d LIMIT 10;"
    )
    expected = large_expected(rows, None, 10)
    response = session.execute(sql, mode=mode)
    if mode == "exact":
        check_rows(response, ["id", "d"], expected, label)
        return
    require_ok(response, label)
    answer = response.get("rows")
    if not isinstance(answer, list) or len(answer) != 10:
        fail(f"{label}: expected 10 rows, got {brief(answer)}")
    truth_ids = [row[0] for row in expected]
    seen = []
    previous = None
    for row in answer:
        if not isinstance(row, list) or len(row) != 2:
            fail(f"{label}: malformed row {brief(row)}")
        pk, distance = row
        if pk in seen:
            fail(f"{label}: duplicate id {pk} in ann result")
        seen.append(pk)
        if not same(distance, large_distance(pk)):
            fail(
                f"{label}: distance for id {pk} is {distance!r}, "
                f"expected {large_distance(pk)!r}"
            )
        if previous is not None and distance < previous:
            fail(f"{label}: distances not ascending: {brief(answer)}")
        previous = distance
    recall = sum(1 for pk in seen if pk in truth_ids) / float(len(truth_ids))
    if recall < 0.9:
        fail(f"{label}: recall@10 {recall:.2f} below sanity threshold 0.90; ids={seen}")


@case("large_data", "bulk_load_and_query")
def large_data_bulk_load_and_query(env):
    rows = env.large_rows
    session = env.open()
    load_large_dataset(session, rows)
    # Spot-check exact records and a range scan after the bulk load.
    check_rows(
        session.execute("SELECT id, bucket FROM big WHERE id = 1 OR id = 42;"),
        ["id", "bucket"],
        [[1, "odd"], [42, "even"]],
        "spot-check bulk-loaded rows",
    )
    tail_start = max(1, rows - 2)
    check_rows(
        session.execute(f"SELECT id FROM big WHERE id >= {tail_start} ORDER BY id;"),
        ["id"],
        [[pk] for pk in range(tail_start, rows + 1)],
        "range scan on large table",
    )
    check_large_topk(session, rows, "exact", "exact TopK over large table")
    check_large_topk(session, rows, "ann", "ann TopK over large table")
    sql_filtered = (
        "SELECT id, L2_DISTANCE(embedding, '["
        + ",".join(["0"] * LARGE_DIM)
        + "]') AS d FROM big WHERE bucket = 'odd' ORDER BY d LIMIT 10;"
    )
    check_rows(
        session.execute(sql_filtered),
        ["id", "d"],
        large_expected(rows, "odd", 10),
        "filtered exact TopK over large table",
    )
    require_ok(
        session.execute("SELECT id FROM big WHERE id = " + str(rows) + ";"),
        "large table still queryable",
    )
    if session.proc is None or session.proc.poll() is not None:
        fail("database died during large-table queries")
    session.shutdown()


@case("durability", "large_data_restart_and_query")
def durability_large_data_restart_and_query(env):
    rows = env.large_rows
    session = env.open()
    load_large_dataset(session, rows)
    session.shutdown()
    session = env.open()
    check_rows(
        session.execute("SELECT id, bucket FROM big WHERE id = 1 OR id = 42;"),
        ["id", "bucket"],
        [[1, "odd"], [42, "even"]],
        "spot-check after reopening a large data set",
    )
    check_large_topk(session, rows, "exact", "exact TopK after reopening large data set")
    check_rows(
        session.execute("SELECT id FROM big WHERE id >= " + str(rows - 2) + " ORDER BY id;"),
        ["id"],
        [[rows - 2], [rows - 1], [rows]],
        "tail rows after reopening large data set",
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def default_binary():
    here = Path(__file__).resolve()
    root = here.parents[2]  # seekdb/
    names = ["minivectordb.exe", "minivectordb"] if os.name == "nt" else ["minivectordb"]
    for profile in ("debug", "release"):
        for name in names:
            candidate = root / "minivectordb" / "target" / profile / name
            if candidate.exists():
                return str(candidate)
    return None


def configure_streams():
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
        except (AttributeError, ValueError):
            pass


def select_cases(args):
    selected = []
    for case_item in CASES:
        if args.group and case_item.group not in args.group:
            continue
        if args.filter and args.filter.lower() not in case_item.id.lower():
            continue
        selected.append(case_item)
    return selected


def write_report(path, binary, results, started):
    groups = {}
    for result in results:
        entry = groups.setdefault(
            result.case.group, {"passed": 0, "failed": 0, "total": 0}
        )
        entry["total"] += 1
        if result.error is None:
            entry["passed"] += 1
        else:
            entry["failed"] += 1
    payload = {
        "suite": "minivectordb-hidden-case-hardening",
        "binary": binary,
        "test_root": str(test_root()),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(started)),
        "duration_ms": round((time.monotonic() - started) * 1000),
        "groups": groups,
        "cases": [],
    }
    for result in results:
        item = {
            "id": result.case.id,
            "group": result.case.group,
            "status": "PASS" if result.error is None else "FAIL",
            "duration_ms": round(result.duration * 1000),
        }
        if result.error is not None:
            item["error"] = result.error
            item["exchanges"] = result.exchanges[-10:]
            item["stderr"] = [
                {"session": name, "tail": tail[-4000:]} for name, tail in result.tails
            ]
        payload["cases"].append(item)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main(argv=None):
    configure_streams()
    parser = argparse.ArgumentParser(
        description="MiniVectorDB hidden-case hardening suite (no pytest)."
    )
    parser.add_argument("--binary", help="database binary (default: debug build)")
    parser.add_argument(
        "--group",
        action="append",
        choices=KNOWN_GROUPS,
        help="run only this group; repeatable",
    )
    parser.add_argument("--filter", help="substring filter on case id")
    parser.add_argument("--list", action="store_true", help="list cases and exit")
    parser.add_argument("--keep-data", action="store_true", help="keep data dirs of failed cases")
    parser.add_argument(
        "--xfail-group",
        action="append",
        choices=KNOWN_GROUPS,
        default=[],
        help="treat failures in this group as expected (exit code stays 0)",
    )
    parser.add_argument("--report", type=Path, help="write a JSON report to this path")
    parser.add_argument("--startup-timeout", type=float, default=120.0)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument(
        "--large-rows",
        type=int,
        default=20000,
        help="row count for the large_data group (default 20000, 128-dim vectors)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    selected = select_cases(args)
    if args.list:
        for case_item in selected:
            print(case_item.id)
        return 0
    if not selected:
        parser.error("no cases selected")
    binary = args.binary or default_binary()
    if not binary:
        parser.error("--binary is required (default debug binary not found)")
    binary = os.path.abspath(binary)
    if not os.path.exists(binary):
        parser.error(f"binary does not exist: {binary}")

    started = time.monotonic()
    print("MiniVectorDB hidden-case hardening suite")
    print(f"binary: {binary}")
    print(f"data root: {test_root()}")
    print(f"cases: {len(selected)}")
    print("")

    results = []
    for case_item in selected:
        result = run_case(case_item, binary, args)
        results.append(result)
        status = "PASS" if result.error is None else "FAIL"
        print(f"{status} [{case_item.group}] {case_item.name} ({result.duration:.2f}s)")
        if result.error is not None:
            print_failure(result)
    print("")

    groups = {}
    for result in results:
        groups.setdefault(result.case.group, []).append(result)
    passed = sum(1 for result in results if result.error is None)
    total = len(results)
    rate = (100.0 * passed / total) if total else 0.0
    print("=== Summary ===")
    print(f"PASS {passed}/{total} ({rate:.1f}%)")
    for group in KNOWN_GROUPS:
        if group not in groups:
            continue
        group_results = groups[group]
        group_passed = sum(1 for result in group_results if result.error is None)
        suffix = ""
        if group == "durability" and group_passed < len(group_results):
            suffix = "  (known: durable DiskStorage is still a stub)"
        print(f"  {group:<16} {group_passed}/{len(group_results)}{suffix}")

    failures = [result for result in results if result.error is not None]
    if failures:
        print("")
        print("FAILURES:")
        for result in failures:
            first_line = (result.error or "").splitlines()[0]
            print(f"  - {result.case.id}: {first_line}")
    unexpected = [
        result
        for result in failures
        if result.case.group not in set(args.xfail_group)
    ]
    if failures and not unexpected:
        print("")
        print(
            "All failures are in expected-failure groups: "
            + ", ".join(sorted(set(args.xfail_group)))
        )
    if args.report:
        write_report(args.report, binary, results, started)
        print(f"Report: {args.report}")
    print(f"Elapsed: {time.monotonic() - started:.2f}s")
    return 0 if not unexpected else 1


if __name__ == "__main__":
    sys.exit(main())
