#!/usr/bin/env python3
""""Official-like" admission suite for MiniVectorDB (stdlib only, no pytest).

Reconstructed from the competition specification (docs/COMPETITION_SPEC.md,
sections 2.1/2.2/2.3/3.2) rather than from our own implementation, so that it
can expose behaviour the 20 public samples do not cover.  Each case documents
the expected behaviour and a confidence level:

* certain  -- directly required by the spec / public README / MySQL semantics
              for the documented subset;
* uncertain-- a reasonable interpretation (e.g. optional MySQL DDL variants or
              ambiguity in affected_rows) that may differ from the official
              reference.  Failures are reported but do not fail the gate.

Categories:
  sql_storage, high_risk, exact_search, ann_search, index_maintenance,
  transaction, recovery, resource

Every case gets an isolated data directory and its own process(es).  Failures
print expectation, request/response tail and stderr tail.  A JSON report can be
written with --report.

Usage:
    python tools/verify/official_like_cases.py --binary <release-or-debug.exe>
    python tools/verify/official_like_cases.py --binary ... --skip-resource
    python tools/verify/official_like_cases.py --binary ... --category high_risk
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import shutil
import struct
import sys
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from jsonl_client import (  # noqa: E402
    ClientTimeout,
    ProtocolError,
    Session,
    make_workdir,
    test_root,
)
from edge_cases import (  # noqa: E402
    CaseEnv,
    CaseFailure,
    brief,
    check_affected,
    check_rows,
    ids,
    require_error,
    require_ok,
    same,
)

BT = chr(96)  # back-tick; kept as a constant so this file has no quoting traps
MAX_BIGINT = 9223372036854775807
NEG_BIGINT = -9223372036854775807

CATEGORIES = (
    "sql_storage",
    "high_risk",
    "exact_search",
    "ann_search",
    "index_maintenance",
    "transaction",
    "recovery",
    "resource",
)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class Case:
    def __init__(self, category, name, expectation, func, confidence):
        self.category = category
        self.name = name
        self.expectation = expectation
        self.func = func
        self.confidence = confidence
        self.id = f"{category}.{name}"


CASES = []


def case(category, name, expectation, confidence="certain"):
    if category not in CATEGORIES:
        raise ValueError(f"unknown category {category!r}")

    def decorator(func):
        CASES.append(Case(category, name, expectation, func, confidence))
        return func

    return decorator


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def make_table(session, name="docs", columns="id BIGINT PRIMARY KEY, content VARCHAR(64)"):
    require_ok(session.execute(f"CREATE TABLE {name} ({columns});"), f"create table {name}")


def make_vector_table(session, name="vecs", dim=2):
    require_ok(
        session.execute(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, embedding VECTOR({dim}));"
        ),
        f"create vector table {name}",
    )


# -- float32 / ANN helpers --------------------------------------------------


def f32(value):
    return struct.unpack("<f", struct.pack("<f", float(value)))[0]


def vector_text(values):
    return "[" + ",".join(f"{float(v):.6f}" for v in values) + "]"


def l2_f32(a, b):
    total = 0.0
    for x, y in zip(a, b):
        delta = f32(x) - f32(y)
        total += delta * delta
    return math.sqrt(total)


def insert_rows(session, table, rows, batch_size=100, what="insert rows"):
    """rows: list of SQL value tuples as strings, e.g. ["(1,'a')", ...]."""
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        check_affected(
            session.execute(f"INSERT INTO {table} VALUES " + ",".join(batch) + ";"),
            len(batch),
            f"{what} batch at {start}",
        )


def gen_dataset(rows, dim, seed):
    rng = random.Random(seed)
    vectors = {}
    for pk in range(1, rows + 1):
        vectors[pk] = [rng.uniform(-1.0, 1.0) for _ in range(dim)]
    query = [rng.uniform(-1.0, 1.0) for _ in range(dim)]
    return vectors, query


def insert_dataset(session, table, vectors, batch_size=100):
    rows = []
    for pk, vec in vectors.items():
        bucket = "odd" if pk % 2 else "even"
        rows.append(f"({pk}, '{bucket}', '{vector_text(vec)}')")
    insert_rows(session, table, rows, batch_size, "insert ANN dataset")


def truth_topk(vectors, query, k, bucket=None):
    scored = []
    for pk, vec in vectors.items():
        if bucket is not None and ("odd" if pk % 2 else "even") != bucket:
            continue
        scored.append((l2_f32(vec, query), pk))
    scored.sort()
    return [(pk, distance) for distance, pk in scored[:k]]


def check_ann_result(response, vectors, query, k, bucket, label, truth):
    require_ok(response, label)
    answer = response.get("rows")
    if not isinstance(answer, list):
        fail_with(f"{label}: rows must be a list; response={brief(response)}")
    if len(answer) > k:
        fail_with(f"{label}: returned {len(answer)} rows > K={k}")
    seen = set()
    previous = None
    for row in answer:
        if not isinstance(row, list) or len(row) != 2:
            fail_with(f"{label}: malformed row {brief(row)}")
        pk, distance = row
        if not isinstance(pk, int) or pk not in vectors:
            fail_with(f"{label}: invalid id {pk!r} (not a currently visible record)")
        if bucket is not None and ("odd" if pk % 2 else "even") != bucket:
            fail_with(f"{label}: id {pk} violates the WHERE filter")
        if pk in seen:
            fail_with(f"{label}: duplicate id {pk} in ANN result")
        seen.add(pk)
        expected_distance = l2_f32(vectors[pk], query)
        if not same(distance, expected_distance, abs_tol=1e-4, rel_tol=1e-4):
            fail_with(
                f"{label}: distance for id {pk} is {distance!r}, "
                f"expected {expected_distance!r} for the stored vector"
            )
        if previous is not None and distance < previous:
            fail_with(f"{label}: distances not ascending: {brief(answer)}")
        previous = distance
    truth_ids = [pk for pk, _ in truth]
    overlap = len(seen.intersection(truth_ids))
    recall = overlap / float(len(truth_ids)) if truth_ids else 1.0
    if truth_ids and recall < 0.95:
        fail_with(
            f"{label}: recall@{k} {recall:.3f} < 0.95; returned={sorted(seen)}, truth={truth_ids}"
        )
    return recall


def fail_with(message):
    raise CaseFailure(message)


# ---------------------------------------------------------------------------
# Category: sql_storage
# ---------------------------------------------------------------------------


@case(
    "sql_storage",
    "ddl_inline_primary_key",
    "CREATE TABLE with an inline BIGINT PRIMARY KEY must succeed and accept rows.",
)
def sql_ddl_inline_primary_key(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a');"), 1, "insert")
    check_rows(session.execute("SELECT id, content FROM docs;"), ["id", "content"], [[1, "a"]], "select")
    session.shutdown()


@case(
    "sql_storage",
    "ddl_column_comment",
    "Column COMMENT clauses (MySQL DDL) should be accepted and ignored.",
)
def sql_ddl_column_comment(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY COMMENT 'primary key', "
            "content VARCHAR(64) COMMENT 'payload');"
        ),
        "create table with column comments",
    )
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a');"), 1, "insert")
    check_rows(session.execute("SELECT content FROM docs;"), ["content"], [["a"]], "select")
    session.shutdown()


@case(
    "sql_storage",
    "ddl_if_not_exists_new_table",
    "CREATE TABLE IF NOT EXISTS on a fresh name must create the table.",
)
def sql_ddl_if_not_exists_new(env):
    session = env.open()
    require_ok(
        session.execute("CREATE TABLE IF NOT EXISTS docs (id BIGINT PRIMARY KEY, content VARCHAR(64));"),
        "create table if not exists",
    )
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a');"), 1, "insert")
    session.shutdown()


@case(
    "sql_storage",
    "ddl_select_star_column_order",
    "SELECT * returns all table columns in definition order with correct values.",
)
def sql_ddl_select_star_column_order(env):
    session = env.open()
    make_table(session, columns="id BIGINT PRIMARY KEY, a VARCHAR(8), b BIGINT")
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'x', 10);"), 1, "insert")
    check_rows(
        session.execute("SELECT * FROM docs;"),
        ["id", "a", "b"],
        [[1, "x", 10]],
        "select star column order",
    )
    session.shutdown()


@case(
    "sql_storage",
    "insert_multirow_affected_rows",
    "A multi-row INSERT reports the number of inserted rows and keeps input order semantics.",
)
def sql_insert_multirow(env):
    session = env.open()
    make_table(session)
    check_affected(
        session.execute("INSERT INTO docs VALUES (3, 'c'), (1, 'a'), (2, 'b');"),
        3,
        "multi-row insert",
    )
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "a"], [2, "b"], [3, "c"]],
        "rows after multi-row insert",
    )
    session.shutdown()


@case(
    "sql_storage",
    "insert_column_list_reordered",
    "INSERT with an explicit column list must map values by column name, not position.",
)
def sql_insert_column_list(env):
    session = env.open()
    make_table(session)
    check_affected(
        session.execute("INSERT INTO docs (content, id) VALUES ('a', 1), ('b', 2);"),
        2,
        "insert with reordered column list",
    )
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "a"], [2, "b"]],
        "column list mapping",
    )
    session.shutdown()


@case(
    "sql_storage",
    "insert_column_list_missing_column",
    "Omitting a non-key column in INSERT fills it with NULL (MySQL default).",
    confidence="uncertain",
)
def sql_insert_missing_column(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs (id) VALUES (1);"), 1, "insert only pk")
    check_rows(
        session.execute("SELECT id, content FROM docs;"),
        ["id", "content"],
        [[1, None]],
        "missing non-key column becomes NULL",
    )
    session.shutdown()


@case(
    "sql_storage",
    "update_by_primary_key",
    "UPDATE ... WHERE pk changes the row and reports one affected row.",
)
def sql_update_by_pk(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a'), (2, 'b');"), 2, "insert")
    check_affected(session.execute("UPDATE docs SET content = 'changed' WHERE id = 1;"), 1, "update")
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "changed"], [2, "b"]],
        "updated row visible",
    )
    session.shutdown()


@case(
    "sql_storage",
    "update_by_condition",
    "UPDATE without WHERE affects every matched row and reports that count.",
)
def sql_update_by_condition(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a'), (2, 'b'), (3, 'c');"), 3, "insert")
    check_affected(session.execute("UPDATE docs SET content = 'z' WHERE id >= 2;"), 2, "conditional update")
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "a"], [2, "z"], [3, "z"]],
        "conditional update rows",
    )
    session.shutdown()


@case(
    "sql_storage",
    "delete_by_condition",
    "DELETE ... WHERE removes exactly the matched rows and reports the count.",
)
def sql_delete_by_condition(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a'), (2, 'b'), (3, 'c');"), 3, "insert")
    check_affected(session.execute("DELETE FROM docs WHERE id >= 2;"), 2, "delete rows")
    check_rows(session.execute("SELECT id FROM docs;"), ["id"], [[1]], "remaining rows")
    session.shutdown()


@case(
    "sql_storage",
    "comparisons_and_filter",
    "=, <, <=, >, >= and AND filters follow MySQL semantics on BIGINT columns.",
)
def sql_comparisons_and(env):
    session = env.open()
    make_table(session, columns="id BIGINT PRIMARY KEY, value BIGINT")
    check_affected(
        session.execute("INSERT INTO docs VALUES (1, 10), (2, 20), (3, 30), (4, 40);"),
        4,
        "insert",
    )
    cases = [
        ("value = 20", [[2]]),
        ("value <> 20", [[1], [3], [4]]),
        ("value < 20", [[1]]),
        ("value <= 20", [[1], [2]]),
        ("value > 20", [[3], [4]]),
        ("value >= 20", [[2], [3], [4]]),
        ("value >= 20 AND value < 40", [[2], [3]]),
    ]
    for where, expected in cases:
        check_rows(
            session.execute(f"SELECT id FROM docs WHERE {where} ORDER BY id;"),
            ["id"],
            expected,
            f"filter {where}",
        )
    session.shutdown()


@case(
    "sql_storage",
    "order_by_asc_desc_and_ties",
    "ORDER BY asc/desc; tied keys are ordered by ascending primary key per the spec.",
)
def sql_order_by_ties(env):
    session = env.open()
    make_table(session, columns="id BIGINT PRIMARY KEY, value BIGINT")
    check_affected(
        session.execute("INSERT INTO docs VALUES (3, 10), (1, 10), (2, 10), (5, 5);"),
        4,
        "insert",
    )
    check_rows(
        session.execute("SELECT id FROM docs ORDER BY value;"),
        ["id"],
        [[5], [1], [2], [3]],
        "ASC with ties by primary key",
    )
    check_rows(
        session.execute("SELECT id FROM docs ORDER BY value DESC;"),
        ["id"],
        [[1], [2], [3], [5]],
        "DESC with tied keys still ordered by ascending primary key",
    )
    check_rows(
        session.execute("SELECT id, value FROM docs ORDER BY value DESC, id DESC;"),
        ["id", "value"],
        [[3, 10], [2, 10], [1, 10], [5, 5]],
        "explicit secondary key",
    )
    session.shutdown()


@case(
    "sql_storage",
    "limit_zero",
    "LIMIT 0 returns columns and no rows for all table states.",
)
def sql_limit_zero(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a'), (2, 'b');"), 2, "insert")
    for sql in (
        "SELECT id FROM docs LIMIT 0;",
        "SELECT id FROM docs ORDER BY id LIMIT 0;",
        "SELECT id, content FROM docs ORDER BY id DESC LIMIT 0;",
    ):
        response = session.execute(sql)
        require_ok(response, sql)
        if response.get("columns") != ["id"] and response.get("columns") != ["id", "content"]:
            fail_with(f"{sql}: unexpected columns {brief(response.get('columns'))}")
        if response.get("rows") != []:
            fail_with(f"{sql}: LIMIT 0 must return no rows, got {brief(response.get('rows'))}")
    session.shutdown()


@case(
    "sql_storage",
    "empty_table_semantics",
    "Empty table: SELECT returns columns with no rows; DELETE/UPDATE report 0.",
)
def sql_empty_table(env):
    session = env.open()
    make_table(session)
    check_rows(session.execute("SELECT id, content FROM docs;"), ["id", "content"], [], "empty select")
    check_affected(session.execute("DELETE FROM docs;"), 0, "delete on empty table")
    check_affected(session.execute("UPDATE docs SET content = 'x';"), 0, "update on empty table")
    check_rows(session.execute("SELECT id FROM docs;"), ["id"], [], "still empty")
    session.shutdown()


@case(
    "sql_storage",
    "multiple_tables_independent",
    "Several tables coexist; mutations on one do not affect the others.",
)
def sql_multiple_tables(env):
    session = env.open()
    make_table(session, "a", "id BIGINT PRIMARY KEY, v VARCHAR(16)")
    make_table(session, "b", "id BIGINT PRIMARY KEY, value BIGINT")
    check_affected(session.execute("INSERT INTO a VALUES (1, 'a1');"), 1, "insert a")
    check_affected(session.execute("INSERT INTO b VALUES (1, 100);"), 1, "insert b")
    check_affected(session.execute("UPDATE b SET value = 200 WHERE id = 1;"), 1, "update b")
    check_rows(session.execute("SELECT v FROM a;"), ["v"], [["a1"]], "table a unchanged")
    check_rows(session.execute("SELECT value FROM b;"), ["value"], [[200]], "table b updated")
    session.shutdown()


@case(
    "sql_storage",
    "varchar_unicode_length",
    "VARCHAR(n) counts Unicode characters: 16 fit, 17 fail (for ASCII, CJK and emoji).",
)
def sql_varchar_length(env):
    session = env.open()
    make_table(session, columns="id BIGINT PRIMARY KEY, s VARCHAR(16)")
    ok_values = [("a" * 16), ("中" * 16), ("😀" * 16)]
    for index, value in enumerate(ok_values, start=1):
        check_affected(
            session.execute(f"INSERT INTO docs VALUES ({index}, '{value}');"), 1, "16 chars accepted"
        )
    for index, value in enumerate(("a" * 17, "中" * 17, "😀" * 17), start=10):
        require_error(
            session.execute(f"INSERT INTO docs VALUES ({index}, '{value}');"), "17 chars rejected"
        )
    session.shutdown()


@case(
    "sql_storage",
    "string_order_by_code_point",
    "String ORDER BY uses Unicode code point order (no locale collation).",
)
def sql_string_codepoint_order(env):
    session = env.open()
    make_table(session, columns="id BIGINT PRIMARY KEY, s VARCHAR(32)")
    for pk, value in [(1, "A"), (2, "a"), (3, "中"), (4, "😀"), (5, "Z")]:
        check_affected(session.execute(f"INSERT INTO docs VALUES ({pk}, '{value}');"), 1, "insert")
    check_rows(
        session.execute("SELECT id FROM docs ORDER BY s;"),
        ["id"],
        [[1], [5], [2], [3], [4]],
        "code point order",
    )
    session.shutdown()


@case(
    "sql_storage",
    "bigint_precision",
    "BIGINT extremes round-trip as exact JSON integers without float precision loss.",
)
def sql_bigint_precision(env):
    session = env.open()
    make_table(session, columns="id BIGINT PRIMARY KEY, value BIGINT")
    check_affected(
        session.execute(
            f"INSERT INTO docs VALUES ({MAX_BIGINT}, {MAX_BIGINT}), "
            f"({NEG_BIGINT}, {NEG_BIGINT}), (0, 0);"
        ),
        3,
        "insert extremes",
    )
    response = session.execute("SELECT id, value FROM docs ORDER BY id;")
    check_rows(
        response,
        ["id", "value"],
        [[NEG_BIGINT, NEG_BIGINT], [0, 0], [MAX_BIGINT, MAX_BIGINT]],
        "exact BIGINT round-trip",
    )
    for row in response["rows"]:
        if type(row[0]) is not int or type(row[1]) is not int:
            fail_with(f"BIGINT value not serialized as JSON integer: {brief(row)}")
    session.shutdown()


@case(
    "sql_storage",
    "bigint_signed_min",
    "BIGINT is a signed 64-bit integer: -9223372036854775808 must be accepted and "
    "round-trip exactly, while 9223372036854775808 must be rejected as out of range.",
)
def sql_bigint_signed_min(env):
    session = env.open()
    make_table(session, columns="id BIGINT PRIMARY KEY, value BIGINT")
    check_affected(
        session.execute(
            "INSERT INTO docs VALUES (-9223372036854775808, -9223372036854775808), "
            f"({MAX_BIGINT}, {MAX_BIGINT});"
        ),
        2,
        "insert signed BIGINT minimum and maximum",
    )
    check_rows(
        session.execute("SELECT id, value FROM docs ORDER BY id;"),
        ["id", "value"],
        [[-9223372036854775808, -9223372036854775808], [MAX_BIGINT, MAX_BIGINT]],
        "signed BIGINT extremes round-trip",
    )
    require_error(
        session.execute("INSERT INTO docs VALUES (9223372036854775808, 1);"),
        "one above BIGINT maximum rejected",
    )
    session.shutdown()


@case(
    "sql_storage",
    "duplicate_primary_key",
    "Duplicate primary key is rejected, the original row is kept, connection continues.",
)
def sql_duplicate_pk(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'orig');"), 1, "insert")
    require_error(session.execute("INSERT INTO docs VALUES (1, 'dup');"), "duplicate primary key")
    check_rows(
        session.execute("SELECT id, content FROM docs;"),
        ["id", "content"],
        [[1, "orig"]],
        "original row kept",
    )
    check_affected(session.execute("INSERT INTO docs VALUES (2, 'two');"), 1, "connection usable")
    session.shutdown()


# ---------------------------------------------------------------------------
# Category: high_risk
# ---------------------------------------------------------------------------


@case(
    "high_risk",
    "update_affected_rows_same_value",
    "MySQL semantics: UPDATE affected_rows counts CHANGED rows; writing the same "
    "value changes nothing, so it should be 0 (not 1).",
    confidence="uncertain",
)
def high_risk_update_same_value(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a');"), 1, "insert")
    check_affected(
        session.execute("UPDATE docs SET content = 'a' WHERE id = 1;"),
        0,
        "same-value UPDATE affected_rows (MySQL changed-rows semantics)",
    )
    check_rows(
        session.execute("SELECT id, content FROM docs;"),
        ["id", "content"],
        [[1, "a"]],
        "row unchanged",
    )
    session.shutdown()


@case(
    "high_risk",
    "update_affected_rows_partial_change",
    "MySQL semantics: 3 rows match but only 2 change; affected_rows should be 2.",
    confidence="uncertain",
)
def high_risk_update_partial_change(env):
    session = env.open()
    make_table(session)
    check_affected(
        session.execute("INSERT INTO docs VALUES (1, 'a'), (2, 'b'), (3, 'c');"), 3, "insert"
    )
    check_affected(
        session.execute("UPDATE docs SET content = 'a' WHERE id >= 1;"),
        2,
        "matched=3 changed=2 affected_rows (MySQL changed-rows semantics)",
    )
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "a"], [2, "a"], [3, "a"]],
        "all matched rows now hold the new value",
    )
    session.shutdown()


@case(
    "high_risk",
    "string_backslash_escapes",
    "MySQL default string escapes: backslash-quote and backslash-n inside a string "
    "literal should be interpreted (not treated as literal backslashes).",
    confidence="uncertain",
)
def high_risk_backslash_escapes(env):
    session = env.open()
    make_table(session)
    require_ok(
        session.execute(
            "INSERT INTO docs VALUES (1, 'It" + "\\" + "'s');"
        ),
        "backslash-quote escape in string literal",
    )
    require_ok(
        session.execute(
            "INSERT INTO docs VALUES (2, 'a" + "\\" + "nb');"
        ),
        "backslash-n escape in string literal",
    )
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "It's"], [2, "a" + chr(10) + "b"]],
        "MySQL escape interpretation",
    )
    session.shutdown()


@case(
    "high_risk",
    "ddl_backtick_identifiers",
    "MySQL-style back-tick quoted identifiers should work in CREATE/INSERT/SELECT.",
    confidence="uncertain",
)
def high_risk_backtick(env):
    session = env.open()
    quoted_table = BT + "docs" + BT
    quoted_id = BT + "id" + BT
    quoted_content = BT + "content" + BT
    require_ok(
        session.execute(
            f"CREATE TABLE {quoted_table} ({quoted_id} BIGINT PRIMARY KEY, "
            f"{quoted_content} VARCHAR(64));"
        ),
        "create table with back-tick quoted identifiers",
    )
    check_affected(
        session.execute(f"INSERT INTO {quoted_table} VALUES (1, 'a');"), 1, "insert"
    )
    check_rows(
        session.execute(
            f"SELECT {quoted_id}, {quoted_content} FROM {quoted_table} ORDER BY {quoted_id};"
        ),
        ["id", "content"],
        [[1, "a"]],
        "select with back-tick identifiers",
    )
    session.shutdown()


@case(
    "high_risk",
    "ddl_table_level_primary_key",
    "MySQL allows a table-level PRIMARY KEY (id) constraint; should be accepted.",
    confidence="uncertain",
)
def high_risk_table_level_pk(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT NOT NULL, content VARCHAR(64), PRIMARY KEY (id));"
        ),
        "create table with table-level PRIMARY KEY",
    )
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a');"), 1, "insert")
    require_error(session.execute("INSERT INTO docs VALUES (1, 'dup');"), "primary key enforced")
    session.shutdown()


@case(
    "high_risk",
    "ddl_table_options",
    "MySQL table options (ENGINE=... DEFAULT CHARSET=...) should be accepted and ignored.",
    confidence="uncertain",
)
def high_risk_table_options(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, content VARCHAR(64)) "
            "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        ),
        "create table with ENGINE/DEFAULT CHARSET options",
    )
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a');"), 1, "insert")
    session.shutdown()


@case(
    "high_risk",
    "ddl_column_collate",
    "MySQL column COLLATE clauses should be accepted for VARCHAR columns.",
    confidence="uncertain",
)
def high_risk_column_collate(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, content VARCHAR(64) COLLATE utf8mb4_bin);"
        ),
        "create table with COLLATE",
    )
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a');"), 1, "insert")
    session.shutdown()


@case(
    "high_risk",
    "ddl_if_not_exists_idempotent",
    "CREATE TABLE IF NOT EXISTS on an existing table must succeed as a no-op (MySQL), "
    "not return table_exists.",
    confidence="uncertain",
)
def high_risk_if_not_exists_idempotent(env):
    session = env.open()
    make_table(session)
    require_ok(
        session.execute(
            "CREATE TABLE IF NOT EXISTS docs (id BIGINT PRIMARY KEY, content VARCHAR(64));"
        ),
        "second CREATE TABLE IF NOT EXISTS must be a no-op",
    )
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a');"), 1, "table still usable")
    session.shutdown()


@case(
    "high_risk",
    "limit_offset_count",
    "MySQL LIMIT syntax LIMIT offset, count should select rows after the offset.",
    confidence="uncertain",
)
def high_risk_limit_offset(env):
    session = env.open()
    make_table(session, columns="id BIGINT PRIMARY KEY, content VARCHAR(64)")
    rows = ",".join(f"({pk}, 'v{pk}')" for pk in range(1, 6))
    check_affected(session.execute(f"INSERT INTO docs VALUES {rows};"), 5, "insert")
    check_rows(
        session.execute("SELECT id FROM docs ORDER BY id LIMIT 1, 2;"),
        ["id"],
        [[2], [3]],
        "LIMIT offset,count",
    )
    check_rows(
        session.execute("SELECT id FROM docs ORDER BY id LIMIT 2 OFFSET 1;"),
        ["id"],
        [[2], [3]],
        "LIMIT count OFFSET offset",
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Category: exact_search
# ---------------------------------------------------------------------------


@case(
    "exact_search",
    "euclidean_distance",
    "L2_DISTANCE returns Euclidean distance (not squared) as a finite JSON number.",
)
def exact_euclidean(env):
    session = env.open()
    make_vector_table(session)
    check_affected(session.execute("INSERT INTO vecs VALUES (1, '[3,4]');"), 1, "insert")
    check_rows(
        session.execute("SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM vecs;"),
        ["id", "d"],
        [[1, 5.0]],
        "euclidean distance",
    )
    session.shutdown()


@case(
    "exact_search",
    "filter_first_topk",
    "Filtered TopK is computed on the WHERE-filtered set, never on a global TopK.",
)
def exact_filter_first(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, category VARCHAR(16), embedding VECTOR(2));"
        ),
        "create table",
    )
    check_affected(
        session.execute(
            "INSERT INTO docs VALUES (1, 'skip', '[0,0]'), (2, 'skip', '[0,0]'), "
            "(3, 'keep', '[5,0]'), (4, 'keep', '[3,0]'), (5, 'other', '[1,0]');"
        ),
        5,
        "insert",
    )
    sql = (
        "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs "
        "WHERE category = 'keep' ORDER BY d LIMIT "
    )
    check_rows(session.execute(sql + "1;"), ["id", "d"], [[4, 3.0]], "filter-first TopK")
    check_rows(
        session.execute(sql + "10;"), ["id", "d"], [[4, 3.0], [3, 5.0]], "all filtered rows when K > N"
    )
    check_rows(
        session.execute(sql.replace("'keep'", "'absent'") + "10;"), ["id", "d"], [], "empty filtered set"
    )
    session.shutdown()


@case(
    "exact_search",
    "topk_ties_by_primary_key",
    "Equal distances are ordered by ascending primary key; TopK keeps the lowest ids.",
)
def exact_ties_by_pk(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, category VARCHAR(16), embedding VECTOR(2));"
        ),
        "create table",
    )
    check_affected(
        session.execute(
            "INSERT INTO docs VALUES (10, 'keep', '[1,0]'), (7, 'keep', '[-1,0]'), "
            "(9, 'keep', '[0,1]'), (20, 'keep', '[3,4]');"
        ),
        4,
        "insert",
    )
    sql = (
        "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs "
        "WHERE category = 'keep' ORDER BY d LIMIT "
    )
    check_rows(session.execute(sql + "3;"), ["id", "d"], [[7, 1.0], [9, 1.0], [10, 1.0]], "ties by pk")
    check_rows(session.execute(sql + "2;"), ["id", "d"], [[7, 1.0], [9, 1.0]], "TopK tie truncation")
    session.shutdown()


@case(
    "exact_search",
    "float32_storage_semantics",
    "Vectors are stored as float32; distances use the rounded stored values.",
)
def exact_float32(env):
    session = env.open()
    make_vector_table(session)
    check_affected(
        session.execute("INSERT INTO vecs VALUES (1, '[16777217,0]'), (2, '[16777216,0]');"),
        2,
        "insert",
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[16777216,0]') AS d FROM vecs ORDER BY id;"
        ),
        ["id", "d"],
        [[1, 0.0], [2, 0.0]],
        "stored f32 rounding",
    )
    session.shutdown()


@case(
    "exact_search",
    "reject_invalid_vectors",
    "NaN/Infinity/float32 overflow/wrong dimension/empty/non-numeric vectors are "
    "rejected with ok:false and a non-empty error.code; no row is written.",
)
def exact_reject_invalid_vectors(env):
    session = env.open()
    make_vector_table(session)
    invalid = ("[NaN,1]", "[Infinity,1]", "[1e39,0]", "[1]", "[]", "[abc,1]")
    for index, literal in enumerate(invalid, start=1):
        require_error(
            session.execute(f"INSERT INTO vecs VALUES ({index}, '{literal}');"),
            f"invalid vector {literal}",
        )
    check_rows(session.execute("SELECT id FROM vecs;"), ["id"], [], "no invalid vector stored")
    session.shutdown()


@case(
    "exact_search",
    "select_star_with_vector_column",
    "SELECT * includes the vector column as a JSON array of finite numbers.",
)
def exact_select_star_vector(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, content VARCHAR(16), embedding VECTOR(2));"
        ),
        "create table",
    )
    check_affected(
        session.execute("INSERT INTO docs VALUES (1, 'a', '[3,4]');"), 1, "insert"
    )
    response = session.execute("SELECT * FROM docs;")
    check_rows(
        response,
        ["id", "content", "embedding"],
        [[1, "a", [3.0, 4.0]]],
        "SELECT * vector shape",
    )
    vector = response["rows"][0][2]
    if not isinstance(vector, list) or len(vector) != 2 or not all(
        type(component) in (int, float) and math.isfinite(component) for component in vector
    ):
        fail_with(f"vector projection is not a finite JSON number array: {brief(vector)}")
    session.shutdown()


@case(
    "exact_search",
    "project_vector_column",
    "SELECT embedding returns columns=['embedding'] and rows of vector arrays.",
)
def exact_project_vector(env):
    session = env.open()
    make_vector_table(session)
    check_affected(
        session.execute("INSERT INTO vecs VALUES (1, '[3,4]'), (2, '[0,1]');"), 2, "insert"
    )
    check_rows(
        session.execute("SELECT embedding FROM vecs ORDER BY id;"),
        ["embedding"],
        [[[3.0, 4.0]], [[0.0, 1.0]]],
        "vector column projection",
    )
    session.shutdown()


@case(
    "exact_search",
    "limit_larger_than_result",
    "K larger than the number of matches returns all matches in distance order.",
)
def exact_limit_larger(env):
    session = env.open()
    make_vector_table(session)
    check_affected(
        session.execute("INSERT INTO vecs VALUES (1, '[3,4]'), (2, '[0,1]');"), 2, "insert"
    )
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM vecs ORDER BY d LIMIT 10;"
        ),
        ["id", "d"],
        [[2, 1.0], [1, 5.0]],
        "K > matches returns all",
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Category: ann_search
# ---------------------------------------------------------------------------


ANN_ROWS = 1000
ANN_DIM = 16


def ann_setup(env, rows=None, dim=None, seed=20261112):
    rows = rows or getattr(env, "ann_rows", ANN_ROWS)
    dim = dim or getattr(env, "ann_dim", ANN_DIM)
    session = env.open()
    require_ok(
        session.execute(
            f"CREATE TABLE ann (id BIGINT PRIMARY KEY, bucket VARCHAR(8), "
            f"embedding VECTOR({dim}));"
        ),
        "create ANN table",
    )
    vectors, query = gen_dataset(rows, dim, seed)
    insert_dataset(session, "ann", vectors)
    return session, vectors, query


@case(
    "ann_search",
    "recall10_unfiltered_and_filtered",
    "ANN mode returns valid, unique ids with correct distances; Recall@10 versus the "
    "exact result is >= 0.95 for filtered and unfiltered queries.",
)
def ann_recall10(env):
    rows = getattr(env, "ann_rows", ANN_ROWS)
    session, vectors, query = ann_setup(env, rows, None)
    query_text = vector_text(query)
    for bucket in (None, "odd"):
        where = f" WHERE bucket = '{bucket}'" if bucket else ""
        truth = truth_topk(vectors, query, 10, bucket)
        label = f"ann recall@10 bucket={bucket}"
        response = session.execute(
            f"SELECT id, L2_DISTANCE(embedding, '{query_text}') AS d FROM ann"
            f"{where} ORDER BY d LIMIT 10;",
            mode="ann",
        )
        check_ann_result(response, vectors, query, 10, bucket, label, truth)
    session.shutdown()


@case(
    "ann_search",
    "exact_mode_remains_exact",
    "Even after a vector index exists, exact mode must return the true TopK.",
)
def ann_exact_mode_true(env):
    session, vectors, query = ann_setup(env)
    require_ok(session.execute("CREATE VECTOR INDEX ann_idx ON ann(embedding);"), "create index")
    query_text = vector_text(query)
    truth = truth_topk(vectors, query, 10, None)
    check_rows(
        session.execute(
            f"SELECT id, L2_DISTANCE(embedding, '{query_text}') AS d FROM ann "
            "ORDER BY d LIMIT 10;",
            mode="exact",
        ),
        ["id", "d"],
        [[pk, distance] for pk, distance in truth],
        "exact TopK after index creation",
    )
    session.shutdown()


@case(
    "ann_search",
    "empty_candidate_set",
    "ANN on an empty table returns columns with an empty rows array.",
)
def ann_empty(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE ann (id BIGINT PRIMARY KEY, bucket VARCHAR(8), embedding VECTOR(4));"
        ),
        "create table",
    )
    response = session.execute(
        "SELECT id, L2_DISTANCE(embedding, '[0,0,0,0]') AS d FROM ann ORDER BY d LIMIT 10;",
        mode="ann",
    )
    check_rows(response, ["id", "d"], [], "ann on empty table")
    session.shutdown()


@case(
    "ann_search",
    "ann_filter_respected",
    "Every ANN result satisfies the WHERE filter and no deleted/absent id is returned.",
)
def ann_filter_respected(env):
    rows = getattr(env, "ann_rows", ANN_ROWS)
    session, vectors, query = ann_setup(env, rows, None)
    check_affected(session.execute("DELETE FROM ann WHERE bucket = 'even';"), rows // 2, "delete even ids")
    remaining = {pk: vec for pk, vec in vectors.items() if pk % 2 == 1}
    query_text = vector_text(query)
    truth = truth_topk(remaining, query, 10, "odd")
    response = session.execute(
        f"SELECT id, L2_DISTANCE(embedding, '{query_text}') AS d FROM ann "
        "WHERE bucket = 'odd' ORDER BY d LIMIT 10;",
        mode="ann",
    )
    check_ann_result(response, remaining, query, 10, "odd", "ann after deletes", truth)
    session.shutdown()


@case(
    "ann_search",
    "ann_mode_other_queries_unchanged",
    "The ann search mode only affects vector TopK; ordinary SELECT results are identical.",
)
def ann_mode_other_queries(env):
    session = env.open()
    make_table(session, "docs", "id BIGINT PRIMARY KEY, value BIGINT")
    check_affected(session.execute("INSERT INTO docs VALUES (2, 20), (1, 10), (3, 30);"), 3, "insert")
    exact = session.execute("SELECT id FROM docs WHERE value >= 20 ORDER BY id;", mode="exact")
    ann = session.execute("SELECT id FROM docs WHERE value >= 20 ORDER BY id;", mode="ann")
    check_rows(exact, ["id"], [[2], [3]], "exact ordinary query")
    check_rows(ann, ["id"], [[2], [3]], "ann ordinary query identical")
    session.shutdown()


# ---------------------------------------------------------------------------
# Category: index_maintenance
# ---------------------------------------------------------------------------


def index_fixture(session):
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, category VARCHAR(8), embedding VECTOR(2));"
        ),
        "create docs",
    )
    check_affected(
        session.execute(
            "INSERT INTO docs VALUES (3, '甲', '[0,2]'), (1, '甲', '[0,0]'), (2, '甲', '[1,0]');"
        ),
        3,
        "insert",
    )
    require_ok(session.execute("CREATE VECTOR INDEX docs_vec ON docs(embedding);"), "create index")


def check_index_state(session, label):
    sql_all = (
        "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs ORDER BY d LIMIT 10;"
    )
    expected_all = [[4, 0.0], [3, 2.0], [1, 5.0]]
    check_rows(session.execute(sql_all), ["id", "d"], expected_all, f"{label} exact TopK")
    check_rows(
        session.execute(sql_all, mode="ann"), ["id", "d"], expected_all, f"{label} ann TopK"
    )


@case(
    "index_maintenance",
    "interleaved_mutations_live",
    "After CREATE VECTOR INDEX, interleaved INSERT/UPDATE(vector)/DELETE are all "
    "reflected in exact and ann results.",
)
def index_interleaved(env):
    session = env.open()
    index_fixture(session)
    check_affected(session.execute("UPDATE docs SET embedding = '[3,4]' WHERE id = 1;"), 1, "update vector")
    check_affected(session.execute("DELETE FROM docs WHERE id = 2;"), 1, "delete")
    check_affected(session.execute("INSERT INTO docs VALUES (4, '甲', '[0,0]');"), 1, "insert")
    check_index_state(session, "live")
    session.shutdown()


@case(
    "index_maintenance",
    "restart_and_mode_switch",
    "After restart the vector index is rebuilt/recovered; exact and ann modes agree "
    "with the currently visible records.",
)
def index_restart_modes(env):
    session = env.open()
    index_fixture(session)
    check_affected(session.execute("UPDATE docs SET embedding = '[3,4]' WHERE id = 1;"), 1, "update")
    check_affected(session.execute("DELETE FROM docs WHERE id = 2;"), 1, "delete")
    check_affected(session.execute("INSERT INTO docs VALUES (4, '甲', '[0,0]');"), 1, "insert")
    session.shutdown()
    session = env.open()
    check_index_state(session, "after restart")
    check_rows(
        session.execute(
            "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs "
            "WHERE category = '甲' ORDER BY d LIMIT 10;",
            mode="ann",
        ),
        ["id", "d"],
        [[4, 0.0], [3, 2.0], [1, 5.0]],
        "ann filtered after restart",
    )
    session.shutdown()


@case(
    "index_maintenance",
    "deleted_rows_disappear",
    "Rows deleted after index creation must not appear in exact or ann TopK.",
)
def index_deleted_absent(env):
    session = env.open()
    index_fixture(session)
    check_affected(session.execute("DELETE FROM docs WHERE id = 1;"), 1, "delete")
    for mode in ("exact", "ann"):
        check_rows(
            session.execute(
                "SELECT id FROM docs ORDER BY L2_DISTANCE(embedding, '[0,0]') LIMIT 10;",
                mode=mode,
            ),
            ["id"],
            [[2], [3]],
            f"deleted id absent in {mode} mode",
        )
    session.shutdown()


@case(
    "index_maintenance",
    "vector_update_changes_order",
    "Updating a vector after index creation changes TopK ordering in both modes.",
)
def index_update_changes_order(env):
    session = env.open()
    index_fixture(session)
    check_affected(session.execute("UPDATE docs SET embedding = '[0,0]' WHERE id = 2;"), 1, "update")
    expected = [[1, 0.0], [2, 0.0], [3, 2.0]]
    for mode in ("exact", "ann"):
        check_rows(
            session.execute(
                "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs ORDER BY d LIMIT 10;",
                mode=mode,
            ),
            ["id", "d"],
            expected,
            f"updated vector order in {mode} mode",
        )
    session.shutdown()


# ---------------------------------------------------------------------------
# Category: transaction
# ---------------------------------------------------------------------------


@case(
    "transaction",
    "commit_visibility",
    "COMMIT makes all transaction changes visible to subsequent statements.",
)
def txn_commit(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'orig');"), 1, "seed")
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("UPDATE docs SET content = 'new' WHERE id = 1;"), 1, "update")
    require_ok(session.execute("COMMIT;"), "commit")
    check_rows(
        session.execute("SELECT content FROM docs WHERE id = 1;"),
        ["content"],
        [["new"]],
        "committed value visible",
    )
    session.shutdown()


@case(
    "transaction",
    "rollback_visibility",
    "ROLLBACK restores both plain query results and vector distances.",
)
def txn_rollback(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, content VARCHAR(16), embedding VECTOR(2));"
        ),
        "create docs",
    )
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'orig', '[0,0]');"), 1, "seed")
    sql = "SELECT id, content, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs;"
    check_rows(session.execute(sql), ["id", "content", "d"], [[1, "orig", 0.0]], "before txn")
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("UPDATE docs SET content = 'tmp', embedding = '[3,4]' WHERE id = 1;"), 1, "update")
    check_rows(session.execute(sql), ["id", "content", "d"], [[1, "tmp", 5.0]], "inside txn reads own writes")
    require_ok(session.execute("ROLLBACK;"), "rollback")
    check_rows(session.execute(sql), ["id", "content", "d"], [[1, "orig", 0.0]], "rollback restores query and distance")
    session.shutdown()


@case(
    "transaction",
    "statement_atomicity_autocommit",
    "A failing multi-row autocommit INSERT must leave no partial rows.",
)
def txn_autocommit_atomic(env):
    session = env.open()
    make_vector_table(session)
    check_affected(session.execute("INSERT INTO vecs VALUES (1, '[0,0]');"), 1, "seed")
    require_error(
        session.execute("INSERT INTO vecs VALUES (2, '[1,1]'), (3, '[1]');"),
        "failing multi-row insert",
    )
    check_rows(session.execute("SELECT id FROM vecs ORDER BY id;"), ["id"], [[1]], "no partial rows")
    session.shutdown()


@case(
    "transaction",
    "statement_failure_keeps_prior_statements",
    "Inside an explicit transaction a failing statement rolls back only itself; "
    "earlier successful statements must still be committable.",
)
def txn_statement_atomic(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, '初始');"), 1, "seed")
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("UPDATE docs SET content = '保留' WHERE id = 1;"), 1, "update")
    require_error(
        session.execute("INSERT INTO docs VALUES (2, '新'), (1, '重复');"),
        "duplicate key inside multi-row insert",
    )
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "保留"]],
        "failed statement rolled back only itself",
    )
    check_affected(session.execute("INSERT INTO docs VALUES (2, '新');"), 1, "insert after failure")
    require_ok(session.execute("COMMIT;"), "commit")
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "保留"], [2, "新"]],
        "prior statement and later insert committed",
    )
    session.shutdown()


@case(
    "transaction",
    "read_own_writes",
    "Inside a transaction, plain queries and vector search see the transaction's writes.",
)
def txn_read_own_writes(env):
    session = env.open()
    require_ok(
        session.execute(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, embedding VECTOR(2));"
        ),
        "create docs",
    )
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("INSERT INTO docs VALUES (1, '[3,4]');"), 1, "insert in txn")
    check_rows(
        session.execute("SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs;"),
        ["id", "d"],
        [[1, 5.0]],
        "transaction sees its own vector row",
    )
    require_ok(session.execute("COMMIT;"), "commit")
    session.shutdown()


@case(
    "transaction",
    "nested_transaction_rejected",
    "Nested transactions are explicitly out of scope; a second BEGIN must return an "
    "error and the outer transaction remains usable.",
)
def txn_nested_begin(env):
    session = env.open()
    make_table(session)
    require_ok(session.execute("BEGIN;"), "begin")
    require_error(session.execute("BEGIN;"), "nested BEGIN rejected")
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a');"), 1, "outer txn still usable")
    require_ok(session.execute("COMMIT;"), "commit outer txn")
    check_rows(session.execute("SELECT id FROM docs;"), ["id"], [[1]], "committed after nested error")
    session.shutdown()


@case(
    "transaction",
    "shutdown_rolls_back_uncommitted",
    "Normal shutdown rolls back an open transaction but keeps committed rows.",
)
def txn_shutdown_rollback(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'committed');"), 1, "committed insert")
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("INSERT INTO docs VALUES (2, 'uncommitted');"), 1, "uncommitted insert")
    session.shutdown()
    session = env.open()
    check_rows(
        session.execute("SELECT id, content FROM docs;"),
        ["id", "content"],
        [[1, "committed"]],
        "shutdown rolled back the open transaction",
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Category: recovery
# ---------------------------------------------------------------------------


@case(
    "recovery",
    "crash_committed_kept",
    "Committed autocommit rows survive a forced process kill and recovery.",
)
def recovery_committed(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'kept');"), 1, "insert")
    session.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id, content FROM docs;"),
        ["id", "content"],
        [[1, "kept"]],
        "committed row after kill",
    )
    session.shutdown()


@case(
    "recovery",
    "crash_uncommitted_rolled_back",
    "An uncommitted transaction is not partially applied after a forced kill.",
)
def recovery_uncommitted(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'committed');"), 1, "committed")
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("INSERT INTO docs VALUES (2, 'uncommitted');"), 1, "uncommitted")
    session.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id, content FROM docs;"),
        ["id", "content"],
        [[1, "committed"]],
        "uncommitted txn discarded after kill",
    )
    session.shutdown()


@case(
    "recovery",
    "kill_twice_during_recovery",
    "Killing the process again while it is recovering must not lose committed data.",
)
def recovery_kill_twice(env):
    session = env.open()
    make_table(session)
    rows = ",".join(f"({pk}, 'v{pk}')" for pk in range(1, 301))
    check_affected(session.execute(f"INSERT INTO docs VALUES {rows};"), 300, "bulk insert")
    session.crash()
    for _ in range(2):
        recovering = env.open_unready()
        recovering.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id, content FROM docs WHERE id = 1 OR id = 150 OR id = 300 ORDER BY id;"),
        ["id", "content"],
        [[1, "v1"], [150, "v150"], [300, "v300"]],
        "data intact after repeated recovery kills",
    )
    session.shutdown()


@case(
    "recovery",
    "multiple_restarts",
    "Several normal restarts reuse the data directory and accumulate committed data.",
)
def recovery_multiple_restarts(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'a');"), 1, "insert 1")
    session.shutdown()
    session = env.open()
    check_affected(session.execute("INSERT INTO docs VALUES (2, 'b');"), 1, "insert 2")
    session.shutdown()
    session = env.open()
    check_affected(session.execute("INSERT INTO docs VALUES (3, 'c');"), 1, "insert 3")
    session.shutdown()
    session = env.open()
    check_rows(
        session.execute("SELECT id, content FROM docs ORDER BY id;"),
        ["id", "content"],
        [[1, "a"], [2, "b"], [3, "c"]],
        "all commits survive restarts",
    )
    session.shutdown()


@case(
    "recovery",
    "commit_ack_then_kill",
    "Once the COMMIT success response is received, a kill must not lose the transaction.",
)
def recovery_commit_ack(env):
    session = env.open()
    make_table(session)
    require_ok(session.execute("BEGIN;"), "begin")
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'committed');"), 1, "insert")
    require_ok(session.execute("COMMIT;"), "commit acknowledged")
    session.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id FROM docs;"),
        ["id"],
        [[1]],
        "transaction durable after COMMIT response",
    )
    session.shutdown()


@case(
    "recovery",
    "autocommit_ack_then_kill",
    "Once an autocommit INSERT response is received, a kill must not lose the row.",
)
def recovery_autocommit_ack(env):
    session = env.open()
    make_table(session)
    check_affected(session.execute("INSERT INTO docs VALUES (1, 'auto');"), 1, "autocommit insert")
    session.crash()
    session = env.open()
    check_rows(
        session.execute("SELECT id FROM docs;"),
        ["id"],
        [[1]],
        "autocommit row durable after response",
    )
    session.shutdown()


# ---------------------------------------------------------------------------
# Category: resource
# ---------------------------------------------------------------------------


def large_expected(rows, limit, bucket=None):
    entries = []
    for pk in range(1, rows + 1):
        if bucket is not None and ("odd" if pk % 2 else "even") != bucket:
            continue
        entries.append((math.hypot(pk % 977, (pk * 7) % 503), pk))
    entries.sort()
    return [[pk, distance] for distance, pk in entries[:limit]]


def load_large_dataset(session, rows, dim, batch_size, env):
    require_ok(
        session.execute(
            f"CREATE TABLE big (id BIGINT PRIMARY KEY, bucket VARCHAR(8), "
            f"embedding VECTOR({dim}));"
        ),
        "create large table",
    )
    started = time.monotonic()
    inserted = 0
    for start in range(1, rows + 1, batch_size):
        stop = min(start + batch_size, rows + 1)
        values = []
        for pk in range(start, stop):
            bucket = "odd" if pk % 2 else "even"
            # Only first two components vary; distance to the zero vector is
            # then decidable in a small integer range (ties broken by pk).
            first = pk % 977
            second = (pk * 7) % 503
            parts = [str(first), str(second)] + ["0"] * (dim - 2)
            values.append(f"({pk}, '{bucket}', '[" + ",".join(parts) + "]')")
        check_affected(
            session.execute("INSERT INTO big VALUES " + ",".join(values) + ";"),
            stop - start,
            f"bulk insert batch {start}..{stop - 1}",
        )
        inserted += stop - start
        if session.proc is None or session.proc.poll() is not None:
            fail_with(f"database process died during bulk load at row {start}")
    env.metrics["bulk_insert_s"] = round(time.monotonic() - started, 3)
    env.metrics["rows"] = inserted
    env.metrics["dim"] = dim
    return "[" + ",".join(["0"] * dim) + "]"


@case(
    "resource",
    "large_dataset_write_query_restart",
    "Large data set (row count/dimension configurable, default 200k x 128): bulk write, "
    "exact/ann TopK and restart must complete without timeout, crash or wrong results.",
)
def resource_large_dataset(env):
    rows = env.large_rows
    dim = env.large_dim
    session = env.open()
    zero_vector = load_large_dataset(session, rows, dim, env.large_batch, env)

    def expected(limit, bucket=None):
        return large_expected(rows, limit, bucket)

    for mode in ("exact", "ann"):
        started = time.monotonic()
        response = session.execute(
            f"SELECT id, L2_DISTANCE(embedding, '{zero_vector}') AS d FROM big "
            "ORDER BY d LIMIT 10;",
            mode=mode,
        )
        env.metrics[f"topk_{mode}_s"] = round(time.monotonic() - started, 3)
        if mode == "exact":
            check_rows(response, ["id", "d"], expected(10), "large exact TopK")
        else:
            check_ann_large(response, rows, 10, None, "large ann TopK")
        started = time.monotonic()
        response = session.execute(
            f"SELECT id, L2_DISTANCE(embedding, '{zero_vector}') AS d FROM big "
            "WHERE bucket = 'odd' ORDER BY d LIMIT 10;",
            mode=mode,
        )
        env.metrics[f"filtered_topk_{mode}_s"] = round(time.monotonic() - started, 3)
        if mode == "exact":
            check_rows(response, ["id", "d"], expected(10, "odd"), "large filtered exact TopK")
        else:
            check_ann_large(response, rows, 10, "odd", "large filtered ann TopK")

    session.shutdown()
    session = env.open()
    last_bucket = "even" if rows % 2 == 0 else "odd"
    check_rows(
        session.execute(f"SELECT id, bucket FROM big WHERE id = 1 OR id = {rows};"),
        ["id", "bucket"],
        [[1, "odd"], [rows, last_bucket]],
        "spot check after reopening large table",
    )
    response = session.execute(
        f"SELECT id, L2_DISTANCE(embedding, '{zero_vector}') AS d FROM big "
        "ORDER BY d LIMIT 10;"
    )
    check_rows(response, ["id", "d"], expected(10), "large exact TopK after restart")
    # Diagnostic only (no official latency threshold is published): after a
    # reopen, point lookups currently scan the whole table.
    started_lookup = time.monotonic()
    for index in range(5):
        pk = max(1, (rows * (index + 1)) // 6)
        require_ok(session.execute(f"SELECT id FROM big WHERE id = {pk};"), "point lookup")
    env.metrics["point_lookup_ms"] = round((time.monotonic() - started_lookup) * 1000 / 5, 1)
    session.shutdown()


@case(
    "resource",
    "large_dataset_crash_recovery",
    "Large data set: after forced kills (including twice during recovery) committed "
    "rows and exact TopK must be recovered without startup timeout.",
)
def resource_large_crash_recovery(env):
    rows = env.large_rows
    dim = env.large_dim
    session = env.open()
    zero_vector = load_large_dataset(session, rows, dim, env.large_batch, env)
    session.crash()
    for _ in range(2):
        recovering = env.open_unready()
        recovering.crash()
    started = time.monotonic()
    session = env.open()
    env.metrics["recovery_open_s"] = round(time.monotonic() - started, 3)
    last_bucket = "even" if rows % 2 == 0 else "odd"
    check_rows(
        session.execute(f"SELECT id, bucket FROM big WHERE id = 1 OR id = {rows};"),
        ["id", "bucket"],
        [[1, "odd"], [rows, last_bucket]],
        "spot check after crash recovery of a large table",
    )
    check_rows(
        session.execute(
            f"SELECT id, L2_DISTANCE(embedding, '{zero_vector}') AS d FROM big "
            "ORDER BY d LIMIT 10;"
        ),
        ["id", "d"],
        large_expected(rows, 10),
        "large exact TopK after crash recovery",
    )
    session.shutdown()


def check_ann_large(response, rows, k, bucket, label):
    require_ok(response, label)
    answer = response.get("rows")
    if not isinstance(answer, list) or len(answer) != k:
        fail_with(f"{label}: expected {k} rows, got {brief(answer)}")
    seen = set()
    previous = None
    for row in answer:
        pk, distance = row
        if pk in seen:
            fail_with(f"{label}: duplicate id {pk}")
        seen.add(pk)
        if not (1 <= pk <= rows):
            fail_with(f"{label}: id {pk} outside 1..{rows}")
        if bucket is not None and ("odd" if pk % 2 else "even") != bucket:
            fail_with(f"{label}: id {pk} violates bucket filter")
        expected_distance = math.hypot(pk % 977, (pk * 7) % 503)
        if not same(distance, expected_distance, abs_tol=1e-4, rel_tol=1e-4):
            fail_with(f"{label}: distance {distance!r} != {expected_distance!r} for id {pk}")
        if previous is not None and distance < previous:
            fail_with(f"{label}: distances not ascending")
        previous = distance


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class CaseResult:
    def __init__(self, case_item, error, duration, exchanges, tails, metrics, kept_dir=None):
        self.case = case_item
        self.error = error
        self.duration = duration
        self.exchanges = exchanges
        self.tails = tails
        self.metrics = metrics
        self.kept_dir = kept_dir

    @property
    def status(self):
        if self.error is None:
            return "PASS"
        return "FAIL_UNCERTAIN" if self.case.confidence == "uncertain" else "FAIL"


def run_case(case_item, binary, args):
    work_dir = make_workdir("mvdb-official-")
    env = CaseEnv(binary, work_dir, args.startup_timeout, args.request_timeout, 1000)
    env.large_rows = args.resource_rows
    env.large_dim = args.resource_dim
    env.large_batch = args.resource_batch
    env.ann_rows = args.ann_rows
    env.ann_dim = args.ann_dim
    env.metrics = {}
    started = time.monotonic()
    error = None
    try:
        case_item.func(env)
    except CaseFailure as exc:
        error = str(exc) or "case failed"
    except (ProtocolError, ClientTimeout) as exc:
        error = f"{type(exc).__name__}: {exc}"
    except Exception:  # noqa: BLE001
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
    return CaseResult(case_item, error, duration, exchanges, tails, dict(env.metrics), kept_dir)


def print_failure(result):
    print(f"  expected: {result.case.expectation}")
    print(f"  confidence: {result.case.confidence}")
    print(f"  reason: {result.error}")
    recent = result.exchanges[-8:]
    if recent:
        print("  last exchanges:")
        for exchange in recent:
            print(f"    [{exchange['session']}] -> {brief(exchange['request'], 300)}")
            print(f"    [{exchange['session']}] <- {brief(exchange['response'], 300)}")
    for name, tail in result.tails:
        print(f"  stderr[{name}] tail:")
        for line in tail.splitlines()[-12:]:
            print(f"    {line}")
    if result.kept_dir:
        print(f"  kept data directory: {result.kept_dir}")


def default_binary():
    here = Path(__file__).resolve()
    root = here.parents[2]
    names = ["minivectordb.exe", "minivectordb"] if os.name == "nt" else ["minivectordb"]
    for profile in ("release", "debug"):
        for name in names:
            candidate = root / "minivectordb" / "target" / profile / name
            if candidate.exists():
                return str(candidate)
    return None


def configure_streams():
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="backslashreplace", line_buffering=True)
        except (AttributeError, ValueError):
            pass


def select_cases(args):
    selected = []
    for case_item in CASES:
        if args.category and case_item.category not in args.category:
            continue
        if args.filter and args.filter.lower() not in case_item.id.lower():
            continue
        if args.skip_resource and case_item.category == "resource":
            continue
        selected.append(case_item)
    return selected


def write_report(path, binary, results, started):
    categories = {}
    for result in results:
        entry = categories.setdefault(
            result.case.category, {"passed": 0, "failed": 0, "uncertain_failed": 0, "total": 0}
        )
        entry["total"] += 1
        if result.error is None:
            entry["passed"] += 1
        elif result.case.confidence == "uncertain":
            entry["uncertain_failed"] += 1
        else:
            entry["failed"] += 1
    payload = {
        "suite": "minivectordb-official-like",
        "binary": binary,
        "test_root": str(test_root()),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(started)),
        "duration_ms": round((time.monotonic() - started) * 1000),
        "categories": categories,
        "cases": [],
    }
    for result in results:
        item = {
            "id": result.case.id,
            "category": result.case.category,
            "status": result.status,
            "confidence": result.case.confidence,
            "expectation": result.case.expectation,
            "duration_ms": round(result.duration * 1000),
            "metrics": result.metrics,
        }
        if result.error is not None:
            item["error"] = result.error
            item["exchanges"] = result.exchanges[-10:]
            item["stderr"] = [{"session": name, "tail": tail[-4000:]} for name, tail in result.tails]
        payload["cases"].append(item)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main(argv=None):
    configure_streams()
    parser = argparse.ArgumentParser(
        description="Official-like admission suite reconstructed from the competition spec."
    )
    parser.add_argument("--binary", help="database binary (default: release, then debug)")
    parser.add_argument(
        "--category", action="append", choices=CATEGORIES, help="run only this category; repeatable"
    )
    parser.add_argument("--filter", help="substring filter on case id")
    parser.add_argument("--list", action="store_true", help="list selected cases and exit")
    parser.add_argument("--skip-resource", action="store_true", help="skip the resource category")
    parser.add_argument("--resource-rows", type=int, default=200000)
    parser.add_argument("--resource-dim", type=int, default=128)
    parser.add_argument("--resource-batch", type=int, default=100)
    parser.add_argument("--ann-rows", type=int, default=1000)
    parser.add_argument("--ann-dim", type=int, default=16)
    parser.add_argument("--startup-timeout", type=float, default=120.0)
    parser.add_argument("--request-timeout", type=float, default=60.0)
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument("--report", type=Path, help="write a JSON report to this path")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    selected = select_cases(args)
    if args.list:
        for case_item in selected:
            print(f"{case_item.id}  [{case_item.confidence}]")
        return 0
    if not selected:
        parser.error("no cases selected")
    binary = args.binary or default_binary()
    if not binary:
        parser.error("--binary is required (no release/debug binary found)")
    binary = os.path.abspath(binary)
    if not os.path.exists(binary):
        parser.error(f"binary does not exist: {binary}")

    started = time.monotonic()
    print("MiniVectorDB official-like admission suite")
    print(f"binary: {binary}")
    print(f"data root: {test_root()}")
    print(f"cases: {len(selected)}")
    print("")

    results = []
    for case_item in selected:
        result = run_case(case_item, binary, args)
        results.append(result)
        suffix = ""
        if result.metrics:
            suffix = " " + json.dumps(result.metrics, ensure_ascii=False, sort_keys=True)
        print(f"{result.status} [{case_item.category}] {case_item.name} ({result.duration:.2f}s){suffix}")
        if result.error is not None:
            print_failure(result)
    print("")

    categories = {}
    for result in results:
        categories.setdefault(result.case.category, []).append(result)
    passed = sum(1 for result in results if result.error is None)
    total = len(results)
    print("=== Summary ===")
    print(f"PASS {passed}/{total} ({100.0 * passed / total:.1f}%)")
    for category in CATEGORIES:
        if category not in categories:
            continue
        group = categories[category]
        group_passed = sum(1 for result in group if result.error is None)
        uncertain = sum(
            1 for result in group if result.status == "FAIL_UNCERTAIN"
        )
        suffix = f" ({uncertain} uncertain-fail)" if uncertain else ""
        print(f"  {category:<18} {group_passed}/{len(group)}{suffix}")

    certain_failures = [result for result in results if result.status == "FAIL"]
    uncertain_failures = [result for result in results if result.status == "FAIL_UNCERTAIN"]
    if certain_failures:
        print("")
        print("CERTAIN FAILURES (must fix):")
        for result in certain_failures:
            print(f"  - {result.case.id}: {(result.error or '').splitlines()[0]}")
    if uncertain_failures:
        print("")
        print("UNCERTAIN FAILURES (verify against official reference):")
        for result in uncertain_failures:
            print(f"  - {result.case.id}: {(result.error or '').splitlines()[0]}")
    if args.report:
        write_report(args.report, binary, results, started)
        print(f"Report: {args.report}")
    print(f"Elapsed: {time.monotonic() - started:.2f}s")
    return 0 if not certain_failures else 1


if __name__ == "__main__":
    sys.exit(main())
