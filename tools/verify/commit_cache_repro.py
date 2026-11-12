#!/usr/bin/env python3
"""Reproduce v1's stale vector cache after an explicit COMMIT.

Sequence: vector query builds the hot cache, then BEGIN/UPDATE/COMMIT changes a
vector, then the same query runs again without restarting the process.
A correct engine returns the new distances; v1 (commit de03d1508) returned the
old ones because COMMIT cleared the dirty markers but kept the cache.
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main():
    binary = Path(sys.argv[1]).resolve()
    label = sys.argv[2] if len(sys.argv) > 2 else binary.name
    data_root = ROOT / ".tmp"
    data_root.mkdir(parents=True, exist_ok=True)
    data_dir = Path(tempfile.mkdtemp(prefix="v1repro-", dir=str(data_root)))
    proc = subprocess.Popen([str(binary), "--data-dir", str(data_dir)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, encoding="utf-8")
    proc.stdout.readline()
    ident = 0

    def run(sql):
        nonlocal ident
        ident += 1
        proc.stdin.write(json.dumps({"id": ident, "op": "execute", "sql": sql,
                                     "search_mode": "exact"}, ensure_ascii=False) + "\n")
        proc.stdin.flush()
        return json.loads(proc.stdout.readline())

    run("CREATE TABLE docs (id BIGINT PRIMARY KEY, embedding VECTOR(2));")
    run("INSERT INTO docs VALUES (1, '[0,0]'), (2, '[1,0]'), (3, '[2,0]');")
    q = "SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs ORDER BY d LIMIT 3;"
    first = run(q)
    run("BEGIN;")
    run("UPDATE docs SET embedding = '[10,0]' WHERE id = 1;")
    run("COMMIT;")
    second = run(q)
    print("binary:", label)
    print("  before txn:", json.dumps(first.get("rows"), ensure_ascii=False))
    print("  after commit (expected id=1 distance 10):", json.dumps(second.get("rows"), ensure_ascii=False))
    expected = [[2, 1], [3, 2], [1, 10]]
    def normalise(row):
        return [row[0], int(row[1]) if float(row[1]).is_integer() else row[1]]
    actual = [normalise(r) for r in (second.get("rows") or [])]
    print("  expected after commit:", expected)
    print("  VERDICT:", "OK" if actual == expected else "WRONG (stale ordering from cache)")
    ident += 1
    proc.stdin.write(json.dumps({"id": ident, "op": "shutdown"}) + "\n")
    proc.stdin.flush()
    proc.stdout.readline()
    proc.wait(timeout=60)
    return 0


if __name__ == "__main__":
    sys.exit(main())