#!/usr/bin/env python3
"""Demonstrate the 'table slightly over cache budget' path.

Student finding (2026-11-12): when the table does not fit the cache budget, the
engine must not re-attempt a (doomed) cache build on every query. With
MINIVECTORDB_CACHE_MIB set small we can exercise that path deliberately.
"""
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main():
    binary = Path(sys.argv[1]).resolve()
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    budget_mib = sys.argv[3] if len(sys.argv) > 3 else "2"
    data_root = ROOT / ".tmp"
    data_root.mkdir(parents=True, exist_ok=True)
    data_dir = Path(tempfile.mkdtemp(prefix="cache-budget-", dir=str(data_root)))
    env = dict(os.environ, MINIVECTORDB_CACHE_MIB=budget_mib)
    proc = subprocess.Popen([str(binary), "--data-dir", str(data_dir)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, encoding="utf-8", env=env)
    print("startup:", proc.stdout.readline().strip())
    ident = 0

    def run(sql):
        nonlocal ident
        ident += 1
        proc.stdin.write(json.dumps({"id": ident, "op": "execute", "sql": sql,
                                     "search_mode": "exact"}, ensure_ascii=False) + "\n")
        proc.stdin.flush()
        return json.loads(proc.stdout.readline())

    run("CREATE TABLE big (id BIGINT PRIMARY KEY, category VARCHAR(8), embedding VECTOR(128));")
    dim = 128
    batch = 100
    for start in range(0, rows, batch):
        values = []
        for i in range(start, min(start + batch, rows)):
            vec = "[" + ",".join(str((i * 7 + j) % 97 * 0.01) for j in range(dim)) + "]"
            values.append(f"({i}, 'c{i % 4}', '{vec}')")
        run("INSERT INTO big VALUES " + ",".join(values) + ";")
    query = ("SELECT id, L2_DISTANCE(embedding, '[0.1,0.2,0.3,0.4]') AS d "
             "FROM big WHERE category = 'c1' ORDER BY d LIMIT 10;")
    # warmup
    run(query.replace("[0.1,0.2,0.3,0.4]", "[" + ",".join(["0.0"] * dim) + "]"))
    start = time.perf_counter()
    n = 30
    for k in range(n):
        q = query.replace("[0.1,0.2,0.3,0.4]", "[" + ",".join(str(0.001 * (k + 1)) for _ in range(dim)) + "]")
        resp = run(q)
        assert resp.get("ok") is True, resp
    elapsed = (time.perf_counter() - start) * 1000
    print(f"rows={rows} dim={dim} cache_budget={budget_mib}MiB")
    print(f"{n} filtered TopK queries: {elapsed:.1f} ms total, {elapsed / n:.2f} ms/query")
    ident += 1
    proc.stdin.write(json.dumps({"id": ident, "op": "shutdown"}) + "\n")
    proc.stdin.flush()
    proc.stdout.readline()
    proc.wait(timeout=30)
    return 0


if __name__ == "__main__":
    sys.exit(main())
