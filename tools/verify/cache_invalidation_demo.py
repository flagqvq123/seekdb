#!/usr/bin/env python3
"""Cache invalidation demo: query, mutate without restart, query again.

Designed from the student's Task C description (2026-11-12). Prints every
request/response pair so the observed behaviour can be quoted as evidence.
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main():
    binary = Path(sys.argv[1]).resolve()
    data_root = Path(sys.argv[2]) if len(sys.argv) > 2 else ROOT / ".tmp"
    data_root.mkdir(parents=True, exist_ok=True)
    data_dir = Path(tempfile.mkdtemp(prefix="cache-demo-", dir=str(data_root)))
    proc = subprocess.Popen(
        [str(binary), "--data-dir", str(data_dir)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, encoding="utf-8",
    )
    ready = proc.stdout.readline().strip()
    print("startup:", ready)

    ident = 0
    def run(sql):
        nonlocal ident
        ident += 1
        proc.stdin.write(json.dumps({"id": ident, "op": "execute", "sql": sql,
                                     "search_mode": "exact"}, ensure_ascii=False) + "\n")
        proc.stdin.flush()
        line = proc.stdout.readline().strip()
        print(f"[{ident}] {sql}\n     -> {line}")
        return json.loads(line)

    run("CREATE TABLE docs (id BIGINT PRIMARY KEY, category VARCHAR(8), embedding VECTOR(2));")
    run("INSERT INTO docs VALUES (1, 'a', '[0,0]'), (2, 'b', '[1,0]'), (3, 'a', '[2,0]');")
    print("\n--- build cache with a filtered query ---")
    run("SELECT id FROM docs WHERE category = 'a' ORDER BY id;")
    print("\n--- mutate a row, then query again WITHOUT restarting ---")
    run("UPDATE docs SET category = 'b' WHERE id = 1;")
    run("SELECT id FROM docs WHERE category = 'a' ORDER BY id;")
    print("\n--- mutate the vector column, then run a TopK query ---")
    run("SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs ORDER BY d LIMIT 3;")
    run("UPDATE docs SET embedding = '[10,0]' WHERE id = 2;")
    run("SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs ORDER BY d LIMIT 3;")
    print("\n--- same mutation inside an explicit transaction (cache must not go stale) ---")
    run("UPDATE docs SET category = 'a' WHERE id = 2;")
    run("SELECT id FROM docs WHERE category = 'a' ORDER BY id;")
    run("BEGIN;")
    run("UPDATE docs SET category = 'zz' WHERE id = 2;")
    run("COMMIT;")
    run("SELECT id FROM docs WHERE category = 'a' ORDER BY id;")

    print("\n--- delete a row, then query again ---")
    run("DELETE FROM docs WHERE id = 3;")
    run("SELECT id, L2_DISTANCE(embedding, '[0,0]') AS d FROM docs ORDER BY d LIMIT 3;")
    ident += 1
    proc.stdin.write(json.dumps({"id": ident, "op": "shutdown"}) + "\n")
    proc.stdin.flush()
    print("\nshutdown:", proc.stdout.readline().strip())
    proc.wait(timeout=30)
    err = proc.stderr.read()
    if err.strip():
        print("stderr tail:", err.strip().splitlines()[-3:])
    return 0


if __name__ == "__main__":
    sys.exit(main())