#!/usr/bin/env python3
"""Large-data timing probe: load, query, shutdown and reopen.

Used to bound per-request latency for the judge's 60s request timeout and the
120s startup timeout on data sets around/over the cache budget.
"""
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def session(binary, data_dir):
    proc = subprocess.Popen([str(binary), "--data-dir", str(data_dir)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, encoding="utf-8")
    ready = proc.stdout.readline().strip()
    assert ready == '{"type":"ready"}', ready
    ident = [0]

    def run(sql, timeout_note=""):
        ident[0] += 1
        start = time.perf_counter()
        proc.stdin.write(json.dumps({"id": ident[0], "op": "execute", "sql": sql,
                                     "search_mode": "exact"}, ensure_ascii=False) + "\n")
        proc.stdin.flush()
        line = proc.stdout.readline()
        elapsed = (time.perf_counter() - start) * 1000
        if not line:
            raise RuntimeError("no response; stderr=%s" % proc.stderr.read()[-500:])
        resp = json.loads(line)
        return resp, elapsed

    return proc, run


def main():
    binary = Path(sys.argv[1]).resolve()
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 500_000
    dim = 128
    data_root = ROOT / ".tmp"
    data_root.mkdir(parents=True, exist_ok=True)
    data_dir = Path(tempfile.mkdtemp(prefix="stress-", dir=str(data_root)))
    proc, run = session(binary, data_dir)
    timings = {}

    resp, ms = run(f"CREATE TABLE big (id BIGINT PRIMARY KEY, category VARCHAR(8), embedding VECTOR({dim}));")
    assert resp["ok"], resp
    timings["create_ms"] = ms

    batch = 100
    slowest = 0.0
    start = time.perf_counter()
    for begin in range(0, rows, batch):
        values = []
        for i in range(begin, min(begin + batch, rows)):
            vec = "[" + ",".join(str(((i * 13 + j * 7) % 251) * 0.01) for j in range(dim)) + "]"
            values.append(f"({i}, 'c{i % 8}', '{vec}')")
        resp, ms = run("INSERT INTO big VALUES " + ",".join(values) + ";")
        if not resp.get("ok"):
            print("insert failed:", resp); return 1
        slowest = max(slowest, ms)
    timings["load_total_ms"] = (time.perf_counter() - start) * 1000
    timings["slowest_insert_request_ms"] = slowest

    q_vec = "[" + ",".join(["0.5"] * dim) + "]"
    for label, where in (("unfiltered", ""), ("filtered", " WHERE category = 'c3'")):
        worst = 0.0
        total = 0.0
        for k in range(10):
            sql = ("SELECT id, L2_DISTANCE(embedding, '%s') AS d FROM big%s ORDER BY d LIMIT 10;"
                   % (q_vec, where))
            resp, ms = run(sql)
            assert resp.get("ok"), resp
            assert len(resp["rows"]) == 10, resp
            worst = max(worst, ms)
            total += ms
        timings[label + "_avg_ms"] = total / 10
        timings[label + "_worst_ms"] = worst

    start = time.perf_counter()
    proc.stdin.write(json.dumps({"id": 999999, "op": "shutdown"}) + "\n")
    proc.stdin.flush()
    proc.stdout.readline()
    proc.wait(timeout=120)
    timings["shutdown_ms"] = (time.perf_counter() - start) * 1000

    start = time.perf_counter()
    proc, run = session(binary, data_dir)
    timings["reopen_ms"] = (time.perf_counter() - start) * 1000
    resp, ms = run("SELECT id FROM big WHERE id = 12345;")
    assert resp["ok"] and resp["rows"] == [[12345]], resp
    timings["spot_query_ms"] = ms
    proc.stdin.write(json.dumps({"id": 1000000, "op": "shutdown"}) + "\n")
    proc.stdin.flush()
    proc.stdout.readline()
    proc.wait(timeout=120)

    size = sum(p.stat().st_size for p in data_dir.rglob("*") if p.is_file())
    print("rows=%d dim=%d data_dir=%.1f MB" % (rows, dim, size / 1e6))
    for key in ("create_ms", "load_total_ms", "slowest_insert_request_ms",
                "unfiltered_avg_ms", "unfiltered_worst_ms",
                "filtered_avg_ms", "filtered_worst_ms",
                "shutdown_ms", "reopen_ms", "spot_query_ms"):
        print("  %-28s %.1f" % (key, timings[key]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
