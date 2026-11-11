#!/usr/bin/env python3
"""Small performance benchmark for MiniVectorDB (section 3.3 shape).

Workload (mirrors the published model):
  * write: 10,000 records x 128-dim vectors, 100 batches of 100 rows, one
    commit per batch; wall time includes persistence;
  * query: 1,000 exact-mode TopK queries, K=10, half of them filtered by an
    ordinary column equality; wall time excludes data loading;
  * resources: peak container memory (docker stats sampling) and an
    approximate /data I/O byte count.

The database protocol is the package JSONL protocol (STDIO_PROTOCOL.md).  The
harness reuses the portable transport from run_public.py, so it works against
a native binary on Windows/Linux or against the Docker image.

Usage:
  python tools/verify/benchmark.py --image minivectordb:round2
  python tools/verify/benchmark.py --binary minivectordb/target/release/minivectordb.exe
  python tools/verify/benchmark.py --image minivectordb:round2 --records 2000 --queries 200

Notes:
  * image mode bind-mounts a scratch data directory so the /data byte size can
    be measured from the host; on Linux make sure it is world-writable for the
    container uid (the script does chmod 777).
  * peak memory for image mode comes from docker stats (cgroup memory, includes
    page cache).  For binary mode it is VmHWM on Linux or the process peak
    working set on Windows.
  * io_bytes_approx is a lower bound: /proc/<pid>/io block I/O when available,
    otherwise the total size of files under /data.

Output: protocol-results/bench-<kind>-<timestamp>/benchmark.json plus a console
summary.  Exit code 0 when the workload completed and every response was valid.
"""
import argparse
import datetime
import json
import os
import random
import re
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[2]
RESULTS_ROOT = ROOT / "protocol-results"
sys.path.insert(0, str(HERE.parent))
from run_public import Session, scratch_dir, strict_json  # noqa: E402

DEFAULT_CONFIG = ROOT / "protocol_check" / "config.json"


def parse_args(argv):
    parser = argparse.ArgumentParser(description="MiniVectorDB performance benchmark.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary", help="path to a built minivectordb executable")
    target.add_argument("--image", help="Docker image tag")
    parser.add_argument("--records", type=int, default=10000)
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20261111)
    parser.add_argument("--autocommit", action="store_true",
                        help="send batches without explicit BEGIN/COMMIT")
    parser.add_argument("--index", action="store_true",
                        help="CREATE VECTOR INDEX before the query phase")
    parser.add_argument("--output", type=Path,
                        help="output directory (default: timestamped protocol-results/bench-*)")
    return parser.parse_args(argv)


def now_stamp():
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


def vector_literal(values):
    return "'[" + ", ".join("%.6f" % value for value in values) + "]'"


def dir_size_bytes(path):
    total = 0
    try:
        for entry in Path(path).rglob("*"):
            try:
                if entry.is_file():
                    total += entry.stat().st_size
            except OSError:
                pass
    except OSError:
        pass
    return total


def read_proc_io(pid):
    try:
        text = Path("/proc/%d/io" % pid).read_text(encoding="utf-8")
    except OSError:
        return None
    values = {}
    for line in text.splitlines():
        key, _, value = line.partition(":")
        if key in ("read_bytes", "write_bytes"):
            try:
                values[key] = int(value.strip())
            except ValueError:
                pass
    if "read_bytes" in values and "write_bytes" in values:
        return values["read_bytes"] + values["write_bytes"]
    return None


def read_proc_peak_mb(pid):
    try:
        text = Path("/proc/%d/status" % pid).read_text(encoding="utf-8")
    except OSError:
        return None
    match = re.search(r"(?m)^VmHWM:\s*(\d+)\s*kB", text)
    return int(match.group(1)) / 1024.0 if match else None


def windows_peak_mb(pid):
    try:
        import ctypes
        import ctypes.wintypes as wt

        class Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wt.DWORD), ("PageFaultCount", wt.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        kernel32 = ctypes.windll.kernel32
        psapi = ctypes.windll.psapi
        handle = kernel32.OpenProcess(0x0400 | 0x0010, False, pid)
        if not handle:
            return None
        try:
            counters = Counters()
            counters.cb = ctypes.sizeof(counters)
            if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters),
                                              ctypes.sizeof(counters)):
                return None
            return counters.PeakWorkingSetSize / (1024.0 * 1024.0)
        finally:
            kernel32.CloseHandle(handle)
    except Exception:  # noqa: BLE001 - optional measurement only
        return None


def parse_mem_usage(text):
    """docker stats MemUsage, e.g. '123.4MiB / 2GiB' -> MiB."""
    match = re.search(r"([0-9.]+)\s*([KMG]i?B)", text)
    if not match:
        return None
    value = float(match.group(1))
    unit = match.group(2).upper()
    factor = {"B": 1.0 / 1048576.0, "KB": 1.0 / 1024.0, "KIB": 1.0 / 1024.0,
              "MB": 1.0, "MIB": 1.0, "GB": 1024.0, "GIB": 1024.0}[unit]
    return value * factor


class DockerMemorySampler(threading.Thread):
    """Poll docker stats in the background; keep the maximum memory reading."""

    def __init__(self, container):
        super().__init__(daemon=True)
        self.container = container
        self.peak_mb = None
        self._stop = threading.Event()
        self.samples = 0

    def run(self):
        while not self._stop.is_set():
            try:
                done = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}",
                     self.container],
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    timeout=30, text=True, check=False)
                if done.returncode == 0:
                    value = parse_mem_usage(done.stdout.strip())
                    if value is not None:
                        self.samples += 1
                        self.peak_mb = value if self.peak_mb is None else max(self.peak_mb, value)
            except (OSError, subprocess.SubprocessError):
                pass
            self._stop.wait(0.25)

    def stop(self):
        self._stop.set()
        self.join(timeout=10)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.records <= 0 or args.batch <= 0 or args.dim <= 0 or args.queries <= 0:
        print("error: --records/--batch/--dim/--queries must be positive", file=sys.stderr)
        return 2
    config = strict_json(DEFAULT_CONFIG.read_text(encoding="utf-8"))
    config.update(startup_timeout=120, request_timeout=300, case_timeout=7200)
    kind = "image" if args.image else "binary"
    out_dir = args.output or (RESULTS_ROOT / ("bench-%s-%s" % (kind, now_stamp())))
    if out_dir.exists():
        print("error: output directory already exists: %s" % out_dir, file=sys.stderr)
        return 2
    out_dir.mkdir(parents=True, exist_ok=False)

    data_dir = None
    bind_dir = None
    if args.image:
        data_dir = tempfile.mkdtemp(prefix="mvdb-bench-data-", dir=scratch_dir())
        if os.name != "nt":
            os.chmod(data_dir, 0o777)
        bind_dir = data_dir
    command = None
    if args.binary:
        binary = Path(args.binary).expanduser().resolve()
        if not binary.is_file():
            print("error: --binary %s is not a file" % binary, file=sys.stderr)
            return 2
        command = [str(binary), "--data-dir", "{data_dir}"]

    rng = random.Random(args.seed)
    vectors = [[rng.uniform(-1.0, 1.0) for _ in range(args.dim)]
               for _ in range(args.records)]
    tags = [index % 10 for index in range(args.records)]
    query_vectors = [[rng.uniform(-1.0, 1.0) for _ in range(args.dim)]
                     for _ in range(args.queries)]
    query_tags = [rng.randrange(10) for _ in range(args.queries)]

    errors = []
    result = {
        "generated_at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "scope": "local benchmark approximation of competition section 3.3",
        "target": {"kind": kind, "value": args.image or args.binary},
        "params": {"records": args.records, "dim": args.dim, "batch": args.batch,
                   "queries": args.queries, "k": args.k, "seed": args.seed,
                   "transactions": not args.autocommit, "vector_index": args.index},
    }
    session = None
    try:
        session = Session(config, out_dir / "benchmark.stderr.log",
                          image=args.image, command=command, bind_dir=bind_dir)
        started = time.perf_counter()
        session.start()
        result["startup_ms"] = round((time.perf_counter() - started) * 1000, 1)

        def execute(sql):
            response = session.request(sql, mode="exact")
            if response.get("ok") is not True:
                raise RuntimeError("SQL failed: %s -> %r" % (sql[:160], response))
            return response

        execute("CREATE TABLE bench (id BIGINT PRIMARY KEY, tag BIGINT, "
                "embedding VECTOR(%d));" % args.dim)

        # -- write phase -------------------------------------------------- #
        write_ms = 0.0
        written = 0
        for start in range(0, args.records, args.batch):
            stop = min(start + args.batch, args.records)
            rows = ["(%d, %d, %s)" % (index + 1, tags[index], vector_literal(vectors[index]))
                    for index in range(start, stop)]
            sql = "INSERT INTO bench VALUES " + ", ".join(rows) + ";"
            batch_start = time.perf_counter()
            if not args.autocommit:
                execute("BEGIN;")
            response = execute(sql)
            if not args.autocommit:
                execute("COMMIT;")
            write_ms += (time.perf_counter() - batch_start) * 1000.0
            written += stop - start
            if response.get("affected_rows") != stop - start:
                errors.append("batch %d: affected_rows=%r expected %d"
                              % (start // args.batch, response.get("affected_rows"),
                                 stop - start))
        result["rows_written"] = written
        result["T_write_ms"] = round(write_ms, 1)

        if args.index:
            index_start = time.perf_counter()
            execute("CREATE VECTOR INDEX bench_vec ON bench(embedding);")
            result["index_build_ms"] = round((time.perf_counter() - index_start) * 1000, 1)

        # -- query phase -------------------------------------------------- #
        sampler = None
        if args.image:
            sampler = DockerMemorySampler(session.name)
            sampler.start()
        query_ms = 0.0
        rows_returned = 0
        unique_ok = True
        for index, vector in enumerate(query_vectors):
            filtered = index % 2 == 0
            where = " WHERE tag = %d" % query_tags[index] if filtered else ""
            sql = ("SELECT id, L2_DISTANCE(embedding, %s) AS distance FROM bench%s "
                   "ORDER BY distance LIMIT %d;" % (vector_literal(vector), where, args.k))
            query_start = time.perf_counter()
            response = execute(sql)
            query_ms += (time.perf_counter() - query_start) * 1000.0
            rows = response.get("rows")
            if not isinstance(rows, list) or len(rows) != args.k:
                errors.append("query %d: expected %d rows, got %r"
                              % (index, args.k, None if rows is None else len(rows)))
                continue
            ids = [row[0] for row in rows if isinstance(row, list) and row]
            rows_returned += len(rows)
            if len(set(ids)) != len(ids):
                unique_ok = False
                errors.append("query %d: duplicate ids in result" % index)
        if sampler is not None:
            sampler.stop()
            result["peak_mem_mb"] = round(sampler.peak_mb, 1) if sampler.peak_mb else None
            result["peak_mem_source"] = "docker_stats" if sampler.peak_mb else None
            result["mem_samples"] = sampler.samples
        result["T_query_ms"] = round(query_ms, 1)
        result["queries_ok"] = args.queries - sum(1 for item in errors if item.startswith("query "))
        result["rows_returned"] = rows_returned
        result["unique_ids"] = unique_ok

        # -- resource accounting ----------------------------------------- #
        pid = session.proc.pid if session.proc is not None else None
        io_bytes = None
        if pid is not None and os.name == "posix":
            io_bytes = read_proc_io(pid)
            if io_bytes is not None:
                result["io_bytes_source"] = "proc_pid_io"
            if result.get("peak_mem_mb") is None:
                result["peak_mem_mb"] = round(read_proc_peak_mb(pid) or 0.0, 1) or None
                result["peak_mem_source"] = "proc_status_vmhwm" if result["peak_mem_mb"] else None
        if pid is not None and os.name == "nt" and result.get("peak_mem_mb") is None:
            peak = windows_peak_mb(pid)
            result["peak_mem_mb"] = round(peak, 1) if peak else None
            result["peak_mem_source"] = "windows_peak_working_set" if peak else None
        measured_dir = data_dir or getattr(session, "data_dir", None)
        if measured_dir:
            result["data_dir_bytes"] = dir_size_bytes(measured_dir)
            if io_bytes is None:
                io_bytes = result["data_dir_bytes"]
                result["io_bytes_source"] = "data_dir_size_lower_bound"
        result["io_bytes_approx"] = io_bytes
        result["write_rows_per_second"] = round(
            written / (write_ms / 1000.0), 1) if write_ms > 0 else None
        result["query_qps"] = round(args.queries / (query_ms / 1000.0), 1) \
            if query_ms > 0 else None

        session.shutdown()
    except Exception as exc:  # noqa: BLE001 - report and keep the partial result
        errors.append("%s: %s" % (type(exc).__name__, exc))
    finally:
        if session is not None:
            try:
                session.close()
            except Exception as exc:  # noqa: BLE001
                errors.append("cleanup failed: %s" % exc)
        if bind_dir:
            import shutil
            shutil.rmtree(bind_dir, ignore_errors=True)

    result["errors"] = errors
    result["ok"] = not errors
    (out_dir / "benchmark.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print("")
    print("MiniVectorDB benchmark (%s: %s)" % (kind, args.image or args.binary))
    print("  workload : %d rows, dim=%d, batch=%d, %d queries K=%d, seed=%d"
          % (args.records, args.dim, args.batch, args.queries, args.k, args.seed))
    for key in ("startup_ms", "T_write_ms", "T_query_ms", "write_rows_per_second",
                "query_qps", "peak_mem_mb", "peak_mem_source", "io_bytes_approx",
                "io_bytes_source", "data_dir_bytes", "index_build_ms",
                "rows_written", "rows_returned"):
        if result.get(key) is not None:
            print("  %-22s %s" % (key, result[key]))
    if errors:
        print("  errors:")
        for item in errors[:10]:
            print("    - " + item)
    print("  JSON: %s" % (out_dir / "benchmark.json"))
    print("  RESULT: %s" % ("PASS" if not errors else "FAIL"))
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
