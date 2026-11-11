#!/usr/bin/env python3
"""Run the public protocol_check cases against a locally built binary.

Usage:
  python minivectordb/tools/run_public.py <path-to-binary> [extra run.py args]
"""
import datetime
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    binary = str(Path(sys.argv[1]).resolve())
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = ROOT / "protocol-results" / ("dev-" + stamp)
    command = json.dumps([binary, "--data-dir", "{data_dir}"])
    argv = [
        sys.executable,
        str(ROOT / "protocol_check" / "run.py"),
        "--command",
        command,
        "--output",
        str(out_dir),
    ] + sys.argv[2:]
    code = subprocess.call(argv)
    result = out_dir / "result.json"
    if result.exists():
        report = json.loads(result.read_text())
        passed = sum(1 for c in report["cases"] if c["status"] == "PASS")
        print("passed %d/%d cases -> %s" % (passed, len(report["cases"]), out_dir))
    return code


if __name__ == "__main__":
    sys.exit(main())
