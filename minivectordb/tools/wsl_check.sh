#!/usr/bin/env bash
# Build the crate with the WSL Rust toolchain and run the official public cases.
# Usage: bash minivectordb/tools/wsl_check.sh [extra run.py args...]
set -uo pipefail
export HOME=/home/prts
export PATH=/home/prts/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export CARGO_TARGET_DIR=/home/prts/target-mvdb
REPO=/mnt/d/4dsh/seekdb-competition/seekdb
BIN=/home/prts/target-mvdb/release/minivectordb

cd "$REPO/minivectordb" || exit 2
if ! cargo build --release 2>&1 | tail -2; then
  echo "BUILD FAILED"
  exit 2
fi

cd "$REPO" || exit 2
STAMP=$(date +%Y%m%d-%H%M%S)
OUT="protocol-results/wsl-$STAMP"
COMMAND_JSON='["'"$BIN"'","--data-dir","{data_dir}"]'
python3 protocol_check/run.py --command "$COMMAND_JSON" --output "$OUT" "$@"
rc=$?
echo "---- summary ----"
python3 - "$OUT" <<'PY'
import json, sys, pathlib
out = pathlib.Path(sys.argv[1])
res = out / "result.json"
if not res.exists():
    print("no result.json at", res); raise SystemExit(1)
report = json.loads(res.read_text())
cases = report["cases"]
passed = sum(1 for c in cases if c["status"] == "PASS")
print("PASS %d / %d  (report: %s)" % (passed, len(cases), res))
for c in cases:
    if c["status"] != "PASS":
        print("  FAIL %-32s %s" % (c["id"], c["message"][:400]))
PY
exit $rc
