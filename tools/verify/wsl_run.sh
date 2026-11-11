#!/usr/bin/env bash
# Build the Linux x86_64 release binary inside WSL/Ubuntu and run the public
# MiniVectorDB protocol cases (all 20 by default).
#
# From Windows PowerShell:
#   wsl -d Ubuntu -e bash /mnt/d/4dsh/seekdb-competition/seekdb/tools/verify/wsl_run.sh
# From inside WSL, from the repository root:
#   bash tools/verify/wsl_run.sh [extra run_public.py args]
#
# Optional env overrides:
#   SOURCE_ROOT  repository root (default: auto-detected from this script)
#
# Extra arguments are forwarded to tools/verify/run_public.py, e.g.:
#   bash tools/verify/wsl_run.sh --handshake-only
#   bash tools/verify/wsl_run.sh --case sql_crud --case storage_restart
set -euo pipefail

SCRIPT_PATH="$0"
case "$SCRIPT_PATH" in
    */*) SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)" ;;
    *)   SCRIPT_DIR="$(pwd)" ;;
esac
if [ -n "$(printenv SOURCE_ROOT 2>/dev/null || true)" ]; then
    ROOT="$(printenv SOURCE_ROOT)"
else
    ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
fi
CRATE="$ROOT/minivectordb"
RESULT_ROOT="$ROOT/protocol-results"
STAMP="$(date +%Y%m%d-%H%M%S)"
BUILD_LOG="$RESULT_ROOT/wsl-build-$STAMP.log"
mkdir -p "$RESULT_ROOT"

if ! command -v cargo >/dev/null 2>&1; then
    echo "error: cargo not found; install a Linux Rust toolchain in WSL first" >&2
    echo "       (rustup: https://rustup.rs , then 'rustup default stable')" >&2
    exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
    echo "error: python3 not found in WSL; 'sudo apt-get install -y python3'" >&2
    exit 2
fi
if [ ! -f "$CRATE/Cargo.toml" ]; then
    echo "error: crate not found under: $CRATE" >&2
    echo "       set SOURCE_ROOT to the seekdb checkout root" >&2
    exit 2
fi

echo "== MiniVectorDB: Linux release build =="
echo "repo     : $ROOT"
echo "toolchain: $(rustc --version 2>/dev/null || echo unknown), $(cargo --version 2>/dev/null || echo unknown)"
( cd "$CRATE" && cargo build --release --locked 2>&1 | tee "$BUILD_LOG" )
echo "build log: $BUILD_LOG"

BIN="$CRATE/target/release/minivectordb"
if [ ! -x "$BIN" ]; then
    echo "error: expected Linux binary not found: $BIN" >&2
    exit 1
fi
file "$BIN" || true

echo ""
echo "== MiniVectorDB: public protocol cases (official protocol_check driver) =="
cd "$ROOT"
exec python3 "$ROOT/tools/verify/run_public.py" --binary "$BIN" "$@"
