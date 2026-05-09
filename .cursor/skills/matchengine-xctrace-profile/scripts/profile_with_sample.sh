#!/usr/bin/env bash
# macOS `sample` on an existing matchengine PID while capturing HTTP state; emits PROFILE_REPORT.md with tables.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

usage() {
  echo "Usage: $0 [--interval SEC] [--output-dir DIR] PID DURATION_SECS [CONFIG_PATH]"
  echo "  Requires ./target/release/matchengine_rust already running (attach-style profiling)."
  exit 1
}

INTERVAL="${POLL_INTERVAL_SECS:-1}"
OUT_DIR=""
POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval) INTERVAL="${2:?}"; shift 2 ;;
    --output-dir) OUT_DIR="${2:?}"; shift 2 ;;
    -h|--help) usage ;;
    *) POSITIONAL+=("$1"); shift ;;
  esac
done

set -- "${POSITIONAL[@]}"
[[ $# -ge 2 ]] || usage
PID="$1"
DURATION_SECS="$2"
CONFIG_PATH="${3:-$REPO_ROOT/config.yaml}"

[[ -f "$CONFIG_PATH" ]] || { echo "config not found: $CONFIG_PATH" >&2; exit 1; }
if ! kill -0 "$PID" 2>/dev/null; then
  echo "no process $PID" >&2
  exit 1
fi

extract_market_name() {
  awk '
    /^market:/ { inmk=1; next }
    inmk && /^[[:alpha:]#]/ && !/^market:/ && $0 !~ /^[[:space:]]/ { exit }
    inmk && /^[[:space:]]+name:/ {
      v=$2
      gsub(/^["'\'']|["'\'']$/, "", v)
      print v
      exit
    }
  ' "$CONFIG_PATH"
}

MARKET="$(extract_market_name)"
[[ -n "$MARKET" ]] || { echo "could not parse market.name from $CONFIG_PATH" >&2; exit 1; }

BASE_URL="${ENGINE_HTTP:-http://127.0.0.1:8080}"
RUN_ID="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/profiling/sample-$RUN_ID}"
mkdir -p "$OUT_DIR"
OUT_ABS="$(cd "$OUT_DIR" && pwd)"

python3 "$SCRIPT_DIR/engine_state_poll.py" \
  --base-url "$BASE_URL" \
  --market "$MARKET" \
  --interval "$INTERVAL" \
  --duration-secs "$DURATION_SECS" \
  --out-ndjson "$OUT_ABS/state-snapshots.ndjson" \
  --out-human-log "$OUT_ABS/http-state.log" &
POLL_PID=$!

cleanup() { kill "$POLL_PID" 2>/dev/null || true; }
trap cleanup EXIT

set +e
sample "$PID" -file "$OUT_ABS/sample.txt" -mayDie -wait "$DURATION_SECS"
SAMPLE_EXIT=$?
set -e

wait "$POLL_PID" 2>/dev/null || true
trap - EXIT

echo "sample $PID -file sample.txt -mayDie -wait $DURATION_SECS" >"$OUT_ABS/profile-command.txt"

python3 <<PY
import json
from pathlib import Path
out = Path("$OUT_ABS")
meta = {
  "run_id": "$RUN_ID",
  "market": "$MARKET",
  "engine_http": "$BASE_URL",
  "out_dir": "$OUT_ABS",
  "profile": {
    "tool": "sample",
    "template": "sample(1) stack snapshot",
    "time_limit": "${DURATION_SECS}s",
    "command_file": "profile-command.txt",
    "trace_path": None,
    "exit_code": int("$SAMPLE_EXIT"),
    "sample_output_path": "sample.txt",
    "notes": "Plain-text stack report; merge mentally with HTTP tables below.",
  },
  "state_poll": {
    "interval_secs": float("$INTERVAL"),
    "ndjson_path": "state-snapshots.ndjson",
    "human_log_path": "http-state.log",
  },
  "artifacts": {"console_log": None},
}
(out / "metadata.json").write_text(json.dumps(meta, indent=2))
PY

python3 "$SCRIPT_DIR/gen_profile_report.py" \
  --metadata "$OUT_ABS/metadata.json" \
  --ndjson "$OUT_ABS/state-snapshots.ndjson" \
  --output "$OUT_ABS/PROFILE_REPORT.md" \
  --profile-exit-code "$SAMPLE_EXIT"

echo "sample run: $OUT_ABS"
