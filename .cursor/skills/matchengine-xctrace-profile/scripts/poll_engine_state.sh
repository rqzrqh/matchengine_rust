#!/usr/bin/env bash
# Poll matcher HTTP state only (engine already running — use alongside Instruments GUI attach or manual profiling).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

usage() {
  echo "Usage: $0 [--duration-secs N] [--interval N] [--output-dir DIR] [CONFIG_PATH]"
  exit 1
}

DURATION_SECS=70
INTERVAL="${POLL_INTERVAL_SECS:-1}"
OUT_DIR=""
CONFIG_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration-secs) DURATION_SECS="${2:?}"; shift 2 ;;
    --interval) INTERVAL="${2:?}"; shift 2 ;;
    --output-dir) OUT_DIR="${2:?}"; shift 2 ;;
    -h|--help) usage ;;
    *)
      [[ -z "$CONFIG_PATH" ]] || usage
      CONFIG_PATH="$1"
      shift
      ;;
  esac
done

CONFIG_PATH="${CONFIG_PATH:-$REPO_ROOT/config.yaml}"
[[ -f "$CONFIG_PATH" ]] || { echo "config not found: $CONFIG_PATH" >&2; exit 1; }

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
OUT_DIR="${OUT_DIR:-$REPO_ROOT/profiling/state-$RUN_ID}"
mkdir -p "$OUT_DIR"
OUT_ABS="$(cd "$OUT_DIR" && pwd)"

python3 "$SCRIPT_DIR/engine_state_poll.py" \
  --base-url "$BASE_URL" \
  --market "$MARKET" \
  --interval "$INTERVAL" \
  --duration-secs "$DURATION_SECS" \
  --out-ndjson "$OUT_ABS/state-snapshots.ndjson" \
  --out-human-log "$OUT_ABS/http-state.log"

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
    "tool": "external",
    "template": "Instruments attach / sample / other",
    "time_limit": "${DURATION_SECS}s poll window",
    "command_file": "profile-command.txt",
    "trace_path": None,
    "exit_code": None,
    "sample_output_path": None,
    "notes": "Profiler trace captured separately; this folder holds HTTP state only.",
  },
  "state_poll": {
    "interval_secs": float("$INTERVAL"),
    "ndjson_path": "state-snapshots.ndjson",
    "human_log_path": "http-state.log",
  },
  "artifacts": {"console_log": None},
}
(out / "metadata.json").write_text(json.dumps(meta, indent=2))
(out / "profile-command.txt").write_text(
    "(paste your profiler command or path to .trace here)\n"
)
PY

python3 "$SCRIPT_DIR/gen_profile_report.py" \
  --metadata "$OUT_ABS/metadata.json" \
  --ndjson "$OUT_ABS/state-snapshots.ndjson" \
  --output "$OUT_ABS/PROFILE_REPORT.md"

echo "State-only run: $OUT_ABS"
