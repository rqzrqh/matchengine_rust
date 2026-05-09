#!/usr/bin/env bash
# Launch matchengine under xctrace Time Profiler while polling HTTP state; emit PROFILE_REPORT.md with tables.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

usage() {
  echo "Usage: $0 [--stop-existing] [--time-limit 1m] [--output-dir DIR] [CONFIG_PATH]"
  echo "  Writes profiling/<run>/ with matchengine.trace, state-snapshots.ndjson, PROFILE_REPORT.md, ..."
  exit 1
}

STOP_EXISTING=0
TIME_LIMIT="1m"
OUT_DIR=""
CONFIG_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --stop-existing) STOP_EXISTING=1; shift ;;
    --time-limit) TIME_LIMIT="${2:?}"; shift 2 ;;
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

RUN_ID="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/profiling/xctrace-$RUN_ID}"
mkdir -p "$OUT_DIR"
OUT_DIR_ABS="$(cd "$OUT_DIR" && pwd)"

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

duration_limit_secs() {
  local z="$1"
  if [[ "$z" =~ ^([0-9]+)(ms|s|m|h)?$ ]]; then
    local n="${BASH_REMATCH[1]}"
    local u="${BASH_REMATCH[2]:-s}"
    case "$u" in
      ms) echo $((n / 1000)) ;;
      s) echo "$n" ;;
      m) echo $((n * 60)) ;;
      h) echo $((n * 3600)) ;;
    esac
  else
    echo 60
  fi
}

MARKET="$(extract_market_name)"
[[ -n "$MARKET" ]] || { echo "could not parse market.name from $CONFIG_PATH" >&2; exit 1; }

BASE_URL="${ENGINE_HTTP:-http://127.0.0.1:8080}"
ENGINE_BIN="$REPO_ROOT/target/release/matchengine_rust"
POLL_IV="${POLL_INTERVAL_SECS:-1}"
LIM="$(duration_limit_secs "$TIME_LIMIT")"
POLL_DURATION=$((LIM + 25))

cd "$REPO_ROOT"

if [[ ! -x "$ENGINE_BIN" ]]; then
  echo "Building release binary..."
  cargo build --release
fi

if [[ "$STOP_EXISTING" -eq 1 ]]; then
  pids="$(pgrep -f '/target/release/matchengine_rust' || true)"
  if [[ -n "$pids" ]]; then
    echo "Stopping existing matchengine (launch mode needs process lock): $pids"
    kill -9 $pids 2>/dev/null || true
    sleep 0.5
  fi
fi

PROFILE_CMD=$(cat <<EOF
xcrun xctrace record \\
  --template 'Time Profiler' \\
  --time-limit '$TIME_LIMIT' \\
  --no-prompt \\
  --env RUST_LOG=${RUST_LOG:-warn} \\
  --target-stdout - \\
  --output '$OUT_DIR_ABS/matchengine.trace' \\
  --launch -- '$ENGINE_BIN' '$CONFIG_PATH'
EOF
)
echo "$PROFILE_CMD" > "$OUT_DIR_ABS/profile-command.txt"

python3 -c "
import json, pathlib
meta = {
  'run_id': '$RUN_ID',
  'market': '$MARKET',
  'engine_http': '$BASE_URL',
  'out_dir': '$OUT_DIR_ABS',
  'profile': {
    'tool': 'xctrace',
    'template': 'Time Profiler',
    'time_limit': '$TIME_LIMIT',
    'command_file': 'profile-command.txt',
    'trace_path': 'matchengine.trace',
    'exit_code': None,
    'sample_output_path': None,
    'notes': 'Under --launch, Time Profiler is reliable here; Allocations often fails to attach.',
  },
  'state_poll': {
    'interval_secs': float('$POLL_IV'),
    'ndjson_path': 'state-snapshots.ndjson',
    'human_log_path': 'http-state.log',
  },
  'artifacts': {'console_log': 'xctrace-console.log'},
}
pathlib.Path('$OUT_DIR_ABS', 'metadata.json').write_text(json.dumps(meta, indent=2))
"

python3 "$SCRIPT_DIR/engine_state_poll.py" \
  --base-url "$BASE_URL" \
  --market "$MARKET" \
  --interval "$POLL_IV" \
  --duration-secs "$POLL_DURATION" \
  --out-ndjson "$OUT_DIR_ABS/state-snapshots.ndjson" \
  --out-human-log "$OUT_DIR_ABS/http-state.log" &
POLL_PID=$!

cleanup() { kill "$POLL_PID" 2>/dev/null || true; }
trap cleanup EXIT

set +e
xcrun xctrace record \
  --template 'Time Profiler' \
  --time-limit "$TIME_LIMIT" \
  --no-prompt \
  --env RUST_LOG="${RUST_LOG:-warn}" \
  --target-stdout - \
  --output "$OUT_DIR_ABS/matchengine.trace" \
  --launch -- "$ENGINE_BIN" "$CONFIG_PATH" \
  2>&1 | tee "$OUT_DIR_ABS/xctrace-console.log"
XCTRACE_EXIT=${PIPESTATUS[0]}
set -e

wait "$POLL_PID" 2>/dev/null || true
trap - EXIT

python3 "$SCRIPT_DIR/gen_profile_report.py" \
  --metadata "$OUT_DIR_ABS/metadata.json" \
  --ndjson "$OUT_DIR_ABS/state-snapshots.ndjson" \
  --output "$OUT_DIR_ABS/PROFILE_REPORT.md" \
  --profile-exit-code "$XCTRACE_EXIT"

echo ""
echo "Done: $OUT_DIR_ABS"
echo "Trace: $OUT_DIR_ABS/matchengine.trace"
echo "Report: $OUT_DIR_ABS/PROFILE_REPORT.md"
