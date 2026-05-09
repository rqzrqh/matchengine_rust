---
name: matchengine-xctrace-profile
description: >-
  Guides profiling of this repo's matching engine on macOS: records reproducible
  xctrace (Instruments) launch commands, includes runnable helpers under this
  skill's scripts/ directory, polls HTTP matcher state alongside CPU profiling
  (Time Profiler, sample(1), or external traces), and produces PROFILE_REPORT.md
  with markdown tables for combined hotspot-and-state analysis. Use when profiling
  matchengine_rust, Instruments, xctrace Time Profiler, engine HTTP summary/status
  correlation, or Kafka publish backlog investigations.
---

# Matching engine profiling + HTTP state

## Objective

Keep **profiler artifacts** (`.trace`, `sample.txt`, …) together with **matcher-visible state**:

| HTTP endpoint | Role |
|---------------|------|
| `GET /markets/{market}/summary` | Order-book aggregates (`doc/http-api.md`, market summary) |
| `GET /markets/{market}/status` | Cursors + **`pushed_*`** publish confirmation (`doc/http-api.md`, internal status) |

Bundled scripts next to **`SKILL.md`** (`scripts/`) write **`state-snapshots.ndjson`**, **`http-state.log`**, **`metadata.json`**, and **`PROFILE_REPORT.md`** (tables + aggregates). Outputs go under repo **`profiling/`** unless **`--output-dir`** overrides it. Correlate report timestamps with the profiler timeline.

## Cursor agent behavior

When this skill applies, **start profiling without prompting for confirmation**. For **`profile_with_xctrace.sh`**, **default to `--stop-existing`** so any running `./target/release/matchengine_rust` is terminated before **`xctrace … --launch`**: duplicate instances hit **`process_lock`** and exit immediately. Only skip **`--stop-existing`** if the user explicitly wants to keep another engine instance alive (then attach-mode / **`poll_engine_state.sh`** instead of the launch bundle). Respect user overrides such as **`--time-limit`**, **`--output-dir`**, or a **`CONFIG_PATH`** argument.

---

## Canonical CLI (`launch` + Time Profiler)

**Launch-mode profiling must stop duplicate binaries**: another `./target/release/matchengine_rust` holds `process_lock` and exits immediately.

```bash
cd /path/to/matchengine_rust

xcrun xctrace record \
  --template 'Time Profiler' \
  --time-limit 1m \
  --no-prompt \
  --env RUST_LOG=warn \
  --target-stdout - \
  --output ./matchengine.trace \
  --launch -- ./target/release/matchengine_rust
```

Pass **`./config.yaml`** or another config path **after** the binary when not running from repo root:

```bash
  --launch -- ./target/release/matchengine_rust /abs/path/config.yaml
```

**Note:** On typical setups **Allocations** (`--instrument Allocations` or the Allocations template) fails under **`--launch`** with *Failed to attach to target process*. Prefer **Time Profiler** for automated launch captures.

---

## Skill layout & bundled scripts

This skill follows Cursor’s layout: **`SKILL.md`** plus **`scripts/`** beside it (`matchengine-xctrace-profile/scripts/`). Prefer invoking helpers via repo-relative paths from `matchengine_rust` root:

`SCRIPT_SKILL=".cursor/skills/matchengine-xctrace-profile/scripts"`

| File under `SCRIPT_SKILL` | Role |
|---------------------------|------|
| **`profile_with_xctrace.sh`** | Full bundle: optional **`--stop-existing`**, parallel NDJSON poll, xctrace **launch**, **`PROFILE_REPORT.md`** with tables. |
| **`profile_with_sample.sh`** | Existing PID + macOS **`sample`**, concurrent NDJSON poll, stacks + tables (**`sample.txt`**). |
| **`poll_engine_state.sh`** | Attach-style workflows — HTTP polls only; paste profiler notes into **`profile-command.txt`**, merge traces manually. |
| **`engine_state_poll.py`** | Low-level poller (used by shell wrappers). |
| **`gen_profile_report.py`** | Regenerate **`PROFILE_REPORT.md`** from **`metadata.json`** + NDJSON. |

### Automated launch + tables

```bash
SCRIPT_SKILL=".cursor/skills/matchengine-xctrace-profile/scripts"
"$SCRIPT_SKILL/profile_with_xctrace.sh" --stop-existing
# optional: --time-limit 1m --output-dir profiling/my-run [CONFIG_PATH]
```

Artifacts directory (`profiling/xctrace-<timestamp>/` by default):

- `matchengine.trace` — Instruments Time Profiler  
- `state-snapshots.ndjson` — machine-readable timeline  
- `http-state.log` — readable snapshots  
- `xctrace-console.log`, `profile-command.txt`, `metadata.json`, **`PROFILE_REPORT.md`**

Env: **`ENGINE_HTTP`**, **`RUST_LOG`**, **`POLL_INTERVAL_SECS`** (defaults to **1** s in shell helpers for ~1 Hz HTTP snapshots alongside the launch capture).

### Attach / Instruments GUI + state

Reuse **`SCRIPT_SKILL`** from [above](#skill-layout--bundled-scripts).

1. Start the engine normally.  
2. Record with Instruments (**Attach**) as usual.  
3. In parallel:  

```bash
SCRIPT_SKILL=".cursor/skills/matchengine-xctrace-profile/scripts"
"$SCRIPT_SKILL/poll_engine_state.sh" --duration-secs 70
```

4. Drop `.trace` path + interpretation notes into that folder’s **`PROFILE_REPORT.md`** by hand, or rerun **`gen_profile_report.py`**:

```bash
SCRIPT_SKILL=".cursor/skills/matchengine-xctrace-profile/scripts"
python3 "$SCRIPT_SKILL/gen_profile_report.py" \
  --metadata profiling/state-<run>/metadata.json \
  --ndjson profiling/state-<run>/state-snapshots.ndjson \
  --output profiling/state-<run>/PROFILE_REPORT.md
```

### `sample` stacks + tables

```bash
SCRIPT_SKILL=".cursor/skills/matchengine-xctrace-profile/scripts"
PID=$(pgrep -f '/target/release/matchengine_rust' | head -1)
"$SCRIPT_SKILL/profile_with_sample.sh" "$PID" 60
```

---

## Reading `PROFILE_REPORT.md`

Sections:

1. **Run metadata / artifacts / verbatim profiler command**  
2. **Snapshot health** — curl failures vs timestamps  
3. **`/summary` table** — book churn vs profiler spikes  
4. **`/status` table** — `input_*` vs `pushed_*` backlog hints  
5. **Aggregate statistics** — numeric min / max / delta  
6. **Bottleneck synthesis (CPU + HTTP)** — `xctrace export` parses the `.trace` Time Profiler table **after** aligning sample timestamps with the trace TOC **`start-date`**, optionally **restricting CPU tables** to the wall-clock span between first/last HTTP-successful `/status` polls. Tables cover thread buckets, heuristic work areas, hottest symbols, HTTP signals (**quote + settle** array maxima / lag proxies), and short interpretation bullets (verify in Instruments).

Combine with **`doc/thread-model.md`** when blaming matcher vs publish threads.
