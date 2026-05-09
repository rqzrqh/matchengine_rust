#!/usr/bin/env python3
"""Build PROFILE_REPORT.md with markdown tables from metadata + state NDJSON."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from bottleneck_from_trace import build_bottleneck_section
from report_viz import mermaid_pie_chart, mermaid_xy_lines, snapshot_health_strip


def esc_cell(s: object) -> str:
    t = "" if s is None else str(s)
    t = t.replace("\n", " ").replace("|", "\\|")
    return t


def dash(v: object) -> str:
    return "—" if v is None else str(v)


def md_table(headers: list[str], rows: list[list[object]]) -> str:
    lines = [
        "| " + " | ".join(esc_cell(h) for h in headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(esc_cell(c) for c in row) + " |")
    return "\n".join(lines)


def settle_digest(ids: object) -> str:
    if not isinstance(ids, list) or not ids:
        return ""
    nums = []
    for x in ids:
        try:
            nums.append(int(x))
        except (TypeError, ValueError):
            pass
    if not nums:
        return str(ids)[:48]
    return f"max={max(nums)} n={len(nums)}"


def parse_num(x: object) -> float | None:
    if x is None:
        return None
    if isinstance(x, bool):
        return None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        return float(x)
    if isinstance(x, str):
        try:
            return float(x)
        except ValueError:
            return None
    return None


def rows_from_ndjson(path: Path) -> list[dict]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def stat_block(title: str, keys: list[str], series: list[dict], prefix: str) -> str:
    stats_headers = ["metric", "min", "max", "delta (last - first)"]
    stat_rows: list[list[object]] = []
    for k in keys:
        vals: list[float] = []
        for r in series:
            v = parse_num(r.get(k))
            if v is not None:
                vals.append(v)
        if len(vals) < 2:
            continue
        stat_rows.append([f"{prefix}.{k}", min(vals), max(vals), vals[-1] - vals[0]])
    if not stat_rows:
        return ""
    lines = [f"### {title}", "", md_table(stats_headers, stat_rows), ""]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate PROFILE_REPORT.md tables.")
    ap.add_argument("--metadata", required=True, type=Path)
    ap.add_argument("--ndjson", required=True, type=Path)
    ap.add_argument("--output", required=True, type=Path)
    ap.add_argument("--profile-exit-code", default="", help="Override metadata profile.exit_code (e.g. xctrace)")
    ap.add_argument(
        "--trace",
        type=Path,
        default=None,
        help="Instruments .trace for CPU bottleneck section (default: out_dir/trace from metadata)",
    )
    ap.add_argument(
        "--no-bottleneck",
        action="store_true",
        help="Skip xctrace export + CPU bottleneck section",
    )
    args = ap.parse_args()

    meta = json.loads(args.metadata.read_text(encoding="utf-8"))
    if args.profile_exit_code != "":
        try:
            meta.setdefault("profile", {})["exit_code"] = int(args.profile_exit_code)
        except ValueError:
            meta.setdefault("profile", {})["exit_code"] = args.profile_exit_code
    nd_rows = rows_from_ndjson(args.ndjson)

    summary_series: list[dict] = []
    status_series: list[dict] = []
    timeline_meta: list[list[object]] = []
    for r in nd_rows:
        ts = r.get("ts", "")
        summary_series.append(r.get("summary") or {})
        status_series.append(r.get("status") or {})
        errs = r.get("errors") or []
        timeline_meta.append([ts, "; ".join(errs) if errs else "ok"])

    summary_keys = [
        "ask_count",
        "bid_count",
        "ask_stock_amount",
        "bid_stock_amount",
        "ask_money_amount",
        "bid_money_amount",
    ]
    status_keys = [
        "oper_id",
        "order_id",
        "deals_id",
        "message_id",
        "input_offset",
        "input_sequence_id",
        "pushed_quote_deals_id",
    ]

    sum_tbl_rows: list[list[object]] = []
    for r, sm in zip(nd_rows, summary_series):
        sum_tbl_rows.append(
            [
                r.get("ts", ""),
                sm.get("ask_count", ""),
                sm.get("bid_count", ""),
                sm.get("ask_stock_amount", ""),
                sm.get("bid_stock_amount", ""),
                sm.get("ask_money_amount", ""),
                sm.get("bid_money_amount", ""),
            ]
        )

    st_tbl_rows: list[list[object]] = []
    for r, st in zip(nd_rows, status_series):
        st_tbl_rows.append(
            [
                r.get("ts", ""),
                st.get("oper_id", ""),
                st.get("order_id", ""),
                st.get("deals_id", ""),
                st.get("message_id", ""),
                st.get("input_offset", ""),
                st.get("input_sequence_id", ""),
                st.get("pushed_quote_deals_id", ""),
                settle_digest(st.get("pushed_settle_message_ids")),
            ]
        )

    prof = meta.get("profile") or {}
    state_poll = meta.get("state_poll") or {}

    arts = meta.get("artifacts") or {}
    artifacts_rows = [
        ["Instruments / trace", dash(prof.get("trace_path"))],
        ["Profiler console log", dash(arts.get("console_log"))],
        ["State NDJSON", dash(state_poll.get("ndjson_path"))],
        ["Human-readable HTTP log", dash(state_poll.get("human_log_path"))],
        ["Profiler command file", dash(prof.get("command_file"))],
        ["Sample output (if used)", dash(prof.get("sample_output_path"))],
    ]

    profile_tbl = [
        ["Tool", dash(prof.get("tool"))],
        ["Template / mode", dash(prof.get("template"))],
        ["Time limit", dash(prof.get("time_limit"))],
        ["Exit code", dash(prof.get("exit_code"))],
        ["Notes", dash(prof.get("notes"))],
    ]

    out: list[str] = []
    out.append(f"# Matching engine profile report ({meta.get('run_id', '')})")
    out.append("")
    out.append("## Run metadata")
    out.append("")
    out.append(md_table(["field", "value"], [["Market", meta.get("market", "")], ["HTTP base", meta.get("engine_http", "")]]))
    out.append("")
    out.append(md_table(["field", "value"], profile_tbl))
    out.append("")
    out.append("## Artifacts")
    out.append("")
    out.append(md_table(["kind", "relative path"], artifacts_rows))
    out.append("")
    out.append("## Profiler command (verbatim)")
    out.append("")
    cmd_file = Path(meta.get("out_dir", ".")) / prof.get("command_file", "profile-command.txt")
    if cmd_file.is_file():
        body = cmd_file.read_text(encoding="utf-8").strip()
        out.append("```bash")
        out.append(body)
        out.append("```")
    else:
        out.append("_profile-command.txt not found (check metadata `out_dir`)._")
    out.append("")
    out.append(f"## State snapshots ({len(nd_rows)} samples)")
    out.append("")
    if not nd_rows:
        out.append("_No NDJSON rows — HTTP may have been unreachable during the run._")
        out.append("")
    else:
        out.append("### Snapshot health")
        out.append("")
        ok_c = sum(1 for r in nd_rows if not (r.get("errors") or []))
        err_c = len(nd_rows) - ok_c
        out.append(
            "_**Mermaid** figures first (Cursor / GitHub previews); ASCII strip is a fallback reader. "
            "Per-timestamp table is below._"
        )
        out.append("")
        out.append("#### Chart — HTTP poll outcomes (row counts)")
        out.append("")
        if ok_c > 0 and err_c > 0:
            pie_h = mermaid_pie_chart("Poll rows: ok vs errors", [("ok", float(ok_c)), ("errors", float(err_c))])
            if pie_h:
                out.append(pie_h)
                out.append("")
        elif ok_c > 0:
            out.append(f"_All {ok_c} poll rows succeeded (no error slice for a pie chart)._")
            out.append("")
        elif err_c > 0:
            out.append(f"_All {err_c} poll rows reported errors._")
            out.append("")
        strip = snapshot_health_strip(ok_c, err_c)
        if strip:
            out.append(strip)
            out.append("")

        ok_only = [r for r in nd_rows if not (r.get("errors") or [])]
        if len(ok_only) >= 2:
            pq: list[float] = []
            ac: list[float] = []
            bc: list[float] = []
            for r in ok_only:
                st = r.get("status") or {}
                sm = r.get("summary") or {}
                pq.append(float(parse_num(st.get("pushed_quote_deals_id")) or 0.0))
                ac.append(float(parse_num(sm.get("ask_count")) or 0.0))
                bc.append(float(parse_num(sm.get("bid_count")) or 0.0))
            out.append("#### Chart — `/status` vs `/summary` (ok rows; x = subsample index)")
            out.append("")
            out.append(
                mermaid_xy_lines(
                    "pushed_quote_deals_id",
                    {"pushed_quote_deals_id": pq},
                    max_points=32,
                    y_axis_label="pushed_quote_deals_id",
                )
            )
            out.append("")
            out.append(
                mermaid_xy_lines(
                    "ask_count and bid_count",
                    {"ask_count": ac, "bid_count": bc},
                    max_points=32,
                    y_axis_label="count",
                )
            )
            out.append("")

        out.append("_Per-timestamp poll log:_")
        out.append("")
        out.append(md_table(["timestamp", "errors"], timeline_meta))
        out.append("")

        out.append("### Order book summary (`/summary`)")
        out.append("")
        out.append(
            md_table(
                ["ts", "ask_cnt", "bid_cnt", "ask_stock", "bid_stock", "ask_money", "bid_money"],
                sum_tbl_rows,
            )
        )
        out.append("")
        out.append("### Cursors & publish (`/status`)")
        out.append("")
        out.append(
            md_table(
                [
                    "ts",
                    "oper_id",
                    "order_id",
                    "deals_id",
                    "msg_id",
                    "input_off",
                    "in_seq",
                    "pushed_quote",
                    "settle_ids",
                ],
                st_tbl_rows,
            )
        )
        out.append("")

        blk1 = stat_block(
            "Numeric rollups — summary",
            summary_keys,
            summary_series,
            "summary",
        )
        blk2 = stat_block(
            "Numeric rollups — status",
            status_keys,
            status_series,
            "status",
        )
        out.append("## Aggregate statistics")
        out.append("")
        if blk1:
            out.append(blk1)
        if blk2:
            out.append(blk2)
        if not blk1 and not blk2:
            out.append("_No numeric fields suitable for min/max/delta._")
            out.append("")

    out_dir_p = Path(meta.get("out_dir", "."))
    trace_path: Path | None = args.trace
    if trace_path is None and not args.no_bottleneck:
        rel = prof.get("trace_path") or "matchengine.trace"
        cand = out_dir_p / rel
        if cand.exists():
            trace_path = cand

    if trace_path is not None and trace_path.exists() and not args.no_bottleneck:
        out.append(build_bottleneck_section(trace_path, nd_rows))
    elif args.trace is not None and not args.trace.exists():
        out.append("## Bottleneck synthesis (CPU + HTTP)")
        out.append("")
        out.append(f"_Trace bundle not found: `{args.trace}`._")
        out.append("")

    out.append("## How to combine with Instruments")
    out.append("")
    out.append(
        "- Open the `.trace` (Time Profiler) and align the Instruments timeline with **`ts`** rows above.\n"
        "- Compare **`input_*`** vs **`pushed_*`** while inspecting hot stacks in publish / Kafka / matcher threads (`doc/thread-model.md`)."
    )
    out.append("")

    args.output.write_text("\n".join(out), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
