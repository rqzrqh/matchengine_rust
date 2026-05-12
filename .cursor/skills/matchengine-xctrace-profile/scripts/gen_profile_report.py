#!/usr/bin/env python3
"""Build PROFILE_REPORT.md with markdown tables from metadata + state NDJSON."""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from bottleneck_from_trace import build_bottleneck_section, trace_recording_start_utc
from report_viz import mermaid_pie_chart, mermaid_xy_lines, snapshot_health_strip, write_svg_line_chart


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


def get_path(d: dict, path: tuple[str, ...]) -> object:
    cur: object = d
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def numeric_path_values(series: list[dict], path: tuple[str, ...]) -> list[float]:
    vals: list[float] = []
    for r in series:
        v = parse_num(get_path(r, path))
        if v is not None:
            vals.append(v)
    return vals


def list_path_values(series: list[dict], path: tuple[str, ...], mode: str) -> list[tuple[float, int | None]]:
    vals: list[tuple[float, int | None]] = []
    for r in series:
        obj = get_path(r, path)
        if not isinstance(obj, list) or not obj:
            continue
        nums: list[tuple[float, int]] = []
        for idx, x in enumerate(obj):
            v = parse_num(x)
            if v is not None:
                nums.append((v, idx))
        if not nums:
            continue
        if mode == "sum":
            vals.append((sum(v for v, _ in nums), None))
        elif mode == "max":
            vals.append(max(nums, key=lambda x: x[0]))
        else:
            raise ValueError(f"unknown list rollup mode: {mode}")
    return vals


def object_group_peak(series: list[dict], path: tuple[str, ...], value_key: str) -> tuple[float | None, int | None]:
    best_value: float | None = None
    best_group: int | None = None
    for r in series:
        obj = get_path(r, path)
        if not isinstance(obj, dict):
            continue
        value = parse_num(obj.get(value_key))
        if value is None:
            continue
        if best_value is None or value > best_value:
            best_value = value
            group = parse_num(obj.get("group_id"))
            best_group = int(group) if group is not None else None
    return best_value, best_group


def fmt_peak_group(group_id: int | None) -> str:
    return "—" if group_id is None else f"group={group_id}"


def fmt_peak_index(index: int | None, label: str) -> str:
    return "—" if index is None else f"{label}={index}"


def stat_row(metric: str, vals: list[float], peak: str = "—") -> list[object] | None:
    if len(vals) < 2:
        return None
    return [metric, fmt_num(min(vals)), fmt_num(max(vals)), fmt_num(vals[-1] - vals[0]), peak]


def publish_backlog_rollup_rows(status_series: list[dict]) -> list[list[object]]:
    rows: list[list[object]] = []

    numeric_paths = [
        ("publish_backlog.quote_pending", ("publish_backlog", "quote_pending")),
        ("publish_backlog.settle_pending", ("publish_backlog", "settle_pending")),
        ("publish_backlog.quote_in_flight", ("publish_backlog", "quote_in_flight")),
        ("publish_backlog.quote_channel_full_count", ("publish_backlog", "quote_channel_full_count")),
        ("publish_backlog.quote_channel_blocked_nanos", ("publish_backlog", "quote_channel_blocked_nanos")),
        ("publish_backlog.quote_channel_max_blocked_nanos", ("publish_backlog", "quote_channel_max_blocked_nanos")),
        ("publish_backlog.quote_queue_full_count", ("publish_backlog", "quote_queue_full_count")),
        ("publish_lag.quote_deals", ("publish_lag", "quote_deals")),
    ]
    for metric, path in numeric_paths:
        row = stat_row(metric, numeric_path_values(status_series, path))
        if row:
            rows.append(row)

    list_paths = [
        ("publish_backlog.settle_group_pending.max", ("publish_backlog", "settle_group_pending"), "max", "group"),
        (
            "publish_backlog.settle_worker_outstanding.max",
            ("publish_backlog", "settle_worker_outstanding"),
            "max",
            "worker",
        ),
        (
            "publish_backlog.settle_worker_queue_full_counts.total",
            ("publish_backlog", "settle_worker_queue_full_counts"),
            "sum",
            "worker",
        ),
        (
            "publish_backlog.settle_worker_queue_full_counts.max_worker",
            ("publish_backlog", "settle_worker_queue_full_counts"),
            "max",
            "worker",
        ),
        ("publish_lag.settle_group_lags.max", ("publish_lag", "settle_group_lags"), "max", "group"),
    ]
    for metric, path, mode, peak_label in list_paths:
        vals_with_peak = list_path_values(status_series, path, mode)
        vals = [v for v, _ in vals_with_peak]
        peak_idx = max(vals_with_peak, key=lambda x: x[0])[1] if vals_with_peak else None
        row = stat_row(metric, vals, fmt_peak_index(peak_idx, peak_label))
        if row:
            rows.append(row)

    peak_pending, pending_group = object_group_peak(
        status_series, ("publish_backlog", "max_settle_group_pending"), "pending"
    )
    peak_lag, lag_group = object_group_peak(status_series, ("publish_lag", "max_settle_group_lag"), "lag")
    if peak_pending is not None:
        rows.append(
            [
                "publish_backlog.max_settle_group_pending.peak",
                "—",
                fmt_num(peak_pending),
                "—",
                fmt_peak_group(pending_group),
            ]
        )
    if peak_lag is not None:
        rows.append(
            [
                "publish_lag.max_settle_group_lag.peak",
                "—",
                fmt_num(peak_lag),
                "—",
                fmt_peak_group(lag_group),
            ]
        )

    return rows


def publish_backlog_rollup_block(status_series: list[dict]) -> str:
    rows = publish_backlog_rollup_rows(status_series)
    if not rows:
        return ""
    headers = ["metric", "min", "max", "delta (last - first)", "peak index"]
    lines = ["### Publish backlog and lag — status", "", md_table(headers, rows), ""]
    return "\n".join(lines)


def _series_num(status_series: list[dict], path: tuple[str, ...]) -> list[float]:
    return [float(parse_num(get_path(st, path)) or 0.0) for st in status_series]


def _series_list_sum(status_series: list[dict], path: tuple[str, ...]) -> list[float]:
    vals: list[float] = []
    for st in status_series:
        obj = get_path(st, path)
        if not isinstance(obj, list):
            vals.append(0.0)
            continue
        vals.append(sum(float(parse_num(x) or 0.0) for x in obj))
    return vals


def _series_list_max(status_series: list[dict], path: tuple[str, ...]) -> list[float]:
    vals: list[float] = []
    for st in status_series:
        obj = get_path(st, path)
        if not isinstance(obj, list) or not obj:
            vals.append(0.0)
            continue
        vals.append(max(float(parse_num(x) or 0.0) for x in obj))
    return vals


def _settle_worker_queue_series(status_series: list[dict]) -> dict[str, list[float]]:
    worker_count = int(_max_path_num(status_series, ("runtime_status", "settle_message_queue", "worker_count")) or 0)
    groups_per_worker = int(
        _max_path_num(status_series, ("runtime_status", "settle_message_queue", "groups_per_worker")) or 0
    )
    if worker_count <= 0 or groups_per_worker <= 0:
        worker_count = int(_max_path_num(status_series, ("publish_backlog", "settle_worker_count")) or 0)
        groups_per_worker = int(_max_path_num(status_series, ("publish_backlog", "settle_groups_per_worker")) or 0)
    if worker_count <= 0 or groups_per_worker <= 0:
        return {}

    series = {f"settle_worker_{worker_id}": [] for worker_id in range(worker_count)}
    for st in status_series:
        group_pending = get_path(st, ("runtime_status", "settle_message_queue", "group_pending"))
        if not isinstance(group_pending, list):
            group_pending = get_path(st, ("publish_backlog", "settle_group_pending"))
        for worker_id in range(worker_count):
            start = worker_id * groups_per_worker
            end = start + groups_per_worker
            pending = 0.0
            if isinstance(group_pending, list):
                pending = sum(float(parse_num(v) or 0.0) for v in group_pending[start:end])
            series[f"settle_worker_{worker_id}"].append(pending)
    return series


def _has_nonzero(series: dict[str, list[float]]) -> bool:
    return any(any(v != 0 for v in values) for values in series.values())


def _max_path_num(status_series: list[dict], path: tuple[str, ...]) -> float | None:
    vals = numeric_path_values(status_series, path)
    return max(vals) if vals else None


def parse_iso_datetime(ts_raw: object) -> datetime | None:
    if ts_raw is None:
        return None
    s = str(ts_raw).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def elapsed_labels_from_rows(rows: list[dict]) -> list[str]:
    times = [parse_iso_datetime(r.get("ts")) for r in rows]
    valid = [dt for dt in times if dt is not None]
    if not valid:
        return [str(i) for i in range(len(rows))]
    start = valid[0]
    labels: list[str] = []
    for idx, dt in enumerate(times):
        if dt is None:
            labels.append(str(idx))
            continue
        elapsed = (dt - start).total_seconds()
        labels.append(f"{elapsed:.3f}s")
    return labels


def svg_chart_md(
    charts_dir: Path,
    filename: str,
    title: str,
    series: dict[str, list[float]],
    x_labels: list[str],
    *,
    label_every: int = 10,
    y_axis_label: str = "value",
    reference_lines: dict[str, float] | None = None,
) -> str:
    path = charts_dir / filename
    if not write_svg_line_chart(
        path,
        title,
        series,
        x_labels,
        label_every=label_every,
        y_axis_label=y_axis_label,
        reference_lines=reference_lines,
    ):
        return ""
    return f"![{title}](charts/{filename})"


def queue_growth_charts(status_series: list[dict], x_labels: list[str], charts_dir: Path) -> str:
    if len(status_series) < 2:
        return ""
    if len(x_labels) != len(status_series):
        x_labels = [str(i) for i in range(len(status_series))]

    charts: list[str] = ["### Queue Growth Curves", ""]
    charts.append(
        "_Curves use successful `/status` snapshots only. The x-axis is elapsed time from the first successful `/status` response._"
    )
    charts.append("")
    charts.append(
        "_Offer, quote, and settle queues are split so each chart tracks one queue family against its own capacity._"
    )
    charts.append("")

    main_queue = {
        "offer_pending_or_blocked": _series_num(status_series, ("main_task_queue", "offer_pending_or_blocked")),
    }
    main_queue_limits = {}
    main_capacity = _max_path_num(status_series, ("main_task_queue", "capacity"))
    if main_capacity:
        main_queue_limits["main_capacity"] = main_capacity
    if _has_nonzero(main_queue):
        chart = svg_chart_md(
            charts_dir,
            "offer-task-queue.svg",
            "Offer task queue pending / blocked",
            main_queue,
            x_labels,
            label_every=10,
            y_axis_label="tasks",
            reference_lines=main_queue_limits,
        )
        if chart:
            charts.append(chart)
            charts.append("")

    settle_channel_cap = _max_path_num(status_series, ("publish_backlog", "settle_channel_capacity_per_worker"))
    settle_worker_count = _max_path_num(status_series, ("publish_backlog", "settle_worker_count"))
    quote_queue = {
        "quote_pending": _series_num(status_series, ("runtime_status", "quote_message_queue", "pending")),
    }
    if not _has_nonzero(quote_queue):
        quote_queue = {"quote_pending": _series_num(status_series, ("publish_backlog", "quote_pending"))}
    quote_queue_limits = {}
    quote_channel_cap = _max_path_num(status_series, ("runtime_status", "quote_message_queue", "capacity"))
    if not quote_channel_cap:
        quote_channel_cap = _max_path_num(status_series, ("publish_backlog", "quote_channel_capacity"))
    if quote_channel_cap:
        quote_queue_limits["quote_channel_capacity"] = quote_channel_cap
    if _has_nonzero(quote_queue):
        chart = svg_chart_md(
            charts_dir,
            "quote-message-queue.svg",
            "Quote message queue pending",
            quote_queue,
            x_labels,
            label_every=10,
            y_axis_label="messages",
            reference_lines=quote_queue_limits,
        )
        if chart:
            charts.append(chart)
            charts.append("")

    settle_worker_queues = _settle_worker_queue_series(status_series)
    settle_queue_limits = {}
    if settle_channel_cap:
        settle_queue_limits["settle_channel_capacity_per_worker"] = settle_channel_cap
    if _has_nonzero(settle_worker_queues):
        chart = svg_chart_md(
            charts_dir,
            "settle-message-queues-by-worker.svg",
            "Settle message queues by worker",
            settle_worker_queues,
            x_labels,
            label_every=10,
            y_axis_label="messages",
            reference_lines=settle_queue_limits,
        )
        if chart:
            charts.append(chart)
            charts.append("")

    kafka_windows = {
        "quote_in_flight": _series_num(status_series, ("publish_backlog", "quote_in_flight")),
        "settle_worker_outstanding_sum": _series_list_sum(
            status_series, ("publish_backlog", "settle_worker_outstanding")
        ),
        "settle_worker_outstanding_max": _series_list_max(
            status_series, ("publish_backlog", "settle_worker_outstanding")
        ),
    }
    kafka_window_limits = {}
    quote_max_outstanding = _max_path_num(status_series, ("publish_backlog", "quote_max_outstanding"))
    settle_worker_max_outstanding = _max_path_num(
        status_series, ("publish_backlog", "settle_worker_max_outstanding")
    )
    if quote_max_outstanding:
        kafka_window_limits["quote_max_outstanding"] = quote_max_outstanding
    if settle_worker_max_outstanding and settle_worker_count:
        kafka_window_limits["settle_worker_outstanding_total_cap"] = (
            settle_worker_max_outstanding * settle_worker_count
        )
    if settle_worker_max_outstanding:
        kafka_window_limits["settle_worker_outstanding_cap"] = settle_worker_max_outstanding
    if _has_nonzero(kafka_windows):
        chart = svg_chart_md(
            charts_dir,
            "kafka-delivery-windows.svg",
            "Kafka delivery windows",
            kafka_windows,
            x_labels,
            label_every=10,
            y_axis_label="in-flight",
            reference_lines=kafka_window_limits,
        )
        if chart:
            charts.append(chart)
            charts.append("")

    publish_lag = {
        "quote_lag": _series_num(status_series, ("publish_lag", "quote_deals")),
        "max_settle_group_lag": _series_num(status_series, ("publish_lag", "max_settle_group_lag", "lag")),
    }
    if _has_nonzero(publish_lag):
        chart = svg_chart_md(
            charts_dir,
            "publish-lag.svg",
            "Publish lag",
            publish_lag,
            x_labels,
            label_every=10,
            y_axis_label="messages",
        )
        if chart:
            charts.append(chart)
            charts.append("")

    return "\n".join(charts) if len(charts) > 3 else ""


def status_throughput_charts(ok_rows: list[dict], charts_dir: Path) -> str:
    if len(ok_rows) < 2:
        return ""

    labels = elapsed_labels_from_rows(ok_rows)
    throughput_labels = labels[1:]
    if not throughput_labels:
        return ""

    def status_at(idx: int) -> dict:
        st = ok_rows[idx].get("status") or {}
        return st if isinstance(st, dict) else {}

    def numeric_delta_per_sec(key: str) -> list[float]:
        vals: list[float] = []
        for idx in range(1, len(ok_rows)):
            prev_ts = parse_iso_datetime(ok_rows[idx - 1].get("ts"))
            cur_ts = parse_iso_datetime(ok_rows[idx].get("ts"))
            elapsed = (cur_ts - prev_ts).total_seconds() if prev_ts and cur_ts else 0.0
            prev = parse_num(status_at(idx - 1).get(key))
            cur = parse_num(status_at(idx).get(key))
            if elapsed <= 0 or prev is None or cur is None:
                vals.append(0.0)
                continue
            vals.append(max(cur - prev, 0.0) / elapsed)
        return vals

    def list_max_delta_per_sec(key: str) -> list[float]:
        vals: list[float] = []
        for idx in range(1, len(ok_rows)):
            prev_ts = parse_iso_datetime(ok_rows[idx - 1].get("ts"))
            cur_ts = parse_iso_datetime(ok_rows[idx].get("ts"))
            elapsed = (cur_ts - prev_ts).total_seconds() if prev_ts and cur_ts else 0.0
            prev = list_max_num(status_at(idx - 1).get(key))
            cur = list_max_num(status_at(idx).get(key))
            if elapsed <= 0 or prev is None or cur is None:
                vals.append(0.0)
                continue
            vals.append(max(cur - prev, 0.0) / elapsed)
        return vals

    matcher = {
        "input_messages_per_sec": numeric_delta_per_sec("input_offset"),
        "deals_created_per_sec": numeric_delta_per_sec("deals_id"),
    }
    publish = {
        "quote_pushed_per_sec": numeric_delta_per_sec("pushed_quote_deals_id"),
        "settle_pushed_max_per_sec": list_max_delta_per_sec("pushed_settle_message_ids"),
    }

    charts: list[str] = ["### Pipeline Throughput Curves", ""]
    charts.append(
        "_These rates are derived from consecutive successful `/status` snapshots; use them to separate high throughput from queue buildup._"
    )
    charts.append("")

    matcher_chart = svg_chart_md(
        charts_dir,
        "matcher-throughput.svg",
        "Matcher input and deal throughput",
        matcher,
        throughput_labels,
        label_every=10,
        y_axis_label="messages / sec",
    )
    if matcher_chart:
        charts.append(matcher_chart)
        charts.append("")

    publish_chart = svg_chart_md(
        charts_dir,
        "publish-throughput.svg",
        "Publish acknowledgement throughput",
        publish,
        throughput_labels,
        label_every=10,
        y_axis_label="messages / sec",
    )
    if publish_chart:
        charts.append(publish_chart)
        charts.append("")

    return "\n".join(charts) if len(charts) > 3 else ""


def instruments_landmarks_section(ok_rows: list[dict], trace_start: datetime | None) -> str:
    if len(ok_rows) < 2:
        return ""

    anchor = trace_start or parse_iso_datetime(ok_rows[0].get("ts"))
    if anchor is None:
        return ""

    def status_at(idx: int) -> dict:
        st = ok_rows[idx].get("status") or {}
        return st if isinstance(st, dict) else {}

    def rel_s(row: dict) -> str:
        dt = parse_iso_datetime(row.get("ts"))
        if dt is None:
            return "—"
        return fmt_num((dt - anchor).total_seconds(), "s")

    def elapsed(idx: int) -> float:
        a = parse_iso_datetime(ok_rows[idx - 1].get("ts"))
        b = parse_iso_datetime(ok_rows[idx].get("ts"))
        return (b - a).total_seconds() if a and b else 0.0

    def peak_numeric_rate(key: str) -> tuple[int, float]:
        best_idx = 1
        best_rate = 0.0
        for idx in range(1, len(ok_rows)):
            dt = elapsed(idx)
            prev = parse_num(status_at(idx - 1).get(key))
            cur = parse_num(status_at(idx).get(key))
            if dt <= 0 or prev is None or cur is None:
                continue
            rate = max(cur - prev, 0.0) / dt
            if rate > best_rate:
                best_idx = idx
                best_rate = rate
        return best_idx, best_rate

    def peak_list_rate(key: str) -> tuple[int, float]:
        best_idx = 1
        best_rate = 0.0
        for idx in range(1, len(ok_rows)):
            dt = elapsed(idx)
            prev = list_max_num(status_at(idx - 1).get(key))
            cur = list_max_num(status_at(idx).get(key))
            if dt <= 0 or prev is None or cur is None:
                continue
            rate = max(cur - prev, 0.0) / dt
            if rate > best_rate:
                best_idx = idx
                best_rate = rate
        return best_idx, best_rate

    def peak_path(path: tuple[str, ...]) -> tuple[int, float]:
        best_idx = 0
        best_value = 0.0
        for idx, row in enumerate(ok_rows):
            value = parse_num(get_path(status_at(idx), path)) or 0.0
            if value > best_value:
                best_idx = idx
                best_value = value
        return best_idx, best_value

    def input_done_idx() -> int:
        vals = [parse_num(status_at(idx).get("input_offset")) for idx in range(len(ok_rows))]
        max_val = max((v for v in vals if v is not None), default=None)
        if max_val is None:
            return len(ok_rows) - 1
        for idx, value in enumerate(vals):
            if value == max_val:
                return idx
        return len(ok_rows) - 1

    rows: list[list[object]] = []
    rows.append(["first successful status", rel_s(ok_rows[0]), ok_rows[0].get("ts", ""), "HTTP/state chart zero point"])

    idx, value = peak_numeric_rate("input_offset")
    rows.append(["peak input throughput", rel_s(ok_rows[idx]), ok_rows[idx].get("ts", ""), f"{fmt_num(value)} msg/s"])

    idx, value = peak_numeric_rate("deals_id")
    rows.append(["peak deal throughput", rel_s(ok_rows[idx]), ok_rows[idx].get("ts", ""), f"{fmt_num(value)} deals/s"])

    idx, value = peak_path(("main_task_queue", "offer_pending_or_blocked"))
    rows.append(["peak offer queue", rel_s(ok_rows[idx]), ok_rows[idx].get("ts", ""), fmt_num(value)])

    idx, value = peak_path(("runtime_status", "quote_message_queue", "pending"))
    rows.append(["peak quote queue", rel_s(ok_rows[idx]), ok_rows[idx].get("ts", ""), fmt_num(value)])

    idx, value = peak_path(("runtime_status", "settle_message_queue", "pending"))
    rows.append(["peak settle queue", rel_s(ok_rows[idx]), ok_rows[idx].get("ts", ""), fmt_num(value)])

    idx, value = peak_path(("publish_lag", "quote_deals"))
    rows.append(["peak quote publish lag", rel_s(ok_rows[idx]), ok_rows[idx].get("ts", ""), fmt_num(value)])

    idx, value = peak_path(("publish_lag", "max_settle_group_lag", "lag"))
    rows.append(["peak settle publish lag", rel_s(ok_rows[idx]), ok_rows[idx].get("ts", ""), fmt_num(value)])

    idx, value = peak_numeric_rate("pushed_quote_deals_id")
    rows.append(["peak quote ack throughput", rel_s(ok_rows[idx]), ok_rows[idx].get("ts", ""), f"{fmt_num(value)} msg/s"])

    idx, value = peak_list_rate("pushed_settle_message_ids")
    rows.append(["peak settle ack throughput", rel_s(ok_rows[idx]), ok_rows[idx].get("ts", ""), f"{fmt_num(value)} msg/s"])

    idx = input_done_idx()
    rows.append(["input backlog drained", rel_s(ok_rows[idx]), ok_rows[idx].get("ts", ""), "input_offset reached max"])

    anchor_label = "trace TOC start-date" if trace_start else "first successful /status"
    lines = [
        "### Instruments Relative-Time Landmarks",
        "",
        f"_Relative seconds are measured from **{anchor_label}**. Use these ranges when selecting time in Instruments._",
        "",
        md_table(["event", "relative time", "wall-clock UTC", "value"], rows),
        "",
    ]
    return "\n".join(lines)


def offer_consumer_obj(status: dict) -> dict:
    obj = get_path(status, ("runtime_status", "offer_consumer"))
    if isinstance(obj, dict):
        return obj
    obj = status.get("offer_consumer")
    return obj if isinstance(obj, dict) else {}


def offer_consumer_10ms_block(status_series: list[dict], include_details: bool = False) -> str:
    snapshots = [offer_consumer_obj(st) for st in status_series]
    snapshots = [s for s in snapshots if s]
    if not snapshots:
        return ""

    latest = snapshots[-1]
    bucket_width_ms = parse_num(latest.get("bucket_width_ms"))
    window_buckets = parse_num(latest.get("window_buckets"))
    buckets_by_generation: dict[int, dict] = {}
    for snapshot in snapshots:
        buckets = snapshot.get("buckets")
        if not isinstance(buckets, list):
            continue
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            generation = parse_num(bucket.get("generation"))
            if generation is None:
                continue
            buckets_by_generation[int(generation)] = bucket

    buckets = [buckets_by_generation[k] for k in sorted(buckets_by_generation)]
    total_bucket_messages = sum(int(parse_num(b.get("messages")) or 0) for b in buckets)
    active_bucket_count = len(
        [b for b in buckets if (parse_num(b.get("messages")) or 0) > 0 or (parse_num(b.get("recv_errors")) or 0) > 0]
    )
    peak_messages_bucket = max(buckets, key=lambda b: parse_num(b.get("messages")) or 0) if buckets else {}
    peak_recv_wait_bucket = max(buckets, key=lambda b: parse_num(b.get("recv_wait_max_nanos")) or 0) if buckets else {}
    peak_send_bucket = max(buckets, key=lambda b: parse_num(b.get("send_max_nanos")) or 0) if buckets else {}

    def bucket_window(bucket: dict) -> str:
        start_ms = parse_num(bucket.get("start_ms"))
        width = bucket_width_ms
        if start_ms is None or width is None:
            return "—"
        return f"{int(start_ms)}-{int(start_ms + width)}ms"

    def format_unix_ms(ms: float | None) -> str:
        if ms is None:
            return "—"
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")

    def bucket_wall_window(bucket: dict) -> str:
        start_unix_ms = parse_num(bucket.get("start_unix_ms"))
        width = bucket_width_ms
        if start_unix_ms is None or width is None:
            return "—"
        return f"{format_unix_ms(start_unix_ms)}..{format_unix_ms(start_unix_ms + width)}"

    def avg_nanos(bucket: dict, key: str) -> str:
        messages = parse_num(bucket.get("messages")) or 0
        total = parse_num(bucket.get(key))
        if messages <= 0 or total is None:
            return "—"
        return fmt_num(total / messages, "ns")

    summary_rows = [
        ["bucket width", fmt_num(bucket_width_ms, "ms"), "Per-bucket resolution emitted by `/status`"],
        ["window buckets", fmt_num(window_buckets), "Ring capacity in one status snapshot"],
        [
            "metric start",
            format_unix_ms(parse_num(latest.get("start_unix_ms"))),
            "Wall-clock anchor for aligning runtime buckets with xctrace",
        ],
        ["active buckets captured", fmt_num(active_bucket_count), "Non-empty 10ms buckets seen across HTTP snapshots"],
        ["bucket messages captured", fmt_num(total_bucket_messages), "Sum of messages in deduplicated buckets"],
        [
            "latest total messages",
            fmt_num(parse_num(latest.get("total_messages"))),
            "Cumulative messages observed by offer-consumer",
        ],
        [
            "latest total recv wait",
            fmt_num(parse_num(latest.get("total_recv_wait_nanos")), "ns"),
            "Cumulative time spent awaiting `consumer.recv()`",
        ],
        [
            "latest total parse",
            fmt_num(parse_num(latest.get("total_parse_nanos")), "ns"),
            "Cumulative MessagePack parse time",
        ],
        [
            "latest total main-queue send",
            fmt_num(parse_num(latest.get("total_send_nanos")), "ns"),
            "Cumulative `MainTaskSender::send` time",
        ],
        [
            "peak messages / 10ms",
            f"{fmt_num(parse_num(peak_messages_bucket.get('messages')))} at {bucket_window(peak_messages_bucket)}",
            "Burstiest successful receive+parse+send bucket",
        ],
        [
            "peak recv wait",
            f"{fmt_num(parse_num(peak_recv_wait_bucket.get('recv_wait_max_nanos')), 'ns')} at {bucket_window(peak_recv_wait_bucket)}",
            "Largest single await before `consumer.recv()` returned",
        ],
        [
            "peak main-queue send",
            f"{fmt_num(parse_num(peak_send_bucket.get('send_max_nanos')), 'ns')} at {bucket_window(peak_send_bucket)}",
            "Largest single send into the matcher queue",
        ],
    ]

    detail_rows = []
    for bucket in buckets:
        messages = parse_num(bucket.get("messages")) or 0
        recv_errors = parse_num(bucket.get("recv_errors")) or 0
        if messages <= 0 and recv_errors <= 0:
            continue
        detail_rows.append(
            [
                bucket_window(bucket),
                bucket_wall_window(bucket),
                fmt_num(messages),
                fmt_num(parse_num(bucket.get("payload_bytes"))),
                fmt_num(parse_num(bucket.get("recv_wait_nanos")), "ns"),
                fmt_num(parse_num(bucket.get("recv_wait_max_nanos")), "ns"),
                fmt_num(parse_num(bucket.get("parse_nanos")), "ns"),
                avg_nanos(bucket, "parse_nanos"),
                fmt_num(parse_num(bucket.get("send_nanos")), "ns"),
                fmt_num(parse_num(bucket.get("send_max_nanos")), "ns"),
                fmt_num(recv_errors),
            ]
        )

    lines = [
        "### Offer consumer — 10ms runtime buckets",
        "",
        md_table(["signal", "value", "meaning"], summary_rows),
        "",
    ]
    if include_details:
        lines.extend(
            [
                "#### Offer consumer 10ms bucket details",
                "",
            ]
        )
        if detail_rows:
            lines.append(
                md_table(
                    [
                        "window",
                        "wall-clock UTC",
                        "messages",
                        "bytes",
                        "recv_wait_total",
                        "recv_wait_max",
                        "parse_total",
                        "parse_avg",
                        "send_total",
                        "send_max",
                        "recv_errors",
                    ],
                    detail_rows,
                )
            )
        else:
            lines.append("_No non-empty offer consumer buckets were captured._")
        lines.append("")
    return "\n".join(lines)


def offer_consumer_bucket_series(status_series: list[dict]) -> tuple[list[str], dict[str, list[float]]]:
    buckets_by_generation: dict[int, dict] = {}
    metric_start_ms: float | None = None
    for status in status_series:
        snapshot = offer_consumer_obj(status)
        start_ms = parse_num(snapshot.get("start_unix_ms"))
        if start_ms is not None and (metric_start_ms is None or start_ms < metric_start_ms):
            metric_start_ms = start_ms
        buckets = snapshot.get("buckets")
        if not isinstance(buckets, list):
            continue
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            generation = parse_num(bucket.get("generation"))
            if generation is None:
                continue
            buckets_by_generation[int(generation)] = bucket

    if not buckets_by_generation:
        return [], {}

    buckets = [buckets_by_generation[k] for k in sorted(buckets_by_generation)]
    if metric_start_ms is None:
        metric_start_ms = parse_num(buckets[0].get("start_unix_ms")) or 0.0

    labels: list[str] = []
    messages: list[float] = []
    recv_wait_ms: list[float] = []
    parse_ms: list[float] = []
    send_ms: list[float] = []
    for bucket in buckets:
        start_unix_ms = parse_num(bucket.get("start_unix_ms"))
        if start_unix_ms is not None:
            labels.append(f"{(start_unix_ms - metric_start_ms) / 1000.0:.3f}s")
        else:
            labels.append(str(int(parse_num(bucket.get("generation")) or len(labels))))
        messages.append(float(parse_num(bucket.get("messages")) or 0.0))
        recv_wait_ms.append(float(parse_num(bucket.get("recv_wait_nanos")) or 0.0) / 1_000_000.0)
        parse_ms.append(float(parse_num(bucket.get("parse_nanos")) or 0.0) / 1_000_000.0)
        send_ms.append(float(parse_num(bucket.get("send_nanos")) or 0.0) / 1_000_000.0)

    return labels, {
        "messages_per_10ms": messages,
        "recv_wait_ms": recv_wait_ms,
        "parse_ms": parse_ms,
        "send_ms": send_ms,
    }


def fmt_num(v: float | None, suffix: str = "") -> str:
    if v is None:
        return "—"
    if abs(v - round(v)) < 1e-9:
        return f"{int(round(v)):,}{suffix}"
    return f"{v:,.4f}".rstrip("0").rstrip(".") + suffix


def first_last_numeric(rows: list[dict], key: str) -> tuple[float | None, float | None, float | None]:
    vals: list[float] = []
    for r in rows:
        v = parse_num(r.get(key))
        if v is not None:
            vals.append(v)
    if len(vals) < 2:
        return None, None, None
    return vals[0], vals[-1], vals[-1] - vals[0]


def list_max_num(obj: object) -> float | None:
    if not isinstance(obj, list) or not obj:
        return None
    vals: list[float] = []
    for x in obj:
        v = parse_num(x)
        if v is not None:
            vals.append(v)
    return max(vals) if vals else None


def first_last_list_max(rows: list[dict], key: str) -> tuple[float | None, float | None, float | None]:
    vals: list[float] = []
    for r in rows:
        v = list_max_num(r.get(key))
        if v is not None:
            vals.append(v)
    if len(vals) < 2:
        return None, None, None
    return vals[0], vals[-1], vals[-1] - vals[0]


def compact_span(rows: list[dict]) -> str:
    if not rows:
        return "—"
    first = rows[0].get("ts", "")
    last = rows[-1].get("ts", "")
    if first == last:
        return str(first)
    return f"{first} → {last}"


def lag_label(lag: float | None) -> str:
    if lag is None:
        return "unknown"
    if lag <= 0:
        return "caught up"
    return f"lag {fmt_num(lag)}"


def quick_read_section(nd_rows: list[dict], ok_only: list[dict], ok_c: int, err_c: int) -> str:
    total = ok_c + err_c
    if total <= 0:
        return "## At a Glance\n\n_No HTTP state rows were captured._\n"

    ok_pct = 100.0 * ok_c / total
    status_rows = [(r.get("status") or {}) for r in ok_only if isinstance(r.get("status"), dict)]
    summary_rows = [(r.get("summary") or {}) for r in ok_only if isinstance(r.get("summary"), dict)]

    _, _, input_delta = first_last_numeric(status_rows, "input_offset")
    _, deals_end, deals_delta = first_last_numeric(status_rows, "deals_id")
    _, quote_end, quote_delta = first_last_numeric(status_rows, "pushed_quote_deals_id")
    _, msg_end, msg_delta = first_last_numeric(status_rows, "message_id")
    _, settle_msg_end, _ = first_last_list_max(status_rows, "settle_message_ids")
    _, settle_push_end, settle_delta = first_last_list_max(status_rows, "pushed_settle_message_ids")

    quote_lag = (deals_end - quote_end) if deals_end is not None and quote_end is not None else None
    settle_lag = (
        settle_msg_end - settle_push_end
        if settle_msg_end is not None and settle_push_end is not None
        else None
    )

    end_summary = summary_rows[-1] if summary_rows else {}
    ask_end = parse_num(end_summary.get("ask_count"))
    bid_end = parse_num(end_summary.get("bid_count"))

    quote_pending_vals = numeric_path_values(status_rows, ("publish_backlog", "quote_pending"))
    settle_pending_vals = numeric_path_values(status_rows, ("publish_backlog", "settle_pending"))
    max_group_pending, max_group_pending_id = object_group_peak(
        status_rows, ("publish_backlog", "max_settle_group_pending"), "pending"
    )
    quote_queue_full_delta = None
    quote_queue_full_vals = numeric_path_values(status_rows, ("publish_backlog", "quote_queue_full_count"))
    if len(quote_queue_full_vals) >= 2:
        quote_queue_full_delta = quote_queue_full_vals[-1] - quote_queue_full_vals[0]
    settle_queue_full_total_vals = [
        v for v, _ in list_path_values(status_rows, ("publish_backlog", "settle_worker_queue_full_counts"), "sum")
    ]
    settle_queue_full_total_delta = None
    if len(settle_queue_full_total_vals) >= 2:
        settle_queue_full_total_delta = settle_queue_full_total_vals[-1] - settle_queue_full_total_vals[0]
    quote_lag_vals = numeric_path_values(status_rows, ("publish_lag", "quote_deals"))
    max_settle_lag, max_settle_lag_group_id = object_group_peak(
        status_rows, ("publish_lag", "max_settle_group_lag"), "lag"
    )

    if not ok_only:
        verdict = "HTTP was unavailable for the whole capture, so the CPU trace cannot be correlated with engine state."
    elif (input_delta or 0) <= 0 and (deals_delta or 0) <= 0:
        verdict = "Engine state was mostly idle during the HTTP-ok window; CPU is mostly background/publish/runtime overhead."
    else:
        verdict = (
            f"Engine processed {fmt_num(input_delta)} input messages and created {fmt_num(deals_delta)} deals "
            f"during successful HTTP snapshots."
        )

    rows = [
        ["HTTP state coverage", f"{ok_c:,}/{total:,} ok ({ok_pct:.1f}%)", "Whether `/summary` and `/status` were reachable"],
        ["HTTP-ok window", compact_span(ok_only), "Use this window when comparing with Instruments"],
        ["Input consumed", fmt_num(input_delta), "`input_offset` delta"],
        ["Deals created", fmt_num(deals_delta), "`deals_id` delta"],
        ["Messages emitted", fmt_num(msg_delta), "`message_id` delta"],
        ["Quote publish", f"{fmt_num(quote_delta)} pushed; {lag_label(quote_lag)}", "`pushed_quote_deals_id` vs `deals_id`"],
        ["Settle publish", f"{fmt_num(settle_delta)} max pushed; {lag_label(settle_lag)}", "max queued settle id vs max pushed settle id"],
        [
            "Publish backlog peak",
            (
                f"quote_pending max={fmt_num(max(quote_pending_vals) if quote_pending_vals else None)}, "
                f"settle_pending max={fmt_num(max(settle_pending_vals) if settle_pending_vals else None)}, "
                f"max_group_pending={fmt_num(max_group_pending)} ({fmt_peak_group(max_group_pending_id)})"
            ),
            "`publish_backlog` pressure during HTTP-ok window",
        ],
        [
            "Kafka QueueFull delta",
            f"quote={fmt_num(quote_queue_full_delta)}, settle_workers_total={fmt_num(settle_queue_full_total_delta)}",
            "Local Kafka producer queue-full counters",
        ],
        [
            "Publish lag peak",
            (
                f"quote_deals max={fmt_num(max(quote_lag_vals) if quote_lag_vals else None)}, "
                f"max_settle_group_lag={fmt_num(max_settle_lag)} ({fmt_peak_group(max_settle_lag_group_id)})"
            ),
            "`publish_lag` cursor backlog",
        ],
        ["Final book depth", f"asks={fmt_num(ask_end)}, bids={fmt_num(bid_end)}", "End of HTTP-ok window"],
    ]

    lines = [
        "## At a Glance",
        "",
        f"**Verdict:** {verdict}",
        "",
        md_table(["signal", "value", "meaning"], rows),
        "",
        "### How to read the rest",
        "",
        "- **Status-based charts** use elapsed time from the first successful `/status` response; offer-consumer charts use the offer metric clock.",
        "- **CPU section** is weighted sample time from Instruments, not wall-clock runtime.",
        "- **Raw tables** are kept for audit/debugging and are folded by default.",
        "",
    ]
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
        ["Matchengine config snapshot", dash(arts.get("matchengine_config"))],
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

    ok_c = sum(1 for r in nd_rows if not (r.get("errors") or []))
    err_c = len(nd_rows) - ok_c
    ok_only = [r for r in nd_rows if not (r.get("errors") or [])]
    ok_status_series = [(r.get("status") or {}) for r in ok_only if isinstance(r.get("status"), dict)]
    has_summary_data = any(isinstance(r.get("summary"), dict) and bool(r.get("summary")) for r in nd_rows)
    out_dir_p = Path(meta.get("out_dir", args.output.parent))
    trace_start: datetime | None = None
    trace_rel = prof.get("trace_path")
    if trace_rel:
        trace_candidate = out_dir_p / str(trace_rel)
        if trace_candidate.exists():
            try:
                trace_start = trace_recording_start_utc(trace_candidate)
            except Exception:
                trace_start = None
    raw_dir = args.output.parent / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    charts_dir = args.output.parent / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    raw_files: list[tuple[str, str]] = []

    def write_raw_file(filename: str, title: str, body: str) -> None:
        path = raw_dir / filename
        path.write_text(f"# {title}\n\n{body.rstrip()}\n", encoding="utf-8")
        raw_files.append((title, f"raw/{filename}"))

    write_raw_file(
        "http-poll-outcomes.md",
        "HTTP Poll Outcomes",
        md_table(["timestamp", "errors"], timeline_meta),
    )
    if has_summary_data and sum_tbl_rows:
        write_raw_file(
            "summary-rows.md",
            "Summary Rows",
            md_table(
                ["ts", "ask_cnt", "bid_cnt", "ask_stock", "bid_stock", "ask_money", "bid_money"],
                sum_tbl_rows,
            ),
        )
    if st_tbl_rows:
        write_raw_file(
            "status-cursors.md",
            "Status Cursor Rows",
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
            ),
        )
    offer_raw = offer_consumer_10ms_block(ok_status_series, include_details=True)
    if offer_raw:
        write_raw_file("offer-consumer-10ms.md", "Offer Consumer 10ms Buckets", offer_raw)

    out: list[str] = []
    out.append(f"# Matching engine profile report ({meta.get('run_id', '')})")
    out.append("")
    out.append(quick_read_section(nd_rows, ok_only, ok_c, err_c))
    out.append("<details>")
    out.append("<summary>Run metadata, artifacts, and profiler command</summary>")
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
    if raw_files:
        out.append("## Raw Detail Files")
        out.append("")
        out.append(md_table(["kind", "relative path"], raw_files))
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
    out.append("</details>")
    out.append("")
    if not nd_rows:
        out.append("_No NDJSON rows — HTTP may have been unreachable during the run._")
        out.append("")
    else:
        landmarks = instruments_landmarks_section(ok_only, trace_start)
        if landmarks:
            out.append(landmarks)

        throughput_charts = status_throughput_charts(ok_only, charts_dir)
        if throughput_charts:
            out.append(throughput_charts)

        queue_charts = queue_growth_charts(
            ok_status_series,
            [label for label, r in zip(elapsed_labels_from_rows(ok_only), ok_only) if isinstance(r.get("status"), dict)],
            charts_dir,
        )
        if queue_charts:
            out.append(queue_charts)

        if len(ok_only) >= 2:
            ok_time_labels = elapsed_labels_from_rows(ok_only)
            pq: list[float] = []
            ac: list[float] = []
            bc: list[float] = []
            qcfc: list[float] = []   # quote_channel_full_count  (cumulative)
            qcbn: list[float] = []   # quote_channel_blocked_nanos (cumulative, ms)
            for r in ok_only:
                st = r.get("status") or {}
                sm = r.get("summary") or {}
                pq.append(float(parse_num(st.get("pushed_quote_deals_id")) or 0.0))
                ac.append(float(parse_num(sm.get("ask_count")) or 0.0))
                bc.append(float(parse_num(sm.get("bid_count")) or 0.0))
                pb = st.get("publish_backlog") or {}
                qcfc.append(float(parse_num(pb.get("quote_channel_full_count")) or 0.0))
                qcbn.append(float((parse_num(pb.get("quote_channel_blocked_nanos")) or 0.0) / 1_000_000.0))
            out.append("#### Progress charts — successful HTTP rows only")
            out.append("")
            out.append(
                "_The x-axis is elapsed time from the first successful `/status` response; "
                "the first response may already include queued work that happened before HTTP could reply._"
            )
            out.append("")
            quote_chart = svg_chart_md(
                charts_dir,
                "quote-deals-pushed.svg",
                "Quote deals pushed",
                {"pushed_quote_deals_id": pq},
                ok_time_labels,
                label_every=10,
                y_axis_label="pushed_quote_deals_id",
            )
            if quote_chart:
                out.append(quote_chart)
                out.append("")
            if has_summary_data:
                depth_chart = svg_chart_md(
                    charts_dir,
                    "order-book-depth.svg",
                    "Order book depth",
                    {"ask_count": ac, "bid_count": bc},
                    ok_time_labels,
                    label_every=10,
                    y_axis_label="count",
                )
                if depth_chart:
                    out.append(depth_chart)
                    out.append("")
            if any(v > 0 for v in qcfc) or any(v > 0 for v in qcbn):
                stall_chart = svg_chart_md(
                    charts_dir,
                    "quote-channel-stall.svg",
                    "Main-thread channel stall - quote publish",
                    {"channel_full_count": qcfc, "channel_blocked_ms": qcbn},
                    ok_time_labels,
                    label_every=10,
                    y_axis_label="count / ms",
                )
                if stall_chart:
                    out.append(stall_chart)
                    out.append("")
                # Compute delta across the ok window for a plain-English summary
                stall_ms_delta = qcbn[-1] - qcbn[0] if len(qcbn) >= 2 else None
                stall_count_delta = qcfc[-1] - qcfc[0] if len(qcfc) >= 2 else None
                out.append(
                    f"_During the HTTP-ok window: main thread blocked on quote channel "
                    f"**{fmt_num(stall_count_delta)} times**, "
                    f"total stall **{fmt_num(stall_ms_delta)} ms**._"
                )
                out.append("")

        offer_labels, offer_series = offer_consumer_bucket_series(ok_status_series)
        if offer_labels and offer_series:
            out.append("### Offer Consumer 10ms Curves")
            out.append("")
            out.append(
                "_This chart uses `offer_consumer` runtime buckets, so it starts at the offer metric clock "
                "rather than the first `/status` response._"
            )
            out.append("")
            offer_chart = svg_chart_md(
                charts_dir,
                "offer-consumer-runtime-10ms.svg",
                "Offer consumer runtime buckets",
                offer_series,
                offer_labels,
                label_every=10,
                y_axis_label="messages / ms",
            )
            if offer_chart:
                out.append(offer_chart)
                out.append("")

        backlog_blk = publish_backlog_rollup_block(ok_status_series)
        if backlog_blk:
            out.append(backlog_blk)
        offer_blk = offer_consumer_10ms_block(ok_status_series)
        if offer_blk:
            out.append(offer_blk)

        if raw_files:
            out.append("### Raw detail files")
            out.append("")
            out.append(
                "_Large per-snapshot tables are written separately so this report stays fast to open._"
            )
            out.append("")
            out.append(md_table(["kind", "relative path"], raw_files))
            out.append("")
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
        blk3 = publish_backlog_rollup_block(ok_status_series)
        out.append("## Aggregate statistics")
        out.append("")
        if blk1:
            out.append(blk1)
        if blk2:
            out.append(blk2)
        if blk3:
            out.append(blk3)
        if not blk1 and not blk2 and not blk3:
            out.append("_No numeric fields suitable for min/max/delta._")
            out.append("")
        out.append("")

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
