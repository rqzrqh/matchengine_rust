#!/usr/bin/env python3
"""Parse xctrace `time-profile` XML export; correlate with HTTP NDJSON for bottleneck notes."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from report_viz import mermaid_horizontal_bars_pct, mermaid_pie_chart


@dataclass
class Sample:
    weight_ns: int
    offset_ns: int
    thread_fmt: str
    stack_fmt: str
    has_engine_binary: bool
    top_engine_symbol: str


def _index_ids(root: ET.Element) -> dict[str, ET.Element]:
    m: dict[str, ET.Element] = {}
    for el in root.iter():
        i = el.get("id")
        if i:
            m[i] = el
    return m


def _deref(el: ET.Element | None, id_map: dict[str, ET.Element]) -> ET.Element | None:
    if el is None:
        return None
    ref = el.get("ref")
    if ref:
        return id_map.get(ref)
    return el


def _weight_ns(w_el: ET.Element | None, id_map: dict[str, ET.Element]) -> int:
    w = _deref(w_el, id_map)
    if w is None or w.text is None:
        return 0
    t = w.text.strip()
    return int(t) if t.isdigit() else 0


def _thread_fmt(t_el: ET.Element | None, id_map: dict[str, ET.Element]) -> str:
    t = _deref(t_el, id_map)
    if t is None:
        return ""
    return (t.get("fmt") or "").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")


def _backtrace_has_engine(tb: ET.Element | None, id_map: dict[str, ET.Element]) -> tuple[bool, str]:
    """True if any frame uses matchengine_rust binary; return first such frame name (demangled-ish)."""
    tb = _deref(tb, id_map)
    if tb is None:
        return False, ""
    bt = tb.find("backtrace")
    bt = _deref(bt, id_map)
    if bt is None:
        return False, ""
    for fr in bt.findall("frame"):
        fr = _deref(fr, id_map)
        if fr is None:
            continue
        b = fr.find("binary")
        b = _deref(b, id_map)
        path = (b.get("path") or "") if b is not None else ""
        if "matchengine_rust" not in path:
            continue
        name = fr.get("name") or ""
        name = (
            name.replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&#10;", " ")
        )
        return True, name
    return False, ""


def _stack_fmt(tb: ET.Element | None, id_map: dict[str, ET.Element]) -> str:
    tb = _deref(tb, id_map)
    if tb is None:
        return ""
    s = tb.get("fmt") or ""
    return s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")


def export_trace_toc_to_path(trace_path: Path, out_xml: Path) -> None:
    subprocess.run(
        [
            "xcrun",
            "xctrace",
            "export",
            "--input",
            str(trace_path),
            "--toc",
            "--output",
            str(out_xml),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def trace_recording_start_utc(trace_path: Path) -> datetime | None:
    fd, tmp = tempfile.mkstemp(suffix="-trace-toc.xml")
    os.close(fd)
    p = Path(tmp)
    try:
        export_trace_toc_to_path(trace_path, p)
        tree = ET.parse(p)
        el = tree.find(".//start-date")
        if el is None or not (el.text and el.text.strip()):
            return None
        raw = el.text.strip()
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (subprocess.CalledProcessError, ET.ParseError, OSError, ValueError):
        return None
    finally:
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass


def export_time_profile_to_path(trace_path: Path, out_xml: Path) -> None:
    xp = '/trace-toc/run[@number="1"]//table[@schema="time-profile"]'
    subprocess.run(
        [
            "xcrun",
            "xctrace",
            "export",
            "--input",
            str(trace_path),
            "--xpath",
            xp,
            "--output",
            str(out_xml),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def _sample_offset_ns(row: ET.Element, id_map: dict[str, ET.Element]) -> int:
    st_el = row.find("sample-time")
    st_el = _deref(st_el, id_map)
    if st_el is None or not st_el.text:
        return -1
    t = st_el.text.strip()
    return int(t) if t.isdigit() else -1


def parse_samples(xml_path: Path) -> list[Sample]:
    tree = ET.parse(xml_path)
    root = tree.getroot()
    id_map = _index_ids(root)
    samples: list[Sample] = []
    for row in root.iter("row"):
        w_el = row.find("weight")
        t_el = row.find("thread")
        tb_el = row.find("tagged-backtrace")
        if tb_el is None:
            continue
        wn = _weight_ns(w_el, id_map)
        if wn <= 0:
            continue
        off_ns = _sample_offset_ns(row, id_map)
        eng, sym = _backtrace_has_engine(tb_el, id_map)
        samples.append(
            Sample(
                weight_ns=wn,
                offset_ns=off_ns,
                thread_fmt=_thread_fmt(t_el, id_map),
                stack_fmt=_stack_fmt(tb_el, id_map),
                has_engine_binary=eng,
                top_engine_symbol=sym,
            )
        )
    return samples


def _thread_bucket(tf: str) -> str:
    tl = tf.lower()
    if "main thread" in tl:
        return "Main thread (matcher loop)"
    if "kafka-consumer" in tl:
        return "kafka-consumer (ingress)"
    if "quote-publish" in tl:
        return "quote-publish (Kafka out)"
    if "settle-publish" in tl:
        return "settle-publish (Kafka out)"
    if "http-worker" in tl or "http-driver" in tl:
        return "HTTP (http-driver / http-worker)"
    if "producer polling" in tl:
        return "librdkafka producer polling"
    if "rdk:" in tl or "rdkafka" in tl:
        return "librdkafka broker threads"
    if "tokio" in tl or "runtime-worker" in tl:
        return "Tokio / unnamed worker"
    return "Other / helper threads"


_RE_SYM = re.compile(
    r"(handle_mq_message|market_put_(?:limit|market)_order|on_order_put|"
    r"publish_(?:put_order|deal|quote_deal)|Order::to_json|json::|"
    r"skiplist|mysql|rd_kafka|FutureProducer|collect_publish_batch|"
    r"Receiver::recv|mpmc::.*recv)"
)


def _symbol_bucket(sym: str, stack_fmt: str) -> str:
    hay = f"{sym} {stack_fmt}"
    m = _RE_SYM.search(hay)
    if m:
        key = m.group(1)
        if key.startswith("handle_mq"):
            return "Kafka → main: MQ dispatch"
        if "market_put" in key or "on_order_put" in key:
            return "Matching / order handlers"
        if key.startswith("publish_"):
            return "Publish payloads (Kafka out)"
        if "to_json" in key or key.startswith("json::"):
            return "JSON / serialization"
        if "skiplist" in key:
            return "Order book (skiplist)"
        if "mysql" in key:
            return "MySQL client"
        if "rd_kafka" in key or "FutureProducer" in key:
            return "Kafka producer / rdkafka"
        if "recv" in key:
            return "Channel recv / parked"
    if "mach_vm" in stack_fmt or "malloc" in stack_fmt.lower():
        return "Allocation / libc"
    return "Other Rust / system"


def aggregate_engine_samples(samples: list[Sample]) -> tuple[list[Sample], int]:
    eng = [s for s in samples if s.has_engine_binary]
    total = sum(s.weight_ns for s in eng)
    return eng, total


def state_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Derive deltas from NDJSON rows (whole series; caller may filter by errors)."""
    ok_status: list[dict[str, Any]] = []
    for r in rows:
        if r.get("errors"):
            continue
        st = r.get("status")
        if isinstance(st, dict) and st:
            ok_status.append(st)
    out: dict[str, Any] = {"ok_snapshots": len(ok_status), "signals": {}}
    if len(ok_status) < 2:
        return out

    def gv(d: dict[str, Any], k: str) -> float | None:
        v = d.get(k)
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    first, last = ok_status[0], ok_status[-1]
    dq = gv(last, "deals_id"), gv(first, "deals_id")
    pq = gv(last, "pushed_quote_deals_id"), gv(first, "pushed_quote_deals_id")
    if all(x is not None for x in (*dq, *pq)):
        out["signals"]["delta_deals_id"] = dq[0] - dq[1]
        out["signals"]["delta_pushed_quote"] = pq[0] - pq[1]
        out["signals"]["quote_publish_lag_end"] = (dq[0] or 0) - (pq[0] or 0)

    io0 = gv(first, "input_offset")
    io1 = gv(last, "input_offset")
    is0 = gv(first, "input_sequence_id")
    is1 = gv(last, "input_sequence_id")
    if io0 is not None and io1 is not None:
        out["signals"]["delta_input_offset"] = io1 - io0
    if is0 is not None and is1 is not None:
        out["signals"]["delta_input_sequence"] = is1 - is0
    if io1 is not None and is1 is not None:
        out["signals"]["input_queue_lag_hint"] = is1 - io1

    def numeric_list_max(obj: Any) -> float | None:
        if not isinstance(obj, list) or not obj:
            return None
        vals = []
        for x in obj:
            try:
                vals.append(float(x))
            except (TypeError, ValueError):
                pass
        return max(vals) if vals else None

    sm0 = numeric_list_max(first.get("settle_message_ids"))
    sm1 = numeric_list_max(last.get("settle_message_ids"))
    ps0 = numeric_list_max(first.get("pushed_settle_message_ids"))
    ps1 = numeric_list_max(last.get("pushed_settle_message_ids"))
    if sm0 is not None and sm1 is not None:
        out["signals"]["delta_settle_message_max"] = sm1 - sm0
    if ps0 is not None and ps1 is not None:
        out["signals"]["delta_pushed_settle_max"] = ps1 - ps0
    if sm1 is not None and ps1 is not None:
        out["signals"]["settle_publish_lag_end"] = sm1 - ps1

    return out


def parse_ts_iso_utc(ts_raw: object) -> datetime | None:
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


def ok_http_snapshot_wall_window(rows: list[dict[str, Any]]) -> tuple[datetime, datetime] | None:
    times: list[datetime] = []
    for r in rows:
        if r.get("errors"):
            continue
        st = r.get("status")
        if not isinstance(st, dict) or not st:
            continue
        dt = parse_ts_iso_utc(r.get("ts"))
        if dt is not None:
            times.append(dt)
    if len(times) < 2:
        return None
    return min(times), max(times)


def filter_engine_samples_wall_window(
    eng: list[Sample],
    trace_start_utc: datetime,
    wall_lo: datetime,
    wall_hi: datetime,
) -> list[Sample]:
    selected: list[Sample] = []
    for s in eng:
        if s.offset_ns < 0:
            continue
        wall = trace_start_utc + timedelta(seconds=s.offset_ns / 1e9)
        if wall_lo <= wall <= wall_hi:
            selected.append(s)
    return selected


def rollup_engine_cpu(eng: list[Sample]) -> tuple[defaultdict[str, int], defaultdict[str, int], defaultdict[str, int], int]:
    thr_weight: defaultdict[str, int] = defaultdict(int)
    bucket_weight: defaultdict[str, int] = defaultdict(int)
    sym_weight: defaultdict[str, int] = defaultdict(int)
    total = sum(s.weight_ns for s in eng)
    for s in eng:
        thr_weight[_thread_bucket(s.thread_fmt)] += s.weight_ns
        bucket_weight[_symbol_bucket(s.top_engine_symbol, s.stack_fmt)] += s.weight_ns
        if s.top_engine_symbol:
            tail = s.top_engine_symbol.split("::")[-1][:56]
            sym_weight[tail] += s.weight_ns
    return thr_weight, bucket_weight, sym_weight, total


def md_escape_cell(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", " ")


def md_table(headers: list[str], rows: list[list[object]]) -> str:
    lines_tbl = [
        "| " + " | ".join(md_escape_cell(str(h)) for h in headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines_tbl.append("| " + " | ".join(md_escape_cell(str(c)) for c in row) + " |")
    return "\n".join(lines_tbl)


def build_bottleneck_section(
    trace_path: Path,
    nd_rows: list[dict[str, Any]],
    *,
    keep_xml: Path | None = None,
) -> str:
    if keep_xml is not None:
        xml_out = keep_xml
    else:
        fd, tmp = tempfile.mkstemp(suffix="-time-profile.xml")
        os.close(fd)
        xml_out = Path(tmp)

    try:
        export_time_profile_to_path(trace_path, xml_out)
    except FileNotFoundError as e:
        return (
            "## Bottleneck synthesis (CPU + HTTP)\n\n"
            f"_Could not run `xcrun xctrace` ({e}). Install Xcode Command Line Tools on macOS._\n"
        )
    except subprocess.CalledProcessError as e:
        err = (e.stderr or e.stdout or str(e)).strip()
        return (
            "## Bottleneck synthesis (CPU + HTTP)\n\n"
            f"_`xctrace export` failed for `{trace_path.name}`: `{err[:400]}`._\n"
        )

    samples = parse_samples(xml_out)
    eng, total_eng = aggregate_engine_samples(samples)
    if total_eng <= 0:
        msg = [
            "## Bottleneck synthesis (CPU + HTTP)",
            "",
            f"_Exported **time-profile** from `{trace_path.name}` but found **no samples** "
            "with `matchengine_rust` frames (startup-only capture or export filter)._",
            "",
        ]
        if keep_xml is None:
            try:
                xml_out.unlink(missing_ok=True)
            except OSError:
                pass
        return "\n".join(msg)

    trace_start = trace_recording_start_utc(trace_path)
    wall_win = ok_http_snapshot_wall_window(nd_rows)
    if trace_start is not None and wall_win is not None:
        wl_eff = max(wall_win[0], trace_start)
        eng_win = filter_engine_samples_wall_window(eng, trace_start, wl_eff, wall_win[1])
    else:
        eng_win = []
    tot_win = sum(s.weight_ns for s in eng_win)
    frac_win = tot_win / total_eng if total_eng else 0.0
    use_filtered = bool(
        trace_start is not None
        and wall_win is not None
        and tot_win > 0
        and frac_win >= 0.10
        and len(eng_win) >= 15
    )
    eng_view = eng_win if use_filtered else eng
    thr_weight, bucket_weight, sym_weight, view_total = rollup_engine_cpu(eng_view)
    view_label = (
        "**CPU tables scope**: snapshots whose profiler timestamp falls between the first "
        "and last **HTTP-successful `/status`** polls (UTC), aligned via the trace TOC `start-date`."
        if use_filtered
        else "**CPU tables scope**: all engine-related samples (window filter inactive — "
        "`start-date`/HTTP alignment missing, or retained weight would be "
        "<10% / fewer than 15 samples)."
    )
    pct_view = lambda ns: (100.0 * ns / view_total) if view_total > 0 else 0.0

    top_syms = sorted(sym_weight.items(), key=lambda kv: -kv[1])[:14]
    thr_rows = sorted(thr_weight.items(), key=lambda kv: -kv[1])[:12]
    bkt_rows = sorted(bucket_weight.items(), key=lambda kv: -kv[1])[:12]

    st = state_metrics(nd_rows)

    iso_lo = wall_win[0].strftime("%Y-%m-%dT%H:%M:%SZ") if wall_win else ""
    iso_hi = wall_win[1].strftime("%Y-%m-%dT%H:%M:%SZ") if wall_win else ""

    lines: list[str] = [
        "## Bottleneck synthesis (CPU + HTTP)",
        "",
        "_Heuristic summary from `xctrace export` (**time-profile** samples containing the "
        "`matchengine_rust` binary) correlated with `/summary` and `/status` deltas. "
        "Use Instruments for drill-down; see `doc/thread-model.md` for thread roles._",
        "",
        f"- **Trace file**: `{trace_path.name}`",
        f"- **All engine-related samples**: {total_eng / 1e9:.3f}s total weight ({len(eng)} samples)",
        "",
        "### Profiler ↔ HTTP alignment",
        "",
        view_label,
        "",
    ]

    scope_rows = [
        ["All engine samples", f"{total_eng / 1e9:.4f}s", "100%" if total_eng else "—"],
    ]
    if trace_start and wall_win:
        scope_rows.append(
            [
                f"HTTP-ok window ({iso_lo} … {iso_hi})",
                f"{tot_win / 1e9:.4f}s",
                f"{100.0 * frac_win:.1f}% of engine weight" if total_eng else "—",
            ]
        )
    else:
        scope_rows.append(
            [
                "HTTP-ok window",
                "—",
                "_Need trace TOC `start-date` + ≥2 successful status polls._",
            ]
        )
    lines.append(md_table(["CPU scope", "sample weight", "notes"], scope_rows))
    lines.append("")
    lines.append("```mermaid")
    lines.append("flowchart LR")
    lines.append('  TS["Instruments trace TOC start-date"] --> CPU["scoped engine samples"]')
    lines.append('  HTTP["HTTP /status ok snapshots"] --> CPU')
    lines.append("```")
    lines.append("")

    lines.append("### CPU — time in thread buckets")
    lines.append("")
    thr_pie = mermaid_pie_chart(
        "Scoped engine CPU by thread bucket (approx %)",
        [(n, pct_view(w)) for n, w in thr_rows],
        top_n=8,
    )
    if thr_pie:
        lines.append(thr_pie)
        lines.append("")
    tb = [f"| {md_escape_cell(name)} | {pct_view(w):.1f}% |" for name, w in thr_rows]
    lines.extend(
        [
            "| thread bucket | approx % of scoped engine CPU |",
            "| --- | ---: |",
            *tb,
            "",
        ]
    )

    lines.extend(
        [
            "### CPU — work-type buckets (from top frames)",
            "",
        ]
    )
    bkt_pie = mermaid_pie_chart(
        "Scoped engine CPU by work-area heuristic (approx %)",
        [(n, pct_view(w)) for n, w in bkt_rows],
        top_n=8,
    )
    if bkt_pie:
        lines.append(bkt_pie)
        lines.append("")
    tb = [f"| {md_escape_cell(n)} | {pct_view(w):.1f}% |" for n, w in bkt_rows]
    lines.extend(["| work area (heuristic) | approx % |", "| --- | ---: |", *tb, ""])

    lines.extend(["### CPU — hottest symbols (suffix of top engine frame)", ""])
    sym_bar = mermaid_horizontal_bars_pct(
        "Top symbol tails (approx % of scoped CPU)",
        [(n, pct_view(w)) for n, w in top_syms],
        max_categories=12,
    )
    if sym_bar:
        lines.append(sym_bar)
        lines.append("")
    tb = [f"| `{md_escape_cell(n)}` | {pct_view(w):.1f}% |" for n, w in top_syms]
    lines.extend(["| symbol tail | approx % |", "| --- | ---: |", *tb, ""])

    signals = st.get("signals") or {}
    if signals:
        lines.append("### HTTP state signals (ok snapshots only)")
        lines.append("")
        for k, v in sorted(signals.items()):
            lines.append(f"- **{k}**: `{v}`")
        lines.append("")

    # Narrative hints (rules, not ground truth) — denominators match scoped CPU tables
    hints: list[str] = []
    ve = float(view_total) if view_total else 1.0

    mq_pct = bucket_weight.get("Kafka → main: MQ dispatch", 0) / ve
    pub_pct = bucket_weight.get("Publish payloads (Kafka out)", 0) / ve
    match_pct = bucket_weight.get("Matching / order handlers", 0) / ve
    json_pct = bucket_weight.get("JSON / serialization", 0) / ve

    lag = signals.get("quote_publish_lag_end")
    try:
        lag_f = float(lag)
    except (TypeError, ValueError):
        lag_f = 0.0

    lag_settle = signals.get("settle_publish_lag_end")
    try:
        lag_s = float(lag_settle)
    except (TypeError, ValueError):
        lag_s = 0.0

    if lag_f > 500 and pub_pct > 0.15:
        hints.append(
            f"**/status** shows **quote publish lag** (~{lag_f:.0f} deals_id ahead of pushed_quote) "
            f"while **publish / JSON stacks** account for a large share (~{(pub_pct + json_pct) * 100:.0f}% combined). "
            "Investigate quote publish batching, payload size, and Kafka producer tuning."
        )
    elif lag_f > 500:
        hints.append(
            f"**/status** shows **quote publish lag** (~{lag_f:.0f}) but CPU is not dominated by publish symbols here — "
            "check broker/network back-pressure or async publish waits (still correlate in Instruments timeline)."
        )

    if lag_s > 200 and (pub_pct + json_pct) > 0.10:
        hints.append(
            f"**Settle backlog** (`settle_publish_lag_end`≈{lag_s:.0f} max settle id − pushed) persists while "
            f"**/status** advances — combined **publish/JSON-ish** CPU≈{(pub_pct + json_pct) * 100:.0f}%. "
            "Cross-check **`spawn_settle_publish_thread`** stacks vs quote path in Instruments."
        )
    elif lag_s > 200:
        hints.append(
            f"**Settle backlog** (~{lag_s:.0f} on max message-id proxy) visible in `/status` — "
            "CPU may be waiting on broker/IO rather than serde; compare with kafka thread activity."
        )

    try:
        ddp = float(signals.get("delta_pushed_quote", 0) or 0)
        ddd = float(signals.get("delta_deals_id", 0) or 0)
    except (TypeError, ValueError):
        ddp, ddd = 0.0, 0.0
    if ddp > max(ddd, 0.0) + 1e3 and json_pct > 0.06:
        hints.append(
            f"**Quote publish catch-up** window: `delta_pushed_quote` ({ddp:.0f}) ≫ `delta_deals_id` ({ddd:.0f}) "
            f"with **JSON/serialization**~{json_pct * 100:.0f}% of scoped CPU — profile `publish_quote_deal` / `Order::to_json` costs."
        )

    if match_pct > 0.25 and mq_pct > 0.15:
        hints.append(
            "**Main thread** spends substantial time in **matching + MQ dispatch** — "
            "CPU may be event-loop bound; profile individual `market_put_*` paths and book structure."
        )

    if thr_weight.get("kafka-consumer (ingress)", 0) / ve > 0.12:
        hints.append(
            "**kafka-consumer** thread shows non-trivial CPU — verify consumer fetch size, "
            "deserialization, and whether ingress outpaces the matcher."
        )

    kon = thr_weight.get("librdkafka broker threads", 0) / ve
    if kon > 0.2:
        hints.append(
            "**librdkafka broker threads** are prominent — often I/O and protocol; "
            "correlate with network and broker load, not only matcher logic."
        )

    if not hints:
        hints.append(
            "No strong automatic verdict — use the tables above with the Instruments timeline "
            "and compare periods where `/status` lag grows vs hot stacks."
        )

    lines.append("### Interpretation hints (automated, verify in Instruments)")
    lines.append("")
    for h in hints:
        lines.append(f"- {h}")
    lines.append("")

    if keep_xml is None:
        try:
            xml_out.unlink(missing_ok=True)
        except OSError:
            pass

    return "\n".join(lines)
