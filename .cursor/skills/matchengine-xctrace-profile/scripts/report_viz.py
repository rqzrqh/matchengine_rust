"""Shared Mermaid snippets for PROFILE_REPORT.md (pie + xychart-beta)."""

from __future__ import annotations

import json
import math
from pathlib import Path


def sanitize_label(s: str, max_len: int = 44) -> str:
    t = str(s).replace("\n", " ").replace('"', "'").strip()
    if len(t) > max_len:
        t = t[: max_len - 1] + "…"
    return t


def mermaid_pie_chart(
    title: str,
    pairs: list[tuple[str, float]],
    *,
    top_n: int = 8,
    min_slice: float = 0.05,
) -> str:
    """Pie chart; values are typically percents (any positive units work; tail merged to Other)."""
    items = [(sanitize_label(a), float(b)) for a, b in pairs if float(b) > 0]
    if not items:
        return ""
    items.sort(key=lambda kv: -kv[1])
    head, tail = items[:top_n], items[top_n:]
    merged = list(head)
    if tail:
        oth = sum(v for _, v in tail)
        if oth >= min_slice:
            merged.append(("Other", oth))
    lines = ["```mermaid", "pie showData", f"    title {json.dumps(title.replace(chr(34), chr(39)))}"]
    for lab, val in merged:
        lines.append(f'    "{sanitize_label(lab, max_len=40)}" : {round(val, 2)}')
    lines.append("```")
    return "\n".join(lines)


def mermaid_horizontal_bars_pct(title: str, pairs: list[tuple[str, float]], *, max_categories: int = 14) -> str:
    """Bar chart (xychart-beta): category labels on x-axis, heights = percentages."""
    items = [(sanitize_label(a, max_len=36), float(b)) for a, b in pairs if float(b) > 0]
    if not items:
        return ""
    items.sort(key=lambda kv: -kv[1])
    items = items[:max_categories]
    labels = ", ".join(f'"{sanitize_label(kv[0], max_len=32)}"' for kv in items)
    vals = [round(kv[1], 2) for kv in items]
    ymax = max(vals + [1.0])
    y_hi = max(math.ceil(min(ymax * 1.1, ymax + 5.0)), 1)
    y_lbl = "% of scoped CPU"
    parts = [
        "```mermaid",
        "xychart-beta",
        f"    title {json.dumps(title.replace(chr(34), chr(39)))}",
        f"    x-axis [{labels}]",
        f"    y-axis {json.dumps(y_lbl)} 0 --> {y_hi:g}",
        "    bar [" + ", ".join(str(v) for v in vals) + "]",
        "```",
    ]
    return "\n".join(parts)


def uniform_sample_indices(n: int, max_points: int) -> list[int]:
    if n <= 0:
        return []
    if max_points <= 1:
        return [0]
    if n <= max_points:
        return list(range(n))
    return sorted(set(int(round(i * (n - 1) / (max_points - 1))) for i in range(max_points)))


def mermaid_num(value: float) -> str:
    """Format numbers for Mermaid xychart without scientific notation."""
    if not math.isfinite(value):
        return "0"
    if value == 0:
        return "0"
    text = f"{value:.8f}".rstrip("0").rstrip(".")
    return text or "0"


def mermaid_xy_lines(
    title: str,
    series: dict[str, list[float]],
    *,
    max_points: int = 28,
    y_axis_label: str = "value",
) -> str:
    """
    One or more line series on a subsampled index axis (0..n-1).
    All series must have the same length.
    """
    if not series:
        return ""
    lens = {len(v) for v in series.values()}
    if len(lens) != 1:
        return ""
    n = lens.pop()
    if n == 0:
        return ""
    idxs = uniform_sample_indices(n, max_points)
    x_labels = [str(i) for i in idxs]
    ymin = math.inf
    ymax = -math.inf
    sampled_vals: dict[str, list[float]] = {}
    for name, ys in series.items():
        samp = [float(ys[i]) for i in idxs]
        sampled_vals[name] = samp
        for v in samp:
            ymin = min(ymin, v)
            ymax = max(ymax, v)
    if ymin == math.inf or ymax == math.inf:
        return ""
    nonneg = ymin >= 0
    pad = max((ymax - ymin) * 0.08, abs(ymax) * 0.02 + 1e-6)
    if nonneg:
        y_lo = 0.0
        y_hi = ymax + pad
    else:
        y_lo = ymin - pad
        y_hi = ymax + pad
    rows = ["```mermaid", "xychart-beta", f"    title {json.dumps(title.replace(chr(34), chr(39)))}"]
    rows.append(f"    x-axis [{', '.join(x_labels)}]")
    rows.append(
        f"    y-axis {json.dumps(y_axis_label.replace(chr(34), chr(39)))} "
        f"{mermaid_num(y_lo)} --> {mermaid_num(y_hi)}"
    )
    for _, samp in sampled_vals.items():
        rows.append(f"    line [{', '.join(mermaid_num(round(v, 8)) for v in samp)}]")
    rows.append("```")
    return "\n".join(rows)


def mermaid_xy_lines_labeled(
    title: str,
    series: dict[str, list[float]],
    x_labels: list[str],
    *,
    label_every: int = 10,
    max_points_per_chart: int = 500,
    y_axis_label: str = "value",
) -> str:
    """
    One or more line series using every data point. X labels are sparse: one
    visible label every `label_every` points, empty labels elsewhere.
    """
    if not series:
        return ""
    lens = {len(v) for v in series.values()}
    if len(lens) != 1:
        return ""
    n = lens.pop()
    if n == 0 or len(x_labels) != n:
        return ""

    label_every = max(label_every, 1)
    max_points_per_chart = max(max_points_per_chart, 1)

    chunks: list[tuple[int, int]] = []
    for start in range(0, n, max_points_per_chart):
        chunks.append((start, min(start + max_points_per_chart, n)))

    def render_chunk(start: int, end: int, part_idx: int, part_count: int) -> str:
        # Mermaid xychart-beta is picky about empty string labels; use "." as a
        # visually small placeholder for ticks where we do not show the time.
        sparse_labels = [
            json.dumps(sanitize_label(x_labels[i], max_len=18)) if i % label_every == 0 else '"."'
            for i in range(start, end)
        ]

        ymin = math.inf
        ymax = -math.inf
        for ys in series.values():
            for v in ys[start:end]:
                fv = float(v)
                ymin = min(ymin, fv)
                ymax = max(ymax, fv)
        if ymin == math.inf or ymax == math.inf:
            return ""
        nonneg = ymin >= 0
        pad = max((ymax - ymin) * 0.08, abs(ymax) * 0.02 + 1e-6)
        if nonneg:
            y_lo = 0.0
            y_hi = ymax + pad
        else:
            y_lo = ymin - pad
            y_hi = ymax + pad

        chunk_title = title
        if part_count > 1:
            chunk_title = f"{title} ({part_idx}/{part_count})"
        rows = ["```mermaid", "xychart-beta", f"    title {json.dumps(chunk_title.replace(chr(34), chr(39)))}"]
        rows.append(f"    x-axis [{', '.join(sparse_labels)}]")
        rows.append(
            f"    y-axis {json.dumps(y_axis_label.replace(chr(34), chr(39)))} "
            f"{mermaid_num(y_lo)} --> {mermaid_num(y_hi)}"
        )
        for _, ys in series.items():
            rows.append(
                f"    line [{', '.join(mermaid_num(round(float(v), 8)) for v in ys[start:end])}]"
            )
        rows.append("```")
        return "\n".join(rows)

    rendered = [
        render_chunk(start, end, idx + 1, len(chunks))
        for idx, (start, end) in enumerate(chunks)
    ]
    return "\n\n".join(chunk for chunk in rendered if chunk)


def snapshot_health_strip(ok_count: int, err_count: int, width: int = 40) -> str:
    """ASCII proportion bar inside a fenced block (works where Mermaid is unavailable)."""
    total = ok_count + err_count
    if total <= 0:
        return ""
    ok_w = int(round(width * ok_count / total))
    bar = "#" * ok_w + "-" * (width - ok_w)
    pct = 100.0 * ok_count / total
    return f"```\nHTTP polls: ok {ok_count} / {total} ({pct:.1f}%)  |{bar}|\n```"


def _svg_escape(s: object) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def write_svg_line_chart(
    output_path: Path,
    title: str,
    series: dict[str, list[float]],
    x_labels: list[str],
    *,
    label_every: int = 10,
    max_x_ticks: int = 12,
    y_axis_label: str = "value",
    width: int = 1200,
    height: int = 620,
    reference_lines: dict[str, float] | None = None,
) -> bool:
    """Write a self-contained SVG line chart using every data point."""
    if not series:
        return False
    lens = {len(v) for v in series.values()}
    if len(lens) != 1:
        return False
    n = lens.pop()
    if n == 0:
        return False
    if len(x_labels) != n:
        x_labels = [str(i) for i in range(n)]

    refs = {
        str(name): float(value)
        for name, value in (reference_lines or {}).items()
        if math.isfinite(float(value))
    }
    data_vals = [float(v) for values in series.values() for v in values if math.isfinite(float(v))]
    if not data_vals:
        return False
    data_ymax = max(data_vals)
    visible_ref_threshold = max(data_ymax * 1.5, data_ymax + 1.0)
    visible_refs = {name: value for name, value in refs.items() if value <= visible_ref_threshold}
    hidden_refs = {name: value for name, value in refs.items() if value > visible_ref_threshold}

    all_vals = data_vals + list(visible_refs.values())
    ymin = min(all_vals)
    ymax = max(all_vals)
    if ymin >= 0:
        y0 = 0.0
        y1 = ymax
    else:
        y0 = ymin
        y1 = ymax
    if abs(y1 - y0) < 1e-12:
        y1 = y0 + 1.0
    pad = (y1 - y0) * 0.08
    y0 = 0.0 if y0 >= 0 else y0 - pad
    y1 = y1 + pad

    margin_left = 78
    margin_right = 24
    margin_top = 58
    margin_bottom = 150
    plot_w = max(width - margin_left - margin_right, 1)
    plot_h = max(height - margin_top - margin_bottom, 1)

    def x_pos(i: int) -> float:
        if n <= 1:
            return margin_left
        return margin_left + plot_w * i / (n - 1)

    def y_pos(v: float) -> float:
        return margin_top + plot_h * (1.0 - (v - y0) / (y1 - y0))

    colors = [
        "#2563eb",
        "#dc2626",
        "#16a34a",
        "#9333ea",
        "#ea580c",
        "#0891b2",
        "#4f46e5",
        "#be123c",
    ]

    max_x_ticks = max(max_x_ticks, 2)
    if n <= max_x_ticks:
        x_tick_indices = list(range(n))
    else:
        x_tick_indices = sorted(
            set(int(round(i * (n - 1) / (max_x_ticks - 1))) for i in range(max_x_ticks))
        )

    y_ticks = 5
    y_tick_vals = [y0 + (y1 - y0) * i / y_ticks for i in range(y_ticks + 1)]

    def fmt_num(v: float) -> str:
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        if abs(v - round(v)) < 1e-6:
            return str(int(round(v)))
        return f"{v:.2f}".rstrip("0").rstrip(".")

    lines: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<style>",
        "text{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:12px;fill:#334155}",
        ".title{font-size:18px;font-weight:700;fill:#0f172a}.axis{stroke:#64748b;stroke-width:1}",
        ".grid{stroke:#e2e8f0;stroke-width:1}.legend{font-size:12px}.line{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}.limit{fill:#475569;font-size:11px}",
        "</style>",
        f'<rect width="{width}" height="{height}" fill="#fff"/>',
        f'<text class="title" x="{margin_left}" y="30">{_svg_escape(title)}</text>',
        f'<text x="{margin_left}" y="48">{_svg_escape(y_axis_label)}</text>',
    ]
    if hidden_refs:
        note = "Reference values above chart: " + ", ".join(
            f"{name}={fmt_num(value)}" for name, value in hidden_refs.items()
        )
        lines.append(f'<text class="limit" x="{margin_left + 150}" y="48">{_svg_escape(note)}</text>')

    for tick in y_tick_vals:
        y = y_pos(tick)
        lines.append(f'<line class="grid" x1="{margin_left}" y1="{y:.2f}" x2="{width - margin_right}" y2="{y:.2f}"/>')
        lines.append(f'<text x="{margin_left - 8}" y="{y + 4:.2f}" text-anchor="end">{_svg_escape(fmt_num(tick))}</text>')

    for i, (name, value) in enumerate(visible_refs.items()):
        color = colors[(i + len(series)) % len(colors)]
        y = y_pos(value)
        label = f"{name}={fmt_num(value)}"
        lines.append(
            f'<line x1="{margin_left}" y1="{y:.2f}" x2="{width - margin_right}" y2="{y:.2f}" '
            f'stroke="{color}" stroke-width="1.5" stroke-dasharray="6 4"/>'
        )
        lines.append(
            f'<text class="limit" x="{margin_left - 8}" y="{y - 4:.2f}" text-anchor="end">{_svg_escape(fmt_num(value))}</text>'
        )
        lines.append(
            f'<text class="limit" x="{width - margin_right - 4}" y="{y - 4:.2f}" text-anchor="end">{_svg_escape(label)}</text>'
        )

    lines.append(f'<line class="axis" x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}"/>')
    lines.append(f'<line class="axis" x1="{margin_left}" y1="{margin_top + plot_h}" x2="{width - margin_right}" y2="{margin_top + plot_h}"/>')

    for idx in x_tick_indices:
        x = x_pos(idx)
        label = x_labels[idx] if idx < len(x_labels) else str(idx)
        lines.append(f'<line class="grid" x1="{x:.2f}" y1="{margin_top}" x2="{x:.2f}" y2="{margin_top + plot_h}"/>')
        tick_label_y = margin_top + plot_h + 36
        lines.append(
            f'<text x="{x:.2f}" y="{tick_label_y:.2f}" text-anchor="end" transform="rotate(-30 {x:.2f} {tick_label_y:.2f})">{_svg_escape(label)}</text>'
        )

    legend_x = margin_left
    legend_y = height - 42
    for i, (name, values) in enumerate(series.items()):
        color = colors[i % len(colors)]
        lx = legend_x + (i % 4) * 230
        ly = legend_y + (i // 4) * 18
        lines.append(f'<line x1="{lx}" y1="{ly - 4}" x2="{lx + 24}" y2="{ly - 4}" stroke="{color}" stroke-width="3"/>')
        lines.append(f'<text class="legend" x="{lx + 30}" y="{ly}">{_svg_escape(name)}</text>')

        points = " ".join(
            f"{x_pos(idx):.2f},{y_pos(float(value)):.2f}" for idx, value in enumerate(values)
        )
        lines.append(f'<polyline class="line" stroke="{color}" points="{points}"/>')

    lines.append("</svg>")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return True
