"""Shared Mermaid snippets for PROFILE_REPORT.md (pie + xychart-beta)."""

from __future__ import annotations

import json
import math


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
    rows.append(f"    y-axis {json.dumps(y_axis_label.replace(chr(34), chr(39)))} {y_lo:g} --> {y_hi:g}")
    for _, samp in sampled_vals.items():
        rows.append(f"    line [{', '.join(f'{round(v, 8):g}' for v in samp)}]")
    rows.append("```")
    return "\n".join(rows)


def snapshot_health_strip(ok_count: int, err_count: int, width: int = 40) -> str:
    """ASCII proportion bar inside a fenced block (works where Mermaid is unavailable)."""
    total = ok_count + err_count
    if total <= 0:
        return ""
    ok_w = int(round(width * ok_count / total))
    bar = "#" * ok_w + "-" * (width - ok_w)
    pct = 100.0 * ok_count / total
    return f"```\nHTTP polls: ok {ok_count} / {total} ({pct:.1f}%)  |{bar}|\n```"
