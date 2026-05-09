#!/usr/bin/env python3
"""Poll matcher HTTP summary + status; write NDJSON and optional human-readable log."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_json(url: str, timeout: float) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def wait_for_ready(summary_url: str, timeout: float, poll: float, req_timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            fetch_json(summary_url, timeout=min(req_timeout, deadline - time.monotonic()))
            return
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError, OSError):
            time.sleep(poll)
    print("engine_state_poll: HTTP not ready within wait timeout", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(description="Poll matcher /summary and /status endpoints.")
    ap.add_argument("--base-url", default="http://127.0.0.1:8080", help="Engine HTTP base URL")
    ap.add_argument("--market", required=True)
    ap.add_argument("--interval", type=float, default=1.0, help="Seconds between snapshots")
    ap.add_argument("--duration-secs", type=float, required=True, help="Total poll window after HTTP is ready")
    ap.add_argument("--http-timeout", type=float, default=3.0)
    ap.add_argument("--wait-ready-secs", type=float, default=90.0)
    ap.add_argument("--out-ndjson", required=True)
    ap.add_argument("--out-human-log", default="", help="Optional append-only debug log")
    args = ap.parse_args()

    base = args.base_url.rstrip("/")
    summary_url = f"{base}/markets/{args.market}/summary"
    status_url = f"{base}/markets/{args.market}/status"

    wait_for_ready(summary_url, timeout=args.wait_ready_secs, poll=0.1, req_timeout=args.http_timeout)

    end = time.monotonic() + args.duration_secs

    with open(args.out_ndjson, "w", encoding="utf-8") as nd:
        hl_cm = None
        if args.out_human_log:
            hl_cm = open(args.out_human_log, "w", encoding="utf-8")
            hl_cm.write(f"# polls -> {summary_url} | {status_url}\n")
            hl_cm.write(f"# interval={args.interval}s\n")

        try:
            while time.monotonic() < end:
                ts = utc_now_iso()
                row: dict = {"ts": ts, "summary": {}, "status": {}, "errors": []}
                try:
                    row["summary"] = fetch_json(summary_url, args.http_timeout)
                except Exception as e:
                    row["errors"].append(f"summary:{e}")
                try:
                    row["status"] = fetch_json(status_url, args.http_timeout)
                except Exception as e:
                    row["errors"].append(f"status:{e}")

                nd.write(json.dumps(row, separators=(",", ":")) + "\n")
                nd.flush()

                if hl_cm:
                    hl_cm.write(f"\n===== {ts} =====\n-- summary --\n")
                    hl_cm.write(json.dumps(row["summary"], indent=2) if row["summary"] else "{}\n")
                    hl_cm.write("\n-- status --\n")
                    hl_cm.write(json.dumps(row["status"], indent=2) if row["status"] else "{}\n")
                    if row["errors"]:
                        hl_cm.write("\n-- errors --\n")
                        hl_cm.write("\n".join(row["errors"]) + "\n")

                time.sleep(args.interval)
        finally:
            if hl_cm:
                hl_cm.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
