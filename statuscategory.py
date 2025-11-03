#!/usr/bin/env python3
"""
Append Jira Status Category (and optional Status Name) to a CSV of issue keys,
writing each output row IMMEDIATELY as it is processed.

- Per-issue endpoint (no pagination surprises):
    GET /rest/api/2/issue/{key}?fields=status

Env (.env) via python-dotenv:
    CLOUD_BASE_URL=https://your-domain.atlassian.net
    CLOUD_EMAIL=you@example.com
    CLOUD_API_TOKEN=<api token>

Usage:
  python add_status_category_stream.py \
    --input input.csv \
    --output output.csv \
    [--column-name "Status Category"] \
    [--include-status-name] \
    [--flush-every 1] \
    [--fsync-every 0] \
    [--no-progress] \
    [--debug]
"""

import argparse
import csv
import os
import sys
import time
from typing import Tuple, Optional

import requests
from dotenv import load_dotenv
from tqdm.auto import tqdm

ISSUE_ENDPOINT = "/rest/api/2/issue/{key}"
RETRY_COUNT = 3
RETRY_BACKOFF = 2.0  # seconds (exponential)

def env_or_die(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        print(f"Missing env var: {name}", file=sys.stderr)
        sys.exit(2)
    return v

def fetch_status_for_key(
    session: requests.Session,
    base_url: str,
    key: str,
    debug: bool = False,
) -> Tuple[str, str]:
    """
    Returns (status_category_name, status_name) for a single issue key.
    On 403/404 or failure after retries, returns ("","").
    """
    if not key:
        return ("", "")

    url = base_url.rstrip("/") + ISSUE_ENDPOINT.format(key=key)
    params = {"fields": "status"}

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = session.get(url, params=params, timeout=30)

            if resp.status_code == 429:  # rate-limited
                retry_after = int(resp.headers.get("Retry-After", "5"))
                if debug:
                    print(f"[429] rate-limited on {key}, sleeping {retry_after}s", file=sys.stderr)
                time.sleep(retry_after)
                continue

            if resp.status_code in (403, 404):
                if debug:
                    print(f"[DEBUG] {key} -> {resp.status_code} (skip)", file=sys.stderr)
                return ("", "")

            resp.raise_for_status()
            data = resp.json()
            fields = data.get("fields") or {}
            createddate = fields.get("created")
            status = fields.get("status") or {}
            cat = (status.get("statusCategory") or {}).get("name") or ""
            name = status.get("name") or ""
            return (cat, name)

        except requests.RequestException as e:
            if attempt == RETRY_COUNT:
                if debug:
                    print(f"[WARN] {key}: {e}", file=sys.stderr)
                return ("", "")
            # exponential-ish backoff
            sleep_s = RETRY_BACKOFF ** attempt
            if debug:
                print(f"[DEBUG] {key}: transient error, retrying in {sleep_s:.1f}s…", file=sys.stderr)
            time.sleep(sleep_s)

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Append Status Category to CSV, writing rows immediately."
    )
    parser.add_argument("--input", required=True, help="Input CSV (first column = issue key)")
    parser.add_argument("--output", required=True, help="Output CSV (written incrementally)")
    parser.add_argument("--column-name", default="Status Category", help="Name for the status-category column")
    parser.add_argument("--include-status-name", action="store_true", help="Also append a 'Status Name' column")
    parser.add_argument("--flush-every", type=int, default=1, help="Flush Python file buffer after this many rows (default 1)")
    parser.add_argument("--fsync-every", type=int, default=0, help="os.fsync() every N rows (0 = never). Safer but slower")
    parser.add_argument("--no-progress", action="store_true", help="Disable tqdm progress bar")
    parser.add_argument("--debug", action="store_true", help="Print debug logs to stderr")
    args = parser.parse_args()

    base_url = env_or_die("CLOUD_BASE_URL")
    email = env_or_die("CLOUD_EMAIL")
    token = env_or_die("CLOUD_API_TOKEN")

    # Read input header first; then we will stream rows
    with open(args.input, "r", newline="", encoding="utf-8-sig") as fin:
        reader = list(csv.reader(fin))
    if not reader or not reader[0]:
        print("Invalid/empty CSV.", file=sys.stderr)
        sys.exit(1)

    header, data_rows = reader[0], reader[1:]

    # Prepare output file for incremental writes (line-buffered)
    # newline="" is important so csv doesn't add extra blank lines on Windows
    # buffering=1 -> line buffered when writing text
    fout = open(args.output, "w", newline="", encoding="utf-8", buffering=1)
    writer = csv.writer(fout)

    # Write header immediately
    out_header = header[:] + [args.column-name] if False else None  # placeholder to keep editor happy
    out_header = header[:] + [args.column_name]
    if args.include_status_name:
        out_header.append("Status Name")
    writer.writerow(out_header)
    fout.flush()
    # (Optionally fsync the header)
    if args.fsync_every and args.fsync_every > 0:
        os.fsync(fout.fileno())

    total = len(data_rows)
    pbar = None
    if not args.no_progress:
        pbar = tqdm(total=total, unit="row", desc="Processing")

    # Use a single session for connection reuse
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    session.auth = (email, token)

    try:
        for idx, row in enumerate(data_rows, start=1):
            row = row or []
            key = (row[0] or "").strip() if row else ""
            cat, name = fetch_status_for_key(session, base_url, key, args.debug)

            out_row = row + [cat]
            if args.include_status_name:
                out_row.append(name)

            # Write the row immediately
            writer.writerow(out_row)

            # Flush frequently so the file on disk stays up-to-date
            if args.flush_every <= 1 or (idx % args.flush_every == 0):
                fout.flush()
                if args.fsync_every and args.fsync_every > 0 and (idx % args.fsync_every == 0):
                    os.fsync(fout.fileno())

            if pbar:
                pbar.update(1)

    finally:
        if pbar:
            pbar.close()
        # final flush & fsync
        try:
            fout.flush()
            if args.fsync_every and args.fsync_every > 0:
                os.fsync(fout.fileno())
        finally:
            fout.close()
        session.close()

    print(f"Done. Wrote: {args.output}")

if __name__ == "__main__":
    main()
