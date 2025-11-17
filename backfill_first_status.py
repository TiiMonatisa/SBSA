#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backfill the FIRST status for Jira issues using the enhanced JQL search (GET /rest/api/3/search/jql).

ENV (.env):
  CLOUD_BASE_URL=https://<your-site>.atlassian.net
  CLOUD_EMAIL=you@company.com
  CLOUD_API_TOKEN=your_api_token
  # Optional to write results:
  JIRA_CUSTOM_FIELD_ID=customfield_12345

Notes:
- Enhanced JQL search paginates with nextPageToken (no startAt/total). We stream pages until there is no token.
- For each issue, we read /rest/api/2/issue/{key}/changelog (Cloud supports this) and infer the first status:
    earliest status change: fromString if present, else toString
    if no status changes exist: use the issue's current status
"""

import os
import sys
import time
import json
import argparse
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests
from requests import Session
from requests.auth import HTTPBasicAuth
from urllib.parse import urlencode, quote

from dotenv import load_dotenv
from tqdm import tqdm

TRANSIENT = (429, 502, 503, 504)


# ------------------------ HTTP client ------------------------

@dataclass
class Client:
    base_url: str
    session: Session
    auth: HTTPBasicAuth
    headers: Dict[str, str]


def required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        print(f"Missing required environment variable: {name}", file=sys.stderr)
        sys.exit(2)
    return v


def build_client(concurrency: int) -> Client:
    load_dotenv()
    base_url = required_env("CLOUD_BASE_URL").rstrip("/")
    email = required_env("CLOUD_EMAIL")
    token = required_env("CLOUD_API_TOKEN")

    sess = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=max(8, concurrency * 2),
        pool_maxsize=max(16, concurrency * 4),
    )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "User-Agent": "first-status-backfill/3.0",
    }
    auth = HTTPBasicAuth(email, token)
    return Client(base_url, sess, auth, headers)


def request_with_backoff(client: Client, method: str, url: str, **kwargs) -> requests.Response:
    max_attempts = 7
    for attempt in range(1, max_attempts + 1):
        r = client.session.request(method, url, auth=client.auth, headers=client.headers, timeout=60, **kwargs)
        if r.status_code not in TRANSIENT:
            return r
        retry_after = r.headers.get("Retry-After")
        sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else min(2 ** min(attempt, 4), 10)
        time.sleep(sleep_s)
    return r


def get_json(client: Client, url: str, **kwargs) -> Dict[str, Any]:
    r = request_with_backoff(client, "GET", url, **kwargs)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text[:500]}")
    return r.json()


def put_json(client: Client, url: str, payload: Dict[str, Any]) -> None:
    r = request_with_backoff(client, "PUT", url, data=json.dumps(payload))
    if r.status_code >= 400:
        raise RuntimeError(f"PUT {url} -> {r.status_code} {r.text[:500]}")


# ------------------------ Jira helpers ------------------------

def stream_issues_enhanced(client: Client, jql: str, fields: List[str], page_size: int = 100):
    """
    Generator over issues using enhanced JQL search (GET /rest/api/3/search/jql).
    Pagination is controlled by nextPageToken; 'total' is not guaranteed.

    Returns tuples of (issueKey, currentStatusName).
    """
    # URL params:
    # - jql: URL-encoded string
    # - fields: comma-separated
    # - maxResults: optional (docs vary; safe to include)
    # - nextPageToken: provided by previous response
    base = f"{client.base_url}/rest/api/3/search/jql"

    next_token: Optional[str] = None
    while True:
        params = {
            "jql": jql,  # requests won’t encode quotes inside; we’ll urlencode below explicitly
            "fields": ",".join(fields),
            "maxResults": str(page_size),
        }
        if next_token:
            params["nextPageToken"] = next_token

        # Manually build query to preserve encoding expectations
        # (especially around quotes and spaces in JQL).
        query = urlencode({k: v for k, v in params.items() if v is not None}, quote_via=quote, safe="=,")
        url = f"{base}?{query}"

        data = get_json(client, url)
        issues = data.get("issues", []) or []

        for it in issues:
            f = it.get("fields", {}) or {}
            status_name = f.get("status", {}).get("name") if isinstance(f.get("status"), dict) else None
            yield it["key"], status_name

        # Enhanced endpoints page with nextPageToken (null/absent at end)
        next_token = data.get("nextPageToken")
        if not next_token:
            break


def first_status_for_issue(client: Client, key: str, current_status_name: Optional[str]) -> str:
    """
    Determine the first-ever status robustly:
      1) Collect ALL status items (created time + stable ordering).
      2) Prefer the earliest item where fromString != toString (ignore no-ops like Done->Done).
      3) If none, use the absolute earliest item.
      4) Compute initial: fromString if present else toString; if nothing, fall back to current status.
    """
    items_all = []  # (created_iso, page_idx, item_idx, fromString, toString)
    start_at = 0
    page_idx = 0

    while True:
        url = f"{client.base_url}/rest/api/2/issue/{key}/changelog?startAt={start_at}&maxResults=100"
        data = get_json(client, url)
        histories = data.get("values") or data.get("histories") or []
        if not histories:
            break

        for h_idx, h in enumerate(histories):
            created = h.get("created") or ""  # ISO 8601 string; Jira orders histories oldest→newest per page
            for i_idx, it in enumerate(h.get("items", [])):
                field = (it.get("field") or it.get("fieldId") or "").lower()
                if field == "status":
                    items_all.append(
                        (created, page_idx, h_idx, i_idx, it.get("fromString"), it.get("toString"))
                    )

        # advance paging
        start_at = (data.get("startAt", start_at) + data.get("maxResults", len(histories)))
        page_idx += 1
        if start_at >= data.get("total", start_at):
            break

    if not items_all:
        return current_status_name or ""

    # Stable chronology: created time, then page index, then within-page order
    items_all.sort(key=lambda x: (x[0], x[1], x[2], x[3]))

    # Prefer the earliest *real* change (from != to)
    for created, pidx, hidx, iidx, frm, to in items_all:
        if (frm or "") != (to or ""):
            return frm if frm else (to or current_status_name or "")

    # If nothing but no-ops, use the earliest item
    _, _, _, _, frm0, to0 = items_all[0]
    return frm0 if frm0 else (to0 or current_status_name or "")


def update_custom_field(client: Client, key: str, custom_field_id: str, value: str):
    url = f"{client.base_url}/rest/api/2/issue/{key}"
    payload = {"fields": {custom_field_id: value}}
    put_json(client, url, payload)


# ------------------------ Orchestrator ------------------------

def main():
    ap = argparse.ArgumentParser(
        description="FAST backfill of first status using enhanced JQL search (GET /rest/api/3/search/jql) + parallel changelog."
    )
    ap.add_argument("--jql", required=True, help='e.g. project = ABC AND "First status" IS EMPTY')
    ap.add_argument("--page-size", type=int, default=100, help="Requested page size for enhanced search (<=100 recommended)")
    ap.add_argument("--concurrency", type=int, default=16, help="Parallel workers for changelog+update")
    ap.add_argument("--dry-run", action="store_true", help="Compute but do not update Jira")
    ap.add_argument("--print-json", action="store_true", help="Print results as JSON lines")
    args = ap.parse_args()

    client = build_client(args.concurrency)
    target_field = os.getenv("JIRA_CUSTOM_FIELD_ID")  # optional

    # Stream issues (no total known), display indeterminate progress bar.
    issues: List[Tuple[str, Optional[str]]] = []
    p_stream = tqdm(unit="issue", desc="Searching (enhanced JQL)", leave=False)
    for key, status_name in stream_issues_enhanced(client, args.jql, fields=["status"], page_size=args.page_size):
        issues.append((key, status_name))
        p_stream.update(1)
    p_stream.close()

    if not issues:
        print("No issues match the JQL.", file=sys.stderr)
        return

    # Process in parallel
    pbar = tqdm(total=len(issues), unit="issue", desc="Processing")
    lock = Lock()
    updated = 0

    def worker(item: Tuple[str, Optional[str]]) -> Tuple[str, str, Optional[str], Optional[str]]:
        key, cur = item
        try:
            first = first_status_for_issue(client, key, cur)
            if target_field and not args.dry_run:
                try:
                    update_custom_field(client, key, target_field, first)
                    return key, first, cur, "UPDATED"
                except Exception as e:
                    return key, first, cur, f"UPDATE FAILED: {e}"
            else:
                return key, first, cur, None
        except Exception as e:
            return key, "", cur, f"ERROR: {e}"

    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as ex:
        futures = [ex.submit(worker, it) for it in issues]
        for fut in as_completed(futures):
            key, first, cur, note = fut.result()
            if args.print_json:
                print(json.dumps({"key": key, "first_status": first}, ensure_ascii=False))
            msg = f"[{key}] first_status='{first}' (current='{cur}')"
            if note:
                msg += f" -> {note}"
                if note == "UPDATED":
                    with lock:
                        updated += 1
            print(msg, file=sys.stderr)
            pbar.update(1)

    pbar.close()
    print(f"Done. Issues processed: {len(issues)}. Updated: {updated}.", file=sys.stderr)


if __name__ == "__main__":
    main()
