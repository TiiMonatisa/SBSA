#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backfill the FIRST status for Jira issues using Jira search (Cloud enhanced JQL
or DC classic JQL) and the issue changelog.

ENV (.env):
  # Jira Cloud
  CLOUD_BASE_URL=https://<your-site>.atlassian.net
  CLOUD_EMAIL=you@company.com
  CLOUD_API_TOKEN=your_api_token

  # Jira Data Center / Server
  DC_BASE_URL=https://your-dc-jira.example.com
  DC_USERNAME=your_dc_username
  DC_API_TOKEN=your_dc_api_token

  # Optional to write results:
  JIRA_CUSTOM_FIELD_ID=customfield_12345

  # Optional SSL verification override:
  #   - leave unset or empty to use requests default (True)
  #   - set to "false", "0", or "no" (case-insensitive) to disable verification
  #     (NOT recommended)
  #   - set to a path (e.g. /opt/file.pem) for a custom CA bundle / server cert
  SSL_VERIFY=/opt/file.pem

Notes:
- For Cloud, we use the enhanced JQL search endpoint:
    GET /rest/api/3/search/jql
  which paginates via nextPageToken (no startAt/total semantics).
- For Data Center / Server, we use the classic search endpoint:
    GET /rest/api/2/search
  with startAt + maxResults pagination.
- For each issue, we read /rest/api/2/issue/{key}/changelog and infer the first
  status:
    earliest status change: fromString if present, else toString.

CLI:
  python backfill_first_status.py \
    --jql 'project = ABC AND "First status" IS EMPTY' \
    --target cloud \
    --page-size 100 \
    --concurrency 16 \
    --dry-run

  Use --target dc to run against Jira Data Center / Server.
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
    verify: Any  # bool or str (PEM path)


def required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        print(f"Missing required environment variable: {name}", file=sys.stderr)
        sys.exit(2)
    return v


def ssl_verify_from_env() -> Any:
    """
    SSL_VERIFY semantics:
      - unset / empty -> default True (requests default)
      - "false", "0", "no" (case-insensitive) -> False (disable verify)
      - anything else -> treated as a path to a CA bundle / PEM file
    """
    v = os.getenv("SSL_VERIFY")
    if not v:
        return True
    if v.lower() in ("false", "0", "no"):
        return False
    return v


def build_client(concurrency: int, target: str) -> Client:
    """
    Build a Jira client for either Cloud or Data Center / Server.

    target:
      - "cloud": uses CLOUD_* env vars and enhanced search.
      - "dc":    uses DC_* env vars and classic /rest/api/2/search.
    """
    load_dotenv()

    verify = ssl_verify_from_env()

    if target == "dc":
        base_url = required_env("DC_BASE_URL").rstrip("/")
        username = required_env("DC_USERNAME")
        token = required_env("DC_API_TOKEN")
        auth = HTTPBasicAuth(username, token)
        user_agent = "first-status-backfill-dc/3.0"
    else:
        base_url = required_env("CLOUD_BASE_URL").rstrip("/")
        email = required_env("CLOUD_EMAIL")
        token = required_env("CLOUD_API_TOKEN")
        auth = HTTPBasicAuth(email, token)
        user_agent = "first-status-backfill-cloud/3.0"

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
        "User-Agent": user_agent,
    }

    return Client(base_url, sess, auth, headers, verify)


def request_with_backoff(client: Client, method: str, url: str, **kwargs) -> requests.Response:
    max_attempts = 7
    for attempt in range(1, max_attempts + 1):
        r = client.session.request(
            method,
            url,
            auth=client.auth,
            headers=client.headers,
            timeout=60,
            verify=client.verify,
            **kwargs,
        )
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


def put_json(client: Client, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = request_with_backoff(client, "PUT", url, json=payload)
    if r.status_code >= 400:
        raise RuntimeError(f"PUT {url} -> {r.status_code} {r.text[:500]}")
    if r.text:
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}
    return {}


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


def stream_issues_dc(client: Client, jql: str, fields: List[str], page_size: int = 100):
    """
    Generator over issues for Jira Data Center / Server using classic search API
    (GET /rest/api/2/search).

    Pagination uses startAt + maxResults.
    """
    base = f"{client.base_url}/rest/api/2/search"
    start_at = 0

    while True:
        params = {
            "jql": jql,
            "fields": ",".join(fields),
            "maxResults": str(page_size),
            "startAt": str(start_at),
        }
        query = urlencode(params, quote_via=quote, safe="=,")
        url = f"{base}?{query}"

        data = get_json(client, url)
        issues = data.get("issues", []) or []

        for it in issues:
            f = it.get("fields", {}) or {}
            status_name = f.get("status", {}).get("name") if isinstance(f.get("status"), dict) else None
            yield it["key"], status_name

        if len(issues) < page_size:
            break
        start_at += page_size


def first_status_for_issue(client: Client, key: str, current_status_name: Optional[str]) -> str:
    """
    Determine the first-ever status robustly.

    Logic:
      1) Fetch the full changelog (prefer the dedicated /changelog endpoint, fall back to ?expand=changelog).
      2) Collect ALL status items with a stable chronological ordering.
      3) Prefer the earliest item where fromString != toString (ignore no-op changes).
      4) If none, use the absolute earliest status item.
      5) First status = fromString if present, else toString, else fall back to current_status_name.
    """

    def _iter_pages_via_changelog_endpoint() -> List[Tuple[str, int, int, int, Optional[str], Optional[str]]]:
        """
        Primary path: /rest/api/2/issue/{key}/changelog?startAt=...&maxResults=...
        Handles both:
          - Top level: {startAt,maxResults,total,values|histories:[...]}
          - Nested:    {changelog:{startAt,maxResults,total,values|histories:[...]}}
        Returns a list of tuples:
          (created_iso, page_idx, history_idx, item_idx, fromString, toString)
        """
        items: List[Tuple[str, int, int, int, Optional[str], Optional[str]]] = []
        start_at = 0
        page_idx = 0

        while True:
            url = f"{client.base_url}/rest/api/2/issue/{key}/changelog?startAt={start_at}&maxResults=100"
            data = get_json(client, url)

            # Some Jira variants wrap changelog under "changelog", others put paging fields at top level.
            container = data.get("changelog") or data

            histories = (
                container.get("values")
                or container.get("histories")
                or []
            )

            if not histories:
                break

            for h_idx, h in enumerate(histories):
                created = h.get("created") or ""
                for i_idx, it in enumerate(h.get("items", []) or []):
                    field = (it.get("field") or it.get("fieldId") or "").lower()
                    if field == "status":
                        items.append(
                            (created, page_idx, h_idx, i_idx, it.get("fromString"), it.get("toString"))
                        )

            start_val = container.get("startAt", start_at)
            max_results = container.get("maxResults", len(histories))
            total = container.get("total", start_val + len(histories))

            # Advance paging
            start_at = start_val + max_results
            page_idx += 1
            if start_at >= total:
                break

        return items

    def _iter_pages_via_expand() -> List[Tuple[str, int, int, int, Optional[str], Optional[str]]]:
        """
        Fallback path if /changelog endpoint is unavailable (for example on some Jira DC setups):
          GET /rest/api/2/issue/{key}?expand=changelog

        This usually returns a single page of changelog:
          { ..., "changelog": { startAt,maxResults,total,histories:[...] } }
        """
        url = f"{client.base_url}/rest/api/2/issue/{key}?expand=changelog"
        data = get_json(client, url)

        changelog = data.get("changelog") or {}
        histories = (
            changelog.get("values")
            or changelog.get("histories")
            or []
        )

        items: List[Tuple[str, int, int, int, Optional[str], Optional[str]]] = []
        page_idx = 0

        for h_idx, h in enumerate(histories):
            created = h.get("created") or ""
            for i_idx, it in enumerate(h.get("items", []) or []):
                field = (it.get("field") or it.get("fieldId") or "").lower()
                if field == "status":
                    items.append(
                        (created, page_idx, h_idx, i_idx, it.get("fromString"), it.get("toString"))
                    )

        return items

    # 1) Try the dedicated /changelog endpoint first.
    items_all: List[Tuple[str, int, int, int, Optional[str], Optional[str]]] = []
    try:
        items_all = _iter_pages_via_changelog_endpoint()
    except RuntimeError as e:
        # Only fall back for "endpoint not found / not allowed" style errors.
        msg = str(e)
        if " 404 " in msg or " 405 " in msg:
            items_all = _iter_pages_via_expand()
        else:
            # Propagate all other errors up to the caller.
            raise

    # 2) If still nothing, fall back to current status.
    if not items_all:
        return current_status_name or ""

    # 3) Sort chronologically and then by (page, history, item) to get a stable order.
    items_all.sort(key=lambda x: (x[0], x[1], x[2], x[3]))

    # 4) Prefer earliest real change where from != to (ignore no-op updates).
    for created, pidx, hidx, iidx, frm, to in items_all:
        if (frm or "") != (to or ""):
            return frm if frm else (to or current_status_name or "")

    # 5) Fallback: use the absolute earliest status item.
    created, pidx, hidx, iidx, frm, to = items_all[0]
    return frm if frm else (to or current_status_name or "")


def compute_first_status(
    client: Client, key: str, current_status_name: Optional[str]
) -> Tuple[str, Optional[str]]:
    """
    Returns (key, first_status_name).
    """
    first = first_status_for_issue(client, key, current_status_name)
    return key, first


def update_custom_field(client: Client, key: str, custom_field_id: str, value: str):
    url = f"{client.base_url}/rest/api/2/issue/{key}"
    payload = {"fields": {custom_field_id: value}}
    put_json(client, url, payload)


# ------------------------ Orchestrator ------------------------


def main():
    ap = argparse.ArgumentParser(
        description="FAST backfill of first status using Jira search (Cloud enhanced JQL or DC classic JQL) + parallel changelog."
    )
    ap.add_argument("--jql", required=True, help='e.g. project = ABC AND "First status" IS EMPTY')
    ap.add_argument("--page-size", type=int, default=100, help="Requested page size for search (<=100 recommended)")
    ap.add_argument("--concurrency", type=int, default=16, help="Parallel workers for changelog+update")
    ap.add_argument(
        "--target",
        choices=["cloud", "dc"],
        default="cloud",
        help="Target Jira deployment: 'cloud' (default, uses enhanced search) or 'dc' (Data Center/server, uses classic search).",
    )
    ap.add_argument("--dry-run", action="store_true", help="Compute but do not update Jira")
    ap.add_argument("--print-json", action="store_true", help="Print results as JSON lines")
    args = ap.parse_args()

    client = build_client(args.concurrency, args.target)
    target_field = os.getenv("JIRA_CUSTOM_FIELD_ID")  # optional

    # Stream issues, display progress bar.
    issues: List[Tuple[str, Optional[str]]] = []
    if args.target == "dc":
        p_stream = tqdm(unit="issue", desc="Searching (DC classic JQL)", leave=False)
        iterator = stream_issues_dc(client, args.jql, fields=["status"], page_size=args.page_size)
    else:
        p_stream = tqdm(unit="issue", desc="Searching (Cloud enhanced JQL)", leave=False)
        iterator = stream_issues_enhanced(client, args.jql, fields=["status"], page_size=args.page_size)

    for key, status_name in iterator:
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
            note = None
            if target_field and not args.dry_run:
                if first:
                    update_custom_field(client, key, target_field, first)
                    note = "UPDATED"
                else:
                    note = "SKIPPED_EMPTY"
            return key, first, cur, note
        except Exception as e:
            return key, "", cur, f"ERROR: {e}"

    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = {ex.submit(worker, it): it for it in issues}
        for fut in as_completed(futures):
            key, first, cur, note = fut.result()
            rec = {
                "key": key,
                "first_status": first,
                "current_status": cur,
                "note": note,
            }
            if args.print_json:
                print(json.dumps(rec, sort_keys=True))
            msg = f"{key}: first={first!r}, current={cur!r}"
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
