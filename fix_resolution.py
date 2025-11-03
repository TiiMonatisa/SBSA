#!/usr/bin/env python3
"""
Clear Resolution in Jira Cloud by EDITING the issue (no workflow transition).

Search: POST /rest/api/2/search/jql   (nextPageToken pagination)
Edit:   PUT  /rest/api/2/issue/{key}   (payload: {"fields":{"resolution": null}})

.env:
  CLOUD_BASE_URL   = https://your-domain.atlassian.net
  CLOUD_EMAIL      = you@example.com
  CLOUD_API_TOKEN  = <jira api token>

Install:
  pip install requests tqdm python-dotenv
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime
from typing import Iterable, Optional, List, Dict

from dotenv import load_dotenv
import requests
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    load_dotenv()

    p = argparse.ArgumentParser(
        description="Clear Resolution (and thus resolutiondate) by editing issues selected by a JQL."
    )
    p.add_argument("--base-url", default=os.getenv("CLOUD_BASE_URL"),
                   help="Jira base URL (env CLOUD_BASE_URL)")
    p.add_argument("--email", default=os.getenv("CLOUD_EMAIL"),
                   help="Jira user email (env CLOUD_EMAIL)")
    p.add_argument("--api-token", default=os.getenv("CLOUD_API_TOKEN"),
                   help="Jira API token (env CLOUD_API_TOKEN)")
    p.add_argument("--jql", required=True, help="JQL to select issues")
    p.add_argument("--page-size", type=int, default=100, help="Search page size (default 100)")
    p.add_argument("--dry-run", action="store_true",
                   help="Do not edit issues; only log what would happen")
    p.add_argument("--log-file", default="cleared_resolution_log.csv",
                   help="CSV file to append logs")
    p.add_argument("--verify", action="store_true",
                   help="After edit, refetch the issue to verify resolution cleared")
    args = p.parse_args()

    missing = []
    if not args.base_url:  missing.append("CLOUD_BASE_URL / --base-url")
    if not args.email:     missing.append("CLOUD_EMAIL / --email")
    if not args.api_token: missing.append("CLOUD_API_TOKEN / --api-token")
    if missing:
        print("Missing required config: " + ", ".join(missing), file=sys.stderr)
        sys.exit(2)
    return args


class JiraClient:
    def __init__(self, base_url: str, email: str, token: str, page_size: int = 100):
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.session = requests.Session()
        self.session.auth = (email, token)
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

    def _rate_limit_guard(self, resp: requests.Response):
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "5"))
            time.sleep(retry_after)
            return True
        return False

    def post(self, path: str, payload: dict) -> requests.Response:
        url = f"{self.base_url}{path}"
        while True:
            resp = self.session.post(url, json=payload)
            if self._rate_limit_guard(resp):
                continue
            return resp

    def get(self, path: str, params: dict | None = None) -> requests.Response:
        url = f"{self.base_url}{path}"
        while True:
            resp = self.session.get(url, params=params)
            if self._rate_limit_guard(resp):
                continue
            return resp

    def put(self, path: str, payload: dict) -> requests.Response:
        url = f"{self.base_url}{path}"
        while True:
            resp = self.session.put(url, json=payload)
            if self._rate_limit_guard(resp):
                continue
            return resp

    # -------- New search API with nextPageToken --------
    def search_jql_collect_keys(self, jql: str) -> List[str]:
        """Collect all issue keys using POST /rest/api/2/search/jql (fields=['key'])."""
        keys: List[str] = []
        next_token = None
        while True:
            body = {"maxResults": self.page_size, "fields": ["key"]}
            if next_token:
                body["nextPageToken"] = next_token
            else:
                body["jql"] = jql
            resp = self.post("/rest/api/2/search/jql", body)
            if not resp.ok:
                raise RuntimeError(f"Search failed: {resp.status_code} {resp.text}")
            data = resp.json()
            for issue in data.get("issues", []):
                keys.append(issue["key"])
            next_token = data.get("nextPageToken")
            if not next_token:
                break
        return keys

    def fetch_issues_fields(self, keys: List[str], fields: Iterable[str]) -> Dict[str, dict]:
        """Fetch fields for keys via batched `key in (...)` JQL using the same endpoint."""
        result: Dict[str, dict] = {}
        fields_list = list(fields)
        batch_size = 200
        for i in range(0, len(keys), batch_size):
            batch = keys[i:i + batch_size]
            jql = "key in (" + ",".join(batch) + ")"
            next_token = None
            while True:
                body = {"maxResults": self.page_size, "fields": fields_list}
                if next_token:
                    body["nextPageToken"] = next_token
                else:
                    body["jql"] = jql
                resp = self.post("/rest/api/2/search/jql", body)
                if not resp.ok:
                    raise RuntimeError(f"Field fetch failed: {resp.status_code} {resp.text}")
                data = resp.json()
                for issue in data.get("issues", []):
                    result[issue["key"]] = issue
                next_token = data.get("nextPageToken")
                if not next_token:
                    break
        return result

    # -------- Direct edit to clear resolution --------
    def clear_resolution(self, issue_key: str) -> requests.Response:
        """
        PUT /rest/api/2/issue/{key}
        payload: {"fields":{"resolution": None}}
        """
        payload = {"fields": {"resolution": None}}
        return self.put(f"/rest/api/2/issue/{issue_key}", payload)

    def get_issue(self, issue_key: str, fields: Iterable[str]) -> dict:
        resp = self.get(f"/rest/api/3/issue/{issue_key}", params={"fields": ",".join(fields)})
        if not resp.ok:
            raise RuntimeError(f"Get issue failed for {issue_key}: {resp.status_code} {resp.text}")
        return resp.json()


def ensure_log_header(path: str):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "timestamp_iso", "issue_key", "action", "message",
                "resolution_before", "resolution_after", "status"
            ])


def log_row(path: str, issue_key: str, action: str, message: str = "",
            resolution_before: Optional[str] = None,
            resolution_after: Optional[str] = None,
            status: Optional[str] = None):
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            datetime.utcnow().isoformat(),
            issue_key, action, message, resolution_before, resolution_after, status
        ])


def main():
    args = parse_args()
    client = JiraClient(args.base_url, args.email, args.api_token, page_size=args.page_size)
    ensure_log_header(args.log_file)

    print("Collecting issues with the new JQL API...")
    keys = client.search_jql_collect_keys(args.jql)
    total = len(keys)
    if total == 0:
        print("No issues matched your JQL.")
        return

    print(f"Found {total} issues. Fetching fields...")
    issues_map = client.fetch_issues_fields(keys, fields=("key", "resolution", "status", "summary"))

    processed = cleared = skipped = errors = 0

    print("Processing…")
    for key in tqdm(keys, total=total, unit="issue"):
        issue = issues_map.get(key, {"key": key, "fields": {}})
        fields = issue.get("fields", {})
        summary = fields.get("summary") or ""
        resolution = fields.get("resolution")
        resolution_name = resolution.get("name") if isinstance(resolution, dict) else None
        status_name = (fields.get("status") or {}).get("name")

        # Skip already-unresolved
        if resolution is None:
            log_row(args.log_file, key, "SKIP_ALREADY_UNRESOLVED", summary, None, None, status_name)
            skipped += 1
            processed += 1
            continue

        try:
            if args.dry_run:
                log_row(args.log_file, key, "DRY_RUN_WOULD_CLEAR", summary, resolution_name, None, status_name)
            else:
                resp = client.clear_resolution(key)
                if resp.status_code not in (204, 200):
                    # Log server message to help diagnose permissions/validators
                    log_row(args.log_file, key, "ERROR_EDIT_FAILED",
                            f"HTTP {resp.status_code}: {resp.text}", resolution_name, None, status_name)
                    errors += 1
                    processed += 1
                    continue

                res_after = None
                if args.verify:
                    ji = client.get_issue(key, fields=("resolution", "resolutiondate", "status"))
                    raf = ji.get("fields", {}).get("resolution")
                    res_after = (raf.get("name") if isinstance(raf, dict)
                                 else (None if raf is None else str(raf)))
                log_row(args.log_file, key, "CLEARED", summary, resolution_name, res_after, status_name)
                cleared += 1

        except Exception as e:
            log_row(args.log_file, key, "ERROR_EXCEPTION", f"{type(e).__name__}: {e}", resolution_name, None, status_name)
            errors += 1

        processed += 1

    print("\n--- Summary ---")
    print(f"Total issues:        {total}")
    print(f"Processed:           {processed}")
    print(f"Cleared:             {cleared}")
    print(f"Already Unresolved:  {skipped}")
    print(f"Errors:              {errors}")
    print(f"\nLog written to: {args.log_file}")
    if args.dry_run:
        print("DRY-RUN mode: no changes were made. Remove --dry-run to apply edits.")


if __name__ == "__main__":
    main()
