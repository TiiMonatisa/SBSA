#!/usr/bin/env python3
"""
Read a CSV/Excel file containing Jira issue keys, fetch details from Jira Cloud,
and save a new CSV containing:

Issue key, Summary, Status, Status Category, Resolved, Created, Resolution, Project Key

Auth: Basic auth with email + API token (Jira Cloud).

Env vars (recommended):
  JIRA_BASE_URL   or CLOUD_BASE_URL
  JIRA_EMAIL      or CLOUD_EMAIL
  JIRA_API_TOKEN  or CLOUD_API_TOKEN
  (All examples: https://your-domain.atlassian.net, you@example.com, your_api_token)

Usage:
  python jira_issue_export.py input.csv output.csv
  python jira_issue_export.py input.xlsx output.csv

Install optional dependency for progress bars:
  pip install tqdm

You can also pass credentials via CLI flags if you prefer (see --help).
"""

import argparse
import os
import sys
import time
import re
from typing import Iterable, List, Dict, Any

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# Resolve environment variables (supports both JIRA_* and CLOUD_* names)
BASE_URL_ENV = os.getenv("JIRA_BASE_URL") or os.getenv("CLOUD_BASE_URL")
EMAIL_ENV = os.getenv("JIRA_EMAIL") or os.getenv("CLOUD_EMAIL")
TOKEN_ENV = os.getenv("JIRA_API_TOKEN") or os.getenv("CLOUD_API_TOKEN")

# --------------------------
# Helpers
# --------------------------

ISSUE_KEY_REGEX = re.compile(r"\b[A-Z][A-Z0-9_]+-\d+\b")

FIELDS = [
    "summary",
    "status",
    "resolution",
    "resolutiondate",
    "created",
    "project",
]

OUTPUT_COLUMNS = [
    "Issue key",
    "Summary",
    "Status",
    "Status Category",
    "Resolved",
    "Created",
    "Resolution",
    "Project Key",
]


def detect_issue_key_column(df: pd.DataFrame) -> str:
    """Try to find a column that contains issue keys.
    Prefers case-insensitive matches for common names, otherwise uses regex sniffing.
    """
    candidates = [
        "issue key",
        "issue",
        "key",
        "ticket",
        "jira",
        "jira key",
    ]
    lower_cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower_cols:
            return lower_cols[c]

    # Fallback: scan columns for something that looks like ABC-123
    best_col = None
    best_hits = 0
    for col in df.columns:
        values = df[col].astype(str).fillna("")
        hits = values.str.contains(ISSUE_KEY_REGEX).sum()
        if hits > best_hits:
            best_hits = hits
            best_col = col
    if best_col is None or best_hits == 0:
        raise ValueError(
            "Could not detect a column with Jira issue keys. Name a column 'Issue key' or include values like ABC-123."
        )
    return best_col


def extract_issue_keys(series: pd.Series) -> List[str]:
    keys: List[str] = []
    for raw in series.astype(str):
        m = ISSUE_KEY_REGEX.search(raw)
        if m:
            keys.append(m.group(0))
    # Deduplicate, preserve order
    seen = set()
    ordered: List[str] = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            ordered.append(k)
    return ordered


def chunked(iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
    chunk: List[Any] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def progress_iter(iterable, total=None, desc=None):
    """Wrap an iterable with a tqdm progress bar if available; otherwise no-op."""
    try:
        from tqdm.auto import tqdm as _tqdm  # type: ignore
        return _tqdm(iterable, total=total, desc=desc)
    except Exception:
        return iterable


class JiraClient:
    def __init__(self, base_url: str, email: str, api_token: str, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.auth = (email, api_token)
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        self.timeout = timeout

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{path}"
        backoff = 1.0
        for attempt in range(6):  # exponential backoff for 429/5xx
            resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
            if resp.status_code in (429, 502, 503, 504):
                retry_after = float(resp.headers.get("Retry-After", backoff))
                time.sleep(retry_after)
                backoff = min(backoff * 2, 30)
                continue
            return resp
        return resp

    def search_issues(self, issue_keys: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fetch using the *enhanced JQL* endpoint (/rest/api/3/search/jql).
        Atlassian removed the old /rest/api/3/search endpoint (410 Gone).
        Uses GET with nextPageToken pagination.
        """
        found: Dict[str, Dict[str, Any]] = {}
        batches = list(chunked(issue_keys, 100))
        for batch in progress_iter(batches, total=len(batches), desc="Fetching from Jira"):
            jql = "issuekey in (" + ",".join(batch) + ")"
            next_token = None
            while True:
                params = {
                    "jql": jql,
                    "maxResults": 100,
                    # fields can be comma-separated string per API
                    "fields": ",".join(FIELDS),
                }
                if next_token:
                    params["nextPageToken"] = next_token
                resp = self._request("GET", "/rest/api/3/search/jql", params=params)
                if resp.status_code != 200:
                    raise RuntimeError(f"Jira search failed: {resp.status_code} {resp.text}")
                data = resp.json()
                for issue in data.get("issues", []):
                    key = issue.get("key")
                    found[key] = issue.get("fields", {})
                # Enhanced API uses isLast + nextPageToken
                if data.get("isLast", True):
                    break
                next_token = data.get("nextPageToken")
                if not next_token:
                    break
        return found


def normalize_row(key: str, f: Dict[str, Any]) -> Dict[str, Any]:
    status = (f.get("status") or {})
    status_name = status.get("name")
    status_cat = (status.get("statusCategory") or {}).get("name")
    resolution = f.get("resolution") or {}
    resolution_name = resolution.get("name")

    return {
        "Issue key": key,
        "Summary": f.get("summary"),
        "Status": status_name,
        "Status Category": status_cat,
        # Jira's field is 'resolutiondate'; user wants 'Resolved'
        "Resolved": f.get("resolutiondate"),
        "Created": f.get("created"),
        "Resolution": resolution_name,
        "Project Key": (f.get("project") or {}).get("key"),
    }


def read_input(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xlsm", ".xls"):
        return pd.read_excel(path)
    elif ext == ".csv":
        return pd.read_csv(path)
    else:
        # try csv by default
        try:
            return pd.read_csv(path)
        except Exception:
            raise ValueError("Unsupported file type. Please provide CSV or Excel (.xlsx).")


def write_output(df: pd.DataFrame, path: str) -> None:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        df.to_csv(path, index=False)
    elif ext in (".xlsx", ".xlsm"):
        df.to_excel(path, index=False)
    else:
        df.to_csv(path, index=False)


def main():
    parser = argparse.ArgumentParser(description="Export Jira issue details to CSV.")
    parser.add_argument("input", help="Path to input CSV/Excel containing issue keys")
    parser.add_argument("output", help="Path to output CSV/Excel")
    parser.add_argument("--base-url", dest="base_url", default=BASE_URL_ENV, help="Jira base URL, e.g. https://your-domain.atlassian.net")
    parser.add_argument("--email", dest="email", default=EMAIL_ENV, help="Jira account email")
    parser.add_argument("--api-token", dest="api_token", default=TOKEN_ENV, help="Jira API token")

    args = parser.parse_args()

    if not args.base_url or not args.email or not args.api_token:
        print("Missing Jira credentials. Set env vars JIRA_BASE_URL/JIRA_EMAIL/JIRA_API_TOKEN or CLOUD_BASE_URL/CLOUD_EMAIL/CLOUD_API_TOKEN, or pass CLI flags.", file=sys.stderr)
        sys.exit(2)

    # Read input file
    df = read_input(args.input)
    col = detect_issue_key_column(df)
    issue_keys = extract_issue_keys(df[col])
    if not issue_keys:
        raise SystemExit("No valid Jira issue keys found in the input file.")

    # Fetch from Jira
    client = JiraClient(args.base_url, args.email, args.api_token)
    fields_by_key = client.search_issues(issue_keys)

    # Build output rows
    rows = []
    for key in progress_iter(issue_keys, total=len(issue_keys), desc="Building output"):
        f = fields_by_key.get(key)
        if f is None:
            rows.append({
                "Issue key": key,
                "Summary": None,
                "Status": None,
                "Status Category": None,
                "Resolved": None,
                "Created": None,
                "Resolution": None,
                "Project Key": None,
            })
        else:
            rows.append(normalize_row(key, f))

    out_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    write_output(out_df, args.output)
    print(f"Wrote {len(out_df)} rows to {args.output}")


if __name__ == "__main__":
    main()
