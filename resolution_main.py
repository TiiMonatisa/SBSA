#!/usr/bin/env python3
"""
Find Jira Cloud issues that have a Resolution and/or resolutiondate even though they
never transitioned into a status that *permits* resolution.

Key features:
- Uses /rest/api/*/search/jql (falls back to /search).
- CSV is flushed + fsync'd after header and every row.
- --append to never erase file contents. If --resume-count > 0, append is forced on.
- Resume from the candidate index you provide (the number you saw in the progress bar).
  The progress bar and final "Checked" counter start at that index (not 0).

Env (optional) — supports CLOUD_* (preferred) and JIRA_* fallbacks:
  CLOUD_BASE_URL / JIRA_BASE_URL
  CLOUD_EMAIL    / JIRA_EMAIL
  CLOUD_API_TOKEN/ JIRA_API_TOKEN
  JIRA_JQL, JIRA_PROJECT, JIRA_ALLOWED_CATEGORIES, JIRA_ALLOWED_NAMES
  JIRA_MAX_RESULTS, JIRA_PROGRESS, JIRA_APPEND, JIRA_OUT, JIRA_RESUME_COUNT, JIRA_ERRORS_FILE
"""

import argparse
import csv
import os
import sys
import time
from typing import Dict, List, Set, Tuple

import requests
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Optional progress bar
try:
    from tqdm import tqdm  # type: ignore
    _HAS_TQDM = True
except Exception:
    _HAS_TQDM = False

# --------------------------- Configuration ----------------------------------

def _env(name: str, default: str = "") -> str:
    # Prefer CLOUD_* if present, else fall back to JIRA_*
    if name.startswith("JIRA_"):
        cloud_name = name.replace("JIRA_", "CLOUD_", 1)
        return os.getenv(cloud_name, os.getenv(name, default))
    return os.getenv(name, default)

BASE_URL = _env("CLOUD_BASE_URL", "https://your-domain.atlassian.net").rstrip("/")
EMAIL = _env("CLOUD_EMAIL", "you@company.com")
API_TOKEN = _env("CLOUD_API_TOKEN", "YOUR_API_TOKEN")
PROJECT_KEY = os.getenv("JIRA_PROJECT")  # optional

DEFAULT_JQL_CORE = "(resolution IS NOT EMPTY OR resolutiondate IS NOT EMPTY) ORDER BY updated DESC"
USER_JQL = os.getenv("JIRA_JQL")

MAX_RESULTS = int(os.getenv("JIRA_MAX_RESULTS", "100"))
RATE_SLEEP = 0.05
PROGRESS = os.getenv("JIRA_PROGRESS", "1") != "0"
APPEND_DEFAULT = os.getenv("JIRA_APPEND", "0") == "1"
OUT_DEFAULT = os.getenv("JIRA_OUT", "suspicious_resolutions.csv")
RESUME_DEFAULT = int(os.getenv("JIRA_RESUME_COUNT", "0"))
ERRORS_DEFAULT = os.getenv("JIRA_ERRORS_FILE", "errors.log")

# Allowed categories: Jira uses keys: new, indeterminate, done
ALLOWED_STATUS_CATEGORIES: Set[str] = set(
    s.strip().lower() for s in os.getenv("JIRA_ALLOWED_CATEGORIES", "done").split(",") if s.strip()
)
# Allowed status names take precedence over categories when present
ALLOWED_STATUS_NAMES: Set[str] = set(
    s.strip().lower() for s in os.getenv("JIRA_ALLOWED_NAMES", "").split(",") if s.strip()
)

SESSION = requests.Session()
SESSION.auth = (EMAIL, API_TOKEN)
SESSION.headers.update({
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "obsidian-resolution-auditor/1.4",
})

API_PREFIX = None  # detected at runtime

# --------------------------- Helpers ----------------------------------------

def jira_get(path: str, params: Dict = None) -> dict:
    url = f"{BASE_URL}{path}"
    resp = SESSION.get(url, params=params, timeout=60)
    if resp.status_code == 429:
        time.sleep(2)
        resp = SESSION.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()

def log_error(errors_file: str, message: str) -> None:
    try:
        with open(errors_file, "a", encoding="utf-8") as ef:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            ef.write(f"[{ts}] {message}\n")
    except Exception:
        pass

def detect_api_prefix() -> str:
    """Detect the highest available Jira REST API version (v3 then v2)."""
    global API_PREFIX
    for ver in ("/rest/api/3", "/rest/api/2"):
        try:
            jira_get(f"{ver}/status")
            API_PREFIX = ver
            return API_PREFIX
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                continue
            raise
    raise RuntimeError("Could not detect Jira REST API version (tried v3 and v2)")

def build_jql() -> str:
    core = USER_JQL.strip() if USER_JQL else DEFAULT_JQL_CORE
    if PROJECT_KEY:
        core = f"project = {PROJECT_KEY} AND (" + core.rstrip().rstrip(")") + ")"
    return core

def get_status_category_map() -> Dict[str, str]:
    assert API_PREFIX, "API prefix not detected"
    data = jira_get(f"{API_PREFIX}/status")
    mapping = {}
    for s in data:
        name = (s.get("name") or "").strip().lower()
        cat = (s.get("statusCategory", {}).get("key") or "").strip().lower()
        if name:
            mapping[name] = cat
    return mapping

def get_candidate_total(jql: str) -> int:
    """Try to get a total count via legacy /search."""
    assert API_PREFIX, "API prefix not detected"
    try:
        data = jira_get(
            f"{API_PREFIX}/search",
            params={"jql": jql, "fields": "none", "maxResults": 0, "startAt": 0},
        )
        total = data.get("total")
        return int(total) if total is not None else -1
    except Exception:
        return -1

def search_issues(jql: str):
    """Prefer /search/jql (nextPageToken,isLast), fall back to /search (startAt,total)."""
    assert API_PREFIX, "API prefix not detected"

    next_token = None
    use_enhanced = True

    while True:
        try:
            params = {
                "jql": jql,
                "fields": "summary,status,resolution,resolutiondate",
                "maxResults": MAX_RESULTS,
            }
            if next_token:
                params["nextPageToken"] = next_token

            data = jira_get(f"{API_PREFIX}/search/jql", params=params)
            issues = data.get("issues", [])
            for issue in issues:
                yield issue

            next_token = data.get("nextPageToken")
            is_last = data.get("isLast")
            if is_last or not next_token:
                break

            time.sleep(RATE_SLEEP)

        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (404, 410):
                use_enhanced = False
            else:
                raise

        if not use_enhanced:
            start_at = 0
            while True:
                data = jira_get(
                    f"{API_PREFIX}/search",
                    params={
                        "jql": jql,
                        "fields": "summary,status,resolution,resolutiondate",
                        "maxResults": MAX_RESULTS,
                        "startAt": start_at,
                    },
                )
                issues = data.get("issues", [])
                for issue in issues:
                    yield issue
                start_at += len(issues)
                if start_at >= data.get("total", 0) or not issues:
                    break
                time.sleep(RATE_SLEEP)
            break

def fetch_full_changelog(issue_id: str) -> List[dict]:
    assert API_PREFIX, "API prefix not detected"
    start_at = 0
    histories: List[dict] = []
    while True:
        data = jira_get(f"{API_PREFIX}/issue/{issue_id}/changelog", params={"startAt": start_at, "maxResults": 100})
        page = data.get("values")
        is_last = data.get("isLast")
        if page is None:
            page = data.get("histories", [])
            total = data.get("total")
            if total is not None:
                is_last = start_at + len(page) >= int(total)
        histories.extend(page or [])
        if is_last or not page:
            break
        start_at += len(page)
        time.sleep(RATE_SLEEP)
    return histories

def ever_entered_allowed_status(issue_id: str, status_map: Dict[str, str]) -> Tuple[bool, List[str]]:
    histories = fetch_full_changelog(issue_id)
    hits: List[str] = []

    def is_allowed(status_name: str) -> bool:
        if not status_name:
            return False
        name_l = status_name.strip().lower()
        if name_l in ALLOWED_STATUS_NAMES:
            return True
        cat = status_map.get(name_l)
        if cat and cat in ALLOWED_STATUS_CATEGORIES:
            return True
        return False

    for h in histories:
        for item in h.get("items", []):
            if (item.get("field") or item.get("fieldId")) == "status":
                to_status = item.get("toString") or item.get("to") or ""
                if is_allowed(to_status):
                    hits.append(str(to_status))
    return (len(hits) > 0, hits)

def has_resolution(fields: dict) -> bool:
    return bool(fields.get("resolution")) or bool(fields.get("resolutiondate"))

# --------------------------- Main -------------------------------------------

def main():
    if "atlassian.net" not in BASE_URL:
        print("ERROR: Please set JIRA_BASE_URL/CLOUD_BASE_URL to your Jira Cloud site", file=sys.stderr)
        sys.exit(2)
    if not EMAIL or not API_TOKEN or EMAIL == "you@company.com" or API_TOKEN == "YOUR_API_TOKEN":
        print("ERROR: Set CLOUD_EMAIL/JIRA_EMAIL and CLOUD_API_TOKEN/JIRA_API_TOKEN.", file=sys.stderr)
        sys.exit(2)

    try:
        detect_api_prefix()
    except Exception as e:
        print(f"ERROR detecting Jira REST API version: {e}", file=sys.stderr)
        sys.exit(2)

    print(f"Using API: {API_PREFIX}")
    print("Search endpoint: trying /search/jql (enhanced); will fall back to /search if needed")

    parser = argparse.ArgumentParser(description="Audit Jira issues with suspicious resolutions")
    parser.add_argument("--append", action="store_true", default=APPEND_DEFAULT, help="Append to CSV instead of overwriting")
    parser.add_argument("--out", default=OUT_DEFAULT, help="Output CSV path")
    parser.add_argument("--resume-count", type=int, default=RESUME_DEFAULT, help="Number of candidate issues to skip before processing")
    parser.add_argument("--errors-file", default=ERRORS_DEFAULT, help="File to append detailed error logs")
    args = parser.parse_args()

    append = bool(args.append)
    out_path = args.out
    resume_count = max(0, int(args.resume_count))
    errors_file = args.errors_file

    # Force append if resuming to guarantee we do NOT erase file contents.
    if resume_count and not append:
        append = True
        print("Append mode enabled automatically because --resume-count was provided.")

    jql = build_jql()
    print(f"Using JQL: {jql}")
    if resume_count:
        print(f"Resuming from candidate #{resume_count + 1} (skipping {resume_count})")

    status_map = get_status_category_map()
    print(f"Allowed categories: {sorted(ALLOWED_STATUS_CATEGORIES)}; Allowed names: {sorted(ALLOWED_STATUS_NAMES)}")

    total_candidates = get_candidate_total(jql)
    if PROGRESS and _HAS_TQDM:
        pbar = tqdm(total=None if total_candidates < 0 else total_candidates,
                    unit="issue", desc="Scanning", initial=resume_count)
    else:
        pbar = None
        if PROGRESS:
            print("Scanning issues... (progress shown every 50)")

    # Prepare CSV open mode and header decision
    need_header = True
    if append and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        need_header = False
    mode = "a" if append else "w"

    with open(out_path, mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if need_header:
            writer.writerow(["key", "summary", "status", "resolution", "resolutiondate", "url", "reason"])
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass

        # Counters
        count_seen = resume_count     # start at resume index so it doesn't restart at 0
        count_flagged = 0
        processed_so_far = 0

        for issue in search_issues(jql):
            # Skip ahead (fast-forward) WITHOUT ticking pbar
            if resume_count and processed_so_far < resume_count:
                processed_so_far += 1
                if PROGRESS and processed_so_far % 50 == 0 and pbar is None:
                    print(f"Skipped {processed_so_far}...")
                continue

            processed_so_far += 1

            # One progress tick per processed candidate (no ticks during skipping)
            if pbar is not None:
                pbar.update(1)
            elif PROGRESS and processed_so_far % 50 == 0:
                print(f"Processed {processed_so_far} candidates...")

            key = issue.get("key")
            fields = issue.get("fields", {})
            if not has_resolution(fields):
                continue  # should be filtered by JQL, but keep safe

            # This is a processed candidate
            count_seen += 1

            status_name = (fields.get("status", {}) or {}).get("name", "")
            resolution_name = (fields.get("resolution", {}) or {}).get("name", "")
            resolution_date = fields.get("resolutiondate", "")

            try:
                entered, hits = ever_entered_allowed_status(issue.get("id"), status_map)
                fetch_error = None
            except requests.HTTPError as e:
                code = e.response.status_code if getattr(e, 'response', None) is not None else 'N/A'
                fetch_error = f"HTTP {code}"
                log_error(errors_file, f"{key}: changelog fetch error {fetch_error}")
                entered, hits = (None, [])
            except Exception as e:
                fetch_error = str(e)
                log_error(errors_file, f"{key}: changelog fetch error {fetch_error}")
                entered, hits = (None, [])

            if fetch_error:
                count_flagged += 1
                url = f"{BASE_URL}/browse/{key}"
                reason = f"Changelog fetch error: {fetch_error}"
                writer.writerow([
                    key,
                    (fields.get("summary") or "").replace("\n", " ").strip(),
                    status_name,
                    resolution_name,
                    resolution_date,
                    url,
                    reason,
                ])
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            elif not entered:
                count_flagged += 1
                url = f"{BASE_URL}/browse/{key}"
                reason = "Has resolution but never entered an allowed status"
                writer.writerow([
                    key,
                    (fields.get("summary") or "").replace("\n", " ").strip(),
                    status_name,
                    resolution_name,
                    resolution_date,
                    url,
                    reason,
                ])
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass

            time.sleep(RATE_SLEEP)

    if pbar is not None:
        pbar.close()

    print(f"Checked {count_seen} candidate issues. Flagged {count_flagged} suspicious issues.")
    print(f"Results written to {out_path}")

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"HTTP error: {e}", file=sys.stderr)
        if e.response is not None:
            try:
                print(e.response.json(), file=sys.stderr)
            except Exception:
                print(e.response.text, file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
