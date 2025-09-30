import json
import csv
import os
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from collections import defaultdict
from dotenv import load_dotenv
import argparse  # <-- needed

# =========================
# === Configurable bits ===
# =========================
load_dotenv()
# ---- Jira Cloud (target to validate) ----
cloud_email = os.getenv("CLOUD_EMAIL")
cloud_api_token = os.getenv("CLOUD_API_TOKEN")
cloud_base_url = os.getenv("CLOUD_BASE_URL")

# ---- Jira Data Center (source of truth) ----
dc_username_or_email = os.getenv("DC_USERNAME")
dc_api_token_or_password = os.getenv("DC_API_TOKEN")
dc_base_url = os.getenv("DC_BASE_URL")

# Where to put per-project CSVs
output_dir = os.getenv("OUTPUT_DIR", "project_csvs")

# Common headers
headers = {"Accept": "application/json"}
cloud_auth = HTTPBasicAuth(cloud_email, cloud_api_token)
dc_auth = HTTPBasicAuth(dc_username_or_email, dc_api_token_or_password)

# =========================
# === DC helpers        ===
# =========================

def dc_list_projects():
    """
    Return a list of project dicts: [{'key': 'EMPLOY', 'id': '10010', 'name': '...'}, ...]
    """
    last_resp = None
    for ver in ("2", "3"):
        url = f"{dc_base_url}/rest/api/{ver}/project"
        print(url)
        resp = requests.get(url, headers=headers, auth=dc_auth, timeout=60, verify='opt/aws-tools.standardbank.pem')
        print(resp)
        last_resp = resp
        if resp.status_code == 200:
            projects = resp.json()
            out = []
            for p in projects:
                key = p.get("key")
                pid = str(p.get("id") or "")
                name = p.get("name") or key
                if key and pid:
                    out.append({"key": key, "id": pid, "name": name})
            return out
    raise RuntimeError(f"Failed to list projects from DC. Last status: {last_resp.status_code if last_resp else 'N/A'}")

def dc_search_issue_keys_for_project(project_id: str, project_key_for_logs: str, updated_since_days: int | None, page_size: int = 100):
    """
    Fetch ALL issue keys for a project from DC via /search pagination.
    Uses project *ID* in JQL to avoid 'value does not exist' errors with keys.
    """
    jql = f"project = {project_id}"
    if isinstance(updated_since_days, int) and updated_since_days > 0:
        jql += f" AND updated >= -{updated_since_days}d"
    jql += " ORDER BY key ASC"

    keys = []
    start_at = 0
    while True:
        params = {"jql": jql, "startAt": start_at, "maxResults": page_size, "fields": "key"}
        url = f"{dc_base_url}/rest/api/2/search"
        resp = requests.get(url, headers=headers, auth=dc_auth, params=params, timeout=90, verify='opt/aws-tools.standardbank.pem')
        if resp.status_code != 200:
            # Be lenient: log and skip the project instead of crashing the whole run
            print(f"[{project_key_for_logs}] DC search failed: {resp.status_code} {resp.text[:300]}... Skipping.")
            return []

        payload = resp.json()
        issues = payload.get("issues", [])
        if not issues:
            break

        keys.extend([i["key"] for i in issues])
        total = payload.get("total", 0)
        start_at += len(issues)
        if start_at >= total:
            break

    return keys

def dc_iter_status_changes(issue_key: str):
    """
    Yield status-change events for a DC issue using expand=changelog.
    """
    resp = None
    for ver in ("2", "3"):
        url = f"{dc_base_url}/rest/api/{ver}/issue/{issue_key}"
        params = {"expand": "changelog", "fields": "summary"}
        resp = requests.get(url, headers=headers, auth=dc_auth, params=params, timeout=90, verify='opt/aws-tools.standardbank.pem')
        if resp.status_code == 200:
            break
    if resp is None or resp.status_code != 200:
        print(f"Failed to fetch DC issue (with changelog) for {issue_key}: {resp.status_code if resp else 'N/A'}")
        return

    data = resp.json()
    changelog = data.get("changelog", {}) or {}
    histories = changelog.get("histories", []) or []
    total = changelog.get("total")
    if total is not None and total > len(histories):
        print(f"Warning: DC changelog for {issue_key} may be truncated "
              f"({len(histories)} of {total}). Consider using --updated-since-days to narrow the window.")

    for history in histories:
        author_obj = history.get("author", {}) or {}
        author = (author_obj.get("displayName")
                  or author_obj.get("name")
                  or author_obj.get("emailAddress")
                  or "Unknown")
        created = history.get("created", "")

        for item in history.get("items", []):
            if item.get("field") == "status":
                yield {
                    "issueKey": issue_key,
                    "fromStatus": {"name": item.get("fromString"), "id": item.get("from")},
                    "toStatus": {"name": item.get("toString"), "id": item.get("to")},
                    "author": author,
                    "timestamp": created,
                }

# =========================
# === Cloud helper      ===
# =========================

def cloud_fetch_status_pairs(issue_key: str):
    """
    Return set of (from_name, to_name) pairs recorded in Jira Cloud for an issue.
    """
    url = f"{cloud_base_url}/rest/api/3/issue/{issue_key}/changelog"
    start_at = 0
    max_results = 100
    pairs = set()

    while True:
        params = {"startAt": start_at, "maxResults": max_results}
        resp = requests.get(url, headers=headers, auth=cloud_auth, params=params, timeout=90, verify='opt/aws-tools.standardbank.pem')
        if resp.status_code != 200:
            print(f"Cloud changelog fetch for {issue_key} returned {resp.status_code}. Treating as empty.")
            return pairs

        payload = resp.json()
        histories = payload.get("values", []) or payload.get("histories", [])

        for hist in histories:
            for item in hist.get("items", []):
                if item.get("field") == "status":
                    pairs.add((item.get("fromString"), item.get("toString")))

        if payload.get("isLast", True):
            break
        start_at += max_results

    return pairs

# =========================
# === Per-project run   ===
# =========================

def process_project(project_key: str, project_id: str, updated_since_days: int | None):
    """
    For a single project:
    - fetch all DC issue keys
    - gather DC status changes
    - gather Cloud pairs
    - write <project_key>.csv (fresh each run)
    """
    print(f"\n=== Processing project: {project_key} (ID {project_id}) ===")

    issue_keys = dc_search_issue_keys_for_project(project_id, project_key, updated_since_days)
    if not issue_keys:
        print(f"[{project_key}] No issues found or search failed. Writing empty CSV with headers.")
        write_csv(project_key, [])
        return

    # Build DC transitions
    print(f"[{project_key}] Fetching DC changelogs for {len(issue_keys)} issue(s)...")
    dc_transitions_by_issue = defaultdict(list)
    for key in tqdm(issue_keys, desc=f"DC changelogs {project_key}"):
        for change in dc_iter_status_changes(key):
            if change["fromStatus"]["name"] and change["toStatus"]["name"]:
                dc_transitions_by_issue[key].append(change)

    if not dc_transitions_by_issue:
        print(f"[{project_key}] No DC status transitions found. Writing empty CSV with headers.")
        write_csv(project_key, [])
        return

    # Cloud cache
    print(f"[{project_key}] Fetching Cloud changelogs...")
    cloud_cache = {}
    for key in tqdm(dc_transitions_by_issue.keys(), desc=f"Cloud changelogs {project_key}"):
        cloud_cache[key] = cloud_fetch_status_pairs(key)

    # Compare
    print(f"[{project_key}] Comparing transitions...")
    missing_rows = []
    for key, entries in tqdm(dc_transitions_by_issue.items(), desc=f"Validating {project_key}"):
        cloud_pairs = cloud_cache.get(key, set())
        for entry in entries:
            f_name = entry["fromStatus"]["name"]
            f_id = entry["fromStatus"]["id"]
            t_name = entry["toStatus"]["name"]
            t_id = entry["toStatus"]["id"]
            transition_str = f"{f_name} \u2192 {t_name}"
            row = {
                "issueKey": key,
                "transition": transition_str,
                "from_status": f"{f_name} ({f_id})",
                "to_status": f"{t_name} ({t_id})",
                "author": entry.get("author", "Unknown"),
                "timestamp": entry.get("timestamp", ""),
            }
            if (f_name, t_name) not in cloud_pairs:
                missing_rows.append(row)

    write_csv(project_key, missing_rows)
    print(f"[{project_key}] ✅ Done. Missing transitions: {len(missing_rows)}")

def write_csv(project_key: str, rows: list):
    """
    Always (re)write a fresh CSV per project:
    ./project_csvs/<PROJECT_KEY>.csv
    """
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{project_key}.csv")
    fieldnames = ["issueKey", "transition", "from_status", "to_status", "author", "timestamp"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)
    print(f"[{project_key}] Wrote CSV to {path} ({len(rows)} row(s))")

# =========================
# === Main              ===
# =========================

def main():
    parser = argparse.ArgumentParser(description="Compare Jira DC vs Cloud status transitions, per-project.")
    parser.add_argument("--only", nargs="+", help="Limit to these project KEYS (space-separated).")
    parser.add_argument("--resume-from", dest="resume_from", help="Project KEY to resume from (inclusive).")
    parser.add_argument("--updated-since-days", type=int, default=None,
                        help="Only include issues updated in the last N days (reduces payload).")
    args = parser.parse_args()

    # Discover projects
    print("Discovering projects from Jira DC...")
    all_projects = dc_list_projects()  # list of dicts with key/id/name

    if args.only:
        wanted = {k.upper() for k in args.only}
        projects = [p for p in all_projects if p["key"].upper() in wanted]
        if not projects:
            print(f"No matching projects for --only {args.only}. Exiting.")
            return
    else:
        projects = all_projects

    # Case-insensitive resume logic
    start_index = 0
    if args.resume_from:
        resume_key = args.resume_from.upper()
        for i, p in enumerate(projects):
            if p["key"].upper() == resume_key:
                start_index = i
                break
        else:
            print(f"Resume key '{args.resume_from}' not found in project list. "
                  f"Proceeding from the beginning.")

    # Run from start_index onward
    for p in projects[start_index:]:
        process_project(project_key=p["key"], project_id=p["id"], updated_since_days=args.updated_since_days)

if __name__ == "__main__":
    main()