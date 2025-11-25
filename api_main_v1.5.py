import json
import csv
import os
import time
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from collections import defaultdict
from dotenv import load_dotenv
import argparse

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

# DC certificate path (as in original script)
DC_VERIFY_PATH = "/opt/aws-tools.standardbank.co.za.pem"

# Common headers
headers = {"Accept": "application/json"}
cloud_auth = HTTPBasicAuth(cloud_email, cloud_api_token)
dc_auth = HTTPBasicAuth(dc_username_or_email, dc_api_token_or_password)

# =========================
# === HTTP helpers      ===
# =========================

def _request_with_retries(
    method: str,
    url: str,
    *,
    auth: HTTPBasicAuth,
    headers: dict,
    params: dict | None = None,
    timeout: int = 60,
    verify: str | bool | None = None,
    max_retries: int = 3,
    backoff_factor: int = 2,
) -> requests.Response:
    """
    Generic HTTP request helper with simple retries and backoff.
    Retries on network errors, 429, and 5xx responses.
    """
    last_exc = None
    last_resp = None

    for attempt in range(1, max_retries + 1):
        try:
            kwargs = {
                "method": method,
                "url": url,
                "headers": headers,
                "auth": auth,
                "params": params,
                "timeout": timeout,
            }
            if verify is not None:
                kwargs["verify"] = verify

            resp = requests.request(**kwargs)
            last_resp = resp

            # Retry on 429 or 5xx
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                print(
                    f"[HTTP] Server error {resp.status_code} on {url} "
                    f"(attempt {attempt}/{max_retries})."
                )
                if attempt == max_retries:
                    return resp
                time.sleep(backoff_factor * attempt)
                continue

            return resp

        except requests.exceptions.RequestException as e:
            last_exc = e
            print(
                f"[HTTP] Transport error on {url} "
                f"(attempt {attempt}/{max_retries}): {e}"
            )
            if attempt == max_retries:
                raise
            time.sleep(backoff_factor * attempt)

    if last_exc:
        raise last_exc
    return last_resp  # fallback, should not really get here


def dc_get(url: str, params: dict | None = None, timeout: int = 60) -> requests.Response:
    """GET helper for DC with retries & custom verify."""
    return _request_with_retries(
        "GET",
        url,
        auth=dc_auth,
        headers=headers,
        params=params,
        timeout=timeout,
        verify=DC_VERIFY_PATH,
    )


def cloud_get(url: str, params: dict | None = None, timeout: int = 60) -> requests.Response:
    """GET helper for Cloud with retries, using default cert store."""
    return _request_with_retries(
        "GET",
        url,
        auth=cloud_auth,
        headers=headers,
        params=params,
        timeout=timeout,
        verify=None,  # use system/requests default
    )

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
        resp = dc_get(url, timeout=60)
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

    raise RuntimeError(
        f"Failed to list projects from DC. Last status: "
        f"{last_resp.status_code if last_resp else 'N/A'}"
    )


def dc_search_issue_keys_for_project(
    project_id: str,
    project_key_for_logs: str,
    updated_since_days: int | None,
    page_size: int = 100,
):
    """
    Fetch ALL issue keys for a project from DC via /search pagination.
    Uses project *ID* in JQL to avoid 'value does not exist' errors with keys.
    Raises on HTTP failure instead of silently returning [].
    """
    jql = f"project = {project_id}"
    if isinstance(updated_since_days, int) and updated_since_days > 0:
        jql += f" AND updated >= -{updated_since_days}d"
    jql += " ORDER BY key ASC"

    keys = []
    start_at = 0

    while True:
        params = {
            "jql": jql,
            "startAt": start_at,
            "maxResults": page_size,
            "fields": "key",
        }
        url = f"{dc_base_url}/rest/api/2/search"
        resp = dc_get(url, params=params, timeout=90)

        if resp.status_code != 200:
            raise RuntimeError(
                f"[{project_key_for_logs}] DC search failed: "
                f"{resp.status_code} {resp.text[:300]}..."
            )

        payload = resp.json()
        issues = payload.get("issues", []) or []
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
    Also provides the DC issue id and the history (changegroup) id
    as changegroup_Id (and also as changeitem_Id for compatibility).
    """
    resp = None
    last_status = None
    last_text = None

    for ver in ("2", "3"):
        url = f"{dc_base_url}/rest/api/{ver}/issue/{issue_key}"
        params = {"expand": "changelog", "fields": "summary"}
        r = dc_get(url, params=params, timeout=90)
        resp = r
        if r.status_code == 200:
            break
        last_status = r.status_code
        last_text = r.text[:300]

    if resp is None or resp.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch DC issue (with changelog) for {issue_key}: "
            f"{last_status if last_status is not None else 'N/A'} "
            f"{(last_text or '')}"
        )

    data = resp.json()
    dc_issue_id = str(data.get("id") or "")
    changelog = data.get("changelog", {}) or {}
    histories = changelog.get("histories", []) or []
    total = changelog.get("total")

    if total is not None and total > len(histories):
        print(
            f"Warning: DC changelog for {issue_key} may be truncated "
            f"({len(histories)} of {total}). Consider using --updated-since-days to narrow the window."
        )

    for history in histories:
        author_obj = history.get("author", {}) or {}
        author = (
            author_obj.get("displayName")
            or author_obj.get("name")
            or author_obj.get("emailAddress")
            or "Unknown"
        )
        created = history.get("created", "")
        history_id = str(history.get("id") or "")  # this is the changegroup.id

        for item in history.get("items", []):
            if item.get("field") == "status":
                yield {
                    "issueKey": issue_key,
                    "dcIssueId": dc_issue_id,
                    "fromStatus": {
                        "name": item.get("fromString"),
                        "id": item.get("from"),
                    },
                    "toStatus": {
                        "name": item.get("toString"),
                        "id": item.get("to"),
                    },
                    "author": author,
                    "timestamp": created,
                    "changegroup_Id": history_id,  # NEW: changegroup id
                    "changeitem_Id": history_id,   # kept for backward compatibility
                }

# =========================
# === Cloud helpers     ===
# =========================

def cloud_project_exists(project_key: str, project_id: str) -> bool:
    """
    Check whether project exists in Cloud.
    Tries key first, then ID.
    """
    for identifier in (project_key, project_id):
        url = f"{cloud_base_url}/rest/api/3/project/{identifier}"
        try:
            resp = cloud_get(url, timeout=60)
        except requests.exceptions.RequestException as e:
            print(f"[{project_key}] Cloud project check transport error: {e}")
            return False

        if resp.status_code == 200:
            return True
        if resp.status_code == 404:
            continue  # try next identifier

        print(
            f"[{project_key}] Cloud project check returned "
            f"{resp.status_code}: {resp.text[:200]}"
        )
        return False

    print(f"[{project_key}] Project not found in Cloud by key or ID.")
    return False


def cloud_fetch_status_pairs(issue_key: str) -> set[tuple[str | None, str | None]]:
    """
    Return set of (from_name, to_name) pairs recorded in Jira Cloud for an issue.
    Uses proper pagination (startAt/total).
    Raises on HTTP error; caller can choose to skip the issue.
    """
    url = f"{cloud_base_url}/rest/api/3/issue/{issue_key}/changelog"
    start_at = 0
    max_results = 100
    pairs: set[tuple[str | None, str | None]] = set()

    while True:
        params = {"startAt": start_at, "maxResults": max_results}
        resp = cloud_get(url, params=params, timeout=90)

        if resp.status_code != 200:
            raise RuntimeError(
                f"Cloud changelog fetch for {issue_key} returned "
                f"{resp.status_code}: {resp.text[:300]}"
            )

        payload = resp.json()
        histories = payload.get("values") or payload.get("histories") or []

        for hist in histories:
            for item in hist.get("items", []):
                if item.get("field") == "status":
                    pairs.add(
                        (item.get("fromString"), item.get("toString"))
                    )

        total = payload.get("total")
        start_at += len(histories)

        # Prefer 'total' if present; fall back to 'isLast' / empty-page behaviour.
        if total is not None:
            if start_at >= total:
                break
        else:
            # If no total, but we got an empty page or isLast=True, stop.
            if not histories or payload.get("isLast", True):
                break

    return pairs


def cloud_get_issue_id(issue_key: str) -> str:
    """
    Return the Jira Cloud issue id for a given issue key.
    Raises on HTTP error so caller can decide to skip.
    """
    url = f"{cloud_base_url}/rest/api/3/issue/{issue_key}"
    params = {"fields": "id"}  # minimal payload

    resp = cloud_get(url, params=params, timeout=60)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Cloud issue id fetch for {issue_key} returned "
            f"{resp.status_code}: {resp.text[:300]}"
        )

    data = resp.json()
    return str(data.get("id") or "")

# =========================
# === Per-project run   ===
# =========================

def process_project(project_key: str, project_id: str, updated_since_days: int | None):
    print(f"\n=== Processing project: {project_key} (ID {project_id}) ===")

    try:
        if not cloud_project_exists(project_key, project_id):
            write_csv(project_key, [])
            print(
                f"[{project_key}] ❎ Skipped. Project not found in Cloud. "
                f"(Wrote empty CSV with headers.)"
            )
            return

        issue_keys = dc_search_issue_keys_for_project(
            project_id, project_key, updated_since_days
        )

        if not issue_keys:
            print(
                f"[{project_key}] No issues found. "
                f"Writing empty CSV with headers."
            )
            write_csv(project_key, [])
            return

        print(
            f"[{project_key}] Fetching DC changelogs for "
            f"{len(issue_keys)} issue(s)..."
        )
        dc_transitions_by_issue: dict[str, list[dict]] = defaultdict(list)
        dc_issue_id_map: dict[str, str] = {}

        for key in tqdm(issue_keys, desc=f"DC changelogs {project_key}"):
            try:
                for change in dc_iter_status_changes(key):
                    if (
                        change["fromStatus"]["name"]
                        and change["toStatus"]["name"]
                    ):
                        dc_transitions_by_issue[key].append(change)
                        if key not in dc_issue_id_map:
                            dc_issue_id_map[key] = change.get("dcIssueId", "")
            except Exception as e:
                print(
                    f"[{project_key}] Error fetching DC changelog "
                    f"for {key}: {e}. Skipping this issue."
                )
                continue

        if not dc_transitions_by_issue:
            print(
                f"[{project_key}] No DC status transitions found. "
                f"Writing empty CSV with headers."
            )
            write_csv(project_key, [])
            return

        print(f"[{project_key}] Fetching Cloud changelogs...")
        cloud_cache: dict[str, set[tuple[str | None, str | None]] | None] = {}
        cloud_issue_id_map: dict[str, str] = {}

        for key in tqdm(
            dc_transitions_by_issue.keys(), desc=f"Cloud changelogs {project_key}"
        ):
            try:
                pairs = cloud_fetch_status_pairs(key)
                cloud_cache[key] = pairs
                cloud_issue_id_map[key] = cloud_get_issue_id(key)
            except Exception as e:
                # If Cloud fails for a given issue, skip it from validation
                print(
                    f"[{project_key}] Cloud error for {key}: {e}. "
                    f"Skipping this issue in comparison."
                )
                cloud_cache[key] = None
                cloud_issue_id_map[key] = ""

        print(f"[{project_key}] Comparing transitions...")
        missing_rows = []

        for key, entries in tqdm(
            dc_transitions_by_issue.items(), desc=f"Validating {project_key}"
        ):
            cloud_pairs = cloud_cache.get(key)

            # If we had a Cloud error for this issue, don't treat all transitions
            # as missing; just skip this issue from comparison.
            if cloud_pairs is None:
                continue

            for entry in entries:
                f_name = entry["fromStatus"]["name"]
                f_id = entry["fromStatus"]["id"]
                t_name = entry["toStatus"]["name"]
                t_id = entry["toStatus"]["id"]
                transition_str = f"{f_name} \u2192 {t_name}"

                row = {
                    "issueKey": key,
                    "dc_issue_id": dc_issue_id_map.get(key, ""),
                    "cloud_issue_id": cloud_issue_id_map.get(key, ""),
                    "transition": transition_str,
                    "from_status": f"{f_name} ({f_id})",
                    "to_status": f"{t_name} ({t_id})",
                    "author": entry.get("author", "Unknown"),
                    "timestamp": entry.get("timestamp", ""),
                    "changegroup_Id": entry.get("changegroup_Id", ""),
                    "changeitem_Id": entry.get("changeitem_Id", ""),
                }

                if (f_name, t_name) not in cloud_pairs:
                    missing_rows.append(row)

        write_csv(project_key, missing_rows)
        print(
            f"[{project_key}] ✅ Done. Missing transitions: "
            f"{len(missing_rows)}"
        )

    except Exception as e:
        # Fail per-project but still produce a CSV with just headers
        print(f"[{project_key}] ❌ Error processing project: {e}")
        write_csv(project_key, [])


def write_csv(project_key: str, rows: list[dict]):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{project_key}.csv")
    fieldnames = [
        "issueKey",
        "dc_issue_id",
        "cloud_issue_id",
        "transition",
        "from_status",
        "to_status",
        "author",
        "timestamp",
        "changegroup_Id",  # NEW column
        "changeitem_Id",
    ]

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
    parser = argparse.ArgumentParser(
        description="Compare Jira DC vs Cloud status transitions, per-project."
    )
    parser.add_argument(
        "--only",
        nargs="+",
        help="Limit to these project KEYS (space-separated).",
    )
    parser.add_argument(
        "--resume-from",
        dest="resume_from",
        help="Project KEY to resume from (inclusive).",
    )
    parser.add_argument(
        "--updated-since-days",
        type=int,
        default=None,
        help="Only include issues updated in the last N days (reduces payload).",
    )
    args = parser.parse_args()

    print("Discovering projects from Jira DC...")
    all_projects = dc_list_projects()

    if args.only:
        wanted = {k.upper() for k in args.only}
        projects = [p for p in all_projects if p["key"].upper() in wanted]
        if not projects:
            print(f"No matching projects for --only {args.only}. Exiting.")
            return
    else:
        projects = all_projects

    start_index = 0
    if args.resume_from:
        resume_key = args.resume_from.upper()
        for i, p in enumerate(projects):
            if p["key"].upper() == resume_key:
                start_index = i
                break
        else:
            print(
                f"Resume key '{args.resume_from}' not found in project list. "
                f"Proceeding from the beginning."
            )

    for p in projects[start_index:]:
        process_project(
            project_key=p["key"],
            project_id=p["id"],
            updated_since_days=args.updated_since_days,
        )


if __name__ == "__main__":
    main()
