import json
import csv
import os
import time
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from collections import defaultdict
from datetime import datetime, timezone
from dotenv import load_dotenv
import argparse
from typing import Optional, Union

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
    params: Optional[dict] = None,
    timeout: int = 60,
    verify: Optional[Union[str, bool]] = None,
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


def dc_get(url: str, params: Optional[dict] = None, timeout: int = 60) -> requests.Response:
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


def cloud_get(url: str, params: Optional[dict] = None, timeout: int = 60) -> requests.Response:
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
# === Compare helpers   ===
# =========================

def _clean_text(value) -> str:
    """Normalize values used in transition matching."""
    if value is None:
        return ""
    return str(value).strip()


def _unique_nonempty(values) -> list[str]:
    out = []
    seen = set()
    for value in values:
        text = _clean_text(value)
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def _jira_timestamp_key(value) -> str:
    """
    Normalize Jira timestamps enough to match DC and Cloud values.
    Falls back to the original text if parsing fails.
    """
    text = _clean_text(value)
    if not text:
        return ""

    normalized = text.replace("Z", "+00:00")
    if (
        len(normalized) >= 5
        and normalized[-5] in "+-"
        and normalized[-4:-2].isdigit()
        and normalized[-2:].isdigit()
        and normalized[-3] != ":"
    ):
        normalized = f"{normalized[:-2]}:{normalized[-2:]}"

    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return text

    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat()


def _history_sort_key(history: dict) -> tuple[str, Union[int, str]]:
    history_id = _clean_text(history.get("id"))
    sortable_id: Union[int, str] = int(history_id) if history_id.isdigit() else history_id
    return (_jira_timestamp_key(history.get("created")), sortable_id)


def _transition_pair(entry: dict) -> tuple[str, str]:
    return (
        _clean_text((entry.get("fromStatus") or {}).get("name")),
        _clean_text((entry.get("toStatus") or {}).get("name")),
    )


def _index_cloud_transitions(cloud_changes: list[dict]) -> dict[tuple[str, str], list[dict]]:
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for change in sorted(
        cloud_changes,
        key=lambda c: (_jira_timestamp_key(c.get("timestamp")), _clean_text(c.get("cloud_changegroup_Id"))),
    ):
        pair = _transition_pair(change)
        if pair[0] and pair[1]:
            buckets[pair].append(change)
    return buckets


def _consume_cloud_transition_match(
    dc_entry: dict,
    cloud_buckets: dict[tuple[str, str], list[dict]],
) -> Optional[dict]:
    """
    Consume one matching Cloud transition for a DC transition.
    Timestamp matches are preferred, but matching falls back to the same status pair
    so formatting differences do not create false positives.
    """
    pair = _transition_pair(dc_entry)
    bucket = cloud_buckets.get(pair)
    if not bucket:
        return None

    dc_ts = _jira_timestamp_key(dc_entry.get("timestamp"))
    if dc_ts:
        for index, cloud_entry in enumerate(bucket):
            if _jira_timestamp_key(cloud_entry.get("timestamp")) == dc_ts:
                return bucket.pop(index)

    return bucket.pop(0)

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
    updated_since_days: Optional[int],
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


def dc_get_project_key_by_id(project_id: str, cache: dict[str, str]) -> str:
    """Resolve a DC project ID to a project key for move reporting."""
    project_id = _clean_text(project_id)
    if not project_id:
        return ""
    if project_id in cache:
        return cache[project_id]

    last_resp = None
    for ver in ("2", "3"):
        url = f"{dc_base_url}/rest/api/{ver}/project/{project_id}"
        resp = dc_get(url, timeout=60)
        last_resp = resp
        if resp.status_code == 200:
            key = _clean_text((resp.json() or {}).get("key"))
            cache[project_id] = key
            return key
        if resp.status_code == 404:
            continue

    print(
        f"[DC] Could not resolve project id {project_id} to a key. "
        f"Last status: {last_resp.status_code if last_resp else 'N/A'}"
    )
    cache[project_id] = ""
    return ""


def dc_fetch_full_changelog_histories(
    issue_key: str,
    initial_histories: list[dict],
    initial_total: Optional[int],
) -> tuple[list[dict], bool]:
    """
    Return the full DC changelog where the paginated changelog endpoint exists.
    The boolean is True when we had to fall back to a truncated expand result.
    """
    if initial_total is None or initial_total <= len(initial_histories):
        return initial_histories, False

    max_results = 100
    last_error = ""

    for ver in ("2", "3"):
        all_histories = []
        start_at = 0

        while True:
            url = f"{dc_base_url}/rest/api/{ver}/issue/{issue_key}/changelog"
            params = {"startAt": start_at, "maxResults": max_results}
            resp = dc_get(url, params=params, timeout=90)

            if resp.status_code == 404:
                last_error = f"v{ver} returned 404"
                break
            if resp.status_code != 200:
                last_error = f"v{ver} returned {resp.status_code}: {resp.text[:200]}"
                break

            payload = resp.json() or {}
            histories = payload.get("values") or payload.get("histories") or []
            all_histories.extend(histories)

            total = payload.get("total")
            start_at += len(histories)

            if not histories:
                if all_histories:
                    return all_histories, False
                last_error = f"v{ver} returned an empty changelog page"
                break
            if total is not None and start_at >= total:
                return all_histories, False
            if total is None and (payload.get("isLast") is True or len(histories) < max_results):
                return all_histories, False

    print(
        f"Warning: DC changelog for {issue_key} may be truncated "
        f"({len(initial_histories)} of {initial_total}). "
        f"Could not page full changelog: {last_error or 'unknown error'}"
    )
    return initial_histories, True


def dc_fetch_issue_meta_and_status_changes(issue_key: str, project_id_to_key_cache: dict[str, str]):
    """
    Fetch DC issue metadata, all available status changes, and every key seen in
    the changelog so Cloud lookup can handle moved issues.
    """
    resp = None
    last_status = None
    last_text = None

    for ver in ("2", "3"):
        url = f"{dc_base_url}/rest/api/{ver}/issue/{issue_key}"
        params = {"expand": "changelog", "fields": "summary,project"}
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

    data = resp.json() or {}
    dc_issue_id = _clean_text(data.get("id"))
    current_key = _clean_text(data.get("key")) or issue_key

    fields = data.get("fields", {}) or {}
    current_project = fields.get("project", {}) or {}
    current_project_id = _clean_text(current_project.get("id"))
    current_project_key = _clean_text(current_project.get("key"))
    if not current_project_key and current_project_id:
        current_project_key = dc_get_project_key_by_id(
            current_project_id,
            project_id_to_key_cache,
        )

    changelog = data.get("changelog", {}) or {}
    initial_histories = changelog.get("histories", []) or []
    total = changelog.get("total")
    histories, dc_changelog_truncated = dc_fetch_full_changelog_histories(
        issue_key,
        initial_histories,
        total,
    )
    histories = sorted(histories, key=_history_sort_key)

    key_from_values = []
    key_to_values = []
    project_from_ids = []
    project_to_ids = []
    status_changes = []

    for history in histories:
        author_obj = history.get("author", {}) or {}
        author = (
            author_obj.get("displayName")
            or author_obj.get("name")
            or author_obj.get("emailAddress")
            or "Unknown"
        )
        created = history.get("created", "")
        history_id = _clean_text(history.get("id"))

        for item in history.get("items", []):
            field = item.get("field")

            if field == "key":
                from_key = _clean_text(item.get("fromString"))
                to_key = _clean_text(item.get("toString"))
                if from_key:
                    key_from_values.append(from_key)
                if to_key:
                    key_to_values.append(to_key)

            if field == "project":
                from_project_id = _clean_text(item.get("from"))
                to_project_id = _clean_text(item.get("to"))
                if from_project_id:
                    project_from_ids.append(from_project_id)
                if to_project_id:
                    project_to_ids.append(to_project_id)

            if field == "status":
                status_changes.append(
                    {
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
                        "changegroup_Id": history_id,
                        "changeitem_Id": history_id,
                    }
                )

    old_project_id = project_from_ids[0] if project_from_ids else current_project_id
    new_project_id = project_to_ids[-1] if project_to_ids else current_project_id
    old_project_key = (
        dc_get_project_key_by_id(old_project_id, project_id_to_key_cache)
        if old_project_id
        else current_project_key
    )
    new_project_key = (
        dc_get_project_key_by_id(new_project_id, project_id_to_key_cache)
        if new_project_id
        else current_project_key
    )

    candidate_keys = _unique_nonempty(
        [current_key, issue_key]
        + list(reversed(key_to_values))
        + list(reversed(key_from_values))
    )

    meta = {
        "dc_issue_id": dc_issue_id,
        "dc_issue_key_old": key_from_values[0] if key_from_values else current_key,
        "dc_issue_key_new": key_to_values[-1] if key_to_values else current_key,
        "dc_issue_key_current": current_key,
        "dc_issue_key_candidates": "|".join(candidate_keys),
        "dc_project_id_old": old_project_id,
        "dc_project_id_new": new_project_id,
        "dc_project_key_old": old_project_key,
        "dc_project_key_new": new_project_key,
        "dc_project_key_current": current_project_key,
        "dc_changelog_truncated": "true" if dc_changelog_truncated else "",
    }

    return meta, status_changes


def dc_iter_status_changes(issue_key: str):
    """Compatibility wrapper for older callers."""
    meta, changes = dc_fetch_issue_meta_and_status_changes(issue_key, {})
    for change in changes:
        change.setdefault("dcIssueId", meta.get("dc_issue_id", ""))
        yield change

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


def cloud_get_issue_identity(issue_id_or_key: str) -> dict:
    """
    Return Cloud issue identity for a key or ID.
    The returned key may differ from the lookup key when Jira resolves a moved issue.
    """
    url = f"{cloud_base_url}/rest/api/3/issue/{issue_id_or_key}"
    params = {"fields": "project"}

    resp = cloud_get(url, params=params, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Cloud issue lookup for {issue_id_or_key} returned "
            f"{resp.status_code}: {resp.text[:300]}"
        )

    data = resp.json() or {}
    fields = data.get("fields", {}) or {}
    project = fields.get("project", {}) or {}
    return {
        "id": _clean_text(data.get("id")),
        "key": _clean_text(data.get("key")),
        "projectKey": _clean_text(project.get("key")),
    }


def cloud_search_issue_identity_by_key(issue_key: str) -> dict:
    """Fallback lookup for historical keys using Cloud's enhanced JQL endpoint."""
    jql = f"issuekey = {issue_key}"
    last_error = ""

    for ver in ("3", "2"):
        url = f"{cloud_base_url}/rest/api/{ver}/search/jql"
        params = {"jql": jql, "fields": "project", "maxResults": 1}
        resp = cloud_get(url, params=params, timeout=60)
        if resp.status_code != 200:
            last_error = f"v{ver} returned {resp.status_code}: {resp.text[:200]}"
            continue

        issues = (resp.json() or {}).get("issues") or []
        if not issues:
            raise RuntimeError(f"Cloud JQL found no issue for {issue_key}")

        issue = issues[0]
        fields = issue.get("fields", {}) or {}
        project = fields.get("project", {}) or {}
        return {
            "id": _clean_text(issue.get("id")),
            "key": _clean_text(issue.get("key")),
            "projectKey": _clean_text(project.get("key")),
        }

    raise RuntimeError(last_error or f"Cloud JQL lookup failed for {issue_key}")


def cloud_fetch_status_changes(issue_id_or_key: str) -> list[dict]:
    """
    Return every status-change event recorded in Jira Cloud for an issue.
    Uses proper pagination (startAt/total).
    Raises on HTTP error; caller can decide how to report the issue.
    """
    url = f"{cloud_base_url}/rest/api/3/issue/{issue_id_or_key}/changelog"
    start_at = 0
    max_results = 100
    changes: list[dict] = []

    while True:
        params = {"startAt": start_at, "maxResults": max_results}
        resp = cloud_get(url, params=params, timeout=90)

        if resp.status_code != 200:
            raise RuntimeError(
                f"Cloud changelog fetch for {issue_id_or_key} returned "
                f"{resp.status_code}: {resp.text[:300]}"
            )

        payload = resp.json()
        histories = payload.get("values") or payload.get("histories") or []

        for hist in histories:
            author_obj = hist.get("author", {}) or {}
            author = (
                author_obj.get("displayName")
                or author_obj.get("name")
                or author_obj.get("emailAddress")
                or "Unknown"
            )
            created = hist.get("created", "")
            history_id = _clean_text(hist.get("id"))

            for item in hist.get("items", []):
                if item.get("field") == "status":
                    changes.append(
                        {
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
                            "cloud_changegroup_Id": history_id,
                        }
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

    return sorted(
        changes,
        key=lambda c: (_jira_timestamp_key(c.get("timestamp")), _clean_text(c.get("cloud_changegroup_Id"))),
    )


def cloud_resolve_issue_and_changelog(candidate_keys: list[str]) -> tuple[dict, Optional[list[dict]], str, str, list[str], str]:
    """
    Try each known DC key as a Cloud current or historical key.
    Returns identity, changelog, candidate used, lookup method, tried labels, error.
    """
    tried = []
    errors = []
    seen_issue_ids = set()

    for candidate in _unique_nonempty(candidate_keys):
        for method, resolver in (
            ("direct", cloud_get_issue_identity),
            ("jql", cloud_search_issue_identity_by_key),
        ):
            label = f"{method}:{candidate}"
            tried.append(label)
            try:
                identity = resolver(candidate)
                cloud_issue_id = identity.get("id") or candidate
                if cloud_issue_id in seen_issue_ids:
                    continue
                seen_issue_ids.add(cloud_issue_id)

                changes = cloud_fetch_status_changes(cloud_issue_id)
                return identity, changes, candidate, method, tried, ""
            except Exception as e:
                errors.append(f"{label} -> {e}")

    return {}, None, "", "", tried, "; ".join(errors[-3:])


def cloud_fetch_status_pairs(issue_key: str) -> set[tuple[Optional[str], Optional[str]]]:
    """Compatibility wrapper for older callers."""
    return {
        _transition_pair(change)
        for change in cloud_fetch_status_changes(issue_key)
    }


def cloud_get_issue_id(issue_key: str) -> str:
    """
    Return the Jira Cloud issue id for a given issue key.
    Raises on HTTP error so caller can decide to skip.
    """
    return cloud_get_issue_identity(issue_key).get("id", "")


def build_missing_row(
    *,
    dc_key: str,
    entry: dict,
    meta: dict,
    cloud_identity: dict,
    cloud_candidate_used: str,
    cloud_lookup_method: str,
    cloud_lookup_error: str,
    comparison_status: str,
) -> dict:
    f_name = _clean_text(entry["fromStatus"]["name"])
    f_id = _clean_text(entry["fromStatus"]["id"])
    t_name = _clean_text(entry["toStatus"]["name"])
    t_id = _clean_text(entry["toStatus"]["id"])
    transition_str = f"{f_name} \u2192 {t_name}"

    return {
        "issueKey": dc_key,
        "dc_issue_id": meta.get("dc_issue_id", ""),
        "cloud_issue_id": cloud_identity.get("id", ""),
        "cloud_issue_key_used": cloud_identity.get("key", ""),
        "cloud_issue_project_key": cloud_identity.get("projectKey", ""),
        "cloud_lookup_candidate": cloud_candidate_used,
        "cloud_lookup_method": cloud_lookup_method,
        "cloud_lookup_error": cloud_lookup_error,
        "comparison_status": comparison_status,
        "dc_issue_key_old": meta.get("dc_issue_key_old", ""),
        "dc_issue_key_new": meta.get("dc_issue_key_new", ""),
        "dc_issue_key_current": meta.get("dc_issue_key_current", ""),
        "dc_issue_key_candidates": meta.get("dc_issue_key_candidates", ""),
        "dc_project_key_old": meta.get("dc_project_key_old", ""),
        "dc_project_key_new": meta.get("dc_project_key_new", ""),
        "dc_project_key_current": meta.get("dc_project_key_current", ""),
        "dc_changelog_truncated": meta.get("dc_changelog_truncated", ""),
        "transition": transition_str,
        "from_status": f"{f_name} ({f_id})",
        "to_status": f"{t_name} ({t_id})",
        "author": entry.get("author", "Unknown"),
        "timestamp": entry.get("timestamp", ""),
        "changegroup_Id": entry.get("changegroup_Id", ""),
        "changeitem_Id": entry.get("changeitem_Id", ""),
    }

# =========================
# === Per-project run   ===
# =========================

def process_project(project_key: str, project_id: str, updated_since_days: Optional[int]):
    print(f"\n=== Processing project: {project_key} (ID {project_id}) ===")

    try:
        if not cloud_project_exists(project_key, project_id):
            print(
                f"[{project_key}] Cloud project was not found by source key/ID. "
                f"Continuing with issue-level lookup because issues may have moved."
            )

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
        dc_issue_move_meta: dict[str, dict] = {}
        project_id_to_key_cache: dict[str, str] = {}

        for key in tqdm(issue_keys, desc=f"DC changelogs {project_key}"):
            try:
                meta, status_changes = dc_fetch_issue_meta_and_status_changes(
                    key,
                    project_id_to_key_cache,
                )
                dc_issue_move_meta[key] = meta
                dc_issue_id_map[key] = meta.get("dc_issue_id", "")

                for change in status_changes:
                    if (
                        _clean_text(change["fromStatus"]["name"])
                        and _clean_text(change["toStatus"]["name"])
                    ):
                        dc_transitions_by_issue[key].append(change)
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
        cloud_cache: dict[str, Optional[list[dict]]] = {}
        cloud_issue_identity_map: dict[str, dict] = {}
        cloud_candidate_used: dict[str, str] = {}
        cloud_lookup_method: dict[str, str] = {}
        cloud_lookup_error: dict[str, str] = {}

        for key in tqdm(
            dc_transitions_by_issue.keys(), desc=f"Cloud changelogs {project_key}"
        ):
            meta = dc_issue_move_meta.get(key, {}) or {}
            candidates = (meta.get("dc_issue_key_candidates") or key).split("|")
            identity, changes, used, method, tried, error = cloud_resolve_issue_and_changelog(
                candidates
            )

            if changes is None:
                print(
                    f"[{project_key}] Cloud issue lookup failed for {key} "
                    f"(tried: {', '.join(tried)}). Rows will be marked unresolved."
                )
                cloud_cache[key] = None
                cloud_issue_identity_map[key] = {}
                cloud_candidate_used[key] = ""
                cloud_lookup_method[key] = ""
                cloud_lookup_error[key] = error
            else:
                cloud_cache[key] = changes
                cloud_issue_identity_map[key] = identity
                cloud_candidate_used[key] = used
                cloud_lookup_method[key] = method
                cloud_lookup_error[key] = ""

        print(f"[{project_key}] Comparing transitions...")
        missing_rows = []

        for key, entries in tqdm(
            dc_transitions_by_issue.items(), desc=f"Validating {project_key}"
        ):
            meta = dc_issue_move_meta.get(key, {}) or {
                "dc_issue_id": dc_issue_id_map.get(key, "")
            }
            cloud_changes = cloud_cache.get(key)
            cloud_identity = cloud_issue_identity_map.get(key, {}) or {}

            sorted_entries = sorted(
                entries,
                key=lambda e: (
                    _jira_timestamp_key(e.get("timestamp")),
                    _clean_text(e.get("changegroup_Id")),
                ),
            )

            if cloud_changes is None:
                for entry in sorted_entries:
                    missing_rows.append(
                        build_missing_row(
                            dc_key=key,
                            entry=entry,
                            meta=meta,
                            cloud_identity=cloud_identity,
                            cloud_candidate_used=cloud_candidate_used.get(key, ""),
                            cloud_lookup_method=cloud_lookup_method.get(key, ""),
                            cloud_lookup_error=cloud_lookup_error.get(key, ""),
                            comparison_status="cloud_issue_not_resolved",
                        )
                    )
                continue

            cloud_buckets = _index_cloud_transitions(cloud_changes)
            for entry in sorted_entries:
                if _consume_cloud_transition_match(entry, cloud_buckets) is None:
                    missing_rows.append(
                        build_missing_row(
                            dc_key=key,
                            entry=entry,
                            meta=meta,
                            cloud_identity=cloud_identity,
                            cloud_candidate_used=cloud_candidate_used.get(key, ""),
                            cloud_lookup_method=cloud_lookup_method.get(key, ""),
                            cloud_lookup_error="",
                            comparison_status="missing_transition",
                        )
                    )

        write_csv(project_key, missing_rows)
        print(
            f"[{project_key}] ✅ Done. Missing/unresolved transitions: "
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
        "cloud_issue_key_used",
        "cloud_issue_project_key",
        "cloud_lookup_candidate",
        "cloud_lookup_method",
        "cloud_lookup_error",
        "comparison_status",
        "dc_issue_key_old",
        "dc_issue_key_new",
        "dc_issue_key_current",
        "dc_issue_key_candidates",
        "dc_project_key_old",
        "dc_project_key_new",
        "dc_project_key_current",
        "dc_changelog_truncated",
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
