#!/usr/bin/env python3
import os
import csv
import sys
import argparse
from urllib.parse import urlencode
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
from tqdm import tqdm

# Include 'project' so we can read project key + category
JIRA_FIELDS = [
    "summary",
    "status",
    "resolutiondate",
    "created",
    "resolution",
    "project"
]

def build_url(base_url: str, jql: str, fields: list[str], next_page_token: str | None) -> str:
    params = {
        "jql": jql,
        "fields": ",".join(fields),
    }
    if next_page_token:
        params["nextPageToken"] = next_page_token
    return f"{base_url.rstrip('/')}/rest/api/2/search/jql?{urlencode(params)}"

def extract_row(issue: dict) -> dict:
    f = issue.get("fields") or {}

    # Status + category
    status = f.get("status") or {}
    sc = status.get("statusCategory") if isinstance(status, dict) else None
    status_category = None
    if isinstance(sc, dict):
        status_category = sc.get("name") or sc.get("key") or sc.get("id")

    # Resolution (name only) + dates
    resolution_obj = f.get("resolution")
    resolution_name = resolution_obj.get("name") if isinstance(resolution_obj, dict) else None

    # Project key + category
    proj = f.get("project") or {}
    project_key = proj.get("key")
    project_category_obj = proj.get("projectCategory") if isinstance(proj, dict) else None
    project_category = None
    if isinstance(project_category_obj, dict):
        project_category = project_category_obj.get("name") or project_category_obj.get("id")

    return {
        "key": issue.get("key"),
        "summary": f.get("summary"),
        "status": status.get("name"),
        "status_category": status_category,
        "resolved": f.get("resolutiondate"),
        "created": f.get("created"),
        "resolution": resolution_name,
        "project_key": project_key,
        "project_category": project_category,
    }

def main():
    parser = argparse.ArgumentParser(description="Export Jira Cloud issues by JQL (token-paged API) to CSV.")
    parser.add_argument("--jql", required=True, help='Quote your JQL. Example: --jql "project = ABC AND statusCategory != Done"')
    parser.add_argument("--out", default="jira_issues.csv", help="Output CSV (default: jira_issues.csv)")
    args = parser.parse_args()

    load_dotenv()
    base_url = os.getenv("CLOUD_BASE_URL")
    email = os.getenv("CLOUD_EMAIL")
    api_token = os.getenv("CLOUD_API_TOKEN")

    if not base_url or not email or not api_token:
        print("ERROR: Missing one or more env vars: CLOUD_BASE_URL, CLOUD_EMAIL, CLOUD_API_TOKEN", file=sys.stderr)
        sys.exit(1)

    auth = HTTPBasicAuth(email, api_token)
    headers = {"Accept": "application/json"}

    # Stream to CSV as we go
    fieldnames = [
        "key",
        "summary",
        "status",
        "status_category",
        "resolved",
        "created",
        "resolution",
        "project_key",
        "project_category",
    ]

    written = 0
    next_page_token = None
    is_last = False

    with open(args.out, "w", newline="", encoding="utf-8") as f_out, tqdm(unit="issue", desc="Fetching issues", total=None) as pbar:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        while not is_last:
            url = build_url(base_url, args.jql, JIRA_FIELDS, next_page_token)
            resp = requests.get(url, headers=headers, auth=auth)
            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                print(f"HTTP error: {e}\nResponse: {resp.text}", file=sys.stderr)
                sys.exit(2)

            data = resp.json() or {}
            issues = data.get("issues") or []

            for issue in issues:
                writer.writerow(extract_row(issue))
            written += len(issues)
            pbar.update(len(issues))

            # Token-based pagination from your API
            next_page_token = data.get("nextPageToken")
            is_last = bool(data.get("isLast"))

    print(f"Done. Wrote {written} issues to {args.out}")

if __name__ == "__main__":
    main()
