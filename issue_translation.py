import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import requests
from requests.auth import HTTPBasicAuth
from urllib3.exceptions import InsecureRequestWarning


ENV_FILE = Path(".env")
CSV_OUTPUT_FILE = "issue_translation.csv"
DEFAULT_JQL = "ORDER BY key ASC"
PAGE_SIZE = 100
MAX_RETRIES = 3
BACKOFF_SECONDS = 2
CHECKPOINT_VERSION = 1

FIELDNAMES = [
    "dc_issue_key",
    "dc_issue_id",
    "cloud_issue_key",
    "cloud_issue_id",
    "cloud_lookup_candidate",
    "cloud_lookup_method",
    "cloud_lookup_error",
    "status",
]


class JiraClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_version: str,
        auth_user: str,
        auth_token: str,
        verify: Union[bool, str],
        label: str,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self.verify = verify
        self.label = label
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(auth_user, auth_token)
        self.session.headers.update({"Accept": "application/json"})

    def get(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 60,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        last_error: Optional[BaseException] = None
        last_response: Optional[requests.Response] = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=timeout,
                    verify=self.verify,
                )
                last_response = response

                if response.status_code == 429 or 500 <= response.status_code < 600:
                    print(
                        f"[{self.label}] HTTP {response.status_code} for {url} "
                        f"(attempt {attempt}/{MAX_RETRIES})"
                    )
                    if attempt < MAX_RETRIES:
                        time.sleep(BACKOFF_SECONDS * attempt)
                        continue

                return response
            except requests.exceptions.RequestException as error:
                last_error = error
                print(
                    f"[{self.label}] Request error for {url} "
                    f"(attempt {attempt}/{MAX_RETRIES}): {error}"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(BACKOFF_SECONDS * attempt)

        if last_error is not None:
            raise last_error
        if last_response is not None:
            return last_response
        raise RuntimeError(f"[{self.label}] Request failed without a response: {url}")

    def search(self, *, jql: str, start_at: int, page_size: int) -> Dict[str, Any]:
        response = self.get(
            f"/rest/api/{self.api_version}/search",
            params={
                "jql": jql,
                "startAt": start_at,
                "maxResults": page_size,
                "fields": "key",
            },
            timeout=90,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"[{self.label}] Search failed at startAt={start_at}: "
                f"{response.status_code} {response.text[:500]}"
            )
        return response.json() or {}

    def issue_identity(self, issue_id_or_key: str) -> Dict[str, str]:
        response = self.get(
            f"/rest/api/{self.api_version}/issue/{issue_id_or_key}",
            params={"fields": "project"},
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"{response.status_code} {response.text[:300]}"
            )

        payload = response.json() or {}
        return {
            "id": clean_text(payload.get("id")),
            "key": clean_text(payload.get("key")),
        }

    def search_identity_by_key(self, issue_key: str) -> Dict[str, str]:
        last_error = ""
        for api_version, path in (
            ("3", "/rest/api/3/search/jql"),
            ("3", "/rest/api/3/search"),
            ("2", "/rest/api/2/search"),
        ):
            if api_version != self.api_version and self.api_version == "2":
                continue

            response = self.get(
                path,
                params={"jql": f"issuekey = {issue_key}", "fields": "key", "maxResults": 1},
                timeout=60,
            )
            if response.status_code != 200:
                last_error = f"{response.status_code} {response.text[:300]}"
                continue

            issues = (response.json() or {}).get("issues") or []
            if not issues:
                raise RuntimeError(f"No Cloud issue found for historical key {issue_key}")

            issue = issues[0]
            return {
                "id": clean_text(issue.get("id")),
                "key": clean_text(issue.get("key")),
            }

        raise RuntimeError(last_error or f"Cloud JQL lookup failed for {issue_key}")


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.split("#", 1)[0].strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


def env(name: str, fallback: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return fallback
    return value


def parse_ssl_verify(value: str) -> Union[bool, str]:
    cleaned = value.strip()
    if cleaned.lower() in ("false", "0", "no", "off"):
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        return False
    if cleaned.lower() in ("true", "1", "yes", "on"):
        return True
    return cleaned


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def unique_nonempty(values: Iterable[str]) -> List[str]:
    output: List[str] = []
    seen: Set[str] = set()
    for value in values:
        text = clean_text(value)
        if text and text not in seen:
            output.append(text)
            seen.add(text)
    return output


def parse_args() -> argparse.Namespace:
    default_output = env("ISSUE_TRANSLATION_CSV")
    if not default_output:
        output_dir = env("OUTPUT_DIR")
        default_output = str(Path(output_dir) / CSV_OUTPUT_FILE) if output_dir else CSV_OUTPUT_FILE

    parser = argparse.ArgumentParser(
        description=(
            "Build a streaming Data Center issue ID to Cloud issue ID translation CSV. "
            "The output CSV is append-only and is used to resume after failures."
        )
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=int(env("ISSUE_TRANSLATION_PAGE_SIZE", str(PAGE_SIZE)) or str(PAGE_SIZE)),
        help=f"DC issues fetched per request. Default: {PAGE_SIZE}.",
    )
    parser.add_argument(
        "--output",
        default=default_output,
        help=f"CSV output path. Default: {CSV_OUTPUT_FILE}.",
    )
    parser.add_argument(
        "--checkpoint",
        default=env("ISSUE_TRANSLATION_CHECKPOINT"),
        help=(
            "Resume checkpoint path. Default: <output>.checkpoint.json. "
            "The CSV is still used to skip rows already written."
        ),
    )
    parser.add_argument(
        "--jql",
        default=env("ISSUE_TRANSLATION_JQL", DEFAULT_JQL),
        help=f"DC JQL used to list source issues. Default: {DEFAULT_JQL!r}.",
    )
    parser.add_argument(
        "--ssl-verify",
        default=env("SSL_VERIFY", "True"),
        help="Default TLS verification: True, False, or path to a .pem certificate bundle.",
    )
    parser.add_argument(
        "--dc-ssl-verify",
        default=env("DC_SSL_VERIFY"),
        help="DC TLS verification override: True, False, or path to a .pem certificate bundle.",
    )
    parser.add_argument(
        "--cloud-ssl-verify",
        default=env("CLOUD_SSL_VERIFY"),
        help="Cloud TLS verification override: True, False, or path to a .pem certificate bundle.",
    )
    parser.add_argument(
        "--refresh-errors",
        action="store_true",
        help="Retry rows previously written with status=error instead of treating them as complete.",
    )
    parser.add_argument(
        "--ignore-checkpoint",
        action="store_true",
        help="Ignore the checkpoint and scan DC from the first page, while still skipping CSV rows already written.",
    )
    return parser.parse_args()


def require_config(args: argparse.Namespace) -> Dict[str, Any]:
    config = {
        "dc_base_url": env("DC_BASE_URL", ""),
        "dc_username": env("DC_USERNAME", ""),
        "dc_api_token": env("DC_API_TOKEN", ""),
        "cloud_base_url": env("CLOUD_BASE_URL", env("JIRA_URL", "")),
        "cloud_email": env("CLOUD_EMAIL", env("JIRA_EMAIL", "")),
        "cloud_api_token": env("CLOUD_API_TOKEN", env("JIRA_API_TOKEN", "")),
        "output_file": args.output,
        "checkpoint_file": args.checkpoint or f"{args.output}.checkpoint.json",
        "jql": args.jql,
        "page_size": max(args.page_size, 1),
        "dc_ssl_verify": parse_ssl_verify(args.dc_ssl_verify or args.ssl_verify),
        "cloud_ssl_verify": parse_ssl_verify(args.cloud_ssl_verify or "True"),
        "refresh_errors": args.refresh_errors,
        "ignore_checkpoint": args.ignore_checkpoint,
    }

    missing = [
        label
        for label, value in (
            ("DC_BASE_URL", config["dc_base_url"]),
            ("DC_USERNAME", config["dc_username"]),
            ("DC_API_TOKEN", config["dc_api_token"]),
            ("CLOUD_BASE_URL", config["cloud_base_url"]),
            ("CLOUD_EMAIL", config["cloud_email"]),
            ("CLOUD_API_TOKEN", config["cloud_api_token"]),
        )
        if not value
    ]

    if missing:
        print("Missing required configuration: " + ", ".join(missing), file=sys.stderr)
        sys.exit(1)

    return config


def load_completed_issue_ids(output_file: str, *, refresh_errors: bool) -> Set[str]:
    path = Path(output_file)
    if not path.exists():
        return set()

    completed: Set[str] = set()
    with path.open(newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        for row in reader:
            dc_issue_id = clean_text(
                row.get("dc_issue_id")
                or row.get("dcIssueId")
                or row.get("server_issue_id")
                or row.get("serverIssueId")
            )
            status = clean_text(row.get("status"))
            if dc_issue_id and (status != "error" or not refresh_errors):
                completed.add(dc_issue_id)
    return completed


def load_checkpoint(
    checkpoint_file: str,
    *,
    output_file: str,
    jql: str,
    page_size: int,
    ignore_checkpoint: bool,
) -> int:
    if ignore_checkpoint:
        return 0

    path = Path(checkpoint_file)
    if not path.exists():
        return 0

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"Could not read checkpoint {checkpoint_file}: {error}. Starting at page 0.")
        return 0

    if data.get("version") != CHECKPOINT_VERSION:
        print(f"Ignoring checkpoint {checkpoint_file}: unsupported version.")
        return 0
    if data.get("output_file") != output_file:
        print(f"Ignoring checkpoint {checkpoint_file}: output file changed.")
        return 0
    if data.get("jql") != jql:
        print(f"Ignoring checkpoint {checkpoint_file}: JQL changed.")
        return 0
    if int(data.get("page_size") or 0) != page_size:
        print(f"Ignoring checkpoint {checkpoint_file}: page size changed.")
        return 0

    return max(int(data.get("next_start_at") or 0), 0)


def save_checkpoint(
    checkpoint_file: str,
    *,
    output_file: str,
    jql: str,
    page_size: int,
    next_start_at: int,
    total: int,
    completed_rows: int,
) -> None:
    path = Path(checkpoint_file)
    if str(path.parent) != ".":
        path.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "version": CHECKPOINT_VERSION,
        "output_file": output_file,
        "jql": jql,
        "page_size": page_size,
        "next_start_at": next_start_at,
        "total": total,
        "completed_rows": completed_rows,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def open_output_writer(output_file: str) -> Tuple[Any, csv.DictWriter]:
    path = Path(output_file)
    if str(path.parent) != ".":
        path.parent.mkdir(parents=True, exist_ok=True)

    needs_header = not path.exists() or path.stat().st_size == 0
    file = path.open("a", newline="", encoding="utf-8")
    writer = csv.DictWriter(file, fieldnames=FIELDNAMES, extrasaction="ignore")
    if needs_header:
        writer.writeheader()
        file.flush()
    return file, writer


def dc_issue_rows(payload: Dict[str, Any]) -> List[Tuple[str, str]]:
    rows = []
    for issue in payload.get("issues", []) or []:
        key = clean_text(issue.get("key"))
        issue_id = clean_text(issue.get("id"))
        if key and issue_id:
            rows.append((key, issue_id))
    return rows


def resolve_cloud_issue(cloud: JiraClient, dc_issue_key: str) -> Dict[str, str]:
    errors = []
    for method, resolver in (
        ("direct", cloud.issue_identity),
        ("jql", cloud.search_identity_by_key),
    ):
        try:
            identity = resolver(dc_issue_key)
            return {
                "cloud_issue_key": identity.get("key", ""),
                "cloud_issue_id": identity.get("id", ""),
                "cloud_lookup_candidate": dc_issue_key,
                "cloud_lookup_method": method,
                "cloud_lookup_error": "",
                "status": "mapped" if identity.get("id") else "not_found",
            }
        except Exception as error:
            errors.append(f"{method}:{error}")

    return {
        "cloud_issue_key": "",
        "cloud_issue_id": "",
        "cloud_lookup_candidate": dc_issue_key,
        "cloud_lookup_method": "",
        "cloud_lookup_error": "; ".join(errors[-2:]),
        "status": "not_found",
    }


def write_translation_row(
    writer: csv.DictWriter,
    file: Any,
    *,
    dc_issue_key: str,
    dc_issue_id: str,
    cloud_result: Dict[str, str],
) -> None:
    writer.writerow(
        {
            "dc_issue_key": dc_issue_key,
            "dc_issue_id": dc_issue_id,
            **cloud_result,
        }
    )
    file.flush()


def process_dc_page(
    *,
    dc: JiraClient,
    cloud: JiraClient,
    writer: csv.DictWriter,
    file: Any,
    completed_issue_ids: Set[str],
    jql: str,
    start_at: int,
    page_size: int,
) -> Tuple[int, int, int]:
    payload = dc.search(jql=jql, start_at=start_at, page_size=page_size)
    total = int(payload.get("total", 0) or 0)
    rows = dc_issue_rows(payload)
    written = 0
    skipped = 0

    for dc_issue_key, dc_issue_id in rows:
        if dc_issue_id in completed_issue_ids:
            skipped += 1
            continue

        try:
            cloud_result = resolve_cloud_issue(cloud, dc_issue_key)
        except Exception as error:
            cloud_result = {
                "cloud_issue_key": "",
                "cloud_issue_id": "",
                "cloud_lookup_candidate": dc_issue_key,
                "cloud_lookup_method": "",
                "cloud_lookup_error": str(error),
                "status": "error",
            }

        write_translation_row(
            writer,
            file,
            dc_issue_key=dc_issue_key,
            dc_issue_id=dc_issue_id,
            cloud_result=cloud_result,
        )
        completed_issue_ids.add(dc_issue_id)
        written += 1

    return total, written, skipped


def main() -> None:
    load_dotenv(ENV_FILE)
    args = parse_args()
    config = require_config(args)

    dc = JiraClient(
        base_url=config["dc_base_url"],
        api_version="2",
        auth_user=config["dc_username"],
        auth_token=config["dc_api_token"],
        verify=config["dc_ssl_verify"],
        label="DC",
    )
    cloud = JiraClient(
        base_url=config["cloud_base_url"],
        api_version="3",
        auth_user=config["cloud_email"],
        auth_token=config["cloud_api_token"],
        verify=config["cloud_ssl_verify"],
        label="Cloud",
    )

    completed_issue_ids = load_completed_issue_ids(
        config["output_file"],
        refresh_errors=config["refresh_errors"],
    )
    output_file, writer = open_output_writer(config["output_file"])
    start_at = load_checkpoint(
        config["checkpoint_file"],
        output_file=config["output_file"],
        jql=config["jql"],
        page_size=config["page_size"],
        ignore_checkpoint=config["ignore_checkpoint"],
    )

    print(f"Resuming with {len(completed_issue_ids)} completed DC issue(s).")
    print(f"Writing translation rows to {config['output_file']}")
    print(f"Using checkpoint {config['checkpoint_file']} from startAt={start_at}")

    total = None
    total_written = 0
    total_skipped = 0

    try:
        while total is None or start_at < total:
            try:
                page_total, written, skipped = process_dc_page(
                    dc=dc,
                    cloud=cloud,
                    writer=writer,
                    file=output_file,
                    completed_issue_ids=completed_issue_ids,
                    jql=config["jql"],
                    start_at=start_at,
                    page_size=config["page_size"],
                )
            except Exception as error:
                print(
                    f"Stopping after page startAt={start_at}. "
                    f"Progress has been saved and the script can be rerun. Error: {error}",
                    file=sys.stderr,
                )
                return

            total = page_total
            total_written += written
            total_skipped += skipped
            processed = min(start_at + config["page_size"], total)
            next_start_at = start_at + config["page_size"]
            save_checkpoint(
                config["checkpoint_file"],
                output_file=config["output_file"],
                jql=config["jql"],
                page_size=config["page_size"],
                next_start_at=next_start_at,
                total=total,
                completed_rows=len(completed_issue_ids),
            )
            print(
                f"DC page {start_at}-{processed}: "
                f"wrote {written}, skipped {skipped}, total {processed}/{total}"
            )
            start_at = next_start_at
    finally:
        output_file.close()

    print(
        f"Done. Wrote {total_written} new row(s), skipped {total_skipped} existing row(s)."
    )


if __name__ == "__main__":
    main()
