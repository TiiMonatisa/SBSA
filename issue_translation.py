import argparse
import csv
import os
import queue
import sqlite3
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from requests.auth import HTTPBasicAuth
from urllib3.exceptions import InsecureRequestWarning


ENV_FILE = Path(".env")
CSV_OUTPUT_FILE = "issue_translation.csv"
SQLITE_FILE = "issue_translation_cache.sqlite3"
DEFAULT_JQL = "ORDER BY key"
PAGE_SIZE = 100
SENTINEL = object()


class ProgressBar:
    def __init__(self, label: str, total: int) -> None:
        self.label = label
        self.total = max(total, 0)
        self.current = 0
        self.lock = threading.Lock()

    def advance(self, amount: int) -> None:
        with self.lock:
            self.current += amount
            self.render()

    def render(self) -> None:
        if self.total <= 0:
            print(f"\r{self.label}: {self.current}", end="", flush=True)
            return

        width = 30
        ratio = min(self.current / self.total, 1)
        filled = int(width * ratio)
        bar = "#" * filled + "-" * (width - filled)
        percent = int(ratio * 100)
        print(
            f"\r{self.label}: [{bar}] {percent:3d}% ({self.current}/{self.total})",
            end="",
            flush=True,
        )

    def finish(self) -> None:
        self.render()
        print()


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Jira issue key, DC issue id, and Cloud issue id by matching on issue key."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=int(env("ISSUE_TRANSLATION_WORKERS", "4") or "4"),
        help="Number of parallel Jira search workers. Default: 4.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=int(env("ISSUE_TRANSLATION_PAGE_SIZE", str(PAGE_SIZE)) or str(PAGE_SIZE)),
        help=f"Issues fetched per request. Default: {PAGE_SIZE}.",
    )
    parser.add_argument(
        "--output",
        default=env("ISSUE_TRANSLATION_CSV", CSV_OUTPUT_FILE),
        help=f"CSV output path. Default: {CSV_OUTPUT_FILE}.",
    )
    parser.add_argument(
        "--cache",
        default=env("ISSUE_TRANSLATION_CACHE", SQLITE_FILE),
        help=f"SQLite cache path for DC issue ids. Default: {SQLITE_FILE}.",
    )
    parser.add_argument(
        "--jql",
        default=env("ISSUE_TRANSLATION_JQL", DEFAULT_JQL),
        help=f"JQL for both DC and Cloud. Default: {DEFAULT_JQL!r}.",
    )
    parser.add_argument(
        "--dc-jql",
        default=env("DC_ISSUE_TRANSLATION_JQL"),
        help="JQL for DC only. Defaults to --jql.",
    )
    parser.add_argument(
        "--cloud-jql",
        default=env("CLOUD_ISSUE_TRANSLATION_JQL"),
        help="JQL for Cloud only. Defaults to --jql.",
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
        "cache_file": args.cache,
        "dc_jql": args.dc_jql or args.jql,
        "cloud_jql": args.cloud_jql or args.jql,
        "page_size": max(args.page_size, 1),
        "workers": max(args.workers, 1),
        "dc_ssl_verify": parse_ssl_verify(args.dc_ssl_verify or args.ssl_verify),
        "cloud_ssl_verify": parse_ssl_verify(args.cloud_ssl_verify or "True"),
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

    config["dc_base_url"] = config["dc_base_url"].rstrip("/")
    config["cloud_base_url"] = config["cloud_base_url"].rstrip("/")
    return config


def fetch_page(
    base_url: str,
    auth_user: str,
    auth_token: str,
    api_version: str,
    jql: str,
    page_size: int,
    start_at: int,
    ssl_verify: Union[bool, str],
) -> Dict[str, Any]:
    session = requests.Session()
    session.auth = HTTPBasicAuth(auth_user, auth_token)
    session.headers.update({"Accept": "application/json"})

    response = session.get(
        f"{base_url}/rest/api/{api_version}/search",
        params={
            "jql": jql,
            "startAt": start_at,
            "maxResults": page_size,
            "fields": "key",
        },
        timeout=60,
        verify=ssl_verify,
    )
    response.raise_for_status()
    return response.json()


def rows_from_issues(issues: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    return [(issue.get("key", ""), issue.get("id", "")) for issue in issues]


def fetch_rows(
    config: Dict[str, Any],
    source: str,
    start_at: int,
) -> Tuple[int, List[Tuple[str, str]]]:
    if source == "dc":
        payload = fetch_page(
            base_url=config["dc_base_url"],
            auth_user=config["dc_username"],
            auth_token=config["dc_api_token"],
            api_version="2",
            jql=config["dc_jql"],
            page_size=config["page_size"],
            start_at=start_at,
            ssl_verify=config["dc_ssl_verify"],
        )
    else:
        payload = fetch_page(
            base_url=config["cloud_base_url"],
            auth_user=config["cloud_email"],
            auth_token=config["cloud_api_token"],
            api_version="3",
            jql=config["cloud_jql"],
            page_size=config["page_size"],
            start_at=start_at,
            ssl_verify=config["cloud_ssl_verify"],
        )

    return start_at, rows_from_issues(payload.get("issues", []))


def worker(
    config: Dict[str, Any],
    source: str,
    task_queue: "queue.Queue[object]",
    result_queue: "queue.Queue[object]",
) -> None:
    while True:
        task = task_queue.get()
        try:
            if task is SENTINEL:
                return
            result_queue.put(fetch_rows(config, source, int(task)))
        except Exception as error:
            result_queue.put(error)
        finally:
            task_queue.task_done()


def enqueue_pages(
    total: int,
    page_size: int,
    task_queue: "queue.Queue[object]",
    workers: int,
) -> None:
    try:
        for start_at in range(page_size, total, page_size):
            task_queue.put(start_at)
    finally:
        for _ in range(workers):
            task_queue.put(SENTINEL)


def setup_cache(path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("DROP TABLE IF EXISTS dc_issues")
    connection.execute("CREATE TABLE dc_issues (key TEXT PRIMARY KEY, dc_issue_id TEXT NOT NULL)")
    return connection


def lookup_dc_issue_id(connection: sqlite3.Connection, key: str) -> str:
    row = connection.execute(
        "SELECT dc_issue_id FROM dc_issues WHERE key = ?",
        (key,),
    ).fetchone()
    return "" if row is None else str(row[0])


def process_remaining_pages(
    config: Dict[str, Any],
    source: str,
    total: int,
    on_rows: Any,
    progress: ProgressBar,
) -> None:
    remaining_pages = max(0, (total - 1) // config["page_size"])
    if not remaining_pages:
        return

    buffer_pages = max(config["workers"] * 3, 1)
    task_queue: "queue.Queue[object]" = queue.Queue(maxsize=buffer_pages)
    result_queue: "queue.Queue[object]" = queue.Queue(maxsize=buffer_pages)

    worker_threads = [
        threading.Thread(
            target=worker,
            args=(config, source, task_queue, result_queue),
            daemon=True,
        )
        for _ in range(config["workers"])
    ]
    for thread in worker_threads:
        thread.start()

    producer = threading.Thread(
        target=enqueue_pages,
        args=(total, config["page_size"], task_queue, config["workers"]),
        daemon=True,
    )
    producer.start()

    completed_pages = 0
    while completed_pages < remaining_pages:
        result = result_queue.get()
        if isinstance(result, Exception):
            raise result

        _, rows = result
        on_rows(rows)
        completed_pages += 1
        progress.advance(len(rows))

    producer.join()
    for thread in worker_threads:
        thread.join()


def build_dc_cache(config: Dict[str, Any]) -> int:
    connection = setup_cache(config["cache_file"])
    total_rows = 0

    first_payload = fetch_page(
        base_url=config["dc_base_url"],
        auth_user=config["dc_username"],
        auth_token=config["dc_api_token"],
        api_version="2",
        jql=config["dc_jql"],
        page_size=config["page_size"],
        start_at=0,
        ssl_verify=config["dc_ssl_verify"],
    )
    total = int(first_payload.get("total", 0))
    progress = ProgressBar("DC", total)

    def insert_rows(rows: List[Tuple[str, str]]) -> None:
        nonlocal total_rows
        connection.executemany(
            "INSERT OR REPLACE INTO dc_issues (key, dc_issue_id) VALUES (?, ?)",
            rows,
        )
        connection.commit()
        total_rows += len(rows)

    first_rows = rows_from_issues(first_payload.get("issues", []))
    insert_rows(first_rows)
    progress.advance(len(first_rows))

    process_remaining_pages(config, "dc", total, insert_rows, progress)
    progress.finish()
    connection.close()
    return total_rows


def export_cloud_translation(config: Dict[str, Any]) -> Tuple[int, int]:
    cache_connection = sqlite3.connect(config["cache_file"])
    written = 0
    missing_dc = 0

    with open(config["output_file"], mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["key", "dc_issue_id", "cloud_issue_id"])

        first_payload = fetch_page(
            base_url=config["cloud_base_url"],
            auth_user=config["cloud_email"],
            auth_token=config["cloud_api_token"],
            api_version="3",
            jql=config["cloud_jql"],
            page_size=config["page_size"],
            start_at=0,
            ssl_verify=config["cloud_ssl_verify"],
        )
        total = int(first_payload.get("total", 0))
        progress = ProgressBar("Cloud", total)

        def write_rows(rows: List[Tuple[str, str]]) -> None:
            nonlocal written, missing_dc
            output_rows = []
            for key, cloud_issue_id in rows:
                dc_issue_id = lookup_dc_issue_id(cache_connection, key)
                if not dc_issue_id:
                    missing_dc += 1
                output_rows.append((key, dc_issue_id, cloud_issue_id))

            writer.writerows(output_rows)
            written += len(output_rows)

        first_rows = rows_from_issues(first_payload.get("issues", []))
        write_rows(first_rows)
        progress.advance(len(first_rows))

        process_remaining_pages(config, "cloud", total, write_rows, progress)
        progress.finish()

    cache_connection.close()
    return written, missing_dc


def main() -> None:
    load_dotenv(ENV_FILE)
    args = parse_args()
    config = require_config(args)

    dc_rows = build_dc_cache(config)
    written, missing_dc = export_cloud_translation(config)

    print(f"Cached {dc_rows} DC issues in {config['cache_file']}")
    print(f"Done. Wrote {written} rows to {config['output_file']}")
    if missing_dc:
        print(f"Warning: {missing_dc} Cloud issues did not have a matching DC issue key")


if __name__ == "__main__":
    main()
