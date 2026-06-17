import argparse
import csv
import os
import queue
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from requests.auth import HTTPBasicAuth
from urllib3.exceptions import InsecureRequestWarning


ENV_FILE = Path(".env")
CSV_OUTPUT_FILE = "issue_translation.csv"
DEFAULT_JQL = "ORDER BY key"
PAGE_SIZE = 100


class ProgressBar:
    def __init__(self, total: int) -> None:
        self.total = max(total, 0)
        self.current = 0
        self.lock = threading.Lock()

    def advance(self, amount: int) -> None:
        with self.lock:
            self.current += amount
            self.render()

    def render(self) -> None:
        if self.total <= 0:
            print(f"\rProcessed {self.current}", end="", flush=True)
            return

        width = 30
        ratio = min(self.current / self.total, 1)
        filled = int(width * ratio)
        bar = "#" * filled + "-" * (width - filled)
        percent = int(ratio * 100)
        print(
            f"\r[{bar}] {percent:3d}% ({self.current}/{self.total})",
            end="",
            flush=True,
        )

    def finish(self) -> None:
        self.render()
        print()


SENTINEL = object()


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Jira Cloud issue keys with DC issue ids and Cloud issue ids."
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
        "--jql",
        default=env("ISSUE_TRANSLATION_JQL", DEFAULT_JQL),
        help=f"JQL for issue selection. Default: {DEFAULT_JQL!r}.",
    )
    parser.add_argument(
        "--ssl-verify",
        default=env("SSL_VERIFY", "True"),
        help="TLS verification: True, False, or path to a .pem certificate bundle.",
    )
    return parser.parse_args()


def require_config(args: argparse.Namespace) -> Dict[str, Any]:
    load_dotenv(ENV_FILE)

    config = {
        "cloud_base_url": env("CLOUD_BASE_URL", env("JIRA_URL", "")),
        "cloud_email": env("CLOUD_EMAIL", env("JIRA_EMAIL", "")),
        "cloud_api_token": env("CLOUD_API_TOKEN", env("JIRA_API_TOKEN", "")),
        "dc_issue_field": env("JIRA_CUSTOM_FIELD_ID", ""),
        "output_file": args.output,
        "jql": args.jql,
        "page_size": max(args.page_size, 1),
        "workers": max(args.workers, 1),
        "ssl_verify": parse_ssl_verify(args.ssl_verify),
    }

    missing = [
        label
        for label, value in (
            ("CLOUD_BASE_URL", config["cloud_base_url"]),
            ("CLOUD_EMAIL", config["cloud_email"]),
            ("CLOUD_API_TOKEN", config["cloud_api_token"]),
            ("JIRA_CUSTOM_FIELD_ID", config["dc_issue_field"]),
        )
        if not value
    ]

    if missing:
        print("Missing required configuration: " + ", ".join(missing), file=sys.stderr)
        sys.exit(1)

    config["cloud_base_url"] = config["cloud_base_url"].rstrip("/")
    return config


def parse_ssl_verify(value: str) -> Union[bool, str]:
    cleaned = value.strip()
    if cleaned.lower() in ("false", "0", "no", "off"):
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        return False
    if cleaned.lower() in ("true", "1", "yes", "on"):
        return True
    return cleaned


def format_field_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        for key in ("id", "value", "key", "name"):
            if value.get(key) is not None:
                return str(value[key])
        return str(value)
    if isinstance(value, list):
        return ";".join(format_field_value(item) for item in value)
    return str(value)


def fetch_page(
    cloud_base_url: str,
    cloud_email: str,
    cloud_api_token: str,
    jql: str,
    field_id: str,
    page_size: int,
    start_at: int,
    ssl_verify: Union[bool, str],
) -> Dict[str, Any]:
    session = requests.Session()
    session.auth = HTTPBasicAuth(cloud_email, cloud_api_token)
    session.headers.update({"Accept": "application/json"})

    response = session.get(
        f"{cloud_base_url}/rest/api/3/search",
        params={
            "jql": jql,
            "startAt": start_at,
            "maxResults": page_size,
            "fields": f"key,{field_id}",
        },
        timeout=60,
        verify=ssl_verify,
    )
    response.raise_for_status()
    return response.json()


def rows_from_issues(issues: List[Dict[str, Any]], field_id: str) -> List[Tuple[str, str, str]]:
    rows = []
    for issue in issues:
        fields = issue.get("fields", {})
        rows.append(
            (
                issue.get("key", ""),
                format_field_value(fields.get(field_id)),
                issue.get("id", ""),
            )
        )
    return rows


def fetch_rows(config: Dict[str, Any], start_at: int) -> Tuple[int, List[Tuple[str, str, str]]]:
    payload = fetch_page(
        cloud_base_url=config["cloud_base_url"],
        cloud_email=config["cloud_email"],
        cloud_api_token=config["cloud_api_token"],
        jql=config["jql"],
        field_id=config["dc_issue_field"],
        page_size=config["page_size"],
        start_at=start_at,
        ssl_verify=config["ssl_verify"],
    )
    return start_at, rows_from_issues(payload.get("issues", []), config["dc_issue_field"])


def worker(
    config: Dict[str, Any],
    task_queue: "queue.Queue[object]",
    result_queue: "queue.Queue[object]",
) -> None:
    while True:
        task = task_queue.get()
        try:
            if task is SENTINEL:
                return
            result_queue.put(fetch_rows(config, int(task)))
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


def main() -> None:
    load_dotenv(ENV_FILE)
    args = parse_args()
    config = require_config(args)

    written = 0

    with open(config["output_file"], mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["key", "dc_issue_id", "cloud_issue_id"])

        first_payload = fetch_page(
            cloud_base_url=config["cloud_base_url"],
            cloud_email=config["cloud_email"],
            cloud_api_token=config["cloud_api_token"],
            jql=config["jql"],
            field_id=config["dc_issue_field"],
            page_size=config["page_size"],
            start_at=0,
            ssl_verify=config["ssl_verify"],
        )

        total = int(first_payload.get("total", 0))
        progress = ProgressBar(total)
        first_rows = rows_from_issues(first_payload.get("issues", []), config["dc_issue_field"])
        writer.writerows(first_rows)
        written += len(first_rows)
        progress.advance(len(first_rows))

        remaining_pages = max(0, (total - 1) // config["page_size"])
        if remaining_pages:
            buffer_pages = max(config["workers"] * 3, 1)
            task_queue: "queue.Queue[object]" = queue.Queue(maxsize=buffer_pages)
            result_queue: "queue.Queue[object]" = queue.Queue(maxsize=buffer_pages)

            workers = [
                threading.Thread(
                    target=worker,
                    args=(config, task_queue, result_queue),
                    daemon=True,
                )
                for _ in range(config["workers"])
            ]
            for thread in workers:
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
                writer.writerows(rows)
                written += len(rows)
                completed_pages += 1
                progress.advance(len(rows))

            producer.join()
            for thread in workers:
                thread.join()

        progress.finish()

    print(f"Done. Wrote {written} rows to {config['output_file']}")


if __name__ == "__main__":
    main()
