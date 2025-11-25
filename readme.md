# Backfill First Status (Jira Cloud & Data Center)

This script backfills a **“First status”** custom field for Jira issues by reading their **changelogs** and computing the earliest status for each issue.

It supports:

- **Jira Cloud** (enhanced JQL search API)
- **Jira Data Center / Server** (classic JQL search API)
- Custom **SSL verification** (e.g. internal certs, custom CA bundle)

---

## Features

- Works with **Cloud** and **Data Center/Server**
- Parallel processing of issues using their **changelog**
- Optional update of a **custom field** with the calculated first status
- `--dry-run` mode (no updates, just prints what it would do)
- `--print-json` to emit machine-readable output
- Configurable SSL verification via `.env`

---

## Requirements

- **Python** 3.8+
- pip packages:
  - `requests`
  - `python-dotenv`
  - `tqdm`

Install dependencies:

```bash
pip install requests python-dotenv tqdm
```

--- 

## Usage

### Basic syntax:

```bash
python backfill_first_status.py \
  --jql "<your JQL>" \
  --target <cloud OR dc> \
  --concurrency 10 
```
