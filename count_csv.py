#!/usr/bin/env python3
"""
Count lines in each CSV file in a folder and write a summary CSV.

Features:
- Counts total lines in each CSV.
- Optional: exclude header row from the count (via --exclude-header).
- Supports recursive scanning (via --recursive).
- Lets you customize file pattern (default: *.csv).
"""

import argparse
import csv
import sys
from pathlib import Path

def count_lines(path: Path) -> int:
    # Count lines efficiently without loading the whole file in memory
    # Uses text mode with universal newlines
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        return sum(1 for _ in f)

def main():
    parser = argparse.ArgumentParser(description="Count lines in CSV files and produce a summary CSV.")
    parser.add_argument("input_dir", type=Path, help="Folder containing CSV files")
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("csv_line_counts.csv"),
        help="Path to write the summary CSV (default: ./csv_line_counts.csv)"
    )
    parser.add_argument(
        "-p", "--pattern",
        default="*.csv",
        help="Glob pattern for CSV files (default: *.csv). Example: '*_export*.csv'"
    )
    parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="Recurse into subfolders"
    )
    parser.add_argument(
        "--exclude-header",
        action="store_true",
        help="Also compute a 'data_rows' column that subtracts 1 header row (min 0)."
    )
    args = parser.parse_args()

    if not args.input_dir.exists() or not args.input_dir.is_dir():
        print(f"Error: {args.input_dir} is not a directory.", file=sys.stderr)
        sys.exit(1)

    # Gather files
    files = list(args.input_dir.rglob(args.pattern) if args.recursive else args.input_dir.glob(args.pattern))
    files = [f for f in files if f.is_file()]

    if not files:
        print("No files matched your pattern. Nothing to do.")
        # Still write an empty CSV with headers for consistency
        with args.output.open("w", newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            header = ["file_path", "file_name", "total_lines"]
            if args.exclude_header:
                header.append("data_rows")
            writer.writerow(header)
        return

    # Prepare output
    with args.output.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        header = ["file_path", "file_name", "total_lines"]
        if args.exclude_header:
            header.append("data_rows")  # total_lines - 1 but not below 0
        writer.writerow(header)

        for fpath in sorted(files):
            try:
                total = count_lines(fpath)
                row = [str(fpath), fpath.name, total]
                if args.exclude_header:
                    data_rows = max(0, total - 1)
                    row.append(data_rows)
                writer.writerow(row)
            except Exception as e:
                # Write an error row for visibility (you can remove if undesired)
                err_name = f"(ERROR) {fpath.name}"
                row = [str(fpath), err_name, ""]
                if args.exclude_header:
                    row.append("")
                writer.writerow(row)
                print(f"Failed to read {fpath}: {e}", file=sys.stderr)

    print(f"Done. Wrote summary to: {args.output.resolve()}")

if __name__ == "__main__":
    main()
