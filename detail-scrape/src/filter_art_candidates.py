#!/usr/bin/env python3
"""
Keep only rows classified as art venue yes/maybe.
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path


DEFAULT_INPUT = str(Path("data") / "naver_local_exhibitions_art_verified.csv")
DEFAULT_OUTPUT = str(Path("data") / "naver_local_exhibitions_art_candidates_yes_maybe.csv")


def filter_rows(input_path: str, output_path: str, encoding: str) -> int:
    with open(input_path, "r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [row for row in reader if (row.get("is_art_venue") or "").strip() in {"yes", "maybe"}]
        fieldnames = list(reader.fieldnames or [])

    tmp_path = f"{output_path}.tmp"
    with open(tmp_path, "w", encoding=encoding, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, output_path)
    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract yes/maybe art venue rows from verified CSV.")
    parser.add_argument("-i", "--input", default=DEFAULT_INPUT, help="Input verified CSV.")
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT, help="Output yes/maybe candidate CSV.")
    parser.add_argument("--encoding", default="utf-8-sig", help="Input/output CSV encoding.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    count = filter_rows(args.input, args.output, args.encoding)
    print(f"Candidate rows: {count:,}")
    print(f"Saved: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
