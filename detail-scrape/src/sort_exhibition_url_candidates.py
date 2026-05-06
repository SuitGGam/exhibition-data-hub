#!/usr/bin/env python3
"""
Sort exhibition URL candidate rows by venue_index, then rank.
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path


DEFAULT_INPUT = str(Path("data") / "naver_local_exhibitions_exhibition_url_candidates.csv")
DEFAULT_OUTPUT = str(Path("data") / "naver_local_exhibitions_exhibition_url_candidates_sorted.csv")


def to_int(value: str, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def sort_csv(input_path: str, output_path: str, encoding: str) -> int:
    with open(input_path, "r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    rows.sort(key=lambda row: (to_int(row.get("venue_index", "")), to_int(row.get("rank", ""))))

    tmp_path = f"{output_path}.tmp"
    with open(tmp_path, "w", encoding=encoding, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, output_path)
    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sort candidate URL CSV by venue_index and rank.")
    parser.add_argument("-i", "--input", default=DEFAULT_INPUT, help="Input candidate URL CSV.")
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT, help="Sorted output CSV.")
    parser.add_argument("--encoding", default="utf-8-sig", help="Input/output CSV encoding.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    count = sort_csv(args.input, args.output, args.encoding)
    print(f"Sorted rows: {count:,}")
    print(f"Saved: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
