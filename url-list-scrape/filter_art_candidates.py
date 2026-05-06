#!/usr/bin/env python3
"""
Keep only rows classified as art venue yes/maybe.
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path


DEFAULT_INPUT_NAME = "naver_local_exhibitions_art_verified.csv"
DEFAULT_OUTPUT_NAME = "naver_local_exhibitions_art_candidates_yes_maybe.csv"


def load_env_file(env_file: str = "") -> None:
    paths: list[Path] = []
    if env_file:
        paths.append(Path(env_file))
    else:
        paths.extend([Path.cwd() / ".env", Path(__file__).resolve().parent / ".env"])

    seen: set[Path] = set()
    for path in paths:
        path = path.expanduser()
        if path in seen or not path.exists():
            continue
        seen.add(path)
        for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                os.environ.setdefault(key, value)


def default_data_path(filename: str) -> str:
    data_dir = os.getenv("ARTMOA_DATA_DIR")
    base = Path(data_dir) if data_dir else Path(__file__).resolve().parent / "data"
    return str(base / filename)


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
    parser.add_argument("-i", "--input", default="", help="Input verified CSV. Defaults to ARTMOA_DATA_DIR/input filename.")
    parser.add_argument("-o", "--output", default="", help="Output yes/maybe candidate CSV. Defaults to ARTMOA_DATA_DIR/output filename.")
    parser.add_argument("--encoding", default="utf-8-sig", help="Input/output CSV encoding.")
    parser.add_argument("--env-file", default="", help="Optional .env file path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env_file(args.env_file)
    args.input = args.input or default_data_path(DEFAULT_INPUT_NAME)
    args.output = args.output or default_data_path(DEFAULT_OUTPUT_NAME)
    count = filter_rows(args.input, args.output, args.encoding)
    print(f"Candidate rows: {count:,}")
    print(f"Saved: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
