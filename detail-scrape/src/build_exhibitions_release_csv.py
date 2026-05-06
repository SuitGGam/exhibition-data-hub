"""Build a release CSV from extracted exhibition list-page results."""

from __future__ import annotations

import argparse
import csv
import io
from pathlib import Path

DEFAULT_INPUT_CSV = "data/extracted_exhibitions.csv"
DEFAULT_OUTPUT_CSV = "data/exhibitions_release.csv"

OUTPUT_FIELDNAMES_BASE = [
    "exhibition_name",
    "exhibition_start_date",
    "exhibition_end_date",
    "exhibition_location",
    "exhibition_detail_url",
    "source_list_url",
]


def normalize_text(value: str) -> str:
    return " ".join((value or "").split())


def load_rows(path: Path) -> list[dict[str, str]]:
    raw_bytes = path.read_bytes()
    for encoding in ("utf-8-sig", "cp949", "utf-8"):
        try:
            text = raw_bytes.decode(encoding)
            handle = io.StringIO(text, newline="")
            return list(csv.DictReader(handle))
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8", raw_bytes, 0, 1, "Unable to decode CSV")


def write_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_rows(
    rows: list[dict[str, str]],
    pass_through_columns: list[str],
    fill_missing_end_date: bool,
) -> list[dict[str, str]]:
    output_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()

    for row in rows:
        name = normalize_text(row.get("exhibition_name", ""))
        start_date = normalize_text(row.get("start_date", ""))
        end_date = normalize_text(row.get("end_date", ""))
        location = normalize_text(row.get("location", ""))
        detail_url = normalize_text(row.get("detail_url", ""))
        source_list_url = normalize_text(row.get("source_list_url", ""))

        if fill_missing_end_date and start_date and not end_date:
            end_date = start_date

        if not name:
            continue
        if not start_date and not end_date:
            continue

        key = (name.lower(), start_date, end_date, detail_url)
        if key in seen:
            continue
        seen.add(key)

        record = {
            "exhibition_name": name,
            "exhibition_start_date": start_date,
            "exhibition_end_date": end_date,
            "exhibition_location": location,
            "exhibition_detail_url": detail_url,
            "source_list_url": source_list_url,
        }

        for col in pass_through_columns:
            record[col] = normalize_text(row.get(col, ""))

        output_rows.append(record)

    return output_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build release CSV from extracted exhibitions.")
    parser.add_argument("--input", default=DEFAULT_INPUT_CSV, help="Input CSV path")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_CSV, help="Output CSV path")
    parser.add_argument(
        "--pass-through-columns",
        default="",
        help="Comma-separated input columns to include in output",
    )
    parser.add_argument(
        "--fill-missing-end-date",
        action="store_true",
        help="Fill missing end_date with start_date",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent
    input_path = root / args.input
    output_path = root / args.output

    if not input_path.exists():
        raise SystemExit(f"Input CSV not found: {input_path}")

    rows = load_rows(input_path)
    if not rows:
        raise SystemExit("Input CSV is empty.")

    pass_through_columns = [col.strip() for col in str(args.pass_through_columns).split(",") if col.strip()]
    output_fieldnames = OUTPUT_FIELDNAMES_BASE + [
        col for col in pass_through_columns if col not in OUTPUT_FIELDNAMES_BASE
    ]

    output_rows = build_rows(rows, pass_through_columns, bool(args.fill_missing_end_date))
    write_rows(output_path, output_rows, output_fieldnames)
    print(f"Saved {len(output_rows)} rows -> {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())