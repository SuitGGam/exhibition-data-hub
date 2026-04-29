import argparse
import csv
import html
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError
from pathlib import Path
from typing import Iterable


NAVER_LOCAL_API_URL = "https://openapi.naver.com/v1/search/local.json"
REQUEST_TIMEOUT_SECONDS = 8
REQUEST_RETRY_COUNT = 2
REQUEST_PAUSE_SECONDS = 0.1
LOCAL_SEARCH_START = 1
LOCAL_SEARCH_MIN_DISPLAY = 1
LOCAL_SEARCH_MAX_DISPLAY = 5
LOCAL_SEARCH_ALLOWED_SORT = {"random", "comment"}
DEFAULT_LOCAL_DISPLAY = 5
DEFAULT_LOCAL_SORT = "random"
DEFAULT_BATCH_SIZE = 25000
DEFAULT_PROGRESS_PATH = "data/naver_local_progress.json"

CSV_FIELDNAMES = [
    "title",
    "link",
    "start_date",
    "end_date",
    "full_address",
    "road_address",
    "region_main",
    "region_sub",
    "region_detail",
    "official_url",
    "summary",
    "tel",
    "category",
    "price",
    "mapx",
    "mapy",
    "source_query",
]


def load_env_from_dotenv(dotenv_path: Path) -> None:
    """Load KEY=VALUE pairs from .env into process environment."""
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def load_markdown_bullets(markdown_path: Path, item_label: str) -> list[str]:
    """Parse unique bullet items from markdown lines such as '- 서울'."""
    if not markdown_path.exists():
        raise FileNotFoundError(f"{item_label.capitalize()} list file not found: {markdown_path}")

    text = markdown_path.read_text(encoding="utf-8")
    regions = []
    seen = set()
    for line in text.splitlines():
        match = re.match(r"^\s*[-*]\s+(.+?)\s*$", line)
        if match:
            value = match.group(1).strip()
            if value and value not in seen:
                regions.append(value)
                seen.add(value)

    if not regions:
        raise ValueError(
            f"No {item_label} found. Add bullet items like '- example' in {markdown_path.name}"
        )
    return regions


def load_regions(markdown_path: Path) -> list[str]:
    """Load region names from docs/regions.md bullet items."""
    return load_markdown_bullets(markdown_path, "regions")


def load_keywords(markdown_path: Path) -> list[str]:
    """Load exhibition keywords from docs/keywords.md bullet items."""
    return load_markdown_bullets(markdown_path, "keywords")


def parse_address_parts(address: str) -> tuple[str, str, str]:
    """Split a Korean address into main/sub/detail using spaces."""
    tokens = [token for token in address.split() if token]
    region_main = tokens[0] if len(tokens) >= 1 else ""
    region_sub = ""
    region_detail = ""

    if len(tokens) >= 2:
        second = tokens[1]
        # 광역시는 일반적으로 시/군/구 중분류가 생략되는 경우가 있어 보정합니다.
        if second.endswith(("구", "군", "시")):
            region_sub = second
            region_detail = tokens[2] if len(tokens) >= 3 else ""
        else:
            region_detail = second

    return region_main, region_sub, region_detail


def clean_text(value: str) -> str:
    """Remove HTML tags and decode escaped entities from API response text."""
    no_tags = re.sub(r"<[^>]+>", "", value or "")
    return html.unescape(no_tags).strip()


def search_local(
    query: str,
    client_id: str,
    client_secret: str,
    display: int = DEFAULT_LOCAL_DISPLAY,
    sort: str = DEFAULT_LOCAL_SORT,
) -> list[dict]:
    if not query.strip():
        raise ValueError("query must not be empty")

    if display < LOCAL_SEARCH_MIN_DISPLAY or display > LOCAL_SEARCH_MAX_DISPLAY:
        raise ValueError(
            f"display must be between {LOCAL_SEARCH_MIN_DISPLAY} and {LOCAL_SEARCH_MAX_DISPLAY}"
        )

    normalized_sort = sort.strip().lower()
    if normalized_sort not in LOCAL_SEARCH_ALLOWED_SORT:
        raise ValueError(
            f"sort must be one of: {', '.join(sorted(LOCAL_SEARCH_ALLOWED_SORT))}"
        )

    params = urllib.parse.urlencode({
        "query": query,
        "display": display,
        "start": LOCAL_SEARCH_START,
        "sort": normalized_sort,
    })
    url = f"{NAVER_LOCAL_API_URL}?{params}"

    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", client_id)
    request.add_header("X-Naver-Client-Secret", client_secret)

    last_error: Exception | None = None
    for attempt in range(1, REQUEST_RETRY_COUNT + 1):
        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                if response.getcode() != 200:
                    raise RuntimeError(f"Naver API request failed: HTTP {response.getcode()}")
                payload = json.loads(response.read().decode("utf-8"))
                return payload.get("items", [])
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < REQUEST_RETRY_COUNT:
                time.sleep(0.25 * attempt)

    raise RuntimeError(f"Naver API request failed after retries: {last_error}")


def write_csv(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def append_csv_rows(rows: Iterable[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = output_path.exists() and output_path.stat().st_size > 0
    mode = "a" if file_exists else "w"
    with output_path.open(mode, encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def load_progress(progress_path: Path) -> dict:
    if not progress_path.exists():
        return {}
    try:
        return json.loads(progress_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_progress(
    progress_path: Path,
    last_processed_index: int,
    next_start_index: int,
    total_queries: int,
    appended_rows: int,
) -> None:
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_processed_index": last_processed_index,
        "next_start_index": next_start_index,
        "total_queries": total_queries,
        "appended_rows": appended_rows,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    progress_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    load_env_from_dotenv(project_root / ".env")
    parser = argparse.ArgumentParser(description="Search Naver Local for exhibitions (supports resume by index)")
    parser.add_argument("--start-index", type=int, default=1, help="1-based start index within the region×keyword combinations")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Number of combinations to process in this run")
    parser.add_argument(
        "--progress-file",
        default=DEFAULT_PROGRESS_PATH,
        help="Progress JSON path used for automatic resume",
    )
    parser.add_argument(
        "--no-auto-resume",
        action="store_true",
        help="Disable automatic resume from progress file",
    )
    args = parser.parse_args()

    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")

    if not client_id or not client_secret:
        print("Missing NAVER_CLIENT_ID or NAVER_CLIENT_SECRET in environment.", file=sys.stderr)
        print("Create .env from .env.example and fill your API credentials.", file=sys.stderr)
        return 1

    try:
        regions = load_regions(project_root / "docs" / "regions.md")
        keywords = load_keywords(project_root / "docs" / "keywords.md")
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    local_display = DEFAULT_LOCAL_DISPLAY
    local_sort = os.getenv("NAVER_LOCAL_SORT", DEFAULT_LOCAL_SORT)
    progress_path = project_root / args.progress_file

    # Load existing rows to avoid duplicates when appending
    output_path = project_root / "data" / "naver_local_exhibitions.csv"
    existing = read_csv_rows(output_path)
    seen = set()
    if existing:
        for er in existing:
            dedupe_key = (er.get("title", ""), er.get("full_address", ""), er.get("official_url", ""))
            seen.add(dedupe_key)

    total_queries = len(regions) * len(keywords)

    start_index = max(1, args.start_index)
    progress = load_progress(progress_path)
    progress_next_start = int(progress.get("next_start_index", 1) or 1)
    if not args.no_auto_resume and args.start_index == 1 and progress_next_start > 1:
        start_index = progress_next_start

    batch_size = max(1, args.batch_size)
    end_index = min(total_queries, start_index + batch_size - 1)

    if start_index > total_queries:
        print(f"start-index {start_index} is beyond total combinations ({total_queries}). Nothing to do.")
        return 0

    new_rows: list[dict] = []
    query_index = 0
    processed = 0
    last_processed_index = start_index - 1

    print(
        f"Running combinations {start_index}..{end_index} / {total_queries} "
        f"(auto-resume={'off' if args.no_auto_resume else 'on'})"
    )

    for region in regions:
        for keyword in keywords:
            query_index += 1
            if query_index < start_index:
                continue
            if query_index > end_index:
                break

            query = f"{region} {keyword}"
            print(f"[{query_index}/{total_queries}] Searching: {query}")
            try:
                items = search_local(
                    query,
                    client_id,
                    client_secret,
                    display=local_display,
                    sort=local_sort,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"[WARN] {query}: {exc}", file=sys.stderr)
            else:
                for item in items:
                    address = item.get("address", "")
                    road_address = item.get("roadAddress", "")
                    region_main, region_sub, region_detail = parse_address_parts(address)
                    mapx = item.get("mapx")
                    mapy = item.get("mapy")
                    row = {
                        "title": clean_text(item.get("title", "")),
                        "link": item.get("link", ""),
                        "start_date": "",
                        "end_date": "",
                        "full_address": address,
                        "road_address": road_address,
                        "region_main": region_main,
                        "region_sub": region_sub,
                        "region_detail": region_detail,
                        "official_url": item.get("link", ""),
                        "summary": clean_text(item.get("description", "")),
                        "tel": item.get("telephone", ""),
                        "category": item.get("category", ""),
                        "price": "unknown",
                        "mapx": mapx if mapx else "",
                        "mapy": mapy if mapy else "",
                        "source_query": query,
                    }
                    # Prevent duplicate rows caused by overlapping keywords or previous runs.
                    dedupe_key = (row["title"], row["full_address"], row["official_url"])
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    new_rows.append(row)
            finally:
                # Mark this query index as processed even if request failed, then resume after it.
                last_processed_index = query_index
                processed += 1
                save_progress(
                    progress_path,
                    last_processed_index=last_processed_index,
                    next_start_index=min(total_queries + 1, last_processed_index + 1),
                    total_queries=total_queries,
                    appended_rows=len(new_rows),
                )
                # Prevent request bursts that can trigger API throttling.
                time.sleep(REQUEST_PAUSE_SECONDS)
        # break outer if we've passed the end index
        if query_index >= end_index:
            break

    if new_rows:
        append_csv_rows(new_rows, output_path)
    save_progress(
        progress_path,
        last_processed_index=last_processed_index,
        next_start_index=min(total_queries + 1, last_processed_index + 1),
        total_queries=total_queries,
        appended_rows=len(new_rows),
    )
    print(f"Appended {len(new_rows)} new rows to {output_path} (processed {processed} queries from {start_index} to {end_index})")
    print(f"Progress saved to {progress_path} (next start index: {min(total_queries + 1, last_processed_index + 1)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
