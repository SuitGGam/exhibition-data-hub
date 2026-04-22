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
    fieldnames = [
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

    with output_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    load_env_from_dotenv(project_root / ".env")

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

    rows = []
    seen = set()
    total_queries = len(regions) * len(keywords)
    query_index = 0

    for region in regions:
        for keyword in keywords:
            query_index += 1
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
                continue

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
                # Prevent duplicate rows caused by overlapping keywords.
                dedupe_key = (row["title"], row["full_address"], row["official_url"])
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                rows.append(row)

            # Prevent request bursts that can trigger API throttling.
            time.sleep(REQUEST_PAUSE_SECONDS)

    output_path = project_root / "data" / "naver_local_exhibitions.csv"
    write_csv(rows, output_path)
    print(f"Saved {len(rows)} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
