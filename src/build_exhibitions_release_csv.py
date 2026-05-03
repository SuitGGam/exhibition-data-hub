"""Build a cleaned exhibition CSV with detail URLs and filled end dates."""

from __future__ import annotations

import argparse
import csv
import html
import io
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path


DEFAULT_INPUT_CSV = "data/exhibitions_valid_data.csv"
DEFAULT_OUTPUT_CSV = "data/exhibitions_release.csv"
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_PAUSE_SECONDS = 0.2
DEFAULT_LIMIT = 0
DEFAULT_MIN_MATCH_TOKENS = 2
DEFAULT_LOG_EVERY = 50
DEFAULT_MAX_LINK_CANDIDATES = 5

OUTPUT_FIELDNAMES = [
    "exhibition_name",
    "exhibition_start_date",
    "exhibition_end_date",
    "institution_name",
    "exhibition_address",
    "exhibition_detail_url",
    "institution_url",
]

DATE_RANGE_PATTERNS = [
    re.compile(
        r"(?P<s>\d{4}[./-]\d{1,2}[./-]\d{1,2})(?:\([^\)]*\))?\s*(?:~|-|–|—|to)\s*(?P<e>\d{4}[./-]\d{1,2}[./-]\d{1,2})(?:\([^\)]*\))?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?P<s>\d{4}[./-]\d{1,2}[./-]\d{1,2})(?:\([^\)]*\))?\s*(?:~|-|–|—|to)\s*(?P<e>\d{1,2}[./-]\d{1,2})(?:\([^\)]*\))?",
        re.IGNORECASE,
    ),
]
SINGLE_DATE_PATTERN = re.compile(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}")

ADDRESS_PATTERNS = [
    re.compile(r"(?:주소|장소|전시장소|전시장|Location)\s*[:：]?\s*([^\n\r|]{5,120})", re.IGNORECASE),
    re.compile(r"[가-힣]{2,}(?:특별시|광역시|도|시|군|구)[^\n\r|]{0,60}(?:로|길)\s*\d+[\w-]*"),
    re.compile(r"\d+[^\n\r|]{0,80}(?:-ro|-gil|road|rd|street|st|avenue|ave|boulevard|blvd)[^\n\r|]{0,40}", re.IGNORECASE),
]


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._active = False
        self._href = ""
        self._text_parts: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            self._active = True
            attrs_map = dict(attrs)
            self._href = attrs_map.get("href") or ""
            self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._active and data.strip():
            self._text_parts.append(data.strip())

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._active:
            text = " ".join(self._text_parts).strip()
            self.links.append((self._href, text))
            self._active = False
            self._href = ""
            self._text_parts = []


@dataclass
class CandidateLink:
    url: str
    text: str
    score: int


def normalize_text(value: str) -> str:
    value = html.unescape(value or "")
    value = unicodedata.normalize("NFKC", value)
    return " ".join(value.split()).strip().lower()


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


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def fetch_text(url: str, timeout_seconds: float) -> tuple[str, int | None, str]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; ExhibitionRelease/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=max(0.5, timeout_seconds)) as response:
            status = getattr(response, "status", None)
            text = response.read().decode(response.headers.get_content_charset() or "utf-8", errors="replace")
            return text, status, response.geturl()
    except urllib.error.HTTPError as exc:
        return str(exc), exc.code, url
    except urllib.error.URLError as exc:
        return str(exc), None, url
    except Exception as exc:  # noqa: BLE001
        return str(exc), None, url


def extract_links(base_url: str, html_text: str) -> list[tuple[str, str]]:
    parser = LinkExtractor()
    parser.feed(html_text)
    links: list[tuple[str, str]] = []
    for href, text in parser.links:
        if not href:
            continue
        full_url = urllib.parse.urljoin(base_url, href)
        links.append((full_url, text))
    return links


def token_candidates(value: str) -> list[str]:
    text = normalize_text(value)
    if not text:
        return []
    tokens: list[str] = []
    for raw in re.split(r"[^0-9a-zA-Z가-힣]+", text):
        token = raw.strip()
        if len(token) >= 2 and token not in tokens:
            tokens.append(token)
    return tokens


def score_link(event_name: str, url: str, text: str) -> int:
    score = 0
    norm_event = normalize_text(event_name)
    norm_text = normalize_text(text)
    if norm_event and norm_event in norm_text:
        score += 5

    tokens = token_candidates(event_name)
    matched = [token for token in tokens if token in norm_text]
    score += len(matched)

    norm_url = normalize_text(url)
    matched_url = [token for token in tokens if token in norm_url]
    score += len(matched_url)

    return score


def pick_detail_candidates(event_name: str, links: list[tuple[str, str]], min_match_tokens: int) -> list[CandidateLink]:
    candidates: list[CandidateLink] = []
    for url, text in links:
        score = score_link(event_name, url, text)
        if score >= min_match_tokens:
            candidates.append(CandidateLink(url=url, text=text, score=score))

    candidates.sort(key=lambda item: item.score, reverse=True)
    return candidates


def parse_date_range(text: str) -> tuple[str, str]:
    norm = normalize_text(text)
    for pattern in DATE_RANGE_PATTERNS:
        match = pattern.search(norm)
        if match:
            start = match.group("s").replace(".", "-").replace("/", "-")
            end = match.group("e").replace(".", "-").replace("/", "-")
            if len(end) == 5 and len(start) >= 10:
                end = start[:5] + end
            return start[:10], end[:10]

    singles = SINGLE_DATE_PATTERN.findall(norm)
    if singles:
        first = singles[0].replace(".", "-").replace("/", "-")
        return first[:10], ""
    return "", ""


def extract_address(text: str) -> str:
    if not text:
        return ""
    for pattern in ADDRESS_PATTERNS:
        match = pattern.search(text)
        if match:
            if match.lastindex:
                value = match.group(1)
            else:
                value = match.group(0)
            value = " ".join(value.split()).strip()
            if value:
                return value
    return ""


def choose_detail_url(
    row: dict[str, str],
    list_html: str,
    list_final_url: str,
    timeout_seconds: float,
    min_match_tokens: int,
    max_candidates: int,
    pause_seconds: float,
) -> tuple[str, str, str]:
    event_name = str(row.get("event_name", "") or "")
    if not event_name:
        return "", "", ""

    links = extract_links(list_final_url, list_html)
    candidates = pick_detail_candidates(event_name, links, min_match_tokens)
    if not candidates:
        return "", "", ""

    best_url = ""
    best_text = ""
    best_score = -1

    for candidate in candidates[:max_candidates]:
        html_text, status, final_url = fetch_text(candidate.url, timeout_seconds)
        detail_text = normalize_text(html_text)
        score = candidate.score
        if detail_text and normalize_text(event_name) in detail_text:
            score += 5
        if str(row.get("start_date", "") or "") and str(row.get("start_date", "")) in detail_text:
            score += 2
        if str(row.get("end_date", "") or "") and str(row.get("end_date", "")) in detail_text:
            score += 2
        if score > best_score:
            best_score = score
            best_url = final_url
            best_text = candidate.text
        if pause_seconds > 0:
            time.sleep(pause_seconds)

    return best_url, best_text, str(best_score if best_score >= 0 else "")


def build_output_rows(
    rows: list[dict[str, str]],
    timeout_seconds: float,
    pause_seconds: float,
    min_match_tokens: int,
    max_candidates: int,
    log_every: int,
) -> list[dict[str, str]]:
    cache_list_page: dict[str, tuple[str, int | None, str]] = {}

    output_rows: list[dict[str, str]] = []
    missing_end_dates = 0
    fallback_end_dates = 0

    for index, row in enumerate(rows, start=1):
        institution_name = str(row.get("institution_title", "") or "")
        institution_url = str(row.get("institution_url", "") or "")
        source_url = str(row.get("source_page_url", "") or "")
        event_name = str(row.get("event_name", "") or "")

        start_date = str(row.get("start_date", "") or "")
        end_date = str(row.get("end_date", "") or "")

        description = str(row.get("description", "") or "")
        evidence = str(row.get("evidence", "") or "")

        detail_url = ""
        address = extract_address(description) or extract_address(evidence)

        if source_url:
            if source_url not in cache_list_page:
                list_html, status, list_final_url = fetch_text(source_url, timeout_seconds)
                cache_list_page[source_url] = (list_html, status, list_final_url)
                if pause_seconds > 0:
                    time.sleep(pause_seconds)
            else:
                list_html, status, list_final_url = cache_list_page[source_url]

            detail_url, _, _ = choose_detail_url(
                row,
                list_html,
                list_final_url,
                timeout_seconds,
                min_match_tokens,
                max_candidates,
                pause_seconds,
            )

        if not detail_url:
            detail_url = source_url

        detail_html = ""
        if detail_url:
            detail_html, _, _ = fetch_text(detail_url, timeout_seconds)
            if pause_seconds > 0:
                time.sleep(pause_seconds)

        if not start_date or not end_date:
            s_date, e_date = parse_date_range(description)
            if not start_date and s_date:
                start_date = s_date
            if not end_date and e_date:
                end_date = e_date

        if not start_date or not end_date:
            s_date, e_date = parse_date_range(evidence)
            if not start_date and s_date:
                start_date = s_date
            if not end_date and e_date:
                end_date = e_date

        if not start_date or not end_date:
            s_date, e_date = parse_date_range(detail_html)
            if not start_date and s_date:
                start_date = s_date
            if not end_date and e_date:
                end_date = e_date

        if not address:
            address = extract_address(detail_html)

        if not end_date:
            missing_end_dates += 1
            if start_date:
                end_date = start_date
                fallback_end_dates += 1

        output_rows.append(
            {
                "exhibition_name": event_name,
                "exhibition_start_date": start_date,
                "exhibition_end_date": end_date,
                "institution_name": institution_name,
                "exhibition_address": address,
                "exhibition_detail_url": detail_url,
                "institution_url": institution_url,
            }
        )

        if log_every > 0 and index % log_every == 0:
            print(f"[BUILD] processed {index}/{len(rows)} rows")

    if missing_end_dates:
        print(f"[WARN] missing end_date after parse: {missing_end_dates}")
        if fallback_end_dates:
            print(f"[WARN] fallback end_date=start_date applied: {fallback_end_dates}")

    return output_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build exhibition release CSV from exhibitions_valid_data.csv")
    parser.add_argument("--input", default=DEFAULT_INPUT_CSV, help="Input CSV path")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_CSV, help="Output CSV path")
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="Fetch timeout in seconds")
    parser.add_argument("--pause-seconds", type=float, default=DEFAULT_PAUSE_SECONDS, help="Pause between requests")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Process only first N rows. 0 means all rows.")
    parser.add_argument("--min-match-tokens", type=int, default=DEFAULT_MIN_MATCH_TOKENS, help="Minimum token matches for detail link")
    parser.add_argument("--max-link-candidates", type=int, default=DEFAULT_MAX_LINK_CANDIDATES, help="Max detail candidates to fetch per row")
    parser.add_argument("--log-every", type=int, default=DEFAULT_LOG_EVERY, help="Log progress every N rows. 0 disables logging.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    root = Path(__file__).resolve().parent.parent
    input_path = root / args.input
    output_path = root / args.output

    rows = load_rows(input_path)
    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    output_rows = build_output_rows(
        rows,
        args.timeout_seconds,
        args.pause_seconds,
        args.min_match_tokens,
        args.max_link_candidates,
        args.log_every,
    )
    write_rows(output_path, output_rows)
    print(f"Saved {len(output_rows)} rows -> {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
