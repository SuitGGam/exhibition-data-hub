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
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError

DEFAULT_INPUT_CSV = "data/test_naver_local_exhibitions.csv"
DEFAULT_OUTPUT_CSV = "data/extracted_exhibitions.csv"
DEFAULT_FAILED_DOMAINS_OUT = "data/failed_domains.csv"
DEFAULT_TIMEOUT_SECONDS = 8
DEFAULT_PAUSE_SECONDS = 0.15
DEFAULT_HTTP_RETRY_COUNT = 2
DEFAULT_MAX_PAGES_PER_INSTITUTION = 8
DEFAULT_MAX_INSTITUTIONS = 0
DEFAULT_MIN_CONFIDENCE = 0.75

EXCLUDE_CATEGORY_KEYWORDS = [
    "자동차",
    "중고차",
    "이벤트,파티",
    "행사대행",
    "주차장",
    "음식점",
    "카페,디저트",
    "부동산",
    "인테리어디자인",
    "학원",
    "병원",
    "숙박",
]

INCLUDE_CATEGORY_KEYWORDS = [
    "문화,예술",
    "박물관",
    "미술관",
    "갤러리",
    "화랑",
    "과학관",
    "기념관",
    "전시관",
]

TITLE_HINT_KEYWORDS = [
    "미술관",
    "박물관",
    "갤러리",
    "화랑",
    "아트센터",
    "기념관",
    "과학관",
    "문화원",
    "전시관",
    "아트홀",
    "문화예술회관",
]

PAGE_HINT_KEYWORDS = [
    "전시",
    "exhibition",
    "event",
    "program",
    "calendar",
    "notice",
    "news",
    "whatson",
    "archive",
    "show",
]

SKIP_PAGE_URL_KEYWORDS = [
    "faq",
    "qna",
    "newsletter",
    "sitemap",
    "privacy",
    "policy",
    "board",
    "bbs",
    "search",
    "notice",
]

EVENT_TITLE_KEYWORDS = [
    "전시",
    "전시회",
    "개인전",
    "기획전",
    "특별전",
    "초대전",
    "그룹전",
    "아트페어",
    "비엔날레",
    "트리엔날레",
    "사진전",
    "조각전",
    "졸업전시",
    "학위청구",
    "오픈스튜디오",
    "레지던시",
    "전시안내",
    "전시 일정",
]

STRONG_EVENT_TITLE_KEYWORDS = [
    "전시회",
    "개인전",
    "기획전",
    "특별전",
    "초대전",
    "그룹전",
    "아트페어",
    "비엔날레",
    "트리엔날레",
    "사진전",
    "조각전",
    "졸업전시",
    "학위청구",
    "오픈스튜디오",
    "레지던시",
    "전시안내",
    "전시 일정",
]

GENERIC_EVENT_LABELS = {
    "전시",
    "전시회",
    "현재전시",
    "예정전시",
    "과거전시",
    "전시기간",
    "전시장소",
    "전시해설",
    "상설전시",
    "기획전시",
    "전시·체험",
    "전시체험",
    "전시안내",
}

EXCLUDE_EVENT_TEXT_KEYWORDS = [
    "채용",
    "공고",
    "합격자",
    "서류심사",
    "면접",
    "모집",
    "일자리",
    "FAQ",
    "문의",
]

LOCATION_TEXT_KEYWORDS = [
    "전시실",
    "전시장소",
    "장소",
    "관람",
    "입장",
    "예약",
    "프로그램",
    "운영",
    "안내",
    "층",
    "호",
]

PRICE_WON_PATTERN = re.compile(r"(\d[\d,]{0,8})\s*원")
DATE_RANGE_PATTERN = re.compile(
    r"(?P<s>\d{4}[./-]\d{1,2}[./-]\d{1,2})\s*(?:~|-|–|—|to)\s*(?P<e>\d{4}[./-]\d{1,2}[./-]\d{1,2}|\d{1,2}[./-]\d{1,2})",
    re.IGNORECASE,
)
SINGLE_DATE_PATTERN = re.compile(r"(\d{4}[./-]\d{1,2}[./-]\d{1,2})")


@dataclass
class Institution:
    institution_id: str
    title: str
    category: str
    official_url: str
    region_main: str
    region_sub: str
    source_query: str


@dataclass
class FailureRecord:
    institution_id: str
    institution_title: str
    domain: str
    url: str
    stage: str
    error_type: str
    error_message: str


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._active = False
        self._href = ""
        self._text_parts: list[str] = []

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


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._skip_depth = 0
        self._skip_tags = {"script", "style", "noscript"}
        self.lines: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in self._skip_tags:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in self._skip_tags and self._skip_depth > 0:
            self._skip_depth -= 1
        if tag.lower() in {"p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "div", "section", "article", "br", "tr"}:
            self.lines.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = " ".join(data.split())
        if text:
            self.lines.append(text)


class JsonLdExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._active = False
        self._buffer: list[str] = []
        self.payloads: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "script":
            return
        attrs_map = {k.lower(): (v or "") for k, v in attrs}
        if attrs_map.get("type", "").lower() == "application/ld+json":
            self._active = True
            self._buffer = []

    def handle_data(self, data: str) -> None:
        if self._active:
            self._buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "script" and self._active:
            content = "".join(self._buffer).strip()
            if content:
                self.payloads.append(content)
            self._active = False
            self._buffer = []


def normalize_text(value: str) -> str:
    return " ".join((value or "").split())


def parse_domain(url: str) -> str:
    return urllib.parse.urlparse(url).netloc.lower()


def add_failure(
    failures: list[FailureRecord],
    institution: Institution,
    url: str,
    stage: str,
    error: Exception,
) -> None:
    failures.append(
        FailureRecord(
            institution_id=institution.institution_id,
            institution_title=institution.title,
            domain=parse_domain(url),
            url=url,
            stage=stage,
            error_type=type(error).__name__,
            error_message=str(error),
        )
    )


def load_markdown_bullets(path: Path) -> list[str]:
    if not path.exists():
        return []
    values: list[str] = []
    seen: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^\s*[-*]\s+(.+?)\s*$", line)
        if not match:
            continue
        item = normalize_text(match.group(1))
        if item and item not in seen:
            values.append(item)
            seen.add(item)
    return values


def canonical_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    candidate = raw_url.strip()
    if not candidate:
        return ""
    if not candidate.startswith(("http://", "https://")):
        candidate = "https://" + candidate
    parsed = urllib.parse.urlparse(candidate)
    if not parsed.netloc:
        return ""
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc.lower(), parsed.path or "/", "", "", ""))


def same_domain(url_a: str, url_b: str) -> bool:
    return urllib.parse.urlparse(url_a).netloc.lower() == urllib.parse.urlparse(url_b).netloc.lower()


def is_exhibition_related(row: dict[str, str], keyword_hints: list[str]) -> bool:
    category = normalize_text(row.get("category", ""))
    title = normalize_text(row.get("title", ""))

    if any(block in category for block in EXCLUDE_CATEGORY_KEYWORDS):
        return False
    if any(keep in category for keep in INCLUDE_CATEGORY_KEYWORDS):
        return True
    if any(hint in title for hint in TITLE_HINT_KEYWORDS):
        return True
    if any(hint in title for hint in keyword_hints):
        return True
    return False


def load_institutions(input_csv: Path, keyword_hints: list[str]) -> list[Institution]:
    rows = list(csv.DictReader(input_csv.open(encoding="utf-8-sig")))
    institutions: list[Institution] = []
    seen: set[str] = set()

    for row in rows:
        if not is_exhibition_related(row, keyword_hints):
            continue
        official = canonical_url(row.get("official_url", "")) or canonical_url(row.get("link", ""))
        if not official or official in seen:
            continue
        seen.add(official)
        institutions.append(
            Institution(
                institution_id=f"inst-{len(institutions) + 1:05d}",
                title=normalize_text(row.get("title", "")),
                category=normalize_text(row.get("category", "")),
                official_url=official,
                region_main=normalize_text(row.get("region_main", "")),
                region_sub=normalize_text(row.get("region_sub", "")),
                source_query=normalize_text(row.get("source_query", "")),
            )
        )

    return institutions


def http_get(url: str, timeout: int) -> str:
    # Normalize URL: encode spaces and control characters in path while preserving structure
    parsed = urllib.parse.urlparse(url)
    # Quote the path while preserving path separators; safe chars include :/?#[]@!$&'()*+,;=
    encoded_path = urllib.parse.quote(parsed.path, safe="/:@!$&'()*+,;=")
    # Reconstruct the URL with encoded path
    clean_url = urllib.parse.urlunparse((
        parsed.scheme,
        parsed.netloc,
        encoded_path,
        parsed.params,
        parsed.query,
        parsed.fragment
    ))
    
    req = urllib.request.Request(
        clean_url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; ExhibitionDataHub/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )

    last_error: Exception | None = None
    for attempt in range(1, DEFAULT_HTTP_RETRY_COUNT + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, errors="replace")
        except HTTPError:
            # HTTP status errors are not typically recoverable by retry.
            raise
        except (URLError, TimeoutError, ConnectionResetError, OSError) as exc:
            last_error = exc
            if attempt < DEFAULT_HTTP_RETRY_COUNT:
                time.sleep(0.25 * attempt)

    raise RuntimeError(f"HTTP request failed after retries: {last_error}")


def extract_links(html: str, base_url: str) -> list[str]:
    parser = LinkExtractor()
    parser.feed(html)
    candidates: list[str] = []
    for href, text in parser.links:
        if not href:
            continue
        full = urllib.parse.urljoin(base_url, href)
        if not full.startswith(("http://", "https://")):
            continue
        if not same_domain(base_url, full):
            continue
        low = (href + " " + text + " " + full).lower()
        if not any(key in low for key in PAGE_HINT_KEYWORDS):
            continue
        if any(skip in low for skip in SKIP_PAGE_URL_KEYWORDS):
            continue
        candidates.append(full)
    return candidates


def extract_sitemap_links(
    institution: Institution,
    base_url: str,
    timeout: int,
    failures: list[FailureRecord],
) -> list[str]:
    parsed = urllib.parse.urlparse(base_url)
    sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
    try:
        xml_text = http_get(sitemap_url, timeout)
    except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError, OSError) as exc:
        add_failure(failures, institution, sitemap_url, "sitemap", exc)
        return []

    links = re.findall(r"<loc>(.*?)</loc>", xml_text, flags=re.IGNORECASE)
    results: list[str] = []
    for link in links:
        value = normalize_text(link)
        low = value.lower()
        if any(skip in low for skip in SKIP_PAGE_URL_KEYWORDS):
            continue
        if any(key in low for key in PAGE_HINT_KEYWORDS):
            results.append(value)
    return results


def discover_pages(
    institution: Institution,
    max_pages: int,
    timeout: int,
    failures: list[FailureRecord],
) -> list[str]:
    base_url = institution.official_url
    pages: list[str] = [base_url]

    try:
        homepage = http_get(base_url, timeout)
        pages.extend(extract_links(homepage, base_url))
    except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError, OSError) as exc:
        add_failure(failures, institution, base_url, "homepage", exc)
        homepage = ""

    pages.extend(extract_sitemap_links(institution, base_url, timeout, failures))

    unique: list[str] = []
    seen: set[str] = set()
    for page in pages:
        normalized = canonical_url(page)
        if not normalized:
            continue
        if not same_domain(base_url, normalized):
            continue
        low = normalized.lower()
        if any(skip in low for skip in SKIP_PAGE_URL_KEYWORDS):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
        if len(unique) >= max_pages:
            break

    return unique


def clean_lines_from_html(html_text: str) -> list[str]:
    parser = TextExtractor()
    parser.feed(html_text)
    merged = "\n".join(parser.lines)
    lines = [normalize_text(line) for line in merged.split("\n")]
    return [line for line in lines if line]


def normalize_date_text(value: str, reference_year: int | None = None) -> str:
    raw = value.strip().replace(".", "-").replace("/", "-")
    parts = [part for part in raw.split("-") if part]
    if len(parts) == 3:
        try:
            return date(int(parts[0]), int(parts[1]), int(parts[2])).isoformat()
        except ValueError:
            return ""
    if len(parts) == 2 and reference_year is not None:
        try:
            return date(reference_year, int(parts[0]), int(parts[1])).isoformat()
        except ValueError:
            return ""
    return ""


def extract_price(text: str) -> tuple[str, str]:
    low = text.lower()
    if "무료" in text or "free" in low:
        return "free", "무료"
    if "유료" in text:
        return "paid", "유료"
    won = PRICE_WON_PATTERN.search(text)
    if won:
        return "paid", won.group(0)
    return "unknown", ""


def event_title_score(line: str, keyword_hints: list[str]) -> float:
    normalized = line.replace(" ", "")
    if normalized in GENERIC_EVENT_LABELS:
        return 0.0
    if len(line) < 4 or len(line) > 120:
        return 0.0

    score = 0.0
    if any(key in line for key in STRONG_EVENT_TITLE_KEYWORDS):
        score += 0.65
    if any(key in line for key in keyword_hints):
        score += 0.2
    if "전시" in line:
        score += 0.35
    if any(ch in line for ch in ["《", "》", "[", "]"]):
        score += 0.2
    return min(score, 1.0)


def parse_jsonld_events(html_text: str) -> list[dict[str, str]]:
    parser = JsonLdExtractor()
    parser.feed(html_text)
    events: list[dict[str, str]] = []

    def walk(node: object) -> None:
        if isinstance(node, dict):
            node_type = str(node.get("@type", "")).lower()
            if node_type == "event":
                name = normalize_text(str(node.get("name", "")))
                start_raw = str(node.get("startDate", ""))
                end_raw = str(node.get("endDate", ""))
                start_date = normalize_date_text(start_raw[:10]) if start_raw else ""
                end_date = normalize_date_text(end_raw[:10]) if end_raw else ""
                price_type = "unknown"
                price_text = ""
                offers = node.get("offers")
                if isinstance(offers, dict):
                    if offers.get("isAccessibleForFree") is True:
                        price_type = "free"
                        price_text = "무료"
                    elif offers.get("price") is not None:
                        price_type = "paid"
                        price_text = str(offers.get("price", "")).strip()
                events.append(
                    {
                        "event_name": name,
                        "start_date": start_date,
                        "end_date": end_date,
                        "price_type": price_type,
                        "price_text": price_text,
                        "evidence": "jsonld",
                        "confidence": "0.95",
                    }
                )
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    for payload in parser.payloads:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        walk(data)

    return events


def parse_text_events(lines: list[str], keyword_hints: list[str]) -> list[dict[str, str]]:
    events: list[dict[str, str]] = []

    for idx, line in enumerate(lines):
        if any(term in line for term in EXCLUDE_EVENT_TEXT_KEYWORDS):
            continue

        line_has_strong_title = any(key in line for key in STRONG_EVENT_TITLE_KEYWORDS) or any(
            ch in line for ch in ["《", "》", "[", "]"]
        )
        if any(term in line for term in LOCATION_TEXT_KEYWORDS) and not line_has_strong_title:
            continue

        title_score = event_title_score(line, keyword_hints)
        if title_score < 0.5:
            continue

        window = lines[max(0, idx - 3): min(len(lines), idx + 4)]
        context = " | ".join(window)
        if any(term in context for term in EXCLUDE_EVENT_TEXT_KEYWORDS):
            continue

        if any(term in line for term in ["전시실", "전시장소", "장소", "운영", "예약", "프로그램"]):
            if not line_has_strong_title:
                continue

        start_date = ""
        end_date = ""
        match = DATE_RANGE_PATTERN.search(context)
        if match:
            start_date = normalize_date_text(match.group("s"))
            ref_year = int(start_date[:4]) if start_date else None
            end_date = normalize_date_text(match.group("e"), reference_year=ref_year)
        else:
            single = SINGLE_DATE_PATTERN.search(context)
            if single:
                start_date = normalize_date_text(single.group(1))

        price_type, price_text = extract_price(context)

        if not start_date and not end_date:
            continue

        confidence = 0.35 + (title_score * 0.45)
        if start_date:
            confidence += 0.15
        if price_type != "unknown":
            confidence += 0.05

        events.append(
            {
                "event_name": line,
                "start_date": start_date,
                "end_date": end_date,
                "price_type": price_type,
                "price_text": price_text,
                "evidence": context[:300],
                "confidence": f"{min(confidence, 0.95):.2f}",
            }
        )

    unique: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for event in events:
        key = (event["event_name"], event["start_date"], event["end_date"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(event)

    return unique


def extract_events_from_page(
    institution: Institution,
    page_url: str,
    timeout: int,
    keyword_hints: list[str],
    failures: list[FailureRecord],
) -> list[dict[str, str]]:
    low = page_url.lower()
    if any(skip in low for skip in SKIP_PAGE_URL_KEYWORDS):
        return []

    try:
        html_text = http_get(page_url, timeout)
    except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError, OSError) as exc:
        add_failure(failures, institution, page_url, "page", exc)
        return []

    events = parse_jsonld_events(html_text)
    if events:
        return events

    lines = clean_lines_from_html(html_text)
    if not lines:
        return []

    return parse_text_events(lines, keyword_hints)


def consolidate_events(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, str]]] = {}
    for row in rows:
        key = (
            row.get("institution_id", ""),
            re.sub(r"\s+", "", row.get("event_name", "").lower()),
            row.get("start_date", ""),
            row.get("end_date", ""),
        )
        grouped.setdefault(key, []).append(row)

    merged: list[dict[str, str]] = []
    for bucket in grouped.values():
        best = max(
            bucket,
            key=lambda r: (
                float(r.get("confidence", "0") or 0),
                1 if r.get("price_type", "unknown") != "unknown" else 0,
                len(r.get("evidence", "")),
            ),
        ).copy()

        source_urls: list[str] = []
        seen_urls: set[str] = set()
        for item in bucket:
            page = item.get("source_page_url", "")
            if page and page not in seen_urls:
                source_urls.append(page)
                seen_urls.add(page)

        best["source_page_count"] = str(len(source_urls))
        best["source_page_urls"] = " | ".join(source_urls)
        merged.append(best)

    return merged


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def summarize_failures(failures: list[FailureRecord]) -> list[dict[str, str]]:
    by_domain: dict[str, dict[str, object]] = {}
    for item in failures:
        domain = item.domain or "(unknown)"
        entry = by_domain.setdefault(
            domain,
            {
                "domain": domain,
                "fail_count": 0,
                "stages": set(),
                "error_types": set(),
                "sample_url": item.url,
                "sample_message": item.error_message,
                "last_institution": item.institution_title,
            },
        )
        entry["fail_count"] = int(entry["fail_count"]) + 1
        cast_stages = entry["stages"]
        cast_errors = entry["error_types"]
        if isinstance(cast_stages, set):
            cast_stages.add(item.stage)
        if isinstance(cast_errors, set):
            cast_errors.add(item.error_type)
        entry["sample_url"] = item.url
        entry["sample_message"] = item.error_message
        entry["last_institution"] = item.institution_title

    rows: list[dict[str, str]] = []
    for domain, entry in sorted(by_domain.items(), key=lambda kv: int(kv[1]["fail_count"]), reverse=True):
        stages = entry["stages"]
        error_types = entry["error_types"]
        rows.append(
            {
                "domain": domain,
                "fail_count": str(entry["fail_count"]),
                "stages": ",".join(sorted(stages)) if isinstance(stages, set) else "",
                "error_types": ",".join(sorted(error_types)) if isinstance(error_types, set) else "",
                "last_institution": str(entry["last_institution"]),
                "sample_url": str(entry["sample_url"]),
                "sample_message": str(entry["sample_message"]),
            }
        )

    return rows


def run_pipeline(args: argparse.Namespace) -> None:
    root = Path(__file__).resolve().parent.parent
    input_csv = root / args.input
    output_csv = root / args.output
    failed_domains_out = root / args.failed_domains_out
    keyword_hints = load_markdown_bullets(root / "docs" / "keywords.md")

    institutions = load_institutions(input_csv, keyword_hints)
    if args.max_institutions > 0:
        institutions = institutions[:args.max_institutions]

    event_rows: list[dict[str, str]] = []
    failures: list[FailureRecord] = []
    print(f"Loaded {len(institutions)} candidate institutions")

    for idx, inst in enumerate(institutions, start=1):
        pages = discover_pages(inst, args.max_pages_per_institution, args.timeout, failures)
        print(f"[{idx}/{len(institutions)}] {inst.title}: {len(pages)} candidate pages")

        for page in pages:
            events = extract_events_from_page(inst, page, args.timeout, keyword_hints, failures)
            for event in events:
                row = {
                    "institution_id": inst.institution_id,
                    "institution_title": inst.title,
                    "institution_url": inst.official_url,
                    "source_page_url": page,
                    "event_name": event.get("event_name", ""),
                    "start_date": event.get("start_date", ""),
                    "end_date": event.get("end_date", ""),
                    "price_type": event.get("price_type", "unknown"),
                    "price_text": event.get("price_text", ""),
                    "confidence": event.get("confidence", "0.00"),
                    "evidence": event.get("evidence", "")[:500],
                }
                event_rows.append(row)

            time.sleep(args.pause)

    dedup_events = consolidate_events(event_rows)
    curated_events = [row for row in dedup_events if float(row.get("confidence", "0") or 0) >= args.min_confidence]

    write_csv(
        output_csv,
        curated_events,
        [
            "institution_id",
            "institution_title",
            "institution_url",
            "source_page_url",
            "source_page_count",
            "source_page_urls",
            "event_name",
            "start_date",
            "end_date",
            "price_type",
            "price_text",
            "confidence",
            "evidence",
        ],
    )

    print(f"Saved {len(curated_events)} exhibition rows -> {output_csv}")

    failed_domain_rows = summarize_failures(failures)
    write_csv(
        failed_domains_out,
        failed_domain_rows,
        [
            "domain",
            "fail_count",
            "stages",
            "error_types",
            "last_institution",
            "sample_url",
            "sample_message",
        ],
    )
    print(f"Saved {len(failed_domain_rows)} failed domains -> {failed_domains_out}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract exhibition dates from institution homepages.")
    parser.add_argument("--input", default=DEFAULT_INPUT_CSV, help="Input CSV with institution/homepage rows")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_CSV, help="Output CSV for extracted exhibitions")
    parser.add_argument(
        "--failed-domains-out",
        default=DEFAULT_FAILED_DOMAINS_OUT,
        help="Output CSV for failed domain summary",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout seconds")
    parser.add_argument("--pause", type=float, default=DEFAULT_PAUSE_SECONDS, help="Pause between page requests")
    parser.add_argument(
        "--max-pages-per-institution",
        type=int,
        default=DEFAULT_MAX_PAGES_PER_INSTITUTION,
        help="Max candidate pages per institution",
    )
    parser.add_argument(
        "--max-institutions",
        type=int,
        default=DEFAULT_MAX_INSTITUTIONS,
        help="Limit institutions for quick test. 0 means no limit.",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=DEFAULT_MIN_CONFIDENCE,
        help="Minimum confidence threshold for output",
    )
    return parser


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    load_env = project_root / ".env"
    if load_env.exists():
        for raw_line in load_env.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value

    parser = build_parser()
    args = parser.parse_args()
    run_pipeline(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
