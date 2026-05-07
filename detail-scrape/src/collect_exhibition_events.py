"""Extract exhibitions from list pages provided via CSV."""

from __future__ import annotations

import argparse
import csv
import html
import io
import json
import os
import re
import shutil
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError

DEFAULT_INPUT_CSV = "data/exhibition_list_pages.csv"
DEFAULT_OUTPUT_CSV = "data/extracted_exhibitions.csv"
DEFAULT_FAILED_PAGES_OUT = "data/failed_pages.csv"
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_PAUSE_SECONDS = 0.2
DEFAULT_HTTP_RETRY_COUNT = 2
DEFAULT_DETAIL_FETCH_MODE = "auto"
DEFAULT_JS_RENDER_TIMEOUT_MS = 12000
DEFAULT_OCR_TIMEOUT_SECONDS = 18.0
DEFAULT_MAX_IMAGES_PER_PAGE = 3
DEFAULT_SAVE_EVERY = 10
DEFAULT_LOG_EVERY = 10
DEFAULT_PROGRESS_PATH = "data/exhibition_list_progress.json"
DEFAULT_LIST_URL_COLUMN = "list_url"

OUTPUT_FIELDNAMES_BASE = [
    "exhibition_name",
    "start_date",
    "end_date",
    "location",
    "detail_url",
    "source_list_url",
    "status",
    "failure_stage",
    "failure_type",
    "failure_message",
]

FAILED_PAGE_FIELDNAMES = [
    "list_url",
    "stage",
    "error_type",
    "error_message",
    "source_title",
]

BLOCK_TAGS = {"li", "article", "tr", "div", "section"}
LINE_BREAK_TAGS = {
    "br",
    "p",
    "li",
    "tr",
    "div",
    "section",
    "article",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
}

DATE_RANGE_PATTERN = re.compile(
    r"(?P<s>\d{4}[./-]\d{1,2}[./-]\d{1,2})\s*(?:~|-|–|—|to)\s*(?P<e>\d{4}[./-]\d{1,2}[./-]\d{1,2}|\d{1,2}[./-]\d{1,2})",
    re.IGNORECASE,
)
SINGLE_DATE_PATTERN = re.compile(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}")

ADDRESS_PATTERNS = [
    re.compile(r"(?:주소|장소|전시장소|전시장|Location)\s*[:：]?\s*([^\n\r|]{5,120})", re.IGNORECASE),
    re.compile(r"[가-힣]{2,}(?:특별시|광역시|도|시|군|구)[^\n\r|]{0,60}(?:로|길)\s*\d+[\w-]*"),
    re.compile(r"\d+[^\n\r|]{0,80}(?:-ro|-gil|road|rd|street|st|avenue|ave|boulevard|blvd)[^\n\r|]{0,40}", re.IGNORECASE),
]

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
]

EVENT_HINT_KEYWORDS = [
    "전시",
    "exhibition",
    "show",
    "gallery",
    "museum",
]

GENERIC_EVENT_LABELS = {
    "전시",
    "전시회",
    "현재전시",
    "예정전시",
    "과거전시",
    "전시기간",
    "전시장소",
    "전시안내",
}

GENERIC_LINK_TEXTS = {
    "상세",
    "상세보기",
    "자세히",
    "더보기",
    "view",
    "detail",
    "read more",
}

IMAGE_EXT_PATTERN = re.compile(r"\.(?:png|jpg|jpeg|webp|gif)(?:$|[?#])", re.IGNORECASE)
IMAGE_HINT_KEYWORDS = [
    "poster",
    "flyer",
    "exhibition",
    "event",
    "schedule",
    "program",
    "notice",
    "banner",
    "전시",
    "전시회",
    "포스터",
    "일정",
    "행사",
    "공지",
    "안내",
]

KNOWN_TESSERACT_PATHS = [
    r"C:\\Program Files\\Tesseract-OCR\\tesseract.exe",
    r"C:\\Program Files (x86)\\Tesseract-OCR\\tesseract.exe",
]


@dataclass
class FailureRecord:
    list_url: str
    stage: str
    error_type: str
    error_message: str
    source_title: str = ""


@dataclass
class HtmlBlock:
    tag: str
    depth: int
    texts: list[str]
    links: list[tuple[str, str]]


@dataclass
class Institution:
    institution_id: str
    title: str
    category: str
    official_url: str
    official_urls: list[str]
    region_main: str
    region_sub: str
    source_query: str


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


class BlockExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.blocks: list[HtmlBlock] = []
        self._stack: list[HtmlBlock] = []
        self._anchor_href = ""
        self._anchor_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lower = tag.lower()
        if lower in BLOCK_TAGS:
            self._stack.append(HtmlBlock(tag=lower, depth=len(self._stack), texts=[], links=[]))
        if lower == "a":
            attrs_map = dict(attrs)
            self._anchor_href = attrs_map.get("href") or ""
            self._anchor_text = []
        if lower in LINE_BREAK_TAGS and self._stack:
            self._stack[-1].texts.append("\n")

    def handle_data(self, data: str) -> None:
        if not data or not self._stack:
            return
        self._stack[-1].texts.append(data)
        if self._anchor_href:
            self._anchor_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        lower = tag.lower()
        if lower == "a" and self._anchor_href:
            text = normalize_text(" ".join(self._anchor_text))
            if self._stack:
                self._stack[-1].links.append((self._anchor_href, text))
            self._anchor_href = ""
            self._anchor_text = []
        if lower in LINE_BREAK_TAGS and self._stack:
            self._stack[-1].texts.append("\n")
        if lower in BLOCK_TAGS and self._stack:
            block = self._stack.pop()
            if block.texts or block.links:
                self.blocks.append(block)


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
        if tag.lower() in LINE_BREAK_TAGS:
            self.lines.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = " ".join(data.split())
        if text:
            self.lines.append(text)


class ImageExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.images: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "img":
            return
        attrs_map = {k.lower(): (v or "") for k, v in attrs}
        src = attrs_map.get("src", "") or attrs_map.get("data-src", "") or attrs_map.get("data-lazy-src", "")
        if not src:
            srcset = attrs_map.get("srcset", "")
            if srcset:
                first_candidate = srcset.split(",", 1)[0].strip().split(" ", 1)[0].strip()
                src = first_candidate
        if not src:
            return

        self.images.append(
            {
                "src": src,
                "alt": normalize_text(attrs_map.get("alt", "")),
                "title": normalize_text(attrs_map.get("title", "")),
            }
        )


def normalize_text(value: str) -> str:
    return " ".join((value or "").split())


def normalize_date_text(value: str, reference_year: int | None = None) -> str:
    raw = normalize_text(value).replace(".", "-").replace("/", "-")
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


def parse_iso_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def effective_end_date(event: dict[str, str]) -> date | None:
    end_date = parse_iso_date(normalize_text(event.get("end_date", "")))
    if end_date:
        return end_date
    start_date = parse_iso_date(normalize_text(event.get("start_date", "")))
    return start_date


def is_event_current(event: dict[str, str], today: date) -> bool:
    end_date = effective_end_date(event)
    if end_date is None:
        return False
    return end_date >= today


def split_lines(text: str) -> list[str]:
    if not text:
        return []
    text = text.replace("\r", "\n")
    chunks = re.split(r"[\n|•·]+", text)
    lines: list[str] = []
    for chunk in chunks:
        cleaned = normalize_text(chunk)
        if cleaned:
            lines.append(cleaned)
    return lines


def html_to_lines(html_text: str) -> list[str]:
    if not html_text:
        return []
    parser = TextExtractor()
    parser.feed(html_text)
    merged = "".join(parser.lines)
    lines = [normalize_text(line) for line in merged.split("\n")]
    return [line for line in lines if line]


def token_candidates(value: str) -> list[str]:
    text = normalize_text(value).lower()
    if not text:
        return []
    tokens: list[str] = []
    for raw in re.split(r"[^0-9a-zA-Z가-힣]+", text):
        token = raw.strip()
        if len(token) >= 2 and token not in tokens:
            tokens.append(token)
    return tokens


def extract_date_range(text: str) -> tuple[str, str]:
    match = DATE_RANGE_PATTERN.search(text)
    if match:
        start = normalize_date_text(match.group("s"))
        ref_year = int(start[:4]) if start else None
        end = normalize_date_text(match.group("e"), reference_year=ref_year)
        return start, end

    single = SINGLE_DATE_PATTERN.search(text)
    if single:
        start = normalize_date_text(single.group(0))
        return start, ""
    return "", ""


def extract_location(text: str) -> str:
    if not text:
        return ""
    for pattern in ADDRESS_PATTERNS:
        match = pattern.search(text)
        if match:
            value = match.group(1) if match.lastindex else match.group(0)
            value = normalize_text(value)
            if value:
                return value
    return ""


def pick_event_name(lines: list[str], link_texts: list[str]) -> str:
    candidates: list[tuple[float, str]] = []
    for text in link_texts + lines:
        line = normalize_text(text)
        if not line or len(line) < 4 or len(line) > 120:
            continue
        if line in GENERIC_EVENT_LABELS:
            continue
        if any(term in line for term in EXCLUDE_EVENT_TEXT_KEYWORDS):
            continue

        score = 0.0
        if any(key in line for key in STRONG_EVENT_TITLE_KEYWORDS):
            score += 0.6
        if any(key in line.lower() for key in EVENT_HINT_KEYWORDS):
            score += 0.25
        if "전시" in line:
            score += 0.3
        if any(ch in line for ch in ["《", "》", "‘", "’", "“", "”"]):
            score += 0.2
        if DATE_RANGE_PATTERN.search(line) or SINGLE_DATE_PATTERN.search(line):
            score -= 0.2
        candidates.append((score, line))

    if not candidates:
        return ""

    candidates.sort(key=lambda item: item[0], reverse=True)
    best_score, best_line = candidates[0]
    if best_score < 0.3:
        return ""
    return best_line


def clean_link_text(text: str) -> str:
    return normalize_text(text).lower()


def pick_detail_url(links: list[tuple[str, str]], base_url: str, event_name: str) -> str:
    if not links:
        return base_url

    tokens = token_candidates(event_name)
    best_url = ""
    best_score = -1

    for href, text in links:
        href = href.strip()
        if not href or href.startswith(("javascript:", "mailto:", "#")):
            continue
        full = urllib.parse.urljoin(base_url, href)
        low_text = clean_link_text(text)
        if low_text in GENERIC_LINK_TEXTS:
            continue

        score = 0
        for token in tokens:
            if token in low_text:
                score += 2
            if token in full.lower():
                score += 1

        if full == base_url:
            score -= 1
        if score > best_score:
            best_score = score
            best_url = full

    return best_url or urllib.parse.urljoin(base_url, links[0][0])


def pick_best_event(events: list[dict[str, str]], target_name: str) -> dict[str, str]:
    if not events:
        return {}
    if not target_name:
        return events[0]

    tokens = token_candidates(target_name)
    best_score = -1
    best_event = events[0]
    for event in events:
        name = normalize_text(event.get("exhibition_name", "")).lower()
        score = sum(1 for token in tokens if token in name)
        if score > best_score:
            best_score = score
            best_event = event
    return best_event


def merge_event_fields(event: dict[str, str], candidate: dict[str, str]) -> None:
    if not candidate:
        return

    if not event.get("exhibition_name") or event.get("exhibition_name") in GENERIC_EVENT_LABELS:
        if candidate.get("exhibition_name"):
            event["exhibition_name"] = candidate.get("exhibition_name", "")

    for key in ["start_date", "end_date", "location", "detail_url"]:
        if not event.get(key) and candidate.get(key):
            event[key] = candidate.get(key, "")


def extract_detail_fields(html_text: str) -> dict[str, str]:
    lines = html_to_lines(html_text)
    if not lines:
        return {"start_date": "", "end_date": "", "location": ""}
    merged = " | ".join(lines)
    merged = merged[:4000]
    start_date, end_date = extract_date_range(merged)
    location = extract_location(merged)
    return {"start_date": start_date, "end_date": end_date, "location": location}


def needs_detail_fields(event: dict[str, str]) -> bool:
    return not event.get("start_date") or not event.get("end_date") or not event.get("location")


def should_run_detail(mode: str, missing: bool) -> bool:
    return mode == "always" or (mode == "auto" and missing)


def enrich_event_from_detail(
    event: dict[str, str],
    detail_url: str,
    html_text: str,
    js_render_mode: str,
    ocr_mode: str,
    js_timeout_ms: int,
    ocr_timeout_seconds: float,
    max_images: int,
    failures: list[FailureRecord],
) -> dict[str, str]:
    base_name = event.get("exhibition_name", "")
    jsonld_events = parse_jsonld_events(html_text, detail_url)
    merge_event_fields(event, pick_best_event(jsonld_events, base_name))
    merge_event_fields(event, extract_detail_fields(html_text))

    missing = needs_detail_fields(event)
    render_html = ""
    if should_run_detail(js_render_mode, missing):
        try:
            render_html = render_page_with_playwright(detail_url, js_timeout_ms)
        except Exception as exc:
            add_failure(failures, None, detail_url, "detail-js-render", exc)
        if render_html:
            jsonld_events = parse_jsonld_events(render_html, detail_url)
            merge_event_fields(event, pick_best_event(jsonld_events, base_name))
            merge_event_fields(event, extract_detail_fields(render_html))

    missing = needs_detail_fields(event)
    if should_run_detail(ocr_mode, missing):
        try:
            ocr_events = extract_ocr_events(render_html or html_text, detail_url, max_images, ocr_timeout_seconds)
            merge_event_fields(event, pick_best_event(ocr_events, base_name))
        except Exception as exc:
            add_failure(failures, None, detail_url, "detail-ocr", exc)

    return event


def normalize_event(event: dict[str, str]) -> dict[str, str]:
    return {
        "exhibition_name": normalize_text(event.get("exhibition_name", "")),
        "start_date": normalize_text(event.get("start_date", "")),
        "end_date": normalize_text(event.get("end_date", "")),
        "location": normalize_text(event.get("location", "")),
        "detail_url": normalize_text(event.get("detail_url", "")),
        "source_list_url": normalize_text(event.get("source_list_url", "")),
    }


def is_event_valid(event: dict[str, str]) -> bool:
    if not event.get("exhibition_name"):
        return False
    if not event.get("start_date") and not event.get("end_date"):
        return False
    return True


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


def http_get(url: str, timeout: float) -> tuple[str, str]:
    parsed = urllib.parse.urlparse(url)
    encoded_path = urllib.parse.quote(parsed.path, safe="/:@!$&'()*+,;=")
    clean_url = urllib.parse.urlunparse((
        parsed.scheme,
        parsed.netloc,
        encoded_path,
        parsed.params,
        parsed.query,
        parsed.fragment,
    ))

    req = urllib.request.Request(
        clean_url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; ExhibitionDataHub/2.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )

    last_error: Exception | None = None
    for attempt in range(1, DEFAULT_HTTP_RETRY_COUNT + 1):
        try:
            with urllib.request.urlopen(req, timeout=max(0.5, float(timeout))) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, errors="replace"), resp.geturl()
        except HTTPError:
            raise
        except (URLError, TimeoutError, ConnectionResetError, OSError) as exc:
            last_error = exc
            if attempt < DEFAULT_HTTP_RETRY_COUNT:
                time.sleep(0.25 * attempt)

    raise RuntimeError(f"HTTP request failed after retries: {last_error}")


def render_page_with_playwright(url: str, timeout_ms: int) -> str:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return ""

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 8000))
        except Exception:
            pass
        content = page.content()
        context.close()
        browser.close()
        return content


def parse_jsonld_events(html_text: str, base_url: str) -> list[dict[str, str]]:
    parser = JsonLdExtractor()
    parser.feed(html_text)
    events: list[dict[str, str]] = []

    def format_location(value: object) -> str:
        if isinstance(value, str):
            return normalize_text(value)
        if isinstance(value, dict):
            name = normalize_text(str(value.get("name", "")))
            address_value = value.get("address")
            address_parts: list[str] = []
            if isinstance(address_value, dict):
                for key in ["streetAddress", "addressLocality", "addressRegion", "postalCode", "addressCountry"]:
                    part = normalize_text(str(address_value.get(key, "")))
                    if part:
                        address_parts.append(part)
            elif isinstance(address_value, str):
                address_parts.append(normalize_text(address_value))
            address = " ".join([part for part in address_parts if part])
            combined = " ".join([name, address]).strip()
            return combined
        return ""

    def walk(node: object) -> None:
        if isinstance(node, dict):
            node_type = str(node.get("@type", "")).lower()
            if node_type == "event":
                name = normalize_text(str(node.get("name", "")))
                start_raw = str(node.get("startDate", ""))
                end_raw = str(node.get("endDate", ""))
                start_date = normalize_date_text(start_raw[:10]) if start_raw else ""
                end_date = normalize_date_text(end_raw[:10], reference_year=int(start_date[:4]) if start_date else None)
                location = format_location(node.get("location"))
                detail_url = str(node.get("url") or node.get("@id") or "")
                detail_url = urllib.parse.urljoin(base_url, detail_url) if detail_url else base_url
                events.append(
                    {
                        "exhibition_name": name,
                        "start_date": start_date,
                        "end_date": end_date,
                        "location": location,
                        "detail_url": detail_url,
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


def extract_block_events(html_text: str, base_url: str) -> list[dict[str, str]]:
    parser = BlockExtractor()
    parser.feed(html_text)

    events: list[dict[str, str]] = []
    for block in parser.blocks:
        text = html.unescape("".join(block.texts))
        if not text:
            continue
        lines = split_lines(text)
        if not lines:
            continue
        link_texts = [link_text for _, link_text in block.links if link_text]
        event_name = pick_event_name(lines, link_texts)
        if not event_name:
            continue
        start_date, end_date = extract_date_range(text)
        location = extract_location(text)
        detail_url = pick_detail_url(block.links, base_url, event_name)
        if not start_date and not end_date:
            if "전시" not in event_name and not any(key in event_name for key in STRONG_EVENT_TITLE_KEYWORDS):
                if detail_url == base_url:
                    continue
        events.append(
            {
                "exhibition_name": event_name,
                "start_date": start_date,
                "end_date": end_date,
                "location": location,
                "detail_url": detail_url,
            }
        )

    return events


def score_ocr_text(text: str) -> float:
    normalized = normalize_text(text)
    if not normalized:
        return 0.0

    score = min(len(normalized) / 120.0, 0.35)
    if DATE_RANGE_PATTERN.search(normalized):
        score += 0.3
    if SINGLE_DATE_PATTERN.search(normalized):
        score += 0.12
    if any(key in normalized for key in STRONG_EVENT_TITLE_KEYWORDS):
        score += 0.2
    if any(ch in normalized for ch in ["《", "》", "‘", "’", "“", "”"]):
        score += 0.1
    if any(term in normalized for term in ["전시", "개인전", "기획전", "특별전", "초대전", "그룹전"]):
        score += 0.15

    return min(score, 1.0)


def preprocess_ocr_image(image: object) -> list[object]:
    try:
        from PIL import Image, ImageEnhance, ImageFilter, ImageOps
    except ImportError:
        return []

    if not isinstance(image, Image.Image):
        return []

    base = ImageOps.exif_transpose(image).convert("RGB")
    variants = [base]

    gray = ImageOps.grayscale(base)
    gray = ImageOps.autocontrast(gray)

    scale = 2
    resized = gray.resize((max(1, gray.width * scale), max(1, gray.height * scale)), Image.Resampling.LANCZOS)
    resized = ImageEnhance.Sharpness(resized).enhance(1.4)
    resized = resized.filter(ImageFilter.SHARPEN)
    variants.append(resized)

    for threshold in (160, 180, 200):
        thresholded = resized.point(lambda p, t=threshold: 255 if p > t else 0)
        variants.append(thresholded)

    return variants


def resolve_tesseract_command() -> str:
    env_value = normalize_text(os.environ.get("TESSERACT_CMD", ""))
    if env_value and Path(env_value).exists():
        return env_value

    detected = shutil.which("tesseract")
    if detected:
        return detected

    for candidate in KNOWN_TESSERACT_PATHS:
        if Path(candidate).exists():
            return candidate

    return ""


def run_tesseract_ocr_variants(image: object) -> str:
    try:
        import pytesseract
    except ImportError:
        return ""

    tesseract_cmd = resolve_tesseract_command()
    if not tesseract_cmd:
        return ""
    pytesseract.pytesseract.tesseract_cmd = tesseract_cmd

    configs = [
        "--oem 3 --psm 6",
        "--oem 3 --psm 11",
        "--oem 3 --psm 12",
    ]

    best_text = ""
    best_score = 0.0
    for variant in preprocess_ocr_image(image):
        for config in configs:
            try:
                text = normalize_text(pytesseract.image_to_string(variant, lang="kor+eng", config=config))
            except Exception:
                continue

            score = score_ocr_text(text)
            if score > best_score or (score == best_score and len(text) > len(best_text)):
                best_score = score
                best_text = text

    return best_text


def extract_ocr_text_from_image(image_url: str, timeout_seconds: float) -> str:
    try:
        from PIL import Image
    except ImportError:
        return ""

    if not image_url or any(ord(c) < 32 for c in image_url):
        return ""

    req = urllib.request.Request(
        image_url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; ExhibitionDataHub/2.0)",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            data = resp.read()
    except Exception:
        return ""

    if not data:
        return ""

    try:
        img = Image.open(io.BytesIO(data))
    except Exception:
        return ""

    return run_tesseract_ocr_variants(img)


def score_image_candidate(url: str, alt: str, title: str) -> float:
    meta = normalize_text(" ".join([url, alt, title])).lower()
    score = 0.0
    if any(key in meta for key in IMAGE_HINT_KEYWORDS):
        score += 0.45
    if any(term in meta for term in ["poster", "flyer", "전시", "포스터"]):
        score += 0.25
    if IMAGE_EXT_PATTERN.search(url.lower()):
        score += 0.1
    if len(normalize_text(alt)) >= 6:
        score += 0.1
    if len(normalize_text(title)) >= 6:
        score += 0.1
    return min(score, 1.0)


def extract_image_candidates(html_text: str, page_url: str) -> list[dict[str, str]]:
    parser = ImageExtractor()
    parser.feed(html_text)
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()
    for image in parser.images:
        full = urllib.parse.urljoin(page_url, image.get("src", ""))
        if not full.startswith(("http://", "https://")):
            continue
        normalized = urllib.parse.urlunparse(urllib.parse.urlparse(full))
        if normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(
            {
                "url": normalized,
                "alt": image.get("alt", ""),
                "title": image.get("title", ""),
                "score": f"{score_image_candidate(normalized, image.get('alt', ''), image.get('title', '')):.2f}",
            }
        )
    candidates.sort(key=lambda item: float(item.get("score", "0") or 0), reverse=True)
    return candidates


def extract_ocr_events(html_text: str, base_url: str, max_images: int, timeout_seconds: float) -> list[dict[str, str]]:
    images = extract_image_candidates(html_text, base_url)
    if not images:
        return []

    events: list[dict[str, str]] = []
    for image in images[: max(0, max_images)]:
        image_url = image.get("url", "")
        if not image_url:
            continue
        ocr_text = extract_ocr_text_from_image(image_url, timeout_seconds)
        if not ocr_text:
            continue

        start_date, end_date = extract_date_range(ocr_text)
        lines = split_lines(ocr_text)
        event_name = pick_event_name(lines, [])
        if not event_name:
            continue
        location = extract_location(ocr_text)
        events.append(
            {
                "exhibition_name": event_name,
                "start_date": start_date,
                "end_date": end_date,
                "location": location,
                "detail_url": base_url,
            }
        )

    return events


def dedupe_events(events: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for event in events:
        normalized = normalize_event(event)
        key = (
            normalized.get("exhibition_name", "").lower(),
            normalized.get("start_date", ""),
            normalized.get("end_date", ""),
            normalized.get("detail_url", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def summarize_failures(records: list[FailureRecord]) -> tuple[str, str, str]:
    if not records:
        return "", "", ""

    stages = sorted({record.stage for record in records if record.stage})
    error_types = sorted({record.error_type for record in records if record.error_type})

    messages: list[str] = []
    for record in records:
        message = normalize_text(record.error_message)
        if message and message not in messages:
            messages.append(message)

    message_text = " | ".join(messages)
    if len(message_text) > 800:
        message_text = message_text[:800] + "..."

    return ",".join(stages), ",".join(error_types), message_text


def add_failure(
    failures: list[FailureRecord],
    institution: Institution | None,
    url: str,
    stage: str,
    error: Exception,
) -> None:
    title = ""
    if institution is not None:
        title = getattr(institution, "title", "") or getattr(institution, "institution_title", "")
    failures.append(
        FailureRecord(
            list_url=url,
            stage=stage,
            error_type=type(error).__name__,
            error_message=str(error),
            source_title=title,
        )
    )


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
    total_pages: int,
    saved_rows: int,
    saved_failures: int,
) -> None:
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_processed_index": last_processed_index,
        "next_start_index": next_start_index,
        "total_pages": total_pages,
        "saved_rows": saved_rows,
        "saved_failures": saved_failures,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    progress_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def process_list_page(
    list_url: str,
    detail_fetch_mode: str,
    js_render_mode: str,
    ocr_mode: str,
    timeout_seconds: float,
    js_timeout_ms: int,
    ocr_timeout_seconds: float,
    max_images: int,
    failures: list[FailureRecord],
    today: date,
) -> tuple[list[dict[str, str]], int]:
    html_text = ""
    final_url = list_url
    try:
        html_text, final_url = http_get(list_url, timeout_seconds)
    except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError, OSError) as exc:
        add_failure(failures, None, list_url, "fetch", exc)
        return [], 0

    events = parse_jsonld_events(html_text, final_url)
    events.extend(extract_block_events(html_text, final_url))
    events = dedupe_events(events)

    should_render = js_render_mode == "always" or (js_render_mode == "auto" and not events)
    render_html = ""
    if should_render:
        try:
            render_html = render_page_with_playwright(list_url, js_timeout_ms)
        except Exception as exc:
            add_failure(failures, None, list_url, "js-render", exc)
        if render_html:
            render_events = parse_jsonld_events(render_html, final_url)
            render_events.extend(extract_block_events(render_html, final_url))
            events = dedupe_events(list(events) + render_events)

    should_ocr = ocr_mode == "always" or (ocr_mode == "auto" and not events)
    if should_ocr:
        try:
            effective_html = render_html or html_text
            ocr_events = extract_ocr_events(effective_html, final_url, max_images, ocr_timeout_seconds)
            events = dedupe_events(list(events) + ocr_events)
        except Exception as exc:
            add_failure(failures, None, list_url, "ocr", exc)

    detail_cache: dict[str, str] = {}
    list_html_for_detail = render_html or html_text
    for event in events:
        event["failure_stage"] = ""
        event["failure_type"] = ""
        event["failure_message"] = ""
        event_failure_start = len(failures)
        detail_url = normalize_text(event.get("detail_url", ""))
        if not detail_url:
            failure_stage, failure_type, failure_message = summarize_failures(failures[event_failure_start:])
            event["failure_stage"] = failure_stage
            event["failure_type"] = failure_type
            event["failure_message"] = failure_message
            continue
        if not should_run_detail(detail_fetch_mode, needs_detail_fields(event)):
            failure_stage, failure_type, failure_message = summarize_failures(failures[event_failure_start:])
            event["failure_stage"] = failure_stage
            event["failure_type"] = failure_type
            event["failure_message"] = failure_message
            continue

        if detail_url == final_url or detail_url == list_url:
            detail_html = list_html_for_detail
        else:
            cached = detail_cache.get(detail_url)
            if cached is None:
                try:
                    detail_html, _ = http_get(detail_url, timeout_seconds)
                    detail_cache[detail_url] = detail_html
                except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError, OSError) as exc:
                    add_failure(failures, None, detail_url, "detail-fetch", exc)
                    continue
            else:
                detail_html = cached

        enrich_event_from_detail(
            event,
            detail_url,
            detail_html,
            js_render_mode,
            ocr_mode,
            js_timeout_ms,
            ocr_timeout_seconds,
            max_images,
            failures,
        )

        failure_stage, failure_type, failure_message = summarize_failures(failures[event_failure_start:])
        event["failure_stage"] = failure_stage
        event["failure_type"] = failure_type
        event["failure_message"] = failure_message

    valid_events = [event for event in events if is_event_valid(event)]
    current_events = [event for event in valid_events if is_event_current(event, today)]
    return current_events, len(valid_events)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract exhibitions from list-page URLs.")
    parser.add_argument("--input", default=DEFAULT_INPUT_CSV, help="Input CSV containing list page URLs")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_CSV, help="Output CSV for extracted exhibitions")
    parser.add_argument(
        "--failed-pages-out",
        default=DEFAULT_FAILED_PAGES_OUT,
        help="Output CSV for failed list pages",
    )
    parser.add_argument("--list-url-column", default=DEFAULT_LIST_URL_COLUMN, help="Column name for list URLs")
    parser.add_argument(
        "--pass-through-columns",
        default="",
        help="Comma-separated input columns to include in output",
    )
    parser.add_argument(
        "--detail-fetch-mode",
        choices=["auto", "always", "off"],
        default=DEFAULT_DETAIL_FETCH_MODE,
        help="Detail-page fetch mode (auto=only if fields missing, always=always fetch, off=disable)",
    )
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="Fetch timeout seconds")
    parser.add_argument("--pause-seconds", type=float, default=DEFAULT_PAUSE_SECONDS, help="Pause between pages")
    parser.add_argument(
        "--js-render-mode",
        choices=["auto", "always", "off"],
        default="auto",
        help="JS rendering mode (auto=only if no events, always=always render, off=disable)",
    )
    parser.add_argument(
        "--js-render-timeout-ms",
        type=int,
        default=DEFAULT_JS_RENDER_TIMEOUT_MS,
        help="Timeout for JS rendering",
    )
    parser.add_argument(
        "--ocr-mode",
        choices=["auto", "always", "off"],
        default="auto",
        help="OCR mode (auto=only if no events, always=always OCR, off=disable)",
    )
    parser.add_argument(
        "--ocr-timeout-seconds",
        type=float,
        default=DEFAULT_OCR_TIMEOUT_SECONDS,
        help="Timeout per OCR image download",
    )
    parser.add_argument(
        "--max-images-per-page",
        type=int,
        default=DEFAULT_MAX_IMAGES_PER_PAGE,
        help="Max images to OCR per page",
    )
    parser.add_argument(
        "--enable-instagram",
        action="store_true",
        help="Enable Instagram extraction when list URL points to Instagram",
    )
    parser.add_argument(
        "--save-every",
        type=int,
        default=DEFAULT_SAVE_EVERY,
        help="Save checkpoint every N pages. 0 disables checkpoints.",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=DEFAULT_LOG_EVERY,
        help="Log progress every N pages. 0 disables logging.",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=1,
        help="1-based list page index to start from (resume support).",
    )
    parser.add_argument(
        "--end-index",
        type=int,
        default=0,
        help="1-based list page index to end at (inclusive). 0 means end.",
    )
    parser.add_argument(
        "--progress-file",
        default=DEFAULT_PROGRESS_PATH,
        help="Progress JSON path used for automatic resume.",
    )
    parser.add_argument(
        "--no-auto-resume",
        action="store_true",
        help="Disable automatic resume from progress file.",
    )
    return parser


def run_pipeline(args: argparse.Namespace) -> None:
    root = Path(__file__).resolve().parent.parent
    input_path = root / args.input
    output_path = root / args.output
    failed_path = root / args.failed_pages_out
    progress_path = root / args.progress_file

    if not input_path.exists():
        print(f"Input CSV not found: {input_path}", file=sys.stderr)
        return

    rows = load_rows(input_path)
    if not rows:
        print("No rows in input CSV.", file=sys.stderr)
        return

    url_column = args.list_url_column
    if url_column not in rows[0]:
        print(f"Missing column '{url_column}' in input CSV.", file=sys.stderr)
        return

    pass_through_columns = [col.strip() for col in str(args.pass_through_columns).split(",") if col.strip()]
    output_fieldnames = OUTPUT_FIELDNAMES_BASE + [col for col in pass_through_columns if col not in OUTPUT_FIELDNAMES_BASE]

    list_pages: list[dict[str, str]] = []
    for row in rows:
        list_url = normalize_text(row.get(url_column, ""))
        if not list_url:
            continue
        list_pages.append({"list_url": list_url, **row})

    total_pages = len(list_pages)
    if total_pages == 0:
        print("No valid list URLs found.", file=sys.stderr)
        return

    start_index = max(1, int(args.start_index or 1))
    progress = load_progress(progress_path)
    progress_next_start = int(progress.get("next_start_index", 1) or 1)
    if not args.no_auto_resume and args.start_index == 1 and progress_next_start > 1:
        start_index = progress_next_start

    end_index = int(args.end_index or 0)
    if end_index <= 0:
        end_index = total_pages
    if start_index > end_index:
        print(f"start-index {start_index} is greater than end-index {end_index}.", file=sys.stderr)
        return

    events: list[dict[str, str]] = []
    failures: list[FailureRecord] = []
    today = date.today()

    def save_checkpoint(processed_index: int) -> None:
        write_rows(output_path, events, output_fieldnames)
        write_rows(
            failed_path,
            [record.__dict__ for record in failures],
            FAILED_PAGE_FIELDNAMES,
        )
        save_progress(
            progress_path,
            last_processed_index=processed_index,
            next_start_index=min(total_pages + 1, processed_index + 1),
            total_pages=total_pages,
            saved_rows=len(events),
            saved_failures=len(failures),
        )
        print(
            f"[SAVE] rows={len(events)} failures={len(failures)} next_index={min(total_pages + 1, processed_index + 1)}"
        )

    last_processed_index = start_index - 1
    interrupted = False
    log_every = int(args.log_every or DEFAULT_LOG_EVERY)

    try:
        for idx in range(start_index - 1, end_index):
            row = list_pages[idx]
            list_url = row["list_url"]
            failure_start = len(failures)

            page_events: list[dict[str, str]] = []
            valid_count = 0

            try:
                if args.enable_instagram and "instagram.com" in list_url.lower():
                    try:
                        try:
                            from src.collect_instagram import extract_events_from_instagram, Institution as IgInstitution
                        except Exception:
                            from collect_instagram import extract_events_from_instagram, Institution as IgInstitution

                        instagram_failure_start = len(failures)
                        inst = IgInstitution(
                            institution_id=f"list-{idx + 1:05d}",
                            title=row.get("institution_name", "") or list_url,
                            category="",
                            official_url=list_url,
                            official_urls=[list_url],
                            region_main=row.get("region_main", ""),
                            region_sub=row.get("region_sub", ""),
                            source_query="",
                        )
                        ig_events = extract_events_from_instagram(inst, list_url, args, failures)
                        instagram_failure_stage, instagram_failure_type, instagram_failure_message = summarize_failures(
                            failures[instagram_failure_start:]
                        )
                        for event in ig_events:
                            page_events.append(
                                {
                                    "exhibition_name": event.get("event_name", ""),
                                    "start_date": event.get("start_date", ""),
                                    "end_date": event.get("end_date", ""),
                                    "location": "",
                                    "detail_url": list_url,
                                    "failure_stage": instagram_failure_stage,
                                    "failure_type": instagram_failure_type,
                                    "failure_message": instagram_failure_message,
                                }
                            )
                        valid_events = [evt for evt in page_events if is_event_valid(evt)]
                        page_events = [evt for evt in valid_events if is_event_current(evt, today)]
                        valid_count = len(valid_events)
                    except Exception as exc:
                        add_failure(failures, None, list_url, "instagram", exc)
                else:
                    page_events, valid_count = process_list_page(
                        list_url=list_url,
                        detail_fetch_mode=str(args.detail_fetch_mode),
                        js_render_mode=str(args.js_render_mode),
                        ocr_mode=str(args.ocr_mode),
                        timeout_seconds=float(args.timeout_seconds),
                        js_timeout_ms=int(args.js_render_timeout_ms),
                        ocr_timeout_seconds=float(args.ocr_timeout_seconds),
                        max_images=int(args.max_images_per_page),
                        failures=failures,
                        today=today,
                    )
            except Exception as exc:
                add_failure(failures, None, list_url, "unexpected", exc)
                page_events = []
                valid_count = 0

            page_failures = failures[failure_start:]
            page_failure_stage, page_failure_type, page_failure_message = summarize_failures(page_failures)

            if page_events:
                for event in page_events:
                    record = normalize_event(event)
                    record["source_list_url"] = list_url
                    record["status"] = "ok"
                    record["failure_stage"] = normalize_text(event.get("failure_stage", ""))
                    record["failure_type"] = normalize_text(event.get("failure_type", ""))
                    record["failure_message"] = normalize_text(event.get("failure_message", ""))
                    for col in pass_through_columns:
                        record[col] = normalize_text(row.get(col, ""))
                    events.append(record)
            else:
                status = "failed"
                if valid_count > 0:
                    status = "filtered"
                    if not page_failure_stage:
                        page_failure_stage = "filtered"
                    if not page_failure_type:
                        page_failure_type = "Filtered"
                    if not page_failure_message:
                        page_failure_message = "Filtered out by end_date < today"

                failure_record = {
                    "exhibition_name": "",
                    "start_date": "",
                    "end_date": "",
                    "location": "",
                    "detail_url": "",
                    "source_list_url": list_url,
                    "status": status,
                    "failure_stage": page_failure_stage or "no-events",
                    "failure_type": page_failure_type or "NoEvents",
                    "failure_message": page_failure_message or "No events extracted",
                }
                for col in pass_through_columns:
                    failure_record[col] = normalize_text(row.get(col, ""))
                events.append(failure_record)

            last_processed_index = idx + 1

            if log_every > 0 and (last_processed_index % log_every == 0 or last_processed_index == end_index):
                print(
                    f"[{last_processed_index}/{total_pages}] {list_url} -> {len(page_events)} events"
                )

            if args.pause_seconds > 0:
                time.sleep(float(args.pause_seconds))

            save_every = int(args.save_every or 0)
            if save_every > 0 and last_processed_index % save_every == 0:
                save_checkpoint(last_processed_index)
    except KeyboardInterrupt:
        interrupted = True
        print("[WARN] Interrupted by user. Saving partial results...")
    finally:
        save_checkpoint(last_processed_index)
        if interrupted:
            print("[SAVE] Partial results were saved after interruption.")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    run_pipeline(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())