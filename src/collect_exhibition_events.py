import argparse
import csv
import html
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
from http.client import IncompleteRead
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError

DEFAULT_INPUT_CSV = "data/test_naver_local_exhibitions.csv"
DEFAULT_OUTPUT_CSV = "data/extracted_exhibitions.csv"
DEFAULT_FAILED_DOMAINS_OUT = "data/failed_domains.csv"
DEFAULT_TIMEOUT_SECONDS = 8
DEFAULT_PAUSE_SECONDS = 0.15
DEFAULT_HTTP_RETRY_COUNT = 2
DEFAULT_MAX_PAGES_PER_INSTITUTION = 5
DEFAULT_MAX_BASE_URLS_PER_INSTITUTION = 3
DEFAULT_MAX_IMAGES_PER_PAGE = 3
DEFAULT_MAX_INSTITUTIONS = 0
DEFAULT_MIN_CONFIDENCE = 0.75
DEFAULT_SAVE_EVERY = 25
DEFAULT_JS_RENDER_TIMEOUT_MS = 12000
DEFAULT_PROGRESS_PATH = "data/exhibition_extraction_progress.json"

EXHIBITION_OUTPUT_FIELDNAMES = [
    "institution_id",
    "institution_title",
    "institution_url",
    "source_page_url",
    "source_page_count",
    "source_page_urls",
    "event_name",
    "description",
    "start_date",
    "end_date",
    "price_type",
    "price_text",
    "confidence",
    "evidence",
]

FAILED_DOMAIN_FIELDNAMES = [
    "domain",
    "fail_count",
    "stages",
    "error_types",
    "last_institution",
    "sample_url",
    "sample_message",
]

URL_SOURCE_FIELDS = [
    "official_url",
    "link",
    "homepage_url",
    "homepage_urls",
    "website",
    "websites",
    "url",
    "urls",
    "blog_url",
    "instagram_url",
]

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

NAVIGATION_NOISE_KEYWORDS = [
    "로그인",
    "회원가입",
    "마이페이지",
    "사이트맵",
    "개인정보처리방침",
    "이용약관",
    "저작권",
    "전체메뉴",
    "메뉴",
    "검색",
    "공지사항",
    "오시는길",
    "바로가기",
    "고객센터",
    "패밀리사이트",
]

EDITORIAL_NOISE_KEYWORDS = [
    "webzine",
    "인터뷰",
    "insight",
    "news",
    "press",
    "보도",
    "기사",
    "소식",
]

WEAK_EVENT_ONLY_KEYWORDS = {
    "전시",
    "전시안내",
    "전시 일정",
    "전시일정",
    "현재전시",
    "예정전시",
    "과거전시",
}

STRONG_EVENT_CONTEXT_KEYWORDS = [
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
    "레지던시",
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
class Institution:
    institution_id: str
    title: str
    category: str
    official_url: str
    official_urls: list[str]
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


def split_candidate_urls(raw_value: str) -> list[str]:
    value = normalize_text(html.unescape(raw_value or ""))
    if not value:
        return []

    extracted = re.findall(r"(?:https?://|www\.)[^\s|,;]+", value, flags=re.IGNORECASE)
    tokens = extracted if extracted else re.split(r"[|,;\n\r\t]+", value)

    urls: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        normalized = canonical_url(token)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


def collect_institution_urls(row: dict[str, str]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    for field in URL_SOURCE_FIELDS:
        for candidate in split_candidate_urls(row.get(field, "")):
            if candidate in seen:
                continue
            seen.add(candidate)
            urls.append(candidate)

    return urls


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
    merged: dict[tuple[str, str, str], dict[str, str | list[str] | set[str]]] = {}

    for row in rows:
        if not is_exhibition_related(row, keyword_hints):
            continue

        title = normalize_text(row.get("title", ""))
        if not title:
            continue

        region_main = normalize_text(row.get("region_main", ""))
        region_sub = normalize_text(row.get("region_sub", ""))
        key = (title, region_main, region_sub)
        discovered_urls = collect_institution_urls(row)
        if not discovered_urls:
            continue

        entry = merged.get(key)
        if entry is None:
            merged[key] = {
                "title": title,
                "category": normalize_text(row.get("category", "")),
                "region_main": region_main,
                "region_sub": region_sub,
                "source_query": normalize_text(row.get("source_query", "")),
                "official_urls": list(discovered_urls),
                "seen_urls": set(discovered_urls),
            }
            continue

        seen_urls = entry.get("seen_urls")
        known: set[str] = seen_urls if isinstance(seen_urls, set) else set()
        official_urls = entry.get("official_urls")
        url_list: list[str] = official_urls if isinstance(official_urls, list) else []

        for url in discovered_urls:
            if url in known:
                continue
            known.add(url)
            url_list.append(url)

        entry["official_urls"] = url_list
        entry["seen_urls"] = known

    institutions: list[Institution] = []
    for _, item in merged.items():
        official_urls_obj = item.get("official_urls")
        official_urls = official_urls_obj if isinstance(official_urls_obj, list) else []
        if not official_urls:
            continue

        institutions.append(
            Institution(
                institution_id=f"inst-{len(institutions) + 1:05d}",
                title=str(item.get("title", "")),
                category=str(item.get("category", "")),
                official_url=official_urls[0],
                official_urls=official_urls,
                region_main=str(item.get("region_main", "")),
                region_sub=str(item.get("region_sub", "")),
                source_query=str(item.get("source_query", "")),
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
        except (URLError, TimeoutError, ConnectionResetError, OSError, IncompleteRead) as exc:
            last_error = exc
            if attempt < DEFAULT_HTTP_RETRY_COUNT:
                time.sleep(0.25 * attempt)

    raise RuntimeError(f"HTTP request failed after retries: {last_error}")


def render_page_with_playwright(url: str, timeout_ms: int) -> str:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return ""

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()

        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 8000))
        except Exception:
            # Some pages never reach network idle; keep best-effort content.
            pass

        content = page.content()
        context.close()
        browser.close()
        return content


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
    base_url: str,
    max_pages: int,
    timeout: int,
    failures: list[FailureRecord],
) -> list[str]:
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


def extract_image_candidates(html_text: str, page_url: str) -> list[dict[str, str]]:
    parser = ImageExtractor()
    parser.feed(html_text)

    candidates: list[dict[str, str]] = []
    seen: set[str] = set()
    for image in parser.images:
        full = urllib.parse.urljoin(page_url, image.get("src", ""))
        if not full.startswith(("http://", "https://")):
            continue

        low = full.lower()
        meta = normalize_text(" ".join([image.get("alt", ""), image.get("title", ""), low]))
        if not IMAGE_EXT_PATTERN.search(low) and "image" not in low:
            continue
        if "logo" in meta or "icon" in meta or "banner" in meta:
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
            except Exception:  # noqa: BLE001
                continue

            score = score_ocr_text(text)
            if score > best_score or (score == best_score and len(text) > len(best_text)):
                best_score = score
                best_text = text

    return best_text


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


def extract_ocr_text_from_image(image_url: str, timeout: int) -> str:
    try:
        import io
        from PIL import Image
    except ImportError:
        return ""

    # URL 검증: 공백이나 제어문자가 있으면 건너뛰기
    if not image_url or any(ord(c) < 32 for c in image_url):
        return ""

    req = urllib.request.Request(
        image_url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; ExhibitionDataHub/1.0)",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
    except (urllib.error.HTTPError, urllib.error.URLError, Exception):
        return ""

    if not data:
        return ""

    try:
        img = Image.open(io.BytesIO(data))
    except Exception:  # noqa: BLE001
        return ""

    return run_tesseract_ocr_variants(img)


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
    if any(ch in line for ch in ["《", "》", "‘", "’", "“", "”"]):
        score += 0.2
    return min(score, 1.0)


def looks_like_navigation_noise(line: str) -> bool:
    compact = normalize_text(line)
    if not compact:
        return True

    if re.fullmatch(r"[\d\W_]+", compact):
        return True

    if any(term in compact for term in NAVIGATION_NOISE_KEYWORDS):
        return True

    lowered = compact.lower()
    if any(token in lowered for token in ["http://", "https://", "www."]):
        return True

    if compact.count("|") >= 2 or compact.count(">") >= 2:
        return True

    return False


def looks_like_editorial_noise(line: str) -> bool:
    compact = normalize_text(line)
    lowered = compact.lower()

    if re.match(r"^\[(news|eng|artist|special|insight)\]", lowered):
        return True

    if any(term in lowered for term in EDITORIAL_NOISE_KEYWORDS):
        return True

    return False


def has_strong_event_signal(line: str) -> bool:
    if any(key in line for key in STRONG_EVENT_CONTEXT_KEYWORDS):
        return True

    if any(ch in line for ch in ["《", "》", "‘", "’", "“", "”"]):
        return True

    return False


def parse_jsonld_events(html_text: str) -> list[dict[str, str]]:
    parser = JsonLdExtractor()
    parser.feed(html_text)
    events: list[dict[str, str]] = []

    def walk(node: object) -> None:
        if isinstance(node, dict):
            node_type = str(node.get("@type", "")).lower()
            if node_type == "event":
                name = normalize_text(str(node.get("name", "")))
                description = normalize_text(str(node.get("description", "")))
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
                        "description": description,
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
        if looks_like_navigation_noise(line):
            continue

        if any(term in line for term in EXCLUDE_EVENT_TEXT_KEYWORDS):
            continue

        if looks_like_editorial_noise(line):
            continue

        line_has_strong_title = any(key in line for key in STRONG_EVENT_TITLE_KEYWORDS) or any(
            ch in line for ch in ["《", "》", "‘", "’", "“", "”"]
        )

        if not line_has_strong_title and any(weak in line for weak in WEAK_EVENT_ONLY_KEYWORDS):
            if not has_strong_event_signal(line):
                continue

        if any(term in line for term in LOCATION_TEXT_KEYWORDS) and not line_has_strong_title:
            continue

        if "전시실" in line and any(token in line for token in ["/", "제 1", "제1", "제 2", "제2", "기획전시실"]):
            if not any(key in line for key in ["개인전", "특별전", "초대전", "그룹전", "비엔날레", "아트페어", "사진전", "조각전"]):
                continue

        title_score = event_title_score(line, keyword_hints)
        if title_score < 0.5:
            continue

        window = lines[max(0, idx - 3): min(len(lines), idx + 4)]
        context = " | ".join(window)

        if any(term in context for term in EXCLUDE_EVENT_TEXT_KEYWORDS):
            continue

        if not line_has_strong_title and not has_strong_event_signal(line):
            if not any(key in context for key in STRONG_EVENT_CONTEXT_KEYWORDS):
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
                "description": context[:300],
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
    enable_js_render: bool,
    js_render_timeout_ms: int,
    enable_image_ocr: bool,
    max_images_per_page: int,
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
    if lines:
        text_events = parse_text_events(lines, keyword_hints)
        if text_events:
            return text_events

    render_html = ""
    if enable_js_render:
        try:
            render_html = render_page_with_playwright(page_url, max(1000, js_render_timeout_ms))
        except Exception as exc:  # noqa: BLE001
            add_failure(failures, institution, page_url, "js-render", exc)
            render_html = ""

    effective_html = render_html if render_html else html_text

    if render_html:
        events = parse_jsonld_events(render_html)
        if events:
            return events

        lines = clean_lines_from_html(render_html)
        if lines:
            text_events = parse_text_events(lines, keyword_hints)
            if text_events:
                return text_events

    if not enable_image_ocr:
        return []

    images = extract_image_candidates(effective_html, page_url)
    if not images:
        return []

    ocr_events: list[dict[str, str]] = []
    for image in images[: max(0, max_images_per_page)]:
        image_url = image.get("url", "")
        if not image_url:
            continue

        try:
            ocr_text = extract_ocr_text_from_image(image_url, timeout)
        except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError, OSError) as exc:
            add_failure(failures, institution, image_url, "ocr-image", exc)
            continue

        if not ocr_text:
            continue

        ocr_lines = [line for line in re.split(r"[|\\n]+", ocr_text) if normalize_text(line)]
        parsed = parse_text_events(ocr_lines, keyword_hints)
        for event in parsed:
            raw_conf = float(event.get("confidence", "0") or 0)
            lowered = max(0.50, raw_conf - 0.12)
            event["confidence"] = f"{lowered:.2f}"
            event["description"] = event.get("description", "") or ocr_text[:300]
            event["evidence"] = f"ocr:{image_url} | {event.get('evidence', '')}"[:300]
            ocr_events.append(event)

    return ocr_events


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


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []

    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_existing_event_rows(path: Path) -> list[dict[str, str]]:
    return read_csv_rows(path)


def load_existing_failures(path: Path) -> list[FailureRecord]:
    failures: list[FailureRecord] = []
    for row in read_csv_rows(path):
        try:
            fail_count = max(1, int(row.get("fail_count", "1") or 1))
        except ValueError:
            fail_count = 1

        stages = [item.strip() for item in str(row.get("stages", "")).split(",") if item.strip()]
        error_types = [item.strip() for item in str(row.get("error_types", "")).split(",") if item.strip()]
        stage = stages[0] if stages else "previous-run"
        error_type = error_types[0] if error_types else "PreviousRun"
        domain = str(row.get("domain", "")).strip() or "(unknown)"
        institution_title = str(row.get("last_institution", "")).strip() or "(previous-run)"
        url = str(row.get("sample_url", "")).strip()
        message = str(row.get("sample_message", "")).strip() or "loaded from previous summary"

        for _ in range(fail_count):
            failures.append(
                FailureRecord(
                    institution_id="previous-run",
                    institution_title=institution_title,
                    domain=domain,
                    url=url,
                    stage=stage,
                    error_type=error_type,
                    error_message=message,
                )
            )

    return failures


def build_curated_events(
    event_rows: list[dict[str, str]],
    min_confidence: float,
) -> list[dict[str, str]]:
    dedup_events = consolidate_events(event_rows)
    return [
        row
        for row in dedup_events
        if float(row.get("confidence", "0") or 0) >= min_confidence
    ]


def save_progress(
    output_csv: Path,
    failed_domains_out: Path,
    event_rows: list[dict[str, str]],
    failures: list[FailureRecord],
    min_confidence: float,
) -> tuple[int, int]:
    curated_events = build_curated_events(event_rows, min_confidence)
    write_csv(
        output_csv,
        curated_events,
        EXHIBITION_OUTPUT_FIELDNAMES,
    )

    failed_domain_rows = summarize_failures(failures)
    write_csv(
        failed_domains_out,
        failed_domain_rows,
        FAILED_DOMAIN_FIELDNAMES,
    )
    return len(curated_events), len(failed_domain_rows)


def load_run_progress(progress_path: Path) -> dict:
    if not progress_path.exists():
        return {}
    try:
        return json.loads(progress_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_run_progress(
    progress_path: Path,
    last_processed_index: int,
    next_start_index: int,
    total_institutions: int,
    saved_rows: int,
    saved_failed: int,
) -> None:
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_processed_index": last_processed_index,
        "next_start_index": next_start_index,
        "total_institutions": total_institutions,
        "saved_rows": saved_rows,
        "saved_failed": saved_failed,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    progress_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
    progress_path = root / args.progress_file
    keyword_hints = load_markdown_bullets(root / "docs" / "keywords.md")

    all_institutions = load_institutions(input_csv, keyword_hints)
    if args.max_institutions > 0:
        all_institutions = all_institutions[:args.max_institutions]
    total_institutions = len(all_institutions)

    start_index = max(1, args.start_index)
    progress = load_run_progress(progress_path)
    progress_next_start = int(progress.get("next_start_index", 1) or 1)
    if not args.no_auto_resume and args.start_index == 1 and progress_next_start > 1:
        start_index = progress_next_start

    end_index = int(args.end_index) if args.end_index > 0 else total_institutions
    if start_index > end_index:
        print(f"[WARN] start-index {start_index} is greater than end-index {end_index}. Nothing to do.")
        return

    if start_index > total_institutions:
        print(
            f"[WARN] start-index {start_index} is beyond the available institution count ({total_institutions})."
        )
        institutions = []
    else:
        institutions = all_institutions[start_index - 1 : end_index]

    event_rows: list[dict[str, str]] = []
    failures: list[FailureRecord] = []
    if start_index > 1:
        previous_events = load_existing_event_rows(output_csv)
        if previous_events:
            event_rows.extend(previous_events)
            print(f"[RESUME] Loaded {len(previous_events)} existing exhibition rows from {output_csv}")

        previous_failures = load_existing_failures(failed_domains_out)
        if previous_failures:
            failures.extend(previous_failures)
            print(f"[RESUME] Loaded {len(previous_failures)} existing failure records from {failed_domains_out}")

    print(f"Loaded {len(institutions)} candidate institutions")
    print(f"Progress file: {progress_path}")
    if start_index > 1 or end_index < total_institutions:
        print(f"Processing range: [{start_index}, {end_index}] / {total_institutions} total")

    checkpoint_step = max(0, args.save_every)
    interrupted = False
    last_processed_index = start_index - 1

    def maybe_checkpoint(processed_count: int, force: bool = False) -> None:
        if not force:
            if checkpoint_step <= 0:
                return
            if processed_count <= 0 or processed_count % checkpoint_step != 0:
                return

        saved_rows, saved_failed = save_progress(
            output_csv,
            failed_domains_out,
            event_rows,
            failures,
            args.min_confidence,
        )
        save_run_progress(
            progress_path,
            last_processed_index=processed_count,
            next_start_index=min(total_institutions + 1, processed_count + 1),
            total_institutions=total_institutions,
            saved_rows=saved_rows,
            saved_failed=saved_failed,
        )
        phase = "final" if force else f"checkpoint @ {processed_count}"
        print(
            f"[SAVE] {phase}: {saved_rows} exhibition rows, {saved_failed} failed domains"
        )

    try:
        for idx, inst in enumerate(institutions, start=1):
            absolute_idx = start_index + idx - 1
            cap_base_urls = max(0, args.max_base_urls_per_institution)
            base_urls = inst.official_urls
            if cap_base_urls > 0:
                base_urls = base_urls[:cap_base_urls]

            pages: list[str] = []
            seen_pages: set[str] = set()
            for base_url in base_urls:
                discovered = discover_pages(
                    inst,
                    base_url,
                    args.max_pages_per_institution,
                    args.timeout,
                    failures,
                )
                for page in discovered:
                    if page in seen_pages:
                        continue
                    seen_pages.add(page)
                    pages.append(page)
                    if len(pages) >= args.max_pages_per_institution:
                        break
                if len(pages) >= args.max_pages_per_institution:
                    break

            print(
                f"[{absolute_idx}/{total_institutions}] {inst.title}: "
                f"[batch: {absolute_idx - start_index + 1}/{end_index - start_index + 1}] "
                f"{len(pages)} candidate pages from {len(base_urls)} base urls"
            )

            # If Instagram handling is enabled, process instagram base URLs with Playwright extractor
            if args.enable_instagram:
                for base_url in base_urls:
                    if "instagram.com" in base_url.lower():
                        try:
                            try:
                                from src.collect_instagram import extract_events_from_instagram
                            except Exception:
                                from collect_instagram import extract_events_from_instagram

                            ig_events = extract_events_from_instagram(inst, base_url, args, failures)
                            for event in ig_events:
                                source_page = event.get("evidence", "")
                                if source_page.startswith("instagram:"):
                                    source_page = source_page.split("instagram:", 1)[-1]

                                row = {
                                    "institution_id": inst.institution_id,
                                    "institution_title": inst.title,
                                    "institution_url": inst.official_url,
                                    "source_page_url": source_page or base_url,
                                    "event_name": event.get("event_name", ""),
                                    "description": event.get("description", ""),
                                    "start_date": event.get("start_date", ""),
                                    "end_date": event.get("end_date", ""),
                                    "price_type": event.get("price_type", "unknown"),
                                    "price_text": event.get("price_text", ""),
                                    "confidence": event.get("confidence", "0.00"),
                                    "evidence": event.get("evidence", "")[:500],
                                }
                                event_rows.append(row)
                        except Exception as exc:  # noqa: BLE001
                            add_failure(failures, inst, base_url, "instagram-invoke", exc)

            for page in pages:
                events = extract_events_from_page(
                    inst,
                    page,
                    args.timeout,
                    keyword_hints,
                    failures,
                    args.enable_js_render,
                    args.js_render_timeout_ms,
                    args.enable_image_ocr,
                    args.max_images_per_page,
                )
                for event in events:
                    row = {
                        "institution_id": inst.institution_id,
                        "institution_title": inst.title,
                        "institution_url": inst.official_url,
                        "source_page_url": page,
                        "event_name": event.get("event_name", ""),
                        "description": event.get("description", ""),
                        "start_date": event.get("start_date", ""),
                        "end_date": event.get("end_date", ""),
                        "price_type": event.get("price_type", "unknown"),
                        "price_text": event.get("price_text", ""),
                        "confidence": event.get("confidence", "0.00"),
                        "evidence": event.get("evidence", "")[:500],
                    }
                    event_rows.append(row)

                time.sleep(args.pause)

            maybe_checkpoint(absolute_idx)
            last_processed_index = absolute_idx
    except KeyboardInterrupt:
        interrupted = True
        print("[WARN] Interrupted by user. Saving partial results...")
    finally:
        saved_rows, saved_failed = save_progress(
            output_csv,
            failed_domains_out,
            event_rows,
            failures,
            args.min_confidence,
        )
        save_run_progress(
            progress_path,
            last_processed_index=last_processed_index,
            next_start_index=min(total_institutions + 1, last_processed_index + 1),
            total_institutions=total_institutions,
            saved_rows=saved_rows,
            saved_failed=saved_failed,
        )
        print(f"Saved {saved_rows} exhibition rows -> {output_csv}")
        print(f"Saved {saved_failed} failed domains -> {failed_domains_out}")
        if interrupted:
            print("[SAVE] Partial results were saved after interruption.")


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
        "--enable-js-render",
        action="store_true",
        help="Enable Playwright-based JavaScript rendering when static HTML extraction finds no events",
    )
    parser.add_argument(
        "--js-render-timeout-ms",
        type=int,
        default=DEFAULT_JS_RENDER_TIMEOUT_MS,
        help="Timeout in milliseconds for JS rendering page load",
    )
    parser.add_argument(
        "--enable-image-ocr",
        action="store_true",
        help="Enable OCR on discovered images when text/JSON-LD extraction finds no events",
    )
    parser.add_argument(
        "--enable-instagram",
        action="store_true",
        help="Enable Instagram profile/post extraction for rows that include instagram_url",
    )
    parser.add_argument(
        "--instagram-max-posts",
        type=int,
        default=int(os.getenv("INSTAGRAM_MAX_POSTS", "20") or 20),
        help="Max Instagram posts to inspect per profile",
    )
    parser.add_argument(
        "--instagram-post-delay",
        type=float,
        default=float(os.getenv("INSTAGRAM_POST_DELAY", "4.0") or 4.0),
        help="Delay in seconds between Instagram post requests",
    )
    parser.add_argument(
        "--instagram-profile-delay",
        type=float,
        default=float(os.getenv("INSTAGRAM_PROFILE_DELAY", "8.0") or 8.0),
        help="Delay in seconds before starting Instagram profile extraction",
    )
    parser.add_argument(
        "--instagram-random-delay-min",
        type=float,
        default=float(os.getenv("INSTAGRAM_RANDOM_DELAY_MIN", "10.0") or 10.0),
        help="Minimum random delay in seconds for Instagram requests",
    )
    parser.add_argument(
        "--instagram-random-delay-max",
        type=float,
        default=float(os.getenv("INSTAGRAM_RANDOM_DELAY_MAX", "30.0") or 30.0),
        help="Maximum random delay in seconds for Instagram requests",
    )
    parser.add_argument(
        "--instagram-timeout-ms",
        type=int,
        default=int(os.getenv("INSTAGRAM_TIMEOUT_MS", "10000") or 10000),
        help="Playwright timeout in milliseconds for Instagram page loads",
    )
    parser.add_argument(
        "--instagram-proxy",
        default=os.getenv("INSTAGRAM_PROXY", ""),
        help="Optional proxy URL for Instagram scraping, e.g. http://host:port",
    )
    parser.add_argument(
        "--instagram-username",
        default=os.getenv("INSTAGRAM_USER", ""),
        help="Optional Instagram username for login-based scraping",
    )
    parser.add_argument(
        "--instagram-password",
        default=os.getenv("INSTAGRAM_PASS", ""),
        help="Optional Instagram password for login-based scraping",
    )
    parser.add_argument(
        "--max-images-per-page",
        type=int,
        default=DEFAULT_MAX_IMAGES_PER_PAGE,
        help="Max images to OCR per page when --enable-image-ocr is enabled",
    )
    parser.add_argument(
        "--max-base-urls-per-institution",
        type=int,
        default=DEFAULT_MAX_BASE_URLS_PER_INSTITUTION,
        help="Max homepage URLs to try per institution. 0 means no limit.",
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
    parser.add_argument(
        "--save-every",
        type=int,
        default=DEFAULT_SAVE_EVERY,
        help="Save checkpoint every N institutions. 0 disables periodic checkpointing.",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=1,
        help="1-based institution index to start from. Use this to resume from a later point.",
    )
    parser.add_argument(
        "--end-index",
        type=int,
        default=0,
        help="1-based institution index to end at (inclusive). 0 means process until the end.",
    )
    parser.add_argument(
        "--progress-file",
        default=DEFAULT_PROGRESS_PATH,
        help="Progress JSON path used for automatic resume and last-processed tracking.",
    )
    parser.add_argument(
        "--no-auto-resume",
        action="store_true",
        help="Disable automatic resume from the progress JSON file.",
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
