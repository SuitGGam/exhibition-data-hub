#!/usr/bin/env python3
"""
Discover likely exhibition-information URLs for art venue candidates.

This script does not scrape exhibition bodies. It finds and ranks URLs that are
likely to contain exhibition lists, current/upcoming exhibitions, notices, news,
or program pages.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import socket
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen


DEFAULT_INPUT_NAME = "naver_local_exhibitions_art_candidates_yes_maybe.csv"
DEFAULT_OUTPUT_NAME = "naver_local_exhibitions_exhibition_url_candidates.csv"

# Optional Naver Search OpenAPI credentials.
# Paste keys here if you do not want to set environment variables or CLI options.
NAVER_CLIENT_ID = ""
NAVER_CLIENT_SECRET = ""

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

PRIMARY_EXHIBITION_KEYWORDS = [
    "전시",
    "전시안내",
    "전시정보",
    "현재전시",
    "예정전시",
    "진행전시",
    "지난전시",
    "상설전시",
    "특별전시",
    "기획전시",
    "exhibition",
    "exhibitions",
    "exhibit",
    "current exhibition",
    "upcoming exhibition",
    "past exhibition",
    "ongoing exhibition",
    "show",
    "shows",
    "what's on",
    "whats on",
    "whatson",
]

SECONDARY_INFO_KEYWORDS = [
    "공지",
    "공지사항",
    "새소식",
    "소식",
    "뉴스",
    "프로그램",
    "행사",
    "일정",
    "캘린더",
    "문화행사",
    "notice",
    "notices",
    "news",
    "program",
    "programs",
    "event",
    "events",
    "calendar",
    "schedule",
]

UPCOMING_KEYWORDS = [
    "예정",
    "예고",
    "upcoming",
    "coming",
    "future",
]

CURRENT_KEYWORDS = [
    "현재",
    "진행",
    "ongoing",
    "current",
    "now",
]

PAST_KEYWORDS = [
    "지난",
    "종료",
    "과거",
    "archive",
    "archives",
    "past",
    "closed",
]

NEGATIVE_KEYWORDS = [
    "대관",
    "공간대여",
    "예약",
    "예매",
    "채용",
    "로그인",
    "회원",
    "오시는길",
    "주차",
    "카페",
    "스토어",
    "상품",
    "교육",
    "강좌",
    "수업",
    "모집",
    "공모",
    "rent",
    "rental",
    "reservation",
    "booking",
    "login",
    "member",
    "recruit",
    "career",
    "shop",
    "store",
    "cafe",
    "education",
    "class",
]

LIKELY_PATHS = [
    "/exhibition",
    "/exhibitions",
    "/exhibit",
    "/exhibits",
    "/ex",
    "/display",
    "/show",
    "/shows",
    "/whatson",
    "/whats-on",
    "/whats_on",
    "/program",
    "/programs",
    "/event",
    "/events",
    "/notice",
    "/notices",
    "/news",
    "/board",
    "/bbs",
    "/calendar",
    "/schedule",
    "/ko/exhibition",
    "/kr/exhibition",
    "/kor/exhibition",
    "/html/exhibition",
    "/sub/exhibition",
    "/contents/exhibition",
]

PATH_HINT_PATTERNS = [
    (r"/exhibition", 55, "url_path=exhibition"),
    (r"/exhibitions", 55, "url_path=exhibitions"),
    (r"/exhibit", 50, "url_path=exhibit"),
    (r"/display", 42, "url_path=display"),
    (r"/show", 38, "url_path=show"),
    (r"what[s-]?on", 44, "url_path=whatson"),
    (r"bo_table=(ex|exhibition|exhibit|gallery)", 58, "board_param=exhibition"),
    (r"bbs.*(ex|exhibition|exhibit|gallery)", 45, "bbs_path=exhibition"),
    (r"board.*(ex|exhibition|exhibit|gallery)", 45, "board_path=exhibition"),
    (r"program", 28, "url_path=program"),
    (r"notice", 25, "url_path=notice"),
    (r"news", 22, "url_path=news"),
    (r"calendar|schedule", 22, "url_path=schedule"),
]


@dataclass
class FetchResult:
    url: str
    final_url: str = ""
    status: str = ""
    http_status: str = ""
    content_type: str = ""
    text: str = ""
    error: str = ""


@dataclass
class PageSummary:
    title: str = ""
    headings: str = ""
    meta: str = ""
    links: list[dict[str, str]] = field(default_factory=list)


@dataclass
class Candidate:
    url: str
    source: str
    anchor_text: str = ""
    source_context: str = ""
    initial_score: int = 0
    page_score: int = 0
    confidence: int = 0
    candidate_type: str = "weak_candidate"
    evidence: list[str] = field(default_factory=list)
    http_status: str = ""
    fetch_status: str = ""
    page_title: str = ""
    heading_text: str = ""
    error: str = ""


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[dict[str, str]] = []
        self.title_parts: list[str] = []
        self.heading_parts: list[str] = []
        self.meta_parts: list[str] = []
        self.tag_stack: list[str] = []
        self.in_title = False
        self.in_heading = False
        self.current_heading: list[str] = []
        self.current_anchor: dict[str, str] | None = None
        self.current_anchor_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attr_map = {key.lower(): value or "" for key, value in attrs}
        context = self._context_label(tag, attr_map)
        self.tag_stack.append(context or tag)

        if tag == "title":
            self.in_title = True
        elif tag in {"h1", "h2", "h3"}:
            self.in_heading = True
            self.current_heading = []
        elif tag == "meta":
            name = (attr_map.get("name") or attr_map.get("property") or "").lower()
            if name in {"description", "keywords", "og:title", "og:description"}:
                self.meta_parts.append(attr_map.get("content", ""))
        elif tag == "a":
            self.current_anchor = {
                "href": attr_map.get("href", ""),
                "context": "|".join(self.tag_stack),
            }
            self.current_anchor_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "title":
            self.in_title = False
        elif tag in {"h1", "h2", "h3"} and self.in_heading:
            heading = normalize_space(" ".join(self.current_heading))
            if heading:
                self.heading_parts.append(heading)
            self.in_heading = False
            self.current_heading = []
        elif tag == "a" and self.current_anchor is not None:
            self.current_anchor["text"] = normalize_space(" ".join(self.current_anchor_parts))
            if self.current_anchor.get("href"):
                self.links.append(self.current_anchor)
            self.current_anchor = None
            self.current_anchor_parts = []

        if self.tag_stack:
            self.tag_stack.pop()

    def handle_data(self, data: str) -> None:
        data = normalize_space(data)
        if not data:
            return
        if self.in_title:
            self.title_parts.append(data)
        if self.in_heading:
            self.current_heading.append(data)
        if self.current_anchor is not None:
            self.current_anchor_parts.append(data)

    @staticmethod
    def _context_label(tag: str, attrs: dict[str, str]) -> str:
        values = " ".join([tag, attrs.get("id", ""), attrs.get("class", ""), attrs.get("role", "")]).lower()
        if tag in {"header", "nav"}:
            return tag
        if re.search(r"\b(nav|gnb|lnb|menu|header|sitemap)\b", values):
            return "nav_like"
        return ""

    def summary(self) -> PageSummary:
        return PageSummary(
            title=normalize_space(" ".join(self.title_parts))[:300],
            headings=normalize_space(" | ".join(self.heading_parts[:8]))[:500],
            meta=normalize_space(" | ".join(self.meta_parts[:6]))[:500],
            links=self.links,
        )


def normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_url(url: str) -> str:
    url = html.unescape(normalize_space(url))
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if not re.match(r"^https?://", url, flags=re.IGNORECASE):
        return "https://" + url
    return url


def clean_url(url: str) -> str:
    parsed = urlparse(normalize_url(url))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    parsed = parsed._replace(fragment="")
    return urlunparse(parsed)


def host_key(url: str) -> str:
    host = (urlparse(url).netloc or "").lower()
    return host[4:] if host.startswith("www.") else host


def is_internal_url(base_url: str, url: str) -> bool:
    return host_key(base_url) == host_key(url)


def decoded_url_text(url: str) -> str:
    return unquote(url).lower()


def get_naver_credentials(client_id: str = "", client_secret: str = "") -> tuple[str, str]:
    resolved_id = normalize_space(client_id) or normalize_space(NAVER_CLIENT_ID) or normalize_space(os.getenv("NAVER_CLIENT_ID"))
    resolved_secret = (
        normalize_space(client_secret)
        or normalize_space(NAVER_CLIENT_SECRET)
        or normalize_space(os.getenv("NAVER_CLIENT_SECRET"))
    )
    return resolved_id, resolved_secret


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
    return str(Path(os.getenv("ARTMOA_DATA_DIR", "data")) / filename)


def choose_homepage(row: dict[str, str]) -> str:
    for field_name in ("verified_url", "input_url", "api_found_url", "official_url", "link"):
        url = clean_url(row.get(field_name, ""))
        if url:
            return url
    return ""


def request_url(url: str, timeout: float, max_bytes: int = 700_000) -> FetchResult:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
        },
    )
    try:
        context = ssl.create_default_context()
        with urlopen(request, timeout=timeout, context=context) as response:
            raw = response.read(max_bytes)
            content_type = response.headers.get("Content-Type", "")
            encoding = get_encoding(content_type)
            return FetchResult(
                url=url,
                final_url=response.geturl(),
                status="ok",
                http_status=str(getattr(response, "status", "")),
                content_type=content_type,
                text=raw.decode(encoding, errors="replace"),
            )
    except HTTPError as exc:
        return FetchResult(
            url=url,
            final_url=getattr(exc, "url", url),
            status="http_error",
            http_status=str(exc.code),
            error=str(exc.reason),
        )
    except (URLError, TimeoutError, socket.timeout, ssl.SSLError) as exc:
        return FetchResult(url=url, status="error", error=str(exc)[:240])
    except Exception as exc:  # noqa: BLE001 - batch discovery should keep moving.
        return FetchResult(url=url, status="error", error=f"{type(exc).__name__}: {exc}"[:240])


def get_encoding(content_type: str) -> str:
    match = re.search(r"charset=([\w.-]+)", content_type or "", flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return "utf-8"


def parse_html(text: str) -> PageSummary:
    parser = LinkExtractor()
    parser.feed(text or "")
    return parser.summary()


def read_rows(path: str, encoding: str) -> list[dict[str, str]]:
    with open(path, "r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        return [{key: value or "" for key, value in row.items()} for row in reader]


def write_rows(path: str, rows: list[dict[str, str]], encoding: str) -> None:
    fieldnames = [
        "venue_index",
        "venue_title",
        "is_art_venue",
        "venue_type",
        "homepage_url",
        "homepage_status",
        "rank",
        "candidate_url",
        "candidate_type",
        "confidence",
        "score",
        "source",
        "source_link_text",
        "evidence_text",
        "http_status",
        "fetch_status",
        "page_title",
        "heading_text",
        "needs_manual_review",
        "error",
    ]
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding=encoding, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, path)


def add_candidate(
    candidates: dict[str, Candidate],
    url: str,
    source: str,
    anchor_text: str = "",
    source_context: str = "",
) -> None:
    url = clean_url(url)
    if not url:
        return
    key = canonical_candidate_key(url)
    if key in candidates:
        current = candidates[key]
        if source not in current.source.split("|"):
            current.source += f"|{source}"
        if anchor_text and anchor_text not in current.anchor_text:
            current.anchor_text = normalize_space(f"{current.anchor_text} | {anchor_text}").strip(" |")
        if source_context and source_context not in current.source_context:
            current.source_context = normalize_space(f"{current.source_context} | {source_context}").strip(" |")
        return
    candidates[key] = Candidate(url=url, source=source, anchor_text=anchor_text, source_context=source_context)


def add_inferred_list_candidates(candidates: dict[str, Candidate], candidate_url: str, anchor_text: str = "") -> None:
    for inferred_url, reason in infer_list_urls(candidate_url):
        add_candidate(candidates, inferred_url, "inferred_list_path", anchor_text, reason)


def infer_list_urls(url: str) -> list[tuple[str, str]]:
    url = clean_url(url)
    if not url:
        return []
    parsed = urlparse(url)
    path = parsed.path or "/"
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    inferred: list[tuple[str, str]] = []

    detail_segments = {"detail", "view", "read", "view.do", "detail.do", "read.do"}
    parts = [part for part in path.split("/") if part]
    if parts and parts[-1].lower() in detail_segments:
        parent_path = "/" + "/".join(parts[:-1])
        inferred.append((urlunparse(parsed._replace(path=parent_path or "/", query="", fragment="")), "removed_detail_path"))
        for replacement in ("list", "index"):
            replaced_path = "/" + "/".join(parts[:-1] + [replacement])
            inferred.append((urlunparse(parsed._replace(path=replaced_path, query="", fragment="")), f"detail_to_{replacement}"))

    if re.search(r"(detail|view|read)", path, flags=re.IGNORECASE):
        without_query = urlunparse(parsed._replace(query="", fragment=""))
        inferred.append((without_query, "removed_detail_query"))

    remove_params = {
        "id",
        "idx",
        "seq",
        "no",
        "num",
        "wr_id",
        "exno",
        "ex_no",
        "exhibitionid",
        "articleid",
        "postid",
        "boardseq",
    }
    kept_pairs = [(key, value) for key, value in query_pairs if key.lower() not in remove_params]
    if query_pairs and len(kept_pairs) < len(query_pairs):
        cleaned_query = urlencode(kept_pairs, doseq=True)
        inferred.append((urlunparse(parsed._replace(query=cleaned_query, fragment="")), "removed_detail_params"))

    clean_inferred = []
    seen = set()
    for inferred_url, reason in inferred:
        inferred_url = clean_url(inferred_url)
        if inferred_url and inferred_url != url and inferred_url not in seen:
            clean_inferred.append((inferred_url, reason))
            seen.add(inferred_url)
    return clean_inferred


def canonical_candidate_key(url: str) -> str:
    parsed = urlparse(clean_url(url))
    host = host_key(url)
    path = (parsed.path or "/").rstrip("/") or "/"
    query = parsed.query
    return f"{host}{path}?{query}".lower()


def collect_homepage_links(base_url: str, summary: PageSummary, max_home_links: int) -> dict[str, Candidate]:
    candidates: dict[str, Candidate] = {}
    for link in summary.links[:max_home_links]:
        href = link.get("href", "")
        if href.startswith(("mailto:", "tel:", "javascript:")):
            continue
        absolute_url = clean_url(urljoin(base_url, href))
        if not absolute_url or not is_internal_url(base_url, absolute_url):
            continue
        anchor_text = normalize_space(link.get("text", ""))
        context = normalize_space(link.get("context", ""))
        if looks_potentially_relevant(absolute_url, anchor_text, context):
            source = "homepage_nav" if is_nav_context(context) else "homepage_link"
            add_candidate(candidates, absolute_url, source, anchor_text, context)
            add_inferred_list_candidates(candidates, absolute_url, anchor_text)
    return candidates


def is_nav_context(context: str) -> bool:
    return any(token in context for token in ("header", "nav", "nav_like"))


def looks_potentially_relevant(url: str, text: str, context: str = "") -> bool:
    haystack = f"{decoded_url_text(url)} {text.lower()} {context.lower()}"
    if any(keyword.lower() in haystack for keyword in PRIMARY_EXHIBITION_KEYWORDS + SECONDARY_INFO_KEYWORDS):
        return True
    if any(re.search(pattern, haystack, flags=re.IGNORECASE) for pattern, _, _ in PATH_HINT_PATTERNS):
        return True
    return False


def collect_common_paths(base_url: str) -> dict[str, Candidate]:
    candidates: dict[str, Candidate] = {}
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    for path in LIKELY_PATHS:
        add_candidate(candidates, urljoin(root, path), "common_path", path.strip("/"), "")
    return candidates


def collect_robots_and_sitemap_urls(base_url: str, timeout: float, max_sitemap_urls: int) -> dict[str, Candidate]:
    candidates: dict[str, Candidate] = {}
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    sitemap_urls = [urljoin(root, "/sitemap.xml"), urljoin(root, "/sitemap_index.xml")]

    robots = request_url(urljoin(root, "/robots.txt"), timeout, max_bytes=150_000)
    if robots.status == "ok":
        for line in robots.text.splitlines():
            match = re.match(r"\s*Sitemap:\s*(\S+)", line, flags=re.IGNORECASE)
            if match:
                sitemap_urls.append(match.group(1).strip())

    seen_sitemaps = set()
    locs: list[str] = []
    for sitemap_url in sitemap_urls:
        sitemap_url = clean_url(sitemap_url)
        if not sitemap_url or sitemap_url in seen_sitemaps:
            continue
        seen_sitemaps.add(sitemap_url)
        sitemap = request_url(sitemap_url, timeout, max_bytes=1_500_000)
        if sitemap.status != "ok":
            continue
        current_locs = extract_sitemap_locs(sitemap.text)
        nested_sitemaps = [loc for loc in current_locs if loc.lower().endswith(".xml")][:10]
        for nested in nested_sitemaps:
            nested_fetch = request_url(nested, timeout, max_bytes=1_500_000)
            if nested_fetch.status == "ok":
                current_locs.extend(extract_sitemap_locs(nested_fetch.text))
        locs.extend(current_locs)
        if len(locs) >= max_sitemap_urls:
            break

    for loc in locs[:max_sitemap_urls]:
        loc = clean_url(loc)
        if loc and is_internal_url(base_url, loc) and looks_potentially_relevant(loc, "", ""):
            add_candidate(candidates, loc, "sitemap", "", "")
            add_inferred_list_candidates(candidates, loc, "")
    return candidates


def extract_sitemap_locs(text: str) -> list[str]:
    locs = re.findall(r"<loc>\s*(.*?)\s*</loc>", text or "", flags=re.IGNORECASE | re.DOTALL)
    return [html.unescape(normalize_space(loc)) for loc in locs]


def collect_naver_site_search_urls(
    row: dict[str, str],
    base_url: str,
    timeout: float,
    client_id: str,
    client_secret: str,
) -> dict[str, Candidate]:
    candidates: dict[str, Candidate] = {}
    client_id, client_secret = get_naver_credentials(client_id, client_secret)
    if not client_id or not client_secret:
        return candidates

    title = normalize_space(row.get("title"))
    host = host_key(base_url)
    query = normalize_space(f"site:{host} {title} 전시 예정전시 현재전시")
    api_url = (
        "https://openapi.naver.com/v1/search/webkr.json"
        f"?query={quote(query)}&display=10&start=1&sort=sim"
    )
    request = Request(
        api_url,
        headers={
            "X-Naver-Client-Id": client_id,
            "X-Naver-Client-Secret": client_secret,
            "User-Agent": USER_AGENT,
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
    except Exception:
        return candidates

    for item in payload.get("items", []):
        item_url = clean_url(html.unescape(item.get("link", "")))
        if not item_url or not is_internal_url(base_url, item_url):
            continue
        title_text = strip_html(item.get("title", ""))
        desc_text = strip_html(item.get("description", ""))
        if looks_potentially_relevant(item_url, f"{title_text} {desc_text}", ""):
            add_candidate(candidates, item_url, "naver_site_search", title_text, desc_text)
            add_inferred_list_candidates(candidates, item_url, title_text)
    return candidates


def strip_html(value: str) -> str:
    value = re.sub(r"<[^>]*>", " ", value or "")
    return html.unescape(normalize_space(value))


def score_candidate(candidate: Candidate, page_summary: PageSummary | None = None) -> Candidate:
    haystack = " ".join(
        [
            decoded_url_text(candidate.url),
            candidate.anchor_text.lower(),
            candidate.source_context.lower(),
            candidate.source.lower(),
        ]
    )
    evidence: list[str] = []
    score = 0

    if any(keyword.lower() in haystack for keyword in PRIMARY_EXHIBITION_KEYWORDS):
        score += 55
        evidence.append("primary_exhibition_keyword")
    if any(keyword.lower() in haystack for keyword in UPCOMING_KEYWORDS):
        score += 14
        evidence.append("upcoming_keyword")
    if any(keyword.lower() in haystack for keyword in CURRENT_KEYWORDS):
        score += 12
        evidence.append("current_keyword")
    if any(keyword.lower() in haystack for keyword in PAST_KEYWORDS):
        score += 8
        evidence.append("past_keyword")
    if any(keyword.lower() in haystack for keyword in SECONDARY_INFO_KEYWORDS):
        score += 26
        evidence.append("secondary_info_keyword")

    for pattern, points, reason in PATH_HINT_PATTERNS:
        if re.search(pattern, haystack, flags=re.IGNORECASE):
            score += points
            evidence.append(reason)

    if "homepage_nav" in candidate.source:
        score += 16
        evidence.append("source=homepage_nav")
    if "homepage_link" in candidate.source:
        score += 8
        evidence.append("source=homepage_link")
    if "sitemap" in candidate.source:
        score += 10
        evidence.append("source=sitemap")
    if "naver_site_search" in candidate.source:
        score += 18
        evidence.append("source=naver_site_search")
    if "common_path" in candidate.source:
        score += 2
        evidence.append("source=common_path")
    if "inferred_list_path" in candidate.source:
        score += 18
        evidence.append("source=inferred_list_path")

    if is_detail_like_url(candidate.url):
        score -= 28
        evidence.append("detail_page_penalty")
    else:
        score += 10
        evidence.append("list_like_url")

    negative_hits = [keyword for keyword in NEGATIVE_KEYWORDS if keyword.lower() in haystack]
    if negative_hits:
        score -= min(45, 12 * len(negative_hits))
        evidence.append("negative=" + "|".join(negative_hits[:4]))

    candidate.initial_score = max(0, score)

    if page_summary is not None:
        page_text = " ".join([page_summary.title.lower(), page_summary.headings.lower(), page_summary.meta.lower()])
        page_score = 0
        if any(keyword.lower() in page_text for keyword in PRIMARY_EXHIBITION_KEYWORDS):
            page_score += 35
            evidence.append("page_title_heading_exhibition")
        if any(keyword.lower() in page_text for keyword in UPCOMING_KEYWORDS):
            page_score += 8
            evidence.append("page_upcoming_keyword")
        if any(keyword.lower() in page_text for keyword in CURRENT_KEYWORDS):
            page_score += 6
            evidence.append("page_current_keyword")
        if any(keyword.lower() in page_text for keyword in SECONDARY_INFO_KEYWORDS):
            page_score += 16
            evidence.append("page_notice_news_program")
        page_negative_hits = [keyword for keyword in NEGATIVE_KEYWORDS if keyword.lower() in page_text]
        if page_negative_hits:
            page_score -= min(35, 8 * len(page_negative_hits))
            evidence.append("page_negative=" + "|".join(page_negative_hits[:4]))
        candidate.page_score = page_score

    if candidate.fetch_status == "ok" and candidate.http_status and not candidate.http_status.startswith("2"):
        candidate.page_score -= 30
        evidence.append("non_2xx_status")
    elif candidate.fetch_status in {"error", "http_error"}:
        candidate.page_score -= 18
        evidence.append("fetch_failed")

    total = max(0, min(100, candidate.initial_score + candidate.page_score))
    if candidate.fetch_status in {"error", "http_error"}:
        total = min(total, 45)
    elif not candidate.fetch_status and "common_path" in candidate.source:
        total = min(total, 48)
    elif not candidate.fetch_status and "inferred_list_path" in candidate.source:
        total = min(total, 65)
    elif not candidate.fetch_status:
        total = min(total, 80)
    if is_detail_like_url(candidate.url):
        total = min(total, 72)
    candidate.confidence = total
    candidate.candidate_type = classify_candidate_type(candidate.url, candidate.anchor_text, page_summary)
    candidate.evidence = list(dict.fromkeys(evidence))
    return candidate


def classify_candidate_type(url: str, anchor_text: str, page_summary: PageSummary | None = None) -> str:
    text = f"{decoded_url_text(url)} {anchor_text.lower()}"
    if page_summary is not None:
        text += f" {page_summary.title.lower()} {page_summary.headings.lower()} {page_summary.meta.lower()}"

    has_exhibition = any(keyword.lower() in text for keyword in PRIMARY_EXHIBITION_KEYWORDS)
    if has_exhibition and is_detail_like_url(url):
        return "exhibition_detail"
    if has_exhibition and any(keyword.lower() in text for keyword in UPCOMING_KEYWORDS):
        return "upcoming_exhibition"
    if has_exhibition and any(keyword.lower() in text for keyword in CURRENT_KEYWORDS):
        return "current_exhibition"
    if has_exhibition and any(keyword.lower() in text for keyword in PAST_KEYWORDS):
        return "past_exhibition"
    if has_exhibition:
        return "exhibition_list"
    if any(keyword.lower() in text for keyword in ["공지", "공지사항", "notice", "notices"]):
        return "notice_likely"
    if any(keyword.lower() in text for keyword in ["소식", "뉴스", "news"]):
        return "news_likely"
    if any(keyword.lower() in text for keyword in ["프로그램", "행사", "program", "event"]):
        return "program_likely"
    return "weak_candidate"


def is_detail_like_url(url: str) -> bool:
    text = decoded_url_text(url)
    parsed = urlparse(url)
    query_keys = {key.lower() for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}
    detail_keys = {
        "id",
        "idx",
        "seq",
        "no",
        "num",
        "wr_id",
        "exno",
        "ex_no",
        "exhibitionid",
        "articleid",
        "postid",
        "boardseq",
    }
    if query_keys & detail_keys:
        return True
    return bool(re.search(r"/(detail|view|read)(\.do|\.php|\.asp|\.aspx|/)?", text, flags=re.IGNORECASE))


def probe_candidates(
    candidates: list[Candidate],
    timeout: float,
    probe_pages: int,
) -> list[Candidate]:
    if probe_pages <= 0:
        return [score_candidate(candidate) for candidate in candidates]

    ranked = sorted((score_candidate(candidate) for candidate in candidates), key=lambda item: item.initial_score, reverse=True)
    probe_targets = ranked[:probe_pages]
    for candidate in probe_targets:
        fetch = request_url(candidate.url, timeout, max_bytes=500_000)
        candidate.fetch_status = fetch.status
        candidate.http_status = fetch.http_status
        candidate.error = fetch.error
        if fetch.status == "ok" and "html" in (fetch.content_type or "").lower():
            summary = parse_html(fetch.text)
            candidate.page_title = summary.title
            candidate.heading_text = summary.headings
            score_candidate(candidate, summary)
        else:
            score_candidate(candidate)

    for candidate in ranked[probe_pages:]:
        score_candidate(candidate)
    return ranked


def process_venue(
    index: int,
    row: dict[str, str],
    *,
    timeout: float,
    top_n: int,
    max_home_links: int,
    max_sitemap_urls: int,
    probe_pages: int,
    use_sitemap: bool,
    use_common_paths: bool,
    use_naver_search: bool,
    naver_client_id: str,
    naver_client_secret: str,
) -> list[dict[str, str]]:
    title = normalize_space(row.get("title"))
    homepage_url = choose_homepage(row)
    if not homepage_url:
        return [
            output_row(
                index,
                row,
                homepage_url="",
                homepage_status="no_homepage_url",
                rank=1,
                candidate=None,
                needs_manual_review="yes",
                error="No verified/input/api URL available.",
            )
        ]

    all_candidates: dict[str, Candidate] = {}
    homepage = request_url(homepage_url, timeout, max_bytes=800_000)
    homepage_status = homepage.status
    final_homepage_url = clean_url(homepage.final_url or homepage_url) or homepage_url

    if homepage.status == "ok" and "html" in (homepage.content_type or "").lower():
        summary = parse_html(homepage.text)
        for key, candidate in collect_homepage_links(final_homepage_url, summary, max_home_links).items():
            all_candidates[key] = candidate
        if looks_potentially_relevant(final_homepage_url, f"{summary.title} {summary.headings}", ""):
            add_candidate(all_candidates, final_homepage_url, "homepage_self", title, summary.title)
    else:
        add_candidate(all_candidates, final_homepage_url, "homepage_unreadable", title, "")

    if use_sitemap:
        for key, candidate in collect_robots_and_sitemap_urls(final_homepage_url, timeout, max_sitemap_urls).items():
            if key in all_candidates:
                add_candidate(all_candidates, candidate.url, candidate.source, candidate.anchor_text, candidate.source_context)
            else:
                all_candidates[key] = candidate

    if use_common_paths:
        for key, candidate in collect_common_paths(final_homepage_url).items():
            if key in all_candidates:
                add_candidate(all_candidates, candidate.url, candidate.source, candidate.anchor_text, candidate.source_context)
            else:
                all_candidates[key] = candidate

    if use_naver_search:
        for key, candidate in collect_naver_site_search_urls(
            row,
            final_homepage_url,
            timeout,
            naver_client_id,
            naver_client_secret,
        ).items():
            if key in all_candidates:
                add_candidate(all_candidates, candidate.url, candidate.source, candidate.anchor_text, candidate.source_context)
            else:
                all_candidates[key] = candidate

    candidates = list(all_candidates.values())
    if not candidates:
        return [
            output_row(
                index,
                row,
                homepage_url=final_homepage_url,
                homepage_status=homepage_status,
                rank=1,
                candidate=None,
                needs_manual_review="yes",
                error="No relevant internal links found.",
            )
        ]

    probed = probe_candidates(candidates, timeout, probe_pages)
    strong = [candidate for candidate in probed if candidate.confidence >= 30]
    selected = sorted(strong or probed, key=lambda item: item.confidence, reverse=True)[:top_n]
    return [
        output_row(
            index,
            row,
            homepage_url=final_homepage_url,
            homepage_status=homepage_status,
            rank=rank,
            candidate=candidate,
            needs_manual_review="yes" if candidate.confidence < 55 else "no",
            error="",
        )
        for rank, candidate in enumerate(selected, 1)
    ]


def output_row(
    index: int,
    row: dict[str, str],
    *,
    homepage_url: str,
    homepage_status: str,
    rank: int,
    candidate: Candidate | None,
    needs_manual_review: str,
    error: str,
) -> dict[str, str]:
    if candidate is None:
        return {
            "venue_index": str(index),
            "venue_title": normalize_space(row.get("title")),
            "is_art_venue": normalize_space(row.get("is_art_venue")),
            "venue_type": normalize_space(row.get("venue_type")),
            "homepage_url": homepage_url,
            "homepage_status": homepage_status,
            "rank": str(rank),
            "candidate_url": "",
            "candidate_type": "not_found",
            "confidence": "0",
            "score": "0",
            "source": "",
            "source_link_text": "",
            "evidence_text": "",
            "http_status": "",
            "fetch_status": "",
            "page_title": "",
            "heading_text": "",
            "needs_manual_review": needs_manual_review,
            "error": error,
        }

    return {
        "venue_index": str(index),
        "venue_title": normalize_space(row.get("title")),
        "is_art_venue": normalize_space(row.get("is_art_venue")),
        "venue_type": normalize_space(row.get("venue_type")),
        "homepage_url": homepage_url,
        "homepage_status": homepage_status,
        "rank": str(rank),
        "candidate_url": candidate.url,
        "candidate_type": candidate.candidate_type,
        "confidence": str(candidate.confidence),
        "score": str(candidate.initial_score + candidate.page_score),
        "source": candidate.source,
        "source_link_text": candidate.anchor_text[:500],
        "evidence_text": "; ".join(candidate.evidence)[:1000],
        "http_status": candidate.http_status,
        "fetch_status": candidate.fetch_status,
        "page_title": candidate.page_title[:300],
        "heading_text": candidate.heading_text[:500],
        "needs_manual_review": needs_manual_review,
        "error": candidate.error or error,
    }


def run(args: argparse.Namespace) -> int:
    load_env_file(args.env_file)
    args.input = args.input or default_data_path(DEFAULT_INPUT_NAME)
    args.output = args.output or default_data_path(DEFAULT_OUTPUT_NAME)
    started = time.time()
    rows = read_rows(args.input, args.encoding)
    if args.limit:
        rows = rows[: args.limit]

    output_rows: list[dict[str, str]] = []
    total = len(rows)
    checkpoint_every = max(0, args.checkpoint_every)

    def save_checkpoint(done: int) -> None:
        write_rows(args.output, output_rows, args.encoding)
        print(f"Saved checkpoint: {done}/{total} -> {args.output}", file=sys.stderr, flush=True)

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                process_venue,
                index,
                row,
                timeout=args.timeout,
                top_n=args.top_n,
                max_home_links=args.max_home_links,
                max_sitemap_urls=args.max_sitemap_urls,
                probe_pages=args.probe_pages,
                use_sitemap=not args.no_sitemap,
                use_common_paths=not args.no_common_paths,
                use_naver_search=args.use_naver_search,
                naver_client_id=args.naver_client_id,
                naver_client_secret=args.naver_client_secret,
            ): index
            for index, row in enumerate(rows, 1)
        }

        done = 0
        for future in as_completed(futures):
            done += 1
            try:
                output_rows.extend(future.result())
            except Exception as exc:  # noqa: BLE001
                index = futures[future]
                row = rows[index - 1]
                output_rows.append(
                    output_row(
                        index,
                        row,
                        homepage_url=choose_homepage(row),
                        homepage_status="error",
                        rank=1,
                        candidate=None,
                        needs_manual_review="yes",
                        error=f"{type(exc).__name__}: {exc}"[:300],
                    )
                )
            if done % 25 == 0 or done == total:
                print(f"Processed venues: {done}/{total}", file=sys.stderr, flush=True)
            if checkpoint_every and (done % checkpoint_every == 0 or done == total):
                save_checkpoint(done)

    write_rows(args.output, output_rows, args.encoding)
    print_summary(output_rows, time.time() - started)
    print(f"\nSaved: {args.output}")
    return 0


def print_summary(rows: list[dict[str, str]], elapsed: float) -> None:
    total_venues = len({row["venue_index"] for row in rows})
    found_venues = len({row["venue_index"] for row in rows if row.get("candidate_url")})
    strong_venues = len(
        {
            row["venue_index"]
            for row in rows
            if row.get("candidate_url") and int(row.get("confidence") or 0) >= 55
        }
    )
    print()
    print("Summary")
    print("-------")
    print(f"venues: {total_venues:,}")
    print(f"venues with candidates: {found_venues:,}")
    print(f"venues with strong candidates: {strong_venues:,}")
    print(f"candidate rows: {len(rows):,}")
    print(f"elapsed: {elapsed:.1f}s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover likely exhibition-list URLs for venue homepages.")
    parser.add_argument("-i", "--input", default="", help="Input yes/maybe venue CSV. Defaults to ARTMOA_DATA_DIR/input filename.")
    parser.add_argument("-o", "--output", default="", help="Output candidate URL CSV. Defaults to ARTMOA_DATA_DIR/output filename.")
    parser.add_argument("--encoding", default="utf-8-sig", help="Input/output CSV encoding.")
    parser.add_argument("--env-file", default="", help="Optional .env file path for NAVER_CLIENT_ID/SECRET.")
    parser.add_argument("--top-n", type=int, default=5, help="Candidate URLs to keep per venue.")
    parser.add_argument("--probe-pages", type=int, default=5, help="Top candidate pages to fetch and inspect per venue.")
    parser.add_argument("--max-home-links", type=int, default=350, help="Maximum homepage links to inspect.")
    parser.add_argument("--max-sitemap-urls", type=int, default=800, help="Maximum sitemap URLs to inspect per venue.")
    parser.add_argument("--timeout", type=float, default=8.0, help="Per-request timeout in seconds.")
    parser.add_argument("--workers", type=int, default=10, help="Concurrent venues to process.")
    parser.add_argument("--checkpoint-every", type=int, default=25, help="Save output every N processed venues.")
    parser.add_argument("--limit", type=int, default=0, help="Process only first N rows for testing.")
    parser.add_argument("--no-sitemap", action="store_true", help="Skip robots.txt and sitemap.xml discovery.")
    parser.add_argument("--no-common-paths", action="store_true", help="Skip common path probing such as /exhibition.")
    parser.add_argument("--use-naver-search", action="store_true", help="Add Naver site:domain search candidates.")
    parser.add_argument("--naver-client-id", default="", help="Naver Search OpenAPI Client ID.")
    parser.add_argument("--naver-client-secret", default="", help="Naver Search OpenAPI Client Secret.")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
