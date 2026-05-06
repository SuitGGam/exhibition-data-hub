#!/usr/bin/env python3
"""
Classify Naver local exhibition-place rows into art-related venue candidates.

The script keeps the original CSV untouched and writes a new CSV with extra
columns for URL verification and art-venue classification.

Optional URL discovery/checking uses Naver Search OpenAPI when credentials are
passed by command option, loaded from .env, or set in the environment.
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
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen


DEFAULT_INPUT_NAME = "naver_local_exhibitions_mod.csv"
DEFAULT_OUTPUT_NAME = "naver_local_exhibitions_art_verified.csv"

# Optional Naver Search OpenAPI credentials.
# You can paste keys here if you do not want to set PowerShell environment variables.
# Leaving these blank is fine; --naver-client-id/--naver-client-secret or
# NAVER_CLIENT_ID/NAVER_CLIENT_SECRET will still work.
NAVER_CLIENT_ID = ""
NAVER_CLIENT_SECRET = ""

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

ART_CATEGORY_EXACT = {
    "문화,예술>미술관": ("yes", 95, "미술관 카테고리"),
    "문화,예술>갤러리,화랑": ("yes", 93, "갤러리/화랑 카테고리"),
}

ART_CATEGORY_MAYBE = {
    "문화,예술>전시관": 76,
    "문화,예술>복합문화공간": 67,
    "문화,예술>박람회,전시회": 62,
    "여행,명소>박물관": 60,
    "문화,예술>문화원": 55,
    "갤러리카페": 55,
}

EXCLUDE_CATEGORY_PATTERNS = [
    r"광고,마케팅>옥외,전시광고",
    r"지원,대행>전시,행사대행",
    r"지원,대행>이벤트,파티",
    r"자동차>전시,판매",
    r"쇼핑,유통>자동차",
    r"학원.*미술교육",
    r"교육,학문>문화센터",
    r"교육원,교육센터>문화센터",
    r"음식점>",
    r"미용>",
    r"생활,편의>꽃집",
    r"도로시설>",
    r"건설업>",
    r"장소대여>",
    r"쇼핑,유통>휴대폰",
    r"쇼핑,유통>열쇠",
    r"제조업>",
]

STRONG_ART_TITLE_PATTERNS = [
    r"미술관",
    r"아트센터",
    r"아트뮤지엄",
    r"아트스페이스",
    r"갤러리",
    r"화랑",
    r"사진미술관",
    r"사진전시",
    r"전시공간",
    r"전시장",
    r"\bart\b",
    r"\bgallery\b",
    r"\bmuseum\b",
]

WEAK_ART_TITLE_PATTERNS = [
    r"문화공간",
    r"문화예술",
    r"복합문화",
    r"박물관",
    r"전시",
    r"사진",
    r"현대미술",
    r"공예",
]

NEGATIVE_TITLE_PATTERNS = [
    r"주차장",
    r"미술학원",
    r"미술교육",
    r"자동차",
    r"분양전시관",
    r"모델하우스",
    r"홍보관",
    r"전시광고",
    r"행사대행",
    r"렌탈",
    r"카페",
    r"식당",
    r"다이닝",
    r"공방",
    r"꽃집",
    r"인테리어",
]

PAGE_ART_KEYWORDS = [
    "미술관",
    "갤러리",
    "화랑",
    "전시",
    "작가",
    "회화",
    "사진전",
    "현대미술",
    "아트센터",
    "art museum",
    "gallery",
    "exhibition",
    "artist",
    "photography",
]

SOCIAL_HOSTS = {
    "instagram.com": "social",
    "www.instagram.com": "social",
    "facebook.com": "social",
    "www.facebook.com": "social",
    "blog.naver.com": "blog",
    "m.blog.naver.com": "blog",
    "post.naver.com": "blog",
    "smartstore.naver.com": "store",
    "booking.naver.com": "booking",
    "map.naver.com": "naver_place",
    "place.map.kakao.com": "kakao_place",
}

SHORTLINK_HOSTS = {
    "litt.ly",
    "bit.ly",
    "linktr.ee",
    "url.kr",
    "naver.me",
    "me2.kr",
}


class TitleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_title = False
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "title":
            self.in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self.in_title = False

    def handle_data(self, data: str) -> None:
        if self.in_title:
            self.parts.append(data.strip())

    @property
    def title(self) -> str:
        return normalize_space(" ".join(part for part in self.parts if part))


@dataclass(frozen=True)
class UrlCheck:
    input_url: str
    checked_url: str
    final_url: str
    status: str
    http_status: str
    url_type: str
    page_title: str
    page_art_keyword_hits: str
    error: str


@dataclass(frozen=True)
class NaverSearchResult:
    found_url: str
    status: str
    url_type: str
    score: int
    reason: str
    query: str


def normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def strip_html_tags(value: str) -> str:
    value = re.sub(r"<[^>]*>", " ", value or "")
    return html.unescape(normalize_space(value))


def has_any(patterns: Iterable[str], text: str) -> list[str]:
    found = []
    for pattern in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            found.append(pattern)
    return found


def get_row_text(row: dict[str, str]) -> str:
    fields = [
        row.get("title", ""),
        row.get("category", ""),
        row.get("summary", ""),
        row.get("source_query", ""),
        row.get("road_address", ""),
        row.get("full_address", ""),
    ]
    return normalize_space(" ".join(fields))


def classify_url_type(url: str) -> str:
    if not url:
        return ""
    host = (urlparse(url).netloc or "").lower()
    if host.startswith("www."):
        bare_host = host[4:]
    else:
        bare_host = host
    if host in SOCIAL_HOSTS:
        return SOCIAL_HOSTS[host]
    if bare_host in SOCIAL_HOSTS:
        return SOCIAL_HOSTS[bare_host]
    if bare_host in SHORTLINK_HOSTS:
        return "shortlink"
    if "naver.com" in bare_host and "blog" in bare_host:
        return "blog"
    if "instagram.com" in bare_host:
        return "social"
    return "official_or_unknown"


def normalize_url(url: str) -> str:
    url = normalize_space(url)
    if not url:
        return ""
    if not re.match(r"^https?://", url, flags=re.IGNORECASE):
        url = "https://" + url
    return url


def classify_art_venue(row: dict[str, str]) -> dict[str, str]:
    title = normalize_space(row.get("title"))
    category = normalize_space(row.get("category"))
    row_text = get_row_text(row)

    reasons: list[str] = []
    score = 0

    if category in ART_CATEGORY_EXACT:
        label, score, reason = ART_CATEGORY_EXACT[category]
        reasons.append(reason)
        return {
            "is_art_venue": label,
            "art_confidence": str(score),
            "venue_type": guess_venue_type(title, category),
            "classification_reason": "; ".join(reasons),
            "needs_manual_review": "no",
        }

    excluded = has_any(EXCLUDE_CATEGORY_PATTERNS, category)
    negative_title = has_any(NEGATIVE_TITLE_PATTERNS, title)
    strong_title = has_any(STRONG_ART_TITLE_PATTERNS, title)
    weak_title = has_any(WEAK_ART_TITLE_PATTERNS, row_text)

    if category in ART_CATEGORY_MAYBE:
        score += ART_CATEGORY_MAYBE[category]
        reasons.append(f"관련 가능 카테고리: {category}")

    if strong_title:
        score += 28
        reasons.append("강한 예술 키워드: " + ", ".join(strong_title[:4]))

    if weak_title:
        score += 12
        reasons.append("약한 예술 키워드: " + ", ".join(weak_title[:4]))

    if excluded:
        score -= 70
        reasons.append("제외 카테고리: " + ", ".join(excluded[:3]))

    if negative_title:
        score -= 35
        reasons.append("제외성 제목 키워드: " + ", ".join(negative_title[:4]))

    if not reasons:
        reasons.append("예술 전시 관련 근거 부족")

    score = max(0, min(99, score))

    if score >= 80:
        label = "yes"
    elif score >= 45:
        label = "maybe"
    else:
        label = "no"

    needs_review = "yes" if label == "maybe" else "no"
    if label == "yes" and (excluded or negative_title):
        needs_review = "yes"

    return {
        "is_art_venue": label,
        "art_confidence": str(score),
        "venue_type": guess_venue_type(title, category),
        "classification_reason": "; ".join(reasons),
        "needs_manual_review": needs_review,
    }


def guess_venue_type(title: str, category: str) -> str:
    text = f"{title} {category}".lower()
    checks = [
        ("art_museum", [r"미술관", r"art museum"]),
        ("gallery", [r"갤러리", r"화랑", r"gallery"]),
        ("photo_space", [r"사진", r"photography"]),
        ("art_center", [r"아트센터", r"art center"]),
        ("exhibition_hall", [r"전시관", r"전시장", r"exhibition"]),
        ("museum", [r"박물관", r"museum"]),
        ("culture_space", [r"복합문화", r"문화공간", r"문화원"]),
    ]
    for label, patterns in checks:
        if has_any(patterns, text):
            return label
    return "unknown"


def read_csv_rows(path: str, encoding: str) -> tuple[list[dict[str, str]], list[str]]:
    with open(path, "r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [{key: value or "" for key, value in row.items()} for row in reader]
        fieldnames = list(reader.fieldnames or [])
    return rows, fieldnames


def write_csv_rows(path: str, rows: list[dict[str, str]], fieldnames: list[str], encoding: str) -> None:
    extra_fields = [
        "input_url",
        "verified_url",
        "url_status",
        "url_http_status",
        "url_type",
        "url_page_title",
        "url_art_keyword_hits",
        "url_error",
        "api_found_url",
        "api_search_status",
        "api_url_type",
        "api_match_score",
        "api_search_reason",
        "api_search_query",
        "selected_url_source",
        "is_art_venue",
        "art_confidence",
        "venue_type",
        "classification_reason",
        "needs_manual_review",
    ]
    final_fields = fieldnames[:]
    for field in extra_fields:
        if field not in final_fields:
            final_fields.append(field)

    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding=encoding, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=final_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, path)


def request_url(url: str, timeout: float) -> tuple[str, str, bytes, str]:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
        },
        method="GET",
    )
    context = ssl.create_default_context()
    with urlopen(request, timeout=timeout, context=context) as response:
        body = response.read(160_000)
        final_url = response.geturl()
        status = str(getattr(response, "status", ""))
        content_type = response.headers.get("Content-Type", "")
    return final_url, status, body, content_type


def extract_page_title(body: bytes, content_type: str) -> str:
    encoding = "utf-8"
    match = re.search(r"charset=([\w.-]+)", content_type or "", flags=re.IGNORECASE)
    if match:
        encoding = match.group(1)
    try:
        text = body.decode(encoding, errors="replace")
    except LookupError:
        text = body.decode("utf-8", errors="replace")
    parser = TitleParser()
    parser.feed(text)
    return parser.title


def find_keyword_hits(*values: str) -> str:
    text = " ".join(values).lower()
    hits = []
    for keyword in PAGE_ART_KEYWORDS:
        if keyword.lower() in text:
            hits.append(keyword)
    return "|".join(dict.fromkeys(hits))


def check_one_url(url: str, timeout: float) -> UrlCheck:
    original_url = normalize_space(url)
    checked_url = normalize_url(original_url)
    if not checked_url:
        return UrlCheck(original_url, "", "", "blank", "", "", "", "", "")

    try:
        final_url, http_status, body, content_type = request_url(checked_url, timeout)
        page_title = extract_page_title(body, content_type)
        keyword_hits = find_keyword_hits(page_title, final_url)
        return UrlCheck(
            input_url=original_url,
            checked_url=checked_url,
            final_url=final_url,
            status="ok",
            http_status=http_status,
            url_type=classify_url_type(final_url),
            page_title=page_title,
            page_art_keyword_hits=keyword_hits,
            error="",
        )
    except HTTPError as exc:
        return UrlCheck(
            input_url=original_url,
            checked_url=checked_url,
            final_url=getattr(exc, "url", checked_url),
            status="http_error",
            http_status=str(exc.code),
            url_type=classify_url_type(checked_url),
            page_title="",
            page_art_keyword_hits="",
            error=str(exc.reason),
        )
    except (URLError, TimeoutError, socket.timeout, ssl.SSLError) as exc:
        return UrlCheck(
            input_url=original_url,
            checked_url=checked_url,
            final_url="",
            status="error",
            http_status="",
            url_type=classify_url_type(checked_url),
            page_title="",
            page_art_keyword_hits="",
            error=str(exc)[:240],
        )
    except Exception as exc:  # noqa: BLE001 - this is a batch cleaner, keep rows flowing.
        return UrlCheck(
            input_url=original_url,
            checked_url=checked_url,
            final_url="",
            status="error",
            http_status="",
            url_type=classify_url_type(checked_url),
            page_title="",
            page_art_keyword_hits="",
            error=f"{type(exc).__name__}: {exc}"[:240],
        )


def choose_existing_url(row: dict[str, str]) -> str:
    for field in ("official_url", "link"):
        url = normalize_space(row.get(field))
        if url:
            return url
    return ""


def apply_url_check(row: dict[str, str], check: UrlCheck) -> None:
    row["input_url"] = check.input_url
    row["verified_url"] = check.final_url or check.checked_url
    row["url_status"] = check.status
    row["url_http_status"] = check.http_status
    row["url_type"] = check.url_type
    row["url_page_title"] = check.page_title
    row["url_art_keyword_hits"] = check.page_art_keyword_hits
    row["url_error"] = check.error


def apply_naver_search_result(row: dict[str, str], result: NaverSearchResult) -> None:
    row["api_found_url"] = result.found_url
    row["api_search_status"] = result.status
    row["api_url_type"] = result.url_type
    row["api_match_score"] = str(result.score) if result.score else ""
    row["api_search_reason"] = result.reason
    row["api_search_query"] = result.query


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


def get_naver_credentials(client_id: str = "", client_secret: str = "") -> tuple[str, str]:
    resolved_id = normalize_space(client_id) or normalize_space(NAVER_CLIENT_ID) or normalize_space(os.getenv("NAVER_CLIENT_ID"))
    resolved_secret = (
        normalize_space(client_secret)
        or normalize_space(NAVER_CLIENT_SECRET)
        or normalize_space(os.getenv("NAVER_CLIENT_SECRET"))
    )
    return resolved_id, resolved_secret


def comparable_url(url: str) -> str:
    parsed = urlparse(normalize_url(url))
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = (parsed.path or "").rstrip("/")
    return f"{host}{path}"


def select_best_url(existing_url: str, api_result: NaverSearchResult) -> tuple[str, str]:
    api_url = api_result.found_url
    if not existing_url and api_url:
        return api_url, "naver_api"
    if existing_url and not api_url:
        return existing_url, "existing"
    if not existing_url and not api_url:
        return "", ""

    if comparable_url(existing_url) == comparable_url(api_url):
        return existing_url, "existing_api_same"

    existing_type = classify_url_type(existing_url)
    api_type = classify_url_type(api_url)
    low_trust_types = {
        "social",
        "blog",
        "store",
        "shortlink",
        "booking",
        "naver_place",
        "kakao_place",
    }

    if existing_type in low_trust_types and api_type == "official_or_unknown":
        return api_url, "naver_api_preferred"
    if existing_type in {"store", "shortlink", "booking"} and api_type not in {"store", "shortlink", "booking"}:
        return api_url, "naver_api_preferred"
    return existing_url, "existing"


def discover_url_with_naver(
    row: dict[str, str],
    timeout: float,
    client_id: str = "",
    client_secret: str = "",
) -> NaverSearchResult:
    client_id, client_secret = get_naver_credentials(client_id, client_secret)
    title = normalize_space(row.get("title"))
    address = normalize_space(row.get("road_address") or row.get("full_address"))
    query = normalize_space(f"{title} {address} 공식")

    if not client_id or not client_secret:
        return NaverSearchResult("", "missing_credentials", "", 0, "Naver API credentials are not set.", query)

    api_url = (
        "https://openapi.naver.com/v1/search/webkr.json"
        f"?query={quote(query)}&display=5&start=1&sort=sim"
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
    except HTTPError as exc:
        return NaverSearchResult("", f"http_error_{exc.code}", "", 0, str(exc.reason), query)
    except Exception as exc:
        return NaverSearchResult("", "error", "", 0, f"{type(exc).__name__}: {exc}"[:240], query)

    title_norm = re.sub(r"\s+", "", title.lower())
    region_sub = normalize_space(row.get("region_sub"))
    best_link = ""
    best_score = -1
    best_reason = ""
    for item in payload.get("items", []):
        item_title = strip_html_tags(item.get("title", ""))
        item_desc = strip_html_tags(item.get("description", ""))
        item_link = html.unescape(item.get("link", ""))
        if not item_link:
            continue
        haystack = re.sub(r"\s+", "", f"{item_title} {item_desc}".lower())
        score = 0
        reasons = []
        if title_norm and title_norm in haystack:
            score += 50
            reasons.append("title_match")
        if region_sub and region_sub in f"{item_title} {item_desc}":
            score += 10
            reasons.append("region_match")
        if find_keyword_hits(item_title, item_desc):
            score += 20
            reasons.append("art_keyword")
        url_type = classify_url_type(item_link)
        if url_type == "official_or_unknown":
            score += 15
            reasons.append("official_like")
        elif url_type in {"blog", "social"}:
            score += 8
            reasons.append(url_type)
        elif url_type == "store":
            score -= 10
            reasons.append("store_penalty")
        if score > best_score:
            best_score = score
            best_link = item_link
            best_reason = "|".join(reasons)

    if not payload.get("items"):
        return NaverSearchResult("", "no_results", "", 0, "No Naver search results.", query)
    if best_link and best_score >= 20:
        return NaverSearchResult(
            best_link,
            "found",
            classify_url_type(best_link),
            best_score,
            best_reason or "best_search_result",
            query,
        )
    return NaverSearchResult("", "no_confident_match", "", max(0, best_score), best_reason, query)


def add_classification(rows: list[dict[str, str]]) -> None:
    for row in rows:
        row.update(classify_art_venue(row))


def add_url_data(
    rows: list[dict[str, str]],
    *,
    verify_urls: bool,
    find_missing_urls: bool,
    search_naver_urls: bool,
    naver_client_id: str,
    naver_client_secret: str,
    only_candidates: bool,
    timeout: float,
    workers: int,
    output_path: str,
    fieldnames: list[str],
    encoding: str,
    checkpoint_every: int,
) -> None:
    target_rows = []
    total_rows = len(rows)
    processed_rows = 0
    checkpoint_every = max(0, checkpoint_every)

    def save_checkpoint(label: str, current: int, total: int) -> None:
        if not output_path:
            return
        write_csv_rows(output_path, rows, fieldnames, encoding)
        print(f"{label}: saved {current}/{total} -> {output_path}", file=sys.stderr, flush=True)

    for row in rows:
        if only_candidates and row.get("is_art_venue") == "no":
            apply_naver_search_result(row, NaverSearchResult("", "skipped", "", 0, "not an art candidate", ""))
            row["selected_url_source"] = ""
            apply_url_check(row, UrlCheck("", "", "", "skipped", "", "", "", "", "not an art candidate"))
            processed_rows += 1
            if checkpoint_every and processed_rows % checkpoint_every == 0:
                save_checkpoint("Prepared rows", processed_rows, total_rows)
            continue

        existing_url = choose_existing_url(row)
        should_search_naver = search_naver_urls or (find_missing_urls and not existing_url)
        if should_search_naver:
            api_result = discover_url_with_naver(row, timeout, naver_client_id, naver_client_secret)
        else:
            api_result = NaverSearchResult("", "not_requested", "", 0, "", "")
        apply_naver_search_result(row, api_result)

        url, selected_source = select_best_url(existing_url, api_result)
        row["selected_url_source"] = selected_source

        if not url:
            apply_url_check(row, UrlCheck("", "", "", "blank", "", "", "", "", ""))
            processed_rows += 1
            if checkpoint_every and processed_rows % checkpoint_every == 0:
                save_checkpoint("Prepared rows", processed_rows, total_rows)
            continue

        row["input_url"] = url
        if verify_urls:
            target_rows.append(row)
        else:
            apply_url_check(
                row,
                UrlCheck(
                    input_url=url,
                    checked_url=normalize_url(url),
                    final_url=normalize_url(url),
                    status="not_checked",
                    http_status="",
                    url_type=classify_url_type(url),
                    page_title="",
                    page_art_keyword_hits="",
                    error="",
                ),
            )
        processed_rows += 1
        if checkpoint_every and processed_rows % checkpoint_every == 0:
            save_checkpoint("Prepared rows", processed_rows, total_rows)

    save_checkpoint("Prepared rows", processed_rows, total_rows)

    if not verify_urls or not target_rows:
        return

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(check_one_url, row["input_url"], timeout): index
            for index, row in enumerate(target_rows)
        }
        done = 0
        total = len(futures)
        for future in as_completed(futures):
            row = target_rows[futures[future]]
            apply_url_check(row, future.result())
            done += 1
            if done % 250 == 0 or done == total:
                print(f"URL checked: {done}/{total}", file=sys.stderr, flush=True)
            if checkpoint_every and (done % checkpoint_every == 0 or done == total):
                save_checkpoint("URL checked", done, total)


def print_summary(rows: list[dict[str, str]], elapsed: float) -> None:
    def count(field: str, value: str) -> int:
        return sum(1 for row in rows if row.get(field) == value)

    print()
    print("Summary")
    print("-------")
    print(f"rows: {len(rows):,}")
    print(f"art yes: {count('is_art_venue', 'yes'):,}")
    print(f"art maybe: {count('is_art_venue', 'maybe'):,}")
    print(f"art no: {count('is_art_venue', 'no'):,}")
    print(f"manual review: {count('needs_manual_review', 'yes'):,}")
    print(f"url ok: {count('url_status', 'ok'):,}")
    print(f"url blank: {count('url_status', 'blank'):,}")
    print(f"url skipped: {count('url_status', 'skipped'):,}")
    print(f"naver api found: {sum(1 for row in rows if row.get('api_found_url')):,}")
    print(f"elapsed: {elapsed:.1f}s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter and verify art-related exhibition venues from a Naver local CSV."
    )
    parser.add_argument("-i", "--input", default="", help="Input CSV path. Defaults to ARTMOA_DATA_DIR/input filename.")
    parser.add_argument("-o", "--output", default="", help="Output CSV path. Defaults to ARTMOA_DATA_DIR/output filename.")
    parser.add_argument("--encoding", default="utf-8-sig", help="Input/output CSV encoding.")
    parser.add_argument("--env-file", default="", help="Optional .env file path for NAVER_CLIENT_ID/SECRET.")
    parser.add_argument(
        "--verify-urls",
        action="store_true",
        help="Actually connect to URLs and record status/final URL/page title.",
    )
    parser.add_argument(
        "--find-missing-urls",
        action="store_true",
        help="Use Naver Search OpenAPI only when an existing URL is missing.",
    )
    parser.add_argument(
        "--search-naver-urls",
        action="store_true",
        help="Use Naver Search OpenAPI for every candidate row and compare it with the existing URL.",
    )
    parser.add_argument(
        "--naver-client-id",
        default="",
        help="Naver Search OpenAPI Client ID. If omitted, the script uses NAVER_CLIENT_ID or the constant near the top.",
    )
    parser.add_argument(
        "--naver-client-secret",
        default="",
        help="Naver Search OpenAPI Client Secret. If omitted, the script uses NAVER_CLIENT_SECRET or the constant near the top.",
    )
    parser.add_argument(
        "--check-all-rows",
        action="store_true",
        help="Verify/discover URLs for every row. By default, rows classified as no are skipped.",
    )
    parser.add_argument("--timeout", type=float, default=8.0, help="Per-request timeout in seconds.")
    parser.add_argument("--workers", type=int, default=16, help="Concurrent URL checks.")
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=100,
        help="Save partial results every N processed rows/checks. Use 0 to disable.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only the first N rows for testing. 0 means all rows.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env_file(args.env_file)
    args.input = args.input or default_data_path(DEFAULT_INPUT_NAME)
    args.output = args.output or default_data_path(DEFAULT_OUTPUT_NAME)
    started = time.time()

    rows, fieldnames = read_csv_rows(args.input, args.encoding)
    if args.limit:
        rows = rows[: args.limit]

    add_classification(rows)
    add_url_data(
        rows,
        verify_urls=args.verify_urls,
        find_missing_urls=args.find_missing_urls,
        search_naver_urls=args.search_naver_urls,
        naver_client_id=args.naver_client_id,
        naver_client_secret=args.naver_client_secret,
        only_candidates=not args.check_all_rows,
        timeout=args.timeout,
        workers=args.workers,
        output_path=args.output,
        fieldnames=fieldnames,
        encoding=args.encoding,
        checkpoint_every=args.checkpoint_every,
    )
    write_csv_rows(args.output, rows, fieldnames, args.encoding)
    print_summary(rows, time.time() - started)
    print(f"\nSaved: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
