import time
import re
import io
import os
import random
import urllib.request
import urllib.parse
from pathlib import Path
from dataclasses import dataclass
from typing import List

try:
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover - playwright optional
    sync_playwright = None  # type: ignore


# Lightweight date regexes reused from main pipeline heuristics
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
    official_urls: list[str]
    region_main: str
    region_sub: str
    source_query: str


def _fetch_image_bytes_via_playwright(page, url: str) -> bytes | None:
    try:
        # page.request is available in modern Playwright
        resp = page.request.get(url)
        if resp.status == 200:
            return resp.body()
    except Exception:
        pass
    return None


def _fetch_image_bytes_fallback(url: str, timeout: int = 10) -> bytes | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception:
        return None


def _parse_dates_from_text(text: str) -> tuple[str, str]:
    s = ""
    e = ""
    match = DATE_RANGE_PATTERN.search(text)
    if match:
        s = match.group("s").replace('.', '-').replace('/', '-')
        e = match.group("e").replace('.', '-').replace('/', '-')
        return s[:10], e[:10]
    single = SINGLE_DATE_PATTERN.search(text)
    if single:
        return single.group(1)[:10], ""
    return "", ""


def _sleep_with_jitter(base_seconds: float, jitter_seconds: float = 1.25) -> None:
    delay = max(0.0, float(base_seconds or 0.0))
    jitter = max(0.0, float(jitter_seconds or 0.0))
    time.sleep(delay + (random.random() * jitter if jitter else 0.0))


def _sleep_random_range(min_seconds: float, max_seconds: float) -> None:
    min_delay = max(0.0, float(min_seconds or 0.0))
    max_delay = max(min_delay, float(max_seconds or 0.0))
    time.sleep(random.uniform(min_delay, max_delay))


def _login_to_instagram(page, username: str, password: str, timeout_ms: int) -> bool:
    username = (username or "").strip()
    password = (password or "").strip()
    if not username or not password:
        return False

    try:
        page.goto("https://www.instagram.com/accounts/login/", wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(2000)
        user_input = page.locator('input[name="username"]')
        pass_input = page.locator('input[name="password"]')
        user_input.fill(username)
        pass_input.fill(password)
        page.locator('button[type="submit"]').click()
        page.wait_for_timeout(6000)
        # A successful login usually redirects away from the login page.
        return "/accounts/login" not in page.url
    except Exception:
        return False


def extract_events_from_instagram(institution: Institution, instagram_url: str, args, failures: list) -> List[dict]:
    """
    Playwright 기반으로 공개 Instagram 프로필에서 게시물(최대 N개)을 읽어
    캡션과 이미지(메모리)를 수집하고 간단한 이벤트 후보 리스트를 반환합니다.

    반환되는 dict는 collect_exhibition_events.py의 event dict 스키마와 호환됩니다.
    """
    results: List[dict] = []
    if sync_playwright is None:
        return results

    max_posts = int(getattr(args, "instagram_max_posts", 8) or 8)
    per_post_delay = float(getattr(args, "instagram_post_delay", 4.0) or 4.0)
    page_timeout = int(getattr(args, "instagram_timeout_ms", 10000) or 10000)
    proxy_url = str(getattr(args, "instagram_proxy", "") or "").strip()
    username = str(getattr(args, "instagram_username", os.getenv("INSTAGRAM_USER", "")) or "").strip()
    password = str(getattr(args, "instagram_password", os.getenv("INSTAGRAM_PASS", "")) or "").strip()
    profile_delay = float(getattr(args, "instagram_profile_delay", 8.0) or 8.0)
    random_delay_min = float(getattr(args, "instagram_random_delay_min", 10.0) or 10.0)
    random_delay_max = float(getattr(args, "instagram_random_delay_max", 30.0) or 30.0)

    try:
        with sync_playwright() as p:
            launch_kwargs = {"headless": True}
            if proxy_url:
                launch_kwargs["proxy"] = {"server": proxy_url}

            browser = p.chromium.launch(**launch_kwargs)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            page = context.new_page()

            if username and password:
                _login_to_instagram(page, username, password, page_timeout)

            _sleep_with_jitter(profile_delay, jitter_seconds=2.0)
            _sleep_random_range(random_delay_min, random_delay_max)

            page.goto(instagram_url, wait_until="domcontentloaded", timeout=page_timeout)
            _sleep_random_range(random_delay_min, random_delay_max)

            # If the input is already a post or reel URL, process only that URL.
            path = urllib.parse.urlparse(instagram_url).path.lower()
            links = []
            is_direct_post = "/p/" in path or "/reel/" in path
            if is_direct_post:
                links.append(instagram_url)
            else:
                # gentle wait to allow the profile grid to appear, then collect a limited set of posts
                try:
                    page.wait_for_selector('a[href*="/p/"]', timeout=min(page_timeout, 5000))
                except Exception:
                    pass

                anchors = page.query_selector_all('a[href*="/p/"]')
                for a in anchors:
                    try:
                        href = a.get_attribute("href") or ""
                    except Exception:
                        href = ""
                    if href and href.startswith("/"):
                        full = urllib.parse.urljoin("https://www.instagram.com", href)
                        if full not in links:
                            links.append(full)
                    if len(links) >= max_posts:
                        break

            for post_url in links[:max_posts]:
                try:
                    page.goto(post_url, wait_until="domcontentloaded", timeout=page_timeout)
                    # caption extraction: article element tends to contain caption text
                    caption = ""
                    try:
                        article = page.query_selector("article")
                        if article:
                            caption = article.inner_text() or ""
                    except Exception:
                        caption = page.content()[:1000]

                    # image candidate
                    img_url = ""
                    try:
                        img = page.query_selector("article img")
                        if img:
                            img_url = img.get_attribute("src") or ""
                    except Exception:
                        img_url = ""

                    ocr_text = ""
                    if img_url:
                        img_bytes = _fetch_image_bytes_via_playwright(page, img_url)
                        if not img_bytes:
                            img_bytes = _fetch_image_bytes_fallback(img_url)
                        if img_bytes:
                            try:
                                from PIL import Image

                                img = Image.open(io.BytesIO(img_bytes))
                                # import OCR function dynamically to avoid circular imports
                                try:
                                    from collect_exhibition_events import run_tesseract_ocr_variants
                                except Exception:
                                    try:
                                        from src.collect_exhibition_events import run_tesseract_ocr_variants
                                    except Exception:
                                        run_tesseract_ocr_variants = None

                                if run_tesseract_ocr_variants:
                                    ocr_text = run_tesseract_ocr_variants(img)
                            except Exception:
                                ocr_text = ""

                    start_date, end_date = _parse_dates_from_text(caption + "\n" + ocr_text)

                    confidence = 0.40
                    if start_date:
                        confidence += 0.25
                    if "전시" in (caption or "") or "전시" in (ocr_text or ""):
                        confidence += 0.20

                    results.append(
                        {
                            "event_name": (caption.splitlines()[0] if caption else post_url)[:120],
                            "description": (caption or ocr_text)[:300],
                            "start_date": start_date,
                            "end_date": end_date,
                            "price_type": "unknown",
                            "price_text": "",
                            "evidence": f"instagram:{post_url}",
                            "confidence": f"{min(confidence,0.95):.2f}",
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    failures.append(
                        {
                            "institution_id": institution.institution_id,
                            "institution_title": institution.title,
                            "domain": "instagram.com",
                            "url": post_url,
                            "stage": "instagram-post",
                            "error_type": type(exc).__name__,
                            "error_message": str(exc),
                        }
                    )
                _sleep_with_jitter(per_post_delay, jitter_seconds=1.5)
                _sleep_random_range(random_delay_min, random_delay_max)

            context.close()
            browser.close()
    except Exception as exc:  # pragma: no cover - runtime guard
        failures.append(
            {
                "institution_id": institution.institution_id,
                "institution_title": institution.title,
                "domain": "instagram.com",
                "url": instagram_url,
                "stage": "instagram-profile",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            }
        )

    return results
