# -*- coding: utf-8 -*-
"""
Weibo dynamic (微博动态) monitor via HTTP polling.

For each monitored UID:
  1. Periodically fetch latest Weibo posts.
  2. Detect new mblog_id not seen before.
  3. On new post -> trigger callback.

Dual-API strategy (ref: napcat-plugin-weibo-push):
  Primary:   m.weibo.cn/api/container/getIndex  (mobile REST, cookie optional)
  Fallback:  weibo.com/ajax/statuses/mymblog    (desktop ajax, cookie required)
"""

import asyncio
import json
import logging
import os
import random
import re
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Protocol

import aiohttp

from .base import Monitor

log = logging.getLogger(__name__)

DEFAULT_WEIBO_POLL_INTERVAL = 60
CACHE_MAXLEN = 100

# ---------------------------------------------------------------------------
# Built-in Chrome UA pool (no network dependency)
# ---------------------------------------------------------------------------
_CHROME_UA_POOL = [
    # Chrome 120-126 on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    # Chrome 120-126 on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    # Chrome 120-126 on Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
]


def _get_random_ua() -> str:
    """Pick a random Chrome user-agent string from the built-in pool."""
    return random.choice(_CHROME_UA_POOL)


def _resolve_proxy(configured_proxy: str) -> str | None:
    """Resolve the effective proxy URL.

    Priority:
      1. Explicitly configured proxy (from weibo.proxy config).
      2. ``https_proxy`` / ``http_proxy`` environment variable.
      3. ``all_proxy`` environment variable.
      4. ``None`` (direct connection).
    """
    if configured_proxy:
        return configured_proxy
    for var in ("https_proxy", "http_proxy", "all_proxy", "HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY"):
        val = os.environ.get(var, "").strip()
        if val:
            return val
    return None


def _extract_xsrf_token(cookie: str) -> str:
    """Extract XSRF-TOKEN from a Weibo cookie string."""
    m = re.search(r"(?:^|;\s*)XSRF-TOKEN=([^;]+)", cookie)
    return m.group(1) if m else ""


def _make_mobile_headers(uid: str, cookie: str = "") -> dict[str, str]:
    """Build browser-like headers for m.weibo.cn API requests (Mobile UA)."""
    ua_str = _get_random_ua()
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "mweibo-pwa": "1",
        "referer": f"https://m.weibo.cn/u/{uid}",
        "sec-ch-ua": '"Chromium";v="125", "Not.A/Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": ua_str,
        "User-Agent": ua_str,
        "x-requested-with": "XMLHttpRequest",
    }
    if cookie:
        headers["cookie"] = cookie
    return headers


def _make_desktop_headers(uid: str, cookie: str) -> dict[str, str]:
    """Build browser-like headers for weibo.com/ajax API (Desktop UA, needs cookie)."""
    ua_str = _get_random_ua()
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "referer": f"https://weibo.com/u/{uid}",
        "sec-ch-ua": '"Chromium";v="125", "Not.A/Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": ua_str,
        "User-Agent": ua_str,
        "x-requested-with": "XMLHttpRequest",
    }
    if cookie:
        headers["cookie"] = cookie
    # XSRF token for ajax POST-like endpoints
    xsrf = _extract_xsrf_token(cookie)
    if xsrf:
        headers["x-xsrf-token"] = xsrf
    return headers


def _strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", text)


class WeiboNewDynamicCallback(Protocol):
    """Protocol for the on_new_dynamic async callback (same as Bilibili's)."""

    async def __call__(
        self,
        uname: str,
        dynamic_id: str,
        content: str,
        pic_url: str | None = None,
        pics_url: list[str] | None = None,
        dynamic_type: str = "",
        dynamic_time: str = "",
        dynamic_url: str = "",
        avatar_url: str | None = None,
    ) -> None: ...


class WeiboDynamicPollMonitor(Monitor):
    """Monitor a single Weibo UID for new posts via polling.

    Uses a dual-API strategy:
      1. Primary: m.weibo.cn mobile API (works without cookie)
      2. Fallback: weibo.com/ajax API (requires cookie)

    Args:
        uid: Weibo user ID to monitor.
        on_new_dynamic: Async callback.
        poll_interval: Seconds between polls (default 60).
        cookie: Optional Weibo cookie for fallback API / "followers only" posts.
        proxy: Optional HTTP proxy URL.
        http_session: Optional shared aiohttp session.
    """

    name: str = "weibo-dynamic-poll"

    def __init__(
        self,
        uid: int,
        *,
        on_new_dynamic: WeiboNewDynamicCallback,
        poll_interval: int = DEFAULT_WEIBO_POLL_INTERVAL,
        cookie: str = "",
        proxy: str = "",
        http_session: aiohttp.ClientSession | None = None,
    ) -> None:
        super().__init__()
        self.uid = uid
        self._on_new_dynamic = on_new_dynamic
        self._poll_interval = poll_interval
        self._cookie = cookie
        self._proxy = proxy

        # Defer session creation to avoid "no running event loop" error
        self._http: aiohttp.ClientSession | None = http_session
        self._owns_session = http_session is None

        # Deque of known mblog IDs (max 100)
        self._seen_ids: deque[str] = deque(maxlen=CACHE_MAXLEN)

        # Cached uname
        self._uname: str = ""

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._http is None:
            self._http = aiohttp.ClientSession()
            self._owns_session = True
        return self._http

    async def _request_json(
        self, url: str, headers: dict[str, str]
    ) -> dict | None:
        """Make an HTTP GET and return parsed JSON, or None on failure."""
        http = await self._get_session()
        try:
            effective_proxy = _resolve_proxy(self._proxy)
            async with http.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
                proxy=effective_proxy,
            ) as resp:
                if resp.status != 200:
                    body_sample = await resp.text()
                    log.warning(
                        "[Weibo UID:%d] %s HTTP %d, body=%.200s",
                        self.uid, url, resp.status, body_sample,
                    )
                    return None
                body = await resp.text()
                try:
                    return json.loads(body)
                except json.JSONDecodeError:
                    log.warning(
                        "[Weibo UID:%d] %s response not JSON: %.200s",
                        self.uid, url, body,
                    )
                    return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("[Weibo UID:%d] %s request error: %s", self.uid, url, e)
            return None

    # ------------------------------------------------------------------
    # Data fetching — primary: m.weibo.cn mobile API
    # ------------------------------------------------------------------

    async def _fetch_mobile_cards(self) -> list[dict] | None:
        """Fetch posts via m.weibo.cn API.

        Returns filtered list of card dicts (each has ``mblog`` key), or None.
        """
        uid_str = str(self.uid)
        url = (
            f"https://m.weibo.cn/api/container/getIndex"
            f"?containerid=107603{uid_str}&page_type=03&page=1"
        )
        headers = _make_mobile_headers(uid_str, self._cookie)

        data = await self._request_json(url, headers)
        if data is None:
            return None

        ok = data.get("ok", 0)
        if ok != 1:
            log.debug(
                "[Weibo UID:%d] mobile API ok=%s, msg=%s",
                self.uid, ok, data.get("msg", ""),
            )
            return None

        cards: list[dict] = data.get("data", {}).get("cards", [])
        if not cards:
            log.debug(
                "[Weibo UID:%d] mobile API returned 0 cards (data keys: %s)",
                self.uid, list(data.get("data", {}).keys()),
            )
            return None

        # Filter out cards without mblog (ad etc.) and pinned posts
        filtered = [
            card
            for card in cards
            if card.get("mblog") is not None
            and card["mblog"].get("isTop", 0) != 1
            and card["mblog"].get("mblogtype", 0) != 2
        ]

        if not filtered:
            log.debug(
                "[Weibo UID:%d] mobile cards=%d but all filtered out "
                "(first card keys: %s)",
                self.uid, len(cards),
                list(cards[0].keys()) if cards else "N/A",
            )

        return filtered

    # ------------------------------------------------------------------
    # Data fetching — fallback: weibo.com/ajax API (requires cookie)
    # ------------------------------------------------------------------

    async def _fetch_ajax_posts(self) -> list[dict] | None:
        """Fetch posts via weibo.com/ajax/statuses/mymblog.

        Returns list of mblog dicts directly (no card wrapper), or None.
        Requires ``self._cookie`` to be set.
        """
        if not self._cookie:
            log.debug("[Weibo UID:%d] no cookie, skip ajax fallback", self.uid)
            return None

        uid_str = str(self.uid)
        url = (
            f"https://weibo.com/ajax/statuses/mymblog"
            f"?uid={uid_str}&page=1&feature=0"
        )
        headers = _make_desktop_headers(uid_str, self._cookie)

        data = await self._request_json(url, headers)
        if data is None:
            return None

        result = data.get("data", {})
        posts: list[dict] = result.get("list", [])
        if not posts:
            log.warning(
                "[Weibo UID:%d] ajax API returned 0 posts (data keys: %s)",
                self.uid, list(result.keys()),
            )
            return None

        # Filter pinned
        filtered = [p for p in posts if p.get("isTop", 0) != 1]
        if not filtered:
            log.warning(
                "[Weibo UID:%d] ajax posts=%d but all pinned",
                self.uid, len(posts),
            )
        return filtered

    # ------------------------------------------------------------------
    # Unified fetch — primary → fallback
    # ------------------------------------------------------------------

    async def _fetch_posts(self) -> list[dict] | None:
        """Fetch Weibo posts, trying primary then fallback.

        Returns a list of **card-like dicts** (each with an ``mblog`` key)
        for unified processing in ``_process_cards``.
        Returns None if both APIs fail.
        """
        # 1) Try primary mobile API
        cards = await self._fetch_mobile_cards()
        if cards is not None:
            return cards

        # 2) Try ajax fallback (wrap mblog dicts in card-like containers)
        if self._cookie:
            log.info("[Weibo UID:%d] mobile API failed, trying ajax fallback...", self.uid)
            posts = await self._fetch_ajax_posts()
            if posts is not None:
                # Wrap each mblog dict in a card-like shape so _process_cards works
                wrapped = [{"mblog": p, "card_type": 9} for p in posts]
                log.debug(
                    "[Weibo UID:%d] ajax fallback returned %d posts",
                    self.uid, len(wrapped),
                )
                return wrapped

        return None

    # ------------------------------------------------------------------
    # Main polling loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main polling loop."""
        self._running = True

        log.info(
            "[Weibo UID:%d] starting poll monitor, interval=%ds",
            self.uid,
            self._poll_interval,
        )

        while self._running:
            try:
                cards = await self._fetch_posts()

                if cards:
                    await self._process_cards(cards)
                else:
                    # Handle empty result (e.g. user never posted, or API banned)
                    if not self._seen_ids:
                        self._seen_ids.append("-1")
                        log.debug(
                            "[Weibo UID:%d] initialized with placeholder id",
                            self.uid,
                        )
            except Exception:
                log.exception(
                    "[Weibo UID:%d] unhandled error in poll loop, recovering...",
                    self.uid,
                )

            await self._sleep(self._poll_interval)

        log.info("[Weibo UID:%d] poll monitor stopped", self.uid)

    # ------------------------------------------------------------------
    # Post processing
    # ------------------------------------------------------------------

    async def _process_cards(self, cards: list[dict]) -> None:
        """Process fetched cards — detect new posts and push."""
        card = cards[0]
        mblog = card["mblog"]
        mblog_id = mblog["id"]
        user = mblog["user"]
        screen_name = user["screen_name"]
        avatar_url = (
            user.get("avatar_hd")
            or user.get("avatar_large")
            or user.get("profile_image_url")
        )

        self._uname = screen_name

        # Initialize if first poll
        if not self._seen_ids:
            for index in range(min(CACHE_MAXLEN, len(cards))):
                self._seen_ids.append(cards[index]["mblog"]["id"])
            log.debug(
                "[Weibo %s(UID:%d)] dynamic initialized, %d ids cached",
                screen_name,
                self.uid,
                len(self._seen_ids),
            )
            return

        # Check if newest id is new
        if mblog_id in self._seen_ids:
            log.debug(
                "[Weibo %s(UID:%d)] poll ok — no new post (latest id=%s)",
                screen_name,
                self.uid,
                mblog_id,
            )
            return

        # Record the new ID
        self._seen_ids.append(mblog_id)

        # Only process card_type 9 (regular posts)
        # NOTE: ajax fallback wraps everything as card_type 9
        card_type = card.get("card_type")
        if card_type not in (9,):
            log.debug(
                "[Weibo %s(UID:%d)] new post card_type=%s skipped",
                screen_name,
                self.uid,
                card_type,
            )
            return

        # Skip posts older than yesterday
        try:
            created_at = time.strptime(mblog["created_at"], "%a %b %d %H:%M:%S %z %Y")
            created_at_ts = time.mktime(created_at)
        except (ValueError, KeyError):
            log.warning(
                "[Weibo %s(UID:%d)] cannot parse created_at",
                screen_name,
                self.uid,
            )
            return

        yesterday = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")
        yesterday_ts = time.mktime(time.strptime(yesterday, "%Y-%m-%d"))
        if created_at_ts < yesterday_ts:
            log.debug(
                "[Weibo %s(UID:%d)] new post but too old (%s), skip push",
                screen_name,
                self.uid,
                mblog.get("created_at", ""),
            )
            return

        dynamic_time = time.strftime("%Y-%m-%d %H:%M:%S", created_at)

        # Extract content
        text = mblog.get("text", "")
        raw_text = mblog.get("raw_text", "")
        content = raw_text if raw_text else _strip_html(text)

        # Extract picture
        pic_url = mblog.get("original_pic", None)
        pics_url: list[str] = []
        if pic_url:
            pics_url = [pic_url]
        # Also check for multiple pictures (pics array)
        pics = mblog.get("pics", [])
        for p in pics:
            url = p.get("original_pic") or p.get("large") or p.get("url")
            if url and url not in pics_url:
                pics_url.append(url)
        # Also check pic_infos dict (from ajax API)
        pic_infos = mblog.get("pic_infos", {})
        for _key, info in pic_infos.items():
            if isinstance(info, dict):
                url = (
                    info.get("largest", {}).get("url")
                    or info.get("mw2000", {}).get("url")
                    or info.get("original", {}).get("url")
                    or info.get("large", {}).get("url")
                )
                if url and url not in pics_url:
                    pics_url.append(url)

        # Build post URL
        scheme = card.get("scheme", "")
        dynamic_url = scheme or f"https://m.weibo.cn/detail/{mblog_id}"

        log.info(
            "[Weibo %s(UID:%d)] new post detected: %s...",
            screen_name,
            self.uid,
            content[:50] if content else "(no text)",
        )

        await self._on_new_dynamic(
            uname=screen_name,
            dynamic_id=mblog_id,
            content=content,
            pic_url=pic_url,
            pics_url=pics_url or None,
            dynamic_type="weibo",
            dynamic_time=dynamic_time,
            dynamic_url=dynamic_url,
            avatar_url=avatar_url,
        )

    async def stop(self) -> None:
        """Stop the polling loop and close connections."""
        self._running = False
        if self._owns_session and self._http and not self._http.closed:
            await self._http.close()

    @property
    def uname(self) -> str:
        return self._uname
