# -*- coding: utf-8 -*-
"""
Bilibili live status monitor via HTTP polling.

For each monitored UID:
  1. Periodically fetch live status via REST API.
  2. Detect live_status transition (0 -> 1 = live start, 1 -> 0 = live end).
  3. On state change -> trigger callback.

Reference: live-monitor by same author.
"""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable

import aiohttp

from .base import Monitor

log = logging.getLogger("bili live monitor")

DEFAULT_POLL_INTERVAL = 30
BUVID3_REFRESH_INTERVAL = 3600


def _make_headers(referer: str = "") -> dict[str, str]:
    """Build browser-like headers for Bilibili API requests."""
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.4396.12 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Origin": "https://space.bilibili.com",
        "Referer": referer or "https://space.bilibili.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }


class BiliLivePollMonitor(Monitor):
    """Monitor a single Bilibili UID for live start/end via HTTP polling.

    Args:
        uid: Bilibili user ID to monitor.
        on_live_start: Async callback(uname, room_id, room_title, cover_url).
        on_live_end: Optional async callback(uname, room_id).
        poll_interval: Seconds between polls (default 30).
        http_session: Optional shared aiohttp session.
    """

    name: str = "bili-live-poll"

    def __init__(
        self,
        uid: int,
        *,
        on_live_start: Callable[[str, int, str, str], Awaitable[None]],
        on_live_end: Callable[[str, int], Awaitable[None]] | None = None,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        http_session: aiohttp.ClientSession | None = None,
    ) -> None:
        super().__init__()
        self.uid = uid
        self._on_live_start = on_live_start
        self._on_live_end = on_live_end
        self._poll_interval = poll_interval

        # Defer session creation to avoid "no running event loop" error
        # when initialized outside an async context.
        self._http: aiohttp.ClientSession | None = http_session
        self._owns_session = http_session is None

        # Cached room info
        self._uname: str = ""
        self._room_id: int = 0

        # State tracking
        self._was_live: bool | None = None

        # Buvid3 management
        self._buvid3: str = ""
        self._last_buvid3_refresh: float = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazily create the HTTP session. Safe to call from async context."""
        if self._http is None:
            self._http = aiohttp.ClientSession()
            self._owns_session = True
        return self._http

    async def _ensure_buvid3(self) -> str:
        """Get or refresh buvid3 from Bilibili SPI endpoint."""
        http = await self._get_session()
        now = time.time()
        if self._buvid3 and (now - self._last_buvid3_refresh) < BUVID3_REFRESH_INTERVAL:
            return self._buvid3

        url = "https://api.bilibili.com/x/frontend/finger/spi"
        headers = _make_headers()

        try:
            async with http.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return self._buvid3 or ""

                data = await resp.json()
                if data.get("code") != 0:
                    return self._buvid3 or ""

                spi_data = data.get("data", {})
                buvid3 = spi_data.get("b_3", "") or spi_data.get("buvid3", "")
                if buvid3:
                    self._buvid3 = buvid3
                    self._last_buvid3_refresh = now
                return buvid3

        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError):
            return self._buvid3 or ""

    async def _query_live_status(self) -> dict | None:
        """Query live status for the monitored UID."""
        http = await self._get_session()
        url = "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids"
        headers = _make_headers(
            referer=f"https://space.bilibili.com/{self.uid}/dynamic"
        )
        headers["Content-Type"] = "application/json"

        buvid3 = await self._ensure_buvid3()
        cookies = {"buvid3": buvid3} if buvid3 else {}

        payload = json.dumps({"uids": [self.uid]})

        try:
            async with http.post(
                url,
                headers=headers,
                cookies=cookies,
                data=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return None

                data = await resp.json()
                code = data.get("code", -1)

                if code == -352:
                    log.warning(
                        "[UID %d] -352 risk control, refreshing buvid3", self.uid
                    )
                    self._buvid3 = ""
                    self._last_buvid3_refresh = 0
                    return None

                if code != 0:
                    return None

                room_data = data.get("data", {}).get(str(self.uid))
                return room_data

        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            log.debug("[UID %d] query error: %s", self.uid, e)
            return None

    async def run(self) -> None:
        """Main polling loop."""
        self._running = True
        await self._ensure_buvid3()

        while self._running:
            room_data = await self._query_live_status()

            if room_data:
                await self._process_room_data(room_data)
            else:
                log.debug("[UID %d] polling returned no data", self.uid)

            await self._sleep(self._poll_interval)

    async def _process_room_data(self, room_data: dict) -> None:
        """Detect live status changes and trigger callbacks."""
        uname = room_data.get("uname", f"UID_{self.uid}")
        room_id = room_data.get("room_id", 0)
        live_status = room_data.get("live_status", 0)
        room_title = room_data.get("title", "")
        cover_url = room_data.get("cover_from_user", "") or room_data.get("face", "")

        is_live = live_status == 1

        # Cache current info
        self._uname = uname
        self._room_id = room_id

        if self._was_live is None:
            # First poll — just record state
            self._was_live = is_live
            log.info(
                "[%s] initial live_status=%d (room_id=%d)",
                uname,
                live_status,
                room_id,
            )
            return

        if is_live and not self._was_live:
            # 0 -> 1: Live started
            self._was_live = True
            log.info("[%s] 🔴 开播！room_id=%d", uname, room_id)
            await self._on_live_start(uname, room_id, room_title, cover_url)

        elif not is_live and self._was_live:
            # 1 -> 0: Live ended
            self._was_live = False
            log.info("[%s] ⏹️ 下播 room_id=%d", uname, room_id)
            if self._on_live_end:
                await self._on_live_end(uname, room_id)

    async def stop(self) -> None:
        """Stop the polling loop and close connections."""
        self._running = False
        if self._owns_session and self._http and not self._http.closed:
            await self._http.close()

    @property
    def uname(self) -> str:
        return self._uname
