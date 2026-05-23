# -*- coding: utf-8 -*-
"""
Bilibili dynamic (动态) monitor via HTTP polling.

For each monitored UID:
  1. Periodically fetch latest dynamic via REST API.
  2. Detect new dynamic_id not seen before.
  3. On new dynamic -> trigger callback.
  4. Supports: images/draw, video posts, articles, forwards (optional), etc.

Reference: aio-dynamic-push (query_task/query_bilibili.py)
"""

import asyncio
import logging
import time
from collections import deque
from typing import Protocol

import aiohttp

from .base import Monitor

log = logging.getLogger(__name__)

DEFAULT_DYNAMIC_POLL_INTERVAL = 60


def _make_headers(uid: int = 0) -> dict[str, str]:
    """Build browser-like headers for Bilibili API requests."""
    return {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate",
        "accept-language": "zh-CN,zh;q=0.9",
        "cache-control": "no-cache",
        "origin": "https://space.bilibili.com",
        "pragma": "no-cache",
        "referer": f"https://space.bilibili.com/{uid}/dynamic",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }


class NewDynamicCallback(Protocol):
    """Protocol for the on_new_dynamic async callback."""

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


class BiliDynamicPollMonitor(Monitor):
    """Monitor a single Bilibili UID for new dynamic posts via polling.

    Requires a cookie with SESSDATA login to access the dynamic API.
    Configure via ``config.local.yml``::

        monitor:
          bilibili:
            cookie: "SESSDATA=...; bili_jct=...; ..."

    Args:
        uid: Bilibili user ID to monitor.
        on_new_dynamic: Async callback(uname, dynamic_id, content, pic_url,
                        dynamic_type, dynamic_time, dynamic_url, avatar_url).
        poll_interval: Seconds between polls (default 60).
        skip_forward: Whether to skip forward/repost dynamics (default True).
        cookie: Cookie string with SESSDATA login (required for API access).
        http_session: Optional shared aiohttp session.
    """

    name: str = "bili-dynamic-poll"

    def __init__(
        self,
        uid: int,
        *,
        on_new_dynamic: NewDynamicCallback,
        poll_interval: int = DEFAULT_DYNAMIC_POLL_INTERVAL,
        skip_forward: bool = True,
        cookie: str = "",
        http_session: aiohttp.ClientSession | None = None,
    ) -> None:
        super().__init__()
        self.uid = uid
        self._on_new_dynamic = on_new_dynamic
        self._poll_interval = poll_interval
        self._skip_forward = skip_forward
        self._cookie = cookie

        # Defer session creation to avoid "no running event loop" error
        self._http: aiohttp.ClientSession | None = http_session
        self._owns_session = http_session is None

        # Deque of known dynamic IDs (max 100)
        self._dynamic_ids: deque[str] = deque(maxlen=100)

        # Cached uname
        self._uname: str = ""

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazily create the HTTP session. Safe to call from async context."""
        if self._http is None:
            self._http = aiohttp.ClientSession()
            self._owns_session = True
        return self._http

    async def _query_latest_dynamic(self) -> dict | None:
        """Fetch the latest dynamic(s) for the monitored UID.

        Returns:
            The first (newest) non-pinned dynamic item dict, or None on failure.
        """
        http = await self._get_session()
        uid = str(self.uid)
        query_url = (
            f"https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/space"
            f"?host_mid={uid}&offset=&my_ts={int(time.time())}&features=itemOpusStyle"
        )
        headers = _make_headers(uid=self.uid)

        # Apply cookie if configured (must include SESSDATA login)
        if self._cookie:
            headers["cookie"] = self._cookie

        try:
            async with http.get(
                query_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    log.warning("[UID:%d] feed/space HTTP %d", self.uid, resp.status)
                    return None

                data = await resp.json()
                code = data.get("code", -1)
                msg = data.get("message", "")

                if code != 0:
                    log.warning(
                        "[UID:%d] feed/space code=%d msg=%s",
                        self.uid,
                        code,
                        msg,
                    )
                    return None

                result_data = data.get("data", {})
                items = result_data.get("items", [])
                if not items:
                    log.debug("[UID:%d] no items in response", self.uid)
                    return None
                else:
                    log.debug("[UID:%d] got %d items", self.uid, len(items))

                # Filter out pinned items (置顶)
                items = [
                    item
                    for item in items
                    if (
                        (item.get("modules", {}).get("module_tag") or {}).get("text")
                        != "置顶"
                    )
                ]

                if not items:
                    log.debug("[UID:%d] all items are pinned", self.uid)
                    return None

                return items[0]

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("[UID:%d] query error: %s", self.uid, e)
            return None

    async def run(self) -> None:
        """Main polling loop."""
        self._running = True

        log.info(
            "[UID:%d] starting poll monitor, interval=%ds",
            self.uid,
            self._poll_interval,
        )

        while self._running:
            try:
                item = await self._query_latest_dynamic()

                if item:
                    await self._process_item(item)
                else:
                    log.debug("[UID:%d] poll skipped — no item returned", self.uid)
            except Exception:
                log.exception(
                    "[UID:%d] unhandled error in poll loop, recovering...",
                    self.uid,
                )

            await self._sleep(self._poll_interval)

        log.info("[UID:%d] poll monitor stopped", self.uid)

    async def _process_item(self, item: dict) -> None:
        """Process a single dynamic item — detect if new and push if so."""
        dynamic_id = item.get("id_str")
        if not dynamic_id:
            return

        # Get author info
        try:
            author_module = item["modules"]["module_author"]
            uname = author_module["name"]
            avatar_url = author_module.get("face")
        except (KeyError, TypeError):
            log.error("[UID:%d] cannot parse author info", self.uid)
            return

        self._uname = uname

        # Initialize if first poll
        if not self._dynamic_ids:
            self._dynamic_ids.append(dynamic_id)
            log.info(
                "[%s(UID:%d)] dynamic initialized, latest id=%s",
                uname,
                self.uid,
                dynamic_id,
            )
            return

        # Check if this is a new dynamic
        if dynamic_id in self._dynamic_ids:
            log.debug(
                "[%s(UID:%d)] poll ok — no new dynamic (latest id=%s)",
                uname,
                self.uid,
                dynamic_id,
            )
            return

        # Record the new ID (appendleft keeps most recent at front)
        self._dynamic_ids.appendleft(dynamic_id)

        # Parse dynamic type
        dynamic_type = item.get("type", "")
        allow_types = {
            "DYNAMIC_TYPE_DRAW",  # 图文/图片动态
            "DYNAMIC_TYPE_WORD",  # 纯文字动态
            "DYNAMIC_TYPE_AV",  # 投稿视频
            "DYNAMIC_TYPE_ARTICLE",  # 投稿专栏
            "DYNAMIC_TYPE_COMMON_SQUARE",  # 装扮
        }
        if not self._skip_forward:
            allow_types.add("DYNAMIC_TYPE_FORWARD")

        if dynamic_type not in allow_types:
            log.info(
                "[%s(UID:%d)] new dynamic type=%s skipped (not in push list)",
                uname,
                self.uid,
                dynamic_type,
            )
            return

        # Parse content, pic, timestamp
        timestamp = int(
            item.get("modules", {}).get("module_author", {}).get("pub_ts", 0)
        )
        dynamic_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
        module_dynamic = item.get("modules", {}).get("module_dynamic", {})

        content = None
        pic_url = None
        pics_url: list[str] = []

        if dynamic_type == "DYNAMIC_TYPE_FORWARD":
            content = module_dynamic.get("desc", {}).get("text", "")

        elif dynamic_type == "DYNAMIC_TYPE_DRAW":
            major = module_dynamic.get("major", {})
            if major.get("type") == "MAJOR_TYPE_OPUS":
                opus = major.get("opus", {})
                summary = opus.get("summary", {})
                content = summary.get("text", "")
                # Prepend title if exists
                title = opus.get("title", "")
                if title:
                    content = f"[{title}] {content}" if content else title
                # Get all pictures
                pics = opus.get("pics", [])
                pics_url = [p.get("url") for p in pics if p.get("url")]
                if pics_url:
                    pic_url = pics_url[0]
            else:
                content = module_dynamic.get("desc", {}).get("text", "")

        elif dynamic_type == "DYNAMIC_TYPE_WORD":
            content = module_dynamic.get("desc", {}).get("text", "")

        elif dynamic_type == "DYNAMIC_TYPE_AV":
            major = module_dynamic.get("major", {})
            archive = major.get("archive", {})
            content = archive.get("title", "")
            cover = archive.get("cover", "")
            if cover:
                pic_url = cover
                pics_url = [cover]

        elif dynamic_type == "DYNAMIC_TYPE_ARTICLE":
            major = module_dynamic.get("major", {})
            opus = major.get("opus", {})
            content = opus.get("title", "")
            pics = opus.get("pics", [])
            pics_url = [p.get("url") for p in pics if p.get("url")]
            if pics_url:
                pic_url = pics_url[0]

        elif dynamic_type == "DYNAMIC_TYPE_COMMON_SQUARE":
            content = module_dynamic.get("desc", {}).get("text", "")

        if not content:
            log.info(
                "[%s(UID:%d)] new dynamic but no parseable content, skip push",
                uname,
                self.uid,
            )
            return

        dynamic_url = f"https://www.bilibili.com/opus/{dynamic_id}"

        log.info(
            "[%s(UID:%d)] new dynamic detected: %s...", uname, self.uid, content[:50]
        )

        await self._on_new_dynamic(
            uname=uname,
            dynamic_id=dynamic_id,
            content=content,
            pic_url=pic_url,
            pics_url=pics_url or None,
            dynamic_type=dynamic_type,
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
