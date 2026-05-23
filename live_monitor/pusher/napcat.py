# -*- coding: utf-8 -*-
"""
NapCatQQ push notification sender.

Sends formatted messages to NapCatQQ via its HTTP API.
Supports both private messages and group messages.
"""

import asyncio
import logging

import aiohttp

from .base import Pusher

log = logging.getLogger("napcat pusher")


class NapCatQQPusher(Pusher):
    """Push notifications to NapCatQQ via HTTP API.

    Args:
        api_url: NapCatQQ HTTP API base URL (e.g., http://localhost:3000).
        user_ids: Target QQ user ID(s) for private messages.
        group_ids: Optional target QQ group ID(s) for group messages.
        token: Optional API access token.
        at_qq: Whether to @all in group messages ("all" or "").
    """

    name: str = "napcat"

    def __init__(
        self,
        api_url: str,
        user_ids: list[int] | None = None,
        group_ids: list[int] | None = None,
        token: str = "",
        at_qq: str = "",
    ) -> None:
        super().__init__()
        self.api_url = api_url.rstrip("/")
        self.user_ids = user_ids or []
        self.group_ids = group_ids or []
        self.token = token
        self.at_qq = at_qq
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _do_send(self, target_type: str, target_id: int, msg: str) -> bool:
        """Low-level HTTP call to NapCatQQ API.

        Args:
            target_type: "private" or "group".
            target_id: QQ user ID or group ID.
            msg: Message text to send.

        Returns:
            True if successfully sent.
        """
        session = await self._get_session()

        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        if target_type == "group":
            endpoint = f"{self.api_url}/send_group_msg"
            payload = {"group_id": target_id, "message": msg}
        else:
            endpoint = f"{self.api_url}/send_private_msg"
            payload = {"user_id": target_id, "message": msg}

        try:
            async with session.post(
                endpoint,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (200, 204):
                    log.info("NapCatQQ: pushed to %s %d OK", target_type, target_id)
                    return True

                body = await resp.text()
                log.warning(
                    "NapCatQQ: %s %d HTTP %d: %s",
                    target_type, target_id, resp.status, body[:200],
                )
                return False

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("NapCatQQ: %s %d request failed: %s", target_type, target_id, e)
            return False

    async def _send_private_all(self, msg: str) -> list[bool]:
        """Send private message to all configured user IDs."""
        return [await self._do_send("private", uid, msg) for uid in self.user_ids]

    async def _send_group_all(self, msg: str) -> list[bool]:
        """Send group message to all configured group IDs."""
        return [await self._do_send("group", gid, msg) for gid in self.group_ids]

    # ── Public API ─────────────────────────────────────────────

    async def push_live_start(
        self,
        uname: str,
        room_id: int,
        room_title: str = "",
        cover_url: str = "",
    ) -> bool:
        """Push a live start notification to private + optional group."""
        live_url = f"https://live.bilibili.com/{room_id}"
        title_part = f" - {room_title}" if room_title else ""

        msg = f"🔴 {uname} 开播啦！{title_part}\n{live_url}"

        results = await self._send_private_all(msg)
        if self.group_ids:
            group_msg = msg
            if self.at_qq == "all":
                group_msg = f"[CQ:at,qq=all]\n{msg}"
            results.extend(await self._send_group_all(group_msg))

        success = any(results) if results else False
        if success:
            log.info("[%s] 开播推送成功 (私聊:%d 群聊:%d)", uname, len(self.user_ids), len(self.group_ids))
        return success

    async def push_live_end(self, uname: str, room_id: int) -> bool:
        """Push a live end notification to private + optional group."""
        msg = f"⏹️ {uname} 已下播"

        results = await self._send_private_all(msg)
        if self.group_ids:
            results.extend(await self._send_group_all(msg))

        success = any(results) if results else False
        if success:
            log.info("[%s] 下播推送成功 (私聊:%d 群聊:%d)", uname, len(self.user_ids), len(self.group_ids))
        return success

    async def push_notification(self, title: str, message: str = "") -> bool:
        """Push a generic notification to private only.

        Used for startup/shutdown/alert messages.
        """
        msg = f"{title}\n{message}" if message else title
        log.info("Pushing notification: %s", title)
        results = await self._send_private_all(msg)
        success = any(results) if results else False
        if success:
            log.info("Notification pushed successfully: %s", title)
        return success

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
