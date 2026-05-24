# -*- coding: utf-8 -*-
"""
Bark push notification client.

Bark is an iOS push notification app (https://github.com/Finb/Bark).
This module provides an alert-only channel (NOT a content pusher) —
it is used exclusively for operational alerts such as NapCatQQ disconnect.
"""

from __future__ import annotations

import asyncio
import logging

import aiohttp

log = logging.getLogger(__name__)


class BarkPusher:
    """Push alerts to Bark App.

    This is an alert-only channel and does NOT inherit from
    :class:`~live_monitor.pusher.base.Pusher`.

    Uses the Bark JSON Push API (``POST /push``) which is the
    recommended format for Bark v2+.

    Args:
        device_key: Bark device key (found in the Bark app).
        server_url: Bark server base URL (default: official https://api.day.app).
    """

    def __init__(
        self,
        device_key: str,
        server_url: str = "https://api.day.app",
    ) -> None:
        self._device_key = device_key
        self._server_url = server_url.rstrip("/")
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def push_alert(self, title: str, body: str = "") -> bool:
        """Send a push alert via Bark.

        Sends a JSON POST to ``{server_url}/push``::

            {"device_key": "...", "title": "...", "body": "...", "isArchive": 1}

        Args:
            title: Notification title (required).
            body: Notification body (optional).

        Returns:
            True if the server responded with 2xx.
        """
        session = await self._get_session()

        url = f"{self._server_url}/push"

        payload: dict = {
            "device_key": self._device_key,
            "title": title,
            "isArchive": 1,
        }
        if body:
            payload["body"] = body

        try:
            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    log.info("Bark: alert sent OK (%s)", title)
                    return True

                body_text = await resp.text()
                log.warning("Bark: HTTP %d: %s", resp.status, body_text[:200])
                return False

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("Bark: request failed: %s", e)
            return False

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
