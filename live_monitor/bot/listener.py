"""
NapCatQQ event listener via Forward WebSocket.

Slim listener — receives WebSocket events, builds a MessageContext,
and dispatches to registered Commands via MessageDispatcher.

Command modules (e.g. pic_proc_cmd.py) handle the actual logic.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import TYPE_CHECKING

import aiohttp

from ..command.base import MessageContext
from ..command.dispatcher import MessageDispatcher
from ..command.pic_proc_cmd import FlipCommand, SymmetryCommand, FlipUpsideDownCommand
from ..pusher.napcat import NapCatQQPusher

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)

# Regex to convert array format back to CQ string
RE_CQ_IMAGE = re.compile(r"\[CQ:image[^\]]*?url=(?P<url>[^\],]+)")
RE_CQ_AT = re.compile(r"\[CQ:at[^\]]*?qq=(?P<qq>\d+)")


def _segments_to_cq_string(segments: list) -> str:
    """Convert NapCatQQ message segment array back to CQ string."""
    parts = []
    for seg in segments:
        seg_type = seg.get("type", "")
        seg_data = seg.get("data", {})
        if seg_type == "text":
            parts.append(seg_data.get("text", ""))
        elif seg_type == "image":
            attrs = ",".join(f"{k}={v}" for k, v in seg_data.items() if v)
            parts.append(f"[CQ:image,{attrs}]")
        elif seg_type == "reply":
            parts.append(f"[CQ:reply,id={seg_data.get('id', '')}]")
        elif seg_type == "at":
            parts.append(f"[CQ:at,qq={seg_data.get('qq', '')}]")
        else:
            attrs = ",".join(f"{k}={v}" for k, v in seg_data.items() if v)
            parts.append(f"[CQ:{seg_type},{attrs}]")
    return "".join(parts)


def _strip_cq_code_text(message: str) -> str:
    """Strip CQ codes to get plain text."""
    return re.sub(r"\[CQ:[^\]]*\]", "", message).strip()


class NapcatEventListener:
    """Listen to NapCatQQ events via Forward WebSocket.

    Connects to NapCatQQ's WebSocket endpoint, builds a MessageContext,
    and dispatches to registered Commands.

    Args:
        ws_url: WebSocket URL (e.g., "ws://127.0.0.1:3000/ws").
        bot_qq: Bot's own QQ number (to detect @bot in group messages).
        pusher: NapCatQQPusher instance for sending messages back.
        http_timeout: HTTP request timeout in seconds.
        reconnect_delay: Base delay (seconds) for reconnect backoff.
    """

    def __init__(
        self,
        ws_url: str,
        bot_qq: int,
        pusher: NapCatQQPusher,
        allowed_groups: list[int] | None = None,
        http_timeout: float = 15.0,
        reconnect_delay: float = 3.0,
    ) -> None:
        self._ws_url = ws_url
        self._bot_qq = str(bot_qq)
        self._pusher = pusher
        self._allowed_groups: set[int] | None = (
            set(allowed_groups) if allowed_groups is not None else None
        )
        self._http_timeout = http_timeout
        self._reconnect_delay = reconnect_delay

        self._session: aiohttp.ClientSession | None = None
        self._running = False

        # ── Build command dispatcher ──
        self._dispatcher = MessageDispatcher()
        self._dispatcher.register(FlipCommand())
        self._dispatcher.register(SymmetryCommand())
        self._dispatcher.register(FlipUpsideDownCommand())

    # ── Event handling ─────────────────────────────────────────────

    async def _handle_event(self, data: dict) -> None:
        """Route incoming WebSocket events to the appropriate handler."""
        post_type = data.get("post_type", "")

        if post_type in ("message", "message_sent"):
            await self._dispatch_message(data)
        elif post_type == "meta_event":
            meta_type = data.get("meta_event_type", "unknown")
            log.debug("Meta event: %s", meta_type)
        else:
            log.debug("Unhandled event type: %s", post_type)

    async def _dispatch_message(self, data: dict) -> None:
        """Build MessageContext from raw event and dispatch to commands."""
        # ── Extract raw fields ──
        message_type = data.get("message_type", "")
        raw_message = data.get("raw_message", "") or data.get("message", "")
        user_id = data.get("user_id", 0)
        group_id = data.get("group_id")
        message_id = data.get("message_id", 0)
        post_type = data.get("post_type", "")

        # ── message_sent remapping ──
        # NapCatQQ puts group_id in target_id for self-sent messages
        if post_type == "message_sent":
            target_id = data.get("target_id")
            if target_id:
                if message_type == "group":
                    group_id = target_id
                elif message_type == "private":
                    user_id = target_id

        # ── Log incoming (debug only) ──
        raw_preview = raw_message[:120] if raw_message else "(empty)"
        log.debug(
            "[RAW] type=%s user=%s group=%s msg_id=%s content=%s",
            message_type,
            user_id,
            group_id,
            message_id,
            raw_preview,
        )

        # ── Convert array → CQ string ──
        if isinstance(raw_message, list):
            raw_message = _segments_to_cq_string(raw_message)

        # ── Build context ──
        ctx = MessageContext(
            post_type=post_type,
            message_type=message_type,
            user_id=user_id,
            group_id=group_id,
            message_id=message_id,
            target_id=data.get("target_id"),
            raw_message=raw_message,
            plain_text=_strip_cq_code_text(raw_message),
            bot_qq=self._bot_qq,
            pusher=self._pusher,
            allowed_groups=self._allowed_groups,
            session=self._session,
        )

        # ── Dispatch ──
        matched = await self._dispatcher.dispatch(ctx)

        if not matched:
            log.debug("No command matched for message %d", message_id)

    # ── Connection lifecycle ──────────────────────────────────────

    def _ws_url_with_token(self) -> str:
        """Append access_token query param to WebSocket URL if configured."""
        token = self._pusher.token
        if not token:
            return self._ws_url
        separator = "&" if "?" in self._ws_url else "?"
        return f"{self._ws_url}{separator}access_token={token}"

    async def run(self) -> None:
        """Connect to NapCatQQ WebSocket and listen for events.

        Automatically reconnects on disconnection with exponential backoff.
        Runs indefinitely until stop() is called.
        """
        self._running = True
        delay = self._reconnect_delay

        while self._running:
            try:
                if self._session is None:
                    self._session = aiohttp.ClientSession()

                ws_url = self._ws_url_with_token()
                log.info("[WS] Connecting to %s", self._ws_url)
                async with self._session.ws_connect(
                    ws_url,
                    heartbeat=30.0,
                ) as ws:
                    log.info("[WS] Connected")
                    delay = self._reconnect_delay

                    async for msg in ws:
                        if not self._running:
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = msg.json()
                                await self._handle_event(data)
                            except Exception as e:
                                log.warning(
                                    "[WS] Failed to process event: %s", e, exc_info=True
                                )
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            log.warning(
                                "[WS] Error: %s (code=%s)",
                                ws.exception(),
                                ws.close_code,
                            )
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            log.info("[WS] Closed (code=%s)", ws.close_code)
                            break

            except asyncio.CancelledError:
                log.info("[WS] Cancelled")
                self._running = False
                break
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                log.warning(
                    "[WS] Connection failed: %s (reconnect in %.0fs)",
                    e,
                    delay,
                )
            except Exception as e:
                log.error("[WS] Unexpected error: %s", e, exc_info=True)

            if not self._running:
                break

            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 30.0)

    async def stop(self) -> None:
        """Stop the listener."""
        self._running = False
        if self._session:
            await self._session.close()
            self._session = None
