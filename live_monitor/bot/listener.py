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
import time
from collections.abc import Awaitable, Callable
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
        allowed_groups: Optional set of group IDs where commands are allowed.
        http_timeout: HTTP request timeout in seconds.
        reconnect_delay: Base delay (seconds) for reconnect backoff.
        on_ws_connected: Optional async callback invoked when the WS
            connection is established (or re-established).
        on_ws_disconnected: Optional async callback invoked when the WS
            connection is lost.  *Not* called on the very first connection
            attempt — only fires after an established connection drops.
        on_reconnect_stalled: Optional async callback invoked when the WS
            reconnect has been failing for longer than
            *reconnect_stall_timeout*.  Fires at most once per disconnect
            episode — subsequent reconnect failures do NOT re-trigger it.
        reconnect_stall_timeout: Seconds after which a reconnect attempt
            is considered "stalled" (default 30).  Only meaningful when
            *on_reconnect_stalled* is provided.
        on_qq_online: Optional async callback invoked when the QQ account
            transitions from offline → online (detected via heartbeat).
        on_qq_offline: Optional async callback invoked when the QQ account
            transitions from online → offline (detected via heartbeat).
    """

    def __init__(
        self,
        ws_url: str,
        bot_qq: int,
        pusher: NapCatQQPusher,
        allowed_groups: list[int] | None = None,
        http_timeout: float = 15.0,
        reconnect_delay: float = 3.0,
        on_ws_connected: Callable[[], Awaitable[None]] | None = None,
        on_ws_disconnected: Callable[[], Awaitable[None]] | None = None,
        on_reconnect_stalled: Callable[[], Awaitable[None]] | None = None,
        reconnect_stall_timeout: float = 30.0,
        on_qq_online: Callable[[], Awaitable[None]] | None = None,
        on_qq_offline: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._ws_url = ws_url
        self._bot_qq = str(bot_qq)
        self._pusher = pusher
        self._allowed_groups: set[int] | None = (
            set(allowed_groups) if allowed_groups is not None else None
        )
        self._http_timeout = http_timeout
        self._reconnect_delay = reconnect_delay
        self._on_ws_connected = on_ws_connected
        self._on_ws_disconnected = on_ws_disconnected
        self._on_reconnect_stalled = on_reconnect_stalled
        self._reconnect_stall_timeout = reconnect_stall_timeout
        self._on_qq_online = on_qq_online
        self._on_qq_offline = on_qq_offline

        self._session: aiohttp.ClientSession | None = None
        self._running = False
        # Tracks whether we have *ever* successfully connected.
        # Used to avoid triggering on_ws_disconnected on initial connect failure.
        self._was_connected = False
        # Whether this is the very first connection attempt.
        # Used to suppress on_ws_connected on initial connect — it only
        # fires on *re*-connection (after having been connected before).
        self._first_connect = True
        # Tracks QQ online status. Defaults to True so that the initial
        # assumption is "online" — subsequent heartbeat events will
        # correct this and trigger the offline callback if needed.
        self._qq_online = True
        # Timestamp (monotonic clock) when the WS connection was last lost.
        # None when connected.
        self._disconnect_time: float | None = None
        # Whether we have already fired on_reconnect_stalled for the
        # current disconnect episode.  Reset on successful reconnect.
        self._stall_notified: bool = False

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
            await self._handle_meta_event(data)
        else:
            log.debug("Unhandled event type: %s", post_type)

    async def _handle_meta_event(self, data: dict) -> None:
        """Handle meta_event (lifecycle, heartbeat, etc.)."""
        meta_type = data.get("meta_event_type", "unknown")

        if meta_type == "heartbeat":
            # OneBot heartbeat — contains QQ online status.
            status = data.get("status", {})
            online = status.get("online", True)

            if online != self._qq_online:
                self._qq_online = online
                if not online and self._on_qq_offline:
                    log.warning("[QQ] QQ account went OFFLINE")
                    try:
                        await self._on_qq_offline()
                    except Exception:
                        log.exception("[QQ] on_qq_offline callback failed")
                elif online and self._on_qq_online:
                    log.info("[QQ] QQ account back ONLINE")
                    try:
                        await self._on_qq_online()
                    except Exception:
                        log.exception("[QQ] on_qq_online callback failed")
        else:
            log.debug("Meta event: %s", meta_type)

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
                    self._was_connected = True

                    # Reset disconnect tracking — we are back online.
                    self._disconnect_time = None
                    self._stall_notified = False

                    # Notify on *re*-connection only — skip the very first
                    # connect to avoid a spurious "✅ NapCat 已恢复" at startup.
                    if not self._first_connect and self._on_ws_connected:
                        try:
                            await self._on_ws_connected()
                        except Exception:
                            log.exception("[WS] on_ws_connected callback failed")
                    self._first_connect = False

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

                    # ── Connection dropped ──────────────────
                    # WS loop exited while we were connected → record time.
                    self._disconnect_time = time.monotonic()
                    log.warning("[WS] Connection lost, starting reconnects...")

                    if self._on_ws_disconnected:
                        try:
                            await self._on_ws_disconnected()
                        except Exception:
                            log.exception("[WS] on_ws_disconnected callback failed")

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

                # ── Reconnect stall detection ──────────────
                if (
                    self._was_connected
                    and self._on_reconnect_stalled
                    and self._disconnect_time is not None
                    and not self._stall_notified
                ):
                    elapsed = time.monotonic() - self._disconnect_time
                    if elapsed >= self._reconnect_stall_timeout:
                        self._stall_notified = True
                        log.warning(
                            "[WS] Reconnect stalled for %.0fs — notifying",
                            elapsed,
                        )
                        try:
                            await self._on_reconnect_stalled()
                        except Exception:
                            log.exception(
                                "[WS] on_reconnect_stalled callback failed"
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
