# -*- coding: utf-8 -*-
"""
Application - main orchestrator.

Responsible for:
  - Loading configuration
  - Creating monitors and pushers
  - Binding event callbacks
  - Managing the asyncio event loop lifecycle
"""

import asyncio
import logging
import signal
import sys

from .bot.listener import NapcatEventListener
from .config import load_config
from .monitor.bilibili_dynamic import BiliDynamicPollMonitor
from .monitor.bilibili_live import BiliLivePollMonitor
from .pusher.napcat import NapCatQQPusher


log = logging.getLogger(__name__)


class Application:
    """Main application class that orchestrates all components.

    Usage:
        app = Application("config.yml")
        app.run()
    """

    def __init__(self, config_path: str = "config.yml") -> None:
        # Setup default logging first so load_config() logs can be seen
        self._setup_logging(level="INFO")

        # Load and validate config
        self.config = load_config(config_path)

        # Update log level from config
        self._setup_logging(level=self.config.log_level)

        # Component lists
        self._pushers: list = []
        self._monitors: list = []
        self._listener: NapcatEventListener | None = None

        # Initialize
        self._init_pushers()
        self._init_monitors()
        self._init_listener()

        # Lifecycle
        self._stop_event = asyncio.Event()

        log.info("Application initialized with config from %s", config_path)

    def _setup_logging(self, level: str | None = None) -> None:
        """Configure logging.

        Args:
            level: Optional log level string (e.g. "INFO", "DEBUG").
                   If None, reads from self.config.log_level.
        """
        level_str = (level or "INFO").upper()
        resolved_level = getattr(logging, level_str, logging.INFO)

        root = logging.getLogger()
        if not root.hasHandlers():
            # First call: install handler + set level
            logging.basicConfig(
                level=resolved_level,
                format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        else:
            # Subsequent calls: only update level (basicConfig is a no-op once
            # handlers exist, so we set level directly)
            root.setLevel(resolved_level)
            for handler in root.handlers:
                handler.setLevel(resolved_level)

    def _init_pushers(self) -> None:
        """Create pusher instances from config."""
        napcat_cfg = self.config.pusher.napcat
        self._pushers.append(
            NapCatQQPusher(
                api_url=napcat_cfg.api_url,
                user_ids=[napcat_cfg.user_id] if napcat_cfg.user_id else [],
                group_ids=napcat_cfg.group_ids,
                token=napcat_cfg.token,
                at_qq=napcat_cfg.at_qq,
            )
        )

    def _init_monitors(self) -> None:
        """Create monitor instances and bind event callbacks."""
        bili_cfg = self.config.monitor.bilibili

        for uid in bili_cfg.uid_list:
            # Live status monitor
            monitor = BiliLivePollMonitor(
                uid=uid,
                on_live_start=self._on_live_start,
                on_live_end=self._on_live_end if bili_cfg.notify_live_end else None,
                poll_interval=bili_cfg.poll_interval,
            )
            self._monitors.append(monitor)
            log.info(
                "Monitor created: UID=%d, interval=%ds%s",
                uid,
                bili_cfg.poll_interval,
                "" if bili_cfg.notify_live_end else " (live_end disabled)",
            )

            # Dynamic (动态) monitor
            if bili_cfg.notify_dynamic:
                dynamic_monitor = BiliDynamicPollMonitor(
                    uid=uid,
                    on_new_dynamic=self._on_new_dynamic,
                    poll_interval=bili_cfg.dynamic_poll_interval,
                    skip_forward=bili_cfg.skip_forward,
                    cookie=bili_cfg.cookie,
                )
                self._monitors.append(dynamic_monitor)
                log.info(
                    "Dynamic monitor created: UID=%d, interval=%ds%s",
                    uid,
                    bili_cfg.dynamic_poll_interval,
                    " (skip_forward)" if bili_cfg.skip_forward else "",
                )

    def _init_listener(self) -> None:
        """Create the NapCatQQ event listener if bot_qq is configured."""
        listener_cfg = self.config.listener
        if not listener_cfg.bot_qq:
            log.warning("未配置 listener.bot_qq，事件监听器未启动")
            return

        # Get the first napcat pusher for sending command replies
        napcat_pushers = [p for p in self._pushers if isinstance(p, NapCatQQPusher)]
        if not napcat_pushers:
            log.warning("未配置 NapCat pusher，事件监听器无法发送回复")
            return

        pusher = napcat_pushers[0]

        # Derive ws_url from api_url if not explicitly configured
        ws_url = listener_cfg.ws_url
        if not ws_url:
            api_url = pusher.api_url
            ws_url = (
                api_url.replace("http://", "ws://").replace("https://", "wss://")
                + "/ws"
            )

        self._listener = NapcatEventListener(
            ws_url=ws_url,
            bot_qq=listener_cfg.bot_qq,
            pusher=pusher,
            allowed_groups=listener_cfg.allowed_groups or None,
        )

        groups_info = (
            f" allowed_groups={listener_cfg.allowed_groups}"
            if listener_cfg.allowed_groups
            else " (no group restriction)"
        )
        log.info(
            "NapCatQQ 事件监听器已创建 (bot_qq=%s, ws=%s%s)",
            listener_cfg.bot_qq,
            ws_url,
            groups_info,
        )

    # ── Event → Push routing ───────────────────────────────────

    async def _on_live_start(
        self, uname: str, room_id: int, room_title: str, cover_url: str
    ) -> None:
        """Handle live start event: push to all pushers."""
        log.info("🔴 %s 开播 (room=%d)", uname, room_id)
        for pusher in self._pushers:
            try:
                await pusher.push_live_start(uname, room_id, room_title, cover_url)
            except Exception:
                log.exception("[%s] push_live_start failed", pusher.name)

    async def _on_live_end(self, uname: str, room_id: int) -> None:
        """Handle live end event: push to all pushers."""
        log.info("⏹️ %s 下播 (room=%d)", uname, room_id)
        for pusher in self._pushers:
            try:
                await pusher.push_live_end(uname, room_id)
            except Exception:
                log.exception("[%s] push_live_end failed", pusher.name)

    async def _on_new_dynamic(
        self,
        uname: str,
        dynamic_id: str,
        content: str,
        pic_url: str | None = None,
        dynamic_type: str = "",
        dynamic_time: str = "",
        dynamic_url: str = "",
        avatar_url: str | None = None,
    ) -> None:
        """Handle new dynamic event: push to all pushers."""
        log.info("📝 %s %s (id=%s)", uname, dynamic_type or "发动态", dynamic_id)
        for pusher in self._pushers:
            try:
                await pusher.push_dynamic(
                    uname=uname,
                    dynamic_id=dynamic_id,
                    content=content,
                    pic_url=pic_url,
                    dynamic_type=dynamic_type,
                    dynamic_time=dynamic_time,
                    dynamic_url=dynamic_url,
                    avatar_url=avatar_url,
                )
            except Exception:
                log.exception("[%s] push_dynamic failed", pusher.name)

    async def _broadcast_notification(self, title: str, message: str = "") -> None:
        """Broadcast a notification to all pushers (startup/shutdown)."""
        for pusher in self._pushers:
            try:
                await pusher.push_notification(title, message)
            except Exception:
                log.exception("[%s] push_notification failed", pusher.name)

    # ── Lifecycle ──────────────────────────────────────────────

    def run(self) -> None:
        """Synchronous entry point. Calls asyncio.run()."""
        asyncio.run(self._run_async())

    async def _run_async(self) -> None:
        """Async main loop."""
        # Startup notification
        uid_list = self.config.monitor.bilibili.uid_list
        if uid_list:
            info = f"监控 UID: {', '.join(str(u) for u in uid_list)}"
            await self._broadcast_notification("🟢 bili-liver-monitor 已启动", info)
        else:
            await self._broadcast_notification("🟢 bili-liver-monitor 已启动")

        # Start all monitors
        tasks = [asyncio.create_task(m.run()) for m in self._monitors]

        # Start the NapCatQQ event listener if configured
        if self._listener:
            tasks.append(asyncio.create_task(self._listener.run()))

        log.info(
            "Started %d monitors, %d pushers%s",
            len(self._monitors),
            len(self._pushers),
            ", listener enabled" if self._listener else "",
        )

        # Setup signal handling
        loop = asyncio.get_running_loop()
        if sys.platform != "win32":
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, self._stop_event.set)
                except NotImplementedError:
                    pass

        # Wait for stop
        try:
            await self._stop_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        """Graceful shutdown of all components."""
        log.info("Shutting down...")

        # Stop all monitors
        for m in self._monitors:
            await m.stop()

        # Stop the listener
        if self._listener:
            await self._listener.stop()

        # Broadcast shutdown
        await self._broadcast_notification("🟠 bili-liver-monitor 已关闭")

        # Close pushers
        for p in self._pushers:
            await p.close()

        log.info("Shutdown complete.")
