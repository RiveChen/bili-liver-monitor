"""Tests for Bilibili live poll monitor.

Core logic tested:
  - _process_room_data state machine (0→1 = live start, 1→0 = live end)
  - Buvid3 management
  - HTTP error handling via aioresponses

We mock the HTTP layer (aioresponses) so tests are fast and deterministic.
"""

import asyncio
import json
from unittest.mock import AsyncMock

import aiohttp
import pytest

from live_monitor.monitor.bilibili_live import BiliLivePollMonitor


# ── Helper: build a realistic room data dict ────────────────────


def _room_data(
    live_status: int = 0,
    uname: str = "TestStreamer",
    room_id: int = 777,
    title: str = "Test Stream",
    cover: str = "https://example.com/cover.jpg",
) -> dict:
    return {
        "uname": uname,
        "room_id": room_id,
        "live_status": live_status,
        "title": title,
        "cover_from_user": cover,
        "face": "https://example.com/face.jpg",
    }


def _spi_response(buvid3: str = "test-buvid3") -> str:
    """Build a Bilibili SPI endpoint response."""
    return json.dumps(
        {
            "code": 0,
            "data": {"b_3": buvid3},
        }
    )


def _live_response(data: dict | None = None) -> str:
    """Build a Bilibili live status response."""
    return json.dumps(
        {
            "code": 0,
            "data": data or {},
        }
    )


# ── Tests for _process_room_data (state machine) ───────────────


class TestProcessRoomData:
    """Test the core state machine that detects live start/end."""

    @pytest.mark.asyncio
    async def test_first_call_records_state_no_callback(self) -> None:
        """On first call (was_live=None), only record state, no callback."""
        from aioresponses import aioresponses

        with aioresponses() as mocked:
            mocked.get(
                "https://api.bilibili.com/x/frontend/finger/spi",
                status=200,
                body=_spi_response(),
                headers={"Content-Type": "application/json"},
            )

            on_start = AsyncMock()
            on_end = AsyncMock()
            monitor = BiliLivePollMonitor(
                uid=1,
                on_live_start=on_start,
                on_live_end=on_end,
                poll_interval=999,
            )

            async with aiohttp.ClientSession() as session:
                monitor._http = session

                await monitor._process_room_data(_room_data(live_status=0))

                assert monitor._was_live is False
                on_start.assert_not_awaited()
                on_end.assert_not_awaited()

                await monitor.stop()

    @pytest.mark.asyncio
    async def test_first_call_live_records_true(self) -> None:
        """If already live on first poll, record as live, no callback."""
        on_start = AsyncMock()
        monitor = BiliLivePollMonitor(uid=1, on_live_start=on_start, poll_interval=999)

        async with aiohttp.ClientSession() as session:
            monitor._http = session
            await monitor._process_room_data(_room_data(live_status=1))

            assert monitor._was_live is True
            on_start.assert_not_awaited()
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_live_start_triggers_callback(self) -> None:
        """0 -> 1 transition should trigger on_live_start."""
        on_start = AsyncMock()
        monitor = BiliLivePollMonitor(uid=1, on_live_start=on_start, poll_interval=999)

        async with aiohttp.ClientSession() as session:
            monitor._http = session
            monitor._was_live = False  # simulate first poll done

            await monitor._process_room_data(
                _room_data(live_status=1, uname="Alice", room_id=42, title="Hello!")
            )

            on_start.assert_awaited_once_with(
                "Alice", 42, "Hello!", "https://example.com/cover.jpg"
            )
            assert monitor._was_live is True
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_live_end_triggers_callback(self) -> None:
        """1 -> 0 transition should trigger on_live_end."""
        on_end = AsyncMock()
        monitor = BiliLivePollMonitor(
            uid=1, on_live_start=AsyncMock(), on_live_end=on_end, poll_interval=999
        )

        async with aiohttp.ClientSession() as session:
            monitor._http = session
            monitor._was_live = True  # simulate was live

            await monitor._process_room_data(
                _room_data(live_status=0, uname="Bob", room_id=99)
            )

            on_end.assert_awaited_once_with("Bob", 99)
            assert monitor._was_live is False
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_no_callback_if_no_on_live_end(self) -> None:
        """When on_live_end is None, end transition does nothing."""
        monitor = BiliLivePollMonitor(
            uid=1,
            on_live_start=AsyncMock(),
            on_live_end=None,
            poll_interval=999,
        )

        async with aiohttp.ClientSession() as session:
            monitor._http = session
            monitor._was_live = True

            # Should not raise
            await monitor._process_room_data(
                _room_data(live_status=0, uname="Charlie", room_id=55)
            )
            assert monitor._was_live is False
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_no_change_no_callback(self) -> None:
        """0->0 or 1->1 should not trigger any callback."""
        on_start = AsyncMock()
        on_end = AsyncMock()
        monitor = BiliLivePollMonitor(
            uid=1,
            on_live_start=on_start,
            on_live_end=on_end,
            poll_interval=999,
        )

        async with aiohttp.ClientSession() as session:
            monitor._http = session
            monitor._was_live = False

            # 0 -> 0
            await monitor._process_room_data(_room_data(live_status=0))
            on_start.assert_not_awaited()
            on_end.assert_not_awaited()

            monitor._was_live = True
            # 1 -> 1
            await monitor._process_room_data(_room_data(live_status=1))
            on_start.assert_not_awaited()
            on_end.assert_not_awaited()
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_live_status_2_is_offline(self) -> None:
        """live_status=2 (rotation/looping) should be treated as not-live."""
        on_start = AsyncMock()
        monitor = BiliLivePollMonitor(uid=1, on_live_start=on_start, poll_interval=999)

        async with aiohttp.ClientSession() as session:
            monitor._http = session
            monitor._was_live = None

            await monitor._process_room_data(_room_data(live_status=2))
            assert monitor._was_live is False  # treated as offline
            on_start.assert_not_awaited()
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_cache_uname_and_room_id(self) -> None:
        """_process_room_data should update cached uname and room_id."""
        monitor = BiliLivePollMonitor(
            uid=1, on_live_start=AsyncMock(), poll_interval=999
        )

        async with aiohttp.ClientSession() as session:
            monitor._http = session

            await monitor._process_room_data(
                _room_data(live_status=0, uname="Dave", room_id=123)
            )
            assert monitor.uname == "Dave"
            assert monitor._room_id == 123
            await monitor.stop()


# ── Buvid3 management tests ─────────────────────────────────────


class TestBuvid3:
    """Buvid3 fetch and refresh logic."""

    @pytest.mark.asyncio
    async def test_ensure_buvid3_caches_value(self) -> None:
        """After fetching buvid3, it should be cached."""
        from aioresponses import aioresponses

        with aioresponses() as mocked:
            mocked.get(
                "https://api.bilibili.com/x/frontend/finger/spi",
                status=200,
                body=_spi_response("my-buvid3-value"),
                headers={"Content-Type": "application/json"},
            )

            monitor = BiliLivePollMonitor(
                uid=1, on_live_start=AsyncMock(), poll_interval=999
            )
            async with aiohttp.ClientSession() as session:
                monitor._http = session

                buvid3 = await monitor._ensure_buvid3()
                assert buvid3 == "my-buvid3-value"
                assert monitor._buvid3 == "my-buvid3-value"

                # Second call should use cache, not HTTP
                buvid3_2 = await monitor._ensure_buvid3()
                assert buvid3_2 == "my-buvid3-value"

                await monitor.stop()

    @pytest.mark.asyncio
    async def test_ensure_buvid3_handles_api_error(self) -> None:
        """When SPI endpoint returns error, return cached or empty."""
        from aioresponses import aioresponses

        with aioresponses() as mocked:
            mocked.get(
                "https://api.bilibili.com/x/frontend/finger/spi",
                status=500,
            )

            monitor = BiliLivePollMonitor(
                uid=1, on_live_start=AsyncMock(), poll_interval=999
            )
            async with aiohttp.ClientSession() as session:
                monitor._http = session

                buvid3 = await monitor._ensure_buvid3()
                assert buvid3 == ""  # no cached value yet

                await monitor.stop()


# ── HTTP polling tests (aioresponses) ───────────────────────────


class TestPolling:
    """Test the HTTP polling logic with mocked network."""

    @pytest.mark.asyncio
    async def test_query_live_status_returns_data(self) -> None:
        """Successful API call should return parsed room data."""
        from aioresponses import aioresponses

        expected_data = _room_data(live_status=0, uname="Diana", room_id=789)

        with aioresponses() as mocked:
            mocked.get(
                "https://api.bilibili.com/x/frontend/finger/spi",
                status=200,
                body=_spi_response(),
                headers={"Content-Type": "application/json"},
            )
            mocked.post(
                "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids",
                status=200,
                body=_live_response({"789": expected_data}),
                headers={"Content-Type": "application/json"},
            )

            monitor = BiliLivePollMonitor(
                uid=789, on_live_start=AsyncMock(), poll_interval=999
            )
            async with aiohttp.ClientSession() as session:
                monitor._http = session
                result = await monitor._query_live_status()

                assert result is not None
                assert result["uname"] == "Diana"
                assert result["live_status"] == 0

                await monitor.stop()

    @pytest.mark.asyncio
    async def test_query_live_status_risk_control(self) -> None:
        """-352 code should clear buvid3 and return None."""
        from aioresponses import aioresponses

        with aioresponses() as mocked:
            mocked.get(
                "https://api.bilibili.com/x/frontend/finger/spi",
                status=200,
                body=_spi_response("old-buvid3"),
                headers={"Content-Type": "application/json"},
            )
            mocked.post(
                "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids",
                status=200,
                body=json.dumps({"code": -352, "data": {}}),
                headers={"Content-Type": "application/json"},
            )

            monitor = BiliLivePollMonitor(
                uid=789, on_live_start=AsyncMock(), poll_interval=999
            )
            async with aiohttp.ClientSession() as session:
                monitor._http = session
                # First get buvid3 cached
                await monitor._ensure_buvid3()
                assert monitor._buvid3 == "old-buvid3"

                result = await monitor._query_live_status()
                assert result is None
                # buvid3 should be cleared
                assert monitor._buvid3 == ""

                await monitor.stop()

    @pytest.mark.asyncio
    async def test_query_live_status_http_error_returns_none(self) -> None:
        """HTTP 500 or network error should return None."""
        from aioresponses import aioresponses

        with aioresponses() as mocked:
            mocked.get(
                "https://api.bilibili.com/x/frontend/finger/spi",
                status=200,
                body=_spi_response(),
                headers={"Content-Type": "application/json"},
            )
            mocked.post(
                "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids",
                status=500,
            )

            monitor = BiliLivePollMonitor(
                uid=789, on_live_start=AsyncMock(), poll_interval=999
            )
            async with aiohttp.ClientSession() as session:
                monitor._http = session
                result = await monitor._query_live_status()
                assert result is None

                await monitor.stop()


# ── Integration: run() loop with mocked HTTP ────────────────────


class TestRunLoop:
    """Test the main run loop with mocked responses."""

    @pytest.mark.asyncio
    async def test_run_loop_detects_live_start(self) -> None:
        """Run loop should detect live start and trigger callback.

        We simulate two API call sequences:
          1st poll: live_status=0 (not live) → was_live=None→False, no callback
          2nd poll: live_status=1 (live)     → was_live=False→True, callback fired
        """
        from aioresponses import aioresponses

        on_start = AsyncMock()
        on_end = AsyncMock()

        with aioresponses() as mocked:
            # Buvid3 (consumed once, cached)
            mocked.get(
                "https://api.bilibili.com/x/frontend/finger/spi",
                status=200,
                body=_spi_response(),
                headers={"Content-Type": "application/json"},
            )
            # First poll: offline (live_status=0)
            mocked.post(
                "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids",
                status=200,
                body=_live_response(
                    {
                        "42": _room_data(
                            live_status=0, uname="Eve", room_id=42, title="Offline"
                        )
                    }
                ),
                headers={"Content-Type": "application/json"},
            )
            # Second poll: online (live_status=1)
            mocked.post(
                "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids",
                status=200,
                body=_live_response(
                    {
                        "42": _room_data(
                            live_status=1, uname="Eve", room_id=42, title="Going Live!"
                        )
                    }
                ),
                headers={"Content-Type": "application/json"},
            )

            monitor = BiliLivePollMonitor(
                uid=42,
                on_live_start=on_start,
                on_live_end=on_end,
                poll_interval=0.05,  # type: ignore
            )

            # Run for a couple cycles then stop
            task = asyncio.create_task(monitor.run())
            await asyncio.sleep(0.15)
            await monitor.stop()
            await task

            # Should have detected the offline→online transition
            on_start.assert_awaited_once()
            args = on_start.await_args
            assert args is not None
            assert args[0][0] == "Eve"  # uname
            assert args[0][1] == 42  # room_id
