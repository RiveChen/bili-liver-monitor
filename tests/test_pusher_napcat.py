"""Tests for NapCatQQ pusher.

All tests use unittest.mock.patch to mock the HTTP layer instead of
aioresponses, since the pusher uses a pre-created aiohttp session.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from live_monitor.pusher.napcat import NapCatQQPusher


PRIVATE_URL = "http://127.0.0.1:3000/send_private_msg"
GROUP_URL = "http://127.0.0.1:3000/send_group_msg"


# ── Helpers ─────────────────────────────────────────────────────


def _mock_post(status: int = 200, body: str = "{}") -> AsyncMock:
    resp = AsyncMock()
    resp.status = status
    resp.text = AsyncMock(return_value=body)
    resp.__aenter__.return_value = resp
    return resp


# ── _do_send ────────────────────────────────────────────────────


class TestDoSend:
    """Low-level HTTP call to NapCatQQ API."""

    @pytest.mark.asyncio
    async def test_private_send_success(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000", user_ids=[10001]
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(200))
        with patch.object(pusher._session, "post", mock_call):
            result = await pusher._do_send("private", 10001, "Hello")
            assert result is True
            mock_call.assert_called_once()
            _, kwargs = mock_call.call_args
            assert kwargs["json"]["user_id"] == 10001
            assert kwargs["json"]["message"] == "Hello"

        await pusher.close()

    @pytest.mark.asyncio
    async def test_group_send_success(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000",
            user_ids=[],
            group_ids=[20001],
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(204))
        with patch.object(pusher._session, "post", mock_call):
            result = await pusher._do_send("group", 20001, "Hello Group")
            assert result is True
            mock_call.assert_called_once()
            _, kwargs = mock_call.call_args
            assert kwargs["json"]["group_id"] == 20001

        await pusher.close()

    @pytest.mark.asyncio
    async def test_http_error_returns_false(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000", user_ids=[10001]
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(404))
        with patch.object(pusher._session, "post", mock_call):
            result = await pusher._do_send("private", 10001, "Test")
            assert result is False

        await pusher.close()

    @pytest.mark.asyncio
    async def test_network_error_returns_false(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000", user_ids=[10001]
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(
            side_effect=aiohttp.ClientConnectionError("Connection refused")
        )
        with patch.object(pusher._session, "post", mock_call):
            result = await pusher._do_send("private", 10001, "Test")
            assert result is False

        await pusher.close()

    @pytest.mark.asyncio
    async def test_sends_auth_token_in_header(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000",
            user_ids=[10001],
            token="my-secret-token",
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(200))
        with patch.object(pusher._session, "post", mock_call):
            await pusher._do_send("private", 10001, "Test")
            _, kwargs = mock_call.call_args
            assert kwargs["headers"].get("Authorization") == "Bearer my-secret-token"

        await pusher.close()

    @pytest.mark.asyncio
    async def test_endpoint_url(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000",
            user_ids=[10001],
            group_ids=[20001],
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(200))
        with patch.object(pusher._session, "post", mock_call):
            await pusher._do_send("private", 10001, "msg")
            args, _ = mock_call.call_args
            assert args[0] == PRIVATE_URL

        mock_call = MagicMock(return_value=_mock_post(200))
        with patch.object(pusher._session, "post", mock_call):
            await pusher._do_send("group", 20001, "msg")
            args, _ = mock_call.call_args
            assert args[0] == GROUP_URL

        await pusher.close()


# ── push_live_start ─────────────────────────────────────────────


class TestPushLiveStart:
    @pytest.mark.asyncio
    async def test_sends_private_and_group(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000",
            user_ids=[10001, 10002],
            group_ids=[20001],
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(200))
        with patch.object(pusher._session, "post", mock_call):
            result = await pusher.push_live_start(
                uname="TestStreamer",
                room_id=777,
                room_title="Hello World",
                cover_url="https://example.com/cover.jpg",
            )
            assert result is True
            assert len(mock_call.call_args_list) == 3

        await pusher.close()

    @pytest.mark.asyncio
    async def test_sends_at_all_when_configured(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000",
            user_ids=[10001],
            group_ids=[20001],
            at_qq="all",
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(200))
        with patch.object(pusher._session, "post", mock_call):
            await pusher.push_live_start("Alice", 1, "", "")
            last_call = mock_call.call_args_list[-1]
            _, kwargs = last_call
            msg = kwargs["json"]["message"]
            assert "[CQ:at,qq=all]" in msg

        await pusher.close()

    @pytest.mark.asyncio
    async def test_returns_false_when_all_fail(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000",
            user_ids=[10001],
            group_ids=[20001],
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(500))
        with patch.object(pusher._session, "post", mock_call):
            result = await pusher.push_live_start("Test", 1, "", "")
            assert result is False

        await pusher.close()

    @pytest.mark.asyncio
    async def test_message_content(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000", user_ids=[10001]
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(200))
        with patch.object(pusher._session, "post", mock_call):
            await pusher.push_live_start(
                uname="Bob", room_id=42, room_title="My Stream", cover_url=""
            )
            _, kwargs = mock_call.call_args
            msg = kwargs["json"]["message"]
            assert "Bob" in msg
            assert "live.bilibili.com/42" in msg
            assert "My Stream" in msg

        await pusher.close()

    @pytest.mark.asyncio
    async def test_no_cover_url_no_title(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000", user_ids=[10001]
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(200))
        with patch.object(pusher._session, "post", mock_call):
            await pusher.push_live_start(
                uname="Bob", room_id=42, room_title="", cover_url=""
            )
            _, kwargs = mock_call.call_args
            msg = kwargs["json"]["message"]
            assert "Bob" in msg
            assert "live.bilibili.com/42" in msg
            assert " - " not in msg

        await pusher.close()


# ── push_live_end ───────────────────────────────────────────────


class TestPushLiveEnd:
    @pytest.mark.asyncio
    async def test_sends_offline_message(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000",
            user_ids=[10001, 10002],
            group_ids=[20001],
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(200))
        with patch.object(pusher._session, "post", mock_call):
            await pusher.push_live_end(uname="Charlie", room_id=55)
            assert len(mock_call.call_args_list) == 3
            _, kwargs = mock_call.call_args_list[0]
            msg = kwargs["json"]["message"]
            assert "Charlie" in msg
            assert "下播" in msg

        await pusher.close()


# ── push_notification ───────────────────────────────────────────


class TestPushNotification:
    @pytest.mark.asyncio
    async def test_sends_to_private_only(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000",
            user_ids=[10001, 10002],
            group_ids=[20001],
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(200))
        with patch.object(pusher._session, "post", mock_call):
            result = await pusher.push_notification("Title", "Message")
            assert result is True
            assert len(mock_call.call_args_list) == 2

        await pusher.close()

    @pytest.mark.asyncio
    async def test_message_without_body(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000", user_ids=[10001]
        )
        pusher._session = aiohttp.ClientSession()

        mock_call = MagicMock(return_value=_mock_post(200))
        with patch.object(pusher._session, "post", mock_call):
            await pusher.push_notification("JustTitle")
            _, kwargs = mock_call.call_args
            msg = kwargs["json"]["message"]
            assert msg == "JustTitle"

        await pusher.close()


# ── Session management ─────────────────────────────────────────


class TestSession:
    @pytest.mark.asyncio
    async def test_close_closes_session(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000", user_ids=[10001]
        )
        async with aiohttp.ClientSession() as session:
            pusher._session = session
            assert not pusher._session.closed
            await pusher.close()

    @pytest.mark.asyncio
    async def test_get_session_lazily_creates(self) -> None:
        pusher = NapCatQQPusher(
            api_url="http://127.0.0.1:3000", user_ids=[10001]
        )
        session = await pusher._get_session()
        assert session is not None
        assert not session.closed
        await pusher.close()

    @pytest.mark.asyncio
    async def test_no_users_no_groups_sends_nothing(self) -> None:
        empty = NapCatQQPusher(
            api_url="http://127.0.0.1:3000", user_ids=[], group_ids=[]
        )
        result = await empty.push_live_start("Test", 1)
        assert result is False
        await empty.close()
