"""Pusher base class.

All content pushers (NapCatQQ, etc.) inherit from this base class.
BarkPusher does NOT inherit from this — it's an alert-only channel,
not a content pusher.
"""


class Pusher:
    """Base class for content pushers.

    Subclasses should override push_* methods they support.
    Unsupported methods return True by default (silently skipped).
    """

    name: str = "pusher"

    async def push_live_start(
        self,
        uname: str,
        room_id: int,
        room_title: str = "",
        cover_url: str = "",
    ) -> bool:
        """Notify that a streamer started streaming."""
        return True

    async def push_live_end(
        self,
        uname: str,
        room_id: int,
    ) -> bool:
        """Notify that a streamer ended streaming."""
        return True

    async def push_dynamic(
        self,
        uname: str,
        dynamic_id: str,
        content: str,
        pic_url: str | None = None,
        dynamic_type: str = "",
        dynamic_time: str = "",
        dynamic_url: str = "",
        avatar_url: str | None = None,
    ) -> bool:
        """Notify that a streamer posted a new dynamic."""
        return True

    async def push_notification(
        self,
        title: str,
        message: str = "",
    ) -> bool:
        """Push a generic notification (startup/shutdown/alert)."""
        return True

    async def close(self) -> None:
        """Release resources (HTTP session, etc.)."""
