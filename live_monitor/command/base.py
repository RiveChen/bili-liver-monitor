"""Base classes for the command framework."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..pusher.napcat import NapCatQQPusher

log = logging.getLogger("live_monitor")


@dataclass
class MessageContext:
    """Context object passed through the command pipeline."""

    # ── Raw event fields ──
    post_type: str = ""
    message_type: str = ""
    user_id: int = 0
    group_id: int | None = None
    message_id: int = 0
    target_id: int | None = None  # only present for message_sent

    # ── Derived fields ──
    raw_message: str = ""  # CQ string representation
    plain_text: str = ""  # text without CQ codes

    # ── Infrastructure ──
    bot_qq: str = ""
    pusher: NapCatQQPusher | None = None
    allowed_groups: set[int] | None = None
    session: object = None  # aiohttp session

    # ── Internal ──
    _matched_args: dict = field(default_factory=dict)

    def is_private(self) -> bool:
        return self.message_type == "private"

    def is_group(self) -> bool:
        return self.message_type == "group"

    def is_self_message(self) -> bool:
        return self.post_type == "message_sent"


class Command(ABC):
    """Abstract base for a chat command.

    Subclasses must implement:
      - match(ctx)   → bool : should this command handle this message?
      - execute(ctx) → None : perform the action
    """

    name: str = ""  # Human-readable name for logging

    @abstractmethod
    def match(self, ctx: MessageContext) -> bool:
        """Return True if this command should handle *ctx*."""
        ...

    @abstractmethod
    async def execute(self, ctx: MessageContext) -> None:
        """Execute the command logic asynchronously."""
        ...
