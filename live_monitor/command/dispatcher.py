"""Message dispatcher — routes incoming messages to matching commands."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import Command, MessageContext

log = logging.getLogger(__name__)


class MessageDispatcher:
    """Holds a list of Commands and dispatches messages to the first matching one.

    Usage::

        dispatcher = MessageDispatcher()
        dispatcher.register(SymmetricCommand(...))

        for msg in websocket_messages:
            ctx = build_context(msg)  # create a MessageContext
            await dispatcher.dispatch(ctx)
    """

    def __init__(self) -> None:
        self._commands: list[Command] = []

    def register(self, command: Command) -> None:
        """Register a Command instance."""
        self._commands.append(command)
        log.debug("Registered command: %s", command.name)

    async def dispatch(self, ctx: MessageContext) -> bool:
        """Run through registered commands, executing the first matching one.

        Args:
            ctx: The message context to check.

        Returns:
            True if a command matched and executed, False otherwise.
        """
        for cmd in self._commands:
            try:
                if cmd.match(ctx):
                    log.info(
                        "[CMD] %s matched user=%s type=%s msg_id=%d",
                        cmd.name,
                        ctx.user_id,
                        ctx.message_type,
                        ctx.message_id,
                    )
                    await cmd.execute(ctx)
                    return True
            except Exception as e:
                log.error(
                    "[CMD] %s error: %s user=%s msg_id=%d",
                    cmd.name,
                    e,
                    ctx.user_id,
                    ctx.message_id,
                    exc_info=True,
                )
        return False
