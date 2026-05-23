"""Monitor base class.

All data source listeners (Bilibili live, dynamic, Weibo, etc.)
inherit from this base class.
"""

import asyncio


class Monitor:
    """Base class for all monitors.

    Subclasses must implement:
        - async def run(self)
        - async def stop(self)

    Subclasses should set self.name for logging purposes.
    """

    name: str = "monitor"

    def __init__(self) -> None:
        self._running = False

    async def run(self) -> None:
        """Main loop. Called by Application via asyncio.create_task."""
        raise NotImplementedError

    async def stop(self) -> None:
        """Graceful shutdown."""
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    async def _sleep(self, seconds: float) -> None:
        """Sleep while respecting stop signal."""
        try:
            await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            self._running = False
            raise
