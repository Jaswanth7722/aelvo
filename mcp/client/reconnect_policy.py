"""ReconnectPolicy — configurable reconnection strategies with exponential backoff."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

log = logging.getLogger("aelvo.mcp.client.reconnect")


class ReconnectPolicy:
    """Configurable reconnection policy with exponential backoff and jitter.

    Supports:
    - Exponential backoff with configurable base
    - Random jitter to prevent thundering herd
    - Maximum delay cap
    - Maximum retry limit (None = unlimited)
    - Reset on successful connection
    """

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        max_retries: Optional[int] = 5,
        jitter: float = 0.1,
    ):
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._max_retries = max_retries
        self._jitter = jitter
        self._attempt = 0

    def reset(self) -> None:
        """Reset the retry counter (call after successful connection)."""
        self._attempt = 0

    def get_next_delay(self) -> float:
        """Calculate the next delay with exponential backoff and jitter.

        delay = min(max_delay, base_delay * 2^attempt) + random_jitter
        """
        self._attempt += 1
        import random
        delay = min(self._max_delay, self._base_delay * (2 ** (self._attempt - 1)))
        jitter_amount = delay * self._jitter * random.random()
        return delay + jitter_amount

    def can_retry(self) -> bool:
        """Check if another retry attempt is allowed."""
        if self._max_retries is None:
            return True
        return self._attempt < self._max_retries

    async def wait(self) -> None:
        """Wait for the next backoff period."""
        delay = self.get_next_delay()
        log.info("ReconnectPolicy: waiting %.1fs before retry %d", delay, self._attempt)
        await asyncio.sleep(delay)

    @property
    def attempt(self) -> int:
        return self._attempt

    @property
    def remaining(self) -> Optional[int]:
        if self._max_retries is None:
            return None
        return max(0, self._max_retries - self._attempt)
