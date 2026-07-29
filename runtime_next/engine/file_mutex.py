import asyncio
from typing import Set, List

class FileMutex:
    """Manages file-level locks to prevent concurrent writes to the same files."""
    def __init__(self):
        self._locked: Set[str] = set()
        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition(lock=self._lock)

    async def acquire(self, paths: List[str]):
        async with self._cond:
            while any(p in self._locked for p in paths):
                await self._cond.wait()
            for p in paths:
                self._locked.add(p)

    async def release(self, paths: List[str]):
        async with self._cond:
            for p in paths:
                self._locked.discard(p)
            self._cond.notify_all()
