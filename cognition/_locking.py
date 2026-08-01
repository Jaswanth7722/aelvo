"""Shared thread-safety helpers for cognition state stores.

The cognition state stores (``CognitiveBlackboard``, ``CognitiveStateEngine``)
are synchronous classes whose shared state dicts are mutated from multiple
worker threads.  ``asyncio.Lock`` cannot be used here because the methods are
synchronous (``with lock:`` is not supported), so a reentrant
``threading.RLock`` is used instead.  Reentrancy is required because public
methods may call other decorated methods internally.
"""
from __future__ import annotations

from functools import wraps


def synchronized(method):
    """Serialize access to shared state guarded by ``self._lock``.

    The decorated method must belong to a class whose ``__init__`` creates a
    ``threading.RLock`` named ``self._lock``.
    """

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper
