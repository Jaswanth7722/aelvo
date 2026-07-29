"""Runtime Session Manager — manages provider execution sessions."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional


logger = logging.getLogger(__name__)


@dataclass
class ExecutionSession:
    """A single execution session with a provider."""

    session_id: str
    provider_id: str
    model_id: str
    created_at: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)
    total_requests: int = 0
    total_tokens: int = 0
    total_cost: float = 0.0
    total_latency_ms: float = 0.0
    status: str = "active"  # active, idle, closed, error
    metadata: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    @property
    def age_seconds(self) -> float:
        return time.time() - self.created_at

    @property
    def idle_seconds(self) -> float:
        return time.time() - self.last_activity

    def record_request(
        self,
        tokens: int = 0,
        cost: float = 0.0,
        latency_ms: float = 0.0,
    ) -> None:
        self.total_requests += 1
        self.total_tokens += tokens
        self.total_cost += cost
        self.total_latency_ms += latency_ms
        self.last_activity = time.time()


class RuntimeSessionManager:
    """Manages execution sessions with lifecycle tracking.

    Each session tracks requests, tokens, costs, and latency
    for a specific provider+model combination.
    """

    def __init__(
        self,
        session_timeout: float = 3600.0,  # 1 hour idle timeout
        max_sessions: int = 100,
    ) -> None:
        self._sessions: dict[str, ExecutionSession] = {}
        self._session_timeout = session_timeout
        self._max_sessions = max_sessions
        self._cleanup_task: Optional[asyncio.Task[Any]] = None

    def create_session(
        self,
        provider_id: str,
        model_id: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> ExecutionSession:
        """Create a new execution session."""
        session_id = str(uuid.uuid4())
        session = ExecutionSession(
            session_id=session_id,
            provider_id=provider_id,
            model_id=model_id,
            metadata=metadata or {},
        )
        self._sessions[session_id] = session

        # Enforce max sessions
        if len(self._sessions) > self._max_sessions:
            self._evict_oldest()

        logger.debug(
            "Created session %s for %s/%s",
            session_id,
            provider_id,
            model_id,
        )
        return session

    def get_session(
        self, session_id: str
    ) -> Optional[ExecutionSession]:
        return self._sessions.get(session_id)

    def close_session(
        self, session_id: str, error: Optional[str] = None
    ) -> None:
        session = self._sessions.get(session_id)
        if session:
            session.status = "error" if error else "closed"
            session.error = error

    def record_request(
        self,
        session_id: str,
        tokens: int = 0,
        cost: float = 0.0,
        latency_ms: float = 0.0,
    ) -> None:
        session = self._sessions.get(session_id)
        if session:
            session.record_request(
                tokens=tokens, cost=cost, latency_ms=latency_ms
            )

    def get_active_sessions(
        self, provider_id: Optional[str] = None
    ) -> list[ExecutionSession]:
        sessions = [
            s
            for s in self._sessions.values()
            if s.status == "active"
        ]
        if provider_id:
            sessions = [
                s for s in sessions if s.provider_id == provider_id
            ]
        return sessions

    def get_idle_sessions(
        self, idle_threshold: float = 300.0
    ) -> list[ExecutionSession]:
        return [
            s
            for s in self._sessions.values()
            if s.status == "active"
            and s.idle_seconds > idle_threshold
        ]

    def _evict_oldest(self) -> None:
        """Evict the oldest closed/idle session when over capacity."""
        removable = [
            s
            for s in self._sessions.values()
            if s.status in ("closed", "idle")
        ]
        if removable:
            oldest = min(removable, key=lambda s: s.last_activity)
            self._sessions.pop(oldest.session_id, None)

    async def start_cleanup_loop(self) -> None:
        """Periodically clean up expired sessions."""

        async def _cleanup() -> None:
            while True:
                await asyncio.sleep(60)
                now = time.time()
                expired = [
                    sid
                    for sid, s in self._sessions.items()
                    if s.status == "active"
                    and (now - s.last_activity) > self._session_timeout
                ]
                for sid in expired:
                    session = self._sessions[sid]
                    session.status = "idle"
                    logger.debug(
                        "Session %s idle (timeout)", sid
                    )

        self._cleanup_task = asyncio.create_task(_cleanup())

    async def stop(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    def summary(self) -> dict[str, Any]:
        active = self.get_active_sessions()
        return {
            "total_sessions": len(self._sessions),
            "active_sessions": len(active),
            "idle_sessions": len(self.get_idle_sessions()),
            "total_requests": sum(
                s.total_requests for s in self._sessions.values()
            ),
            "total_tokens": sum(
                s.total_tokens for s in self._sessions.values()
            ),
            "total_cost": round(
                sum(s.total_cost for s in self._sessions.values()), 4
            ),
        }
