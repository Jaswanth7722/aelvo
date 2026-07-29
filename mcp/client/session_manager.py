"""SessionManager — tracks active MCP sessions, protocol state, and pending requests."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from pydantic import BaseModel, Field

from ..transport.base_transport import MCPMessage
from ..registry.models import CapabilityProfile

log = logging.getLogger("aelvo.mcp.client.session")


class SessionInfo(BaseModel):
    """Information about an active MCP session."""
    server_id: str
    protocol_version: str = "unknown"
    negotiated_capabilities: CapabilityProfile = Field(default_factory=lambda: CapabilityProfile(server_id=""))
    connected_at: datetime = Field(default_factory=datetime.utcnow)
    last_activity: datetime = Field(default_factory=datetime.utcnow)
    pending_requests: int = 0
    completed_requests: int = 0
    failed_requests: int = 0
    is_active: bool = True


class SessionManager:
    """Manages MCP session state for all connected servers.

    Tracks:
    - Protocol version per session
    - Negotiated capabilities
    - Pending and completed requests
    - Session health metrics
    """

    def __init__(self):
        self._sessions: Dict[str, SessionInfo] = {}
        self._pending_responses: Dict[str, asyncio.Future] = {}  # request_id -> Future

    # ------------------------------------------------------------------
    # Session Lifecycle
    # ------------------------------------------------------------------

    def create_session(self, server_id: str) -> SessionInfo:
        """Create a new session for a server."""
        session = SessionInfo(server_id=server_id)
        self._sessions[server_id] = session
        log.info("SessionManager: created session for '%s'", server_id)
        return session

    def close_session(self, server_id: str) -> Optional[SessionInfo]:
        """Close a session and cancel all pending requests."""
        session = self._sessions.pop(server_id, None)
        if session:
            session.is_active = False

            # Cancel all pending requests for this server
            to_cancel = [
                rid for rid, fut in self._pending_responses.items()
                if rid.startswith(f"{server_id}:")
            ]
            for rid in to_cancel:
                fut = self._pending_responses.pop(rid, None)
                if fut and not fut.done():
                    fut.cancel()

            log.info("SessionManager: closed session for '%s'", server_id)
        return session

    # ------------------------------------------------------------------
    # Request Tracking
    # ------------------------------------------------------------------

    def register_pending(self, request_id: str, server_id: str) -> asyncio.Future:
        """Register a pending request and return a Future for the response."""
        future = asyncio.get_event_loop().create_future()
        self._pending_responses[request_id] = future

        session = self._sessions.get(server_id)
        if session:
            session.pending_requests += 1
            session.last_activity = datetime.now(timezone.utc)

        return future

    def resolve_pending(self, request_id: str, result: Any) -> bool:
        """Resolve a pending request with a result."""
        future = self._pending_responses.pop(request_id, None)
        if future and not future.done():
            future.set_result(result)

            # Extract server_id from request_id (format: "server_id:req_id")
            server_id = request_id.split(":")[0] if ":" in request_id else ""
            session = self._sessions.get(server_id)
            if session:
                session.pending_requests = max(0, session.pending_requests - 1)
                session.completed_requests += 1
                session.last_activity = datetime.now(timezone.utc)

            return True
        return False

    def fail_pending(self, request_id: str, error: Exception) -> bool:
        """Fail a pending request with an error."""
        future = self._pending_responses.pop(request_id, None)
        if future and not future.done():
            future.set_exception(error)

            server_id = request_id.split(":")[0] if ":" in request_id else ""
            session = self._sessions.get(server_id)
            if session:
                session.pending_requests = max(0, session.pending_requests - 1)
                session.failed_requests += 1
                session.last_activity = datetime.now(timezone.utc)

            return True
        return False

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_session(self, server_id: str) -> Optional[SessionInfo]:
        """Get session info for a server."""
        return self._sessions.get(server_id)

    def list_sessions(self) -> List[SessionInfo]:
        """List all active sessions."""
        return [s for s in self._sessions.values() if s.is_active]

    def get_pending_count(self, server_id: str) -> int:
        """Get the number of pending requests for a server."""
        session = self._sessions.get(server_id)
        return session.pending_requests if session else 0

    @property
    def total_pending(self) -> int:
        return len(self._pending_responses)

    @property
    def active_session_count(self) -> int:
        return sum(1 for s in self._sessions.values() if s.is_active)
