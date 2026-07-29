"""Provider session token management and persistence."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from ..types import AuthCredentials, ProviderAuthStatus

logger = logging.getLogger(__name__)


@dataclass
class SessionToken:
    """A stored provider session with metadata."""

    provider_id: str
    access_token: str
    refresh_token: Optional[str] = None
    expires_at: Optional[float] = None
    token_type: str = "bearer"
    scopes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    last_used_at: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() >= self.expires_at

    @property
    def is_stale(self) -> bool:
        """Token hasn't been used in over an hour."""
        return time.time() - self.last_used_at > 3600

    @property
    def time_to_expiry(self) -> float:
        if self.expires_at is None:
            return float("inf")
        return max(0.0, self.expires_at - time.time())

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at,
            "token_type": self.token_type,
            "scopes": self.scopes,
            "metadata": self.metadata,
            "created_at": self.created_at,
            "last_used_at": self.last_used_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SessionToken:
        return cls(**data)

    @classmethod
    def from_credentials(
        cls, creds: AuthCredentials
    ) -> SessionToken:
        return cls(
            provider_id=creds.provider_id,
            access_token=creds.access_token or creds.api_key or "",
            refresh_token=creds.refresh_token,
            expires_at=creds.expires_at,
            scopes=creds.scopes.split() if creds.scopes else [],
            metadata=creds.metadata or {},
        )


class SessionManager:
    """Manages active provider sessions with lifecycle tracking."""

    def __init__(self) -> None:
        self._sessions: dict[str, SessionToken] = {}
        self._history: dict[str, list[SessionToken]] = {}
        self._max_history: int = 10

    def register(self, token: SessionToken) -> None:
        """Register a session token for a provider."""
        provider_id = token.provider_id
        self._sessions[provider_id] = token
        if provider_id not in self._history:
            self._history[provider_id] = []
        self._history[provider_id].append(token)
        # Trim history
        if len(self._history[provider_id]) > self._max_history:
            self._history[provider_id] = self._history[provider_id][
                -self._max_history:
            ]

    def get(self, provider_id: str) -> Optional[SessionToken]:
        """Get the active session token for a provider."""
        token = self._sessions.get(provider_id)
        if token is None:
            return None
        token.last_used_at = time.time()
        return token

    def invalidate(self, provider_id: str) -> None:
        """Invalidate the active session for a provider."""
        self._sessions.pop(provider_id, None)

    def refresh_needed(self, provider_id: str) -> bool:
        """Check if a provider's token needs refresh."""
        token = self._sessions.get(provider_id)
        if token is None:
            return True
        return token.is_expired or token.time_to_expiry < 300  # 5 min buffer

    def list_active(self) -> dict[str, SessionToken]:
        """List all active sessions."""
        return dict(self._sessions)

    def list_history(self, provider_id: str) -> list[SessionToken]:
        """Get session history for a provider."""
        return list(self._history.get(provider_id, []))

    def get_status(self, provider_id: str) -> ProviderAuthStatus:
        """Get detailed auth status for a provider."""
        token = self._sessions.get(provider_id)
        if token is None:
            return ProviderAuthStatus(
                provider_id=provider_id,
                authenticated=False,
                reason="No active session",
            )
        return ProviderAuthStatus(
            provider_id=provider_id,
            authenticated=True,
            expires_at=token.expires_at,
            reason=(
                "Session active"
                if not token.is_expired
                else "Session expired"
            ),
        )

    def clear(self) -> None:
        """Clear all sessions."""
        self._sessions.clear()

    def to_dict(self) -> dict[str, Any]:
        return {
            pid: token.to_dict()
            for pid, token in self._sessions.items()
        }

    def from_dict(self, data: dict[str, Any]) -> None:
        for pid, token_data in data.items():
            self._sessions[pid] = SessionToken.from_dict(token_data)
