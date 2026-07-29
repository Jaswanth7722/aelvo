"""Credential refresh and token rotation for provider authentication."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import httpx

from ..types import AuthCredentials

logger = logging.getLogger(__name__)

try:
    from typing import Awaitable
except ImportError:
    from collections.abc import Awaitable  # type: ignore[assignment]

RefreshHandler = Callable[[str], Awaitable[Optional[AuthCredentials]]]


@dataclass
class RefreshConfig:
    """Configuration for credential refresh behavior."""

    provider_id: str
    refresh_url: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    extra_params: dict[str, str] = field(default_factory=dict)
    refresh_before_expiry: float = 300.0  # Refresh 5 min before expiry
    max_refresh_attempts: int = 3
    refresh_cooldown: float = 60.0  # Don't refresh more than once per 60s


class CredentialRefreshEngine:
    """Handles credential refresh and token rotation for all providers."""

    def __init__(self) -> None:
        self._handlers: dict[str, RefreshHandler] = {}
        self._custom_handlers: dict[str, RefreshHandler] = {}
        self._last_refresh: dict[str, float] = {}
        self._refresh_counts: dict[str, int] = {}
        self._configs: dict[str, RefreshConfig] = {}
        self._http = httpx.AsyncClient(timeout=30.0)

    def register_handler(
        self, provider_id: str, handler: RefreshHandler
    ) -> None:
        self._custom_handlers[provider_id] = handler

    def register_config(self, config: RefreshConfig) -> None:
        self._configs[config.provider_id] = config

    def register_default_handler(
        self, provider_id: str, config: Optional[RefreshConfig] = None
    ) -> None:
        """Register the default OAuth token refresh handler."""
        if config:
            self.register_config(config)
        self._handlers[provider_id] = self._default_refresh_handler

    async def refresh(self, provider_id: str) -> Optional[AuthCredentials]:
        """Refresh credentials for a provider."""
        config = self._configs.get(provider_id)

        # Check cooldown
        last = self._last_refresh.get(provider_id, 0.0)
        cooldown = config.refresh_cooldown if config else 60.0
        if time.time() - last < cooldown:
            logger.debug(
                "Refresh skipped for %s (cooldown active, %.1fs remaining)",
                provider_id,
                cooldown - (time.time() - last),
            )
            return None

        # Check max attempts
        count = self._refresh_counts.get(provider_id, 0)
        max_attempts = config.max_refresh_attempts if config else 3
        if count >= max_attempts:
            logger.warning(
                "Max refresh attempts (%d) reached for %s",
                max_attempts,
                provider_id,
            )
            raise RuntimeError(
                f"Credential refresh failed after {max_attempts} attempts for {provider_id}"
            )

        # Try custom handler first, then default
        handler = self._custom_handlers.get(provider_id) or self._handlers.get(provider_id)
        if handler is None:
            logger.warning("No refresh handler registered for %s", provider_id)
            return None

        try:
            self._last_refresh[provider_id] = time.time()
            self._refresh_counts[provider_id] = count + 1
            result = await handler(provider_id)
            if result:
                self._refresh_counts[provider_id] = 0  # Reset on success
            return result
        except Exception as e:
            logger.error(
                "Failed to refresh credentials for %s: %s",
                provider_id,
                e,
            )
            raise

    async def _default_refresh_handler(
        self, provider_id: str
    ) -> Optional[AuthCredentials]:
        """Default OAuth2 token refresh handler."""
        config = self._configs.get(provider_id)
        if not config or not config.refresh_url:
            logger.debug(
                "No refresh URL configured for %s", provider_id
            )
            return None

        # The actual refresh token must be provided externally
        # This is called by the auth provider with stored refresh tokens
        logger.info(
            "Default refresh handler called for %s (requires refresh token)",
            provider_id,
        )
        return None

    async def refresh_with_token(
        self,
        provider_id: str,
        refresh_token: str,
        config: Optional[RefreshConfig] = None,
    ) -> AuthCredentials:
        """Exchange a refresh token for new credentials."""
        cfg = config or self._configs.get(provider_id)
        if not cfg or not cfg.refresh_url:
            raise ValueError(
                f"Refresh URL not configured for {provider_id}"
            )

        data: dict[str, Any] = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": cfg.client_id or provider_id,
        }
        if cfg.client_secret:
            data["client_secret"] = cfg.client_secret
        data.update(cfg.extra_params)

        response = await self._http.post(cfg.refresh_url, data=data)
        response.raise_for_status()
        token_data = response.json()

        return AuthCredentials(
            provider_id=provider_id,
            api_key=token_data.get("access_token"),
            access_token=token_data.get("access_token"),
            refresh_token=token_data.get("refresh_token", refresh_token),
            expires_at=(
                time.time() + token_data["expires_in"]
                if "expires_in" in token_data
                else None
            ),
            scopes=token_data.get("scope", ""),
            metadata={"auth_type": "token_refresh"},
        )

    def needs_refresh(self, creds: AuthCredentials) -> bool:
        """Check if credentials need refresh."""
        if creds.expires_at is None:
            return False
        buffer = self._configs.get(
            creds.provider_id, RefreshConfig(provider_id=creds.provider_id)
        ).refresh_before_expiry
        return time.time() >= (creds.expires_at - buffer)

    def reset_count(self, provider_id: str) -> None:
        """Reset the refresh attempt counter for a provider."""
        self._refresh_counts[provider_id] = 0

    async def close(self) -> None:
        await self._http.aclose()
