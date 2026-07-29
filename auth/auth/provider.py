"""Provider auth orchestrator — unified entry point for all auth flows."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Optional

from ..types import AuthCredentials, ProviderAuthStatus, ProviderKind
from ..cred_storage import EncryptedCredentialStorage
from .api_key import APIKeyAuth
from .oauth import OAuthDeviceFlow, OAuthClientFlow
from .browser import BrowserAuthFlow, SessionAuth
from .session import SessionManager, SessionToken
from .local import LocalRuntimeAuth
from .cloud import AzureAuth, BedrockAuth, VertexAuth
from .refresh import CredentialRefreshEngine

logger = logging.getLogger(__name__)


@dataclass
class AuthProviderConfig:
    """Configuration for the auth provider orchestrator."""

    storage_path: str = "~/.aelvo/credentials.enc"
    encryption_key: str = ""
    auto_refresh: bool = True
    refresh_interval: float = 3600.0  # Check every hour
    validate_on_startup: bool = True


class ProviderAuthOrchestrator:
    """Unified entry point for all provider authentication flows.

    Combines API key, OAuth, browser, session, local, and cloud auth
    into a single orchestrator with credential storage and auto-refresh.
    """

    def __init__(self, config: Optional[AuthProviderConfig] = None) -> None:
        self.config = config or AuthProviderConfig()
        self.storage = EncryptedCredentialStorage(
            db_path=self.config.storage_path,
            passphrase=self.config.encryption_key,
        )
        self.sessions = SessionManager()
        self.refresh_engine = CredentialRefreshEngine()
        self._api_key_auth = APIKeyAuth(cred_store=self.storage)
        self._oauth_device_flows: dict[str, OAuthDeviceFlow] = {}
        self._oauth_client_flows: dict[str, OAuthClientFlow] = {}
        self._browser_flows: dict[str, BrowserAuthFlow] = {}
        self._local_auths: dict[str, LocalRuntimeAuth] = {}
        self._azure_auth: Optional[AzureAuth] = None
        self._bedrock_auth: Optional[BedrockAuth] = None
        self._vertex_auth: Optional[VertexAuth] = None
        self._session_auths: dict[str, SessionAuth] = {}
        self._refresh_task: Optional[asyncio.Task[Any]] = None

    # ── Registration ──────────────────────────────────────────────

    def register_api_key(self, provider_id: str, api_key: str) -> None:
        """Register an API key for a provider."""
        self._api_key_auth.store(provider_id, api_key)

    def register_oauth_device_flow(
        self, provider_id: str, flow: OAuthDeviceFlow
    ) -> None:
        self._oauth_device_flows[provider_id] = flow

    def register_oauth_client_flow(
        self, provider_id: str, flow: OAuthClientFlow
    ) -> None:
        self._oauth_client_flows[provider_id] = flow

    def register_browser_flow(
        self, provider_id: str, flow: BrowserAuthFlow
    ) -> None:
        self._browser_flows[provider_id] = flow

    def register_local_auth(
        self, provider_id: str, auth: LocalRuntimeAuth
    ) -> None:
        self._local_auths[provider_id] = auth

    def register_session_auth(
        self, provider_id: str, auth: SessionAuth
    ) -> None:
        self._session_auths[provider_id] = auth

    def set_azure_auth(self, auth: AzureAuth) -> None:
        self._azure_auth = auth

    def set_bedrock_auth(self, auth: BedrockAuth) -> None:
        self._bedrock_auth = auth

    def set_vertex_auth(self, auth: VertexAuth) -> None:
        self._vertex_auth = auth

    # ── Authentication ────────────────────────────────────────────

    async def authenticate(
        self, provider_id: str, provider_type: Optional[ProviderKind] = None
    ) -> AuthCredentials:
        """Authenticate with a provider using the best available method.

        Priority:
        1. Active session
        2. Encrypted storage
        3. Registered auth flow (OAuth, browser, API key, local, cloud)
        4. Environment variables
        """
        # 1. Check active session
        session = self.sessions.get(provider_id)
        if session is not None and not session.is_expired:
            logger.debug("Using active session for %s", provider_id)
            return AuthCredentials(
                provider_id=provider_id,
                api_key=session.access_token,
                access_token=session.access_token,
                refresh_token=session.refresh_token,
                expires_at=session.expires_at,
                metadata={"source": "active_session"},
            )

        # 2. Check encrypted storage
        stored = self.storage.get_for_provider(provider_id)
        if stored is not None:
            self.sessions.register(SessionToken(
                provider_id=stored.provider,
                access_token=stored.value,
                expires_at=stored.expires_at,
            ))
            return AuthCredentials(
                provider_id=provider_id,
                api_key=stored.value,
                metadata={"source": "encrypted_storage"},
            )

        # 3. Try registered auth flows
        creds = await self._try_registered_flows(provider_id, provider_type)
        if creds:
            self.sessions.register(SessionToken(
                provider_id=provider_id,
                access_token=creds.api_key or creds.access_token or "",
                refresh_token=creds.refresh_token,
                expires_at=creds.expires_at,
            ))
            return creds

        # 4. Try environment variables
        creds = await self._try_env_auth(provider_id)
        if creds:
            self.sessions.register(SessionToken(
                provider_id=provider_id,
                access_token=creds.api_key or "",
            ))
            return creds

        raise RuntimeError(
            f"No authentication method available for {provider_id}. "
            f"Register credentials via register_api_key() or set "
            f"{provider_id.upper()}_API_KEY environment variable."
        )

    async def _try_registered_flows(
        self, provider_id: str, provider_type: Optional[ProviderKind] = None
    ) -> Optional[AuthCredentials]:
        """Try all registered auth flows in priority order."""
        # Cloud auth
        if provider_id == "azure" and self._azure_auth:
            return await self._azure_auth.authenticate()
        if provider_id == "bedrock" and self._bedrock_auth:
            return await self._bedrock_auth.authenticate()
        if provider_id == "vertex" and self._vertex_auth:
            return await self._vertex_auth.authenticate()

        # Local auth
        if provider_id in self._local_auths:
            return await self._local_auths[provider_id].authenticate()

        # API key from store
        cred = self._api_key_auth.resolve_from_store(provider_id)
        if cred:
            return AuthCredentials(
                provider_id=provider_id,
                api_key=cred.value,
                metadata={"source": "registered_api_key"},
            )

        # OAuth device flow
        oauth_device = self._oauth_device_flows.get(provider_id)
        if oauth_device:
            # Simplified — in production, would start device flow and poll
            logger.info("OAuth device flow available for %s", provider_id)
            return None

        # OAuth client flow
        oauth_client = self._oauth_client_flows.get(provider_id)
        if oauth_client:
            logger.info("OAuth client flow available for %s", provider_id)
            return None

        # Browser flow
        browser = self._browser_flows.get(provider_id)
        if browser:
            device = await browser.start_device_flow()
            logger.info(
                "Browser auth needed for %s. Visit: %s",
                provider_id,
                device.get("verification_uri_complete", device.get("verification_uri", "")),
            )
            return await browser.poll_for_token(device.get("device_code", ""))

        # Session auth
        session_auth = self._session_auths.get(provider_id)
        if session_auth:
            return await session_auth.login("", "")

        return None

    async def _try_env_auth(
        self, provider_id: str
    ) -> Optional[AuthCredentials]:
        """Try to authenticate using environment variables."""
        import os

        # API key
        api_key = (
            os.environ.get(f"{provider_id.upper()}_API_KEY")
            or os.environ.get(f"{provider_id.upper()}_KEY")
            or os.environ.get(f"{provider_id.upper()}_TOKEN")
        )
        if api_key:
            return AuthCredentials(
                provider_id=provider_id,
                api_key=api_key,
                access_token=api_key,
                metadata={"source": "environment"},
            )

        return None

    # ── Session Management ────────────────────────────────────────

    def get_session(self, provider_id: str) -> Optional[SessionToken]:
        return self.sessions.get(provider_id)

    def list_sessions(self) -> dict[str, SessionToken]:
        return self.sessions.list_active()

    def invalidate_session(self, provider_id: str) -> None:
        self.sessions.invalidate(provider_id)

    # ── Status ────────────────────────────────────────────────────

    async def check_auth_status(
        self, provider_id: str
    ) -> ProviderAuthStatus:
        """Check the authentication status for a provider."""
        # Check session
        session_status = self.sessions.get_status(provider_id)
        if session_status.authenticated:
            return session_status

        # Check storage
        stored = self.storage.get_for_provider(provider_id)
        if stored is not None:
            return ProviderAuthStatus(
                provider_id=provider_id,
                authenticated=True,
                reason="Credentials available in storage",
                expires_at=stored.expires_at,
            )

        # Check registered flows
        try:
            creds = await self._try_registered_flows(provider_id)
            if creds:
                return ProviderAuthStatus(
                    provider_id=provider_id,
                    authenticated=True,
                    reason="Auth flow available",
                )
        except Exception as e:
            return ProviderAuthStatus(
                provider_id=provider_id,
                authenticated=False,
                reason=f"Auth flow failed: {e}",
            )

        return ProviderAuthStatus(
            provider_id=provider_id,
            authenticated=False,
            reason="No credentials available",
        )

    # ── Auto-Refresh ──────────────────────────────────────────────

    async def start_auto_refresh(self) -> None:
        """Start automatic credential refresh loop."""
        if self._refresh_task is not None:
            return

        async def _refresh_loop() -> None:
            while True:
                await asyncio.sleep(self.config.refresh_interval)
                for provider_id, session in self.sessions.list_active().items():
                    if (
                        session.refresh_token
                        and session.time_to_expiry < 3600
                    ):
                        try:
                            creds = await self.refresh_engine.refresh(
                                provider_id
                            )
                            if creds:
                                self.sessions.register(
                                    SessionToken(
                                        provider_id=provider_id,
                                        access_token=creds.api_key or creds.access_token or "",
                                        refresh_token=creds.refresh_token,
                                        expires_at=creds.expires_at,
                                    )
                                )
                        except Exception as e:
                            logger.warning(
                                "Auto-refresh failed for %s: %s",
                                provider_id,
                                e,
                            )

        self._refresh_task = asyncio.create_task(_refresh_loop())

    async def stop_auto_refresh(self) -> None:
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            self._refresh_task = None

    # ── Lifecycle ─────────────────────────────────────────────────

    async def close(self) -> None:
        """Close all auth resources."""
        await self.stop_auto_refresh()
        for device_flow in self._oauth_device_flows.values():
            await device_flow.close()
        for browser_flow in self._browser_flows.values():
            await browser_flow.close()
        for session in self._session_auths.values():
            await session.close()
        await self.refresh_engine.close()

    async def __aenter__(self) -> ProviderAuthOrchestrator:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
