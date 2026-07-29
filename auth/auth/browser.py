"""Browser-based login and session authentication for providers."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

from ..types import AuthCredentials

logger = logging.getLogger(__name__)


@dataclass
class BrowserAuthConfig:
    """Configuration for browser-based authentication."""

    provider_id: str
    auth_url: str
    token_url: str
    client_id: str
    redirect_uri: str = "http://localhost:0/callback"
    scopes: list[str] = field(default_factory=list)
    extra_params: dict[str, str] = field(default_factory=dict)
    timeout_seconds: float = 300.0
    poll_interval: float = 2.0


class BrowserAuthFlow:
    """Handles browser-based login flows (OAuth PKCE, device code, etc.)."""

    def __init__(self, config: BrowserAuthConfig) -> None:
        self.config = config
        self._http = httpx.AsyncClient(timeout=30.0)
        self._pending_tokens: dict[str, Any] = {}

    async def start_device_flow(self) -> dict[str, Any]:
        """Start a device authorization flow.

        Returns device code, user code, and verification URL.
        """
        response = await self._http.post(
            self.config.auth_url,
            data={
                "client_id": self.config.client_id,
                "scope": " ".join(self.config.scopes),
                **self.config.extra_params,
            },
        )
        response.raise_for_status()
        data = response.json()
        self._pending_tokens[data["device_code"]] = {"started_at": time.time()}
        return data

    async def poll_for_token(
        self, device_code: str, interval: float = 2.0, timeout: float = 300.0
    ) -> AuthCredentials:
        """Poll the token endpoint until the user completes the browser flow."""
        start = time.time()
        last_error: Optional[str] = None

        while time.time() - start < timeout:
            try:
                response = await self._http.post(
                    self.config.token_url,
                    data={
                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                        "device_code": device_code,
                        "client_id": self.config.client_id,
                    },
                )
                data = response.json()

                if "access_token" in data:
                    creds = AuthCredentials(
                        provider_id=self.config.provider_id,
                        api_key=data.get("access_token"),
                        access_token=data.get("access_token"),
                        refresh_token=data.get("refresh_token"),
                        expires_at=(
                            time.time() + data["expires_in"]
                            if "expires_in" in data
                            else None
                        ),
                        scopes=data.get("scope", " ".join(self.config.scopes)),
                        metadata={"auth_type": "browser_device_flow"},
                    )
                    self._pending_tokens.pop(device_code, None)
                    return creds

                if data.get("error") == "authorization_pending":
                    await asyncio.sleep(interval)
                    continue
                if data.get("error") == "slow_down":
                    await asyncio.sleep(interval + 1.0)
                    continue
                if data.get("error"):
                    last_error = data["error"]
                    raise RuntimeError(
                        f"Device flow error: {data['error']}: {data.get('error_description', '')}"
                    )

            except httpx.HTTPError as e:
                last_error = str(e)
                await asyncio.sleep(interval)

        raise TimeoutError(
            f"Browser auth flow timed out after {timeout}s. "
            f"Last error: {last_error}"
        )

    async def exchange_code(
        self, code: str, code_verifier: Optional[str] = None
    ) -> AuthCredentials:
        """Exchange an authorization code for tokens."""
        data: dict[str, Any] = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
        }
        if code_verifier:
            data["code_verifier"] = code_verifier
        data.update(self.config.extra_params)

        response = await self._http.post(self.config.token_url, data=data)
        response.raise_for_status()
        token_data = response.json()

        return AuthCredentials(
            provider_id=self.config.provider_id,
            api_key=token_data.get("access_token"),
            access_token=token_data.get("access_token"),
            refresh_token=token_data.get("refresh_token"),
            id_token=token_data.get("id_token"),
            expires_at=(
                time.time() + token_data["expires_in"]
                if "expires_in" in token_data
                else None
            ),
            scopes=token_data.get("scope", " ".join(self.config.scopes)),
            metadata={"auth_type": "browser_code_exchange"},
        )

    async def close(self) -> None:
        await self._http.aclose()


@dataclass
class SessionAuthConfig:
    """Configuration for session-based authentication."""

    provider_id: str
    login_url: str
    session_cookie_name: str = "session"
    extra_headers: dict[str, str] = field(default_factory=dict)


class SessionAuth:
    """Manages session-based auth for providers (browser login cookies)."""

    def __init__(self, config: SessionAuthConfig) -> None:
        self.config = config
        self._http = httpx.AsyncClient(timeout=30.0)

    async def login(
        self, username: str, password: str
    ) -> AuthCredentials:
        """Authenticate with username/password and capture session."""
        response = await self._http.post(
            self.config.login_url,
            data={"username": username, "password": password},
            headers=self.config.extra_headers,
        )
        response.raise_for_status()

        session_token = response.cookies.get(self.config.session_cookie_name)
        if not session_token:
            # Try to find it in response body
            try:
                data = response.json()
                session_token = data.get("token") or data.get(
                    "session_token"
                ) or data.get("session")
            except (json.JSONDecodeError, KeyError):
                pass

        return AuthCredentials(
            provider_id=self.config.provider_id,
            api_key=session_token,
            session_token=session_token,
            metadata={
                "auth_type": "session",
                "cookies": dict(response.cookies),
            },
        )

    async def close(self) -> None:
        await self._http.aclose()
