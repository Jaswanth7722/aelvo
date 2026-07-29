# oauth.py - OAuth Authentication Flows
"""OAuth device flow and client credentials authentication."""

from __future__ import annotations

import logging
import time
import webbrowser
import asyncio
from typing import Any, Dict, Optional

import httpx

from auth.types import Credential, CredentialType, ProviderConfig
from auth.cred_storage import CredentialStore

log = logging.getLogger("aelvo.auth.oauth")


class OAuthDeviceFlow:
    """OAuth 2.0 Device Authorization Flow (RFC 8628)."""

    def __init__(self, cred_store: CredentialStore):
        self.cred_store = cred_store
        self._client = httpx.AsyncClient(timeout=15.0)

    async def close(self) -> None:
        """Close the device flow client."""
        await self._client.aclose()

    async def start_device_flow(
        self,
        provider_config: ProviderConfig,
        client_id: str,
        scopes: Optional[list[str]] = None,
    ) -> Dict[str, Any]:
        """
        Start an OAuth device flow.

        Returns device code, user code, verification URI, and interval.
        The user must visit the verification URI and enter the user code.
        Call poll_for_token() with the device code to get the access token.
        """
        auth = provider_config.auth
        device_endpoint = auth.oauth_device_endpoint or f"{provider_config.base_url}/oauth/device"
        scopes = scopes or auth.oauth_scopes or ["openid", "profile", "email"]

        payload = {
            "client_id": client_id,
            "scope": " ".join(scopes),
        }

        try:
            resp = await self._client.post(device_endpoint, json=payload)
            resp.raise_for_status()
            data = resp.json()

            result = {
                "device_code": data["device_code"],
                "user_code": data["user_code"],
                "verification_uri": data.get("verification_uri", data.get("verification_url", "")),
                "interval": data.get("interval", 5),
                "expires_in": data.get("expires_in", 300),
            }

            log.info(
                f"OAuth device flow started for {provider_config.name}. "
                f"User code: {result['user_code']} at {result['verification_uri']}"
            )

            # Try to open browser automatically
            try:
                webbrowser.open(result["verification_uri"])
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)

            return result

        except httpx.HTTPError as e:
            log.error(f"OAuth device flow start failed for {provider_config.name}: {e}")
            raise RuntimeError(f"OAuth device flow failed: {e}")

    async def poll_for_token(
        self,
        provider_config: ProviderConfig,
        client_id: str,
        device_code: str,
        interval: int = 5,
        timeout: int = 300,
    ) -> Optional[Credential]:
        """
        Poll for OAuth token after device flow is authorized by user.
        Returns a Credential with the access token.

        Blocks until the user authorizes or timeout expires.
        """
        auth = provider_config.auth
        token_endpoint = auth.oauth_token_endpoint or f"{provider_config.base_url}/oauth/token"
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                resp = await self._client.post(
                    token_endpoint,
                    json={
                        "client_id": client_id,
                        "device_code": device_code,
                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    },
                )
                data = resp.json()

                if resp.status_code == 200:
                    access_token = data.get("access_token", "")
                    refresh_token = data.get("refresh_token", "")
                    expires_in = data.get("expires_in", 3600)

                    credential = Credential(
                        provider=provider_config.name.lower(),
                        credential_type=CredentialType.OAUTH_TOKEN,
                        value=access_token,
                        label=f"{provider_config.name} OAuth Token",
                        expires_at=time.time() + expires_in,
                        metadata={
                            "refresh_token": refresh_token,
                            "scopes": auth.oauth_scopes,
                            "provider": provider_config.name,
                        },
                    )

                    self.cred_store.store(credential)

                    # Store refresh token separately if present
                    if refresh_token:
                        refresh_cred = Credential(
                            provider=provider_config.name.lower(),
                            credential_type=CredentialType.REFRESH_TOKEN,
                            value=refresh_token,
                            label=f"{provider_config.name} Refresh Token",
                            metadata={
                                "access_token_id": credential.id,
                                "provider": provider_config.name,
                            },
                        )
                        self.cred_store.store(refresh_cred)

                    log.info(f"OAuth token obtained for {provider_config.name}")
                    return credential

                elif data.get("error") == "authorization_pending":
                    await asyncio.sleep(interval)
                    continue

                elif data.get("error") == "slow_down":
                    interval += 5
                    await asyncio.sleep(interval)
                    continue

                elif data.get("error") == "expired_token":
                    log.error("OAuth device flow expired. Restart device flow.")
                    return None

                else:
                    log.error(f"OAuth poll error: {data.get('error', 'unknown')}")
                    await asyncio.sleep(interval)

            except httpx.HTTPError as e:
                log.error(f"OAuth poll failed: {e}")
                await asyncio.sleep(interval)

        log.error(f"OAuth device flow timed out after {timeout}s")
        return None


class OAuthClientFlow:
    """OAuth 2.0 Client Credentials Flow."""

    def __init__(self, cred_store: CredentialStore):
        self.cred_store = cred_store
        self._client = httpx.AsyncClient(timeout=15.0)

    async def close(self) -> None:
        """Close the client flow client."""
        await self._client.aclose()

    async def get_client_credentials_token(
        self,
        provider_config: ProviderConfig,
        client_id: str,
        client_secret: str,
        scopes: Optional[list[str]] = None,
    ) -> Optional[Credential]:
        """Get an OAuth token using client credentials grant."""
        auth = provider_config.auth
        token_endpoint = auth.oauth_token_endpoint or f"{provider_config.base_url}/oauth/token"
        scopes = scopes or auth.oauth_scopes or []

        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        }
        if scopes:
            payload["scope"] = " ".join(scopes)

        try:
            resp = await self._client.post(token_endpoint, json=payload)
            resp.raise_for_status()
            data = resp.json()

            access_token = data.get("access_token", "")
            expires_in = data.get("expires_in", 3600)

            credential = Credential(
                provider=provider_config.name.lower(),
                credential_type=CredentialType.OAUTH_TOKEN,
                value=access_token,
                label=f"{provider_config.name} Client OAuth Token",
                expires_at=time.time() + expires_in,
                metadata={
                    "grant_type": "client_credentials",
                    "scopes": scopes,
                },
            )

            self.cred_store.store(credential)
            log.info(f"OAuth client credentials token obtained for {provider_config.name}")
            return credential

        except httpx.HTTPError as e:
            log.error(f"OAuth client credentials failed for {provider_config.name}: {e}")
            return None
