"""Local runtime authentication for local providers."""

from __future__ import annotations

import json
import logging
import os
import platform
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from ..types import AuthCredentials, ProviderAuthStatus

logger = logging.getLogger(__name__)


@dataclass
class LocalAuthConfig:
    """Configuration for local runtime authentication."""

    provider_id: str
    base_url: str = ""
    trust_mode: bool = True
    require_health_check: bool = True
    health_endpoint: str = "/health"
    allowed_origins: list[str] = field(default_factory=lambda: ["localhost", "127.0.0.1"])
    allowed_ports: list[int] = field(default_factory=lambda: [11434, 1234, 8000, 8080])


class LocalRuntimeAuth:
    """Handles authentication for local provider runtimes.

    Local providers (Ollama, LM Studio, llama.cpp, vLLM)
    typically run on localhost and may not require traditional auth.
    This manages trust boundaries and optional local credentials.
    """

    def __init__(self, config: LocalAuthConfig) -> None:
        self.config = config

    async def check_running(self) -> bool:
        """Check if the local runtime is actually running."""
        if not self.config.base_url:
            return False
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(
                    f"{self.config.base_url.rstrip('/')}/{self.config.health_endpoint.lstrip('/')}"
                )
                return response.status_code < 500
        except Exception:
            return False

    async def authenticate(self) -> AuthCredentials:
        """Authenticate with a local runtime.

        In trust mode, returns a trust-based credential.
        Otherwise checks for local API keys or env vars.
        """
        if self.config.trust_mode:
            return AuthCredentials(
                provider_id=self.config.provider_id,
                api_key="__trusted_local__",
                metadata={
                    "auth_type": "local_trust",
                    "hostname": platform.node(),
                    "base_url": self.config.base_url,
                },
            )

        # Try to find credentials in environment or config files
        api_key = self._find_local_credential()
        return AuthCredentials(
            provider_id=self.config.provider_id,
            api_key=api_key or "",
            metadata={
                "auth_type": "local_credential",
                "found_in_env": api_key is not None,
            },
        )

    def _find_local_credential(self) -> Optional[str]:
        """Search for local credentials in env vars and config files."""
        env_vars = [
            f"{self.config.provider_id.upper()}_API_KEY",
            f"{self.config.provider_id.upper()}_TOKEN",
            f"{self.config.provider_id.upper()}_KEY",
        ]
        for var in env_vars:
            value = os.environ.get(var)
            if value:
                return value

        # Check local config files
        config_dirs = []
        if platform.system() == "Windows":
            config_dirs.append(Path(os.environ.get("APPDATA", "")) / self.config.provider_id)
        else:
            config_dirs.append(
                Path.home() / ".config" / self.config.provider_id.lower()
            )
        config_dirs.append(Path.cwd() / ".provider" / self.config.provider_id)

        for config_dir in config_dirs:
            config_file = config_dir / "credentials.json"
            if config_file.exists():
                try:
                    data = json.loads(config_file.read_text())
                    return data.get("api_key") or data.get("token")
                except (json.JSONDecodeError, OSError):
                    continue

        return None

    async def ensure_authenticated(self) -> ProviderAuthStatus:
        """Verify local runtime is accessible and authenticated."""
        if self.config.require_health_check:
            running = await self.check_running()
            if not running:
                return ProviderAuthStatus(
                    provider_id=self.config.provider_id,
                    authenticated=False,
                    reason=f"Local runtime not reachable at {self.config.base_url}",
                )

        creds = await self.authenticate()
        return ProviderAuthStatus(
            provider_id=self.config.provider_id,
            authenticated=bool(creds.api_key),
            reason="Local runtime available" if creds.api_key else "No local credentials",
        )

    def validate_origin(self, url: str) -> bool:
        """Check if a URL is within the allowed local origins."""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        hostname = parsed.hostname or ""
        port = parsed.port

        if hostname not in self.config.allowed_origins and not hostname.endswith(".local"):
            return False
        if port is not None and port not in self.config.allowed_ports:
            return False
        return True
