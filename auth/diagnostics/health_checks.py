"""Health check routines — reusable health check functions for diagnostics."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class HealthCheckResult:
    """Result of a single health check."""

    name: str
    passed: bool
    duration_ms: float
    detail: str = ""
    error: Optional[str] = None


class HealthCheckRunner:
    """Runs configurable health checks and aggregates results.

    Automatically registers standard checks on init:
        - 'dns': resolves a hostname via socket.getaddrinfo
        - 'connectivity': checks if a URL is reachable via HTTP GET
    """

    def __init__(self) -> None:
        self._checks: dict[str, Callable[..., Any]] = {}
        self._check_results: dict[str, list[HealthCheckResult]] = {}

        # Register standard checks automatically
        self.register_check("connectivity", self._connectivity_check_fn)
        self.register_check("dns", self._dns_check_fn)

    async def _connectivity_check_fn(self, url: str, timeout: float = 5.0) -> bool:
        return await self.connectivity_check(url, timeout)

    async def _dns_check_fn(self, hostname: str) -> bool:
        return await self.dns_check(hostname)

    def register_provider_check(
        self,
        provider_id: str,
        base_url: Optional[str] = None,
        hostname: Optional[str] = None,
    ) -> None:
        """Register a composite health check for a specific provider.

        Creates a check named 'provider:{provider_id}' that runs DNS
        resolution and connectivity checks against the provider's endpoint.
        If base_url or hostname are not provided, they will be derived
        from the provider_id.

        Args:
            provider_id: The provider key (e.g. 'openai', 'anthropic').
            base_url: The provider's base API URL. If omitted, a default
                ``https://api.{provider_id}.com`` is used.
            hostname: Hostname for DNS resolution. If omitted, derived
                from base_url via urlparse.
        """
        if not base_url:
            base_url = f"https://api.{provider_id}.com"
        if not hostname:
            from urllib.parse import urlparse
            hostname = urlparse(base_url).hostname or f"api.{provider_id}.com"

        async def _provider_check() -> bool:
            """Run DNS + connectivity checks for this provider."""
            # DNS resolution
            dns_ok = await self.dns_check(hostname)
            if not dns_ok:
                return False
            # Connectivity check with short timeout
            conn_ok = await self.connectivity_check(base_url, timeout=5.0)
            return conn_ok

        self.register_check(f"provider:{provider_id}", _provider_check)

    def get_check_names(self) -> list[str]:
        """Return the names of all registered checks."""
        return list(self._checks.keys())

    def get_provider_check_name(self, provider_id: str) -> str:
        """Get the registered check name for a provider.

        Returns a string like 'provider:openai' that can be used with
        run_check() to run the composite health check for that provider.
        """
        return f"provider:{provider_id}"

    def create_provider_check_fn(
        self,
        provider_id: str,
        base_url: Optional[str] = None,
        hostname: Optional[str] = None,
    ) -> Callable[[], Any]:
        """Create a reusable async callable for monitoring a provider.

        This returns a zero-argument async function that runs the
        composite provider health check. It is designed to be passed
        to HealthCheckPolicy.check_fn so the proactive monitoring
        loop uses real connectivity checks instead of guessing endpoints.

        The check function is NOT registered — use register_provider_check()
        if you also want to access it via run_check().

        Args:
            provider_id: The provider key.
            base_url: The provider's base API URL.
            hostname: Hostname for DNS resolution.

        Returns:
            An async callable () -> bool.
        """
        if not base_url:
            base_url = f"https://api.{provider_id}.com"
        if not hostname:
            from urllib.parse import urlparse
            hostname = urlparse(base_url).hostname or f"api.{provider_id}.com"

        async def _check_fn() -> bool:
            dns_ok = await self.dns_check(hostname)
            if not dns_ok:
                return False
            conn_ok = await self.connectivity_check(base_url, timeout=5.0)
            return conn_ok

        return _check_fn

    def register_check(self, name: str, check_fn: Callable[..., Any]) -> None:
        self._checks[name] = check_fn

    async def run_check(
        self, name: str, *args: Any, **kwargs: Any
    ) -> HealthCheckResult:
        """Run a single health check."""
        check_fn = self._checks.get(name)
        if not check_fn:
            return HealthCheckResult(
                name=name,
                passed=False,
                duration_ms=0,
                error=f"No check registered: {name}",
            )

        start = time.time()
        try:
            result = await check_fn(*args, **kwargs)
            duration = (time.time() - start) * 1000
            if isinstance(result, bool):
                return HealthCheckResult(
                    name=name,
                    passed=result,
                    duration_ms=duration,
                )
            return HealthCheckResult(
                name=name,
                passed=True,
                duration_ms=duration,
                detail=str(result),
            )
        except Exception as e:
            duration = (time.time() - start) * 1000
            return HealthCheckResult(
                name=name,
                passed=False,
                duration_ms=duration,
                error=str(e),
            )

    async def run_all(self, *args: Any, **kwargs: Any) -> list[HealthCheckResult]:
        """Run all registered health checks."""
        results = await asyncio.gather(
            *[self.run_check(name, *args, **kwargs) for name in self._checks],
            return_exceptions=True,
        )
        return [
            r if isinstance(r, HealthCheckResult)
            else HealthCheckResult(name="unknown", passed=False, duration_ms=0, error=str(r))
            for r in results
        ]

    async def connectivity_check(self, url: str, timeout: float = 5.0) -> bool:
        """Check if a URL is reachable."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url)
                return response.status_code < 500
        except Exception:
            return False

    async def dns_check(self, hostname: str) -> bool:
        """Check if a hostname resolves."""
        try:
            import socket
            socket.getaddrinfo(hostname, 80)
            return True
        except Exception:
            return False
