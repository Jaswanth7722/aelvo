"""HttpTransport — HTTP/SSE transport for REST-style remote MCP servers.

Supports request/response pattern and Server-Sent Events for server-to-client streaming.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncIterator, Dict, Optional
from .base_transport import BaseTransport, MCPMessage
from .stdio_transport import MCPConnectionError

log = logging.getLogger("aelvo.mcp.transport.http")


class HttpTransport(BaseTransport):
    """HTTP-based transport for MCP servers that expose REST endpoints.

    Supports:
    - POST request/response for tool calls
    - SSE (Server-Sent Events) for server-to-client messages
    - Configurable headers and timeouts
    """

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout_ms: int = 30000,
        use_sse: bool = False,
        sse_url: Optional[str] = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._headers = headers or {}
        self._timeout_ms = timeout_ms
        self._use_sse = use_sse
        self._sse_url = sse_url or f"{self._base_url}/events"
        self._connected = False
        self._session: Optional[Any] = None
        self._read_buffer: asyncio.Queue = asyncio.Queue()
        self._sse_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        """Initialize the HTTP session (no persistent connection for HTTP)."""
        if self._connected:
            return

        try:
            import aiohttp
        except ImportError:
            raise MCPConnectionError("aiohttp package not installed. Install with: pip install aiohttp")

        self._session = aiohttp.ClientSession(
            headers={"Content-Type": "application/json", **self._headers},
            timeout=aiohttp.ClientTimeout(total=self._timeout_ms / 1000),
        )

        self._connected = True

        if self._use_sse:
            self._sse_task = asyncio.create_task(self._read_sse())

        log.info("HttpTransport: connected to %s", self._base_url)

    async def disconnect(self) -> None:
        """Close the HTTP session."""
        if not self._connected:
            return

        if self._sse_task:
            self._sse_task.cancel()
            try:
                await self._sse_task
            except asyncio.CancelledError:
                pass
            self._sse_task = None

        if self._session:
            await self._session.close()
            self._session = None

        self._connected = False
        log.info("HttpTransport: disconnected from %s", self._base_url)

    async def send(self, message: MCPMessage) -> None:
        """Send a JSON-RPC message via HTTP POST."""
        if not self._connected or not self._session:
            raise MCPConnectionError("Not connected")

        url = f"{self._base_url}/rpc"
        data = message.model_dump(exclude_none=True)

        try:
            async with self._session.post(url, json=data) as response:
                response.raise_for_status()
                result_data = await response.json()
                result_message = MCPMessage(
                    id=message.id,
                    result=result_data,
                )
                await self._read_buffer.put(result_message)
        except Exception as e:
            error_message = MCPMessage(
                id=message.id,
                error={"code": -1, "message": str(e)},
            )
            await self._read_buffer.put(error_message)
            raise MCPConnectionError(f"HTTP request failed: {e}")

    async def receive(self) -> AsyncIterator[MCPMessage]:
        """Yield parsed messages from the read buffer."""
        while self._connected or not self._read_buffer.empty():
            try:
                message = await asyncio.wait_for(self._read_buffer.get(), timeout=1.0)
                yield message
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    @property
    def is_connected(self) -> bool:
        return self._connected and self._session is not None and not self._session.closed

    # ------------------------------------------------------------------
    # SSE Support
    # ------------------------------------------------------------------

    async def _read_sse(self) -> None:
        """Read Server-Sent Events and push to buffer."""
        if not self._session:
            return

        try:
            async with self._session.get(self._sse_url) as response:
                async for line in response.content:
                    if line:
                        text = line.decode("utf-8").strip()
                        if text.startswith("data: "):
                            try:
                                data = json.loads(text[6:])
                                message = MCPMessage(**data)
                                await self._read_buffer.put(message)
                            except json.JSONDecodeError:
                                pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("HttpTransport: SSE read error: %s", e)
