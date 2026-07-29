"""WebSocketTransport — persistent full-duplex transport for remote MCP servers."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncIterator, Dict, Optional
from .base_transport import BaseTransport, MCPMessage
from .stdio_transport import MCPConnectionError

log = logging.getLogger("aelvo.mcp.transport.websocket")


class WebSocketTransport(BaseTransport):
    """WebSocket-based transport for persistent MCP server connections.

    Supports full-duplex communication with connect/disconnect lifecycle,
    automatic reconnection, and message framing.
    """

    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None, max_retries: int = 3):
        self._url = url
        self._headers = headers or {}
        self._max_retries = max_retries
        self._ws: Optional[Any] = None
        self._connected = False
        self._read_buffer: asyncio.Queue = asyncio.Queue()
        self._reader_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        """Establish a WebSocket connection."""
        if self._connected:
            return

        try:
            import websockets
        except ImportError:
            raise MCPConnectionError("websockets package not installed. Install with: pip install websockets")

        log.info("WebSocketTransport: connecting to %s", self._url)

        # Attempt connection with retries
        last_error = None
        for attempt in range(self._max_retries):
            try:
                self._ws = await websockets.connect(
                    self._url,
                    extra_headers=self._headers,
                    ping_interval=30,
                    ping_timeout=10,
                )
                self._connected = True
                self._reader_task = asyncio.create_task(self._read_messages())
                log.info("WebSocketTransport: connected to %s", self._url)
                return
            except Exception as e:
                last_error = e
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    log.warning("WebSocketTransport: connection attempt %d failed, retrying", attempt + 1)

        raise MCPConnectionError(f"Failed to connect to {self._url}: {last_error}")

    async def disconnect(self) -> None:
        """Close the WebSocket connection."""
        if not self._connected:
            return

        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None

        if self._ws:
            await self._ws.close()

        self._connected = False
        self._ws = None
        log.info("WebSocketTransport: disconnected from %s", self._url)

    async def send(self, message: MCPMessage) -> None:
        """Send a JSON-RPC message over WebSocket."""
        if not self._connected or not self._ws:
            raise MCPConnectionError("Not connected")

        data = message.model_dump(exclude_none=True)
        await self._ws.send(json.dumps(data))

    async def receive(self) -> AsyncIterator[MCPMessage]:
        """Yield parsed JSON-RPC messages from WebSocket."""
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
        return self._connected and self._ws is not None and not self._ws.closed

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _read_messages(self) -> None:
        """Read messages from WebSocket and push to buffer."""
        if not self._ws:
            return

        try:
            async for raw in self._ws:
                if raw is None:
                    break
                try:
                    data = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode("utf-8"))
                    message = MCPMessage(**data)
                    await self._read_buffer.put(message)
                except (json.JSONDecodeError, Exception) as e:
                    log.warning("WebSocketTransport: invalid message: %s", e)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("WebSocketTransport: read error: %s", e)
