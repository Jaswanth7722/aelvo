"""Abstract transport interface for MCP server communication.

All transport implementations must conform to this interface.
Transports handle the wire protocol — sending and receiving typed MCP messages.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, Optional
from datetime import datetime, timezone
from pydantic import BaseModel, Field


class MCPMessage(BaseModel):
    """A typed MCP protocol message."""
    id: str = ""
    method: str = ""
    params: Dict[str, Any] = Field(default_factory=dict)
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    jsonrpc: str = "2.0"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))


class BaseTransport(ABC):
    """Abstract base class for MCP transport implementations.

    Transports manage the lifecycle of a connection to an MCP server
    and provide a uniform interface for sending/receiving messages.
    """

    @abstractmethod
    async def connect(self) -> None:
        """Establish a connection to the MCP server."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully disconnect from the MCP server."""
        ...

    @abstractmethod
    async def send(self, message: MCPMessage) -> None:
        """Send a message to the MCP server."""
        ...

    @abstractmethod
    async def receive(self) -> AsyncIterator[MCPMessage]:
        """Receive messages from the MCP server as an async iterator."""
        ...

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the transport is currently connected."""
        ...

    async def __aenter__(self) -> "BaseTransport":
        await self.connect()
        return self

    async def __aexit__(self, *args) -> None:
        await self.disconnect()
