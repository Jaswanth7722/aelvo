"""Streaming Normalization Layer — canonical streaming event models."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, AsyncIterator, Callable, Optional

from ..types import TokenUsage

logger = logging.getLogger(__name__)
log = logger


class StreamEventType(Enum):
    """Canonical stream event types."""
    TOKEN = auto()
    CHUNK = auto()
    DELTA = auto()
    DONE = auto()
    ERROR = auto()
    METADATA = auto()
    REASONING = auto()
    TOOL_CALL = auto()
    TOOL_RESULT = auto()
    INTERRUPT = auto()
    CANCELLED = auto()


@dataclass
class StreamEvent:
    """Canonical stream event emitted by all providers."""

    event_type: StreamEventType
    provider_id: str
    model_id: str
    content: str = ""
    finish_reason: Optional[str] = None
    usage: Optional[TokenUsage] = None
    error: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    sequence: int = 0


@dataclass
class StreamState:
    """Tracks state of an active stream."""

    provider_id: str
    model_id: str
    accumulated_content: str = ""
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    usage: Optional[TokenUsage] = None
    finish_reason: Optional[str] = None
    error: Optional[str] = None
    is_complete: bool = False
    is_cancelled: bool = False
    start_time: float = field(default_factory=time.time)
    event_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class StreamEventEmitter:
    """Emits canonical StreamEvents from any source."""

    def __init__(self) -> None:
        self._listeners: list[Callable[[StreamEvent], None]] = []
        self._state: Optional[StreamState] = None

    def add_listener(
        self, listener: Callable[[StreamEvent], None]
    ) -> None:
        self._listeners.append(listener)

    def remove_listener(
        self, listener: Callable[[StreamEvent], None]
    ) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    async def emit(self, event: StreamEvent) -> None:
        if self._state:
            self._state.event_count = event.sequence
            if event.event_type == StreamEventType.TOKEN:
                self._state.accumulated_content += event.content
            elif event.event_type == StreamEventType.TOOL_CALL:
                self._state.tool_calls.append(
                    json.loads(event.content) if event.content else {}
                )
            elif event.event_type == StreamEventType.DONE:
                self._state.is_complete = True
                self._state.finish_reason = event.finish_reason
                self._state.usage = event.usage
            elif event.event_type == StreamEventType.ERROR:
                self._state.error = event.error

        for listener in self._listeners:
            try:
                listener(event)
            except Exception as e:
                logger.error("Stream listener error: %s", e)

    def set_state(self, state: StreamState) -> None:
        self._state = state

    def clear_listeners(self) -> None:
        self._listeners.clear()


class StreamNormalizer:
    """Normalizes provider-specific streaming into canonical StreamEvents.

    Each provider has a normalizer function that converts its
    raw stream chunks into canonical StreamEvents.
    """

    def __init__(self) -> None:
        self._normalizers: dict[str, Callable[..., AsyncIterator[StreamEvent]]] = {}

    def register_normalizer(
        self,
        provider_id: str,
        normalizer: Callable[..., AsyncIterator[StreamEvent]],
    ) -> None:
        """Register a stream normalizer for a provider."""
        self._normalizers[provider_id] = normalizer

    async def normalize(
        self,
        provider_id: str,
        raw_stream: Any,
        model_id: str = "",
        **kwargs: Any,
    ) -> AsyncIterator[StreamEvent]:
        """Normalize a raw provider stream into canonical events."""
        normalizer = self._normalizers.get(provider_id)
        if normalizer is None:
            # Default pass-through normalizer
            async for event in self._default_normalizer(
                raw_stream, provider_id, model_id
            ):
                yield event
            return

        async for event in normalizer(raw_stream, provider_id, model_id, **kwargs):
            yield event

    async def _default_normalizer(
        self,
        raw_stream: Any,
        provider_id: str,
        model_id: str,
    ) -> AsyncIterator[StreamEvent]:
        """Default normalizer that treats each chunk as a token event."""
        seq = 0
        async for chunk in raw_stream:
            content = ""
            if isinstance(chunk, str):
                content = chunk
            elif isinstance(chunk, dict):
                content = chunk.get("content", chunk.get("text", json.dumps(chunk)))
            elif hasattr(chunk, "choices") and chunk.choices:
                delta = chunk.choices[0].delta
                content = getattr(delta, "content", "") or ""
            else:
                content = str(chunk)

            yield StreamEvent(
                event_type=StreamEventType.TOKEN,
                provider_id=provider_id,
                model_id=model_id,
                content=content,
                sequence=seq,
            )
            seq += 1

    def create_state(
        self, provider_id: str, model_id: str
    ) -> StreamState:
        return StreamState(provider_id=provider_id, model_id=model_id)


class StreamManager:
    """Manages multiple concurrent streams with cancellation support."""

    def __init__(self) -> None:
        self._streams: dict[str, StreamState] = {}
        self._emitters: dict[str, StreamEventEmitter] = {}
        self._tasks: dict[str, asyncio.Task[Any]] = {}

    def create_stream(
        self, provider_id: str, model_id: str
    ) -> tuple[str, StreamEventEmitter]:
        """Create a new stream and return (stream_id, emitter)."""
        import uuid

        state = StreamState(provider_id=provider_id, model_id=model_id)
        emitter = StreamEventEmitter()
        emitter.set_state(state)

        stream_id = str(uuid.uuid4())
        self._streams[stream_id] = state
        self._emitters[stream_id] = emitter
        return stream_id, emitter

    async def cancel(self, stream_id: str) -> None:
        """Cancel an active stream."""
        state = self._streams.get(stream_id)
        if state:
            state.is_cancelled = True

        task = self._tasks.get(stream_id)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError as _ex:
                log.warning("Silenced exception: %s", _ex)

        emitter = self._emitters.get(stream_id)
        if emitter:
            await emitter.emit(
                StreamEvent(
                    event_type=StreamEventType.CANCELLED,
                    provider_id=state.provider_id if state else "",
                    model_id=state.model_id if state else "",
                    content="Stream cancelled by user",
                )
            )

    def get_state(self, stream_id: str) -> Optional[StreamState]:
        return self._streams.get(stream_id)

    def get_emitter(self, stream_id: str) -> Optional[StreamEventEmitter]:
        return self._emitters.get(stream_id)

    async def close(self) -> None:
        for sid in list(self._streams.keys()):
            await self.cancel(sid)
        self._streams.clear()
        self._emitters.clear()
        self._tasks.clear()
