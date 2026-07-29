"""Tests for streaming normalization."""

import pytest
from auth.runtime.streaming import (
    StreamEvent,
    StreamEventType,
    StreamState,
    StreamManager,
    StreamNormalizer,
    StreamEventEmitter,
)


class TestStreamEvent:
    def test_create_token_event(self):
        event = StreamEvent(
            event_type=StreamEventType.TOKEN,
            provider_id="openai",
            model_id="gpt-4o",
            content="Hello",
        )
        assert event.event_type == StreamEventType.TOKEN
        assert event.content == "Hello"
        assert event.provider_id == "openai"
        assert event.sequence == 0

    def test_create_done_event(self):
        event = StreamEvent(
            event_type=StreamEventType.DONE,
            provider_id="openai",
            model_id="gpt-4o",
            finish_reason="stop",
        )
        assert event.finish_reason == "stop"

    def test_create_error_event(self):
        event = StreamEvent(
            event_type=StreamEventType.ERROR,
            provider_id="test",
            model_id="test",
            error="Connection timeout",
        )
        assert event.error == "Connection timeout"


class TestStreamState:
    def test_accumulate_content(self):
        state = StreamState(provider_id="openai", model_id="gpt-4o")
        state.accumulated_content = "Hello"
        assert state.accumulated_content == "Hello"
        assert not state.is_complete

    def test_completion(self):
        state = StreamState(provider_id="openai", model_id="gpt-4o")
        state.is_complete = True
        state.finish_reason = "stop"
        assert state.is_complete
        assert state.finish_reason == "stop"

    def test_cancellation(self):
        state = StreamState(provider_id="openai", model_id="gpt-4o")
        state.is_cancelled = True
        assert state.is_cancelled


class TestStreamEventEmitter:
    @pytest.mark.asyncio
    async def test_emit_and_listen(self):
        emitter = StreamEventEmitter()
        received = []

        def listener(event):
            received.append(event)

        emitter.add_listener(listener)
        event = StreamEvent(
            event_type=StreamEventType.TOKEN,
            provider_id="test",
            model_id="test",
            content="Hello",
        )
        await emitter.emit(event)
        assert len(received) == 1
        assert received[0].content == "Hello"

    @pytest.mark.asyncio
    async def test_remove_listener(self):
        emitter = StreamEventEmitter()
        received = []

        def listener(event):
            received.append(event)

        emitter.add_listener(listener)
        emitter.remove_listener(listener)
        event = StreamEvent(
            event_type=StreamEventType.TOKEN,
            provider_id="test",
            model_id="test",
            content="test",
        )
        await emitter.emit(event)
        assert len(received) == 0


class TestStreamManager:
    @pytest.mark.asyncio
    async def test_create_stream(self):
        mgr = StreamManager()
        stream_id, emitter = mgr.create_stream("openai", "gpt-4o")
        assert stream_id is not None
        assert emitter is not None
        state = mgr.get_state(stream_id)
        assert state is not None
        assert state.provider_id == "openai"

    @pytest.mark.asyncio
    async def test_cancel_stream(self):
        mgr = StreamManager()
        stream_id, _ = mgr.create_stream("openai", "gpt-4o")
        await mgr.cancel(stream_id)
        state = mgr.get_state(stream_id)
        assert state.is_cancelled

    @pytest.mark.asyncio
    async def test_close_all(self):
        mgr = StreamManager()
        mgr.create_stream("openai", "gpt-4o")
        mgr.create_stream("anthropic", "claude-3")
        await mgr.close()
        assert len(mgr._streams) == 0


class TestStreamNormalizer:
    @pytest.mark.asyncio
    async def test_create_state(self):
        normalizer = StreamNormalizer()
        state = normalizer.create_state("openai", "gpt-4o")
        assert state.provider_id == "openai"
        assert state.model_id == "gpt-4o"
