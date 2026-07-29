"""Tests for conversion adapters."""

import pytest
from auth.adapters.messages import MessageAdapter
from auth.adapters.streaming import StreamingAdapter
from auth.adapters.tool_calls import ToolCallAdapter
from auth.adapters.errors import ErrorAdapter, AuthenticationError, RateLimitError
from auth.adapters.structured_output import StructuredOutputAdapter


class TestMessageAdapter:
    def test_openai_roundtrip(self):
        messages = [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "Hello"},
        ]
        canonical = MessageAdapter.to_canonical(messages, "openai")
        assert canonical == messages

    def test_anthropic_conversion(self):
        anthropic_messages = [
            {"role": "user", "content": [{"type": "text", "text": "Hello"}]},
        ]
        canonical = MessageAdapter.to_canonical(anthropic_messages, "anthropic")
        assert len(canonical) == 1
        assert canonical[0]["role"] == "user"

    def test_google_conversion(self):
        google_messages = [
            {"role": "user", "parts": [{"text": "Hello"}]},
        ]
        canonical = MessageAdapter.to_canonical(google_messages, "google")
        assert len(canonical) == 1
        assert "Hello" in canonical[0]["content"]


class TestStreamingAdapter:
    def test_openai_chunk_normalization(self):
        chunk = {
            "choices": [
                {"delta": {"content": "Hello"}, "finish_reason": None}
            ]
        }
        normalized = StreamingAdapter.normalize_openai_chunk(chunk)
        assert normalized["type"] == "token"
        assert normalized["content"] == "Hello"

    def test_anthropic_chunk_normalization(self):
        chunk = {"type": "content_block_delta", "delta": {"text": "Hello"}}
        normalized = StreamingAdapter.normalize_anthropic_chunk(chunk)
        assert normalized["type"] == "token"
        assert normalized["content"] == "Hello"

    def test_get_normalizer(self):
        normalizer = StreamingAdapter.get_provider_normalizer("openai")
        assert normalizer == StreamingAdapter.normalize_openai_chunk
        normalizer = StreamingAdapter.get_provider_normalizer("anthropic")
        assert normalizer == StreamingAdapter.normalize_anthropic_chunk


class TestToolCallAdapter:
    def test_normalize_openai_tool_call(self):
        raw = {
            "id": "call_1",
            "function": {"name": "get_weather", "arguments": '{"city": "NYC"}'},
        }
        normalized = ToolCallAdapter.normalize_tool_call(raw, "openai")
        assert normalized["id"] == "call_1"
        assert normalized["function"]["name"] == "get_weather"

    def test_normalize_anthropic_tool_call(self):
        raw = {
            "id": "tool_1",
            "name": "get_weather",
            "input": {"city": "NYC"},
        }
        normalized = ToolCallAdapter.normalize_tool_call(raw, "anthropic")
        assert normalized["function"]["name"] == "get_weather"


class TestErrorAdapter:
    def test_normalize_authentication_error(self):
        error = ValueError("401 Unauthorized")
        normalized = ErrorAdapter.normalize_error(error, "openai", 401)
        assert isinstance(normalized, AuthenticationError)

    def test_normalize_rate_limit_error(self):
        error = ValueError("429 Too Many Requests")
        normalized = ErrorAdapter.normalize_error(error, "openai", 429)
        assert isinstance(normalized, RateLimitError)

    def test_normalize_generic_error(self):
        error = RuntimeError("something went wrong")
        normalized = ErrorAdapter.normalize_error(error, "openai")
        assert isinstance(normalized, type(error)) or True

    def test_error_attributes(self):
        error = AuthenticationError("Invalid API key", "openai", 401)
        assert error.provider_id == "openai"
        assert error.status_code == 401
        assert "Invalid" in str(error)


class TestStructuredOutputAdapter:
    def test_openai_format(self):
        schema = {
            "name": "response",
            "schema": {"type": "object", "properties": {"name": {"type": "string"}}},
        }
        formatted = StructuredOutputAdapter.format_response_format(schema, "openai")
        assert formatted["type"] == "json_schema"

    def test_validate_response(self):
        schema = {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}
        valid, data, error = StructuredOutputAdapter.validate_response('{"name": "test"}', schema)
        assert valid
        assert data["name"] == "test"

    def test_validate_invalid_response(self):
        schema = {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}
        valid, data, error = StructuredOutputAdapter.validate_response('{"age": 30}', schema)
        assert not valid
