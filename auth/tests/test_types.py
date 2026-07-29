"""Tests for auth types module."""

import pytest
import time
from auth.types import (
    AuthCredentials,
    ProviderAuthStatus,
    ProviderStatus,
    ProviderKind,
    Capability,
    ModelFamily,
    Usage,
    Credential,
    CredentialType,
    AuthConfig,
    AuthMethod,
    ProviderConfig,
    ModelConfig,
    FinishReason,
    StreamEvent,
    StreamEventType,
    HealthStatus,
    RoutingDecision,
    RoutingStrategy,
    ProviderError,
)


class TestAuthCredentials:
    def test_create_credentials(self):
        creds = AuthCredentials(
            provider_id="openai",
            api_key="sk-test-123",
        )
        assert creds.provider_id == "openai"
        assert creds.api_key == "sk-test-123"
        assert creds.metadata == {}

    def test_with_access_token(self):
        creds = AuthCredentials(
            provider_id="openai",
            access_token="tok_abc123",
            refresh_token="ref_xyz789",
            expires_at=time.time() + 3600,
        )
        assert creds.access_token == "tok_abc123"
        assert creds.refresh_token == "ref_xyz789"
        assert creds.expires_at is not None

    def test_with_scopes(self):
        creds = AuthCredentials(
            provider_id="openai",
            api_key="sk-test",
            scopes="openid profile email",
        )
        assert creds.scopes == "openid profile email"
        assert creds.token_type == "bearer"


class TestProviderAuthStatus:
    def test_authenticated_status(self):
        status = ProviderAuthStatus(
            provider_id="openai",
            authenticated=True,
            reason="API key valid",
        )
        assert status.authenticated
        assert status.provider_id == "openai"

    def test_unauthenticated_status(self):
        status = ProviderAuthStatus(
            provider_id="openai",
            authenticated=False,
            reason="No API key",
        )
        assert not status.authenticated


class TestUsage:
    def test_create_usage(self):
        usage = Usage(prompt_tokens=10, completion_tokens=20)
        assert usage.prompt_tokens == 10
        assert usage.completion_tokens == 20
        assert usage.total_tokens == 30

    def test_with_cost(self):
        usage = Usage(prompt_tokens=1000, completion_tokens=500, cost_usd=0.05)
        assert usage.cost_usd == 0.05


class TestCredential:
    def test_create_credential(self):
        cred = Credential(
            provider="openai",
            credential_type=CredentialType.API_KEY,
            value="sk-test-key-12345",
        )
        assert cred.provider == "openai"
        assert cred.credential_type == CredentialType.API_KEY
        assert cred.value == "sk-test-key-12345"
        assert cred.is_valid is True

    def test_credential_validates_non_empty(self):
        with pytest.raises(Exception):
            Credential(
                provider="openai",
                credential_type=CredentialType.API_KEY,
                value="",
            )


class TestProviderConfig:
    def test_minimal_config(self):
        config = ProviderConfig(
            name="Test",
            kind=ProviderKind.FOUNDATION,
            auth=AuthConfig(
                method=AuthMethod.API_KEY,
                credential_type=CredentialType.API_KEY,
            ),
        )
        assert config.name == "Test"
        assert config.kind == ProviderKind.FOUNDATION
        assert config.models == []
        assert config.max_context_length == 128000


class TestModelConfig:
    def test_minimal_model(self):
        model = ModelConfig(
            id="test-model",
            provider="test",
            family=ModelFamily.INSTRUCTION,
        )
        assert model.id == "test-model"
        assert model.family == ModelFamily.INSTRUCTION
        assert model.context_window == 128000


class TestStreamEvent:
    def test_create_chunk_event(self):
        event = StreamEvent(
            type=StreamEventType.CHUNK,
            content="Hello",
        )
        assert event.type == StreamEventType.CHUNK
        assert event.content == "Hello"

    def test_create_done_event(self):
        event = StreamEvent(
            type=StreamEventType.DONE,
            finish_reason=FinishReason.STOP,
        )
        assert event.finish_reason == FinishReason.STOP


class TestHealthStatus:
    def test_default_unknown(self):
        health = HealthStatus(provider="test")
        assert health.status == ProviderStatus.UNKNOWN
        assert health.auth_valid is True

    def test_healthy_status(self):
        health = HealthStatus(
            provider="test",
            status=ProviderStatus.ACTIVE,
            latency_p50_ms=150.0,
        )
        assert health.status == ProviderStatus.ACTIVE
        assert health.latency_p50_ms == 150.0


class TestRoutingDecision:
    def test_routing(self):
        decision = RoutingDecision(
            strategy=RoutingStrategy.PRIMARY,
            selected_provider="openai",
            selected_model="gpt-4o",
            reason="Primary provider available",
        )
        assert decision.strategy == RoutingStrategy.PRIMARY
        assert decision.selected_provider == "openai"


class TestProviderKind:
    def test_kind_values(self):
        assert ProviderKind.FOUNDATION.value == "foundation"
        assert ProviderKind.LOCAL.value == "local"
        assert ProviderKind.EMBEDDING.value == "embedding"


class TestCapability:
    def test_capability_values(self):
        assert Capability.STREAMING.value == "streaming"
        assert Capability.TOOL_CALLING.value == "tool_calling"
        assert Capability.VISION.value == "vision"


class TestModelFamily:
    def test_family_values(self):
        assert ModelFamily.INSTRUCTION.value == "instruction"
        assert ModelFamily.REASONING.value == "reasoning"
        assert ModelFamily.CODING.value == "coding"


class TestProviderError:
    def test_error_creation(self):
        err = ProviderError(
            provider="openai",
            model="gpt-4o",
            error_type="rate_limit",
            message="Too many requests",
            status_code=429,
            retryable=True,
        )
        assert err.provider == "openai"
        assert err.error_type == "rate_limit"
        assert err.status_code == 429
        assert err.retryable is True
