"""Tests for runtime subsystems."""

import pytest
from auth.runtime.registry import ProviderRegistry
from auth.runtime.model_registry import ModelRegistry
from auth.runtime.capability import CapabilityRegistry
from auth.runtime.health import ProviderHealthRuntime
from auth.runtime.usage import UsageTracker, ProviderPricing
from auth.runtime.retry import RetryEngine, RetryConfig
from auth.types import (
    ProviderConfig,
    AuthConfig,
    AuthMethod,
    CredentialType,
    Capability,
    ModelFamily,
    ProviderKind,
    Usage,
)

# Minimal helpers for test data
def make_provider_info(pid, name="Test", ptype=ProviderKind.FOUNDATION):
    return type("ProviderInfo", (), {
        "provider_id": pid,
        "name": name,
        "provider_type": ptype,
        "description": "",
    })()

def make_provider_config(pid, api_key="test-key"):
    return ProviderConfig(
        name=pid,
        kind=ProviderKind.FOUNDATION,
        auth=AuthConfig(
            method=AuthMethod.API_KEY,
            credential_type=CredentialType.API_KEY,
        ),
    )

def make_provider_caps(pid, capabilities=None, families=None):
    caps = capabilities or set()
    families = families or set()
    return type("ProviderCapabilities", (), {
        "provider_id": pid,
        "capabilities": caps,
        "model_families": families,
        "max_context_length": None,
    })()

def make_model_info(mid, family=ModelFamily.INSTRUCTION, pid="test", cl=4096):
    caps = set()
    if family == ModelFamily.REASONING:
        caps.add(Capability.REASONING)
    if family == ModelFamily.MULTIMODAL:
        caps.add(Capability.VISION)
    return type("ModelInfo", (), {
        "model_id": mid,
        "family": family,
        "provider_id": pid,
        "context_length": cl,
        "capabilities": caps,
    })()


class TestProviderRegistry:
    @pytest.fixture
    def registry(self):
        return ProviderRegistry()

    def test_register_provider(self, registry):
        info = make_provider_info("test", "Test Provider")
        config = make_provider_config("test")
        caps = make_provider_caps("test", capabilities={Capability.STREAMING}, families={ModelFamily.INSTRUCTION})
        registry.register(info, config, caps)
        assert registry.has_provider("test")
        assert registry.count == 1

    def test_list_providers(self, registry):
        info1 = make_provider_info("p1", "P1", ProviderKind.FOUNDATION)
        config1 = make_provider_config("p1")
        caps1 = make_provider_caps("p1", capabilities={Capability.STREAMING}, families={ModelFamily.INSTRUCTION})
        registry.register(info1, config1, caps1)

        info2 = make_provider_info("p2", "P2", ProviderKind.LOCAL)
        config2 = make_provider_config("p2")
        caps2 = make_provider_caps("p2", capabilities={Capability.STREAMING}, families={ModelFamily.LOCAL_GGUF})
        registry.register(info2, config2, caps2)

        assert len(registry.list_providers()) == 2
        local_providers = registry.list_providers(provider_type=ProviderKind.LOCAL)
        assert len(local_providers) >= 1

    def test_find_by_capability(self, registry):
        info = make_provider_info("test")
        config = make_provider_config("test")
        caps = make_provider_caps("test", capabilities={Capability.STREAMING, Capability.TOOL_CALLING})
        registry.register(info, config, caps)
        streaming = registry.find_by_capability(Capability.STREAMING)
        assert "test" in streaming
        tool_call = registry.find_by_capability(Capability.TOOL_CALLING)
        assert "test" in tool_call

    def test_unregister_provider(self, registry):
        info = make_provider_info("test")
        config = make_provider_config("test")
        caps = make_provider_caps("test")
        registry.register(info, config, caps)
        registry.unregister("test")
        assert not registry.has_provider("test")

    def test_alias_resolution(self, registry):
        info = make_provider_info("openai", "OpenAI")
        config = make_provider_config("openai")
        caps = make_provider_caps("openai")
        registry.register(info, config, caps, aliases=["gpt"])
        assert registry.resolve_alias("gpt") == "openai"

    def test_get_config(self, registry):
        info = make_provider_info("test")
        config = make_provider_config("test")
        caps = make_provider_caps("test")
        registry.register(info, config, caps)
        retrieved = registry.get_config("test")
        assert retrieved is not None


class TestModelRegistry:
    @pytest.fixture
    def model_registry(self):
        return ModelRegistry()

    def test_register_model(self, model_registry):
        info = make_model_info("gpt-4o", ModelFamily.INSTRUCTION, "openai", 128000)
        model_registry.register(info, provider_ids=["openai"])
        assert model_registry.has_model("gpt-4o")

    def test_find_by_family(self, model_registry):
        info = make_model_info("test-model", ModelFamily.INSTRUCTION, "test")
        model_registry.register(info)
        assert "test-model" in model_registry.find_by_family(ModelFamily.INSTRUCTION)

    def test_find_by_provider(self, model_registry):
        info = make_model_info("test-model", ModelFamily.INSTRUCTION, "test")
        model_registry.register(info, provider_ids=["test"])
        assert "test-model" in model_registry.find_by_provider("test")

    def test_add_provider_for_model(self, model_registry):
        info = make_model_info("test-model", ModelFamily.INSTRUCTION, "test")
        model_registry.register(info)
        model_registry.add_provider_for_model("test-model", "azure")
        assert "azure" in model_registry.get_providers_for_model("test-model")


class TestCapabilityRegistry:
    @pytest.fixture
    def cap_registry(self):
        return CapabilityRegistry()

    def test_register_capability(self, cap_registry):
        cap_registry.register_capability(Capability.STREAMING, "Supports streaming")
        assert cap_registry.get_capability(Capability.STREAMING) is not None

    def test_providers_with_capability(self, cap_registry):
        caps = make_provider_caps("test", capabilities={Capability.STREAMING, Capability.TOOL_CALLING})
        cap_registry.register_provider_capabilities("test", caps)
        assert "test" in cap_registry.providers_with_capability(Capability.STREAMING)

    def test_providers_with_all(self, cap_registry):
        caps = make_provider_caps("test", capabilities={Capability.STREAMING, Capability.TOOL_CALLING})
        cap_registry.register_provider_capabilities("test", caps)
        assert "test" in cap_registry.providers_with_all(Capability.STREAMING, Capability.TOOL_CALLING)
        assert "test" not in cap_registry.providers_with_all(Capability.STREAMING, Capability.VISION)


class TestHealthRuntime:
    @pytest.fixture
    def health(self):
        return ProviderHealthRuntime()

    def test_record_success(self, health):
        health.record_success("openai", 150.0)
        assert health.is_healthy("openai")

    def test_record_error(self, health):
        health.record_error("openai", "Connection error")
        assert not health.is_healthy("openai")

    def test_uptime_percentage(self, health):
        health.record_success("openai", 100)
        health.record_success("openai", 150)
        health.record_error("openai", "error")
        assert health.uptime_percentage("openai") > 0.5

    def test_average_latency(self, health):
        health.record_success("openai", 100)
        health.record_success("openai", 200)
        assert health.average_latency("openai") == 150.0

    def test_get_recommendation(self, health):
        health.record_success("openai", 100)
        rec = health.get_recommendation("openai")
        assert "Recommended" in rec

    def test_rate_limit_recording(self, health):
        health.record_rate_limit("openai")
        assert not health.is_healthy("openai")
        assert health.get_status("openai").name == "RATE_LIMITED"


class TestUsageTracker:
    @pytest.fixture
    def tracker(self):
        return UsageTracker()

    def test_record_usage(self, tracker):
        usage = Usage(prompt_tokens=10, completion_tokens=20)
        tracker.record("openai", "gpt-4o", usage)
        assert tracker.total_tokens() == 30

    def test_total_cost(self, tracker):
        usage = Usage(prompt_tokens=100, completion_tokens=100)
        tracker.record("openai", "gpt-4o", usage)
        assert tracker.total_cost() == 0.0  # No pricing registered

    def test_cost_with_pricing(self, tracker):
        tracker.register_pricing(ProviderPricing(
            provider_id="openai", model_id="gpt-4o",
            input_cost_per_1k=0.01, output_cost_per_1k=0.03,
        ))
        usage = Usage(prompt_tokens=1000, completion_tokens=500)
        tracker.record("openai", "gpt-4o", usage)
        assert tracker.total_cost() > 0

    def test_filter_by_provider(self, tracker):
        tracker.record("openai", "gpt-4o", Usage(prompt_tokens=10, completion_tokens=10))
        tracker.record("anthropic", "claude-3", Usage(prompt_tokens=5, completion_tokens=5))
        assert tracker.total_tokens(provider_id="openai") == 20
        assert tracker.total_tokens(provider_id="anthropic") == 10

    def test_summary(self, tracker):
        tracker.record("openai", "gpt-4o", Usage(prompt_tokens=100, completion_tokens=50))
        summary = tracker.summary()
        assert summary["total_requests"] == 1
        assert summary["total_tokens"] == 150


class TestRetryEngine:
    @pytest.fixture
    def engine(self):
        return RetryEngine(RetryConfig(max_attempts=3, base_delay=0.01))

    @pytest.mark.asyncio
    async def test_retry_success(self, engine):
        call_count = 0

        async def flaky_op():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("temporarily unavailable - retry me")
            return "success"

        result = await engine.execute(flaky_op)
        assert result.success
        assert result.result == "success"
        assert result.attempts == 2

    @pytest.mark.asyncio
    async def test_retry_failure(self, engine):
        async def always_fails():
            raise ValueError("server error - keep failing")

        result = await engine.execute(always_fails)
        assert not result.success
        assert result.attempts == 3

    def test_should_retry(self, engine):
        assert engine.should_retry(ValueError("timeout"), 1)
        assert not engine.should_retry(ValueError("persistent"), 4)  # past max attempts

    def test_calculate_delay(self, engine):
        delay1 = engine.calculate_delay(1)
        delay2 = engine.calculate_delay(2)
        assert delay1 >= 0
        assert delay2 > delay1
