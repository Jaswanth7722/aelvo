"""Tests for auth config module."""

import pytest
from auth.config import PROVIDER_REGISTRY, MODEL_REGISTRY
from auth.config import (
    get_provider,
    get_model,
    get_models_by_provider,
    get_models_by_family,
    get_providers_by_capability,
    get_models_by_capability,
    list_all_providers,
    list_all_models,
    get_local_providers,
    get_cloud_providers,
)
from auth.types import Capability, ModelFamily, ProviderKind


class TestProviderConfigs:
    def test_all_providers_configured(self):
        """Verify all expected providers have configurations."""
        expected_providers = [
            "openai", "anthropic", "google", "groq", "mistral",
            "cohere", "xai", "deepseek", "together", "fireworks",
            "perplexity", "openrouter", "huggingface", "ollama",
            "lm_studio", "vllm", "llama_cpp", "azure", "bedrock", "vertex",
        ]
        for pid in expected_providers:
            assert pid in PROVIDER_REGISTRY, f"Missing config for {pid}"
        assert len(PROVIDER_REGISTRY) == 20

    def test_openai_config(self):
        config = PROVIDER_REGISTRY["openai"]
        assert config.name == "OpenAI"
        assert config.kind == ProviderKind.FOUNDATION
        assert "api.openai.com" in (config.base_url or "")

    def test_anthropic_config(self):
        config = PROVIDER_REGISTRY["anthropic"]
        assert config.name == "Anthropic"
        assert config.kind == ProviderKind.FOUNDATION

    def test_ollama_config(self):
        config = PROVIDER_REGISTRY["ollama"]
        assert config.kind == ProviderKind.LOCAL
        assert config.local is True
        assert "localhost" in (config.base_url or "")

    def test_azure_config(self):
        config = PROVIDER_REGISTRY["azure"]
        assert config.kind == ProviderKind.FOUNDATION
        assert not config.local

    def test_config_properties(self):
        for pid, config in PROVIDER_REGISTRY.items():
            assert config.name
            assert config.kind
            assert config.default_model

    def test_local_providers(self):
        local = get_local_providers()
        assert "ollama" in local
        assert "lm_studio" in local
        assert "vllm" in local
        assert "llama_cpp" in local
        assert len(local) == 4


class TestModelRegistry:
    def test_model_count(self):
        assert len(MODEL_REGISTRY) >= 40  # At least 40 models across all providers

    def test_openai_models(self):
        models = get_models_by_provider("openai")
        model_ids = [m.id for m in models]
        assert "gpt-4o" in model_ids
        assert "gpt-4o-mini" in model_ids

    def test_get_model_by_id(self):
        model = get_model("gpt-4o")
        assert model is not None
        # gpt-4o is registered by both openai and azure; the last registration wins
        assert model.provider in ("openai", "azure")

    def test_models_by_family(self):
        reasoning = get_models_by_family(ModelFamily.REASONING)
        assert len(reasoning) >= 3  # o1, o3-mini, o4-mini, deepseek-reasoner, etc.

    def test_models_by_capability(self):
        vision_models = get_models_by_capability(Capability.VISION)
        assert len(vision_models) >= 3


class TestLookupHelpers:
    def test_get_provider(self):
        config = get_provider("openai")
        assert config is not None
        assert config.name == "OpenAI"

    def test_get_provider_case_insensitive(self):
        config = get_provider("OPENAI")
        assert config is not None

    def test_get_unknown_provider(self):
        config = get_provider("nonexistent")
        assert config is None

    def test_providers_by_capability(self):
        providers = get_providers_by_capability(Capability.STREAMING)
        assert "openai" in providers
        assert "anthropic" in providers

    def test_list_all_providers(self):
        providers = list_all_providers()
        assert len(providers) == 20
        assert "openai" in providers
        assert "vertex" in providers

    def test_list_all_models(self):
        models = list_all_models()
        assert "gpt-4o" in models

    def test_cloud_providers(self):
        cloud = get_cloud_providers()
        assert "openai" in cloud
        assert "anthropic" in cloud
        assert len(cloud) >= 15
