"""Tests for fallback routing."""

import pytest
from auth.runtime.fallback import FallbackRouter
from auth.runtime.health import ProviderHealthRuntime


class TestFallbackRouter:
    @pytest.fixture
    def router(self):
        health = ProviderHealthRuntime()
        health.record_success("openai", 100)
        health.record_success("anthropic", 150)
        health.record_success("google", 200)
        return FallbackRouter(health)

    def test_register_family_group(self, router):
        router.register_family_group("gpt-4o", ["openai", "azure"])
        # No assert needed — just verify no error

    def test_register_capability_group(self, router):
        router.register_capability_group("streaming", ["openai", "anthropic"])
        assert hasattr(router, '_provider_capabilities')

    def test_get_candidates_with_health(self, router):
        router.health.record_success("openai", 100)
        router.health.record_success("openai", 150)
        candidates = router._get_candidates("openai")
        assert isinstance(candidates, list)

    def test_viable_fallback(self, router):
        router.health.record_success("openai", 100)
        router.health.record_success("openai", 150)
        assert router._is_viable_fallback("openai")

    def test_not_viable_fallback(self, router):
        router.health.record_error("down-provider", "down")
        assert not router._is_viable_fallback("down-provider")

    def test_explain_routing(self, router):
        router.health.record_success("openai", 100)
        explanation = router.explain_routing("openai")
        assert "provider_id" in explanation
        assert "health" in explanation

    def test_get_decisions(self, router):
        decisions = router.get_decisions()
        assert isinstance(decisions, list)
