"""Unit tests for the TTL-based system prompt caching in AelvoAgent.

Tests that get_system_prompt() calls are cached, regenerated on TTL expiry,
regenerated on anchor_hash mismatch, and tracked properly.
"""

import os
import pytest
import time
from unittest.mock import MagicMock, patch


class MockProviderConfig:
    """Minimal mock for provider configuration."""
    sdk = "openai"
    base_url = None


@pytest.fixture
def agent():
    """Create an AelvoAgent with fully mocked internals for testing."""
    from main import AelvoAgent

    config = MockProviderConfig()
    ag = AelvoAgent(
        api_key=os.environ.get("TEST_API_KEY", "sk-test-placeholder"),
        model="test-model",
        provider_name="test-provider",
        provider_config=config,
    )
    # Mock the API client to avoid actual network calls
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "mock response"
    ag.client = MagicMock()
    ag.client.chat.completions.create.return_value = mock_response
    return ag


class TestSystemPromptCaching:
    """Tests for the TTL-based system prompt caching."""

    def test_cache_used_within_ttl(self, agent):
        """Within TTL, repeated calls with the same hash use the cached prompt."""
        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "cached prompt"

            # First call — should call get_system_prompt
            agent._call_llm([{"role": "user", "content": "hello"}])
            assert mock_get.call_count == 1
            assert agent._cached_system_prompt == "cached prompt"
            assert hasattr(agent, "_cache_time")

            # Second call — same TTL window, no hash change → use cache
            agent._call_llm([{"role": "user", "content": "hello again"}])
            assert mock_get.call_count == 1, "Should NOT regenerate within TTL"

    def test_cache_regenerates_after_ttl(self, agent):
        """After TTL expires, the cache is regenerated."""
        agent.SYSTEM_PROMPT_CACHE_TTL = 0.1  # 100 ms

        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "fresh prompt"

            agent._call_llm([{"role": "user", "content": "hello"}])
            assert mock_get.call_count == 1

            time.sleep(0.15)  # Wait past TTL

            agent._call_llm([{"role": "user", "content": "hello again"}])
            assert mock_get.call_count == 2, "Should regenerate after TTL expiry"

    def test_cache_regenerates_on_hash_mismatch(self, agent):
        """A different anchor_hash bypasses the cache regardless of TTL."""
        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "fresh prompt"

            # First call — no last_context, hash is ""
            agent._call_llm([{"role": "user", "content": "hello"}])
            assert mock_get.call_count == 1

            # Second call with different anchor_hash
            agent.last_context = {"anchor_hash": "different_hash"}
            agent._call_llm([{"role": "user", "content": "hello"}])
            assert mock_get.call_count == 2, "Should regenerate on hash change"

    def test_cache_skipped_on_first_call(self, agent):
        """First call always regenerates because _cached_system_prompt doesn't exist."""
        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"

            assert not hasattr(agent, "_cached_system_prompt")
            agent._call_llm([{"role": "user", "content": "hello"}])

            assert mock_get.call_count == 1
            assert agent._cached_system_prompt == "prompt"
            # _cache_time should be set to a recent timestamp
            assert time.time() - agent._cache_time < 5

    def test_empty_hash_still_caches(self, agent):
        """Even with no anchor_hash, the cache is used on subsequent calls."""
        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"

            # First call — no last_context
            agent._call_llm([{"role": "user", "content": "hello"}])
            assert mock_get.call_count == 1

            # Second call — still no last_context, hash unchanged
            agent._call_llm([{"role": "user", "content": "world"}])
            assert mock_get.call_count == 1, "Should use cache with empty hash"

    def test_default_ttl_value(self, agent):
        """Default SYSTEM_PROMPT_CACHE_TTL should be 300 seconds."""
        assert agent.SYSTEM_PROMPT_CACHE_TTL == 300, \
            "Default TTL should be 5 minutes (300 seconds)"

    def test_ttl_resets_on_regen(self, agent):
        """After regeneration, the TTL clock resets so a new window begins."""
        agent.SYSTEM_PROMPT_CACHE_TTL = 0.1

        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt A"

            agent._call_llm([{"role": "user", "content": "first"}])
            t1 = agent._cache_time

            time.sleep(0.15)  # Expire first TTL

            mock_get.return_value = "prompt B"
            agent._call_llm([{"role": "user", "content": "second"}])
            assert mock_get.call_count == 2

            # After regeneration, cache_time should be newer
            assert agent._cache_time > t1, "Cache time should advance on regen"

            # Now within new TTL window — should NOT regenerate
            time.sleep(0.05)
            agent._call_llm([{"role": "user", "content": "third"}])
            assert mock_get.call_count == 2, "Should use freshly cached prompt"

    def test_cache_time_not_set_until_first_call(self, agent):
        """_cache_time should not exist until _call_llm is invoked."""
        assert not hasattr(agent, "_cache_time"), \
            "cache_time should not be set before first call"

    def test_hash_trumps_ttl_before_expiry(self, agent):
        """Hash mismatch should trigger regen even when TTL hasn't expired."""
        agent.SYSTEM_PROMPT_CACHE_TTL = 60  # Long TTL

        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt A"

            agent._call_llm([{"role": "user", "content": "first"}])
            assert mock_get.call_count == 1

            # Change hash immediately (within TTL)
            agent.last_context = {"anchor_hash": "new_hash"}
            agent._call_llm([{"role": "user", "content": "second"}])
            assert mock_get.call_count == 2, \
                "Should regen on hash change even within TTL"

    def test_timer_starts_at_first_call(self, agent):
        """The TTL timer starts at the first _call_llm invocation, not at init."""
        agent.SYSTEM_PROMPT_CACHE_TTL = 0.15

        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"

            # Start timer
            agent._call_llm([{"role": "user", "content": "first"}])
            timer_start = agent._cache_time

            # Within TTL
            time.sleep(0.05)
            agent._call_llm([{"role": "user", "content": "second"}])
            assert mock_get.call_count == 1

            # Still within TTL (0.05 + 0.05 = 0.10 < 0.15)
            time.sleep(0.05)
            agent._call_llm([{"role": "user", "content": "third"}])
            assert mock_get.call_count == 1

            # Past TTL (0.10 + 0.10 = 0.20 > 0.15)
            time.sleep(0.10)
            agent._call_llm([{"role": "user", "content": "fourth"}])
            assert mock_get.call_count == 2, "Should regen after TTL timer expires"

            # Verify the timer reset
            assert agent._cache_time > timer_start

    def test_long_running_conversation_stays_fresh(self, agent):
        """In a long conversation, the prompt is periodically refreshed."""
        agent.SYSTEM_PROMPT_CACHE_TTL = 0.1

        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"

            # Turn 1 — regen
            agent._call_llm([{"role": "user", "content": "turn 1"}])
            assert mock_get.call_count == 1

            time.sleep(0.12)  # Past TTL

            # Turn 2 — regen
            agent._call_llm([{"role": "user", "content": "turn 2"}])
            assert mock_get.call_count == 2

            time.sleep(0.12)  # Past TTL

            # Turn 3 — regen
            agent._call_llm([{"role": "user", "content": "turn 3"}])
            assert mock_get.call_count == 3

            # Turn 4 — within TTL of turn 3
            agent._call_llm([{"role": "user", "content": "turn 4"}])
            assert mock_get.call_count == 3, "Should cache within TTL window"
