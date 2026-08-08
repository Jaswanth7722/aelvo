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
def agent(monkeypatch):
    """Create an AelvoAgent with fully mocked internals for testing."""
    # Hermetic: never let a developer's AELVO_PROMPT_CACHE_TTL / AELVO_MAX_TOKENS
    # leak into tests that assume the default/instance values.
    monkeypatch.delenv("AELVO_PROMPT_CACHE_TTL", raising=False)
    monkeypatch.delenv("AELVO_MAX_TOKENS", raising=False)
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
    # Hermetic LLM cache: always miss, so every _call_llm reaches the mocked
    # client (the shared llm_cache.db would otherwise return cross-test hits
    # and make create() unreachable).
    ag._llm_cache = MagicMock()
    ag._llm_cache.get.return_value = None
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


class TestEnvVarTTL:
    """Tests for AELVO_PROMPT_CACHE_TTL env-var configuration."""

    def test_env_var_overrides_class_default(self, agent, monkeypatch):
        """A valid env var takes precedence over the class default TTL."""
        monkeypatch.setenv("AELVO_PROMPT_CACHE_TTL", "0.1")
        assert agent._resolve_prompt_cache_ttl() == 0.1

        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"

            # First call — regenerate
            agent._call_llm([{"role": "user", "content": "hello"}])
            assert mock_get.call_count == 1

            time.sleep(0.15)  # Past the 0.1s env TTL

            agent._call_llm([{"role": "user", "content": "hello again"}])
            assert mock_get.call_count == 2, \
                "Env-var TTL should force regeneration after expiry"

    def test_env_var_respected_within_ttl(self, agent, monkeypatch):
        """Within the env-configured TTL, the cache is still used."""
        monkeypatch.setenv("AELVO_PROMPT_CACHE_TTL", "0.2")

        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"

            agent._call_llm([{"role": "user", "content": "first"}])
            assert mock_get.call_count == 1

            # Well within the 0.2s window
            time.sleep(0.05)
            agent._call_llm([{"role": "user", "content": "second"}])
            assert mock_get.call_count == 1, "Should use cache within env TTL"

    def test_env_var_invalid_falls_back_to_class_var(self, agent, monkeypatch):
        """A non-numeric env var falls back to the class variable TTL."""
        monkeypatch.setenv("AELVO_PROMPT_CACHE_TTL", "not-a-number")

        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"

            # With default 300s, a quick second call must hit the cache
            agent._call_llm([{"role": "user", "content": "first"}])
            assert mock_get.call_count == 1
            agent._call_llm([{"role": "user", "content": "second"}])
            assert mock_get.call_count == 1, \
                "Invalid env var should fall back to default TTL (cache hit)"

    def test_env_var_non_positive_falls_back(self, agent, monkeypatch):
        """Zero/negative env values are rejected and fall back."""
        for bad in ("0", "-5", "0.0"):
            monkeypatch.setenv("AELVO_PROMPT_CACHE_TTL", bad)
            resolved = agent._resolve_prompt_cache_ttl()
            assert resolved == agent.SYSTEM_PROMPT_CACHE_TTL, \
                f"{bad!r} should fall back to class default"

    def test_env_var_non_finite_falls_back(self, agent, monkeypatch):
        """inf/nan/overflow env values are rejected and fall back."""
        for bad in ("inf", "-inf", "nan", "1e309"):
            monkeypatch.setenv("AELVO_PROMPT_CACHE_TTL", bad)
            resolved = agent._resolve_prompt_cache_ttl()
            assert resolved == agent.SYSTEM_PROMPT_CACHE_TTL, \
                f"{bad!r} should fall back to class default"

    def test_env_var_float_accepted(self, agent, monkeypatch):
        """Fractional (float) TTL values from env are honored."""
        monkeypatch.setenv("AELVO_PROMPT_CACHE_TTL", "0.05")
        assert agent._resolve_prompt_cache_ttl() == 0.05

    def test_env_var_int_accepted(self, agent, monkeypatch):
        """Integer TTL values from env are honored."""
        monkeypatch.setenv("AELVO_PROMPT_CACHE_TTL", "42")
        assert agent._resolve_prompt_cache_ttl() == 42.0

    def test_instance_attribute_still_works_without_env(self, agent):
        """With no env var, an instance-level TTL override is respected."""
        agent.SYSTEM_PROMPT_CACHE_TTL = 123
        assert agent._resolve_prompt_cache_ttl() == 123

    def test_class_default_is_fallback(self, agent, monkeypatch):
        """With no env var, the class variable (300s) is the fallback."""
        assert agent._resolve_prompt_cache_ttl() == 300

    def test_env_var_wins_over_instance_attribute(self, agent, monkeypatch):
        """A valid env var takes precedence over an instance override."""
        monkeypatch.setenv("AELVO_PROMPT_CACHE_TTL", "7")
        agent.SYSTEM_PROMPT_CACHE_TTL = 123
        assert agent._resolve_prompt_cache_ttl() == 7.0

    def test_env_var_regenerates_after_expiry(self, agent, monkeypatch):
        """End-to-end: env TTL expiry actually regenerates the prompt."""
        monkeypatch.setenv("AELVO_PROMPT_CACHE_TTL", "0.1")

        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "fresh"
            agent._call_llm([{"role": "user", "content": "first"}])
            assert mock_get.call_count == 1

            time.sleep(0.15)
            agent._call_llm([{"role": "user", "content": "second"}])
            assert mock_get.call_count == 2, \
                "Env-configured TTL should drive cache regeneration"


class TestPromptCacheMetrics:
    """Tests for system-prompt cache hit/miss metric tracking."""

    def test_metrics_start_at_zero(self, agent):
        """Counters begin at zero and no cache exists yet."""
        assert agent._prompt_cache_hits == 0
        assert agent._prompt_cache_misses == 0
        assert agent.prompt_cache_hit_rate() == 0.0
        stats = agent.prompt_cache_stats()
        assert stats["hits"] == 0
        assert stats["regenerations"] == 0
        assert stats["hit_rate"] == 0.0

    def test_first_call_counts_as_miss(self, agent):
        """First call is a regeneration (miss), reason 'first_call'."""
        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"
            agent._call_llm([{"role": "user", "content": "hello"}])

        stats = agent.prompt_cache_stats()
        assert stats["hits"] == 0
        assert stats["regenerations"] == 1
        assert stats["regen_reasons"]["first_call"] == 1

    def test_second_call_counts_as_hit(self, agent):
        """A cache reuse within TTL counts as a hit."""
        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"
            agent._call_llm([{"role": "user", "content": "hello"}])  # miss
            agent._call_llm([{"role": "user", "content": "hello again"}])  # hit

        stats = agent.prompt_cache_stats()
        assert stats["hits"] == 1
        assert stats["regenerations"] == 1
        assert stats["hit_rate"] == 0.5

    def test_hash_mismatch_tracks_reason(self, agent):
        """Hash change counts as a miss with reason 'hash_mismatch'."""
        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"
            agent._call_llm([{"role": "user", "content": "first"}])  # miss
            agent.last_context = {"anchor_hash": "changed"}
            agent._call_llm([{"role": "user", "content": "second"}])  # miss: hash

        stats = agent.prompt_cache_stats()
        assert stats["regenerations"] == 2
        assert stats["regen_reasons"]["hash_mismatch"] == 1

    def test_ttl_expiry_tracks_reason(self, agent):
        """TTL expiry counts as a miss with reason 'ttl_expired'."""
        agent.SYSTEM_PROMPT_CACHE_TTL = 0.1
        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"
            agent._call_llm([{"role": "user", "content": "first"}])  # miss
            time.sleep(0.15)
            agent._call_llm([{"role": "user", "content": "second"}])  # miss: ttl

        stats = agent.prompt_cache_stats()
        assert stats["regenerations"] == 2
        assert stats["regen_reasons"]["ttl_expired"] == 1

    def test_hit_rate_after_mixed_sequence(self, agent):
        """Hit rate reflects hits / (hits + misses)."""
        with patch("main.get_system_prompt") as mock_get:
            mock_get.return_value = "prompt"
            agent._call_llm([{"role": "user", "content": "a"}])  # miss
            agent._call_llm([{"role": "user", "content": "b"}])  # hit
            agent._call_llm([{"role": "user", "content": "c"}])  # hit
            agent.last_context = {"anchor_hash": "x"}
            agent._call_llm([{"role": "user", "content": "d"}])  # miss
            agent._call_llm([{"role": "user", "content": "e"}])  # hit

        stats = agent.prompt_cache_stats()
        assert stats["hits"] == 3
        assert stats["regenerations"] == 2
        assert stats["hit_rate"] == 0.6
        assert agent.prompt_cache_hit_rate() == 0.6

    def test_stats_are_a_snapshot(self, agent):
        """prompt_cache_stats returns a copy; mutating it doesn't affect state."""
        stats = agent.prompt_cache_stats()
        stats["hits"] = 999
        assert agent._prompt_cache_hits == 0

    def test_stats_regen_reasons_is_copied(self, agent):
        """The nested regen_reasons dict is copied, not shared."""
        stats = agent.prompt_cache_stats()
        stats["regen_reasons"]["first_call"] = 999
        assert agent._prompt_cache_miss_reasons["first_call"] == 0


class TestLazyHeavyImports:
    """chromadb/scrapy must not be imported at module load time — a module-level
    ``import chromadb`` costs ~2.7s and ``import scrapy`` ~0.5s of boot time.
    These guards keep ``import main`` fast (was 4.18s, now ~1.30s)."""

    def test_kernel_defers_chromadb_to_first_use(self, monkeypatch):
        """core.governance.kernel's sentinel stays unloaded until requested."""
        import core.governance.kernel as k
        # Order-independent: reset the sentinel, assert it stays None until the
        # loader is called, then restore whatever the rest of the suite set.
        saved = k._chromadb
        monkeypatch.setattr(k, "_chromadb", None)
        try:
            assert k._load_chromadb() is not None, "loader should load chromadb"
            assert k._chromadb is not None, "sentinel populated after load"
        finally:
            monkeypatch.setattr(k, "_chromadb", saved)

    def test_kernel_sentinel_not_loaded_at_import(self):
        """Importing the module alone must not populate the chromadb sentinel."""
        import core.governance.kernel as k
        # The sentinel only becomes non-None once _load_chromadb() runs, so
        # freshly re-importing the module (new interpreter state would be
        # cleanest, but we assert the invariant directly): the loader is the
        # ONLY writer.
        assert hasattr(k, "_load_chromadb")
        # Loading is the only way the sentinel changes; verify the loader is
        # idempotent and cached after the first call.
        first = k._load_chromadb()
        second = k._load_chromadb()
        assert first is second, "lazy loader must cache its result"

    def test_consensus_extended_defers_chromadb(self, monkeypatch):
        """cognition.consensus_extended defers chromadb to first use."""
        import cognition.consensus_extended as ce
        saved = ce._chromadb
        monkeypatch.setattr(ce, "_chromadb", None)
        try:
            assert ce._load_chromadb() is not None, "loader should load chromadb"
            assert ce._HAS_CHROMADB is True, "flag set after load"
        finally:
            monkeypatch.setattr(ce, "_chromadb", saved)
            monkeypatch.setattr(ce, "_HAS_CHROMADB", saved is not None)

    def test_web_scraping_defers_scrapy(self, monkeypatch):
        """core.scraping.web_scraping defers scrapy to first use."""
        from core.scraping import web_scraping as ws
        saved = ws._scrapy_mod
        monkeypatch.setattr(ws, "_scrapy_mod", None)
        try:
            s, c = ws._load_scrapy()
            assert s is not None and c is not None, "loader should load scrapy"
        finally:
            monkeypatch.setattr(ws, "_scrapy_mod", saved)

    def test_spider_rebasing_produces_valid_scrapy_spider(self):
        """The dynamic scrapy.Spider subclass works for heavy crawls."""
        from core.scraping.web_scraping import AelvoSpider, _load_scrapy
        scrapy, _ = _load_scrapy()
        Spider = type(
            "AelvoSpider",
            (scrapy.Spider,),
            {
                k: v for k, v in AelvoSpider.__dict__.items()
                if k not in ("__dict__", "__weakref__")
            },
        )
        assert issubclass(Spider, scrapy.Spider)
        s = Spider(target_url="http://example.com", result_queue=None)
        assert s.target_url == "http://example.com"
        reqs = list(s.start_requests())
        assert len(reqs) == 1 and reqs[0].url == "http://example.com"


class TestMaxTokensCapped:
    """LLM calls must send a bounded max_tokens so aggregators like OpenRouter
    don't default to the model's full output budget (e.g. 65536), which wastes
    credits and can trigger 402 payment errors on limited-balance accounts."""

    def test_openai_path_sends_max_tokens(self, agent):
        """The OpenAI-compatible path passes max_tokens=4096."""
        agent._call_llm([{"role": "user", "content": "hello"}])
        _, kwargs = agent.client.chat.completions.create.call_args
        assert kwargs.get("max_tokens") == 4096, (
            "max_tokens must be explicitly capped (got %r)" % kwargs.get("max_tokens")
        )

    def test_openai_max_tokens_is_bounded(self, agent):
        """The cap is finite and well below the 65536 full-budget default."""
        agent._call_llm([{"role": "user", "content": "hello"}])
        _, kwargs = agent.client.chat.completions.create.call_args
        mt = kwargs.get("max_tokens")
        assert isinstance(mt, int) and 0 < mt < 65536, f"Unexpected max_tokens: {mt!r}"

    def test_anthropic_path_sends_max_tokens(self, agent):
        """The Anthropic path also caps max_tokens (was already bounded)."""
        agent.sdk_type = "anthropic"
        agent.client = MagicMock()
        agent.client.messages.create.return_value = MagicMock(content=[MagicMock(text="ok")])
        agent._call_llm([{"role": "user", "content": "hello"}])
        _, kwargs = agent.client.messages.create.call_args
        mt = kwargs.get("max_tokens")
        assert isinstance(mt, int) and 0 < mt < 65536, f"Unexpected max_tokens: {mt!r}"

    def test_env_var_overrides_cap(self, agent, monkeypatch):
        """AELVO_MAX_TOKENS overrides the default output cap."""
        monkeypatch.setenv("AELVO_MAX_TOKENS", "8192")
        assert agent._resolve_max_output_tokens() == 8192
        agent._call_llm([{"role": "user", "content": "hello"}])
        _, kwargs = agent.client.chat.completions.create.call_args
        assert kwargs.get("max_tokens") == 8192

    def test_env_var_invalid_falls_back(self, agent, monkeypatch):
        """Invalid AELVO_MAX_TOKENS falls back to the class default."""
        for bad in ("0", "-3", "abc", ""):
            monkeypatch.setenv("AELVO_MAX_TOKENS", bad)
            assert agent._resolve_max_output_tokens() == 4096, f"{bad!r} should fall back"

    def test_default_cap_is_bounded(self, agent):
        """Default cap is 4096 unless overridden by env."""
        assert agent._resolve_max_output_tokens() == 4096
