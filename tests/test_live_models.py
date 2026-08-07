"""Tests for cli.live_models — fetching fresh model lists from provider APIs."""

import httpx
import pytest

import cli.live_models as lm
from core.registry.models import get_provider_config


# ── fake httpx transport ─────────────────────────────────────────────────────

class FakeResponse:
    def __init__(self, payload=None, status=200, exc=None):
        self._payload = payload
        self.status = status
        self._exc = exc

    def raise_for_status(self):
        if self._exc is not None:
            raise self._exc
        if self.status >= 400:
            raise httpx.HTTPStatusError(
                f"HTTP {self.status}", request=None, response=self
            )

    def json(self):
        return self._payload


class FakeClient:
    """Context-manager httpx.Client stand-in; records every request."""

    instances = []

    def __init__(self, *args, **kwargs):
        self.requests = []
        self._responses = []
        FakeClient.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        response = self._responses.pop(0) if len(self._responses) > 1 else self._responses[0]
        return response


@pytest.fixture
def fake_http(monkeypatch):
    """Patch httpx.Client with a FakeClient seeded with one response."""
    FakeClient.instances = []

    def _seed(payload=None, status=200, exc=None):
        client = FakeClient()
        client._responses.append(FakeResponse(payload=payload, status=status, exc=exc))
        monkeypatch.setattr(lm.httpx, "Client", lambda *a, **k: client)
        return client

    yield _seed
    FakeClient.instances = []


@pytest.fixture(autouse=True)
def _clean_cache():
    lm.clear_cache()
    yield
    lm.clear_cache()


# ── endpoint construction ────────────────────────────────────────────────────

def test_openai_compatible_uses_bearer_and_default_base(fake_http):
    cfg = get_provider_config("openai")
    client = fake_http(
        {"data": [{"id": "gpt-5"}, {"id": "text-embedding-3-large"}, {"id": "gpt-5-mini"}]}
    )
    models = lm.fetch_live_models("openai", cfg, "sk-live", cache=False)

    assert models == ["gpt-5", "gpt-5-mini"]  # embeddings filtered out
    url, kwargs = client.requests[0]
    assert url == "https://api.openai.com/v1/models"  # default base_url
    assert kwargs["headers"]["Authorization"] == "Bearer sk-live"


def test_provider_with_custom_base_url_uses_it(fake_http):
    cfg = get_provider_config("groq")
    client = fake_http({"data": [{"id": "llama-3.3-70b-versatile"}]})
    lm.fetch_live_models("groq", cfg, "key", cache=False)

    url, _ = client.requests[0]
    assert url == "https://api.groq.com/openai/v1/models"


def test_anthropic_endpoint_and_headers(fake_http):
    cfg = get_provider_config("anthropic")
    client = fake_http({"data": [{"id": "claude-sonnet-4-20250514"}]})
    models = lm.fetch_live_models("anthropic", cfg, "sk-ant-123", cache=False)

    assert models == ["claude-sonnet-4-20250514"]
    url, kwargs = client.requests[0]
    assert url == "https://api.anthropic.com/v1/models"
    assert kwargs["headers"]["x-api-key"] == "sk-ant-123"
    assert kwargs["headers"]["anthropic-version"] == "2023-06-01"


def test_google_strips_models_prefix_and_uses_query_key(fake_http):
    cfg = get_provider_config("google")
    client = fake_http(
        {
            "models": [
                {"name": "models/gemini-2.5-pro"},
                {"name": "models/gemini-2.5-flash"},
                {"name": "models/text-embedding-004"},
            ]
        }
    )
    models = lm.fetch_live_models("google", cfg, "gkey", cache=False)

    assert models == ["gemini-2.5-pro", "gemini-2.5-flash"]  # embedding filtered
    url, kwargs = client.requests[0]
    assert url == "https://generativelanguage.googleapis.com/v1beta/models"
    assert kwargs["params"] == {"key": "gkey"}


# ── failure → None (callers fall back to the curated catalog) ───────────────

def test_http_error_returns_none(fake_http):
    cfg = get_provider_config("openai")
    fake_http(payload=None, status=500)
    assert lm.fetch_live_models("openai", cfg, "bad", cache=False) is None


def test_network_error_returns_none(fake_http):
    cfg = get_provider_config("openai")
    fake_http(exc=httpx.ConnectError("boom"))
    assert lm.fetch_live_models("openai", cfg, "bad", cache=False) is None


def test_unparsable_payload_returns_none(fake_http):
    cfg = get_provider_config("openai")
    fake_http(payload={"unexpected": "shape"})
    assert lm.fetch_live_models("openai", cfg, "k", cache=False) is None


def test_empty_chat_list_normalized_to_none(fake_http):
    cfg = get_provider_config("openai")
    fake_http(payload={"data": [{"id": "text-embedding-3-large"}]})  # all filtered
    assert lm.fetch_live_models("openai", cfg, "k", cache=False) is None


# ── caching ──────────────────────────────────────────────────────────────────

def test_success_is_cached(fake_http):
    cfg = get_provider_config("openai")
    client = fake_http({"data": [{"id": "gpt-5"}]})

    assert lm.fetch_live_models("openai", cfg, "k") == ["gpt-5"]
    assert lm.fetch_live_models("openai", cfg, "k") == ["gpt-5"]
    assert len(client.requests) == 1  # second call served from cache


def test_cache_expiry_refetches(fake_http, monkeypatch):
    cfg = get_provider_config("openai")
    client = fake_http({"data": [{"id": "gpt-5"}]})

    clock = {"t": 1000.0}
    monkeypatch.setattr(lm.time, "monotonic", lambda: clock["t"])

    lm.fetch_live_models("openai", cfg, "k")  # cached at t=1000
    clock["t"] += 899  # inside the 900s TTL
    lm.fetch_live_models("openai", cfg, "k")
    assert len(client.requests) == 1

    clock["t"] += 2  # now past TTL
    lm.fetch_live_models("openai", cfg, "k")
    assert len(client.requests) == 2


def test_failure_is_short_cached_then_retried(fake_http, monkeypatch):
    cfg = get_provider_config("openai")
    client = fake_http(payload=None, status=500)

    clock = {"t": 1000.0}
    monkeypatch.setattr(lm.time, "monotonic", lambda: clock["t"])

    assert lm.fetch_live_models("openai", cfg, "bad") is None
    assert lm.fetch_live_models("openai", cfg, "bad") is None
    assert len(client.requests) == 1  # failure cached (30s)

    clock["t"] += 31
    lm.fetch_live_models("openai", cfg, "bad")
    assert len(client.requests) == 2  # retried after failure TTL


def test_ttl_env_override(monkeypatch):
    monkeypatch.setenv("AELVO_MODELS_CACHE_TTL", "7")
    assert lm.live_ttl() == 7

    monkeypatch.setenv("AELVO_MODELS_CACHE_TTL", "not-a-number")
    assert lm.live_ttl() == lm._DEFAULT_LIVE_TTL

    monkeypatch.delenv("AELVO_MODELS_CACHE_TTL", raising=False)
    assert lm.live_ttl() == lm._DEFAULT_LIVE_TTL


def test_clear_cache(fake_http):
    cfg = get_provider_config("openai")
    client = fake_http({"data": [{"id": "gpt-5"}]})
    lm.fetch_live_models("openai", cfg, "k")
    lm.clear_cache()
    lm.fetch_live_models("openai", cfg, "k")
    assert len(client.requests) == 2
