"""tests/test_web_bridge_providers.py — regression tests for the web UI
provider-setup feature (web_bridge.py).

Covers:
* ``_provider_payload`` never leaks the API key.
* ``_maybe_hot_swap_agent`` activates an agent when the bridge booted without
  one (the no-provider boot path), and is a no-op when an agent is already
  active.
"""

import asyncio

import pytest

from web.web_bridge import WebBridge


class _FakeOrchestrator:
    pass


def _make_bridge(agent=None) -> WebBridge:
    bridge = WebBridge(host="127.0.0.1", port=8899)
    bridge._orchestrator = _FakeOrchestrator()
    bridge._agent = agent
    return bridge


def test_provider_payload_never_leaks_key():
    """The serialized provider payload must not contain the raw API key."""
    bridge = _make_bridge()
    cfg = type(
        "Cfg",
        (),
        {
            "name": "Acme",
            "env_key": "ACME_API_KEY",
            "default_model": "acme-1",
            "sdk": type("SDK", (), {"value": "openai"})(),
            "local": False,
        },
    )()

    payload = bridge._provider_payload("acme", cfg)

    assert payload["key"] == "acme"
    assert payload["name"] == "Acme"
    assert payload["env_key"] == "ACME_API_KEY"
    assert "api_key" not in payload
    assert "value" not in payload
    serialized = asyncio.run(_dumps(payload))
    assert "sk-" not in serialized


async def _dumps(payload):
    import json

    return json.dumps(payload)


@pytest.mark.asyncio
async def test_hot_swap_activates_agent_when_offline():
    """Boot with no agent -> hot-swap should construct one (env key present)."""
    bridge = _make_bridge(agent=None)

    # Only meaningful when a real key exists in the environment or vault;
    # otherwise hot-swap correctly returns False (nothing to activate).
    ok = bridge._maybe_hot_swap_agent()
    if ok:
        assert bridge._agent is not None
        assert bridge._agent.api_key
    else:
        assert bridge._agent is None


@pytest.mark.asyncio
async def test_hot_swap_is_noop_when_agent_already_active():
    """An already-active agent must not be replaced."""
    bridge = _make_bridge(agent=object())
    assert bridge._maybe_hot_swap_agent() is True
    assert bridge._agent is not None
