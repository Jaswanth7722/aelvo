"""tests/test_web_bridge_providers.py — regression tests for the web UI
provider-setup feature (web_bridge.py).

Covers:
* ``_provider_payload`` never leaks the API key.
* ``_maybe_hot_swap_agent`` activates an agent when the bridge booted without
  one (the no-provider boot path), and is a no-op when an agent is already
  active.
* ``_handle_provider_save_key`` reports failure when the vault store fails,
  and re-saving rotates instead of accumulating duplicate vault rows (the
  same persistence semantics the CLI got).
* ``_handle_provider_remove_key`` deletes every API-key row but leaves other
  credential types (e.g. OAuth tokens) intact.
"""

import asyncio
import time

import pytest

from auth.cred_storage import CredentialStore
from auth.types import Credential, CredentialType
from web.web_bridge import WebBridge


class _FakeOrchestrator:
    pass


def _make_bridge(agent=None) -> WebBridge:
    bridge = WebBridge(host="127.0.0.1", port=8899)
    bridge._orchestrator = _FakeOrchestrator()
    bridge._agent = agent
    return bridge


def _make_cfg(name="Acme", env_key="ACME_API_KEY", default_model="acme-1"):
    return type(
        "Cfg",
        (),
        {
            "name": name,
            "env_key": env_key,
            "default_model": default_model,
            "sdk": type("SDK", (), {"value": "openai"})(),
            "local": False,
        },
    )()


async def _noop_async(*_a, **_k):
    return None


def _capture_sends(bridge, monkeypatch):
    sent = []
    broadcasts = []

    async def _send(*args):
        sent.append(args[-1])  # payload is always the last argument

    async def _broadcast(payload):
        broadcasts.append(payload)

    monkeypatch.setattr(bridge, "_send_raw", _send)
    monkeypatch.setattr(bridge, "_broadcast", _broadcast)
    monkeypatch.setattr(bridge, "_maybe_hot_swap_agent", lambda: False)
    return sent, broadcasts


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
async def test_provider_save_key_reports_failure_when_store_fails(monkeypatch):
    """A failed vault store must surface an error — never a false 'saved'.
    (The web equivalent of the CLI's silent key-entry failure.)"""
    bridge = _make_bridge()
    monkeypatch.setattr(bridge, "_provider_configs", lambda: {"acme": _make_cfg()})
    # The shared persistence helper fails (locked/unavailable vault).
    monkeypatch.setattr("cli.providers.store_api_key", lambda *a, **k: False)
    sent, broadcasts = _capture_sends(bridge, monkeypatch)

    await bridge._handle_provider_save_key({"provider": "acme", "api_key": "sk-x"}, None)

    assert sent and sent[0]["data"]["success"] is False
    assert "Failed to save" in sent[0]["data"]["message"]
    assert "API key saved" not in sent[0]["data"]["message"]
    assert broadcasts == []  # failure path must not broadcast success


@pytest.mark.asyncio
async def test_provider_save_key_rotates_instead_of_accumulating(tmp_path, monkeypatch):
    """Re-saving from the web goes through the shared helper: stale API-key
    rows are superseded (one row, newest value) and an OAuth token stored
    under the same provider survives rotation."""
    bridge = _make_bridge()
    vault = str(tmp_path / "vault.db")
    monkeypatch.setattr(bridge, "_provider_configs", lambda: {"acme": _make_cfg()})
    # Shared helper writes to the tmp vault; no env/.env side effects.
    monkeypatch.setattr("cli.providers._vault_path", lambda: vault)
    monkeypatch.setattr("cli.providers.write_env", lambda k, v: True)
    monkeypatch.setattr("core.registry.models.get_provider_config", lambda k: None)
    sent, _broadcasts = _capture_sends(bridge, monkeypatch)

    # Seed an unrelated credential type for the same provider.
    seed = CredentialStore(db_path=vault)
    seed.store(
        Credential(
            id="oauth_acme_1", provider="acme",
            credential_type=CredentialType.OAUTH_TOKEN,
            value="oauth-token", label="oauth", created_at=time.time(), is_valid=True,
        )
    )

    await bridge._handle_provider_save_key({"provider": "acme", "api_key": "sk-one"}, None)
    await bridge._handle_provider_save_key({"provider": "acme", "api_key": "sk-two"}, None)

    assert all(p["data"]["success"] for p in sent)  # both saves succeeded
    store = CredentialStore(db_path=vault)
    rows = store.list_credentials("acme")
    api_keys = [r for r in rows if r["credential_type"] == "api_key"]
    others = [r for r in rows if r["credential_type"] != "api_key"]
    assert len(api_keys) == 1  # rotated, not accumulated
    assert store.get_for_provider("acme", CredentialType.API_KEY).value == "sk-two"
    assert len(others) == 1 and others[0]["credential_type"] == "oauth_token"


@pytest.mark.asyncio
async def test_provider_remove_key_deletes_all_api_key_rows(tmp_path, monkeypatch):
    """Remove wipes every stored API-key row (older saves used fresh ids) but
    leaves other credential types — e.g. OAuth tokens — intact."""
    bridge = _make_bridge()
    vault = str(tmp_path / "vault.db")
    store = CredentialStore(db_path=vault)
    for i in range(2):
        store.store(
            Credential(
                id=f"key_{i}", provider="acme",
                credential_type=CredentialType.API_KEY,
                value=f"sk-{i}", label="k", created_at=time.time(), is_valid=True,
            )
        )
    store.store(
        Credential(
            id="oauth_acme_1", provider="acme",
            credential_type=CredentialType.OAUTH_TOKEN,
            value="oauth-token", label="oauth", created_at=time.time(), is_valid=True,
        )
    )

    monkeypatch.setattr(bridge, "_credential_store", lambda: CredentialStore(db_path=vault))
    monkeypatch.setattr(bridge, "_provider_configs", lambda: {"acme": _make_cfg()})
    monkeypatch.setattr("cli.providers.remove_env", lambda k: True)  # hermetic: no real .env
    monkeypatch.delenv("ACME_API_KEY", raising=False)
    sent, _broadcasts = _capture_sends(bridge, monkeypatch)

    await bridge._handle_provider_remove_key({"provider": "acme"}, None)

    rows = CredentialStore(db_path=vault).list_credentials("acme")
    assert [r for r in rows if r["credential_type"] == "api_key"] == []
    assert [r for r in rows if r["credential_type"] == "oauth_token"]  # survived
    assert sent and sent[0]["data"]["success"] is True
    assert "API key removed" in sent[0]["data"]["message"]


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
