"""Unit tests for the CLI update check (cli/update_check.py).

Covers version comparison, the opt-out env var, the TTL override, the
cache-first / network-fallback behavior, the cache-only (refresh=False)
mode used by the boot banner, and the banner integration itself.
"""

from __future__ import annotations

import json
import time
from io import StringIO

import pytest

from cli import update_check


@pytest.fixture
def no_cache(tmp_path, monkeypatch):
    """Point the cache into a fresh tmp dir and clear the env knobs."""
    p = tmp_path / ".aelvo_runtime"
    monkeypatch.setattr(update_check, "cache_path", lambda: p / "update_check.json")
    monkeypatch.delenv("AELVO_NO_UPDATE_CHECK", raising=False)
    monkeypatch.delenv("AELVO_UPDATE_CHECK_TTL", raising=False)
    return p


def seed_cache(no_cache, latest: str, checked_at: float) -> None:
    cache = update_check.cache_path()
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(
        json.dumps({"latest": latest, "checked_at": checked_at}), encoding="utf-8"
    )


# ── version parsing ──────────────────────────────────────────────────────────

def test_version_tuple_parsing():
    assert update_check._version_tuple("2.10.1") == (2, 10, 1)
    assert update_check._version_tuple("2.2.0-beta") == (2, 2, 0)
    assert update_check._version_tuple("3") == (3, 0, 0)
    assert update_check._version_tuple("1.2") == (1, 2, 0)
    assert update_check._version_tuple("0.0.1") == (0, 0, 1)


# ── enabled / TTL ────────────────────────────────────────────────────────────

def test_enabled_defaults_true(no_cache):
    assert update_check.enabled() is True


def test_enabled_opt_out(no_cache, monkeypatch):
    monkeypatch.setenv("AELVO_NO_UPDATE_CHECK", "1")
    assert update_check.enabled() is False
    monkeypatch.setenv("AELVO_NO_UPDATE_CHECK", "true")
    assert update_check.enabled() is False


def test_reminder_returns_none_when_disabled(no_cache, monkeypatch):
    monkeypatch.setenv("AELVO_NO_UPDATE_CHECK", "1")
    assert update_check.reminder(current="2.3.0") is None


def test_ttl_override(no_cache, monkeypatch):
    monkeypatch.setenv("AELVO_UPDATE_CHECK_TTL", "300")
    assert update_check.ttl_seconds() == 300
    monkeypatch.setenv("AELVO_UPDATE_CHECK_TTL", "not-a-number")
    assert update_check.ttl_seconds() == update_check.DEFAULT_TTL
    monkeypatch.delenv("AELVO_UPDATE_CHECK_TTL")
    assert update_check.ttl_seconds() == update_check.DEFAULT_TTL


# ── reminder logic ───────────────────────────────────────────────────────────

def test_reminder_none_when_up_to_date(no_cache, monkeypatch):
    monkeypatch.setattr(update_check, "fetch_latest", lambda: "2.3.0")
    assert update_check.reminder(current="2.3.0") is None


def test_reminder_flags_newer_version(no_cache, monkeypatch):
    monkeypatch.setattr(update_check, "fetch_latest", lambda: "2.4.0")
    hint = update_check.reminder(current="2.3.0")
    assert hint is not None
    assert "2.4.0" in hint and "2.3.0" in hint
    assert "npm install -g aelvo@latest" in hint


def test_reminder_uses_fresh_cache_without_network(no_cache, monkeypatch):
    seed_cache(no_cache, latest="9.9.9", checked_at=time.time())
    calls = {"n": 0}

    def boom():
        calls["n"] += 1
        raise AssertionError("network touched on fresh cache")

    monkeypatch.setattr(update_check, "fetch_latest", boom)
    hint = update_check.reminder(current="2.3.0")
    assert hint is not None and "9.9.9" in hint
    assert calls["n"] == 0


def test_reminder_stale_cache_refreshes_and_rewrites(no_cache, monkeypatch):
    seed_cache(no_cache, latest="1.0.0", checked_at=0)
    monkeypatch.setattr(update_check, "fetch_latest", lambda: "2.5.0")
    hint = update_check.reminder(current="2.3.0", refresh=True)
    assert hint is not None and "2.5.0" in hint
    data = json.loads(update_check.cache_path().read_text(encoding="utf-8"))
    assert data["latest"] == "2.5.0"


def test_reminder_refresh_false_never_network(no_cache, monkeypatch):
    # Empty (never-checked) cache + refresh=False must stay fully offline —
    # that is the guarantee the boot banner relies on.
    calls = {"n": 0}

    def boom():
        calls["n"] += 1
        raise AssertionError("network touched with refresh=False")

    monkeypatch.setattr(update_check, "fetch_latest", boom)
    assert update_check.reminder(current="2.3.0", refresh=False) is None
    assert calls["n"] == 0


def test_reminder_network_failure_is_silent(no_cache, monkeypatch):
    monkeypatch.setattr(update_check, "fetch_latest", lambda: None)
    assert update_check.reminder(current="2.3.0", refresh=True) is None


def test_reminder_never_raises_on_bad_cache(no_cache, monkeypatch):
    cache = update_check.cache_path()
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text("{not-json", encoding="utf-8")
    monkeypatch.setattr(update_check, "fetch_latest", lambda: None)
    assert update_check.reminder(current="2.3.0") is None


# ── banner integration ───────────────────────────────────────────────────────

def test_banner_shows_cached_reminder(no_cache):
    seed_cache(no_cache, latest="9.9.9", checked_at=time.time())

    from cli.app import _print_banner
    from cli.commands import CliContext
    from rich.console import Console

    out = StringIO()
    console = Console(file=out, force_terminal=True)
    ctx = CliContext(
        agent=None,
        orchestrator=None,
        memory_engine=None,
        aelvo_kernel=None,
        console=console,
        db_path="",
        workspace_path=".",
        project="t",
        provider_name="openai",
        model="gpt-4o",
    )
    _print_banner(console, ctx)
    rendered = out.getvalue()
    assert "9.9.9" in rendered
    assert "aelvo@latest" in rendered


def test_banner_hidden_when_up_to_date(no_cache):
    seed_cache(no_cache, latest="2.3.0", checked_at=time.time())

    from cli.app import _print_banner
    from cli.commands import CliContext
    from rich.console import Console

    out = StringIO()
    console = Console(file=out, force_terminal=True)
    ctx = CliContext(
        agent=None,
        orchestrator=None,
        memory_engine=None,
        aelvo_kernel=None,
        console=console,
        db_path="",
        workspace_path=".",
        project="t",
        provider_name="openai",
        model="gpt-4o",
    )
    _print_banner(console, ctx)
    assert "aelvo@latest" not in out.getvalue()


def test_banner_never_blocks_on_missing_cache(no_cache, monkeypatch):
    # No cache at all: the banner must not fetch (refresh=False) and must not
    # raise — boot stays fast and quiet on first run.
    calls = {"n": 0}

    def boom():
        calls["n"] += 1
        raise AssertionError("banner touched the network")

    monkeypatch.setattr(update_check, "fetch_latest", boom)
    from cli.app import _print_banner
    from cli.commands import CliContext
    from rich.console import Console

    out = StringIO()
    console = Console(file=out, force_terminal=True)
    ctx = CliContext(
        agent=None,
        orchestrator=None,
        memory_engine=None,
        aelvo_kernel=None,
        console=console,
        db_path="",
        workspace_path=".",
        project="t",
    )
    _print_banner(console, ctx)  # must not raise
    assert calls["n"] == 0
