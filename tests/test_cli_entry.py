"""Tests for the dedicated CLI entry point (``cli/__main__.py`` + lean boot).

Covers the argument parser, the lightweight ``--version`` /
``--list-providers`` paths (no backend boot), and the no-provider one-shot
guidance (hermetic — no LLM call, no vault/.env dependence).
"""

import asyncio
import os
import subprocess
import sys

import pytest

from cli.__main__ import __version__, parse_args


@pytest.fixture(autouse=True)
def _clean_provider_env():
    """Restore LLM_*/AELVO_* env vars mutated by main() between tests."""
    saved = {
        k: v
        for k, v in os.environ.items()
        if k.startswith("LLM_") or k.startswith("AELVO_")
    }
    yield
    for k in list(os.environ):
        if k.startswith("LLM_") or k.startswith("AELVO_"):
            os.environ.pop(k, None)
    os.environ.update(saved)


# ── argument parsing ─────────────────────────────────────────────────────────

def test_parse_args_positional_prompt_is_one_shot():
    args = parse_args(["refactor the auth module"])
    assert args.prompt == ["refactor the auth module"]
    assert args.ask == ""


def test_parse_args_ask_flag():
    args = parse_args(["--ask", "hello agent"])
    assert args.ask == "hello agent"


def test_parse_args_workspace_flag():
    args = parse_args(["-w", "/tmp/x"])
    assert args.workspace == "/tmp/x"


def test_parse_args_folder_positional():
    # The workspace-style --project flag was removed: folders replace named
    # workspaces. A positional that is an existing directory opens that folder.
    import tempfile

    with tempfile.TemporaryDirectory() as folder:
        args = parse_args([folder])
        assert args.prompt == [folder]
    args = parse_args(["fix the auth bug"])
    assert args.prompt == ["fix the auth bug"]


def test_parse_args_provider_and_model_flags():
    args = parse_args(["--provider", "openai", "--model", "gpt-4o"])
    assert args.provider == "openai"
    assert args.model == "gpt-4o"


def test_parse_args_version_and_list_providers():
    assert parse_args(["--version"]).version is True
    assert parse_args(["--list-providers"]).list_providers is True


def test_main_maps_flags_to_env(monkeypatch):
    """main() with provider/model flags must export them to the environment
    before the boot runs (boot + provider detection read the env)."""
    import cli.__main__ as cli_main

    captured = {}

    def fake_run(coro):
        coro.close()  # avoid 'coroutine never awaited' warning
        captured["env"] = {
            k: v
            for k, v in os.environ.items()
            if k.startswith("LLM_") or k.startswith("AELVO_")
        }
        return None

    monkeypatch.setattr(cli_main.asyncio, "run", fake_run)
    monkeypatch.setattr(cli_main, "_configure_logging", lambda: None)
    rc = cli_main.main(
        ["--provider", "openai", "--model", "gpt-4o", "-w", "/tmp/ws"]
    )
    assert rc == 0
    assert captured["env"].get("LLM_PROVIDER") == "openai"
    assert captured["env"].get("AELVO_PROVIDER") == "openai"
    assert captured["env"].get("LLM_MODEL") == "gpt-4o"


def test_main_version_does_not_boot(monkeypatch):
    """--version must print and exit without booting the backend."""
    import cli.__main__ as cli_main

    called = []

    monkeypatch.setattr(cli_main, "_print_version", lambda: called.append("v"))
    rc = cli_main.main(["--version"])
    assert rc == 0
    assert called == ["v"]


# ── lightweight subprocess smoke tests (real entry points) ───────────────────

def _run_cli(*args):
    """Run the dedicated CLI in a subprocess, decoding UTF-8 (rich emits
    box-drawing chars that Windows' default cp1252 reader cannot decode)."""
    return subprocess.run(
        [sys.executable, "-m", "cli", *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=60,
    )


def test_cli_version_exits_zero():
    out = _run_cli("--version")
    assert out.returncode == 0
    assert "AELVO" in out.stdout
    assert __version__ in out.stdout


def test_cli_list_providers_prints_registry():
    out = _run_cli("--list-providers")
    assert out.returncode == 0
    assert "openai" in out.stdout
    assert "nvidia" in out.stdout
    assert "Creds" in out.stdout


# ── no-provider one-shot guidance (hermetic) ─────────────────────────────────

def test_run_cli_oneshot_no_provider_exits_one(capsys):
    """agent=None + one_shot must print setup guidance and exit 1 — without
    touching the vault, .env, or the network."""
    from cli.app import run_cli

    with pytest.raises(SystemExit) as excinfo:
        asyncio.run(
            run_cli(
                agent=None,
                orchestrator=None,
                memory_engine=None,
                aelvo_kernel=None,
                db_path="",
                workspace_path="/tmp/ws",
                project="t",
                one_shot="do something",
            )
        )
    assert excinfo.value.code == 1
    assert "No LLM provider is configured" in capsys.readouterr().out
