"""
tests/test_local_models_fix.py — Local-runtime model handling.

Fixes the user-facing confusion when a local provider (Ollama etc.) 404s on
a model that was never installed:

* ``_merge_models``: for local providers, curated catalog entries that are
  NOT in the live (installed) list are dropped — the picker never offers a
  model that would 404 on selection.
* ``_format_llm_error``: a 404 on a local runtime says HOW to fix it
  (e.g. ``ollama pull <model>``) instead of a generic "verify the model id".
"""

from __future__ import annotations

import asyncio
from io import StringIO

from cli.commands import CliContext
from cli.theme import build_console


def _ctx(tmp_path) -> CliContext:
    console = build_console()
    console.file = StringIO()
    return CliContext(
        agent=None,
        orchestrator=None,
        memory_engine=None,
        aelvo_kernel=None,
        console=console,
        db_path="",
        workspace_path=str(tmp_path),
        project="t",
    )


# ── local merge filtering ───────────────────────────────────────────────────


def test_merge_models_local_filters_uninstalled_curated():
    """Local runtimes: curated models not installed are dropped."""
    from cli.providers import _merge_models

    curated = ["llama3.2", "llama3.1", "mistral", "qwen2.5", "deepseek-r1"]
    live = ["qwen2.5", "deepseek-r1", "llama3.2"]  # only these are pulled
    merged = _merge_models("ollama", curated, live)
    assert set(merged) == {"qwen2.5", "deepseek-r1", "llama3.2"}
    # Curated order preserved: installed curated first, then live extras.
    assert merged.index("llama3.2") < merged.index("qwen2.5")
    assert "mistral" not in merged


def test_merge_models_local_keeps_live_only_models():
    """Live-only models (not in curated) still appear for local runtimes."""
    from cli.providers import _merge_models

    merged = _merge_models("ollama", ["llama3.2"], ["llama3.2", "qwen2.5-coder:0.5b"])
    assert "qwen2.5-coder:0.5b" in merged


def test_merge_models_cloud_keeps_full_curated():
    """Cloud providers are unchanged: full curated catalog + live extras."""
    from cli.providers import _merge_models

    curated = ["gpt-5", "gpt-4o", "o3"]
    merged = _merge_models("openai", curated, ["gpt-5", "new-live-model"])
    assert "gpt-4o" in merged and "o3" in merged  # curated kept intact
    assert "new-live-model" in merged


def test_picker_local_only_offers_installed(tmp_path, monkeypatch):
    """End-to-end: available_models for ollama returns only pulled models."""
    from cli import live_models, providers as P

    ctx = _ctx(tmp_path)

    async def fake_fetch(provider_key, cfg, api_key):
        return ["qwen2.5-coder:0.5b"]  # the live installed list

    monkeypatch.setattr(live_models, "fetch_live_models_async", fake_fetch)
    available, source = asyncio.run(P.available_models(ctx, "ollama"))
    assert source == "live"
    # Curated llama3.2 etc. are NOT installed → dropped from the picker.
    assert available == ["qwen2.5-coder:0.5b"]


def test_picker_local_falls_back_to_curated_when_server_down(tmp_path, monkeypatch):
    """Ollama unreachable → curated catalog still offered (offline fallback)."""
    from cli import live_models, providers as P

    ctx = _ctx(tmp_path)

    async def fake_fetch(provider_key, cfg, api_key):
        return None  # server down

    monkeypatch.setattr(live_models, "fetch_live_models_async", fake_fetch)
    available, source = asyncio.run(P.available_models(ctx, "ollama"))
    assert source == "catalog"
    assert "llama3.2" in available  # curated default still offered offline


# ── provider-aware 404 message ──────────────────────────────────────────────


class _Fake404:
    status_code = 404
    message = "model not found"


def test_format_llm_error_404_cloud_generic():
    from main import _format_llm_error

    msg = _format_llm_error(_Fake404())
    assert "model was not found" in msg
    assert "pull" not in msg  # cloud 404 stays generic


def test_format_llm_error_404_ollama_pull_hint():
    from main import _format_llm_error

    msg = _format_llm_error(
        _Fake404(), provider_name="ollama", model="gemma3:270m",
    )
    assert "gemma3:270m" in msg
    assert "ollama pull" in msg
    assert "not installed" in msg


def test_format_llm_error_404_other_local_runtimes():
    from main import _format_llm_error

    assert "LM Studio" in _format_llm_error(
        _Fake404(), provider_name="lm_studio", model="local-model"
    )
    assert "vLLM" in _format_llm_error(
        _Fake404(), provider_name="vllm", model="x"
    )
    assert "llama.cpp" in _format_llm_error(
        _Fake404(), provider_name="llama_cpp", model="x"
    )
