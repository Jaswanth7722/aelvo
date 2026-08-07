"""
live_models.py — Fresh model lists straight from each provider's API.

The CLI pickers normally show the curated catalog in ``core.registry.models``.
When the provider's API key is available, ``fetch_live_models`` reaches out to
the provider's model-list endpoint so the picker always offers the freshest
models (new releases appear without a code update).

* OpenAI-compatible providers (openai, groq, mistral, cohere, deepseek,
  moonshot, nvidia, together, openrouter, ...): ``GET {base_url}/models`` with
  ``Authorization: Bearer <key>`` → ``{"data": [{"id": ...}]}``.
* Anthropic: ``GET https://api.anthropic.com/v1/models`` with ``x-api-key``.
* Google Gemini: ``GET https://generativelanguage.googleapis.com/v1beta/models``
  with the key as a query parameter → ``{"models": [{"name": "models/..."}]}``.

Every failure path (missing key, network error, non-2xx, unparsable payload)
returns ``None`` so callers silently fall back to the curated catalog —
a live fetch can never break the picker.

Results are cached in-process: successful fetches for ``AELVO_MODELS_CACHE_TTL``
seconds (default 15 min), failures for 30 seconds so a dead network doesn't
stall every picker open.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Dict, List, Optional, Tuple

import httpx

log = logging.getLogger("aelvo.cli")

#: Default time a successful live list stays fresh (seconds).
_DEFAULT_LIVE_TTL = 15 * 60
#: Failed fetches are retried after this long, so a blip recovers quickly.
_FAIL_TTL = 30
#: Per-request timeout — the picker must never hang on a slow API.
_TIMEOUT = 4.0

#: Model-id fragments that are not agent chat models. Excluded so live lists
#: never surface embeddings, rerankers, TTS, images, realtime/audio models, etc.
_NON_CHAT_FRAGMENTS = (
    "embedding",
    "moderation",
    "whisper",
    "tts",
    "speech",
    "dall-e",
    "dalle",
    "rerank",
    "realtime",
    "transcri",
    "audio",
    "classif",
    "img",
    "imagen",  # Google image generation
    "bison",   # Google legacy PaLM chat-bison / text-bison
)

#: provider_key -> (time.monotonic() at fetch, models or None)
_CACHE: Dict[str, Tuple[float, Optional[List[str]]]] = {}


# ── TTL ──────────────────────────────────────────────────────────────────────

def live_ttl() -> int:
    """TTL for successful fetches; env ``AELVO_MODELS_CACHE_TTL`` overrides."""
    raw = os.environ.get("AELVO_MODELS_CACHE_TTL", "").strip()
    try:
        return max(int(raw), 5) if raw else _DEFAULT_LIVE_TTL
    except ValueError:
        return _DEFAULT_LIVE_TTL


# ── request / parse per SDK ──────────────────────────────────────────────────

def models_url(cfg, api_key: str) -> Tuple[str, dict, dict]:
    """Return ``(url, headers, params)`` for a provider's model-list endpoint."""
    from core.registry.models import SDKType

    if cfg.sdk == SDKType.ANTHROPIC:
        return (
            "https://api.anthropic.com/v1/models",
            {"x-api-key": api_key, "anthropic-version": "2023-06-01"},
            {},
        )
    if cfg.sdk == SDKType.GOOGLE:
        return (
            "https://generativelanguage.googleapis.com/v1beta/models",
            {},
            {"key": api_key},
        )
    base = (cfg.base_url or "https://api.openai.com/v1").rstrip("/")
    return (f"{base}/models", {"Authorization": f"Bearer {api_key}"}, {})


def parse_models(payload, cfg) -> List[str]:
    """Extract chat-model ids from a provider's JSON response."""
    from core.registry.models import SDKType

    if cfg.sdk == SDKType.GOOGLE:
        names = (payload or {}).get("models") or []
        ids = [str(m.get("name", "")) for m in names if isinstance(m, dict)]
        ids = [name.removeprefix("models/") for name in ids]
    else:
        data = (payload or {}).get("data") or []
        ids = [str(m.get("id", "")) for m in data if isinstance(m, dict)]
    return [mid for mid in ids if mid and not _is_non_chat(mid)]


def _is_non_chat(model_id: str) -> bool:
    low = model_id.lower()
    return any(frag in low for frag in _NON_CHAT_FRAGMENTS)


# ── fetch ────────────────────────────────────────────────────────────────────

def fetch_live_models(
    provider_key: str, cfg, api_key: str, *, cache: bool = True
) -> Optional[List[str]]:
    """Fetch the provider's current model list; ``None`` on any failure.

    Callers treat ``None`` (and only ``None``) as "use the curated catalog".
    An empty list is normalized to ``None``: a payload that yields no chat
    models almost certainly means the API shape changed.
    """
    key = (provider_key or "").lower()
    if cache:
        hit = _CACHE.get(key)
        if hit is not None:
            ts, models = hit
            ttl = live_ttl() if models is not None else _FAIL_TTL
            if time.monotonic() - ts < ttl:
                return models
    try:
        url, headers, params = models_url(cfg, api_key)
        with httpx.Client(timeout=_TIMEOUT, follow_redirects=True) as client:
            resp = client.get(url, headers=headers, params=params)
            resp.raise_for_status()
        models = parse_models(resp.json(), cfg) or None
    except Exception as exc:
        log.debug("live model fetch failed for %s: %s", key, exc)
        models = None
    if cache:
        _CACHE[key] = (time.monotonic(), models)
    return models


async def fetch_live_models_async(
    provider_key: str, cfg, api_key: str, *, cache: bool = True
) -> Optional[List[str]]:
    """Async wrapper for ``fetch_live_models`` (runs off the event loop).

    Both entry points share the same cache, so a sync call (e.g. inside
    ``list_models_for``) and an async one (the picker) never double-fetch.
    """
    return await asyncio.to_thread(fetch_live_models, provider_key, cfg, api_key, cache=cache)


def clear_cache() -> None:
    """Drop all cached live lists (used by tests and CLI key changes)."""
    _CACHE.clear()
