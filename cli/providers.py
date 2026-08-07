"""
providers.py — LLM provider selection for the AELVO CLI.

Backs the ``/provider``, ``/model`` and ``/apikey`` slash commands:

* List the registered providers with default models and credential status.
* Switch the active provider at runtime: resolve (or prompt for) an API key,
  persist it to the encrypted credential vault, remember the provider in
  ``.env`` (``LLM_PROVIDER``), and hot-swap the live ``AelvoAgent`` without a
  restart.
* Switch the active model on the current agent and persist ``LLM_MODEL``.

All heavy imports (``main``, the provider runtime) are lazy so importing this
module stays cheap for tests and other tooling.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Optional, Tuple

from rich.table import Table
from rich.text import Text

log = logging.getLogger("aelvo.cli")


# ── paths ────────────────────────────────────────────────────────────────────

def _vault_path() -> str:
    """Canonical encrypted credential vault path (same as provider_runtime)."""
    from core.provider_runtime import DEFAULT_VAULT_PATH

    return DEFAULT_VAULT_PATH


def _env_path() -> str:
    from config.settings import BASE_DIR

    return os.path.join(BASE_DIR, ".env")


# ── registry helpers ────────────────────────────────────────────────────────

def get_registry() -> dict:
    """MODEL_REGISTRY mapping provider key → ProviderConfig."""
    from core.registry.models import MODEL_REGISTRY

    return MODEL_REGISTRY


def provider_models(provider_key: str) -> list:
    """Curated, provider-scoped model list (default + special cases).

    Uses the same registry as the provider picker (``core.registry.models``)
    so the models offered are exactly the ones the provider supports — never
    the runtime's uncurated catalog, which mixes in non-agent ids such as
    OpenAI's ``text-embedding-*`` models.
    """
    cfg = get_registry().get((provider_key or "").lower())
    if cfg is None:
        return []
    ids = [cfg.default_model] + [m.id for m in cfg.special_cases]
    seen = set()
    return [m for m in ids if not (m in seen or seen.add(m))]


#: Vendor prefixes kept when merging OpenRouter's live list (several hundred
#: routed variants would otherwise flood the picker). Everything else stays out.
_OPENROUTER_PREFIXES = (
    "anthropic/", "openai/", "google/", "deepseek/", "qwen/", "moonshotai/",
    "x-ai/", "meta-llama/", "mistralai/", "nvidia/", "cohere/",
    "amazon/", "perplexity/", "minimax/", "z-ai/",
)

#: Hard cap on appended live-only ids — the picker must stay scrollable.
_MAX_LIVE_EXTRAS = 80


def _merge_models(provider_key: str, curated: list, live: list) -> list:
    """Live ∪ curated, preserving curated order (default first), live extras appended.

    OpenRouter's live list is huge, so only its known vendor-prefixed routing
    families are appended; other providers are capped at ``_MAX_LIVE_EXTRAS``
    fresh ids as a safety net.
    """
    if (provider_key or "").lower() == "openrouter":
        live = [m for m in live if m.startswith(_OPENROUTER_PREFIXES)]
    seen = set(curated)
    merged = list(curated)
    for m in live:
        if m not in seen:
            seen.add(m)
            merged.append(m)
        if len(merged) - len(curated) >= _MAX_LIVE_EXTRAS:
            break
    return merged


async def available_models(ctx, provider_key: str):
    """Live-first model list for a provider: ``(models, source)``.

    ``source`` is one of ``'live'`` (fetched from the provider's API),
    ``'catalog'`` (curated registry — offline fallback), ``'runtime'``
    (runtime registry, for providers outside the curated one) or ``''``.
    """
    key = (provider_key or "").lower()
    cfg = get_registry().get(key)
    curated = provider_models(key)
    if cfg is not None:
        api_key = resolve_api_key(key, cfg.env_key)
        if api_key:
            from cli.live_models import fetch_live_models_async

            live = await fetch_live_models_async(key, cfg, api_key)
            if live:
                return _merge_models(key, curated, live), "live"
        if curated:
            return curated, "catalog"
    if ctx.provider_runtime is not None:
        try:
            models = ctx.provider_runtime.list_models(key) or []
            strings = [m for m in models if isinstance(m, str)]
            if strings:
                return strings, "runtime"
        except Exception as exc:
            log.debug("provider_runtime.list_models failed: %s", exc)
    return [], ""


def list_models_for(ctx, provider_key: str) -> list:
    """Models available for a provider: live API first, curated, then runtime.

    Uses the sync ``fetch_live_models`` (cached), so repeated calls in a
    session (validation, ``/models``, pickers) only hit the API once per TTL.
    Any live failure falls back to the curated registry, then the runtime
    registry for providers outside the curated set.
    """
    key = (provider_key or "").lower()
    cfg = get_registry().get(key)
    curated = provider_models(key)
    if cfg is not None:
        api_key = resolve_api_key(key, cfg.env_key)
        if api_key:
            from cli.live_models import fetch_live_models

            live = fetch_live_models(key, cfg, api_key)
            if live:
                return _merge_models(key, curated, live)
        if curated:
            return curated
    if ctx.provider_runtime is not None:
        try:
            models = ctx.provider_runtime.list_models(key) or []
            strings = [m for m in models if isinstance(m, str)]
            if strings:
                return strings
        except Exception as exc:
            log.debug("provider_runtime.list_models failed: %s", exc)
    return []


# ── credentials ──────────────────────────────────────────────────────────────

def _vault_key(provider_key: str) -> str:
    """API key for a provider from the encrypted vault, or ''."""
    try:
        from auth.cred_storage import CredentialStore

        store = CredentialStore(db_path=_vault_path())
        cred = store.get_for_provider(provider_key)
        return cred.value if cred else ""
    except Exception as exc:
        log.debug("Vault lookup failed for %s: %s", provider_key, exc)
        return ""


def resolve_api_key(provider_key: str, env_key: str) -> str:
    """Find a usable key: environment first, then the encrypted vault."""
    key = os.environ.get(env_key, "").strip()
    if key:
        return key
    return _vault_key(provider_key)


def has_api_key(provider_key: str, env_key: str) -> bool:
    """Cheap presence check (no decryption): env var, else vault metadata.

    Avoids the expensive PBKDF2 decrypt per provider when rendering the
    provider table — only ``list_credentials`` metadata is read here.
    """
    if os.environ.get(env_key, "").strip():
        return True
    try:
        from auth.cred_storage import CredentialStore

        store = CredentialStore(db_path=_vault_path())
        return bool(store.list_credentials(provider_key))
    except Exception as exc:
        log.debug("Vault presence check failed for %s: %s", provider_key, exc)
        return False


def store_api_key(provider_key: str, display_name: str, api_key: str) -> bool:
    """Persist an API key to the encrypted vault and the current process env."""
    import time
    import uuid

    from auth.cred_storage import CredentialStore
    from auth.types import Credential, CredentialType

    try:
        store = CredentialStore(db_path=_vault_path())
        cred = Credential(
            id=f"key_{provider_key}_{uuid.uuid4().hex[:8]}",
            provider=provider_key,
            credential_type=CredentialType.API_KEY,
            value=api_key,
            label=f"{display_name} API key (set from CLI)",
            created_at=time.time(),
            is_valid=True,
            metadata={"source": "cli"},
        )
        ok = store.store(cred)
    except Exception as exc:
        log.warning("Credential store failed: %s", exc)
        ok = False
    if ok:
        # A new key may expose different models — drop the cached live list.
        try:
            from cli.live_models import clear_cache

            clear_cache()
        except Exception as exc:
            log.debug("Live cache clear failed: %s", exc)
        try:
            from core.registry.models import get_provider_config

            cfg = get_provider_config(provider_key)
            if cfg is not None:
                os.environ[cfg.env_key] = api_key
        except Exception as exc:
            log.debug("Env key set failed: %s", exc)
    return ok


def write_env(key: str, value: str) -> None:
    """Set ``key=value`` in ``.env``, preserving comments and other lines."""
    path = _env_path()
    try:
        lines: list = []
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        target = f"{key}={value}\n"
        replaced = False
        for i, line in enumerate(lines):
            if line.strip().startswith(f"{key}="):
                lines[i] = target
                replaced = True
                break
        if not replaced:
            lines.append(target)
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(lines)
    except Exception as exc:
        log.warning("Could not update %s: %s", path, exc)


# ── interactive key prompt ──────────────────────────────────────────────────

async def prompt_api_key(display_name: str) -> str:
    """Prompt for an API key with a hidden field (interactive terminals only)."""
    try:
        if not (sys.stdin.isatty() and sys.stdout.isatty()):
            return ""
        from prompt_toolkit.shortcuts import prompt_async

        return (await prompt_async(f"API key for {display_name}: ", is_password=True)).strip()
    except Exception as exc:
        log.debug("API key prompt failed: %s", exc)
        return ""


# ── agent hot-swap ──────────────────────────────────────────────────────────

def build_agent(provider_key: str, cfg, api_key: str, model: str, provider_runtime) -> Any:
    """Build a fresh AelvoAgent and register it as the active agent."""
    try:
        from main import AelvoAgent, _ACTIVE_AGENT

        agent = AelvoAgent(
            api_key=api_key,
            model=model or cfg.default_model,
            provider_name=provider_key,
            provider_config=cfg,
            provider_runtime=provider_runtime,
        )
        _ACTIVE_AGENT.set(agent)
        return agent
    except Exception as exc:
        log.warning("Agent build failed: %s", exc)
        return None


async def switch_provider(
    ctx, name: str, inline_key: str = "", model_override: str = ""
) -> bool:
    """Switch the active LLM provider; resolves/persists a key and hot-swaps the agent.

    ``model_override`` (from the two-step picker) is honored as-is — it was
    already validated against the provider's live list. The fallback chain
    (previous provider's model, then the default) is clamped to a model the
    provider actually offers.
    """
    cfg = get_registry().get(name.lower())
    if cfg is None:
        ctx.console.print(Text(f"Unknown provider: {name} — use /provider to list.", style="aelvo.err"))
        return False

    api_key = inline_key.strip() or resolve_api_key(name.lower(), cfg.env_key)
    if not api_key:
        api_key = await prompt_api_key(cfg.name)
    if not api_key:
        ctx.console.print(Text("No API key provided — provider not switched.", style="aelvo.err"))
        return False

    store_api_key(name.lower(), cfg.name, api_key)
    write_env("LLM_PROVIDER", name.lower())
    os.environ["LLM_PROVIDER"] = name.lower()
    os.environ["AELVO_PROVIDER"] = name.lower()

    model = model_override.strip() or (ctx.model or "").strip() or cfg.default_model
    if not model_override.strip():
        # Only clamp the *fallback* chain to a valid curated model. An explicit
        # picker choice (model_override) was already validated against the live
        # list — clamping it would silently undo the user's selection.
        available, _src = await available_models(ctx, name.lower())
        if available and model not in available:
            model = cfg.default_model
    agent = build_agent(name.lower(), cfg, api_key, model, ctx.provider_runtime)
    if agent is None:
        ctx.console.print(
            Text("Key stored, but the live agent could not be rebuilt — restart AELVO to apply.", style="aelvo.err")
        )
        return False

    ctx.agent = agent
    ctx.provider_name = name.lower()
    ctx.model = model
    # A fresh provider must not retry the previous provider's last prompt.
    ctx.state["last_prompt"] = ""
    ctx.console.print(
        Text(f"✓ Provider switched to {cfg.name} — model {model}", style="aelvo.ok")
    )
    ctx.console.print(
        Text("  Key saved to the encrypted vault. Type /status to confirm.", style="aelvo.dim")
    )
    return True


def set_api_key(ctx, provider_key: str, api_key: str) -> bool:
    """Store an API key for the current provider (no agent rebuild)."""
    cfg = get_registry().get(provider_key.lower())
    display = cfg.name if cfg is not None else provider_key
    return store_api_key(provider_key.lower(), display, api_key.strip())


# ── interactive pickers ──────────────────────────────────────────────────────

async def pick_provider(ctx) -> Optional[Tuple[str, str]]:
    """Two-step picker: choose a provider, then one of its models.

    Returns ``(provider_key, model)`` after both steps, or ``None`` when the
    provider step is cancelled (callers fall back to the plain table).
    Cancelling the model step yields ``(provider_key, default_model)`` — the
    switch still happens, on the new provider's default model.
    Non-interactive terminals return ``None`` (``pick_item`` returns None
    off-tty).
    """
    from cli.picker import pick_item

    current = (ctx.provider_name or "").lower()
    items = []
    for key, cfg in sorted(get_registry().items()):
        creds = "✓ key" if has_api_key(key, cfg.env_key) else "no key"
        marker = " ● active" if key == current else ""
        label = f"{cfg.name:<20} {cfg.default_model}   [{creds}]{marker}"
        items.append((key, label))
    if not items:
        return None
    picked = await pick_item(
        "Select a provider",
        items,
        subtitle="↑/↓ or j/k move · Enter opens its models · Esc cancels",
        default=current or None,
    )
    if not picked:
        return None
    model = await pick_model(ctx, picked)
    if not model:
        # Model step cancelled: switch on the provider's default model.
        cfg = get_registry().get(picked)
        model = cfg.default_model if cfg is not None else ""
    return picked, model


async def pick_model(ctx, provider_key: str = "") -> str:
    """Full-screen picker over a provider's models; returns the id or ''.

    The list is the provider's live API models merged with the curated
    catalog (curated when no key / offline) — the title shows ``(live)`` when
    the live list is in use. With no ``provider_key`` the active provider
    (``ctx.provider_name``) is used and the current model is preselected and
    marked ``[● current]``. When called for a *different* provider (the
    two-step ``/provider`` flow) its default model is preselected and marked
    ``[● default]``. The result is validated against the offered list, so
    ``''`` means "cancelled" and callers fall back to the default model.
    """
    from cli.picker import pick_item
    from core.registry.models import get_model_manifest

    provider_key = (provider_key or ctx.provider_name or "").lower()
    if not provider_key:
        return ""
    available, source = await available_models(ctx, provider_key)
    if not available:
        return ""
    cfg = get_registry().get(provider_key)
    default_model = cfg.default_model if cfg is not None else available[0]
    is_active = provider_key == (ctx.provider_name or "").lower()
    current = ctx.model if is_active else default_model
    if current not in available:  # stale / cross-provider model id
        current = default_model
    marker = "current" if is_active else "default"

    items = []
    for m in available:
        manifest = get_model_manifest(provider_key, m)
        abilities = ", ".join(a.value.replace("_", " ") for a in manifest.abilities)
        hint = f"({abilities})" if abilities else ""
        if m == current:
            items.append((m, f"{m:<30} {hint:<26} [● {marker}]"))
        else:
            items.append((m, f"{m:<30} {hint}"))
    source_tag = f" ({source})" if source in ("live", "runtime") else ""
    picked = await pick_item(
        f"Select a model · {provider_key}{source_tag}",
        items,
        subtitle="↑/↓ or j/k move · Enter switches · Esc uses the default",
        default=current or None,
    )
    if picked and picked in available:
        return picked
    return ""


# ── tables ───────────────────────────────────────────────────────────────────

def provider_table(ctx) -> Table:
    """Table of all registered providers with credential + active markers."""
    table = Table(title="Available providers", title_style="aelvo.gold")
    table.add_column("Key", style="aelvo.brand")
    table.add_column("Provider", style="aelvo.snow")
    table.add_column("Default model", style="aelvo.purple")
    table.add_column("Creds", style="aelvo.snow")
    table.add_column("", style="aelvo.ok")
    for key, cfg in sorted(get_registry().items()):
        creds = "✓" if has_api_key(key, cfg.env_key) else "—"
        active = "● active" if key == (ctx.provider_name or "").lower() else ""
        table.add_row(key, cfg.name, cfg.default_model, creds, active)
    return table
