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
from typing import Any

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


def list_models_for(ctx, provider_key: str) -> list:
    """Models available for a provider: curated registry first, runtime fallback.

    The curated registry is authoritative for the providers the CLI can
    switch to; the runtime registry is only consulted for providers that are
    not in it.
    """
    curated = provider_models(provider_key)
    if curated:
        return curated
    if ctx.provider_runtime is not None:
        try:
            models = ctx.provider_runtime.list_models(provider_key) or []
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


async def switch_provider(ctx, name: str, inline_key: str = "") -> bool:
    """Switch the active LLM provider; resolves/persists a key and hot-swaps the agent."""
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

    model = (ctx.model or "").strip() or cfg.default_model
    available = list_models_for(ctx, name.lower())
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

async def pick_provider(ctx) -> str:
    """Full-screen picker over the registered providers; returns the key or ''.

    Non-interactive terminals fall back to ``''`` so callers can print the
    plain table instead.
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
        return ""
    picked = await pick_item(
        "Select a provider",
        items,
        subtitle="↑/↓ or j/k move · Enter switches · Esc cancels",
        default=current or None,
    )
    return picked or ""


async def pick_model(ctx) -> str:
    """Full-screen picker over the active provider's models; returns the id or ''.

    Only the provider's curated models are offered (``provider_models``), and
    each row carries the same visual style as the provider picker — model id,
    ability hints, and a ``[● current]`` marker.
    """
    from cli.picker import pick_item
    from core.registry.models import get_model_manifest

    if not ctx.provider_name:
        return ""
    available = provider_models(ctx.provider_name)
    if not available:
        return ""
    current = ctx.model or ""
    items = []
    for m in available:
        manifest = get_model_manifest(ctx.provider_name, m)
        abilities = ", ".join(a.value.replace("_", " ") for a in manifest.abilities)
        hint = f"({abilities})" if abilities else ""
        if m == current:
            items.append((m, f"{m:<30} {hint:<26} [● current]"))
        else:
            items.append((m, f"{m:<30} {hint}"))
    picked = await pick_item(
        f"Select a model · {ctx.provider_name}",
        items,
        subtitle="↑/↓ or j/k move · Enter switches · Esc cancels",
        default=current or None,
    )
    return picked or ""


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
