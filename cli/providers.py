"""
providers.py — LLM provider selection for the AELVO CLI.

Backs the ``/provider`` and ``/model`` slash commands:

* List the registered providers with default models and credential status.
* Switch the active provider at runtime: the API key is entered as part of
  provider selection (prompted inline when none is stored), persisted to the
  encrypted credential vault, the provider is remembered in ``.env``
  (``LLM_PROVIDER``), and the live ``AelvoAgent`` is hot-swapped without a
  restart.
* Switch the active model on the current agent and persist ``LLM_MODEL``.

All heavy imports (``main``, the provider runtime) are lazy so importing this
module stays cheap for tests and other tooling.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional, Tuple

from rich.table import Table
from rich.text import Text

log = logging.getLogger("aelvo.cli")

#: Cost-tier symbols shown next to each model row in the picker.
_TIER_SYMBOLS = {
    "budget": "$",
    "standard": "$$",
    "premium": "$$$",
}


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


#: Curated picker list is capped at the provider's top ~10 default models.
_MAX_DEFAULT_MODELS = 10

#: Sentinel value for the "Custom model id…" entry at the bottom of the model
#: picker. Real model ids can never collide with this.
_CUSTOM_MODEL_MARKER = "__aelvo_custom_model__"


def provider_models(provider_key: str) -> list:
    """Curated, provider-scoped model list (default + top ~10 special cases).

    Uses the same registry as the provider picker (``core.registry.models``)
    so the models offered are exactly the ones the provider supports — never
    the runtime's uncurated catalog, which mixes in non-agent ids such as
    OpenAI's ``text-embedding-*`` models. Capped at ``_MAX_DEFAULT_MODELS``
    so the default list stays a tight top-10; live API models are appended
    separately.
    """
    cfg = get_registry().get((provider_key or "").lower())
    if cfg is None:
        return []
    ids = [cfg.default_model] + [m.id for m in cfg.special_cases]
    seen = set()
    return [m for m in ids if not (m in seen or seen.add(m))][:_MAX_DEFAULT_MODELS]


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


def api_key_source(provider_key: str, env_key: str) -> str:
    """Where the provider's API key lives: ``'env'``, ``'vault'``, or ``''``.

    Resolution order mirrors ``resolve_api_key`` (env first, then the
    encrypted vault) so ``/status`` can tell the user which source is
    actually in use without ever revealing the key itself.
    """
    if os.environ.get(env_key, "").strip():
        return "env"
    # Env was empty and has_api_key is True ⇒ the vault has it.
    return "vault" if has_api_key(provider_key, env_key) else ""


def store_api_key(
    provider_key: str,
    display_name: str,
    api_key: str,
    *,
    label: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> bool:
    """Persist an API key to the encrypted vault and the current process env.

    Shared by the CLI provider flow and the web dashboard's Provider Setup:
    the new key supersedes any previously stored API-key row for the provider
    (replacing/rotating never leaves stale credentials behind, and other
    credential types such as OAuth tokens are untouched). When the provider's
    env var was already set (an env-sourced key), the new value is also
    written to ``.env`` so the rotation survives a restart — the env var wins
    over the vault at resolution time. Returns True when the key was stored.

    ``label``/``metadata`` customise the stored row (defaults match the CLI).
    """
    import time
    import uuid

    from auth.cred_storage import CredentialStore
    from auth.types import Credential, CredentialType

    cfg = None
    env_was_set = False
    try:
        from core.registry.models import get_provider_config

        cfg = get_provider_config(provider_key)
        if cfg is not None:
            env_was_set = bool(os.environ.get(cfg.env_key, "").strip())
    except Exception as exc:
        log.debug("Provider config lookup failed: %s", exc)

    try:
        store = CredentialStore(db_path=_vault_path())
        cred = Credential(
            id=f"key_{provider_key}_{uuid.uuid4().hex[:8]}",
            provider=provider_key,
            credential_type=CredentialType.API_KEY,
            value=api_key,
            label=label or f"{display_name} API key (set from CLI)",
            created_at=time.time(),
            is_valid=True,
            metadata=metadata or {"source": "cli"},
        )
        ok = store.store(cred)
        if ok:
            # Supersede stale API-key rows for this provider (store the new one
            # first, then drop older rows — never a window with no key in the
            # vault). Only API-key rows are touched: other credential types
            # (e.g. OAuth tokens) stored under the same provider must survive.
            for old in store.list_credentials(provider_key):
                if old["id"] != cred.id and old["credential_type"] == cred.credential_type.value:
                    store.delete(old["id"])
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
            if cfg is not None:
                os.environ[cfg.env_key] = api_key
                if env_was_set and not write_env(cfg.env_key, api_key):
                    log.warning(
                        "Key rotated for '%s' but .env could not be updated "
                        "— the change applies to this session only.",
                        provider_key,
                    )
        except Exception as exc:
            log.debug("Env key set failed: %s", exc)
    return ok


def write_env(key: str, value: str) -> bool:
    """Set ``key=value`` in ``.env`` (preserving comments); True on success."""
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
        return True
    except Exception as exc:
        log.warning("Could not update %s: %s", path, exc)
        return False


def remove_env(key: str) -> bool:
    """Strip a ``key=value`` line from ``.env``; True on success (or when the
    key was never there).

    Used when removing a provider key: an env-sourced key would otherwise win
    over the vault at resolution time and survive the removal on restart.
    """
    path = _env_path()
    try:
        if not os.path.exists(path):
            return True
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        kept = [ln for ln in lines if not ln.strip().startswith(f"{key}=")]
        if len(kept) == len(lines):
            return True  # nothing to strip
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(kept)
        return True
    except Exception as exc:
        log.warning("Could not update %s: %s", path, exc)
        return False


# ── interactive key prompt ──────────────────────────────────────────────────

async def _prompt_input(
    title: str,
    hint: str,
    *,
    password: bool,
    _input: Any = None,
    _output: Any = None,
) -> str:
    """Inline ``prompt_toolkit`` Application (hidden field when ``password``).

    Enter submits the text, Esc/Ctrl+C cancels (``''``), and non-interactive
    terminals (pipes, CI, tests) return ``''`` immediately so callers never
    hang. ``_input``/``_output`` inject prompt_toolkit streams for tests (e.g.
    ``create_pipe_input`` + ``DummyOutput``); ``None`` uses the terminal.
    """
    from cli.picker import is_interactive

    if not is_interactive():
        return ""
    try:
        from prompt_toolkit.application import Application
        from prompt_toolkit.key_binding import KeyBindings
        from prompt_toolkit.layout import HSplit, Layout
        from prompt_toolkit.styles import Style
        from prompt_toolkit.widgets import Label, TextArea

        text_area = TextArea(multiline=False, password=password)
        kb = KeyBindings()

        @kb.add("enter", eager=True)
        def _submit(event) -> None:
            event.app.exit(result=text_area.text)

        @kb.add("escape", eager=True)
        @kb.add("c-c", eager=True)
        def _cancel(event) -> None:
            event.app.exit(result="")

        layout = Layout(
            HSplit(
                [
                    Label(title, style="class:keyprompt.title"),
                    text_area,
                    Label(hint, style="class:keyprompt.hint"),
                ]
            )
        )
        app = Application(
            layout=layout,
            key_bindings=kb,
            full_screen=False,  # inline prompt — keeps the REPL context visible
            mouse_support=False,
            style=Style.from_dict(
                {
                    "keyprompt.title": "bold #FFD98E",  # golden white
                    "keyprompt.hint": "#9B938A",        # dim
                    "text-area": "#F6F1EA",             # snow body
                }
            ),
            input=_input,
            output=_output,
        )
        return (await app.run_async()).strip()
    except Exception as exc:
        log.debug("Inline prompt failed: %s", exc)
        return ""


async def prompt_api_key(
    display_name: str,
    *,
    _input: Any = None,
    _output: Any = None,
) -> str:
    """Prompt for an API key with a hidden field (interactive terminals only).

    ``prompt_async`` was removed from prompt_toolkit 3.x, so the original
    implementation raised ``ImportError`` and key entry silently failed ("No
    API key provided"). Enter saves the key, Esc/Ctrl+C cancels (``''``).
    """
    return await _prompt_input(
        f"API key for {display_name}:",
        "Paste the key · Enter to save · Esc to cancel",
        password=True,
        _input=_input,
        _output=_output,
    )


async def prompt_model_id(
    provider_key: str,
    *,
    _input: Any = None,
    _output: Any = None,
) -> str:
    """Prompt for a custom model id (models not in the curated/live lists).

    Enter uses the typed id, Esc/Ctrl+C cancels (``''``). The id is returned
    as-is — the caller (``pick_model``) passes it through unclamped.
    """
    return await _prompt_input(
        f"Custom model id for {provider_key}:",
        "Type the exact id the provider accepts · Enter to use · Esc to cancel",
        password=False,
        _input=_input,
        _output=_output,
    )


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
    key_was_new = False
    if not api_key:
        api_key = await prompt_api_key(cfg.name)
        key_was_new = bool(api_key)
    if not api_key:
        ctx.console.print(Text("No API key provided — provider not switched.", style="aelvo.err"))
        return False

    if key_was_new or inline_key.strip():
        # Persist only a freshly provided key. An existing key (including the
        # one the two-step picker stored before opening the model list) must
        # not create a duplicate vault row on every switch.
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
    if key_was_new or inline_key.strip():
        ctx.console.print(
            Text("  Key saved to the encrypted vault. Type /status to confirm.", style="aelvo.dim")
        )
    return True


# ── interactive pickers ──────────────────────────────────────────────────────

async def pick_provider(ctx) -> Optional[Tuple[str, str]]:
    """Provider → model picker, with key entry and rotation built in.

    1. Pick a provider.
    2. Key handling right there: if none is stored, prompt and persist it;
       if one already exists, offer to replace it (re-prompt + overwrite the
       vault entry). Either way the fresh key powers the model step's *live*
       model list.
    3. Pick a model (Esc = the provider's default model).

    Returns ``(provider_key, model)`` after all steps, or ``None`` when the
    provider step is cancelled, the key entry is cancelled, or the terminal is
    non-interactive (callers fall back to the plain table).
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
    cfg = get_registry().get(picked)
    if cfg is None:
        return None

    # The API key is entered as part of provider selection (before the model
    # step), so a fresh key also powers the live model list.
    if not resolve_api_key(picked, cfg.env_key):
        api_key = await prompt_api_key(cfg.name)
        if not api_key:
            ctx.console.print(
                Text(f"No API key provided for {cfg.name} — provider not switched.", style="aelvo.err")
            )
            return None
        store_api_key(picked, cfg.name, api_key)
    else:
        # A key already exists — offer to replace/rotate it right here. Esc or
        # "No" keeps the current key and continues to the model step.
        rotate = await pick_item(
            f"Replace the stored key for {cfg.name}?",
            [("yes", "Yes — enter a new key"), ("no", "No — keep the existing key")],
            subtitle="The current key stays in use until you replace it",
            default="no",
        )
        if rotate == "yes":
            new_key = await prompt_api_key(cfg.name)
            if new_key and store_api_key(picked, cfg.name, new_key):
                ctx.console.print(
                    Text(f"✓ API key for {cfg.name} replaced.", style="aelvo.ok")
                )
            elif new_key:
                ctx.console.print(
                    Text(
                        f"Could not store the new key for {cfg.name} — keeping the existing one.",
                        style="aelvo.err",
                    )
                )
            else:
                ctx.console.print(
                    Text(
                        f"Key rotation cancelled for {cfg.name} — keeping the existing key.",
                        style="aelvo.dim",
                    )
                )

    model = await pick_model(ctx, picked)
    if not model:
        # Model step cancelled: switch on the provider's default model.
        model = cfg.default_model
    return picked, model


async def pick_model(ctx, provider_key: str = "") -> str:
    """Full-screen picker over a provider's models; returns the id or ''.

    The list is the provider's live API models merged with the curated
    catalog (curated when no key / offline, capped at the top ~10) — the
    title shows ``(live)`` when the live list is in use. With no
    ``provider_key`` the active provider (``ctx.provider_name``) is used and
    the current model is preselected and marked ``[● current]``. When called
    for a *different* provider (the two-step ``/provider`` flow) its default
    model is preselected and marked ``[● default]``.

    A ``✏️  Custom model id…`` entry at the bottom lets you type any id the
    provider accepts — it is returned as-is (never clamped to the list).
    ``''`` means "cancelled" and callers fall back to the default model.
    """
    from cli.picker import pick_item
    from core.registry.models import format_context_window, get_model_manifest

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
        # Context window + cost tier hint, e.g. "400k · $$".
        meta = (
            f"{format_context_window(manifest.context_window)} · "
            f"{_TIER_SYMBOLS.get(manifest.cost_tier.value, '$$')}"
        )
        if m == current:
            items.append((m, f"{m:<28} {meta:<12} {hint:<22} [● {marker}]"))
        else:
            items.append((m, f"{m:<28} {meta:<12} {hint:<22}"))
    # Escape hatch for models the curated/live lists don't cover.
    items.append((_CUSTOM_MODEL_MARKER, "✏️  Custom model id…"))
    source_tag = f" ({source})" if source in ("live", "runtime") else ""
    picked = await pick_item(
        f"Select a model · {provider_key}{source_tag}",
        items,
        subtitle="↑/↓ or j/k move · Enter switches · Esc uses the default · custom id at the bottom",
        default=current or None,
    )
    if picked == _CUSTOM_MODEL_MARKER:
        custom = (await prompt_model_id(provider_key)).strip()
        if custom and " " not in custom:
            return custom  # passed through as-is, never clamped
        if custom:
            ctx.console.print(
                Text("Custom model id must not contain spaces.", style="aelvo.err")
            )
        return ""
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
