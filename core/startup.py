"""core/startup.py — Web-only boot helpers for AELVO.

Relocated from the retired ``ui.menu`` terminal menus. Everything here is
non-interactive so the web dashboard can boot with zero terminal UI:

* ``select_project`` — deterministic workspace resolution
  (``--project`` / ``AELVO_PROJECT`` / most-recently-used / ``"default"``).
* ``detect_provider`` — credential detection from the environment or the
  encrypted credential vault (no terminal wizard; returns ``None``s when no
  key exists so the web UI can surface the connection error instead).
* ``migrate_env_keys`` — moves plaintext provider keys out of ``.env`` into
  the encrypted vault.
"""

import logging
import os
import sqlite3

from config.settings import BASE_DIR

log = logging.getLogger("aelvo.startup")

# Paths resolved via BASE_DIR (same convention as the old ui.menu)
GLOBAL_DB_PATH = os.path.join(BASE_DIR, "global_memory.db")
GLOBAL_ANCHOR_PATH = os.path.join(BASE_DIR, "global_anchor.md")
WORKSPACE_BASE = os.path.join(BASE_DIR, "workspace")
_VAULT_PATH = os.path.join(BASE_DIR, ".aelvo_runtime", "credential_vault.db")

# Placeholder keys that are never real credentials
_PLACEHOLDER_KEYS = {"your-api-key-here", "your-anthropic-api-key-here", ""}


def init_global_metadata() -> None:
    """Ensure the global metadata database and anchor scaffold are ready."""
    try:
        with sqlite3.connect(GLOBAL_DB_PATH) as db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE,
                    description TEXT,
                    path TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_opened TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS user_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            if not os.path.exists(GLOBAL_ANCHOR_PATH):
                with open(GLOBAL_ANCHOR_PATH, "w", encoding="utf-8") as f:
                    f.write(
                        "---\n"
                        "meta: AELVO Global Constraints\n"
                        "version: 1.0\n"
                        "---\n"
                        "# Global Rules\n"
                        "All projects inherit these root constraints.\n"
                    )
    except Exception as exc:  # pragma: no cover - defensive boot path
        log.warning("Global init error: %s", exc)


def _register_project(name: str) -> None:
    """Record/refresh a workspace in the global metadata DB."""
    try:
        with sqlite3.connect(GLOBAL_DB_PATH) as db:
            db.execute(
                "INSERT OR IGNORE INTO projects (name, description, path) VALUES (?, ?, ?)",
                (name, "", os.path.join(WORKSPACE_BASE, name)),
            )
            db.execute(
                "UPDATE projects SET last_opened = CURRENT_TIMESTAMP WHERE name = ?",
                (name,),
            )
    except Exception as exc:  # pragma: no cover - defensive boot path
        log.warning("Could not register project %s: %s", name, exc)


def select_project(project_name: str | None = None) -> str:
    """Resolve the workspace name deterministically (no interactive menu).

    Priority: explicit arg → ``AELVO_PROJECT`` env → most recently opened
    project → ``"default"``.
    """
    init_global_metadata()

    name = (project_name or "").strip()
    if not name:
        name = os.environ.get("AELVO_PROJECT", "").strip()

    if name:
        _register_project(name)
        return name

    # Fall back to the most recently opened project, then "default"
    try:
        with sqlite3.connect(GLOBAL_DB_PATH) as db:
            row = db.execute(
                "SELECT name FROM projects ORDER BY last_opened DESC LIMIT 1"
            ).fetchone()
            if row:
                _register_project(row[0])
                return row[0]
    except Exception as exc:
        log.warning("Project lookup failed, using default: %s", exc)

    _register_project("default")
    return "default"


def _vault_key(provider: str) -> str:
    """Look up a provider API key in the encrypted credential vault."""
    try:
        from auth.cred_storage import CredentialStore

        store = CredentialStore(db_path=_VAULT_PATH)
        cred = store.get_for_provider(provider)
        return cred.value if cred else ""
    except Exception as exc:
        log.debug("Vault lookup failed for %s: %s", provider, exc)
        return ""


def migrate_env_keys() -> None:
    """Move plaintext provider keys out of ``.env`` into the encrypted vault."""
    env_path = os.path.join(BASE_DIR, ".env")
    if not os.path.exists(env_path):
        return

    try:
        import time
        import uuid

        from auth.cred_storage import CredentialStore
        from auth.types import Credential, CredentialType
        from core.registry import MODEL_REGISTRY

        store = CredentialStore(db_path=_VAULT_PATH)
        env_to_provider = {cfg.env_key: name for name, cfg in MODEL_REGISTRY.items()}

        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        new_lines: list[str] = []
        migrated = False
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                new_lines.append(line)
                continue
            if "=" in stripped:
                key, _, raw_val = stripped.partition("=")
                key = key.strip()
                val = raw_val.strip().strip('"').strip("'")
                if (
                    key in env_to_provider
                    and val not in _PLACEHOLDER_KEYS
                ):
                    provider_name = env_to_provider[key]
                    existing = store.get_for_provider(provider_name)
                    if not existing or existing.value != val:
                        cred = Credential(
                            id=f"key_{provider_name}_{uuid.uuid4().hex[:8]}",
                            provider=provider_name,
                            credential_type=CredentialType.API_KEY,
                            value=val,
                            label=f"{provider_name} API key (Migrated from .env)",
                            created_at=time.time(),
                            is_valid=True,
                            metadata={"source": "env_migration"},
                        )
                        store.store(cred)
                    new_lines.append(f"# {key} (Migrated to encrypted credential store vault)\n")
                    migrated = True
                    continue
            new_lines.append(line)

        if migrated:
            with open(env_path, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            log.info("Plaintext API keys removed from .env (migrated to encrypted vault).")
    except Exception as exc:
        log.warning("API key migration failed: %s", exc)


def detect_provider(model_registry: dict) -> tuple:
    """Non-interactive provider detection (env → encrypted vault).

    Returns ``(provider_name, config, api_key, model)`` or a tuple of four
    ``None``s when no usable key exists, so boot never blocks on a wizard and
    the web UI can surface the connection error instead.
    """
    migrate_env_keys()

    explicit = os.environ.get("LLM_PROVIDER", "").strip().lower()
    model_override = os.environ.get("LLM_MODEL", "").strip()

    if explicit:
        if explicit not in model_registry:
            log.warning("Unknown LLM_PROVIDER='%s' in environment variables.", explicit)
            return None, None, None, None
        cfg = model_registry[explicit]
        key = os.environ.get(cfg.env_key, "").strip() or _vault_key(explicit)
        if not key or key in _PLACEHOLDER_KEYS:
            log.warning("LLM_PROVIDER is '%s' but no usable API key was found.", explicit)
            return None, None, None, None
        return explicit, cfg, key, model_override or cfg.default_model

    # Auto-detect: scan environment for the first available API key
    for name, cfg in model_registry.items():
        key = os.environ.get(cfg.env_key, "").strip()
        if key and key not in _PLACEHOLDER_KEYS:
            return name, cfg, key, model_override or cfg.default_model

    # Then scan the encrypted vault
    for name, cfg in model_registry.items():
        key = _vault_key(name)
        if key:
            return name, cfg, key, model_override or cfg.default_model

    log.warning(
        "No LLM provider keys found in environment or credential vault. "
        "Set one in .env (or the encrypted vault) and restart."
    )
    return None, None, None, None
