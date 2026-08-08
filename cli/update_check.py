"""update_check.py — remind users when a newer aelvo is on npm.

The CLI queries the npm registry for the latest published ``aelvo`` version
once per TTL (default 24h) and caches the result in the user data dir. When
a newer version exists, the boot banner shows a one-line reminder; ``/version``
re-checks live. Everything degrades to silence on any failure — a network
hiccup must never slow the REPL down or print an error.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from pathlib import Path

from cli.version import __version__

log = logging.getLogger("aelvo.cli.update_check")

#: npm registry "latest" endpoint for the aelvo package.
_NPM_LATEST_URL = "https://registry.npmjs.org/aelvo/latest"

#: How often to re-query the registry (seconds).
DEFAULT_TTL = 24 * 3600

#: Network timeout — never let the check hold up the REPL for long.
_FETCH_TIMEOUT = 3.0

#: Cache file name inside the data dir's .aelvo_runtime folder.
_CACHE_NAME = "update_check.json"


def cache_path() -> Path:
    """Location of the update-check cache (data dir / .aelvo_runtime)."""
    try:
        from config.settings import get_data_dir

        base = Path(str(get_data_dir()))
    except Exception:  # pragma: no cover - defensive
        base = Path.home()
    return base / ".aelvo_runtime" / _CACHE_NAME


def ttl_seconds() -> int:
    """Cache TTL; env ``AELVO_UPDATE_CHECK_TTL`` (seconds) overrides."""
    raw = os.environ.get("AELVO_UPDATE_CHECK_TTL", "").strip()
    try:
        return max(int(raw), 60) if raw else DEFAULT_TTL
    except ValueError:
        return DEFAULT_TTL


def enabled() -> bool:
    """False when the user opted out via ``AELVO_NO_UPDATE_CHECK=1``."""
    raw = os.environ.get("AELVO_NO_UPDATE_CHECK", "").strip().lower()
    return raw not in ("1", "true", "yes", "on")


def _read_cache() -> dict | None:
    try:
        data = json.loads(cache_path().read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("latest"), str):
            return data
    except Exception:
        pass
    return None


def _write_cache(latest: str) -> None:
    try:
        p = cache_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps({"latest": latest, "checked_at": time.time()}),
            encoding="utf-8",
        )
    except Exception as exc:  # pragma: no cover - defensive
        log.debug("update-check cache write failed: %s", exc)


def _version_tuple(v: str) -> tuple:
    """``2.10.1`` → (2, 10, 1); tolerate suffixes like ``2.2.0-beta``."""
    parts = []
    for chunk in v.split("."):
        digits = ""
        for ch in chunk:
            if ch.isdigit():
                digits += ch
            else:
                break
        parts.append(int(digits) if digits else 0)
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts[:3])


def fetch_latest() -> str | None:
    """Live query of the npm registry; ``None`` on any failure."""
    try:
        with urllib.request.urlopen(_NPM_LATEST_URL, timeout=_FETCH_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        latest = (data or {}).get("version")
        return str(latest).strip() if latest else None
    except Exception as exc:
        log.debug("update check failed: %s", exc)
        return None


def reminder(current: str | None = None, refresh: bool = True) -> str | None:
    """A short update reminder string, or ``None`` when up to date.

    Uses the cached latest version when fresh. When the cache is stale (or
    missing) and ``refresh`` is True, re-queries the registry (throttled by
    the TTL) and re-caches the result. Pass ``refresh=False`` for a strictly
    cache-only read — that is what the boot banner uses so startup never
    blocks on the network. Never raises.
    """
    if not enabled():
        return None
    current = current or __version__

    cached = _read_cache()
    latest: str | None = None
    fresh = False
    if cached:
        checked_at = cached.get("checked_at", 0)
        fresh = (time.time() - checked_at) < ttl_seconds()
        if fresh:
            latest = cached.get("latest")

    if not fresh and refresh:
        latest = fetch_latest()
        if latest:
            _write_cache(latest)

    if latest and _version_tuple(latest) > _version_tuple(current):
        return (
            f"📦 aelvo {latest} is available (you have {current}) — "
            "update with: npm install -g aelvo@latest"
        )
    return None
