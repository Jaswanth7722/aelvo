"""version.py — single source of truth for the AELVO CLI version.

The CLI version is read from ``package.json`` (present in the repo root and
in an npm install) so the version the CLI reports always matches the version
on npm. A hardcoded fallback keeps ``python -m cli`` working in checkouts
without a package file, but package.json wins whenever it exists.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

#: Fallback used only when package.json cannot be read.
_FALLBACK_VERSION = "2.3.1"


def _package_json_path() -> Path | None:
    # cli/version.py → repo/npm root is two levels up.
    candidate = Path(__file__).resolve().parent.parent / "package.json"
    return candidate if candidate.is_file() else None


def _read_version() -> str:
    pkg = _package_json_path()
    if pkg is not None:
        try:
            data = json.loads(pkg.read_text(encoding="utf-8"))
            v = data.get("version")
            if isinstance(v, str) and v.strip():
                return v.strip()
        except Exception:
            pass
    # Allow a direct override (e.g. test fixtures / CI).
    return os.environ.get("AELVO_VERSION", "").strip() or _FALLBACK_VERSION


#: The AELVO CLI version (matches the npm package version).
__version__ = _read_version()
