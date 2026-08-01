"""Regression tests for two REPORT_ANALYZE.md fixes.

HIGH #8 — core/workers/python_worker.py workspace containment:
    The old check used ``str(workspace).startswith(str(PROJECT_ROOT))`` which is
    vulnerable to prefix-collision escapes (a sibling like ``<root>-evil`` passes)
    and to symlink escapes. Fixed with ``Path.relative_to`` containment after
    ``resolve()``, rejecting with PermissionError.

HIGH #9 — core/security/security_memory.py persistence:
    SecurityMemory was in-memory-only unless a ChromaDB collection was passed;
    default instantiations (incl. the orchestrator) lost all security events on
    restart. Added optional SQLite persistence via ``db_path`` with load-on-init,
    upsert-on-add, delete-on-remove, and clear. Also verifies indexes are rebuilt
    and string-serialized enums are coerced back to enum types on load.
"""

import pytest

from core.security.security_memory import MemoryEntryType, SecurityMemory
from core.workers import python_worker


# ============================================================================
# HIGH #8 — python_worker workspace containment
# ============================================================================


def test_workspace_accepts_nested_path(monkeypatch, tmp_path):
    root = tmp_path / "proj"
    root.mkdir()
    monkeypatch.setattr(python_worker, "PROJECT_ROOT", root)

    ws = python_worker._workspace({"workspace": str(root / "sub" / "dir")})
    assert ws == (root / "sub" / "dir").resolve()


def test_workspace_defaults_to_project_root(monkeypatch, tmp_path):
    root = tmp_path / "proj"
    root.mkdir()
    monkeypatch.setattr(python_worker, "PROJECT_ROOT", root)

    ws = python_worker._workspace({})
    assert ws == root.resolve()


def test_workspace_rejects_prefix_collision_sibling(monkeypatch, tmp_path):
    """The old str.startswith check let a sibling '<root>-evil' pass."""
    root = tmp_path / "proj"
    root.mkdir()
    monkeypatch.setattr(python_worker, "PROJECT_ROOT", root)

    with pytest.raises(PermissionError):
        python_worker._workspace({"workspace": str(root) + "-evil"})


def test_workspace_rejects_outside_path(monkeypatch, tmp_path):
    root = tmp_path / "proj"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(python_worker, "PROJECT_ROOT", root)

    with pytest.raises(PermissionError):
        python_worker._workspace({"workspace": str(outside)})


def test_workspace_rejects_symlink_escape(monkeypatch, tmp_path):
    """A symlink inside the root pointing outside must resolve outside and be rejected."""
    root = tmp_path / "proj"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    link = root / "escape"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not permitted on this platform")
    monkeypatch.setattr(python_worker, "PROJECT_ROOT", root)

    with pytest.raises(PermissionError):
        python_worker._workspace({"workspace": str(link)})


# ============================================================================
# HIGH #9 — SecurityMemory SQLite persistence
# ============================================================================


def test_sqlite_roundtrip_persists_entries(tmp_path):
    db = str(tmp_path / "security_memory.db")
    mem = SecurityMemory(db_path=db)
    mem.record_hostile_entity("command", "rm -rf /", "destructive")
    mem.record_risky_action("git push --force", specialist="FORGE")

    mem2 = SecurityMemory(db_path=db)
    assert len(mem2._entries) == 2

    hostile = mem2.get_hostile_entities()
    assert len(hostile) == 1
    assert hostile[0].target == "rm -rf /"
    # Enum types must be coerced back on load (not plain strings)
    assert type(hostile[0].entry_type) is MemoryEntryType

    # Loaded entries must be queryable (query scans _entries directly)
    forge = mem2.query(specialist="FORGE")
    assert len(forge) == 1
    assert forge[0].target == "git push --force"


def test_dangerous_pattern_dedup_survives_reload(tmp_path):
    db = str(tmp_path / "security_memory.db")
    mem = SecurityMemory(db_path=db)
    first = mem.record_dangerous_pattern("shell_injection", "curl | sh", "risky")

    mem2 = SecurityMemory(db_path=db)
    second = mem2.record_dangerous_pattern("shell_injection", "curl | sh", "risky")
    # _find_existing uses the _by_target index — must find the reloaded entry
    assert second == first
    assert len(mem2.query(entry_type=MemoryEntryType.DANGEROUS_PATTERN)) == 1


def test_sqlite_clear_empties_db(tmp_path):
    db = str(tmp_path / "security_memory.db")
    mem = SecurityMemory(db_path=db)
    mem.record_risky_action("git push --force")
    mem.clear()

    mem2 = SecurityMemory(db_path=db)
    assert len(mem2._entries) == 0


def test_no_db_path_is_in_memory_only():
    mem = SecurityMemory()
    mem.record_risky_action("git push --force")

    mem2 = SecurityMemory()
    assert len(mem2._entries) == 0
