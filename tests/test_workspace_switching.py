"""tests/test_workspace_switching.py — Agent direct folder/workspace access

Verifies the runtime "open folder" capability added for the web UI:

- ``AelvoFileSystem.set_base_path`` re-jails the agent's file tools to a new
  workspace root (the same mechanism CLI/web/desktop coding agents use when
  you open a folder).
- Reads resolve against the NEW root after switching, not the old one.
- Path traversal outside the new root still fails closed.

These tests require the compiled Rust sandbox binary; they are skipped when
it is not available so CI without a Rust toolchain stays green.
"""

import os
import shutil
import tempfile

import pytest

from core.execution import AelvoKernel
from core.filesystem import AelvoFileSystem

SANDBOX_BINARY = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "sandbox_core", "target", "release", "sandbox_core.exe",
)


def _kernel(db: str, anchor: str, backup: str) -> AelvoKernel:
    with open(anchor, "w", encoding="utf-8") as f:
        f.write("---\nconstraints:\n  test: {value: ok}\n---\n")
    return AelvoKernel(
        db_path=db,
        anchor_path=anchor,
        backup_dir=backup,
    )


@pytest.fixture
def sandbox_env():
    """Two sibling folders with distinct marker files + a kernel."""
    root = tempfile.mkdtemp(prefix="aelvo_ws_")
    folder_a = os.path.join(root, "projA")
    folder_b = os.path.join(root, "projB")
    os.makedirs(folder_a, exist_ok=True)
    os.makedirs(folder_b, exist_ok=True)
    with open(os.path.join(folder_a, "a.txt"), "w", encoding="utf-8") as f:
        f.write("AAA")
    with open(os.path.join(folder_b, "b.txt"), "w", encoding="utf-8") as f:
        f.write("BBB")

    kernel = _kernel(
        db=os.path.join(root, "k.db"),
        anchor=os.path.join(root, "a.md"),
        backup=os.path.join(root, "bk"),
    )
    try:
        yield {
            "root": root,
            "folder_a": folder_a,
            "folder_b": folder_b,
            "kernel": kernel,
        }
    finally:
        try:
            kernel.conn.close()
        except Exception:
            pass
        shutil.rmtree(root, ignore_errors=True)


@pytest.mark.skipif(
    not os.path.exists(SANDBOX_BINARY),
    reason="Rust sandbox binary not built (run cargo build in sandbox_core)",
)
def test_set_base_path_reroots_tool_access(sandbox_env):
    """After set_base_path the agent's tools resolve against the new folder."""
    env = sandbox_env
    fs = AelvoFileSystem(base_path=env["folder_a"], kernel=env["kernel"])

    # Sanity: reads resolve inside folder A
    before = fs.read_file("a.txt")
    assert before.get("status") == "success", before
    assert "AAA" in before.get("data", ""), before

    # Switch workspace → tools must now read from folder B
    new_root = fs.set_base_path(env["folder_b"])
    assert os.path.normcase(new_root) == os.path.normcase(
        os.path.abspath(env["folder_b"])
    )
    assert os.path.normcase(str(fs.base_path)) == os.path.normcase(
        os.path.abspath(env["folder_b"])
    )

    after = fs.read_file("b.txt")
    assert after.get("status") == "success", after
    assert "BBB" in after.get("data", ""), after

    # The old folder's file must no longer resolve by its bare name
    old = fs.read_file("a.txt")
    assert old.get("status") == "error", old


@pytest.mark.skipif(
    not os.path.exists(SANDBOX_BINARY),
    reason="Rust sandbox binary not built (run cargo build in sandbox_core)",
)
def test_traversal_fails_closed_after_switch(sandbox_env):
    """Escaping the NEW workspace root is still denied."""
    env = sandbox_env
    fs = AelvoFileSystem(base_path=env["folder_a"], kernel=env["kernel"])
    fs.set_base_path(env["folder_b"])

    # Path traversal into the sibling project must be blocked
    bad = fs.read_file("../projA/a.txt")
    assert bad.get("status") == "error", bad

    # Absolute path outside the root must be blocked too
    bad_abs = fs.read_file(os.path.abspath(env["folder_a"]))
    assert bad_abs.get("status") == "error", bad_abs


def test_set_base_path_rejects_missing_and_files(sandbox_env):
    """set_base_path validates the target folder (exists + is a directory)."""
    env = sandbox_env
    fs = AelvoFileSystem(base_path=env["folder_a"], kernel=env["kernel"])

    with pytest.raises(FileNotFoundError):
        fs.set_base_path(os.path.join(env["root"], "does_not_exist"))

    # Pointing at a file (not a directory) is rejected
    with pytest.raises(NotADirectoryError):
        fs.set_base_path(os.path.join(env["folder_a"], "a.txt"))

    # Root unchanged after failures
    assert os.path.normcase(str(fs.base_path)) == os.path.normcase(
        os.path.abspath(env["folder_a"])
    )
