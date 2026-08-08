"""Pure-Python sandbox fallback tests.

The Rust sandbox binary (``sandbox_core/target/release/sandbox_core.exe``) is
gitignored and is NOT shipped in the npm package. When it is absent,
``AelvoFileSystem`` must keep working via pure-Python implementations that
mirror the Rust response shapes and enforce the same path jail + command
policy. These tests monkeypatch the binary path to force the fallback path.
"""

from __future__ import annotations

import os

import pytest

from core.execution import AelvoKernel
from core.filesystem import AelvoFileSystem


@pytest.fixture
def fs_env(tmp_path):
    """AelvoKernel + AelvoFileSystem rooted at tmp_path with a missing sandbox."""
    anchor = tmp_path / "anchor.md"
    anchor.write_text("---\nconstraints:\n  test: {value: ok}\n---\n", encoding="utf-8")
    kernel = AelvoKernel(
        db_path=str(tmp_path / "memory.db"),
        anchor_path=str(anchor),
        backup_dir=str(tmp_path / "backups"),
    )
    fs = AelvoFileSystem(base_path=str(tmp_path), kernel=kernel)
    # Force the pure-Python fallback regardless of any locally built binary.
    monkey = pytest.MonkeyPatch()
    monkey.setattr(
        fs,
        "_sandbox_binary_path",
        lambda: os.path.join(str(tmp_path), "definitely_missing_sandbox.exe"),
    )
    yield fs
    monkey.undo()
    try:
        kernel.conn.close()
    except Exception:
        pass


class TestFallbackFileOperations:
    def test_fallback_is_active(self, fs_env):
        assert not fs_env._sandbox_available()

    def test_write_and_read_roundtrip(self, fs_env, tmp_path):
        res = fs_env.write_atomic("hello.txt", "hello world")
        assert res.get("status") == "success"
        read = fs_env.read_file("hello.txt")
        assert read.get("status") == "success"
        assert read.get("data") == "hello world"
        assert (tmp_path / "hello.txt").read_text(encoding="utf-8") == "hello world"

    def test_read_range(self, fs_env, tmp_path):
        (tmp_path / "lines.txt").write_text("\n".join(f"line{i}" for i in range(1, 11)), encoding="utf-8")
        res = fs_env.read_file_range("lines.txt", 2, 4)
        assert res.get("status") == "success"
        assert res.get("data") == ["line2", "line3", "line4"]

    def test_read_file_on_directory_returns_list_files_hint(self, fs_env):
        """read_file on a directory (the tiny-model 'list the files' mistake:
        read_file on '.') must fail with a clear pointer to list_files, not a
        confusing backend error — and must never return success."""
        res = fs_env.read_file(".")
        assert res.get("status") == "error"
        assert "list_files" in res.get("logs", "")
        assert "directory" in res.get("logs", "")

        res2 = fs_env.read_file_range(".")
        assert res2.get("status") == "error"
        assert "list_files" in res2.get("logs", "")

    def test_read_file_normal_file_still_works(self, fs_env, tmp_path):
        """The directory guard must not break reading real files."""
        (tmp_path / "note.txt").write_text("content", encoding="utf-8")
        res = fs_env.read_file("note.txt")
        assert res.get("status") == "success"
        assert res.get("data") == "content"

    def test_edit_file_block(self, fs_env, tmp_path):
        (tmp_path / "code.py").write_text("def old():\n    pass\n", encoding="utf-8")
        res = fs_env.edit_file_block("code.py", "def old():", "def new():")
        assert res.get("status") == "success"
        assert "def new():" in (tmp_path / "code.py").read_text(encoding="utf-8")

    def test_edit_missing_block_fails(self, fs_env, tmp_path):
        (tmp_path / "code.py").write_text("def old():\n", encoding="utf-8")
        res = fs_env.edit_file_block("code.py", "def nope():", "def new():")
        assert res.get("status") == "error"

    def test_grep_file(self, fs_env, tmp_path):
        (tmp_path / "app.py").write_text("import os\nprint('x')\nimport sys\n", encoding="utf-8")
        res = fs_env.grep_file("app.py", "import", case_sensitive=True)
        assert res.get("status") == "success"
        matches = res.get("data", [])
        assert [m["line"] for m in matches] == [1, 3]

    def test_find_files(self, fs_env, tmp_path):
        (tmp_path / "a.py").write_text("a", encoding="utf-8")
        (tmp_path / "b.md").write_text("b", encoding="utf-8")
        res = fs_env.find_files("*.py")
        assert res.get("status") == "success"
        assert res.get("data") == ["a.py"]

    def test_project_tree(self, fs_env, tmp_path):
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "x.py").write_text("x", encoding="utf-8")
        res = fs_env.project_tree(max_depth=2)
        assert res.get("status") == "success"
        joined = "\n".join(res.get("data", []))
        assert "sub/" in joined

    def test_search_code(self, fs_env, tmp_path):
        (tmp_path / "app.py").write_text("def search_target():\n    pass\n", encoding="utf-8")
        res = fs_env.search_code("search_target")
        assert res.get("status") == "success"
        matches = res.get("data", [])
        assert any(m["line"] == 1 for m in matches)

    def test_python_exec(self, fs_env, tmp_path):
        script = tmp_path / "hello.py"
        script.write_text("print('hi from fallback')\n", encoding="utf-8")
        res = fs_env.python_exec("hello.py", timeout=30)
        assert "hi from fallback" in res.get("stdout", "")

    def test_bash_exec_allowed_command(self, fs_env):
        res = fs_env.bash_exec("echo hello", timeout=10)
        assert res.get("status") == "success"
        assert res.get("stdout", "").strip() == "hello"


class TestFallbackSecurity:
    def test_traversal_denied(self, fs_env, tmp_path):
        with pytest.raises(PermissionError):
            fs_env._validate_path("../../etc/passwd")

    def test_blocked_command_pattern(self, fs_env):
        res = fs_env.bash_exec("rm -rf /", timeout=10)
        assert res.get("status") == "error"
        assert "blocked" in res.get("logs", "").lower()

    def test_not_in_allowlist_denied(self, fs_env):
        res = fs_env.bash_exec("super_evil_unknown_tool --all", timeout=10)
        assert res.get("status") == "error"

    def test_injection_pattern_denied(self, fs_env):
        res = fs_env.bash_exec("echo $(rm -rf /)", timeout=10)
        assert res.get("status") == "error"
