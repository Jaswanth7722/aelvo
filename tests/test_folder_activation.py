"""Folder-based activation tests — `Aelvo` opens any folder, not named workspaces.

Verifies the post-workspace-model semantics:
  * ``boot_backend`` with no folder opens the current working directory.
  * ``boot_backend`` with a folder opens that folder (any directory).
  * Per-folder state (memory DB, anchor, backups) lives in a hidden
    ``<folder>/.aelvo/`` directory.
  * ``_resolve_folder_and_prompt`` distinguishes an existing directory (open
    it) from a prompt (one-shot).
"""

from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, patch


def _boot_backend(workspace_dir: str = ""):
    """Import lazily so boot side effects don't hit every test file.

    ``detect_provider`` / ``init_provider_runtime`` are stubbed so the boot
    is hermetic (no real provider registry, vault, or network).
    """
    from cli.boot import boot_backend

    with (
        patch("core.startup.detect_provider", return_value=(None, None, None, None)),
        patch("core.provider_runtime.init_provider_runtime", new=AsyncMock(return_value=None)),
    ):
        return boot_backend(workspace_dir=workspace_dir)


class TestBootBackendFolderSemantics:
    def test_default_opens_current_directory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        backend = asyncio.run(_boot_backend())
        try:
            assert os.path.abspath(backend["workspace_path"]) == os.path.abspath(str(tmp_path))
            assert str(backend["fs"].base_path).replace("\\", "/") == os.path.abspath(str(tmp_path)).replace("\\", "/")
            # State must be inside the hidden .aelvo/ dir, not the folder root.
            assert os.path.basename(os.path.dirname(backend["db_path"])) == ".aelvo"
            assert os.path.basename(os.path.dirname(backend["aelvo_kernel"].db_path)) == ".aelvo"
        finally:
            _close_backend(backend)

    def test_opens_any_folder(self, tmp_path):
        folder = tmp_path / "my_project"
        folder.mkdir()
        (folder / "README.md").write_text("hi", encoding="utf-8")

        backend = asyncio.run(_boot_backend(workspace_dir=str(folder)))
        try:
            assert os.path.abspath(backend["workspace_path"]) == os.path.abspath(str(folder))
            # The filesystem jail points at the opened folder itself.
            assert os.path.abspath(backend["fs"].base_path) == os.path.abspath(str(folder))
            # No stray memory.db in the user's project root.
            assert not (folder / "memory.db").exists()
            assert (folder / ".aelvo" / "anchor.md").exists()
            assert os.path.basename(os.path.dirname(backend["db_path"])) == ".aelvo"
        finally:
            _close_backend(backend)

    def test_anchor_and_state_scaffolded_in_aelvo(self, tmp_path):
        folder = tmp_path / "fresh"
        folder.mkdir()
        backend = asyncio.run(_boot_backend(workspace_dir=str(folder)))
        try:
            state = folder / ".aelvo"
            assert state.is_dir()
            assert (state / "anchor.md").is_file()
            assert "constraints:" in (state / "anchor.md").read_text(encoding="utf-8")
            assert (state / "backups").is_dir()
        finally:
            _close_backend(backend)

    def test_folder_switcher_moves_state(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        import main as _main

        backend = asyncio.run(_boot_backend())
        try:
            new_folder = tmp_path / "other"
            new_folder.mkdir()
            resolved = _main.set_active_workspace(str(new_folder))
            assert resolved == os.path.abspath(str(new_folder))
            assert _main.WORKSPACE_PATH == os.path.abspath(str(new_folder))
            assert os.path.basename(os.path.dirname(_main.DB_PATH)) == ".aelvo"
            assert (new_folder / ".aelvo" / "anchor.md").exists()
        finally:
            _close_backend(backend)


def _close_backend(backend):
    """Best-effort teardown (same as cli/__main__ does on exit)."""
    for obj in (backend.get("aelvo_kernel"), backend.get("memory_engine")):
        try:
            obj.conn.close()
        except Exception:
            pass
        try:
            obj.db.close()
        except Exception:
            pass


class TestResolveBootFolder:
    """Web/full boot (main_async) must share the CLI's folder semantics."""

    def test_default_is_cwd(self, tmp_path, monkeypatch):
        import main as _main

        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("AELVO_FOLDER", raising=False)
        monkeypatch.delenv("AELVO_PROJECT", raising=False)
        assert _main._resolve_boot_folder() == os.getcwd()

    def test_aelvo_folder_env_wins(self, tmp_path, monkeypatch):
        import main as _main

        folder = tmp_path / "proj"
        folder.mkdir()
        monkeypatch.setenv("AELVO_FOLDER", str(folder))
        monkeypatch.setenv("AELVO_PROJECT", str(tmp_path))  # ignored: FOLDER wins
        assert _main._resolve_boot_folder() == os.path.abspath(str(folder))

    def test_aelvo_project_used_as_folder_path(self, tmp_path, monkeypatch):
        import main as _main

        folder = tmp_path / "proj"
        folder.mkdir()
        monkeypatch.delenv("AELVO_FOLDER", raising=False)
        monkeypatch.setenv("AELVO_PROJECT", str(folder))
        assert _main._resolve_boot_folder() == os.path.abspath(str(folder))

    def test_missing_folder_falls_back_to_cwd(self, tmp_path, monkeypatch):
        import main as _main

        monkeypatch.delenv("AELVO_FOLDER", raising=False)
        monkeypatch.setenv("AELVO_PROJECT", str(tmp_path / "does_not_exist"))
        monkeypatch.chdir(tmp_path)
        assert _main._resolve_boot_folder() == os.getcwd()

    def test_main_async_uses_folder_state(self, tmp_path, monkeypatch):
        """main_async resolves to the folder and scaffolds .aelvo/ state."""
        from unittest.mock import patch as _patch

        folder = tmp_path / "webproj"
        folder.mkdir()
        monkeypatch.delenv("AELVO_FOLDER", raising=False)
        monkeypatch.setenv("AELVO_PROJECT", str(folder))
        monkeypatch.chdir(tmp_path)

        # Stub the heavy boot pieces; assert only the folder resolution + state.
        import main as _main

        with _patch("core.startup.detect_provider", return_value=(None, None, None, None)), \
             _patch("main.init_provider_runtime", new=AsyncMock(return_value=None)), \
             _patch("core.registry.MODEL_REGISTRY", {}):
            # main_async is huge; directly verify the same helpers it now uses.
            target = _main._resolve_boot_folder()
            assert target == os.path.abspath(str(folder))
            _ws = os.path.basename(os.path.normpath(target))
            _main._scaffold_folder_state(target, _ws)
            assert (folder / ".aelvo" / "anchor.md").exists()
            assert os.path.basename(os.path.dirname(
                _main._folder_state_paths(target)[0]
            )) == ".aelvo"


class TestResolveFolderAndPrompt:
    def _args(self, prompt=None, workspace="", ask=""):
        return type("Args", (), {"prompt": prompt or [], "workspace": workspace, "ask": ask})()

    def test_no_args_opens_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from cli.__main__ import _resolve_folder_and_prompt

        folder, prompt = _resolve_folder_and_prompt(self._args())
        assert folder == ""
        assert prompt == ""

    def test_existing_dir_positional_opens_folder(self, tmp_path):
        from cli.__main__ import _resolve_folder_and_prompt

        folder = tmp_path / "proj"
        folder.mkdir()
        res_folder, prompt = _resolve_folder_and_prompt(self._args(prompt=[str(folder)]))
        assert res_folder == os.path.abspath(str(folder))
        assert prompt == ""

    def test_relative_dir_positional_opens_folder(self, tmp_path, monkeypatch):
        """Regression: `Aelvo ./proj` must open ./proj, not double-resolve it
        into proj/proj (the npm launcher keeps cwd = the shell cwd, so the
        CLI resolves the relative path once against it)."""
        from cli.__main__ import _resolve_folder_and_prompt

        (tmp_path / "proj").mkdir()
        monkeypatch.chdir(tmp_path)
        res_folder, prompt = _resolve_folder_and_prompt(self._args(prompt=["./proj"]))
        assert res_folder == os.path.abspath(str(tmp_path / "proj"))
        assert not (tmp_path / "proj" / "proj").exists()
        assert prompt == ""

    def test_non_dir_positional_is_one_shot_prompt(self, tmp_path):
        from cli.__main__ import _resolve_folder_and_prompt

        res_folder, prompt = _resolve_folder_and_prompt(
            self._args(prompt=["fix the auth bug"])
        )
        assert res_folder == ""
        assert prompt == "fix the auth bug"

    def test_ask_wins_over_positionals(self, tmp_path):
        from cli.__main__ import _resolve_folder_and_prompt

        folder = tmp_path / "proj"
        folder.mkdir()
        res_folder, prompt = _resolve_folder_and_prompt(
            self._args(prompt=[str(folder)], ask="do the thing")
        )
        assert prompt == "do the thing"

    def test_workspace_flag_with_prompt(self, tmp_path):
        from cli.__main__ import _resolve_folder_and_prompt

        folder = tmp_path / "proj"
        folder.mkdir()
        res_folder, prompt = _resolve_folder_and_prompt(
            self._args(prompt=["fix it"], workspace=str(folder))
        )
        assert res_folder == os.path.abspath(str(folder))
        assert prompt == "fix it"
