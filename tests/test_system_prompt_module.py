"""Unit tests for core/system_prompt.py.

The system prompt module is standalone (no main.py dependency) so these
tests exercise the dynamic prompt generation, workspace path sync via
configure_paths(), and graceful degradation when the state DB / anchor
file are missing.
"""

import os
import shutil
import sqlite3
import tempfile

import pytest

from core.system_prompt import get_system_prompt, configure_paths, DB_PATH, ANCHOR_PATH, WORKSPACE_PATH

# Snapshot the module's initial defaults once so the autouse fixture can
# restore them after each test (reading them at teardown time would see
# the already-mutated values and become a no-op).
_DEFAULT_DB_PATH = DB_PATH
_DEFAULT_ANCHOR_PATH = ANCHOR_PATH
_DEFAULT_WORKSPACE_PATH = WORKSPACE_PATH


@pytest.fixture
def workspace():
    """A throwaway workspace dir. Uses mkdtemp (not pytest tmp_path, which
    is broken in this Windows environment)."""
    d = tempfile.mkdtemp(prefix="aelvo_prompt_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(autouse=True)
def restore_default_paths():
    """Restore the module's default paths after each test."""
    yield
    configure_paths(
        db_path=_DEFAULT_DB_PATH,
        anchor_path=_DEFAULT_ANCHOR_PATH,
        workspace_path=_DEFAULT_WORKSPACE_PATH,
    )


class TestPromptStructure:
    """Static structure of the generated prompt."""

    def test_prompt_contains_core_sections(self):
        prompt = get_system_prompt()
        for marker in (
            "AELVO",
            "SYSTEM CONTEXT",
            "CURRENT DATE & TIME",
            "PERSISTENT ANCHOR",
            "KERNEL STATE",
            "FORMAT 1",
            "FORMAT 2",
            "FORMAT 3",
            "ITERATIVE DEBUGGING PROTOCOL",
        ):
            assert marker in prompt, f"missing {marker!r} in prompt"

    def test_prompt_has_live_timestamp(self):
        prompt = get_system_prompt()
        import datetime

        today = datetime.datetime.now().strftime("%Y-%m-%d")
        assert today in prompt, "prompt should embed the current date"

    def test_prompt_includes_workspace_jail(self):
        prompt = get_system_prompt()
        assert "Workspace Jail" in prompt
        assert os.path.abspath(WORKSPACE_PATH) in prompt

    def test_prompt_accepts_user_query(self):
        # Signature compatibility: user_query is accepted (reserved for RAG).
        prompt = get_system_prompt(user_query="hello world")
        assert isinstance(prompt, str) and len(prompt) > 100

    def test_prompt_falls_back_when_no_workspace(self, workspace):
        """With a nonexistent state DB/anchor, the prompt still builds."""
        configure_paths(
            db_path=os.path.join(workspace, "missing.db"),
            anchor_path=os.path.join(workspace, "missing.md"),
            workspace_path=workspace,
        )
        prompt = get_system_prompt()
        assert "(empty)" in prompt
        assert "(none)" in prompt

    def test_prompt_has_codebase_understanding_protocol(self):
        """Codebase questions must trigger folder mapping + entry-file reads,
        never a generic "please provide more details" reply."""
        prompt = get_system_prompt()
        assert "CODEBASE QUESTIONS" in prompt
        assert "list_files" in prompt
        assert "project_tree" in prompt
        assert "README" in prompt
        assert "package.json" in prompt
        # The protocol names the user-facing trigger phrases and explicitly
        # bans the anti-pattern the transcript showed (the generic reply).
        assert "present the folder" in prompt
        assert "what is this project" in prompt
        assert "Do not reply" in prompt


class TestConfigurePaths:
    """configure_paths() updates where the prompt reads state and anchors."""

    def test_workspace_jail_updates(self, workspace):
        configure_paths(workspace_path=workspace)
        prompt = get_system_prompt()
        assert workspace in prompt
        assert "Workspace Jail" in prompt

    def test_kernel_state_injected(self, workspace):
        db = os.path.join(workspace, "memory.db")
        conn = sqlite3.connect(db)
        try:
            conn.execute("CREATE TABLE state (key TEXT, value TEXT)")
            conn.execute("INSERT INTO state VALUES ('DEV_NAME', 'TestDev')")
            conn.execute("INSERT INTO state VALUES ('runtime:noise', 'hidden')")
            conn.commit()
        finally:
            conn.close()

        configure_paths(db_path=db, workspace_path=workspace)
        prompt = get_system_prompt()
        assert "TestDev" in prompt
        assert "runtime:noise" not in prompt, "runtime:* state should be excluded"

    def test_anchor_constraints_injected(self, workspace):
        anchor = os.path.join(workspace, "anchor.md")
        with open(anchor, "w", encoding="utf-8") as f:
            f.write("---\nconstraints:\n  DEV_NAME: {value: Alice, locked: true}\n---\n")

        configure_paths(anchor_path=anchor, workspace_path=workspace)
        prompt = get_system_prompt()
        assert "Alice" in prompt
        assert "ANCHOR CONSTRAINTS" in prompt

    def test_partial_path_updates_only_change_those(self, workspace):
        """configure_paths(workspace_path=...) leaves db/anchor untouched."""
        before_db = _DEFAULT_DB_PATH
        configure_paths(workspace_path=workspace)
        assert DB_PATH == before_db


class TestModuleImports:
    """The module is importable standalone and re-exported by main."""

    def test_standalone_import(self):
        import importlib

        module = importlib.import_module("core.system_prompt")
        assert callable(module.get_system_prompt)
        assert callable(module.configure_paths)

    def test_main_reexports(self):
        import main

        assert main.get_system_prompt is get_system_prompt
        assert main.configure_paths is configure_paths
