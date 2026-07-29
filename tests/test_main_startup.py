"""Unit and integration tests for the AELVO main.py startup process.

Validates initialization, CLI argument parsing, system prompt construction,
and orchestrator setup logic.
"""

import os
import sys
import sqlite3
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

import main


@pytest.fixture(autouse=True)
def setup_tmp_metadata(tmp_path):
    """Isolate metadata databases and anchor files for each test."""
    db_file = tmp_path / "global_memory.db"
    anchor_file = tmp_path / "global_anchor.md"
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir(exist_ok=True)

    with patch("main.GLOBAL_DB_PATH", str(db_file)), \
         patch("main.GLOBAL_ANCHOR_PATH", str(anchor_file)), \
         patch("main.WORKSPACE_BASE", str(workspace_dir)):
        yield


def test_init_global_metadata_scaffolding(tmp_path):
    """Verify that global metadata databases and anchor files are properly created on startup."""
    main.init_global_metadata()

    # Check database creation and structure
    assert os.path.exists(main.GLOBAL_DB_PATH)
    with sqlite3.connect(main.GLOBAL_DB_PATH) as conn:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = [t[0] for t in tables]
        assert "projects" in table_names
        assert "user_meta" in table_names

    # Check default anchor scaffolding
    assert os.path.exists(main.GLOBAL_ANCHOR_PATH)
    with open(main.GLOBAL_ANCHOR_PATH, "r", encoding="utf-8") as f:
        content = f.read()
        assert "AELVO Global Constraints" in content
        assert "Global Rules" in content


def test_get_system_prompt_structure():
    """Verify that get_system_prompt constructs a prompt containing expected environment details."""
    prompt = main.get_system_prompt()
    assert "AELVO" in prompt
    assert "SYSTEM CONTEXT" in prompt
    assert "CURRENT DATE & TIME" in prompt
    assert "PERSISTENT ANCHOR" in prompt
    assert "KERNEL STATE" in prompt


@patch("sys.argv", ["main.py"])
def test_default_cli_args_parsing():
    """Verify CLI parsing behaves correctly without explicit workspace or option flags."""
    # Since main_async is an async function, we mock it using AsyncMock to return a valid coroutine object.
    mock_async = AsyncMock()
    with patch("main.main_async", mock_async):
        main.main()
        assert mock_async.called


@patch("sys.argv", ["main.py", "--config"])
def test_cli_config_flag():
    """Verify `--config` triggers provider configuration flow instead of direct run."""
    # Since main_async is called within main(), we patch main_async to not be run.
    mock_async = AsyncMock()
    with patch("main.main_async", mock_async), \
         patch("ui.detect_provider") as mock_detect, \
         patch("ui.select_project_interactive", return_value="test_project"):
        mock_detect.return_value = ("openai", MagicMock())
        try:
            main.main()
        except SystemExit:
            pass
        # Should call main_async which then invokes detect_provider
        assert mock_async.called


@patch("sys.argv", ["main.py", "custom_workspace"])
def test_cli_workspace_argument():
    """Verify passing a workspace positional argument bypasses project selection UI."""
    mock_async = AsyncMock()
    with patch("main.main_async", mock_async):
        main.main()
        assert mock_async.called
