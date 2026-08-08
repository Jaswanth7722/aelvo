"""Unit and integration tests for the AELVO main.py startup process.

Validates initialization, CLI argument parsing, system prompt construction,
and orchestrator setup logic.
"""

import os
import sqlite3
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

import main
import logging

log = logging.getLogger(__name__)



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
    """Verify `--config` starts the boot flow instead of direct run."""
    # Since main_async is called within main(), we patch main_async to not be run.
    mock_async = AsyncMock()
    with patch("main.main_async", mock_async), \
         patch("core.startup.select_project", return_value="test_project"), \
         patch("core.startup.detect_provider") as mock_detect:
        mock_detect.return_value = ("openai", MagicMock(), "key", "gpt-4o")
        try:
            main.main()
        except SystemExit as _ex:
            log.warning("Silenced exception: %s", _ex)
        # Should call main_async which then invokes select_project/detect_provider
        assert mock_async.called


@patch("sys.argv", ["main.py", "custom_workspace"])
def test_cli_workspace_argument():
    """Verify passing a workspace positional argument bypasses project selection UI."""
    mock_async = AsyncMock()
    with patch("main.main_async", mock_async):
        main.main()
        assert mock_async.called


# ==============================================================================
# Logging: the console must stay fully quiet by default
# ==============================================================================

def test_console_level_defaults_to_critical(monkeypatch):
    """Console log level defaults to CRITICAL so the TUI shows zero log noise.

    Regression: the console handler previously defaulted to ERROR, which
    spilled ``[ERROR] ... aelvo.orchestrator - Tool ... failed`` lines into
    the interactive prompt_toolkit UI on every tool hiccup.
    """
    monkeypatch.delenv("AELVO_LOG_LEVEL", raising=False)
    assert main._resolve_console_level() == logging.CRITICAL


def test_console_level_respects_env_override(monkeypatch):
    """AELVO_LOG_LEVEL still opts into verbose console logging."""
    monkeypatch.setenv("AELVO_LOG_LEVEL", "debug")
    assert main._resolve_console_level() == logging.DEBUG
    monkeypatch.setenv("AELVO_LOG_LEVEL", "error")
    assert main._resolve_console_level() == logging.ERROR


def test_console_level_ignores_bad_env(monkeypatch):
    """Invalid AELVO_LOG_LEVEL values fall back to the quiet default."""
    monkeypatch.setenv("AELVO_LOG_LEVEL", "banana")
    assert main._resolve_console_level() == logging.CRITICAL


def test_configure_logging_keeps_file_and_silences_console(monkeypatch, tmp_path):
    """_configure_logging must keep full file diagnostics while the console
    handler stays at CRITICAL — so ERROR records reach the log file only."""
    import logging as _logging

    # Point the file log into a temp dir and reconfigure.
    monkeypatch.setattr(main, "_LOG_FILE_PATH", str(tmp_path / "aelvo.log"))
    monkeypatch.delenv("AELVO_LOG_LEVEL", raising=False)

    captured = []
    original_handler = _logging.StreamHandler

    class _CaptureStreamHandler(original_handler):
        def emit(self, record):
            captured.append(record)
            super().emit(record)

    monkeypatch.setattr(_logging, "StreamHandler", _CaptureStreamHandler)

    main._configure_logging()

    root = _logging.getLogger()
    file_h = next((h for h in root.handlers if isinstance(h, _logging.FileHandler)), None)
    # FileHandler subclasses StreamHandler, so pick the non-file stream handler.
    console = next(
        (h for h in root.handlers
         if isinstance(h, _logging.StreamHandler) and not isinstance(h, _logging.FileHandler)),
        None,
    )

    assert file_h is not None, "file handler must exist"
    assert file_h.level <= _logging.INFO, "file handler must capture INFO+"
    assert console is not None, "console handler must exist"
    assert console.level == _logging.CRITICAL, "console handler must be CRITICAL"

    # An ERROR record must reach the FILE but NOT the console handler.
    probe = _logging.LogRecord(
        name="aelvo.orchestrator", level=_logging.ERROR,
        pathname=__file__, lineno=1, msg="probe", args=(), exc_info=None,
    )
    assert console.level > probe.levelno, "console would show ERROR — too loud"
    assert file_h.level <= probe.levelno, "file would drop ERROR — too quiet"
