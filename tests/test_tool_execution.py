"""tests/test_tool_execution.py — Phase 14: Advanced Tool Execution & Persistent Sandbox

Tests the ToolExecutionRegistry, PersistentSandboxSession, and their
integration patterns.
"""

import asyncio
import os
import json
import tempfile
import time
import pytest
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, AsyncMock, patch

from core.execution.tool_registry import (
    ToolExecutionRegistry,
    ToolSpec,
    ToolResult,
    CacheEntry,
    RetryPolicy,
    ToolCategory,
    ExecutionStrategy,
)
from core.execution.sandbox_session import (
    PersistentSandboxSession,
    SandboxSessionState,
    SandboxSessionCheckpoint,
    FileChangeRecord,
    SessionFileChange,
    SessionStatus,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def registry() -> ToolExecutionRegistry:
    return ToolExecutionRegistry()


@pytest.fixture
def mock_fs():
    """A mock AelvoFileSystem-like object."""
    fs = MagicMock()
    fs.read_file.return_value = {"status": "success", "data": "file content"}
    return fs


@pytest.fixture
def session(mock_fs) -> PersistentSandboxSession:
    return PersistentSandboxSession(
        filesystem=mock_fs,
        workspace_root="/tmp/test_workspace",
    )


# ============================================================================
# ToolSpec & ToolResult Tests
# ============================================================================


class TestToolSpec:
    """ToolSpec dataclass creation and defaults."""

    def test_create_minimal(self):
        spec = ToolSpec(name="test_tool")
        assert spec.name == "test_tool"
        assert spec.category == ToolCategory.UTILITY
        assert spec.timeout == 30.0
        assert spec.retry_policy == RetryPolicy.NO_RETRY

    def test_create_full(self):
        spec = ToolSpec(
            name="read_file",
            category=ToolCategory.FILE_OPERATION,
            description="Read a file from disk",
            timeout=10.0,
            retry_policy=RetryPolicy.RETRY_ON_FAILURE,
            max_retries=3,
            cache_ttl=60.0,
            tags=["read", "file"],
        )
        assert spec.name == "read_file"
        assert spec.category == ToolCategory.FILE_OPERATION
        assert spec.cache_ttl == 60.0
        assert "read" in spec.tags


class TestToolResult:
    """ToolResult dataclass creation and helpers."""

    def test_create_success(self):
        result = ToolResult(
            tool_name="test",
            status="success",
            output="done",
            duration_ms=10.0,
        )
        assert result.is_success is True
        assert result.is_error is False
        assert result.cached is False

    def test_create_error(self):
        result = ToolResult(
            tool_name="test",
            status="error",
            error="Something broke",
        )
        assert result.is_success is False
        assert result.is_error is True

    def test_to_dict(self):
        result = ToolResult(
            tool_name="read_file",
            status="success",
            output="content",
            duration_ms=5.5,
        )
        d = result.to_dict()
        assert d["tool_name"] == "read_file"
        assert d["status"] == "success"
        assert d["duration_ms"] == 5.5


class TestCacheEntry:
    """CacheEntry TTL tracking."""

    def test_not_expired(self):
        entry = CacheEntry(
            result=ToolResult(tool_name="test", status="success"),
        )
        assert entry.is_expired(60.0) is False

    def test_expired(self):
        entry = CacheEntry(
            result=ToolResult(tool_name="test", status="success"),
            created_at=time.time() - 100,
        )
        assert entry.is_expired(10.0) is True


# ============================================================================
# ToolExecutionRegistry Tests
# ============================================================================


class TestToolExecutionRegistry:
    """ToolExecutionRegistry registration and execution."""

    def test_register(self, registry):
        """Register a tool with spec and handler."""
        spec = ToolSpec(name="greet", description="Says hello")
        async def handler(name: str = "world"):
            return f"Hello, {name}!"

        registry.register(spec, handler)
        assert registry.is_registered("greet") is True
        assert registry.count() == 1

        retrieved = registry.get_spec("greet")
        assert retrieved is not None
        assert retrieved.name == "greet"

    def test_register_sync_handler(self, registry):
        """Register with a sync handler."""
        spec = ToolSpec(name="sync_tool")
        def handler():
            return "sync result"

        registry.register(spec, handler)
        assert registry.is_registered("sync_tool")

    def test_unregister(self, registry):
        """Unregister removes a tool."""
        spec = ToolSpec(name="temp")
        async def handler(): pass
        registry.register(spec, handler)
        assert registry.unregister("temp") is True
        assert registry.is_registered("temp") is False
        assert registry.unregister("nonexistent") is True  # no-op

    def test_get_handler(self, registry):
        """get_handler returns the registered handler."""
        spec = ToolSpec(name="test")
        async def handler(): pass
        registry.register(spec, handler)
        assert registry.get_handler("test") is handler
        assert registry.get_handler("nonexistent") is None

    def test_list_tools(self, registry):
        """list_tools returns filtered lists."""
        registry.register(ToolSpec(name="read", category=ToolCategory.FILE_OPERATION, tags=["file"]), lambda: None)
        registry.register(ToolSpec(name="search", category=ToolCategory.CODE_ANALYSIS, tags=["code"]), lambda: None)
        registry.register(ToolSpec(name="write", category=ToolCategory.FILE_OPERATION, tags=["file"]), lambda: None)

        all_tools = registry.list_tools()
        assert len(all_tools) == 3

        file_tools = registry.list_tools(category=ToolCategory.FILE_OPERATION)
        assert len(file_tools) == 2

        tagged = registry.list_tools(tags=["file"])
        assert len(tagged) == 2

    @pytest.mark.asyncio
    async def test_execute_success(self, registry):
        """Execute a registered tool successfully."""
        spec = ToolSpec(name="greet")
        async def handler(name: str = "world"):
            return {"status": "success", "logs": f"Hello, {name}!"}

        registry.register(spec, handler)
        result = await registry.execute("greet", {"name": "Alice"})

        assert result.is_success
        assert "Alice" in result.output
        assert result.duration_ms > 0

    @pytest.mark.asyncio
    async def test_execute_unknown_tool(self, registry):
        """Execute returns error for unknown tool."""
        result = await registry.execute("nonexistent")
        assert result.is_error
        assert "Unknown tool" in result.error

    @pytest.mark.asyncio
    async def test_execute_timeout(self, registry):
        """Execute handles timeout correctly."""
        spec = ToolSpec(name="slow", timeout=0.1)
        async def handler():
            await asyncio.sleep(10)
            return "done"

        registry.register(spec, handler)
        result = await registry.execute("slow")

        assert result.is_error
        assert "timed out" in result.error

    @pytest.mark.asyncio
    async def test_execute_raises_exception(self, registry):
        """Execute handles handler exceptions."""
        spec = ToolSpec(name="broken")
        async def handler():
            raise RuntimeError("Internal error")

        registry.register(spec, handler)
        result = await registry.execute("broken")

        assert result.is_error
        assert "Internal error" in result.error

    @pytest.mark.asyncio
    async def test_retry_on_failure(self, registry):
        """Retry policy retries on failure."""
        spec = ToolSpec(
            name="flaky",
            retry_policy=RetryPolicy.RETRY_ON_FAILURE,
            max_retries=2,
            retry_delay=0.05,
        )
        attempt_count = [0]

        async def handler():
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                return {"status": "error", "logs": "Not ready yet"}
            return {"status": "success", "logs": "Succeeded on attempt 3"}

        registry.register(spec, handler)
        result = await registry.execute("flaky")

        assert result.is_success
        assert attempt_count[0] == 3
        assert result.retry_attempt == 2  # 0-indexed

    @pytest.mark.asyncio
    async def test_retry_exhausted(self, registry):
        """Retry gives up after max_retries."""
        spec = ToolSpec(
            name="always_broken",
            retry_policy=RetryPolicy.RETRY_ON_FAILURE,
            max_retries=2,
            retry_delay=0.05,
        )

        async def handler():
            return {"status": "error", "logs": "Always fails"}

        registry.register(spec, handler)
        result = await registry.execute("always_broken")

        assert result.is_error
        assert "3 attempt" in result.error or "failed" in result.error

    @pytest.mark.asyncio
    async def test_cache_hit(self, registry):
        """Cached results are returned on repeat calls."""
        call_count = [0]

        spec = ToolSpec(
            name="cached_tool",
            cache_ttl=60.0,
        )
        async def handler(value: str = "default"):
            call_count[0] += 1
            return {"status": "success", "data": f"result:{value}"}

        registry.register(spec, handler)

        result1 = await registry.execute("cached_tool", {"value": "test"})
        assert result1.is_success
        assert call_count[0] == 1

        result2 = await registry.execute("cached_tool", {"value": "test"})
        assert result2.is_success
        assert result2.cached is True
        assert call_count[0] == 1  # Handler not called again

    @pytest.mark.asyncio
    async def test_cache_bypass(self, registry):
        """Cache is bypassed with bypass_cache=True."""
        call_count = [0]

        spec = ToolSpec(name="cached", cache_ttl=60.0)
        async def handler():
            call_count[0] += 1
            return "ok"

        registry.register(spec, handler)

        await registry.execute("cached")
        await registry.execute("cached")
        await registry.execute("cached", bypass_cache=True)

        assert call_count[0] == 2  # First call + bypassed third call

    @pytest.mark.asyncio
    async def test_cache_invalidation(self, registry):
        """invalidate_cache clears entries."""
        spec = ToolSpec(name="tool", cache_ttl=60.0)
        async def handler(): return "ok"

        registry.register(spec, handler)
        await registry.execute("tool")

        assert registry.get_cache_stats()["total_entries"] == 1
        registry.invalidate_cache()
        assert registry.get_cache_stats()["total_entries"] == 0

    @pytest.mark.asyncio
    async def test_execute_batch(self, registry):
        """execute_batch runs multiple tools respecting strategy."""
        spec_a = ToolSpec(name="tool_a", strategy=ExecutionStrategy.DIRECT)
        spec_b = ToolSpec(name="tool_b", strategy=ExecutionStrategy.PARALLEL)
        spec_c = ToolSpec(name="tool_c", strategy=ExecutionStrategy.DIRECT)

        order = []

        async def handler_a():
            order.append("a")
            return "a"

        async def handler_b():
            await asyncio.sleep(0.05)
            order.append("b")
            return "b"

        async def handler_c():
            order.append("c")
            return "c"

        registry.register(spec_a, handler_a)
        registry.register(spec_b, handler_b)
        registry.register(spec_c, handler_c)

        results = await registry.execute_batch([
            ("tool_a", {}),
            ("tool_b", {}),
            ("tool_c", {}),
        ])

        assert len(results) == 3
        assert all(r.is_success for r in results)

    def test_get_statistics(self, registry):
        """get_statistics returns execution stats."""
        stats = registry.get_statistics("nonexistent")
        assert stats["total"] == 0

    def test_snapshot(self, registry):
        """snapshot returns registry state."""
        snapshot = registry.snapshot()
        assert "registered_tools" in snapshot
        assert "cache_entries" in snapshot

    def test_to_terminal_display(self, registry):
        """to_terminal_display returns human-readable output."""
        spec = ToolSpec(name="test_tool", category=ToolCategory.FILE_OPERATION)
        async def handler(): pass
        registry.register(spec, handler)

        display = registry.to_terminal_display()
        assert "TOOL EXECUTION REGISTRY" in display
        assert "file_operation" in display
        assert "1 registered" in display

    @pytest.mark.asyncio
    async def test_execute_returns_tool_result_directly(self, registry):
        """Handler returning a ToolResult directly works."""
        spec = ToolSpec(name="direct")
        async def handler():
            return ToolResult(
                tool_name="direct",
                status="success",
                output="direct result",
                metadata={"custom": "value"},
            )

        registry.register(spec, handler)
        result = await registry.execute("direct")

        assert result.is_success
        assert result.output == "direct result"
        assert result.metadata.get("custom") == "value"

    @pytest.mark.asyncio
    async def test_execute_retry_on_timeout_policy(self, registry):
        """RETRY_ON_TIMEOUT only retries on timeout errors."""
        spec = ToolSpec(
            name="timeout_tool",
            retry_policy=RetryPolicy.RETRY_ON_TIMEOUT,
            max_retries=2,
            retry_delay=0.05,
        )
        attempt_count = [0]

        async def handler():
            attempt_count[0] += 1
            raise asyncio.TimeoutError("timed out")

        registry.register(spec, handler)
        result = await registry.execute("timeout_tool")

        assert result.is_error
        assert attempt_count[0] == 3  # initial + 2 retries


# ============================================================================
# SandboxSessionState Tests
# ============================================================================


class TestSandboxSessionState:
    """SandboxSessionState dataclass."""

    def test_create(self):
        state = SandboxSessionState()
        assert state.cwd == "/"
        assert state.env_vars == {}
        assert state.tool_executions == 0

    def test_snapshot(self):
        state = SandboxSessionState()
        state.tool_executions = 5
        state.files_created.add("/tmp/test.py")
        snap = state.snapshot()
        assert snap["tool_executions"] == 5
        assert "/tmp/test.py" in snap["files_created"]


class TestSandboxSessionCheckpoint:
    """SandboxSessionCheckpoint dataclass."""

    def test_create(self):
        ckpt = SandboxSessionCheckpoint(
            checkpoint_id="ckpt_001",
            label="Before risky operation",
        )
        assert ckpt.checkpoint_id == "ckpt_001"
        assert ckpt.label == "Before risky operation"
        assert ckpt.tool_count == 0

    def test_to_dict(self):
        ckpt = SandboxSessionCheckpoint(
            checkpoint_id="ckpt_001",
            label="test",
            backup_paths={"/a": "/backup/a"},
            tool_count=3,
        )
        d = ckpt.to_dict()
        assert d["checkpoint_id"] == "ckpt_001"
        assert d["files_backed_up"] == 1
        assert d["tool_count"] == 3


class TestFileChangeRecord:
    """FileChangeRecord dataclass."""

    def test_create(self):
        record = FileChangeRecord(
            path="/tmp/test.py",
            change_type=SessionFileChange.CREATED,
            tool_name="write_file",
        )
        assert record.change_type == SessionFileChange.CREATED
        assert record.tool_name == "write_file"

    def test_to_dict(self):
        record = FileChangeRecord(
            path="/tmp/test.py",
            change_type=SessionFileChange.MODIFIED,
            previous_content="old",
            new_content="new",
        )
        d = record.to_dict()
        assert d["change_type"] == "modified"
        assert d["previous_content_length"] == 3


# ============================================================================
# PersistentSandboxSession Tests
# ============================================================================


class TestPersistentSandboxSession:
    """PersistentSandboxSession state tracking and lifecycle."""

    def test_create(self, mock_fs):
        """Create a new session."""
        session = PersistentSandboxSession(
            filesystem=mock_fs,
            workspace_root="/workspace",
        )
        assert len(session.session_id) == 16
        assert session.status == SessionStatus.ACTIVE
        assert session.state.tool_executions == 0
        assert session.state.cwd.replace("\\", "/").endswith("/workspace")
        assert session.is_dirty is False

    def test_properties(self, session):
        """Properties return correct values."""
        assert session.file_change_count == 0
        assert session.is_dirty is False
        assert len(session.session_id) == 16

    @pytest.mark.asyncio
    async def test_tool_lifecycle_creates_file(self, session, mock_fs):
        """before_tool/after_tool tracks file creation."""
        mock_fs.read_file.return_value = {"status": "error", "logs": "Not found"}

        await session.before_tool("write_file", {"path": "/new_file.py"})
        await session.after_tool("write_file", {"status": "success", "logs": "Wrote file"})

        assert session.state.tool_executions == 1
        assert "/new_file.py" in session.state.files_created
        assert session.is_dirty is True

    @pytest.mark.asyncio
    async def test_tool_lifecycle_modifies_file(self, session, mock_fs):
        """before_tool/after_tool tracks file modification."""
        mock_fs.read_file.return_value = {"status": "success", "data": "old content"}

        await session.before_tool("edit_file_block", {"path": "/existing.py"})
        await session.after_tool("edit_file_block", {"status": "success"})

        assert session.state.tool_executions == 1
        assert "/existing.py" in session.state.files_modified
        assert session.file_change_count == 1

    @pytest.mark.asyncio
    async def test_tool_lifecycle_tracks_errors(self, session):
        """after_tool tracks errors."""
        await session.before_tool("bash_exec", {"command": "ls"})
        await session.after_tool("bash_exec", {"status": "error", "logs": "Command failed"})

        assert session.state.errors == 1
        assert session.state.tool_executions == 1

    @pytest.mark.asyncio
    async def test_tool_lifecycle_no_pending_backup(self, session):
        """Tools without file changes work fine."""
        await session.before_tool("search", {"query": "test"})
        await session.after_tool("search", {"status": "success", "logs": "Found 3 results"})

        assert session.state.tool_executions == 1
        assert session.file_change_count == 0

    def test_create_checkpoint(self, session, tmp_path):
        """create_checkpoint captures session state."""
        session.state.tool_executions = 5
        session.state.files_created.add(str(tmp_path / "test.py"))

        ckpt = session.create_checkpoint("Before test")
        assert ckpt.checkpoint_id is not None
        assert ckpt.label == "Before test"
        assert ckpt.tool_count == 5
        assert session.status == SessionStatus.CHECKPOINTED

    def test_get_checkpoint(self, session):
        """get_checkpoint retrieves saved checkpoint."""
        ckpt = session.create_checkpoint("Test checkpoint")
        retrieved = session.get_checkpoint(ckpt.checkpoint_id)
        assert retrieved is not None
        assert retrieved.checkpoint_id == ckpt.checkpoint_id

        assert session.get_checkpoint("nonexistent") is None

    def test_list_checkpoints(self, session):
        """list_checkpoints returns all checkpoints in order."""
        session.create_checkpoint("First")
        import time
        time.sleep(0.05)
        session.create_checkpoint("Second")
        time.sleep(0.05)
        session.create_checkpoint("Third")

        checkpoints = session.list_checkpoints()
        assert len(checkpoints) == 3
        assert checkpoints[0].label == "Third"  # Most recent first

    def test_rollback_to_checkpoint(self, session, tmp_path):
        """rollback_to_checkpoint restores session state."""
        session.state.tool_executions = 5
        session.state.files_created.add(str(tmp_path / "test.py"))

        ckpt = session.create_checkpoint("Before changes")

        # Make more changes
        session.state.tool_executions = 10
        session.state.files_created.add(str(tmp_path / "new.py"))

        # Rollback
        result = session.rollback_to_checkpoint(ckpt.checkpoint_id)
        assert result is True
        assert session.state.tool_executions == 5
        assert str(tmp_path / "new.py") not in session.state.files_created
        assert session.status == SessionStatus.ROLLED_BACK

    def test_rollback_to_nonexistent_checkpoint(self, session):
        """rollback returns False for unknown checkpoint."""
        result = session.rollback_to_checkpoint("nonexistent")
        assert result is False

    def test_clear(self, session):
        """clear resets session state."""
        session.state.tool_executions = 10
        session.state.files_created.add("/tmp/test.py")
        session.create_checkpoint("test")

        session.clear()
        assert session.state.tool_executions == 0
        assert session.file_change_count == 0
        assert len(session.list_checkpoints()) == 0

    def test_close(self, session):
        """close marks session as closed."""
        session.close()
        assert session.status == SessionStatus.CLOSED
        assert session.state.tool_executions == 0

    def test_get_file_changes(self, session, mock_fs):
        """get_file_changes returns tracked changes."""
        mock_fs.read_file.return_value = {"status": "error", "logs": "Not found"}

        async def run_tool():
            await session.before_tool("write_file", {"path": "/file1.py"})
            await session.after_tool("write_file", {"status": "success"})

        asyncio.run(run_tool())

        changes = session.get_file_changes()
        assert len(changes) == 1

        created = session.get_file_changes(change_type=SessionFileChange.CREATED)
        assert len(created) == 1

        modified = session.get_file_changes(change_type=SessionFileChange.MODIFIED)
        assert len(modified) == 0

    def test_get_summary(self, session):
        """get_summary returns human-readable summary."""
        summary = session.get_summary()
        assert summary["session_id"] == session.session_id[:12]
        assert summary["tool_executions"] == 0

    def test_snapshot(self, session):
        """snapshot returns full state."""
        snap = session.snapshot()
        assert snap["session_id"] == session.session_id[:12]
        assert "state" in snap
        assert "checkpoints" in snap

    def test_to_terminal_display(self, session):
        """to_terminal_display returns human-readable output."""
        display = session.to_terminal_display()
        assert "SANDBOX SESSION" in display
        assert session.session_id[:8] in display

    def test_different_sessions_have_different_ids(self, mock_fs):
        """Each session gets a unique ID."""
        s1 = PersistentSandboxSession(filesystem=mock_fs)
        s2 = PersistentSandboxSession(filesystem=mock_fs)
        assert s1.session_id != s2.session_id

    @pytest.mark.asyncio
    async def test_rollback_restores_files(self, session, tmp_path):
        """Rollback actually restores file content from backup."""
        # Create a real file
        test_file = tmp_path / "test_file.py"
        test_file.write_text("original content")

        session.state.files_modified.add(str(test_file))
        session.state.tool_executions = 1

        ckpt = session.create_checkpoint("Before changes")

        # Modify the file (simulating changes made during session)
        test_file.write_text("modified content")
        session.state.files_modified.add(str(test_file))
        session.state.tool_executions = 2

        # Rollback
        result = session.rollback_to_checkpoint(ckpt.checkpoint_id)
        assert result is True

        # File should be in the backup_paths for restoration
        assert str(test_file) in ckpt.backup_paths

        # Restore the file from backup
        import shutil
        backup_path = ckpt.backup_paths[str(test_file)]
        shutil.copy2(backup_path, str(test_file))
        assert test_file.read_text() == "original content"
