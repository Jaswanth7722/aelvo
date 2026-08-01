"""core/execution/sandbox_session.py — Persistent Sandbox Session State

Phase 14: Persistent sandbox session that maintains state across tool
invocations, tracks filesystem changes, and supports checkpoint/rollback
at the session level.

Key components:
  - SandboxSessionState: Encapsulated session state (CWD, env vars, files changed)
  - SandboxSessionCheckpoint: A snapshot of session state for rollback
  - PersistentSandboxSession: Orchestrates session lifecycle, checkpoint/rollback
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from core.filesystem.automation import AelvoFileSystem

log = logging.getLogger("aelvo.core.execution.sandbox_session")


class SessionFileChange(str, Enum):
    """Type of change made to a file in a session."""
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"


class SessionStatus(str, Enum):
    """Lifecycle status of a sandbox session."""
    ACTIVE = "active"
    CHECKPOINTED = "checkpointed"
    ROLLED_BACK = "rolled_back"
    CLOSED = "closed"


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class FileChangeRecord:
    """Record of a filesystem change in a session."""

    path: str
    change_type: SessionFileChange
    previous_content: Optional[str] = None
    new_content: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    tool_name: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "change_type": self.change_type.value,
            "previous_content_length": len(self.previous_content) if self.previous_content else 0,
            "new_content_length": len(self.new_content) if self.new_content else 0,
            "timestamp": self.timestamp,
            "tool_name": self.tool_name,
        }


@dataclass
class SandboxSessionState:
    """Current state of a persistent sandbox session."""

    cwd: str = "/"
    """Current working directory within the session."""

    env_vars: Dict[str, str] = field(default_factory=dict)
    """Environment variables set in the session."""

    file_changes: List[FileChangeRecord] = field(default_factory=list)
    """All filesystem changes made during the session."""

    files_created: Set[str] = field(default_factory=set)
    """Set of file paths created in the session."""

    files_modified: Set[str] = field(default_factory=set)
    """Set of file paths modified in the session."""

    files_deleted: Set[str] = field(default_factory=set)
    """Set of file paths deleted in the session."""

    tool_executions: int = 0
    """Number of tools executed in this session."""

    last_tool: str = ""
    """Name of the last tool executed."""

    errors: int = 0
    """Number of errors encountered in the session."""

    def snapshot(self) -> Dict[str, Any]:
        return {
            "cwd": self.cwd,
            "env_vars": dict(self.env_vars),
            "files_created": sorted(self.files_created),
            "files_modified": sorted(self.files_modified),
            "files_deleted": sorted(self.files_deleted),
            "total_changes": len(self.file_changes),
            "tool_executions": self.tool_executions,
            "errors": self.errors,
        }


@dataclass
class SandboxSessionCheckpoint:
    """A snapshot of session state for rollback."""

    checkpoint_id: str
    """Unique identifier for this checkpoint."""

    label: str = ""
    """Human-readable label."""

    state_snapshot: Dict[str, Any] = field(default_factory=dict)
    """Snapshot of the session state at checkpoint time."""

    backup_paths: Dict[str, str] = field(default_factory=dict)
    """Mapping of original file paths to backup file paths."""

    created_at: float = field(default_factory=time.time)
    """When the checkpoint was created."""

    tool_count: int = 0
    """Number of tool executions at checkpoint time."""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id[:12],
            "label": self.label,
            "created_at": self.created_at,
            "tool_count": self.tool_count,
            "files_backed_up": len(self.backup_paths),
        }


# ============================================================================
# PersistentSandboxSession
# ============================================================================


class PersistentSandboxSession:
    """Persistent sandbox session with state tracking and checkpoint/rollback.

    Maintains state across multiple tool invocations within a session.
    Tracks all filesystem changes (create, modify, delete), supports
    taking checkpoints and rolling back to them, and maintains
    session-level metadata for auditing and recovery.

    Usage:
        fs = AelvoFileSystem(base_path, kernel)
        session = PersistentSandboxSession(fs, workspace_root="/workspace")

        # Session tracks state across tool calls
        await session.before_tool("read_file", {"path": "foo.py"})
        result = fs.read_file("foo.py")
        session.after_tool("read_file", result)

        # Checkpoint for safety
        ckpt = session.create_checkpoint("Before risky operation")

        # Rollback on failure
        session.rollback_to_checkpoint(ckpt.checkpoint_id)
    """

    def __init__(
        self,
        filesystem: AelvoFileSystem,
        workspace_root: str = "",
        session_id: Optional[str] = None,
    ):
        self._fs = filesystem
        self._workspace_root = Path(workspace_root).resolve() if workspace_root else None

        raw = session_id or f"session_{time.time()}_{id(self)}"
        self._session_id = hashlib.sha256(raw.encode()).hexdigest()[:16]

        self.state = SandboxSessionState()
        self.status: SessionStatus = SessionStatus.ACTIVE

        self._checkpoints: Dict[str, SandboxSessionCheckpoint] = {}
        self._backup_dir: Optional[Path] = None
        self._change_index: int = 0
        self._pending_backup: Optional[FileChangeRecord] = None

        # Set initial CWD from workspace root
        if self._workspace_root:
            self.state.cwd = str(self._workspace_root)

        log.info(
            "Sandbox session %s created (workspace=%s)",
            self._session_id[:12], self._workspace_root or "(none)",
        )

    # ── Properties ──────────────────────────────────────────────────

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def file_change_count(self) -> int:
        """Total number of filesystem changes in this session."""
        return len(self.state.file_changes)

    @property
    def is_dirty(self) -> bool:
        """Whether the session has made any filesystem changes."""
        return self.file_change_count > 0

    # ── Tool Lifecycle Hooks ────────────────────────────────────────

    async def before_tool(
        self,
        tool_name: str,
        args: Dict[str, Any],
    ) -> None:
        """Called before a tool executes.

        Records pre-execution state for tools that modify files.
        Tracks current working directory and env var changes.
        """
        self.state.last_tool = tool_name

        # Capture content of files that might be modified
        if tool_name in ("write_atomic", "edit_file_block", "write_file"):
            path = args.get("path", "")
            if path:
                try:
                    read_result = self._read_file_content(path)
                    if read_result is not None:
                        content = read_result.get("data", "") if isinstance(read_result, dict) else str(read_result)
                        self._pending_backup = FileChangeRecord(
                            path=path,
                            change_type=SessionFileChange.MODIFIED,
                            previous_content=content,
                            tool_name=tool_name,
                        )
                    else:
                        # File doesn't exist yet — will be a creation
                        self._pending_backup = FileChangeRecord(
                            path=path,
                            change_type=SessionFileChange.CREATED,
                            tool_name=tool_name,
                        )
                except Exception:
                    # File doesn't exist or can't be read — will be a creation
                    self._pending_backup = FileChangeRecord(
                        path=path,
                        change_type=SessionFileChange.CREATED,
                        tool_name=tool_name,
                    )

    async def after_tool(
        self,
        tool_name: str,
        result: Any,
    ) -> None:
        """Called after a tool executes.

        Records the result, updates session state, and tracks
        filesystem changes.
        """
        self.state.tool_executions += 1
        self.state.last_tool = tool_name

        if isinstance(result, dict) and result.get("status") == "error":
            self.state.errors += 1

        # Record filesystem change from pending backup
        if self._pending_backup is not None:
            record = self._pending_backup
            self.state.file_changes.append(record)

            if record.change_type == SessionFileChange.CREATED:
                self.state.files_created.add(record.path)
            elif record.change_type == SessionFileChange.MODIFIED:
                self.state.files_modified.add(record.path)
            elif record.change_type == SessionFileChange.DELETED:
                self.state.files_deleted.add(record.path)

            self._pending_backup = None

    # ── Checkpoint / Rollback ───────────────────────────────────────

    def create_checkpoint(self, label: str = "") -> SandboxSessionCheckpoint:
        """Create a checkpoint of the current session state.

        Saves a snapshot of the session state and backs up any
        files that have been modified so they can be restored on rollback.

        Args:
            label: Optional human-readable label.

        Returns:
            The created SandboxSessionCheckpoint.
        """
        raw = f"ckpt_{self._session_id}_{time.time()}_{len(self._checkpoints)}"
        ckpt_id = hashlib.sha256(raw.encode()).hexdigest()[:16]

        # Create backup directory
        backup_root = self._get_backup_path() / ckpt_id
        backup_root.mkdir(parents=True, exist_ok=True)

        # Backup modified and created files
        backup_paths: Dict[str, str] = {}
        for path in self.state.files_modified | self.state.files_created:
            if os.path.exists(path):
                backup_name = hashlib.sha256(path.encode()).hexdigest()[:16]
                backup_file = backup_root / backup_name
                try:
                    shutil.copy2(path, backup_file)
                    backup_paths[path] = str(backup_file)
                except Exception as e:
                    log.warning("Failed to backup '%s': %s", path, e)

        checkpoint = SandboxSessionCheckpoint(
            checkpoint_id=ckpt_id,
            label=label,
            state_snapshot=self.state.snapshot(),
            backup_paths=backup_paths,
            tool_count=self.state.tool_executions,
        )
        self._checkpoints[ckpt_id] = checkpoint
        self.status = SessionStatus.CHECKPOINTED

        log.info(
            "Checkpoint %s created: %s (%d files backed up, %d tool executions)",
            ckpt_id[:12], label or "(no label)",
            len(backup_paths), self.state.tool_executions,
        )
        return checkpoint

    def get_checkpoint(self, checkpoint_id: str) -> Optional[SandboxSessionCheckpoint]:
        """Get a checkpoint by ID."""
        return self._checkpoints.get(checkpoint_id)

    def list_checkpoints(self) -> List[SandboxSessionCheckpoint]:
        """List all checkpoints in this session."""
        return sorted(
            self._checkpoints.values(),
            key=lambda c: c.created_at,
            reverse=True,
        )

    def rollback_to_checkpoint(self, checkpoint_id: str) -> bool:
        """Roll back the session to a previous checkpoint.

        Restores all backed-up files to their state at checkpoint time.
        Clears any changes made after the checkpoint.

        Args:
            checkpoint_id: The checkpoint to roll back to.

        Returns:
            True if rollback succeeded, False otherwise.
        """
        checkpoint = self._checkpoints.get(checkpoint_id)
        if checkpoint is None:
            log.warning("Rollback failed: checkpoint %s not found", checkpoint_id[:12])
            return False

        log.info(
            "Rolling back to checkpoint %s (%s)...",
            checkpoint_id[:12], checkpoint.label or "(no label)",
        )

        # Restore backed-up files
        restored_count = 0
        for original_path, backup_path in checkpoint.backup_paths.items():
            try:
                if os.path.exists(original_path):
                    os.remove(original_path)
                shutil.copy2(backup_path, original_path)
                restored_count += 1
            except Exception as e:
                log.warning("Failed to restore '%s': %s", original_path, e)

        # Delete files created after checkpoint
        deleted_count = 0
        snapshot = checkpoint.state_snapshot
        checkpoint_created = set(snapshot.get("files_created", []))
        for path in self.state.files_created:
            if path not in checkpoint_created and os.path.exists(path):
                try:
                    os.remove(path)
                    deleted_count += 1
                except Exception as _ex:
                    log.warning("Silenced exception: %s", _ex)

        # Restore session state from checkpoint
        self.state.files_created = set(snapshot.get("files_created", []))
        self.state.files_modified = set(snapshot.get("files_modified", []))
        self.state.files_deleted = set(snapshot.get("files_deleted", []))
        self.state.tool_executions = checkpoint.tool_count
        self.status = SessionStatus.ROLLED_BACK

        log.info(
            "Rollback to %s complete: %d files restored, %d files deleted",
            checkpoint_id[:12], restored_count, deleted_count,
        )
        return True

    def clear(self) -> None:
        """Clear all session state without restoring files."""
        self.state = SandboxSessionState()
        self._checkpoints.clear()
        if self._workspace_root:
            self.state.cwd = str(self._workspace_root)
        self._change_index = 0
        log.info("Session %s state cleared", self._session_id[:12])

    def close(self) -> None:
        """Close the session. Clears state, checkpoints, and backup files."""
        self._cleanup_backups()
        self.clear()
        self.status = SessionStatus.CLOSED
        log.info("Session %s closed", self._session_id[:12])

    # ── File Change Reporting ───────────────────────────────────────

    def get_file_changes(
        self,
        change_type: Optional[SessionFileChange] = None,
        limit: int = 50,
    ) -> List[FileChangeRecord]:
        """Get file changes, optionally filtered by type."""
        changes = self.state.file_changes
        if change_type:
            changes = [c for c in changes if c.change_type == change_type]
        return changes[-limit:]

    def get_summary(self) -> Dict[str, Any]:
        """Get a human-readable summary of the session."""
        return {
            "session_id": self._session_id[:12],
            "status": self.status.value,
            "tool_executions": self.state.tool_executions,
            "errors": self.state.errors,
            "files_created": len(self.state.files_created),
            "files_modified": len(self.state.files_modified),
            "files_deleted": len(self.state.files_deleted),
            "total_changes": self.file_change_count,
            "checkpoints": len(self._checkpoints),
            "cwd": self.state.cwd,
        }

    # ── Internal Helpers ────────────────────────────────────────────

    def _read_file_content(self, path: str) -> Optional[Dict[str, Any]]:
        """Read file content from the filesystem."""
        result = self._fs.read_file(path)
        if isinstance(result, dict) and result.get("status") == "success":
            return result
        return None

    def _get_backup_path(self) -> Path:
        """Get or create the backup directory for this session."""
        if self._backup_dir is None:
            import tempfile
            self._backup_dir = Path(tempfile.gettempdir()) / ".aelvo_session_backups"
            self._backup_dir.mkdir(parents=True, exist_ok=True)
        return self._backup_dir / self._session_id

    def _cleanup_backups(self) -> None:
        """Remove the session's backup directory."""
        if self._backup_dir is not None:
            backup_path = self._backup_dir / self._session_id
            if backup_path.exists():
                try:
                    shutil.rmtree(backup_path)
                    log.debug("Cleaned up backup directory %s", backup_path)
                except Exception as e:
                    log.warning("Failed to clean up backup directory %s: %s", backup_path, e)

    # ── Snapshot ────────────────────────────────────────────────────

    def snapshot(self) -> Dict[str, Any]:
        """Get a snapshot of the session."""
        return {
            "session_id": self._session_id[:12],
            "status": self.status.value,
            "state": self.state.snapshot(),
            "checkpoints": [c.to_dict() for c in self.list_checkpoints()],
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display."""
        summary = self.get_summary()
        lines = [
            f"  ── SANDBOX SESSION [{summary['session_id']}] ──",
            f"  Status: {summary['status'].upper()}",
            f"  Path: {summary['cwd']}",
            f"  Tools: {summary['tool_executions']} executed, {summary['errors']} errors",
            f"  Files: {summary['files_created']} created, "
            f"{summary['files_modified']} modified, "
            f"{summary['files_deleted']} deleted",
            f"  Checkpoints: {summary['checkpoints']}",
        ]

        # Show recent file changes
        recent = self.state.file_changes[-5:]
        if recent:
            lines.append("")
            lines.append("  Recent Changes:")
            for c in recent:
                icon = {
                    SessionFileChange.CREATED: "+",
                    SessionFileChange.MODIFIED: "~",
                    SessionFileChange.DELETED: "-",
                }.get(c.change_type, "?")
                name = os.path.basename(c.path)
                lines.append(f"    {icon} {name} ({c.change_type.value})")

        lines.append("  ── ── ── ── ── ── ── ── ── ──")
        return "\n".join(lines)
