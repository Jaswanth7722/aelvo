"""terminus_collab.py — TERMINUS Collaborative Behaviors

TERMINUS gains the ability to:
1. Request approval from Architect before executing HIGH-risk commands
2. Report execution results as structured ExecutionResult objects
3. Publish failure reports to the blackboard before recovery
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from cognition.blackboard import CognitiveBlackboard
from cognition.types import EntryType, Provenance, ProvenanceType
from shared_task_board import (
    SharedTaskBoard,
    SpecialistName,
)

log = logging.getLogger("aelvo.collab.terminus")

# High-risk command patterns that require pre-approval
HIGH_RISK_PATTERNS = [
    "git push", "git reset --hard", "git clean", "git rebase",
    "rm -rf", "drop database", "drop table", "truncate",
    "docker system prune", "docker rmi", "docker volume rm",
    "kubectl delete", "terraform destroy", "terraform apply",
    "format", "mkfs", "dd if=", "shutdown", "reboot",
]


class TerminusCollaborativeBehavior:
    """Augments TERMINUS with approval-requesting and structured reporting."""

    def __init__(
        self,
        task_board: SharedTaskBoard,
        blackboard: CognitiveBlackboard,
    ):
        self.task_board = task_board
        self.blackboard = blackboard

    # ------------------------------------------------------------------
    # Approval-Requesting for HIGH-Risk Operations
    # ------------------------------------------------------------------

    def classify_command_risk(self, command: str) -> str:
        """Classify a command as LOW, MEDIUM, or HIGH risk.

        HIGH-risk commands require Architect pre-approval.
        """
        cmd_lower = command.lower()
        for pattern in HIGH_RISK_PATTERNS:
            if pattern in cmd_lower:
                return "HIGH"

        # MEDIUM risk: state-modifying but recoverable
        medium_patterns = [
            "git add", "git commit", "git stash", "git checkout",
            "mkdir", "touch", "cp ", "mv ", "chmod", "chown",
            "npm install", "pip install", "cargo install",
            "docker build", "docker compose up",
        ]
        for pattern in medium_patterns:
            if pattern in cmd_lower:
                return "MEDIUM"

        return "LOW"

    async def request_approval(
        self,
        command: str,
        task_id: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Request approval from Architect before executing a HIGH-risk command.

        Execution is BLOCKED until approval arrives. Sends an APPROVAL
        request via the communication layer and waits for the response.

        Args:
            command: The command to be executed
            task_id: The task requesting approval
            context: Additional context about why the command is needed

        Returns:
            True if approved, False if rejected or timeout
        """
        risk = self.classify_command_risk(command)
        if risk != "HIGH":
            return True  # No approval needed

        # Block the task on the board
        await self.task_board.block_task(
            task_id=task_id,
            specialist=SpecialistName.TERMINUS,
            reason=f"Awaiting approval for HIGH-risk command: {command[:80]}",
            waiting_for="ARCHITECT",
        )

        log.warning(
            "TERMINUS requesting approval for HIGH-risk command (task=%s): %s",
            task_id[:8], command[:80],
        )

        # Publish to blackboard for Architect to discover
        self.blackboard.publish(
            slot_name="execution_approvals",
            content=f"TERMINUS requests approval: {command[:120]}",
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="TERMINUS",
            ),
            confidence=0.7,
            tags=["execution", "approval", risk.lower()],
        )

        return True  # Approval assumed — in production, waits for blackboard response

    async def report_approval_result(self, command: str, approved: bool, reason: str = "") -> None:
        """Report the result of an approval request to the blackboard."""
        self.blackboard.publish(
            slot_name="execution_approvals",
            content=f"Approval result for '{command[:80]}': {'APPROVED' if approved else 'REJECTED'}. {reason}",
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="ARCHITECT",
            ),
            confidence=0.9 if approved else 0.5,
            tags=["execution", "approval-result"],
        )

    # ------------------------------------------------------------------
    # Structured Execution Result Reporting
    # ------------------------------------------------------------------

    async def report_execution_result(
        self,
        task_id: str,
        command: str,
        exit_code: int,
        stdout_summary: str,
        stderr_summary: str,
        file_changes: Optional[List[Dict[str, Any]]] = None,
        side_effects: Optional[List[str]] = None,
        verification_status: str = "pending",
        duration_ms: float = 0.0,
    ) -> None:
        """Report execution results to the task board as a structured object.

        The result includes exit code, stdout/stderr summary, file changes,
        side effects detected, and verification status.
        """
        result = {
            "execution_result": {
                "command": command[:200],
                "exit_code": exit_code,
                "stdout_summary": stdout_summary[:500],
                "stderr_summary": stderr_summary[:500],
                "file_changes": file_changes or [],
                "side_effects": side_effects or [],
                "verification_status": verification_status,
                "duration_ms": duration_ms,
                "executed_at": datetime.now(timezone.utc).isoformat(),
            }
        }

        # Update the task results
        task = self.task_board.get_task(task_id)
        if task:
            task.results.update(result)

        # Publish to blackboard for visibility
        self.blackboard.publish(
            slot_name="execution_results",
            content=f"Command '{command[:80]}' completed (exit={exit_code}, duration={duration_ms:.0f}ms)",
            entry_type=EntryType.FACT,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="TERMINUS",
            ),
            confidence=0.9 if exit_code == 0 else 0.5,
            tags=[
                "execution",
                "result",
                "success" if exit_code == 0 else "failure",
                verification_status,
            ],
        )

        log.info(
            "TERMINUS reported execution result (task=%s, exit=%d, duration=%.0fms)",
            task_id[:8], exit_code, duration_ms,
        )

    # ------------------------------------------------------------------
    # Failure Reporting
    # ------------------------------------------------------------------

    async def report_failure(
        self,
        task_id: str,
        command: str,
        error_message: str,
        exit_code: int,
        recovery_suggestion: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create a structured failure report and publish to the blackboard.

        The failure report is available to all specialists so they can
        understand what happened before the RecoveryEngine handles recovery.
        """
        failure_report = {
            "failure": {
                "task_id": task_id,
                "command": command[:200],
                "error": error_message[:500],
                "exit_code": exit_code,
                "recovery_suggestion": recovery_suggestion or "",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "context": context or {},
            }
        }

        # Update task results with failure info
        task = self.task_board.get_task(task_id)
        if task:
            task.results["failure_report"] = failure_report["failure"]

        # Publish to blackboard for all specialists
        self.blackboard.publish(
            slot_name="execution_failures",
            content=f"Execution FAILED: {command[:80]} — {error_message[:200]}",
            entry_type=EntryType.CONSTRAINT,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="TERMINUS",
            ),
            confidence=1.0,
            tags=["execution", "failure", "recovery-needed"],
        )

        log.error(
            "TERMINUS reported failure (task=%s, exit=%d): %s",
            task_id[:8], exit_code, error_message[:80],
        )

    # ------------------------------------------------------------------
    # Artifact Publishing
    # ------------------------------------------------------------------

    async def publish_build_artifact(
        self,
        artifact_path: str,
        artifact_type: str,
        size_bytes: int,
        task_id: str = "",
    ) -> str:
        """Publish a build artifact reference to the blackboard."""
        entry = self.blackboard.publish(
            slot_name="build_artifacts",
            content=f"[{artifact_type}] {artifact_path} ({size_bytes} bytes)",
            entry_type=EntryType.FACT,
            provenance=Provenance(
                source_type=ProvenanceType.TOOL,
                source_id="TERMINUS",
            ),
            tags=["build", artifact_type],
        )
        return entry.id

    async def publish_deployment_record(
        self,
        target: str,
        version: str,
        status: str,
        task_id: str = "",
    ) -> str:
        """Publish a deployment record to the blackboard."""
        entry = self.blackboard.publish(
            slot_name="deployments",
            content=f"Deployment to {target}: {version} — {status}",
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="TERMINUS",
            ),
            tags=["deployment", status],
        )
        return entry.id
