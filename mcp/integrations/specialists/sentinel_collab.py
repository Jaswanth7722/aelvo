"""sentinel_collab.py — SENTINEL Collaborative Behaviors

SENTINEL gains the ability to:
1. Formal REVIEW authority with typed APPROVAL / APPROVAL_WITH_CONDITIONS / REJECTION
2. Binding REJECTION that blocks execution until SENTINEL approves
3. ESCALATE to Architect for architectural-level security issues
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from cognition.blackboard import CognitiveBlackboard
from cognition.types import EntryType, Provenance, ProvenanceType, ConflictSeverity, ConflictRecord
from shared_task_board import (
    SharedTaskBoard,
    Task,
    TaskStatus,
    SpecialistName,
)
from shared_task_board.models import ReviewRequest

log = logging.getLogger("aelvo.collab.sentinel")


class SentinelCollaborativeBehavior:
    """Augments SENTINEL with formal review authority and escalation."""

    def __init__(
        self,
        task_board: SharedTaskBoard,
        blackboard: CognitiveBlackboard,
    ):
        self.task_board = task_board
        self.blackboard = blackboard

    # ------------------------------------------------------------------
    # Formal REVIEW Authority
    # ------------------------------------------------------------------

    async def approve(
        self,
        task_id: str,
        artifact_description: str,
        conditions: Optional[List[str]] = None,
        confidence: float = 0.85,
    ) -> Task:
        """Approve an implementation with optional conditions.

        APPROVAL means the implementation passes security review.
        If conditions are specified, it's APPROVED_WITH_CONDITIONS
        and the conditions must be tracked.
        """
        conditions = conditions or []

        # Find the pending review request
        review = await self._find_review(task_id)
        if review:
            conditions_str = "; ".join(conditions) if conditions else "None"
            await self.task_board.respond_to_review(
                review_id=review.review_id,
                specialist=SpecialistName.SENTINEL,
                response=f"Approved. Conditions: {conditions_str}",
                approved=True,
            )

        log.info(
            "SENTINEL APPROVED task %s (%d conditions)",
            task_id[:8], len(conditions),
        )

        task = self.task_board.get_task(task_id)
        return task

    async def approve_with_conditions(
        self,
        task_id: str,
        conditions: List[str],
        confidence: float = 0.7,
    ) -> Task:
        """Approve with specific conditions that must be tracked."""
        return await self.approve(task_id, "Approved with conditions", conditions, confidence)

    async def reject(
        self,
        task_id: str,
        reasons: List[str],
        suggested_revisions: List[str],
        blocking: bool = True,
    ) -> Task:
        """Reject an implementation with specific reasons and required remediations.

        REJECTION from SENTINEL is a hard stop. The implementation cannot
        proceed to execution until SENTINEL approves.

        Args:
            task_id: The implementation task to reject
            reasons: Specific vulnerabilities found and their severity
            suggested_revisions: Required remediations
            blocking: Whether this blocks execution (always True for SENTINEL)
        """
        # Find the review and respond with rejection
        review = await self._find_review(task_id)
        if review:
            await self.task_board.respond_to_review(
                review_id=review.review_id,
                specialist=SpecialistName.SENTINEL,
                response=f"REJECTED. Reasons: {'; '.join(reasons)}. Required: {'; '.join(suggested_revisions)}",
                approved=False,
            )

        # Send rejection notification to Architect via blackboard
        self.blackboard.publish(
            slot_name="security_escalations",
            content=f"SENTINEL REJECTED task {task_id[:8]} (blocking): {'; '.join(reasons)}",
            entry_type=EntryType.CONSTRAINT,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="SENTINEL",
            ),
            confidence=0.9,
            tags=["security", "rejection", "blocking", "architect-awareness"],
        )

        # Record the rejection in the security_findings slot
        self.blackboard.publish(
            slot_name="security_findings",
            content=f"SENTINEL REJECTED task {task_id[:8]}: {'; '.join(reasons)}",
            entry_type=EntryType.CONSTRAINT,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="SENTINEL",
            ),
            confidence=0.9,
            tags=["security", "rejection", "blocking"],
        )

        log.info(
            "SENTINEL REJECTED task %s: %s",
            task_id[:8], "; ".join(reasons),
        )

        task = self.task_board.get_task(task_id)
        return task

    # ------------------------------------------------------------------
    # ESCALATION to Architect
    # ------------------------------------------------------------------

    async def escalate(
        self,
        issue: str,
        attempted_resolutions: List[str],
        recommended_action: str,
        urgency: str = "high",
        task_id: str = "",
    ) -> str:
        """Escalate a security issue to Architect via the blackboard.

        When SENTINEL identifies a security issue that requires architectural
        changes rather than implementation fixes, it escalates to the
        ``security_escalations`` blackboard slot. Architect monitors this slot
        and decides on action.

        No direct messaging. Escalations flow through the blackboard.

        Args:
            issue: Description of the escalated issue.
            attempted_resolutions: What was already tried.
            recommended_action: What Architect should do.
            urgency: Urgency level (low, medium, high, critical).
            task_id: Optional task ID this escalation relates to.

        Returns:
            The blackboard entry ID for the published escalation.
        """
        # Record escalation in blackboard for Architect to discover
        entry = self.blackboard.publish(
            slot_name="security_escalations",
            content=f"SENTINEL escalated to ARCHITECT: {issue[:120]}",
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="SENTINEL",
            ),
            confidence=0.85,
            tags=["security", "escalation", urgency],
        )

        log.warning(
            "SENTINEL ESCALATED to ARCHITECT via blackboard (urgency=%s): %s",
            urgency, issue[:80],
        )
        return entry.id

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _find_review(self, task_id: str) -> Optional[ReviewRequest]:
        """Find a pending review request for a task directed at SENTINEL."""
        task = self.task_board.get_task(task_id)
        if task:
            for review in task.review_requests:
                if review.response is None and review.reviewing_specialist == SpecialistName.SENTINEL:
                    return review
        return None
