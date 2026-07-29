"""forge_collab.py — FORGE Collaborative Behaviors

FORGE gains the ability to:
1. Submit implementation proposals as UNDER_REVIEW (not COMPLETED)
2. Handle REJECTION messages from SENTINEL with specific revisions
3. REQUEST research from ORACLE when encountering unfamiliar APIs
4. Transitions to BLOCKED while waiting for research responses
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from cognition.blackboard import CognitiveBlackboard
from cognition.types import EntryType, Provenance, ProvenanceType
from shared_task_board import (
    SharedTaskBoard,
    Task,
    SpecialistName,
)

log = logging.getLogger("aelvo.collab.forge")


class ForgeCollaborativeBehavior:
    """Augments FORGE with collaborative revision and research capabilities."""

    def __init__(
        self,
        task_board: SharedTaskBoard,
        blackboard: CognitiveBlackboard,
    ):
        self.task_board = task_board
        self.blackboard = blackboard

    # ------------------------------------------------------------------
    # UNDER_REVIEW Submission
    # ------------------------------------------------------------------

    async def submit_for_review(
        self,
        task_id: str,
        specialist: SpecialistName,
        results: Dict[str, Any],
    ) -> Task:
        """Submit an implementation proposal as UNDER_REVIEW.

        The implementation requests a SECURITY_REVIEW from SENTINEL
        before FORGE considers the task done.
        """
        task = await self.task_board.complete_task(task_id, specialist, results)
        log.info(
            "FORGE submitted task %s for review by SENTINEL",
            task_id[:8],
        )

        # Request security review from SENTINEL
        await self.task_board.request_review(
            task_id=task_id,
            from_specialist=specialist,
            to_specialist=SpecialistName.SENTINEL,
            question=f"Security review requested for implementation: {task.title}",
        )

        return task

    # ------------------------------------------------------------------
    # REJECTION Handling
    # ------------------------------------------------------------------

    async def handle_rejection(
        self,
        task_id: str,
        rejection_message: Message,
        revision_results: Dict[str, Any],
    ) -> Task:
        """Handle a REJECTION message from SENTINEL.

        Reads the typed REJECTION content (reasons, suggested_revisions),
        applies the revisions, and resubmits for review. The revision
        cycle is tracked on the task board via events.
        """
        reasons = rejection_message.content.get("reasons", [])
        suggested_revisions = rejection_message.content.get("suggested_revisions", [])

        log.info(
            "FORGE handling rejection for task %s: %d reasons, %d suggested revisions",
            task_id[:8], len(reasons), len(suggested_revisions),
        )

        # Update results with revision metadata
        revision_results["_revision_metadata"] = {
            "rejection_reasons": reasons,
            "suggested_revisions": suggested_revisions,
            "revision_attempt": 1,
        }

        # Resubmit for review
        task = await self.task_board.complete_task(task_id, SpecialistName.FORGE, revision_results)
        log.info("FORGE resubmitted task %s after revision", task_id[:8])
        return task

    # ------------------------------------------------------------------
    # Research Request (BLOCKED pattern)
    # ------------------------------------------------------------------

    async def request_research(
        self,
        question: str,
        task_id: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Request research from ORACLE when encountering an unfamiliar API or library.

        FORGE's task transitions to BLOCKED while waiting for ORACLE's response.
        Sends a QUESTION message to ORACLE via the communication layer.

        Returns the message_id of the sent message.
        """
        # Block the task on the board
        await self.task_board.block_task(
            task_id=task_id,
            specialist=SpecialistName.FORGE,
            reason=f"Waiting for research: {question[:80]}",
            waiting_for="ORACLE",
        )

        log.info("FORGE blocked task %s waiting for ORACLE research", task_id[:8])

        # Publish the question to the blackboard — ORACLE monitors this slot
        self.blackboard.publish(
            slot_name="research_requests",
            content=f"FORGE requests research: {question}",
            entry_type=EntryType.QUERY,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="FORGE",
            ),
            tags=["research-request", "blocked"],
        )

        return entry.id

    async def unblock_from_research(
        self,
        task_id: str,
        resolution: str,
    ) -> None:
        """Unblock a task that was waiting for research."""
        await self.task_board.unblock_task(task_id, resolution)
        log.info("FORGE task %s unblocked after receiving research", task_id[:8])

    # ------------------------------------------------------------------
    # Code Pattern Publishing
    # ------------------------------------------------------------------

    async def publish_code_pattern(
        self,
        pattern_description: str,
        file_path: str = "",
        language: str = "",
        confidence: float = 0.6,
    ) -> str:
        """Publish a code pattern to the blackboard for all specialists to reference."""
        entry = self.blackboard.publish(
            slot_name="code_patterns",
            content=f"[{language}] {file_path}: {pattern_description}",
            entry_type=EntryType.FACT,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="FORGE",
            ),
            confidence=confidence,
            tags=["code-pattern", "forge", language.lower()] if language else ["code-pattern", "forge"],
        )
        return entry.id

    async def publish_architecture_note(
        self,
        note: str,
        component: str = "",
        confidence: float = 0.5,
    ) -> str:
        """Publish an architecture observation to the blackboard."""
        entry = self.blackboard.publish(
            slot_name="architecture",
            content=f"[{component}] {note}" if component else note,
            entry_type=EntryType.OBSERVATION,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="FORGE",
            ),
            confidence=confidence,
            tags=["architecture", "forge"],
        )
        return entry.id
