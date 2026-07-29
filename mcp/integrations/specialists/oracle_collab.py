"""oracle_collab.py — ORACLE Collaborative Behaviors

ORACLE gains the ability to:
1. Publish research findings to the blackboard proactively
2. Respond to QUESTION messages from other specialists
3. CHALLENGE assumptions when research contradicts them
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from cognition.blackboard import CognitiveBlackboard
from cognition.types import EntryType, Provenance, ProvenanceType
from shared_task_board.models import SpecialistName

log = logging.getLogger("aelvo.collab.oracle")


class OracleCollaborativeBehavior:
    """Augments ORACLE with collaborative capabilities."""

    def __init__(
        self,
        blackboard: CognitiveBlackboard,
    ):
        self.blackboard = blackboard

    # ------------------------------------------------------------------
    # Evidence Publishing
    # ------------------------------------------------------------------

    async def publish_finding(
        self,
        research_question: str,
        finding: str,
        confidence: float = 0.5,
        tags: Optional[List[str]] = None,
        sources: Optional[List[str]] = None,
    ) -> str:
        """Publish a research finding to the blackboard as a structured FindingSlot entry.

        All specialists can access this finding via the blackboard.
        """
        entry = self.blackboard.publish(
            slot_name="research_findings",
            content=finding,
            entry_type=EntryType.FINDING,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="ORACLE",
            ),
            confidence=confidence,
            tags=(["research", "oracle"] + (tags or [])),
        )

        log.info(
            "ORACLE published finding to blackboard (slot=research_findings, confidence=%.2f)",
            confidence,
        )
        return entry.id

    async def publish_multiple_findings(
        self,
        findings: List[Dict[str, Any]],
    ) -> List[str]:
        """Publish multiple research findings to the blackboard."""
        ids = []
        for finding in findings:
            entry_id = await self.publish_finding(
                research_question=finding.get("question", ""),
                finding=finding.get("content", ""),
                confidence=finding.get("confidence", 0.5),
                tags=finding.get("tags"),
                sources=finding.get("sources"),
            )
            ids.append(entry_id)
        return ids

    # ------------------------------------------------------------------
    # Question Response
    # ------------------------------------------------------------------

    async def handle_question(
        self,
        question: str,
        answer: str,
        specialist: str = "",
        evidence: Optional[List[Dict[str, Any]]] = None,
        confidence: float = 0.5,
        caveats: Optional[List[str]] = None,
        task_id: str = "",
    ) -> str:
        """Respond to a research question by publishing the answer to the blackboard.

        Publishes a structured answer to the ``research_answers`` slot.
        The requesting specialist monitors this slot for their answer.

        Args:
            question: The original research question.
            answer: The research answer.
            specialist: The specialist who asked the question.
            evidence: Supporting evidence.
            confidence: Confidence in the answer.
            caveats: Any caveats or limitations.
            task_id: Optional task ID this answer relates to.

        Returns:
            The blackboard entry ID for the published answer.
        """
        entry = self.blackboard.publish(
            slot_name="research_answers",
            content=f"[{specialist}] {answer}",
            entry_type=EntryType.FINDING,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="ORACLE",
            ),
            confidence=confidence,
            tags=["research-answer", "oracle", specialist.lower()] if specialist else ["research-answer", "oracle"],
        )
        log.info(
            "ORACLE answered research question (confidence=%.2f)",
            confidence,
        )
        return entry.id

    async def handle_research_request(
        self,
        question: str,
        research_results: str,
        sources: Optional[List[str]] = None,
        specialist: str = "",
        task_id: str = "",
    ) -> str:
        """Handle a research request, publishing results to the blackboard."""
        return await self.handle_question(
            question=question,
            answer=research_results,
            specialist=specialist,
            evidence=[{"source": s} for s in (sources or [])],
            confidence=0.7,
            caveats=["Based on available information — verify if critical"],
            task_id=task_id,
        )

    # ------------------------------------------------------------------
    # Challenge Assumptions
    # ------------------------------------------------------------------

    async def challenge_assumption(
        self,
        entry_id: str,
        challenger: str,
        challenged_claim: str,
        evidence: str,
        proposed_alternative: str,
    ) -> None:
        """Challenge an assumption on the blackboard.

        When ORACLE's research reveals an assumption in FORGE's implementation
        is incorrect, this triggers the challenge resolution process.

        No direct messaging. Challenges flow through the blackboard.
        """
        self.blackboard.challenge(
            slot_type="findings",
            entry_id=entry_id,
            challenger=challenger,
            challenged_claim=challenged_claim,
            evidence=evidence,
            proposed_alternative=proposed_alternative,
        )
        log.info(
            "ORACLE challenged assumption on entry %s: %s",
            entry_id[:8], challenged_claim[:60],
        )
