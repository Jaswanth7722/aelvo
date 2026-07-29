"""
trust.py — Trust Metadata Layer for Collaboration Artifacts

Every collaboration artifact must answer: Why should this be trusted?

This module provides:
1. TrustMetadata — standardised trust metadata for any artifact
2. TrustReport — a compiled trust report for display
3. assign_trust_metadata() — populate trust from any evidence/entry

The trust metadata flows into:
- Blackboard evidence exports
- Consensus outcomes
- TUI displays
- HERALD reports
- Audit summaries
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from pydantic import BaseModel, Field


class TrustMetadata(BaseModel):
    """Standardised trust metadata for a collaboration artifact.

    Every artifact (finding, implementation, review, decision, etc.)
    carries these fields so judges can evaluate: **Why should this
    be trusted?**

    Fields:
        source: Where the artifact came from (repository_analysis,
            code_review, security_scan, web_search, etc.)
        confidence: Numeric confidence (0.0–1.0) from the producing agent.
        owner_agent: Which specialist produced it (ORACLE, FORGE, etc.).
        verification_status: Current verification state.
        timestamp: When the artifact was created.
        affected_files: Files impacted by the artifact.
        challenged: Whether the artifact has been challenged.
        challenge_count: Number of active challenges.
        consumed_by: Which specialists have consumed this artifact.
    """
    source: str = Field(default="", description="Provenance source")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0, description="Confidence score")
    owner_agent: str = Field(default="", description="Producing specialist")
    verification_status: str = Field(default="pending", description="Verification state")
    timestamp: str = Field(default="", description="ISO-8601 creation timestamp")
    affected_files: List[str] = Field(default_factory=list, description="Affected file paths")
    challenged: bool = Field(default=False, description="Whether this artifact is challenged")
    challenge_count: int = Field(default=0, description="Number of active challenges")
    consumed_by: List[str] = Field(default_factory=list, description="Specialists that consumed this")

    def to_summary_line(self) -> str:
        """One-line summary for TUI / log display."""
        parts = [
            f"[{self.owner_agent}]",
            f"src={self.source}" if self.source else "",
            f"conf={self.confidence:.2f}",
            f"ver={self.verification_status}",
        ]
        if self.challenged:
            parts.append(f"⚡{self.challenge_count}")
        if self.affected_files:
            parts.append(f"files={len(self.affected_files)}")
        return " ".join(p for p in parts if p)

    def to_rich_dict(self) -> Dict[str, Any]:
        """Full dict for HERALD reports."""
        return {
            "source": self.source,
            "confidence": self.confidence,
            "owner_agent": self.owner_agent,
            "verification_status": self.verification_status,
            "timestamp": self.timestamp,
            "affected_files": self.affected_files,
            "challenged": self.challenged,
            "challenge_count": self.challenge_count,
            "consumed_by": self.consumed_by,
        }


def assign_trust_metadata(
    owner_agent: str,
    source: str = "",
    confidence: float = 0.0,
    verification_status: str = "pending",
    affected_files: Optional[List[str]] = None,
    challenged: bool = False,
    challenge_count: int = 0,
    consumed_by: Optional[List[str]] = None,
) -> TrustMetadata:
    """Create a TrustMetadata object with the given values.

    This is a convenience factory that ensures every trust metadata
    object has a timestamp set correctly.
    """
    return TrustMetadata(
        source=source,
        confidence=confidence,
        owner_agent=owner_agent,
        verification_status=verification_status,
        timestamp=datetime.now(timezone.utc).isoformat(),
        affected_files=affected_files or [],
        challenged=challenged,
        challenge_count=challenge_count,
        consumed_by=consumed_by or [],
    )


def extract_trust_from_evidence(evidence: Any) -> TrustMetadata:
    """Extract trust metadata from a CollaborationEvidence or similar object.

    Works with any object that has the standard trust fields.
    """
    if hasattr(evidence, "owner_agent"):
        # CollaborationEvidence or similar typed object
        return TrustMetadata(
            source=getattr(evidence, "source", ""),
            confidence=getattr(evidence, "confidence", 0.0),
            owner_agent=evidence.owner_agent,
            verification_status=getattr(evidence, "verification_status", "pending"),
            timestamp=(
                evidence.timestamp.isoformat()
                if hasattr(evidence, "timestamp") and hasattr(evidence.timestamp, "isoformat")
                else str(getattr(evidence, "timestamp", ""))
            ),
            affected_files=getattr(evidence, "affected_files", []),
            challenged=(
                getattr(evidence.lifecycle_status, "value", "") == "challenged"
                if hasattr(evidence, "lifecycle_status") and evidence.lifecycle_status is not None
                else False
            ),
            challenge_count=(
                evidence.metadata.get("challenge_count", 0)
                if hasattr(evidence, "metadata") and isinstance(evidence.metadata, dict)
                else 0
            ),
            consumed_by=(
                evidence.metadata.get("consumed_by", [])
                if hasattr(evidence, "metadata") and isinstance(evidence.metadata, dict)
                else []
            ),
        )
    return TrustMetadata()


class TrustReport(BaseModel):
    """A compiled trust report for a set of artifacts.

    Provides aggregate trust information for HERALD reports and
    TUI display.
    """
    total_artifacts: int = 0
    by_agent: Dict[str, int] = Field(default_factory=dict)
    avg_confidence: float = 0.0
    challenged_count: int = 0
    verified_count: int = 0
    pending_count: int = 0
    artifacts: List[TrustMetadata] = Field(default_factory=list)

    @classmethod
    def from_evidence_list(cls, evidence_list: List[Any]) -> TrustReport:
        """Build a TrustReport from a list of CollaborationEvidence objects."""
        trust_list = [extract_trust_from_evidence(ev) for ev in evidence_list]

        by_agent: Dict[str, int] = {}
        total_conf = 0.0
        challenged = 0
        verified = 0
        pending = 0

        for t in trust_list:
            by_agent[t.owner_agent] = by_agent.get(t.owner_agent, 0) + 1
            total_conf += t.confidence
            if t.challenged:
                challenged += 1
            if t.verification_status == "verified":
                verified += 1
            elif t.verification_status == "pending":
                pending += 1

        return cls(
            total_artifacts=len(trust_list),
            by_agent=by_agent,
            avg_confidence=round(total_conf / len(trust_list), 4) if trust_list else 0.0,
            challenged_count=challenged,
            verified_count=verified,
            pending_count=pending,
            artifacts=trust_list,
        )

    def to_summary(self) -> str:
        """Human-readable summary for terminal display."""
        lines = [
            "Trust Report",
            f"  Total artifacts: {self.total_artifacts}",
            f"  Average confidence: {self.avg_confidence:.2f}",
            f"  Verified: {self.verified_count} | "
            f"Pending: {self.pending_count} | "
            f"Challenged: {self.challenged_count}",
        ]
        if self.by_agent:
            lines.append("  By agent:")
            for agent, count in sorted(self.by_agent.items()):
                lines.append(f"    {agent}: {count}")
        return "\n".join(lines)
