"""
trust_indicator.py — Trust Metadata Display Model

Carries trust metadata through the UI event pipeline so every
finding display can show: source, confidence, verification
status, owner, timestamp, and challenge status.

Connects the cognition layer's TrustMetadata / CollaborationEvidence
models to the TUI display layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


# ── Verification status display helpers ─────────────────────────

VERIFICATION_STATUS_COLORS = {
    "verified": "#00e38c",
    "pending": "#52627f",
    "challenged": "#f7b731",
    "rejected": "#ff5c7a",
    "escalated": "#ff5c7a",
    "passed": "#00e38c",
    "failed": "#ff5c7a",
    "running": "#1f8fff",
    "created": "#52627f",
    "consumed": "#00d889",
    "approved": "#00e38c",
    "archived": "#52627f",
}

VERIFICATION_STATUS_LABELS = {
    "verified": "VERIFIED",
    "pending": "PENDING",
    "challenged": "CHALLENGED",
    "rejected": "REJECTED",
    "escalated": "ESCALATED",
    "passed": "PASSED",
    "failed": "FAILED",
    "running": "RUNNING",
    "created": "CREATED",
    "consumed": "CONSUMED",
    "approved": "APPROVED",
    "archived": "ARCHIVED",
}

LIFECYCLE_STATUS_LABELS = {
    "created": "🟢 created",
    "verified": "✅ verified",
    "consumed": "📖 consumed",
    "referenced": "🔗 referenced",
    "challenged": "⚡ challenged",
    "approved": "👍 approved",
    "rejected": "❌ rejected",
    "archived": "📦 archived",
}

TRUST_CONFIDENCE_COLORS = {
    (0.0, 0.4): "#ff5c7a",    # low confidence — red
    (0.4, 0.7): "#f7b731",    # medium — amber
    (0.7, 1.0): "#00e38c",    # high — green
}


def confidence_color(confidence: float) -> str:
    """Get a color for a confidence value."""
    if confidence >= 0.7:
        return "#00e38c"
    elif confidence >= 0.4:
        return "#f7b731"
    else:
        return "#ff5c7a"


def confidence_bar(confidence: float, width: int = 8) -> str:
    """Render a small confidence bar."""
    filled = max(0, min(width, int(confidence * width)))
    empty = width - filled
    color = confidence_color(confidence)
    return f"[{color}]{'█' * filled}[/][#52627f]{'░' * empty}[/]"


@dataclass
class TrustIndicator:
    """Compact trust metadata for any finding/collaboration event display.

    Attached to CollaborationEvent.trust to enrich the TUI display
    with source, confidence, verification state, owner, and lifecycle
    information.
    """

    source: str = ""
    """Provenance source (repository_analysis, code_review, security_scan, etc.)"""

    confidence: float = 0.0
    """Confidence score from the producing agent (0.0–1.0)"""

    verification_status: str = "pending"
    """Current verification state (verified, pending, challenged, rejected, etc.)"""

    owner: str = ""
    """Specialist that produced this artifact (ORACLE, FORGE, etc.)"""

    timestamp: float = 0.0
    """Unix timestamp when the artifact was created"""

    evidence_type: str = "finding"
    """Type of evidence (finding, implementation, review, challenge, decision, etc.)"""

    challenged: bool = False
    """Whether this artifact has been actively challenged"""

    challenge_count: int = 0
    """Number of active challenges"""

    lifecycle_status: str = "created"
    """Current lifecycle state (created, verified, consumed, challenged, etc.)"""

    affected_files: List[str] = field(default_factory=list)
    """Files impacted by this artifact"""

    @property
    def verification_color(self) -> str:
        return VERIFICATION_STATUS_COLORS.get(self.verification_status, "#52627f")

    @property
    def verification_label(self) -> str:
        return VERIFICATION_STATUS_LABELS.get(self.verification_status, self.verification_status.upper())

    @property
    def lifecycle_label(self) -> str:
        return LIFECYCLE_STATUS_LABELS.get(self.lifecycle_status, self.lifecycle_status)

    @property
    def confidence_pct(self) -> int:
        return int(self.confidence * 100)

    @property
    def is_high_confidence(self) -> bool:
        return self.confidence >= 0.7

    @property
    def is_low_confidence(self) -> bool:
        return self.confidence < 0.4

    @property
    def confidence_color(self) -> str:
        return confidence_color(self.confidence)

    def to_summary_line(self) -> str:
        """One-line trust summary for compact displays."""
        parts = []
        if self.source:
            parts.append(f"[#52627f]src:[/]{self.source[:20]}")
        color = self.verification_color
        parts.append(f"[{color}]{self.verification_label}[/]")
        if self.challenged:
            parts.append(f"[#f7b731]⚡{self.challenge_count}[/]")
        return " ".join(parts)

    def to_badge_line(self) -> str:
        """Short badge line for event trace entries."""
        color = self.confidence_color
        ver_color = self.verification_color
        parts = []
        parts.append(f"[{color}]✧ conf:{self.confidence:.2f}[/]")
        parts.append(f"[{ver_color}]ver:{self.verification_label}[/]")
        if self.challenged:
            parts.append(f"[#f7b731]⚡{self.challenge_count}[/]")
        if self.source:
            parts.append(f"[#52627f]{self.source[:15]}[/]")
        return " ".join(parts)
