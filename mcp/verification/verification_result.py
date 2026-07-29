"""VerificationResult — typed outcome for MCP output verification."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from ..events.event_schemas import VerificationAction


class VerificationResult(BaseModel):
    """Result of a single MCP verification check."""
    verifier_id: str
    passed: bool
    findings: List[Dict[str, Any]] = Field(default_factory=list)
    confidence: float = 1.0
    action_required: VerificationAction = VerificationAction.PASS
    details: Dict[str, Any] = Field(default_factory=dict)
    diagnostics: List[str] = Field(default_factory=list)
    duration_ms: float = 0.0
    timestamp: datetime = Field(default_factory=datetime.utcnow)
