"""
blackboard_schemas.py — Typed Blackboard Entry Content Schemas

Each schema represents the structured *payload* that a specialist
publishes to the blackboard as the ``content`` field of a
``BlackboardEntry``.  Schemas serialize to/from JSON strings so they
can round-trip through the existing ``BlackboardEntry.content``
field.

No agent-to-agent messaging.  All collaboration flows through the
blackboard.  These schemas define the structure of what goes *into*
the blackboard.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Re-export the existing challenge / vote models so consumers have a single
# import location for all blackboard schemas.
# ---------------------------------------------------------------------------


def _serialize(obj: BaseModel) -> str:
    """Serialize a schema instance to a JSON string for BlackboardEntry.content."""
    return obj.model_dump_json(exclude_none=True)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ===========================================================================
# Specialist Publication Schemas
#
# Each schema can be converted to/from a blackboard entry content string.
# ===========================================================================


class FindingEntry(BaseModel):
    """A research finding published by ORACLE."""
    finding_id: str = Field(default="", description="Unique finding identifier")
    specialist: str = Field(default="ORACLE")
    summary: str = Field(..., description="Short summary of the finding")
    detail: str = Field(default="", description="Full detail / evidence")
    sources: List[str] = Field(default_factory=list)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    tags: List[str] = Field(default_factory=list)
    created_at: str = Field(default_factory=_now)

    @classmethod
    def from_entry_content(cls, content: str) -> "FindingEntry":
        return cls(**json.loads(content))

    def to_entry_content(self) -> str:
        return _serialize(self)


class ImplementationEntry(BaseModel):
    """An implementation artifact published by FORGE."""
    impl_id: str = Field(default="", description="Unique implementation identifier")
    specialist: str = Field(default="FORGE")
    summary: str = Field(..., description="What was implemented")
    files_changed: List[str] = Field(default_factory=list)
    files_created: List[str] = Field(default_factory=list)
    changes_description: str = Field(default="")
    test_summary: str = Field(default="")
    security_review_requested: bool = Field(default=True)
    pattern_references: List[str] = Field(default_factory=list)
    created_at: str = Field(default_factory=_now)

    @classmethod
    def from_entry_content(cls, content: str) -> "ImplementationEntry":
        return cls(**json.loads(content))

    def to_entry_content(self) -> str:
        return _serialize(self)


class RejectionEntry(BaseModel):
    """A rejection reason published by SENTINEL or ARCHITECT."""
    rejection_id: str = Field(default="", description="Unique rejection identifier")
    rejected_by: str = Field(..., description="Specialist who rejected")
    entry_id: str = Field(default="", description="ID of the rejected entry")
    reason: str = Field(..., description="Why it was rejected")
    findings: List[str] = Field(default_factory=list, description="Specific issues found")
    remediations: List[str] = Field(default_factory=list, description="Suggested fixes")
    severity: str = Field(default="medium", description="low, medium, high, critical")
    created_at: str = Field(default_factory=_now)

    @classmethod
    def from_entry_content(cls, content: str) -> "RejectionEntry":
        return cls(**json.loads(content))

    def to_entry_content(self) -> str:
        return _serialize(self)


class ApprovalEntry(BaseModel):
    """An approval published by SENTINEL, ARCHITECT, or consensus."""
    approval_id: str = Field(default="", description="Unique approval identifier")
    approved_by: str = Field(..., description="Specialist who approved")
    entry_id: str = Field(default="", description="ID of the approved entry")
    reason: str = Field(default="", description="Why it was approved")
    conditions: List[str] = Field(default_factory=list, description="Conditions of approval")
    confidence: float = Field(default=0.8, ge=0.0, le=1.0)
    created_at: str = Field(default_factory=_now)

    @classmethod
    def from_entry_content(cls, content: str) -> "ApprovalEntry":
        return cls(**json.loads(content))

    def to_entry_content(self) -> str:
        return _serialize(self)


class EscalationEntry(BaseModel):
    """An escalation published when a specialist needs Architect intervention."""
    escalation_id: str = Field(default="", description="Unique escalation identifier")
    escalated_by: str = Field(..., description="Specialist escalating")
    reason: str = Field(..., description="Why escalation is needed")
    context: Dict[str, Any] = Field(default_factory=dict, description="Supporting context")
    suggested_action: str = Field(default="", description="What the escalator suggests")
    urgency: str = Field(default="medium", description="low, medium, high, critical")
    created_at: str = Field(default_factory=_now)

    @classmethod
    def from_entry_content(cls, content: str) -> "EscalationEntry":
        return cls(**json.loads(content))

    def to_entry_content(self) -> str:
        return _serialize(self)


class ConsensusEntry(BaseModel):
    """A consensus position or outcome published by the Consensus System.

    Per Amendment 3: All consensus outcomes are ADVISORY.  The outcome
    informs the Architect, but the Architect has final authority.
    """
    consensus_id: str = Field(default="", description="Unique consensus identifier")
    topic: str = Field(..., description="The topic consensus was reached on")
    outcome: str = Field(default="agreed", description="agreed, disagreed, partial")
    positions: Dict[str, str] = Field(default_factory=dict, description="specialist -> position")
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    recommendation: str = Field(default="", description="Advisory recommendation for Architect")
    participants: List[str] = Field(default_factory=list)
    advisory: bool = Field(default=True, description="Always True — consensus is advisory, Architect decides")
    created_at: str = Field(default_factory=_now)

    @classmethod
    def from_entry_content(cls, content: str) -> "ConsensusEntry":
        return cls(**json.loads(content))

    def to_entry_content(self) -> str:
        return _serialize(self)


class ExecutionResultEntry(BaseModel):
    """An execution result published by TERMINUS."""
    execution_id: str = Field(default="", description="Unique execution identifier")
    specialist: str = Field(default="TERMINUS")
    command: str = Field(default="", description="Command that was executed")
    exit_code: int = Field(default=0)
    stdout: str = Field(default="")
    stderr: str = Field(default="")
    success: bool = Field(default=True)
    duration_ms: float = Field(default=0.0)
    created_at: str = Field(default_factory=_now)

    @classmethod
    def from_entry_content(cls, content: str) -> "ExecutionResultEntry":
        return cls(**json.loads(content))

    def to_entry_content(self) -> str:
        return _serialize(self)


class QuestionEntry(BaseModel):
    """A question published by a specialist seeking information."""
    question_id: str = Field(default="", description="Unique question identifier")
    asked_by: str = Field(..., description="Specialist asking the question")
    question: str = Field(..., description="The question text")
    context: Dict[str, Any] = Field(default_factory=dict, description="Supporting context")
    directed_to: str = Field(default="", description="Target specialist (empty = anyone)")
    tags: List[str] = Field(default_factory=list)
    created_at: str = Field(default_factory=_now)

    @classmethod
    def from_entry_content(cls, content: str) -> "QuestionEntry":
        return cls(**json.loads(content))

    def to_entry_content(self) -> str:
        return _serialize(self)


class AnswerEntry(BaseModel):
    """An answer to a question, published by a responding specialist."""
    answer_id: str = Field(default="", description="Unique answer identifier")
    question_id: str = Field(..., description="ID of the question being answered")
    answered_by: str = Field(..., description="Specialist answering")
    answer: str = Field(..., description="The answer text")
    evidence: List[str] = Field(default_factory=list)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    created_at: str = Field(default_factory=_now)

    @classmethod
    def from_entry_content(cls, content: str) -> "AnswerEntry":
        return cls(**json.loads(content))

    def to_entry_content(self) -> str:
        return _serialize(self)


# ===========================================================================
# Schema Registry — maps entry types to their schema classes
# ===========================================================================

ENTRY_SCHEMA_REGISTRY: Dict[str, type[BaseModel]] = {
    "finding": FindingEntry,
    "implementation": ImplementationEntry,
    "rejection": RejectionEntry,
    "approval": ApprovalEntry,
    "escalation": EscalationEntry,
    "consensus": ConsensusEntry,
    "execution_result": ExecutionResultEntry,
    "question": QuestionEntry,
    "answer": AnswerEntry,
}


def deserialize_entry_content(content: str, schema_type: str) -> BaseModel:
    """Deserialize a blackboard entry content string into its typed schema.

    Args:
        content: The JSON string from ``BlackboardEntry.content``.
        schema_type: The schema type key from ``ENTRY_SCHEMA_REGISTRY``.

    Returns:
        An instance of the corresponding schema class.

    Raises:
        KeyError: If ``schema_type`` is not in the registry.
    """
    cls = ENTRY_SCHEMA_REGISTRY[schema_type]
    return cls(**json.loads(content))


def serialize_to_entry_content(schema: BaseModel) -> str:
    """Serialize a typed schema to a JSON string for ``BlackboardEntry.content``.

    Args:
        schema: Any schema instance from this module.

    Returns:
        JSON string suitable for use as ``BlackboardEntry.content``.
    """
    return schema.model_dump_json(exclude_none=True)
