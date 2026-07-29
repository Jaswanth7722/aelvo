"""
context_schemas.py — Typed Context Schemas for Task Types

Each task type has a corresponding context schema that defines
the input data a specialist needs to execute the task.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class TaskContext(BaseModel):
    """Base context for all task types.

    Every task receives HermesContext fields for global cognition
    access, along with type-specific context.
    """
    task_id: str = Field(default="", description="The task this context belongs to")
    hermes_context_ref: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Reference snapshot of HermesContext fields",
    )
    session_id: str = Field(default="")
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ResearchContext(TaskContext):
    """Context for RESEARCH tasks (ORACLE)."""
    query: str = Field(..., description="The research question or topic")
    scope: str = Field(
        default="codebase",
        description="Scope: 'codebase', 'web', 'docs', 'all'",
    )
    existing_findings: List[str] = Field(
        default_factory=list,
        description="Findings from previous research tasks to build upon",
    )
    max_sources: int = Field(default=5, ge=1, le=20)


class ImplementContext(TaskContext):
    """Context for IMPLEMENT tasks (FORGE)."""
    specification: str = Field(
        ..., description="What to implement — detailed spec",
    )
    affected_files: List[str] = Field(
        default_factory=list,
        description="Files that may be modified",
    )
    pattern_references: List[str] = Field(
        default_factory=list,
        description="Reference implementations to follow",
    )
    test_required: bool = Field(default=True)
    security_review_required: bool = Field(default=True)


class SecurityReviewContext(TaskContext):
    """Context for SECURITY_REVIEW tasks (SENTINEL)."""
    files_to_review: List[str] = Field(
        ..., description="Files to inspect for security issues",
    )
    changes_summary: str = Field(
        default="",
        description="Summary of what was changed",
    )
    risk_focus: str = Field(
        default="all",
        description="Focus area: 'secrets', 'injection', 'auth', 'all'",
    )


class ExecuteContext(TaskContext):
    """Context for EXECUTE tasks (TERMINUS)."""
    commands: List[str] = Field(
        ..., description="Commands to execute",
    )
    working_directory: str = Field(
        default="",
        description="Working directory for execution",
    )
    timeout_seconds: int = Field(default=30, ge=1, le=300)
    require_security_clearance: bool = Field(default=True)


class ConsensusContext(TaskContext):
    """Context for CONSENSUS tasks (Consensus System)."""
    topic: str = Field(..., description="The topic to reach consensus on")
    positions: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Collected specialist positions",
    )
    required_participants: List[str] = Field(
        default_factory=list,
        description="Specialists required to participate",
    )
    resolution_strategy: str = Field(
        default="majority",
        description="Strategy: majority, supermajority, unanimous, weighted",
    )


class ReportContext(TaskContext):
    """Context for REPORT tasks (HERALD)."""
    execution_summary: str = Field(
        default="",
        description="Summary of what was executed",
    )
    task_results: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Results from completed tasks to include",
    )
    include_details: bool = Field(default=True)
    format: str = Field(default="terminal", description="'terminal' or 'markdown'")
