"""
result_schemas.py — Typed Result Schemas for Task Types

Each task type has a corresponding result schema that defines
the output data a specialist produces upon completion.
"""

from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class TaskResult(BaseModel):
    """Base result for all task types."""
    task_id: str = Field(default="", description="The task this result belongs to")
    success: bool = Field(default=True)
    summary: str = Field(default="", description="Human-readable summary of the result")
    duration_ms: float = Field(default=0.0)
    artifacts: Dict[str, Any] = Field(default_factory=dict)


class ResearchResult(TaskResult):
    """Result from RESEARCH tasks (ORACLE)."""
    findings: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Research findings with evidence",
    )
    sources: List[str] = Field(
        default_factory=list,
        description="Source citations",
    )
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    open_questions: List[str] = Field(
        default_factory=list,
        description="Questions that remain unanswered",
    )


class ImplementResult(TaskResult):
    """Result from IMPLEMENT tasks (FORGE)."""
    files_changed: List[str] = Field(
        default_factory=list,
        description="Files that were created or modified",
    )
    files_created: List[str] = Field(
        default_factory=list,
        description="New files created",
    )
    changes_summary: str = Field(
        default="",
        description="Summary of code changes made",
    )
    pattern_extractions: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Patterns extracted from the implementation",
    )


class SecurityReviewResult(TaskResult):
    """Result from SECURITY_REVIEW tasks (SENTINEL)."""
    cleared: bool = Field(default=True)
    findings: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Security findings with severity",
    )
    vulnerabilities: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Confirmed vulnerabilities",
    )
    remediations: List[str] = Field(
        default_factory=list,
        description="Suggested remediations",
    )
    risk_level: str = Field(default="low")


class ExecuteResult(TaskResult):
    """Result from EXECUTE tasks (TERMINUS)."""
    exit_code: int = Field(default=0)
    stdout: str = Field(default="")
    stderr: str = Field(default="")
    executed_commands: List[str] = Field(default_factory=list)


class ConsensusResult(TaskResult):
    """Result from CONSENSUS tasks (Consensus System)."""
    outcome: str = Field(
        default="agreed",
        description="agreed, disagreed, partial, not_attempted",
    )
    positions: Dict[str, str] = Field(
        default_factory=dict,
        description="specialist -> position mapping",
    )
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    recommendation: str = Field(
        default="",
        description="Advisory recommendation for Architect",
    )


class ReportResult(TaskResult):
    """Result from REPORT tasks (HERALD)."""
    report: str = Field(default="", description="Full report content")
    format: str = Field(default="terminal")
    next_steps: List[str] = Field(default_factory=list)
