"""
hermes_context.py — Immutable HermesContext Model

HermesContext is the primary output of the HERMES cognition layer.
It is:
  - Created ONCE at the start of every request by HERMES
  - IMMUTABLE — never modified by any component after creation
  - Consumed EVERYWHERE — every component receives it

Components that receive HermesContext:
  - Architect, Task Board, Blackboard, Consensus
  - Terminus, Herald, Recovery, Verification, Memory, Learning

Per Amendment 4: Hermes is NOT a preprocessing step. Hermes remains
active throughout every workflow. HermesContext is the immutable
snapshot produced at the start and consumed throughout.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class HermesContext(BaseModel):
    """Immutable global cognition context produced by HERMES.

    Created once at the start of every request. Never modified.
    Every component in the system reads from this context.
    """

    # ── Core Analysis ──────────────────────────────────────────────
    task: str = Field(
        description="Raw user input / task description",
    )
    intent: str = Field(
        description="HERMES's interpretation of the user's core intent",
    )
    goals: List[str] = Field(
        default_factory=list,
        description="Decomposed goals extracted from the task",
    )
    constraints: Dict[str, Any] = Field(
        default_factory=dict,
        description="Extracted constraints from the task and anchor file",
    )

    # ── Risk & Complexity ──────────────────────────────────────────
    risk_profile: str = Field(
        default="low",
        description="Risk level: low, medium, high, critical",
    )
    complexity: int = Field(
        default=1, ge=1, le=10,
        description="Task complexity on a 1-10 scale",
    )

    # ── Memory & User Model ────────────────────────────────────────
    memory_context: Dict[str, Any] = Field(
        default_factory=dict,
        description="Relevant memory context retrieved by HERMES",
    )
    user_model: Dict[str, Any] = Field(
        default_factory=dict,
        description="The user's communication model",
    )

    # ── Permissions & Identity ─────────────────────────────────────
    execution_permissions: List[str] = Field(
        default_factory=lambda: ["read", "write", "execute", "search"],
        description="Allowed execution operations for this turn",
    )
    session_id: str = Field(
        default="",
        description="Current session identifier",
    )

    # ── HERMES Enriched Analysis ───────────────────────────────────
    hermes_analysis: Dict[str, Any] = Field(
        default_factory=dict,
        description="HERMES's detailed analysis",
    )

    # ── Metadata ───────────────────────────────────────────────────
    created_at: float = Field(
        default_factory=time.time,
        description="Unix timestamp of context creation",
    )

    # ── Immutability ───────────────────────────────────────────────
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    # ── Public Accessors ──────────────────────────────────────────

    @property
    def age_seconds(self) -> float:
        """Seconds since this context was created."""
        return time.time() - self.created_at

    @property
    def formatted_age(self) -> str:
        """Human-readable age string."""
        age = self.age_seconds
        if age < 1:
            return "just now"
        if age < 60:
            return f"{age:.0f}s ago"
        if age < 3600:
            return f"{age / 60:.0f}m ago"
        return f"{age / 3600:.1f}h ago"

    # ── Summary ───────────────────────────────────────────────────

    def summary(self) -> Dict[str, Any]:
        """Compact summary for logging / UI display."""
        return {
            "intent": self.intent[:80],
            "goals": len(self.goals),
            "constraints": len(self.constraints),
            "risk_profile": self.risk_profile,
            "complexity": self.complexity,
            "execution_permissions": self.execution_permissions,
            "session_id": self.session_id,
            "created_at": datetime.fromtimestamp(
                self.created_at, tz=timezone.utc,
            ).isoformat(),
            "age": self.formatted_age,
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display."""
        lines = [
            "HERMES CONTEXT",
            f"  Intent: {self.intent[:80]}",
            f"  Goals ({len(self.goals)}):",
        ]
        for g in self.goals:
            lines.append(f"    + {g[:60]}")
        lines.append(
            f"  Risk: {self.risk_profile.upper()}  |  "
            f"Complexity: {self.complexity}/10"
        )
        lines.append(
            f"  Permissions: {', '.join(self.execution_permissions)}"
        )
        lines.append(
            f"  Session: {self.session_id[:16] if self.session_id else '(none)'}"
        )
        lines.append(f"  Age: {self.formatted_age}")
        lines.append("-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --")
        return "\n".join(lines)

    # ── Factory ────────────────────────────────────────────────────

    @classmethod
    def create(
        cls,
        task: str,
        intent: str = "",
        goals: Optional[List[str]] = None,
        constraints: Optional[Dict[str, Any]] = None,
        risk_profile: str = "",
        complexity: int = 0,
        memory_context: Optional[Dict[str, Any]] = None,
        user_model: Optional[Dict[str, Any]] = None,
        execution_permissions: Optional[List[str]] = None,
        session_id: str = "",
        hermes_analysis: Optional[Dict[str, Any]] = None,
    ) -> "HermesContext":
        """Create a new HermesContext with defaults applied.

        This is the canonical factory method. All components should
        use this instead of direct construction for consistency.

        When risk_profile or complexity are empty/zero, they are
        inferred from the task text. When explicitly provided,
        they are used as-is.

        When execution_permissions is not provided, they are inferred
        from the task text (read-only tasks don't get write perms).
        """
        return cls(
            task=task,
            intent=intent or cls._infer_intent(task),
            goals=goals or cls._decompose_goals(task),
            constraints=constraints or {},
            risk_profile=risk_profile or cls._infer_risk(task),
            complexity=complexity or cls._estimate_complexity(task),
            memory_context=memory_context or {},
            user_model=user_model or {},
            execution_permissions=execution_permissions
                or cls._infer_permissions(task, risk_profile or cls._infer_risk(task)),
            session_id=session_id,
            hermes_analysis=hermes_analysis or {},
        )

    @staticmethod
    def _infer_risk(task: str) -> str:
        """Infer risk profile from task content."""
        lower = task.lower()

        high_risk_keywords = [
            "delete", "drop", "truncate", "rm -rf", "format",
            "production", "prod", "deploy", "release", "publish",
            "database", "migration", "rollback", "credentials",
            "secret", "password", "api_key", "token",
            "chmod 777", "sudo", "root",
        ]
        medium_risk_keywords = [
            "security", "auth", "permission", "firewall",
            "network", "config", "configuration",
            "docker", "kubernetes", "k8s",
            "commit", "push", "merge",
            "refactor", "rewrite",
        ]

        for keyword in high_risk_keywords:
            if keyword in lower:
                return "high"

        for keyword in medium_risk_keywords:
            if keyword in lower:
                return "medium"

        return "low"

    @staticmethod
    def _estimate_complexity(task: str) -> int:
        """Estimate task complexity on a 1-10 scale."""
        lower = task.lower()
        score = 1

        # Length factor
        words = len(task.split())
        if words > 50:
            score += 1
        if words > 100:
            score += 1
        if words > 200:
            score += 1

        # Multiple requirements
        separators = sum(1 for sep in (" and ", ", ", ". ", "; ") if sep in lower)
        if separators > 3:
            score += 1
        if separators > 6:
            score += 1

        # Technical complexity
        tech_terms = (
            "database", "migration", "refactor", "architecture",
            "design pattern", "dependency", "integration",
            "multi-thread", "async", "concurrent", "distributed",
            "docker", "kubernetes", "ci/cd", "pipeline",
            "api", "graphql", "rest", "websocket", "grpc",
        )
        tech_count = sum(1 for t in tech_terms if t in lower)
        if tech_count >= 2:
            score += 1
        if tech_count >= 4:
            score += 1

        # Multi-file or system-wide scope
        scope_terms = ("all files", "everywhere", "entire", "global", "system", "full")
        if any(t in lower for t in scope_terms):
            score += 1

        return min(10, max(1, score))

    @staticmethod
    def _infer_permissions(task: str, risk_profile: str = "") -> List[str]:
        """Infer execution permissions from task content."""
        lower = task.lower()
        permissions = ["read", "search"]

        # Write permissions for code changes
        if any(w in lower for w in (
            "write", "create", "edit", "implement", "fix",
            "refactor", "update", "modify", "change", "add",
        )):
            permissions.append("write")

        # Execute permissions for operations
        if any(w in lower for w in (
            "run", "execute", "deploy", "test", "bash",
            "terminal", "command", "install", "build", "compile",
        )):
            permissions.append("execute")

        # Restrict permissions in high-risk scenarios
        # Infer risk from task if not explicitly provided
        inferred_risk = risk_profile or HermesContext._infer_risk(task)
        if inferred_risk == "high" and "execute" in permissions:
            permissions.remove("execute")
        if inferred_risk == "high":
            permissions.append("requires_security_review")

        return permissions

    @staticmethod
    def _infer_intent(task: str) -> str:
        """Infer the user's core intent from the task text."""
        lower = task.lower()

        # Debug/fix takes highest priority
        if any(w in lower for w in ("fix", "bug", "error", "broken", "crash", "fail")):
            return "debug_and_fix"
        # Refactor/rewrite checked before implement/create/write to avoid
        # "rewrite" matching "write" (implement), "restructure" matching "structure"
        if any(w in lower for w in ("refactor", "rewrite", "restructure")):
            return "refactor_code"
        if any(w in lower for w in ("implement", "create", "build", "add", "write", "new")):
            return "implement_feature"
        if any(w in lower for w in ("research", "search", "find", "investigate", "explain")):
            return "research_and_explain"
        if any(w in lower for w in ("deploy", "run", "execute", "test")):
            return "execute_operation"
        if any(w in lower for w in ("security", "audit", "vulnerability", "secret")):
            return "security_audit"

        return "general_assistance"

    @staticmethod
    def _decompose_goals(task: str) -> List[str]:
        """Extract initial goal hints from the task."""
        goals = []
        lower = task.lower()

        if any(w in lower for w in ("fix", "bug", "error")):
            goals.append("Diagnose the root cause")
            goals.append("Apply the fix")
            goals.append("Verify the fix resolves the issue")

        if any(w in lower for w in ("implement", "create", "build", "add")):
            goals.append("Understand requirements and existing patterns")
            goals.append("Implement the feature")
            goals.append("Verify the implementation")

        if any(w in lower for w in ("refactor", "rewrite", "restructure")):
            goals.append("Analyze current structure and identify improvement areas")
            goals.append("Apply refactoring while preserving behavior")
            goals.append("Verify no regressions")

        if any(w in lower for w in ("research", "search", "find", "explain")):
            goals.append("Gather relevant information")
            goals.append("Synthesize findings")
            goals.append("Present clear explanation")

        if not goals:
            goals.append("Understand the request")
            goals.append("Execute the task")
            goals.append("Report results")

        return goals
