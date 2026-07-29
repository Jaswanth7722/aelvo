"""Layer 8 â€” Learned Recovery Memory.

Every recovery attempt becomes runtime learning.

Persisted data:
  - failure type
  - recovery used
  - success/failure
  - repo context
  - toolchain context
  - runtime state
  - graph conditions

Future recoveries should improve.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from pathlib import Path
from pydantic import BaseModel, Field

from .types import (
    FailureClassification,
    RecoveryAction,
    RecoveryStrategy,
    Confidence,
)

log = logging.getLogger("aelvo.runtime.verification.memory")


class RecoveryMemoryEntry(BaseModel):
    """A single learned recovery experience."""

    id: str = Field(..., description="Unique memory identifier")
    failure_type: FailureClassification = Field(
        ..., description="What type of failure"
    )
    recovery_strategy_id: str = Field(
        ..., description="Which strategy was used"
    )
    recovery_strategy_name: str = Field(
        ..., description="Human-readable strategy name"
    )
    success: bool = Field(..., description="Did recovery succeed")
    node_context: str = Field(
        default="", description="Brief description of the node context"
    )
    repo_context: str = Field(
        default="", description="Repository/project context"
    )
    toolchain_context: Dict[str, Any] = Field(
        default_factory=dict,
        description="Tool versions and environment",
    )
    runtime_state_hash: str = Field(
        default="", description="Hash of runtime state at recovery time"
    )
    graph_conditions: Dict[str, Any] = Field(
        default_factory=dict,
        description="Graph state at recovery time",
    )
    duration_ms: float = Field(
        default=0.0, description="How long recovery took"
    )
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class LearnedRecoveryMemory:
    """Persistent memory for recovery experiences.

    Stores, queries, and analyzes recovery history to improve
    future recovery decisions.
    """

    def __init__(self, storage_path: Optional[str] = None):
        self._entries: List[RecoveryMemoryEntry] = []
        self._storage_path = Path(storage_path) if storage_path else None
        self._load()

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    async def record(
        self,
        action: RecoveryAction,
        strategy: Optional[RecoveryStrategy] = None,
        success: Optional[bool] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> RecoveryMemoryEntry:
        """Record a recovery attempt into memory."""
        ctx = context or {}
        effective_success = action.success if success is None else success
        strategy_id = strategy.id if strategy else action.strategy_id
        strategy_name = strategy.name if strategy else action.strategy_id

        entry = RecoveryMemoryEntry(
            id=hashlib.sha256(
                f"recmem_{action.id}_{time.time()}".encode()
            ).hexdigest()[:16],
            failure_type=action.failure_classification,
            recovery_strategy_id=strategy_id,
            recovery_strategy_name=strategy_name,
            success=effective_success,
            node_context=ctx.get("node_description", ""),
            repo_context=ctx.get("project_name", ""),
            toolchain_context=ctx.get("toolchain", {}),
            runtime_state_hash=hashlib.sha256(
                json.dumps(
                    ctx.get("runtime_state", {}),
                    sort_keys=True,
                    default=str,
                ).encode()
            ).hexdigest(),
            graph_conditions=ctx.get("graph_conditions", {}),
            duration_ms=action.duration_ms,
            metadata={
                "failure_count": ctx.get("failure_count", 0),
                "retry_count": ctx.get("retry_count", 0),
            },
        )

        self._entries.append(entry)
        self._save()
        log.info(
            f"Recorded recovery memory: "
            f"{strategy.name} for {action.failure_classification.value} -> "
            f"{'SUCCESS' if success else 'FAILURE'}"
        )
        return entry

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    async def find_similar_failures(
        self,
        failure_type: FailureClassification,
        context: Optional[Dict[str, Any]] = None,
        limit: int = 5,
    ) -> List[Tuple[RecoveryMemoryEntry, float]]:
        """Find similar past failures and their recovery outcomes.

        Returns list of (entry, similarity_score) tuples.
        """
        candidates = [
            e
            for e in self._entries
            if e.failure_type == failure_type
        ]

        if not candidates:
            return []

        scored = []
        for entry in candidates:
            score = self._compute_similarity(entry, context)
            scored.append((entry, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:limit]

    async def best_recovery_for(
        self,
        failure_type: FailureClassification,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[RecoveryMemoryEntry, float]]:
        """Find the best past recovery for a failure type."""
        similar = await self.find_similar_failures(failure_type, context, limit=1)
        if similar:
            return similar[0]
        return None

    async def success_rate(
        self,
        failure_type: Optional[FailureClassification] = None,
        strategy_id: Optional[str] = None,
    ) -> float:
        """Compute success rate for recovery attempts."""
        entries = self._entries
        if failure_type:
            entries = [e for e in entries if e.failure_type == failure_type]
        if strategy_id:
            entries = [e for e in entries if e.recovery_strategy_id == strategy_id]

        if not entries:
            return 0.0

        successes = sum(1 for e in entries if e.success)
        return successes / len(entries)

    async def strategy_ranking(
        self,
        failure_type: FailureClassification,
    ) -> List[Tuple[str, float, int]]:
        """Rank strategies by success rate for a failure type.

        Returns list of (strategy_id, success_rate, attempt_count).
        """
        relevant = [
            e for e in self._entries if e.failure_type == failure_type
        ]

        if not relevant:
            return []

        # Group by strategy
        strategy_groups: Dict[str, List[RecoveryMemoryEntry]] = {}
        for entry in relevant:
            sid = entry.recovery_strategy_id
            if sid not in strategy_groups:
                strategy_groups[sid] = []
            strategy_groups[sid].append(entry)

        ranking = []
        for sid, group in strategy_groups.items():
            success_rate = sum(1 for e in group if e.success) / len(group)
            ranking.append(
                (sid, success_rate, len(group))
            )

        ranking.sort(key=lambda x: x[1], reverse=True)
        return ranking

    def _compute_similarity(
        self,
        entry: RecoveryMemoryEntry,
        context: Optional[Dict[str, Any]] = None,
    ) -> float:
        """Compute similarity between a memory entry and current context."""
        if not context:
            return 0.5  # Default similarity when no context

        score = 0.0
        weights = 0.0

        # Context match
        if context.get("project_name"):
            weights += 1.0
            if entry.repo_context == context.get("project_name", ""):
                score += 1.0

        # Node context similarity
        if context.get("node_description") and entry.node_context:
            weights += 1.0
            if (
                context["node_description"].lower()
                in entry.node_context.lower()
                or entry.node_context.lower()
                in context["node_description"].lower()
            ):
                score += 0.8

        # Recency boost (more recent = more relevant)
        weights += 0.5
        ts = entry.timestamp
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_hours = (
            datetime.now(timezone.utc) - ts
        ).total_seconds() / 3600
        recency_boost = max(0.0, 1.0 - age_hours / 168.0)  # Decay over 1 week
        score += recency_boost * 0.5

        if weights > 0:
            return score / weights
        return 0.5

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save(self):
        """Persist recovery memory to disk."""
        if not self._storage_path:
            return
        try:
            self._storage_path.parent.mkdir(parents=True, exist_ok=True)
            data = [
                entry.model_dump(mode="json")
                for entry in self._entries
            ]
            self._storage_path.write_text(
                json.dumps(data, indent=2), encoding="utf-8"
            )
        except Exception as e:
            log.error(f"Failed to save recovery memory: {e}")

    def _load(self):
        """Load recovery memory from disk."""
        if not self._storage_path or not self._storage_path.exists():
            return
        try:
            data = json.loads(self._storage_path.read_text(encoding="utf-8"))
            self._entries = [RecoveryMemoryEntry(**item) for item in data]
            log.info(
                f"Loaded {len(self._entries)} recovery memory entries "
                f"from {self._storage_path}"
            )
        except Exception as e:
            log.error(f"Failed to load recovery memory: {e}")

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------

    @property
    def entries(self) -> List[RecoveryMemoryEntry]:
        return list(self._entries)

    @property
    def total_entries(self) -> int:
        return len(self._entries)

    @property
    def overall_success_rate(self) -> float:
        if not self._entries:
            return 0.0
        successes = sum(1 for e in self._entries if e.success)
        return successes / len(self._entries)

    def get_entries_by_type(
        self, failure_type: FailureClassification
    ) -> List[RecoveryMemoryEntry]:
        return [
            e for e in self._entries if e.failure_type == failure_type
        ]

    def clear(self):
        self._entries.clear()
        self._save()
