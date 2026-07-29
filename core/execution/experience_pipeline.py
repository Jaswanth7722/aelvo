"""core/execution/experience_pipeline.py — Experience Learning Pipeline

Phase 15: Learns from tool execution history and sandbox session outcomes.
Identifies failure patterns, error clusters, and suggests optimal retry policies
based on accumulated evidence.

Key components:
  - ExperienceRecord: Normalized execution experience (tool result + session context)
  - FailurePattern: A detected cluster of similar failures
  - RetrySuggestion: Policy recommendation backed by evidence
  - ExperienceLearningPipeline: Ingests, analyzes, and surfaces learning
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from core.execution.tool_registry import (
    ToolExecutionRegistry,
    ToolResult,
    RetryPolicy,
)

log = logging.getLogger("aelvo.core.execution.experience_pipeline")


# ============================================================================
# Enums
# ============================================================================


class ErrorCategory(str, Enum):
    """Categorization of execution errors by type."""
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    RATE_LIMIT = "rate_limit"
    PERMISSION = "permission"
    NOT_FOUND = "not_found"
    LOGIC_ERROR = "logic_error"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    UNKNOWN = "unknown"


class PatternSeverity(str, Enum):
    """Severity of a detected failure pattern."""
    LOW = "low"            # Occasional failures, low impact
    MEDIUM = "medium"      # Regular failures worth monitoring
    HIGH = "high"          # Frequent failures affecting reliability
    CRITICAL = "critical"  # Systemic failures requiring immediate action


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class ExperienceRecord:
    """A single execution experience, normalized from ToolResult + session context.

    Attributes:
        tool_name: The tool that was executed.
        session_id: Session in which this execution occurred.
        status: 'success' or 'error'.
        error: Error message (empty on success).
        error_category: Classified error type.
        duration_ms: Execution duration.
        cached: Whether result was from cache.
        retry_attempt: Which retry attempt (0 = first try).
        rollback_triggered: Whether this execution led to a session rollback.
        timestamp: When the execution occurred.
    """

    tool_name: str
    session_id: str
    status: str
    error: str = ""
    error_category: ErrorCategory = ErrorCategory.UNKNOWN
    duration_ms: float = 0.0
    cached: bool = False
    retry_attempt: int = 0
    rollback_triggered: bool = False
    timestamp: float = field(default_factory=time.time)

    @property
    def is_success(self) -> bool:
        return self.status == "success"

    @property
    def is_error(self) -> bool:
        return self.status == "error"

    @property
    def error_signature(self) -> str:
        """A stable signature for clustering similar errors.

        Uses the error category and a normalized form of the error message
        so that similar errors (e.g., 'timeout after 30s', 'timeout after 60s')
        cluster together.
        """
        if not self.error:
            return ""
        # Extract the core error type from the message
        msg_lower = self.error.lower()
        for keyword in ["timed out", "timeout", "connection refused",
                        "connection reset", "rate limit", "permission denied",
                        "not found", "no such file", "out of memory",
                        "resource temporarily"]:
            if keyword in msg_lower:
                return f"{self.error_category.value}:{keyword}"
        # Fall back to first 60 chars of error
        return f"{self.error_category.value}:{self.error[:60]}"


@dataclass
class FailurePattern:
    """A detected cluster of similar failures across executions.

    Attributes:
        error_signature: The signature that defines this pattern.
        error_category: The category of errors in this pattern.
        tool_names: Tools affected by this pattern.
        occurrence_count: How many times this pattern was observed.
        success_rate: Success rate for executions matching this pattern.
        avg_duration_ms: Average duration of matching executions.
        first_seen: When the first occurrence was observed.
        last_seen: When the last occurrence was observed.
        distinct_sessions: How many sessions this pattern spans.
        severity: Computed severity of the pattern.
        suggested_retry_policy: Which retry policy best addresses this pattern.
    """

    error_signature: str
    error_category: ErrorCategory = ErrorCategory.UNKNOWN
    tool_names: Set[str] = field(default_factory=set)
    occurrence_count: int = 0
    success_rate: float = 0.0
    avg_duration_ms: float = 0.0
    first_seen: float = 0.0
    last_seen: float = 0.0
    distinct_sessions: int = 0
    severity: PatternSeverity = PatternSeverity.LOW
    suggested_retry_policy: Optional[RetryPolicy] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_signature": self.error_signature,
            "error_category": self.error_category.value,
            "tool_count": len(self.tool_names),
            "tools": sorted(self.tool_names),
            "occurrence_count": self.occurrence_count,
            "success_rate": round(self.success_rate, 4),
            "avg_duration_ms": round(self.avg_duration_ms, 2),
            "distinct_sessions": self.distinct_sessions,
            "severity": self.severity.value,
            "suggested_retry_policy": self.suggested_retry_policy.value
                if self.suggested_retry_policy else None,
        }


@dataclass
class RetrySuggestion:
    """A recommendation to change a tool's retry policy based on learned patterns.

    Attributes:
        tool_name: The tool being recommended for change.
        current_policy: The tool's current retry policy.
        suggested_policy: The recommended retry policy.
        confidence: Confidence in this suggestion (0.0-1.0).
        evidence: Evidence supporting this suggestion.
        failure_count: Number of observed failures for this tool.
        success_rate: Observed success rate.
        reason: Human-readable explanation.
    """

    tool_name: str
    current_policy: RetryPolicy
    suggested_policy: RetryPolicy
    confidence: float = 0.0
    evidence: str = ""
    failure_count: int = 0
    success_rate: float = 0.0
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tool_name": self.tool_name,
            "current_policy": self.current_policy.value,
            "suggested_policy": self.suggested_policy.value,
            "confidence": round(self.confidence, 4),
            "evidence": self.evidence,
            "failure_count": self.failure_count,
            "success_rate": round(self.success_rate, 4),
        }


# ============================================================================
# Helpers
# ============================================================================


def _classify_error(error: str) -> ErrorCategory:
    """Classify an error message into an ErrorCategory."""
    msg = error.lower()

    # Timeout errors
    if any(t in msg for t in ["timed out", "timeout"]):
        return ErrorCategory.TIMEOUT

    # Connection errors
    if any(c in msg for c in ["connection refused", "connection reset",
                                "connection error", "connection failed",
                                "network error", "dns resolution"]):
        return ErrorCategory.CONNECTION

    # Rate limit errors
    if any(r in msg for r in ["rate limit", "too many requests",
                                "429", "rate exceeded"]):
        return ErrorCategory.RATE_LIMIT

    # Permission errors
    if any(p in msg for p in ["permission denied", "access denied",
                                "not authorized", "forbidden"]):
        return ErrorCategory.PERMISSION

    # Not found errors
    if any(n in msg for n in ["not found", "no such file", "does not exist",
                                "cannot find", "file not found"]):
        return ErrorCategory.NOT_FOUND

    # Resource exhaustion
    if any(r in msg for r in ["out of memory", "resource temporarily",
                                "no space", "too many open"]):
        return ErrorCategory.RESOURCE_EXHAUSTED

    return ErrorCategory.LOGIC_ERROR


def _compute_severity(
    occurrence_count: int,
    success_rate: float,
    distinct_sessions: int,
) -> PatternSeverity:
    """Compute pattern severity based on frequency and impact."""
    if occurrence_count >= 20 or success_rate < 0.3:
        return PatternSeverity.CRITICAL
    if occurrence_count >= 10 or success_rate < 0.5:
        return PatternSeverity.HIGH
    if occurrence_count >= 5 or distinct_sessions >= 3:
        return PatternSeverity.MEDIUM
    return PatternSeverity.LOW


def _suggest_policy_for_pattern(
    error_category: ErrorCategory,
    success_rate: float = 0.0,
) -> Optional[RetryPolicy]:
    """Suggest the most appropriate retry policy for a failure pattern.

    Args:
        error_category: The category of errors in the pattern.
        success_rate: The success rate for executions matching this pattern.
            Used for LOGIC_ERROR category — if > 30%, retry may help;
            if <= 30%, failure is near-certain so retry is not suggested.

    Returns:
        The suggested RetryPolicy, or None if no clear suggestion exists.
    """
    if error_category == ErrorCategory.TIMEOUT:
        return RetryPolicy.RETRY_ON_TIMEOUT
    if error_category == ErrorCategory.CONNECTION:
        return RetryPolicy.RETRY_ON_CONDITION
    if error_category == ErrorCategory.RATE_LIMIT:
        return RetryPolicy.RETRY_ON_CONDITION
    if error_category == ErrorCategory.RESOURCE_EXHAUSTED:
        return RetryPolicy.RETRY_ON_CONDITION
    if error_category == ErrorCategory.PERMISSION:
        return RetryPolicy.NO_RETRY  # Retrying won't help
    if error_category == ErrorCategory.NOT_FOUND:
        return RetryPolicy.NO_RETRY  # Retrying won't help
    if success_rate > 0.3:
        return RetryPolicy.RETRY_ON_FAILURE
    return None  # No clear suggestion


# ============================================================================
# ExperienceLearningPipeline
# ============================================================================


class ExperienceLearningPipeline:
    """Learns from tool execution history and session outcomes.

    Ingests ToolResult records and sandbox session rollback events,
    clusters failures into patterns, and produces actionable retry
    policy suggestions backed by evidence.

    Integrates with:
      - ToolExecutionRegistry: consumes execution history
      - AnalyticsEngine: feeds first-attempt and session metrics
      - PersistentSandboxSession: consumes rollback events
    """

    def __init__(
        self,
        registry: Optional[ToolExecutionRegistry] = None,
        analytics_engine: Any = None,
        max_history: int = 5000,
    ):
        self._registry = registry
        self._analytics = analytics_engine

        # Storage
        self._records: List[ExperienceRecord] = []
        self._rollbacks: List[Dict[str, Any]] = []
        self._max_history = max_history

        # Computed patterns (lazily recomputed on demand)
        self._cached_patterns: Dict[int, List[FailurePattern]] = {}  # keyed by min_occurrences
        self._cached_suggestions: Optional[List[RetrySuggestion]] = None
        self._dirty: bool = True

        # Per-tool tracking
        self._tool_records: Dict[str, List[ExperienceRecord]] = defaultdict(list)
        self._tool_success_counts: Dict[str, int] = defaultdict(int)
        self._tool_failure_counts: Dict[str, int] = defaultdict(int)

        log.info("ExperienceLearningPipeline initialized (max_history=%d)", max_history)

    # ── Ingestion ────────────────────────────────────────────────────────

    def record_execution(
        self,
        result: ToolResult,
        session_id: str = "",
        rollback_triggered: bool = False,
    ) -> ExperienceRecord:
        """Ingest a tool execution result into the learning pipeline.

        Args:
            result: The ToolResult from tool execution.
            session_id: Optional session identifier.
            rollback_triggered: Whether this execution caused a session rollback.

        Returns:
            The created ExperienceRecord.
        """
        error_category = ErrorCategory.UNKNOWN
        if result.is_error:
            error_category = _classify_error(result.error)

        record = ExperienceRecord(
            tool_name=result.tool_name,
            session_id=session_id,
            status=result.status,
            error=result.error,
            error_category=error_category,
            duration_ms=result.duration_ms,
            cached=result.cached,
            retry_attempt=result.retry_attempt,
            rollback_triggered=rollback_triggered,
            timestamp=result.timestamp,
        )

        self._records.append(record)
        if len(self._records) > self._max_history:
            self._records = self._records[-self._max_history:]

        self._tool_records[result.tool_name].append(record)
        if record.is_success:
            self._tool_success_counts[result.tool_name] += 1
        else:
            self._tool_failure_counts[result.tool_name] += 1

        self._dirty = True

        # Record first-attempt in analytics if available
        if (
            self._analytics
            and result.retry_attempt == 0
            and session_id
        ):
            try:
                self._analytics.record_first_attempt(
                    specialist=result.tool_name,
                    task_description=f"execute:{result.tool_name}",
                    succeeded=result.is_success,
                    confidence_at_time=self._compute_tool_confidence(result.tool_name),
                )
            except Exception:
                pass

        return record

    def record_rollback(
        self,
        session_id: str,
        checkpoint_id: str = "",
        reason: str = "",
        tool_name: str = "",
    ) -> None:
        """Record a session rollback event.

        Args:
            session_id: The session that was rolled back.
            checkpoint_id: The checkpoint rolled back to.
            reason: Why the rollback occurred.
            tool_name: Optional tool that triggered the rollback.
        """
        event = {
            "session_id": session_id,
            "checkpoint_id": checkpoint_id,
            "reason": reason,
            "tool_name": tool_name,
            "timestamp": time.time(),
        }
        self._rollbacks.append(event)

        # Mark recent executions of this tool as rollback-triggering
        if tool_name:
            tool_records = self._tool_records.get(tool_name, [])
            for r in reversed(tool_records):
                if r.session_id == session_id and not r.rollback_triggered:
                    r.rollback_triggered = True
                    break

        self._dirty = True
        log.info(
            "Rollback recorded: session=%s reason='%s' tool=%s",
            session_id[:12] if len(session_id) > 12 else session_id,
            reason[:60], tool_name,
        )

    # ── Bulk Ingestion from Registry ─────────────────────────────────────

    def ingest_registry_history(
        self,
        registry: Optional[ToolExecutionRegistry] = None,
        limit: int = 500,
    ) -> int:
        """Bulk-ingest history from a ToolExecutionRegistry.

        Args:
            registry: The registry to ingest from. Falls back to self._registry.
            limit: Max number of recent records to ingest.

        Returns:
            Number of records ingested.
        """
        target = registry or self._registry
        if target is None:
            return 0

        history = target.get_history(limit=limit)
        count = 0
        for result in history:
            self.record_execution(result, session_id="bulk_ingest")
            count += 1

        log.info("Bulk-ingested %d records from registry", count)
        return count

    def ingest_registry_statistics(self) -> Dict[str, Any]:
        """Ingest aggregate statistics from the registry for initial patterns.

        Returns:
            Dict with ingested tool stats.
        """
        if self._registry is None:
            return {}

        stats = {}
        for spec in self._registry.list_tools():
            tool_stats = self._registry.get_statistics(spec.name)
            if tool_stats["total"] > 0:
                stats[spec.name] = tool_stats
        return stats

    # ── Analysis ─────────────────────────────────────────────────────────

    def analyze_failure_patterns(
        self,
        min_occurrences: int = 2,
    ) -> List[FailurePattern]:
        """Analyze experience records and cluster into failure patterns.

        Patterns are grouped by error signature. Each pattern captures
        the frequency, affected tools, severity, and suggested retry policy.

        Results are cached by min_occurrences value and invalidated
        when new records are ingested.

        Args:
            min_occurrences: Minimum occurrences to form a pattern.

        Returns:
            List of FailurePattern objects, sorted by severity.
        """
        if not self._dirty and min_occurrences in self._cached_patterns:
            return self._cached_patterns[min_occurrences]

        # Single pass: cluster records by error signature, counting both successes and failures
        clusters: Dict[str, Dict[str, Any]] = {}
        for record in self._records:
            sig = record.error_signature
            if not sig and record.is_error:
                sig = f"unclassified:{record.error[:40]}"

            if not sig:
                continue  # Success with no error signature

            if sig not in clusters:
                clusters[sig] = {
                    "error_category": record.error_category,
                    "tool_names": set(),
                    "occurrences": 0,
                    "total_duration": 0.0,
                    "first_seen": record.timestamp,
                    "last_seen": record.timestamp,
                    "sessions": set(),
                    "success_count": 0,
                    "failure_count": 0,
                }

            c = clusters[sig]
            c["tool_names"].add(record.tool_name)
            c["occurrences"] += 1
            c["total_duration"] += record.duration_ms
            c["last_seen"] = max(c["last_seen"], record.timestamp)
            c["first_seen"] = min(c["first_seen"], record.timestamp)
            if record.session_id:
                c["sessions"].add(record.session_id)

            if record.is_success:
                c["success_count"] += 1
            else:
                c["failure_count"] += 1

        # Build patterns from clusters
        patterns: List[FailurePattern] = []
        for sig, data in clusters.items():
            if data["occurrences"] < min_occurrences:
                continue

            total = data["occurrences"]
            failures = data["failure_count"]
            success_rate = (total - failures) / total if total > 0 else 0.0
            avg_duration = data["total_duration"] / total if total > 0 else 0.0

            severity = _compute_severity(
                occurrence_count=total,
                success_rate=success_rate,
                distinct_sessions=len(data["sessions"]),
            )

            pattern = FailurePattern(
                error_signature=sig,
                error_category=data["error_category"],
                tool_names=data["tool_names"],
                occurrence_count=total,
                success_rate=success_rate,
                avg_duration_ms=avg_duration,
                first_seen=data["first_seen"],
                last_seen=data["last_seen"],
                distinct_sessions=len(data["sessions"]),
                severity=severity,
                suggested_retry_policy=_suggest_policy_for_pattern(
                    data["error_category"], success_rate
                ),
            )
            patterns.append(pattern)

        # Sort by severity (critical first), then by occurrence count
        severity_order = {
            PatternSeverity.CRITICAL: 0,
            PatternSeverity.HIGH: 1,
            PatternSeverity.MEDIUM: 2,
            PatternSeverity.LOW: 3,
        }
        patterns.sort(key=lambda p: (severity_order.get(p.severity, 99), -p.occurrence_count))

        self._cached_patterns[min_occurrences] = patterns
        self._dirty = False
        return patterns

    def suggest_retry_policies(
        self,
        min_confidence: float = 0.3,
    ) -> List[RetrySuggestion]:
        """Suggest optimal retry policies based on learned patterns.

        Analyzes each tool's execution history and detected failure patterns
        to recommend policy changes that would improve reliability.

        Args:
            min_confidence: Minimum confidence threshold for suggestions.

        Returns:
            List of RetrySuggestion objects, sorted by confidence.
        """
        if not self._dirty and self._cached_suggestions is not None:
            return self._cached_suggestions

        patterns = self.analyze_failure_patterns(min_occurrences=1)
        suggestions: List[RetrySuggestion] = []

        # Get all tools with execution history
        tools_with_history = set(self._tool_records.keys())
        if self._registry:
            tools_with_history.update(
                spec.name for spec in self._registry.list_tools()
            )

        for tool_name in sorted(tools_with_history):
            tool_records = self._tool_records.get(tool_name, [])
            if not tool_records:
                continue

            total = len(tool_records)
            successes = sum(1 for r in tool_records if r.is_success)
            failures = total - successes
            success_rate = successes / total if total > 0 else 0.0

            # Get current policy
            current_policy = RetryPolicy.NO_RETRY
            if self._registry:
                spec = self._registry.get_spec(tool_name)
                if spec:
                    current_policy = spec.retry_policy

            # Find matching failure patterns for this tool
            matching_patterns = [
                p for p in patterns
                if tool_name in p.tool_names and p.severity != PatternSeverity.LOW
            ]

            if not matching_patterns:
                # No significant failure patterns — no suggestion needed
                continue

            # Aggregate suggested policy from patterns
            policy_votes: Dict[RetryPolicy, int] = defaultdict(int)
            for p in matching_patterns:
                if p.suggested_retry_policy:
                    policy_votes[p.suggested_retry_policy] += p.occurrence_count

            if not policy_votes:
                continue

            # Pick the most-voted policy
            best_policy = max(policy_votes, key=policy_votes.get)

            if best_policy == current_policy:
                continue  # Already using the best policy

            # Compute confidence based on evidence strength
            total_pattern_occurrences = sum(p.occurrence_count for p in matching_patterns)
            evidence_weight = min(total_pattern_occurrences / 10.0, 1.0)
            failure_ratio = failures / total if total > 0 else 0.0
            confidence = evidence_weight * min(failure_ratio * 2, 1.0)

            if confidence < min_confidence:
                continue

            # Build evidence summary
            evidence_parts = []
            for p in matching_patterns[:3]:
                evidence_parts.append(
                    f"{p.error_category.value}({p.occurrence_count}x)"
                )
            evidence = ", ".join(evidence_parts)

            # Build human-readable reason
            top_pattern = matching_patterns[0]
            reason = (
                f"Tool '{tool_name}' has {failures} failures in {total} executions "
                f"({success_rate:.0%} success rate). "
                f"Primary issue: {top_pattern.error_category.value} errors "
                f"({top_pattern.occurrence_count} occurrences). "
                f"Switching from {current_policy.value} to {best_policy.value} "
                f"would improve reliability."
            )

            suggestion = RetrySuggestion(
                tool_name=tool_name,
                current_policy=current_policy,
                suggested_policy=best_policy,
                confidence=round(confidence, 4),
                evidence=evidence,
                failure_count=failures,
                success_rate=success_rate,
                reason=reason,
            )
            suggestions.append(suggestion)

        suggestions.sort(key=lambda s: s.confidence, reverse=True)
        self._cached_suggestions = suggestions
        return suggestions

    # ── Queries ──────────────────────────────────────────────────────────

    def get_tool_learning_curve(self, tool_name: str) -> Dict[str, Any]:
        """Get the learning curve data for a specific tool.

        Returns a time-series of success rates measured in batches
        so you can see if a tool is getting more reliable over time.

        Args:
            tool_name: The tool to query.

        Returns:
            Dict with tool_name, total, success_rate, batches, trend.
        """
        records = self._tool_records.get(tool_name, [])
        if not records:
            return {
                "tool_name": tool_name,
                "total": 0,
                "success_rate": 0.0,
                "batches": [],
                "trend": "insufficient_data",
            }

        # Sort by timestamp
        sorted_records = sorted(records, key=lambda r: r.timestamp)

        # Group into batches of ~20 records
        batch_size = max(20, len(sorted_records) // 10)
        batches = []
        for i in range(0, len(sorted_records), batch_size):
            batch = sorted_records[i:i + batch_size]
            batch_successes = sum(1 for r in batch if r.is_success)
            batches.append({
                "start_index": i,
                "count": len(batch),
                "success_rate": round(batch_successes / len(batch), 4),
                "avg_duration_ms": round(
                    sum(r.duration_ms for r in batch) / len(batch), 2
                ),
            })

        # Compute trend direction
        if len(batches) >= 2:
            first_half = batches[:len(batches)//2]
            second_half = batches[len(batches)//2:]
            first_rate = sum(b["success_rate"] for b in first_half) / len(first_half)
            second_rate = sum(b["success_rate"] for b in second_half) / len(second_half)
            if second_rate - first_rate > 0.05:
                trend = "improving"
            elif first_rate - second_rate > 0.05:
                trend = "degrading"
            else:
                trend = "stable"
        else:
            trend = "insufficient_data"

        total_successes = sum(1 for r in sorted_records if r.is_success)
        return {
            "tool_name": tool_name,
            "total": len(sorted_records),
            "success_rate": round(total_successes / len(sorted_records), 4),
            "failures": len(sorted_records) - total_successes,
            "batches": batches,
            "trend": trend,
        }

    def get_experience_summary(self) -> Dict[str, Any]:
        """Get aggregate summary of all experience data."""
        if not self._records:
            return {
                "total_executions": 0,
                "total_sessions": 0,
                "total_rollbacks": 0,
                "tools_tracked": 0,
                "success_rate": 0.0,
                "patterns_detected": 0,
                "suggestions_active": 0,
            }

        successes = sum(1 for r in self._records if r.is_success)
        sessions = set(r.session_id for r in self._records if r.session_id)
        patterns = self.analyze_failure_patterns(min_occurrences=2)
        suggestions = self.suggest_retry_policies(min_confidence=0.3)

        return {
            "total_executions": len(self._records),
            "total_sessions": len(sessions),
            "total_rollbacks": len(self._rollbacks),
            "tools_tracked": len(self._tool_records),
            "success_rate": round(successes / len(self._records), 4),
            "avg_duration_ms": round(
                sum(r.duration_ms for r in self._records) / len(self._records), 2
            ),
            "patterns_detected": len(patterns),
            "critical_patterns": sum(
                1 for p in patterns if p.severity == PatternSeverity.CRITICAL
            ),
            "high_patterns": sum(
                1 for p in patterns if p.severity == PatternSeverity.HIGH
            ),
            "suggestions_active": len(suggestions),
            "rollback_rate": round(
                len(self._rollbacks) / len(self._records), 4
            ) if self._records else 0.0,
        }

    def get_failure_patterns(
        self,
        min_severity: PatternSeverity = PatternSeverity.LOW,
    ) -> List[FailurePattern]:
        """Get detected failure patterns, optionally filtered by severity."""
        patterns = self.analyze_failure_patterns()
        severity_order = {
            PatternSeverity.CRITICAL: 0,
            PatternSeverity.HIGH: 1,
            PatternSeverity.MEDIUM: 2,
            PatternSeverity.LOW: 3,
        }
        min_order = severity_order.get(min_severity, 3)
        return [
            p for p in patterns
            if severity_order.get(p.severity, 3) <= min_order
        ]

    def get_retry_suggestions(
        self,
        min_confidence: float = 0.3,
    ) -> List[RetrySuggestion]:
        """Get retry policy suggestions, optionally filtered by confidence."""
        return [
            s for s in self.suggest_retry_policies(min_confidence)
            if s.confidence >= min_confidence
        ]

    # ── Snapshot & Display ──────────────────────────────────────────────

    def snapshot(self) -> Dict[str, Any]:
        """Get a snapshot of the experience pipeline state."""
        summary = self.get_experience_summary()
        patterns = self.analyze_failure_patterns(min_occurrences=2)
        suggestions = self.suggest_retry_policies(min_confidence=0.3)

        return {
            "summary": summary,
            "failure_patterns": [p.to_dict() for p in patterns[:20]],
            "retry_suggestions": [s.to_dict() for s in suggestions[:20]],
            "recent_rollbacks": self._rollbacks[-10:],
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display."""
        summary = self.get_experience_summary()
        patterns = self.get_failure_patterns(min_severity=PatternSeverity.LOW)
        suggestions = self.get_retry_suggestions(min_confidence=0.3)

        lines = [
            "  ── EXPERIENCE LEARNING PIPELINE ──",
            f"  Executions: {summary['total_executions']} | "
            f"Success rate: {summary['success_rate']:.1%}",
            f"  Sessions: {summary['total_sessions']} | "
            f"Rollbacks: {summary['total_rollbacks']}",
            f"  Tools tracked: {summary['tools_tracked']}",
        ]

        if summary["total_executions"] > 0:
            lines.append(
                f"  Avg duration: {summary['avg_duration_ms']:.0f}ms | "
                f"Rollback rate: {summary['rollback_rate']:.1%}"
            )

        # Failure patterns
        if patterns:
            lines.append("")
            lines.append("  Failure Patterns:")
            for p in patterns[:10]:
                severity_icon = {
                    PatternSeverity.CRITICAL: "!!",
                    PatternSeverity.HIGH: "! ",
                    PatternSeverity.MEDIUM: "~ ",
                    PatternSeverity.LOW: "  ",
                }.get(p.severity, "  ")
                tools_str = ", ".join(sorted(p.tool_names)[:3])
                if len(p.tool_names) > 3:
                    tools_str += "..."
                lines.append(
                    f"    {severity_icon} [{p.severity.value.upper()}] "
                    f"{p.error_category.value} ({p.occurrence_count}x) "
                    f"success={p.success_rate:.0%} "
                    f"tools=[{tools_str}]"
                )

        # Retry suggestions
        if suggestions:
            lines.append("")
            lines.append("  Retry Policy Suggestions:")
            for s in suggestions[:10]:
                lines.append(
                    f"    {s.tool_name}: "
                    f"{s.current_policy.value} → {s.suggested_policy.value} "
                    f"(confidence={s.confidence:.0%}, failures={s.failure_count})"
                )

        lines.append("  ── ── ── ── ── ── ── ── ── ── ── ──")
        return "\n".join(lines)

    # ── Internal Helpers ───────────────────────────────────────────────

    def _compute_tool_confidence(self, tool_name: str) -> float:
        """Compute a simple confidence score for a tool based on its history."""
        records = self._tool_records.get(tool_name, [])
        if not records:
            return 0.0

        total = len(records)
        successes = sum(1 for r in records if r.is_success)
        recent = records[-min(50, total):]
        recent_successes = sum(1 for r in recent if r.is_success)

        overall_rate = successes / total
        recent_rate = recent_successes / len(recent)

        # Weighted: 70% recent, 30% overall
        return round(0.7 * recent_rate + 0.3 * overall_rate, 4)

    def reset(self) -> None:
        """Reset all accumulated experience data."""
        self._records.clear()
        self._rollbacks.clear()
        self._tool_records.clear()
        self._tool_success_counts.clear()
        self._tool_failure_counts.clear()
        self._cached_patterns.clear()
        self._cached_suggestions = None
        self._dirty = True
        log.info("ExperienceLearningPipeline reset")
