# learning/specialist_adapter.py - KnowledgeAdapter
# Bridges the PatternExtractionEngine with all 7 AELVO specialists

from __future__ import annotations

import time
import logging
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING
from datetime import datetime, timezone

from learning.types import (
    EditCategory, EngineeringPattern, PatternQuery, PatternQueryResult,
    ValidationState, DeltaSource,
)

if TYPE_CHECKING:
    from learning.engine import PatternExtractionEngine

log = logging.getLogger("aelvo.learning.adapter")

# ── Specialist-to-Pattern Mappings ──────────────────────────────────────────

SPECIALIST_PATTERN_CATEGORIES: Dict[str, List[EditCategory]] = {
    "FORGE": [
        EditCategory.ADD_IMPORT_DEPENDENCY,
        EditCategory.REMOVE_IMPORT_DEPENDENCY,
        EditCategory.ADD_CALL_DEPENDENCY,
        EditCategory.ADD_INHERITANCE,
        EditCategory.ADD_IMPLEMENTS,
        EditCategory.REFACTOR_INTERNAL,
        EditCategory.ADD_FILE,
        EditCategory.MODIFY_SYMBOL_SIGNATURE,
        EditCategory.CHANGE_TYPE_ANNOTATION,
        EditCategory.CHANGE_EXPORT_STATUS,
        EditCategory.BREAK_CYCLE,
        EditCategory.CREATE_CYCLE,
    ],
    "ARCHITECT": [
        EditCategory.ADD_LAYER,
        EditCategory.ADD_FILE,
        EditCategory.ADD_IMPORT_DEPENDENCY,
        EditCategory.BREAK_CYCLE,
        EditCategory.MIXED,
    ],
    "TERMINUS": [
        EditCategory.ADD_FILE,
        EditCategory.DELETE_FILE,
        EditCategory.ADD_IMPORT_DEPENDENCY,
        EditCategory.REMOVE_IMPORT_DEPENDENCY,
    ],
    "SENTINEL": [
        EditCategory.CHANGE_EXPORT_STATUS,
        EditCategory.CHANGE_TYPE_ANNOTATION,
        EditCategory.MODIFY_SYMBOL_SIGNATURE,
        EditCategory.CREATE_CYCLE,
    ],
    "ORACLE": [],  # Research patterns are text-based, not graph-structural
    "HERMES": [],  # User preferences are not graph-structural
    "HERALD": [],  # Communication patterns are not graph-structural
}

SPECIALIST_DESCRIPTIONS: Dict[str, str] = {
    "FORGE": "code generation and refactoring",
    "ARCHITECT": "system architecture and design",
    "TERMINUS": "DevOps and shell operations",
    "SENTINEL": "security analysis",
    "ORACLE": "web research",
    "HERMES": "user communication calibration",
    "HERALD": "stakeholder communication",
}

# Specialists whose domain knowledge is structural graph patterns
GRAPH_STRUCTURAL_SPECIALISTS: Set[str] = {"FORGE", "ARCHITECT", "TERMINUS", "SENTINEL"}


class KnowledgeAdapter:
    """Bridges the PatternExtractionEngine with specialist context injection
    and experience capture.

    Two-directional integration:

    1. **Knowledge Injection** (PatternExtractionEngine → Specialists):
       Queries learned engineering patterns relevant to a specialist's domain
       and injects them as a structured knowledge packet into the specialist's
       context dict, which flows into get_system_prompt().

    2. **Experience Capture** (Specialists → PatternExtractionEngine):
       Receives snapshots of the dependency graph before and after a specialist's
       execution, along with outcome metadata, and feeds them into the
       PatternExtractionEngine for pattern learning.

    Usage in specialist workflow:
        adapter = KnowledgeAdapter(engine)
        context = specialist.build_memory_context(task, memory_engine)

        # Inject learned patterns into context
        adapter.enrich_context("FORGE", task, project, context)

        # Build system prompt with enriched context
        prompt = specialist.get_system_prompt(context)

        # ... specialist executes ...

        # Capture the experience
        adapter.capture_experience(
            specialist_name="FORGE",
            task=task,
            outcome="success",
            before_snapshot=graph_before,
            after_snapshot=graph_after,
        )
    """

    def __init__(self, engine: Optional["PatternExtractionEngine"] = None):
        self._engine = engine
        self._metrics: List[Dict] = []

    @property
    def engine(self) -> Optional["PatternExtractionEngine"]:
        return self._engine

    @engine.setter
    def engine(self, engine: "PatternExtractionEngine") -> None:
        self._engine = engine

    # ── Knowledge Injection ───────────────────────────────────────────────

    def build_knowledge_packet(
        self,
        specialist_name: str,
        task: str = "",
        project: Optional[str] = None,
        max_tokens: int = 1500,
        min_confidence: float = 0.4,
    ) -> str:
        """Build a formatted knowledge context packet for a specialist.

        Queries the PatternExtractionEngine for patterns relevant to the
        specialist's domain, formats them as a structured text block, and
        returns a string ready for injection into the specialist's system prompt.

        Args:
            specialist_name: One of FORGE, ARCHITECT, TERMINUS, SENTINEL, etc.
            task: Current task description for relevance scoring.
            project: Project scope to filter patterns by.
            max_tokens: Approximate token budget for the packet.
            min_confidence: Minimum pattern confidence to include.

        Returns:
            A formatted string to inject into the specialist's system prompt,
            or an empty string if the engine is not available or no patterns found.
        """
        if not self._engine:
            return ""

        start = time.time()
        upper_name = specialist_name.upper()
        categories = SPECIALIST_PATTERN_CATEGORIES.get(upper_name, [])

        # Query patterns for each relevant category
        all_patterns: List[EngineeringPattern] = []
        seen_ids: Set[str] = set()

        for category in categories:
            query = PatternQuery(
                category=category,
                min_confidence=min_confidence,
                min_freshness=0.3,
                validation_state=ValidationState.VALIDATED,
                project_scope=project,
                max_results=5,
            )
            result = self._engine.query_patterns(query)
            for p in result.patterns:
                if p.id not in seen_ids:
                    all_patterns.append(p)
                    seen_ids.add(p.id)

        # Also query OBSERVED patterns with higher min_confidence
        query_observed = PatternQuery(
            min_confidence=0.6,
            min_freshness=0.5,
            project_scope=project,
            max_results=10,
        )
        result_obs = self._engine.query_patterns(query_observed)
        for p in result_obs.patterns:
            if p.id not in seen_ids:
                all_patterns.append(p)
                seen_ids.add(p.id)

        # For non-structural specialists, always return learning stats even if no patterns
        if not all_patterns:
            elapsed = (time.time() - start) * 1000
            self._record_metric("build_knowledge_packet", elapsed, {
                "specialist": upper_name,
                "patterns_found": 0,
            })
            if upper_name not in GRAPH_STRUCTURAL_SPECIALISTS:
                lines: List[str] = []
                desc = SPECIALIST_DESCRIPTIONS.get(upper_name, upper_name.lower())
                lines.append(f"━━━ LEARNING SYSTEM STATUS — {desc} ━━━")
                lines.append(self._build_learning_stats_summary())
                lines.append("")
                return "\n".join(lines)
            return ""

        # Sort by confidence descending, then by freshness descending
        all_patterns.sort(key=lambda p: (p.confidence, p.freshness), reverse=True)

        # Token-budget-aware selection: each pattern ~50 tokens + overhead
        budget_per_pattern = max(50, max_tokens // max(len(all_patterns), 1))
        selected: List[EngineeringPattern] = []
        token_estimate = 0
        header_tokens = 80  # Rough estimate for header text

        for p in all_patterns:
            pattern_tokens = 30 + len(p.category_signature.signature_hash) + 5
            if p.provenance:
                pattern_tokens += min(20, sum(len(d) // 10 for d in p.provenance[-2:]))
            if token_estimate + pattern_tokens + header_tokens <= max_tokens:
                selected.append(p)
                token_estimate += pattern_tokens

        # Build the formatted packet
        lines: List[str] = []
        if upper_name in GRAPH_STRUCTURAL_SPECIALISTS:
            desc = SPECIALIST_DESCRIPTIONS.get(upper_name, upper_name.lower())
            lines.append(f"━━━ LEARNED PATTERNS — {desc} ━━━")
            lines.append(f"Patterns extracted from {len(all_patterns)} past {desc} operations.")
            lines.append("")

            for p in selected:
                cat_name = p.category.value.replace("_", " ").title()
                lines.append(f"  [{cat_name}] confidence={p.confidence:.2f} | "
                             f"observations={p.observation_count} | "
                             f"success_rate={p.success_rate:.0%}")
                lines.append(f"  Signature: {p.category_signature.signature_hash[:12]}")
                if p.provenance:
                    last_prov = p.provenance[-1]
                    if len(last_prov) > 100:
                        last_prov = last_prov[:100] + "..."
                    lines.append(f"  Context: {last_prov}")
                lines.append("")

            lines.append(f"  ({len(selected)} patterns shown of {len(all_patterns)} matched)")
        else:
            # Non-structural specialists get learning system statistics instead
            desc = SPECIALIST_DESCRIPTIONS.get(upper_name, upper_name.lower())
            lines.append(f"━━━ LEARNING SYSTEM STATUS — {desc} ━━━")
            lines.append(self._build_learning_stats_summary())
            lines.append("")

        result = "\n".join(lines)

        elapsed = (time.time() - start) * 1000
        self._record_metric("build_knowledge_packet", elapsed, {
            "specialist": upper_name,
            "patterns_found": len(all_patterns),
            "patterns_injected": len(selected),
            "token_estimate": token_estimate + header_tokens,
        })

        return result

    def enrich_context(
        self,
        specialist_name: str,
        task: str,
        project: Optional[str],
        context: Dict[str, Any],
        max_tokens: int = 1500,
    ) -> Dict[str, Any]:
        """Inject learned patterns directly into a specialist's context dict.

        This is the primary integration method. Call this after
        `build_memory_context()` and before `get_system_prompt()`.

        The patterns are injected under the key `learned_patterns` in the
        context dict. Specialists that already call
        `context.get("learned_patterns")` in their `get_system_prompt()`
        will automatically receive the injection.

        Args:
            specialist_name: One of FORGE, ARCHITECT, TERMINUS, SENTINEL, etc.
            task: Current task for relevance.
            project: Project scope filter.
            context: The specialist's context dict (mutated in place).
            max_tokens: Token budget for the knowledge packet.

        Returns:
            The enriched context dict (same object, mutated in place).
        """
        packet = self.build_knowledge_packet(
            specialist_name=specialist_name,
            task=task,
            project=project,
            max_tokens=max_tokens,
        )
        context["learned_patterns"] = packet

        # Also inject structured data for specialists that can use it programmatically
        if self._engine and specialist_name.upper() in GRAPH_STRUCTURAL_SPECIALISTS:
            categories = SPECIALIST_PATTERN_CATEGORIES.get(specialist_name.upper(), [])
            structured_patterns: List[Dict[str, Any]] = []
            for category in categories[:3]:  # Top 3 categories only
                query = PatternQuery(
                    category=category,
                    min_confidence=0.5,
                    project_scope=project,
                    max_results=3,
                )
                result = self._engine.query_patterns(query)
                for p in result.patterns:
                    structured_patterns.append({
                        "id": p.id,
                        "category": p.category.value,
                        "confidence": p.confidence,
                        "observation_count": p.observation_count,
                        "success_rate": p.success_rate,
                        "signature_hash": p.category_signature.signature_hash,
                    })
            context["structured_patterns"] = structured_patterns

        # Inject learning statistics
        if self._engine:
            context["learning_stats"] = self._engine.get_learning_statistics()

        return context

    # ── Experience Capture ────────────────────────────────────────────────

    def capture_experience(
        self,
        specialist_name: str,
        task: str,
        outcome: str,
        before_snapshot: Any = None,
        after_snapshot: Any = None,
        project: str = "",
        duration_ms: float = 0.0,
    ) -> Optional[EngineeringPattern]:
        """Capture a specialist's execution outcome as a learning experience.

        Feeds the before/after dependency graph snapshots into the
        PatternExtractionEngine's pipeline: delta compute → classify →
        extract → accumulate → persist.

        Args:
            specialist_name: Which specialist executed.
            task: Description of what was done.
            outcome: "success" or "failure".
            before_snapshot: GraphSnapshot before execution.
            after_snapshot: GraphSnapshot after execution.
            project: Project scope.
            duration_ms: Execution duration.

        Returns:
            The EngineeringPattern created/updated, or None if below threshold.
        """
        if not self._engine:
            return None

        if before_snapshot is None or after_snapshot is None:
            log.debug(
                f"No graph snapshots for {specialist_name} — "
                f"skipping experience capture"
            )
            return None

        start = time.time()

        # Use the engine's convenience method
        pattern = self._engine.process_execution_event(
            task_id=f"{specialist_name}_{int(time.time())}",
            before_snapshot=before_snapshot,
            after_snapshot=after_snapshot,
            specialist=specialist_name,
            project=project,
            outcome=outcome,
            execution_duration_ms=duration_ms,
            task_description=task,
        )

        elapsed = (time.time() - start) * 1000
        self._record_metric("capture_experience", elapsed, {
            "specialist": specialist_name,
            "outcome": outcome,
            "pattern_created": pattern is not None,
        })

        return pattern

    # ── Knowledge Injection Builder ───────────────────────────────────────

    def build_injection_for_specialist(
        self,
        specialist_name: str,
        task: str,
        project: Optional[str] = None,
        max_tokens: int = 2000,
    ) -> str:
        """Build a complete knowledge injection text for a specialist.

        This is the highest-level integration method. It produces the
        full formatted text that should be injected into the specialist's
        system prompt.

        Returns a string that includes:
        - Learned engineering patterns (for graph-structural specialists)
        - Learning system statistics (for all specialists)
        - Freshness and confidence indicators

        The output is token-budget-aware and formatted for direct
        injection into get_system_prompt().
        """
        if not self._engine:
            return ""

        parts: List[str] = []

        # 1. Knowledge packet
        packet = self.build_knowledge_packet(
            specialist_name=specialist_name,
            task=task,
            project=project,
            max_tokens=max_tokens,
        )
        if packet:
            parts.append(packet)

        # 2. Learning stats footer (for all specialists)
        stats = self._build_learning_stats_inline()
        if stats:
            parts.append(stats)

        return "\n\n".join(parts)

    # ── Specialized Queries ───────────────────────────────────────────────

    def get_high_confidence_patterns(
        self,
        project: Optional[str] = None,
        min_confidence: float = 0.8,
        max_results: int = 10,
    ) -> List[EngineeringPattern]:
        """Get the highest-confidence patterns across all categories.

        These are the patterns most likely to be correct and useful.
        Used for cross-specialist knowledge sharing.
        """
        if not self._engine:
            return []

        query = PatternQuery(
            min_confidence=min_confidence,
            min_freshness=0.3,
            validation_state=ValidationState.VALIDATED,
            project_scope=project,
            max_results=max_results,
        )
        result = self._engine.query_patterns(query)
        return result.patterns

    def get_patterns_for_category(
        self,
        category: EditCategory,
        project: Optional[str] = None,
        min_confidence: float = 0.0,
        max_results: int = 20,
    ) -> List[EngineeringPattern]:
        """Get all patterns for a specific edit category."""
        if not self._engine:
            return []

        query = PatternQuery(
            category=category,
            min_confidence=min_confidence,
            project_scope=project,
            max_results=max_results,
        )
        result = self._engine.query_patterns(query)
        return result.patterns

    # ── Metrics & Statistics ──────────────────────────────────────────────

    def get_specialist_learning_summary(
        self, specialist_name: str, project: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get a summary of what has been learned for a specific specialist.

        Returns:
            Dict with keys: specialist, total_patterns, by_category,
            avg_confidence, high_confidence_count, stale_count,
            relevant_categories.
        """
        if not self._engine:
            return {"specialist": specialist_name, "total_patterns": 0}

        upper_name = specialist_name.upper()
        categories = SPECIALIST_PATTERN_CATEGORIES.get(upper_name, [])
        all_patterns: List[EngineeringPattern] = []
        by_category: Dict[str, int] = {}
        total_conf = 0.0
        validated_count = 0
        stale_count = 0

        for category in categories:
            patterns = self.get_patterns_for_category(
                category=category,
                project=project,
                min_confidence=0.0,
                max_results=50,
            )
            for p in patterns:
                all_patterns.append(p)
                cat = p.category.value
                by_category[cat] = by_category.get(cat, 0) + 1
                total_conf += p.confidence
                if p.validation_state == ValidationState.VALIDATED:
                    validated_count += 1
                if p.freshness < 0.3:
                    stale_count += 1

        total = len(all_patterns)
        return {
            "specialist": upper_name,
            "total_patterns": total,
            "by_category": dict(sorted(by_category.items())),
            "avg_confidence": round(total_conf / total, 4) if total > 0 else 0.0,
            "high_confidence_count": validated_count,
            "stale_count": stale_count,
            "relevant_categories": [c.value for c in categories],
        }

    def get_cross_project_insights(self) -> Dict[str, Any]:
        """Get insights that span across projects."""
        if not self._engine:
            return {}

        stats = self._engine.get_learning_statistics()
        return {
            "total_patterns_across_projects": stats.get("total_patterns", 0),
            "calibration_accuracy": stats.get("calibration_accuracy", 0.0),
            "contradictions_detected": stats.get("contradictions_detected", 0),
            "total_deltas_processed": stats.get("total_deltas_processed", 0),
        }

    # ── Internal Helpers ──────────────────────────────────────────────────

    def _build_learning_stats_summary(self) -> str:
        """Build a multi-line summary of learning system state."""
        if not self._engine:
            return "  Learning engine not available."

        stats = self._engine.get_learning_statistics()
        lines = [
            f"  Total patterns: {stats.get('total_patterns', 0)}",
            f"  Avg confidence: {stats.get('avg_confidence', 0.0):.3f}",
            f"  Calibration accuracy: {stats.get('calibration_accuracy', 0.0):.2%}",
            f"  Contradictions resolved: {stats.get('contradictions_detected', 0)}",
            f"  Sessions active: {stats.get('session_active', False)}",
        ]
        return "\n".join(lines)

    def _build_learning_stats_inline(self) -> str:
        """Build a one-line inline stats footer."""
        if not self._engine:
            return ""

        stats = self._engine.get_learning_statistics()
        total = stats.get("total_patterns", 0)
        avg_conf = stats.get("avg_confidence", 0.0)
        calibration = stats.get("calibration_accuracy", 0.0)
        return (
            f"┈ learning: {total} patterns · "
            f"avg confidence {avg_conf:.2f} · "
            f"calibration {calibration:.0%} ┈"
        )

    def _record_metric(self, operation: str, duration_ms: float, extra: Optional[Dict] = None) -> None:
        metric = {"operation": operation, "duration_ms": round(duration_ms, 2)}
        if extra:
            metric.update(extra)
        self._metrics.append(metric)

    def get_metrics(self) -> List[Dict]:
        return self._metrics.copy()
