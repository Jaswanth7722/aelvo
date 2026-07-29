# planning/debt_forecasting.py - Technical Debt Forecasting Engine for AELVO OMEGA
"""
The Technical Debt Forecasting Engine doesn't invent debt estimates.
It reads existing memory entries — error_recovery, security_rule, system_decision —
and identifies patterns that indicate structural fragility in a subsystem.

This is not a code analysis engine. It operates entirely in memory space.
Its input is the AELVO memory substrate; its output is a risk signal that
propagates into milestone planning so that debt remediation is scheduled
proactively rather than reactively.

Architecture:
1. Query existing memory by type (error_recovery, security_rule, system_decision)
2. Group results by subsystem (inferred from file paths in metadata)
3. Score each subsystem's debt across four dimensions
4. Write a DebtForecastEntry for high-scoring subsystems
5. Optionally create remediation Initiative nodes in the hierarchy
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from planning.memory_types import (
    DebtForecastEntry,
    RiskLevel,
    MEMORY_TYPE_DEBT_FORECAST,
    IMPORTANCE_DEBT_FORECAST,
)
from planning.goal_hierarchy import GoalHierarchyEngine

log = logging.getLogger("aelvo.planning.debt")


# Thresholds for debt scoring
_DEBT_HIGH_THRESHOLD = 0.60   # Overall debt score above this → HIGH risk
_DEBT_MEDIUM_THRESHOLD = 0.30  # Above this → MEDIUM risk


class TechnicalDebtForecaster:
    """Evidence-grounded technical debt forecaster.

    Reads existing AELVO memory to detect subsystems with growing fragility.
    Produces DebtForecastEntry records that inform milestone risk assessment.
    """

    # Memory types to scan for debt signals
    DEBT_SIGNAL_TYPES = [
        "error_recovery",
        "security_rule",
        "system_decision",
        "code_pattern",
    ]

    # Weights for each debt dimension in overall score
    _DIMENSION_WEIGHTS = {
        "implementation": 0.30,
        "security": 0.30,
        "design": 0.25,
        "quality": 0.15,
    }

    def __init__(
        self,
        memory_engine,
        hierarchy: GoalHierarchyEngine,
        project: str,
    ):
        self.memory_engine = memory_engine
        self.hierarchy = hierarchy
        self.project = project
        self.collection = memory_engine.memory_collection
        self.db = memory_engine.db
        self._forecasts: Dict[str, DebtForecastEntry] = {}  # subsystem → forecast

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_scan(self, target_subsystems: Optional[List[str]] = None) -> List[DebtForecastEntry]:
        """Scan memory for debt signals and produce forecasts.

        If target_subsystems is specified, only those paths are scanned.
        Otherwise all memory entries with file_path metadata are scanned.

        Returns list of DebtForecastEntry for subsystems with detected debt.
        """
        log.info("Technical debt scan started for project=%s", self.project)

        # Pull all memory entries in the debt-signal types
        signal_entries = self._fetch_debt_signal_entries()

        # Group by subsystem
        subsystem_groups = self._group_by_subsystem(signal_entries, target_subsystems)

        forecasts = []
        for subsystem, entries in subsystem_groups.items():
            forecast = self._score_subsystem(subsystem, entries)
            if forecast.overall_debt_score >= 0.10:  # Only persist non-trivial debt
                self._persist_forecast(forecast)
                self._forecasts[subsystem] = forecast
                forecasts.append(forecast)

        # Sort by overall debt score descending
        forecasts.sort(key=lambda f: f.overall_debt_score, reverse=True)

        log.info(
            "Debt scan complete: %d subsystems with detected debt (of %d scanned)",
            len(forecasts), len(subsystem_groups),
        )
        return forecasts

    def get_forecast_for_subsystem(self, subsystem: str) -> Optional[DebtForecastEntry]:
        """Return the most recent debt forecast for a subsystem."""
        return self._forecasts.get(subsystem)

    def get_high_risk_subsystems(self) -> List[DebtForecastEntry]:
        """Return all subsystems with HIGH or CRITICAL overall risk."""
        return [
            f for f in self._forecasts.values()
            if f.risk_at_milestone in (RiskLevel.HIGH, RiskLevel.CRITICAL)
        ]

    def propose_remediation_initiative(
        self,
        forecast: DebtForecastEntry,
        target_objective_id: str,
    ) -> Optional[str]:
        """Create a remediation Initiative node for a high-debt subsystem.

        Returns the node_id of the created initiative, or None if creation failed.
        This is the primary integration point between debt forecasting and the
        goal hierarchy — debt evidence directly generates planned work.
        """
        from planning.memory_types import HierarchyLevel, PlanNodeState

        title = f"Debt Remediation: {forecast.subsystem}"
        content = (
            f"Remediation initiative for {forecast.subsystem}. "
            f"Overall debt score: {forecast.overall_debt_score:.2f}. "
            f"Primary signals: {forecast.error_recovery_count} error recovery entries, "
            f"{forecast.security_violation_count} security violations, "
            f"{forecast.decision_reversal_count} decision reversals. "
            f"Risk at next milestone: {forecast.risk_at_milestone.value}."
        )
        success_criteria = [
            f"Error recovery rate for {forecast.subsystem} reduces by >50%",
            f"No new security violations in {forecast.subsystem} for 3 sessions",
            "Lint violation growth rate at or below zero",
        ]

        node = self.hierarchy.create_node(
            level=HierarchyLevel.INITIATIVE,
            title=title,
            content=content,
            parent_id=target_objective_id,
            success_criteria=success_criteria,
        )

        if node:
            forecast.remediation_initiative_id = node.node_id
            log.info(
                "Remediation initiative created for '%s': node_id=%s",
                forecast.subsystem, node.node_id,
            )
            return node.node_id

        return None

    # ------------------------------------------------------------------
    # Internal: Fetch and group
    # ------------------------------------------------------------------

    def _fetch_debt_signal_entries(self) -> List[Dict[str, Any]]:
        """Fetch all debt-signal memory entries for this project."""
        entries = []
        for mem_type in self.DEBT_SIGNAL_TYPES:
            try:
                results = self.collection.get(
                    where={
                        "type": mem_type,
                        "project": self.project,
                    },
                    include=["documents", "metadatas", "ids"],
                    limit=200,
                )
                ids = results.get("ids", []) or []
                docs = results.get("documents", []) or []
                metas = results.get("metadatas", []) or []
                for entry_id, doc, meta in zip(ids, docs, metas):
                    if isinstance(meta, dict):
                        entries.append({
                            "id": entry_id,
                            "type": mem_type,
                            "document": doc or "",
                            "meta": meta,
                        })
            except Exception as exc:
                log.debug("Failed to fetch %s entries: %s", mem_type, exc)

        log.debug("Fetched %d total debt-signal entries", len(entries))
        return entries

    def _group_by_subsystem(
        self,
        entries: List[Dict[str, Any]],
        target_subsystems: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Group memory entries by subsystem (inferred from file_path metadata)."""
        groups: Dict[str, List[Dict[str, Any]]] = {}

        for entry in entries:
            meta = entry["meta"]
            # Try to extract subsystem from file_path or similar fields
            subsystem = self._infer_subsystem(meta, entry["document"])

            if target_subsystems and not any(
                s in subsystem for s in target_subsystems
            ):
                continue

            groups.setdefault(subsystem, [])
            groups[subsystem].append(entry)

        return groups

    def _infer_subsystem(self, meta: Dict[str, Any], document: str) -> str:
        """Infer the subsystem from metadata fields."""
        # Try file_path first
        file_path = meta.get("file_path", "") or ""
        if file_path:
            # Take the directory part as the subsystem
            parts = file_path.replace("\\", "/").split("/")
            if len(parts) >= 2:
                return "/".join(parts[:2])
            return parts[0] or "unknown"

        # Try module_path or other location fields
        for field in ("module_path", "module", "component", "subsystem"):
            val = meta.get(field, "")
            if val:
                return str(val)[:50]

        # Infer from document content (first word if it looks like a path)
        if document:
            words = document.split()
            for word in words[:5]:
                if "/" in word or "\\" in word:
                    return word.replace("\\", "/").split("/")[0]

        return "unknown"

    # ------------------------------------------------------------------
    # Internal: Scoring
    # ------------------------------------------------------------------

    def _score_subsystem(
        self, subsystem: str, entries: List[Dict[str, Any]]
    ) -> DebtForecastEntry:
        """Compute debt scores for a subsystem from its evidence set."""
        error_recovery = [e for e in entries if e["type"] == "error_recovery"]
        security = [e for e in entries if e["type"] == "security_rule"]
        system_decisions = [e for e in entries if e["type"] == "system_decision"]
        code_patterns = [e for e in entries if e["type"] == "code_pattern"]

        # Count decision reversals (system_decision entries with "revised" in content)
        reversals = [
            e for e in system_decisions
            if "revis" in e["document"].lower() or "changed" in e["document"].lower()
        ]

        # Score dimensions (0.0–1.0)
        impl_score = min(1.0, len(error_recovery) * 0.10)          # 10 errors → 1.0
        sec_score = min(1.0, len(security) * 0.20)                   # 5 security issues → 1.0
        design_score = min(1.0, len(reversals) * 0.15)               # 7 reversals → 1.0
        quality_score = min(1.0, len(code_patterns) * 0.05)          # 20 patterns → 1.0

        overall = (
            impl_score * self._DIMENSION_WEIGHTS["implementation"]
            + sec_score * self._DIMENSION_WEIGHTS["security"]
            + design_score * self._DIMENSION_WEIGHTS["design"]
            + quality_score * self._DIMENSION_WEIGHTS["quality"]
        )

        risk = RiskLevel.LOW
        if overall >= _DEBT_HIGH_THRESHOLD:
            risk = RiskLevel.HIGH
        elif overall >= _DEBT_MEDIUM_THRESHOLD:
            risk = RiskLevel.MEDIUM
        if len(security) >= 3 and sec_score >= 0.60:
            risk = RiskLevel.CRITICAL

        entry = DebtForecastEntry(
            type=MEMORY_TYPE_DEBT_FORECAST,
            content=(
                f"Technical debt in {subsystem}: overall={overall:.2f}, "
                f"impl={impl_score:.2f}, sec={sec_score:.2f}, "
                f"design={design_score:.2f}, quality={quality_score:.2f}"
            ),
            importance=IMPORTANCE_DEBT_FORECAST,
            project=self.project,
            subsystem=subsystem,
            file_paths=list({e["meta"].get("file_path", "") for e in entries if e["meta"].get("file_path")}),
            error_recovery_count=len(error_recovery),
            error_recovery_ids=[e["id"] for e in error_recovery],
            security_violation_count=len(security),
            security_violation_ids=[e["id"] for e in security],
            decision_reversal_count=len(reversals),
            decision_reversal_ids=[e["id"] for e in reversals],
            implementation_debt_score=round(impl_score, 4),
            security_debt_score=round(sec_score, 4),
            design_debt_score=round(design_score, 4),
            quality_debt_score=round(quality_score, 4),
            overall_debt_score=round(overall, 4),
            risk_at_milestone=risk,
        )
        return entry

    def _persist_forecast(self, forecast: DebtForecastEntry) -> None:
        """Write a DebtForecastEntry to ChromaDB."""
        try:
            meta = {
                "type": MEMORY_TYPE_DEBT_FORECAST,
                "importance": float(forecast.importance),
                "timestamp_unix": float(forecast.timestamp_unix),
                "usage_count": int(forecast.usage_count),
                "project": self.project,
                "source_specialist": "planning",
                "subsystem": forecast.subsystem,
                "overall_debt_score": float(forecast.overall_debt_score),
                "risk_at_milestone": forecast.risk_at_milestone.value,
                "error_recovery_count": forecast.error_recovery_count,
                "security_violation_count": forecast.security_violation_count,
                "decision_reversal_count": forecast.decision_reversal_count,
            }
            self.collection.add(
                ids=[forecast.id],
                documents=[forecast.content],
                metadatas=[meta],
            )
        except Exception as exc:
            log.warning("Debt forecast persist failed for %s: %s", forecast.subsystem, exc)
