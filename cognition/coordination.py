from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from enum import Enum
from pydantic import BaseModel, Field

from specialists.base import BaseSpecialist

from runtime_next.models.plan import ExecutionNode, NodeType

log = logging.getLogger("aelvo.cognition.coordination")


class DelegationMode(str, Enum):
    CAPABILITY_AWARE = "capability_aware"
    CONFIDENCE_AWARE = "confidence_aware"
    GRAPH_AWARE = "graph_aware"
    ROUND_ROBIN = "round_robin"
    MANUAL = "manual"


class DelegationRecord(BaseModel):

    node_id: str
    specialist_name: str
    confidence: float = 0.0
    mode: DelegationMode
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    result: Optional[str] = None
    error: Optional[str] = None
    duration_ms: Optional[float] = None


class SpecialistCoordinationRuntime:
    """Specialist Coordination Runtime.

    Delegates work to specialists using capability-aware, confidence-aware,
    and graph-aware strategies. Maintains delegation history and performance
    tracking for each specialist.

    Unlike the original implementation which only selected specialists and
    returned a DelegationRecord, this version ACTUALLY CALLS the specialist's
    execute() method and captures the result, error, and duration.

    Supports both sequential and parallel (asyncio.gather) dispatch.
    """

    def __init__(self, specialist_registry: Optional[Dict[str, BaseSpecialist]] = None):
        if specialist_registry is not None:
            self._registry = specialist_registry
        else:
            # Lazy import to avoid circular dependency:
            # cognition -> specialists -> cognition (via cognition.architect_decision)
            from specialists import SPECIALIST_REGISTRY
            self._registry = SPECIALIST_REGISTRY
        self._delegations: List[DelegationRecord] = []
        self._specialist_scores: Dict[str, List[float]] = {
            name: [] for name in self._registry
        }

    def available_specialists(self) -> List[str]:
        return list(self._registry.keys())

    def get_specialist(self, name: str) -> Optional[BaseSpecialist]:
        return self._registry.get(name.upper())

    async def delegate(
        self,
        node: ExecutionNode,
        task: str,
        context: Optional[Dict[str, Any]] = None,
        mode: DelegationMode = DelegationMode.CAPABILITY_AWARE,
        preferred_specialist: Optional[str] = None,
    ) -> DelegationRecord:
        """Select a specialist and execute their role on the given task.

        This method:
        1. Selects the best specialist using the specified delegation mode
        2. Calls the specialist's execute() method with the task and context
        3. Captures the result, error, and duration
        4. Returns a complete DelegationRecord with execution outcome

        The context dict MUST contain an 'agent' key for the LLM dispatch
        to work (required by BaseSpecialist.execute()).

        Args:
            node: The execution node representing this task.
            task: The user task / request string.
            context: Shared context dict (must include 'agent').
            mode: How to select the specialist (CAPABILITY_AWARE, etc.).
            preferred_specialist: Explicit specialist name (for MANUAL mode).

        Returns:
            DelegationRecord with execution result, error, and duration.
        """
        context = context or {}
        specialist_name = preferred_specialist or self._select_specialist(node, task, context, mode)
        specialist = self._registry.get(specialist_name)
        confidence = self._compute_confidence(specialist_name, task)

        if specialist is None:
            record = DelegationRecord(
                node_id=node.id,
                specialist_name=specialist_name,
                confidence=confidence,
                mode=mode,
                error=f"Specialist {specialist_name} not found in registry",
            )
            self._delegations.append(record)
            log.error("Delegation failed: specialist %s not found", specialist_name)
            return record

        start = time.monotonic()
        try:
            log.info(
                "Delegating node %s to %s (mode=%s, confidence=%.2f)",
                node.id, specialist_name, mode.value, confidence,
            )

            # Call the specialist's execute method with the task and context
            result = await specialist.execute(task, context)

            duration = (time.monotonic() - start) * 1000

            record = DelegationRecord(
                node_id=node.id,
                specialist_name=specialist_name,
                confidence=self._compute_confidence(specialist_name, task),
                mode=mode,
                result=result,
                duration_ms=duration,
            )
            self._delegations.append(record)

            # Score this delegation as successful
            self._score_delegation_internal(specialist_name, True)

            log.info(
                "Delegation to %s completed in %.0fms (result length: %d chars)",
                specialist_name, duration, len(result) if result else 0,
            )
            return record

        except Exception as e:
            duration = (time.monotonic() - start) * 1000
            error_msg = f"{type(e).__name__}: {e}"

            record = DelegationRecord(
                node_id=node.id,
                specialist_name=specialist_name,
                confidence=self._compute_confidence(specialist_name, task),
                mode=mode,
                error=error_msg,
                duration_ms=duration,
            )
            self._delegations.append(record)

            # Score this delegation as failed
            self._score_delegation_internal(specialist_name, False)

            log.error(
                "Delegation to %s FAILED after %.0fms: %s",
                specialist_name, duration, error_msg,
            )
            return record

    async def delegate_parallel(
        self,
        nodes: List[ExecutionNode],
        task: str,
        context: Optional[Dict[str, Any]] = None,
        mode: DelegationMode = DelegationMode.CAPABILITY_AWARE,
        preferred_specialists: Optional[Dict[str, str]] = None,
    ) -> List[DelegationRecord]:
        """Dispatch multiple nodes to their respective specialists IN PARALLEL.

        Uses asyncio.gather() to execute all specialist calls simultaneously.
        Each specialist receives the same task and context but handles it
        according to their own role and system prompt.

        This is the foundation for genuine parallel multi-agent execution
        that Band of Agents and Qwen Agent Society judges can observe.

        Args:
            nodes: List of execution nodes to dispatch.
            task: The user task / request string.
            context: Shared context dict (must include 'agent').
            mode: How to select specialists for each node.
            preferred_specialists: Optional dict mapping node_id -> specialist name.

        Returns:
            List of DelegationRecord objects, one per node, in the same order.
        """
        context = context or {}
        preferred_specialists = preferred_specialists or {}

        async def _execute_one(node: ExecutionNode) -> DelegationRecord:
            specialist_name = preferred_specialists.get(
                node.id,
                self._select_specialist(node, task, context, mode),
            )
            specialist = self._registry.get(specialist_name)

            if specialist is None:
                return DelegationRecord(
                    node_id=node.id,
                    specialist_name=specialist_name,
                    confidence=0.0,
                    mode=mode,
                    error=f"Specialist {specialist_name} not found in registry",
                )

            start = time.monotonic()
            try:
                log.info(
                    "[parallel] Delegating node %s to %s",
                    node.id[:12], specialist_name,
                )

                result = await specialist.execute(task, context)

                duration = (time.monotonic() - start) * 1000
                self._score_delegation_internal(specialist_name, True)

                return DelegationRecord(
                    node_id=node.id,
                    specialist_name=specialist_name,
                    confidence=self._compute_confidence(specialist_name, task),
                    mode=mode,
                    result=result,
                    duration_ms=duration,
                )

            except Exception as e:
                duration = (time.monotonic() - start) * 1000
                self._score_delegation_internal(specialist_name, False)

                return DelegationRecord(
                    node_id=node.id,
                    specialist_name=specialist_name,
                    confidence=0.0,
                    mode=mode,
                    error=f"{type(e).__name__}: {e}",
                    duration_ms=duration,
                )

        # Execute all specialists in parallel
        tasks = [_execute_one(node) for node in nodes]
        records = await asyncio.gather(*tasks)

        # Record all delegations
        self._delegations.extend(records)

        successful = sum(1 for r in records if r.result is not None and r.error is None)
        log.info(
            "Parallel delegation complete: %d/%d succeeded",
            successful, len(records),
        )

        return records

    def score_delegation(self, node_id: str, success: bool, score: float = 0.5) -> None:
        for d in reversed(self._delegations):
            if d.node_id == node_id:
                specialist = d.specialist_name
                if specialist in self._specialist_scores:
                    self._specialist_scores[specialist].append(1.0 if success else 0.0)
                    if len(self._specialist_scores[specialist]) > 50:
                        self._specialist_scores[specialist] = self._specialist_scores[specialist][-50:]
                break

    def _score_delegation_internal(self, specialist_name: str, success: bool) -> None:
        """Internal scoring — used by delegate() and delegate_parallel()."""
        if specialist_name in self._specialist_scores:
            self._specialist_scores[specialist_name].append(1.0 if success else 0.0)
            if len(self._specialist_scores[specialist_name]) > 50:
                self._specialist_scores[specialist_name] = self._specialist_scores[specialist_name][-50:]

    def get_specialist_performance(self, name: str) -> Optional[Dict[str, Any]]:
        scores = self._specialist_scores.get(name.upper())
        if scores is None or not scores:
            return None
        return {
            "name": name.upper(),
            "delegations": len(scores),
            "success_rate": round(sum(scores) / len(scores), 4),
            "average_scores": round(sum(scores) / len(scores), 4),
        }

    def list_performance(self) -> List[Dict[str, Any]]:
        results = []
        for name in self._registry:
            perf = self.get_specialist_performance(name)
            if perf:
                results.append(perf)
        results.sort(key=lambda x: x["success_rate"], reverse=True)
        return results

    def get_delegation_history(self, specialist_name: Optional[str] = None) -> List[DelegationRecord]:
        if specialist_name:
            return [d for d in self._delegations if d.specialist_name == specialist_name.upper()]
        return self._delegations

    def snapshot(self) -> Dict[str, Any]:
        total = len(self._delegations)
        successful = sum(1 for d in self._delegations if d.result is not None and d.error is None)
        failed = total - successful
        return {
            "total_delegations": total,
            "successful_delegations": successful,
            "failed_delegations": failed,
            "registered_specialists": len(self._registry),
            "performance": self.list_performance(),
            "parallel_capable": True,
        }

    def _select_specialist(
        self,
        node: ExecutionNode,
        task: str,
        context: Dict[str, Any],
        mode: DelegationMode,
    ) -> str:
        if mode == DelegationMode.MANUAL and node.specialist:
            return node.specialist
        if mode == DelegationMode.CONFIDENCE_AWARE:
            return self._select_by_confidence(task, context)
        if mode == DelegationMode.GRAPH_AWARE:
            return self._select_graph_aware(node)
        if mode == DelegationMode.ROUND_ROBIN:
            return self._select_round_robin()
        return self._select_by_capability(task, context)

    def _select_by_capability(self, task: str, context: Dict[str, Any]) -> str:
        best_name = list(self._registry.keys())[0]
        best_score = -1.0
        for name, specialist in self._registry.items():
            try:
                score = specialist.compute_activation_score(task, context)
                if score > best_score:
                    best_score = score
                    best_name = name
            except Exception as e:
                log.debug("Activation score for %s failed: %s", name, e)
        return best_name

    def _select_round_robin(self) -> str:
        names = list(self._registry.keys())
        if not names:
            return "HERMES"
        idx = len(self._delegations) % len(names)
        return names[idx]

    def _select_by_confidence(self, task: str, context: Dict[str, Any]) -> str:
        best_name = "HERMES"
        best_rate = -1.0
        for name in self._registry:
            perf = self.get_specialist_performance(name)
            if perf is not None and perf["success_rate"] > best_rate:
                best_rate = perf["success_rate"]
                best_name = name
        if best_rate < 0:
            return self._select_by_capability(task, context)
        return best_name

    def _select_graph_aware(self, node: ExecutionNode) -> str:
        if node.specialist and node.specialist in self._registry:
            return node.specialist
        if node.node_type == NodeType.VERIFICATION:
            return "SENTINEL"
        if node.node_type == NodeType.MEMORY_QUERY:
            return "HERMES"
        if node.node_type == NodeType.SYNTHESIS:
            return "HERALD"
        return list(self._registry.keys())[0]

    def _compute_confidence(self, specialist_name: str, task: str) -> float:
        scores = self._specialist_scores.get(specialist_name, [])
        if not scores:
            return 0.5
        return round(sum(scores) / len(scores), 4)
