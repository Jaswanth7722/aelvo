"""Layer 7 — Runtime Consistency Validation.

The runtime itself must be verifiable. This subsystem validates:
  - graph integrity
  - serialization integrity
  - replay consistency
  - event ordering
  - mutex correctness
  - capability freshness
  - dependency validity

The runtime cannot trust itself blindly.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timezone

from .types import (
    ConsistencyResult,
    Severity,
    Confidence,
)

log = logging.getLogger("aelvo.runtime.verification.consistency")


class RuntimeConsistencyValidator:
    """Validates that the runtime itself is in a consistent state.

    Performs multiple checks and returns a detailed consistency report.
    """

    def __init__(self):
        self._check_history: List[ConsistencyResult] = []
        self._snapshot_hashes: Dict[str, str] = {}

    async def validate_all(
        self,
        graph_state: Optional[Dict[str, Any]] = None,
        serialization_state: Optional[Dict[str, Any]] = None,
        event_log_state: Optional[Dict[str, Any]] = None,
        mutex_state: Optional[Dict[str, Any]] = None,
        capability_state: Optional[Dict[str, Any]] = None,
        replay_state: Optional[Dict[str, Any]] = None,
    ) -> ConsistencyResult:
        """Run all consistency checks and produce a combined result."""
        start = time.monotonic()
        checks_performed: List[str] = []
        violations: List[Dict[str, Any]] = []
        flags: Dict[str, bool] = {}

        # 1. Graph integrity check
        checks_performed.append("graph_integrity")
        graph_ok = True
        if graph_state:
            graph_ok, graph_violations = self._check_graph_integrity(
                graph_state
            )
            violations.extend(graph_violations)
            if not graph_ok:
                log.warning(
                    f"Graph integrity violation: {graph_violations}"
                )
        flags["graph_integrity"] = graph_ok

        # 2. Serialization integrity check
        checks_performed.append("serialization_integrity")
        serial_ok = True
        if serialization_state:
            serial_ok, serial_violations = (
                self._check_serialization_integrity(serialization_state)
            )
            violations.extend(serial_violations)
        flags["serialization_integrity"] = serial_ok

        # 3. Replay consistency check
        checks_performed.append("replay_consistency")
        replay_ok = True
        if replay_state:
            replay_ok, replay_violations = (
                self._check_replay_consistency(replay_state)
            )
            violations.extend(replay_violations)
        flags["replay_consistency"] = replay_ok

        # 4. Mutex correctness check
        checks_performed.append("mutex_correctness")
        mutex_ok = True
        if mutex_state:
            mutex_ok, mutex_violations = self._check_mutex_correctness(
                mutex_state
            )
            violations.extend(mutex_violations)
        flags["mutex_correctness"] = mutex_ok

        # 5. Capability freshness check
        checks_performed.append("capability_freshness")
        cap_ok = True
        if capability_state:
            cap_ok, cap_violations = self._check_capability_freshness(
                capability_state
            )
            violations.extend(cap_violations)
        flags["capability_freshness"] = cap_ok

        # 6. Event ordering check
        checks_performed.append("event_ordering")
        events_ok = True
        if event_log_state:
            events_ok, events_violations = self._check_event_ordering(
                event_log_state
            )
            violations.extend(events_violations)
        flags["event_ordering"] = events_ok

        # 7. Dependency validity check
        checks_performed.append("dependency_validity")
        dep_ok = True
        if graph_state:
            dep_ok, dep_violations = self._check_dependency_validity(
                graph_state
            )
            violations.extend(dep_violations)
        flags["dependency_validity"] = dep_ok

        duration = (time.monotonic() - start) * 1000
        is_consistent = all(flags.values())

        result = ConsistencyResult(
            is_consistent=is_consistent,
            checks_performed=checks_performed,
            violations=violations,
            duration_ms=round(duration, 2),
            **flags,
        )

        self._check_history.append(result)

        if is_consistent:
            log.info(
                f"Consistency check PASSED "
                f"({len(checks_performed)} checks in {duration:.0f}ms)"
            )
        else:
            log.warning(
                f"Consistency check FAILED "
                f"({len(violations)} violations in {duration:.0f}ms)"
            )

        return result

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def _check_graph_integrity(
        self, graph_state: Dict[str, Any]
    ) -> tuple[bool, List[Dict[str, Any]]]:
        """Validate graph structure and state consistency."""
        violations: List[Dict[str, Any]] = []
        nodes = graph_state.get("nodes", {})
        edges = graph_state.get("edges", [])

        for node_id, node_info in nodes.items():
            # Check that node state is valid
            state = node_info.get("state")
            valid_states = {
                "pending",
                "ready",
                "running",
                "completed",
                "failed",
                "skipped",
                "blocked",
                "retrying",
            }
            if state and state not in valid_states:
                violations.append(
                    {
                        "check": "graph_integrity",
                        "node_id": node_id,
                        "detail": f"Invalid state: {state}",
                    }
                )

            # Check that completed/failed nodes have timestamps
            if state in ("completed", "failed"):
                if not node_info.get("end_time"):
                    violations.append(
                        {
                            "check": "graph_integrity",
                            "node_id": node_id,
                            "detail": f"Terminal node missing end_time",
                        }
                    )

        # Check edge consistency
        edge_sources = set()
        for edge in edges:
            source = edge.get("source_node_id") or edge.get("source")
            target = edge.get("target_node_id") or edge.get("target")
            if source not in nodes:
                violations.append(
                    {
                        "check": "graph_integrity",
                        "detail": f"Edge references non-existent source: {source}",
                    }
                )
            if target not in nodes:
                violations.append(
                    {
                        "check": "graph_integrity",
                        "detail": f"Edge references non-existent target: {target}",
                    }
                )
            edge_sources.add(source)

        return len(violations) == 0, violations

    def _check_serialization_integrity(
        self, serialization_state: Dict[str, Any]
    ) -> tuple[bool, List[Dict[str, Any]]]:
        """Validate serialization state."""
        violations: List[Dict[str, Any]] = []

        if not serialization_state.get("is_valid", True):
            violations.append(
                {
                    "check": "serialization_integrity",
                    "detail": serialization_state.get(
                        "error", "Serialization marked as invalid"
                    ),
                }
            )

        return len(violations) == 0, violations

    def _check_replay_consistency(
        self, replay_state: Dict[str, Any]
    ) -> tuple[bool, List[Dict[str, Any]]]:
        """Validate replay consistency."""
        violations: List[Dict[str, Any]] = []

        expected_hash = replay_state.get("expected_hash")
        actual_hash = replay_state.get("actual_hash")
        if expected_hash and actual_hash and expected_hash != actual_hash:
            violations.append(
                {
                    "check": "replay_consistency",
                    "detail": (
                        f"Hash mismatch: expected={expected_hash[:16]}..., "
                        f"actual={actual_hash[:16]}..."
                    ),
                    "expected_hash": expected_hash,
                    "actual_hash": actual_hash,
                }
            )

        divergent = replay_state.get("divergent_nodes", [])
        if divergent:
            violations.append(
                {
                    "check": "replay_consistency",
                    "detail": f"Divergent nodes: {divergent}",
                    "divergent_nodes": divergent,
                }
            )

        return len(violations) == 0, violations

    def _check_mutex_correctness(
        self, mutex_state: Dict[str, Any]
    ) -> tuple[bool, List[Dict[str, Any]]]:
        """Validate mutex/correctness."""
        violations: List[Dict[str, Any]] = []

        held_locks = mutex_state.get("held_locks", {})
        waiting = mutex_state.get("waiting", {})

        # Check for deadlock cycles in waiting graph
        if waiting:
            visited: Set[str] = set()
            for waiter, lock in waiting.items():
                if waiter in visited:
                    continue
                cycle = self._detect_lock_cycle(waiter, waiting, set())
                if cycle:
                    violations.append(
                        {
                            "check": "mutex_correctness",
                            "detail": f"Potential deadlock cycle: {' -> '.join(cycle)}",
                            "cycle": cycle,
                        }
                    )
                visited.add(waiter)

        return len(violations) == 0, violations

    def _detect_lock_cycle(
        self,
        start: str,
        waiting: Dict[str, str],
        visited: Set[str],
    ) -> Optional[List[str]]:
        """Detect if there's a cycle in the lock waiting graph."""
        if start in visited:
            return [start]
        visited.add(start)
        lock_id = waiting.get(start)
        if lock_id is None:
            return None
        # Find who holds this lock
        for waiter, held_lock in waiting.items():
            if held_lock == lock_id and waiter != start:
                cycle = self._detect_lock_cycle(waiter, waiting, visited)
                if cycle is not None:
                    return [start] + cycle
        return None

    def _check_capability_freshness(
        self, capability_state: Dict[str, Any]
    ) -> tuple[bool, List[Dict[str, Any]]]:
        """Validate capability freshness."""
        violations: List[Dict[str, Any]] = []

        health = capability_state.get("health", "")
        if health == "offline":
            violations.append(
                {
                    "check": "capability_freshness",
                    "detail": "Environment is OFFLINE",
                }
            )
        elif health == "degraded":
            violations.append(
                {
                    "check": "capability_freshness",
                    "detail": "Environment is DEGRADED",
                    "severity": Severity.WARNING,
                }
            )

        # Check for stale snapshot
        timestamp = capability_state.get("timestamp")
        if timestamp:
            # Normalize naive datetimes to UTC-aware to avoid
            # TypeError: can't subtract offset-naive and offset-aware datetimes
            if isinstance(timestamp, datetime) and timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            age_seconds = (
                datetime.now(timezone.utc) - timestamp
            ).total_seconds()
            if age_seconds > 300:  # 5 minutes
                violations.append(
                    {
                        "check": "capability_freshness",
                        "detail": (
                            f"Capability snapshot is {age_seconds:.0f}s old "
                            f"(threshold: 300s)"
                        ),
                        "age_seconds": age_seconds,
                    }
                )

        return len(violations) == 0, violations

    def _check_event_ordering(
        self, event_log_state: Dict[str, Any]
    ) -> tuple[bool, List[Dict[str, Any]]]:
        """Validate event ordering consistency."""
        violations: List[Dict[str, Any]] = []

        events = event_log_state.get("events", [])
        prev_timestamp: Optional[datetime] = None
        for event in events:
            ts = event.get("timestamp")
            # Normalize naive datetimes to UTC-aware for safe comparison
            if isinstance(ts, datetime) and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts and prev_timestamp:
                if ts < prev_timestamp:
                    violations.append(
                        {
                            "check": "event_ordering",
                            "detail": (
                                f"Out-of-order event: "
                                f"{event.get('event_id', 'unknown')}"
                            ),
                        }
                    )
            if ts:
                prev_timestamp = ts

        return len(violations) == 0, violations

    def _check_dependency_validity(
        self, graph_state: Dict[str, Any]
    ) -> tuple[bool, List[Dict[str, Any]]]:
        """Validate dependency graph for cycles and dead ends."""
        violations: List[Dict[str, Any]] = []
        nodes = graph_state.get("nodes", {})

        # Check for cycles using DFS
        visited: Set[str] = set()
        in_stack: Set[str] = set()

        def dfs_cycle(node_id: str, path: List[str]) -> bool:
            visited.add(node_id)
            in_stack.add(node_id)
            node_info = nodes.get(node_id, {})
            deps = node_info.get("dependencies", [])
            for dep in deps:
                if dep in nodes:
                    if dep in in_stack:
                        violations.append(
                            {
                                "check": "dependency_validity",
                                "detail": (
                                    f"Circular dependency: "
                                    f"{' -> '.join(path + [dep])}"
                                ),
                                "cycle": path + [dep],
                            }
                        )
                        return True
                    if dep not in visited:
                        if dfs_cycle(dep, path + [dep]):
                            return True
            in_stack.discard(node_id)
            return False

        for node_id in nodes:
            if node_id not in visited:
                dfs_cycle(node_id, [node_id])

        return len(violations) == 0, violations

    # ------------------------------------------------------------------
    # Snapshot hashing for integrity comparison
    # ------------------------------------------------------------------

    def take_snapshot_hash(
        self, name: str, state: Dict[str, Any]
    ) -> str:
        """Take a deterministic hash of a state snapshot for later comparison."""
        import json

        serialized = json.dumps(state, sort_keys=True, default=str)
        hash_value = hashlib.sha256(serialized.encode()).hexdigest()
        self._snapshot_hashes[name] = hash_value
        return hash_value

    def verify_snapshot_hash(
        self, name: str, state: Dict[str, Any]
    ) -> bool:
        """Verify a state snapshot against a previously taken hash."""
        import json

        expected = self._snapshot_hashes.get(name)
        if expected is None:
            return True  # No snapshot to compare against
        serialized = json.dumps(state, sort_keys=True, default=str)
        actual = hashlib.sha256(serialized.encode()).hexdigest()
        return expected == actual

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def check_history(self) -> List[ConsistencyResult]:
        return list(self._check_history)

    def is_consistently_healthy(self, recent_checks: int = 5) -> bool:
        """Check if the most recent consistency checks all passed."""
        recent = self._check_history[-recent_checks:]
        return all(r.is_consistent for r in recent)
