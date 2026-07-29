"""verification_coordinator.py â€” Verification coordination for AELVO OMEGA."""
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional

from runtime_next.verification.pipeline import VerificationPipeline
from runtime_next.verification.types import (
    VerificationType, VerificationManifest, VerificationScope,
    VerificationResult, Confidence, Severity, Retryability,
)
from runtime_next.models.node import NodeState

log = logging.getLogger("aelvo.verification_coordinator")


class VerificationCoordinator:
    """Coordinates verification across the pipeline, graph, and sandbox.

    Responsibilities:
    - Register plan-specific verification handlers
    - Run plan verification checks against graph results
    - Build verification summaries
    - Run sandbox verification on graph nodes
    """

    def __init__(self, verification_pipeline: Optional[VerificationPipeline] = None,
                 runtime_graph=None, runtime_bus=None):
        self.verification_pipeline = verification_pipeline
        self.runtime_graph = runtime_graph
        self.runtime_bus = runtime_bus
        self._last_plan_phase_node_map: Dict[str, str] = {}

    # Mapping from architect plan method â†’ pipeline type
    _VERIFICATION_METHOD_MAP = {
        "unit_test": VerificationType.UNIT_TEST,
        "integration_test": VerificationType.INTEGRATION_TEST,
        "typecheck": VerificationType.TYPECHECK,
        "lint": VerificationType.LINT,
        "security_scan": VerificationType.SECURITY_SCAN,
        "architecture_check": VerificationType.ARCHITECTURE_VALIDATION,
        "comparison": VerificationType.RUNTIME_VALIDATION,
        "manual_review": None,
    }

    _STANDARD_VERIFIER_TYPES: set = {
        "lint", "typecheck", "sandbox_validation", "graph_consistency",
    }

    def register_plan_checks(self, plan) -> set:
        """Register dynamic verifier handlers for plan-specific checks."""
        registered_custom: set = set()

        for check in plan.verification_plan.checks:
            vtype = self._VERIFICATION_METHOD_MAP.get(check.method.value)
            if vtype is None or self.verification_pipeline is None:
                continue

            if vtype.value in registered_custom or vtype.value in self._STANDARD_VERIFIER_TYPES:
                continue

            registered_custom.add(vtype.value)

            check_desc = check.description
            check_method = check.method.value
            check_vtype = vtype
            check_is_blocking = check.is_blocking

            async def _plan_check_handler(
                node_id: str, scope: VerificationScope, context: Dict[str, Any],
                _desc=check_desc, _method=check_method,
                _vtype=check_vtype, _is_blocking=check_is_blocking,
            ) -> VerificationResult:
                start = time.monotonic()
                diagnostics: List[str] = []
                evidence_found = False

                if self.runtime_graph:
                    node = self.runtime_graph.nodes.get(node_id)
                    if node and node.result:
                        result_output = str(node.result.get("output", ""))
                        for keyword in _desc.lower().split()[:5]:
                            if keyword in result_output.lower():
                                diagnostics.append(
                                    f"Found evidence for '{_desc[:40]}' in node result"
                                )
                                evidence_found = True
                                break

                if not evidence_found:
                    msg = f"No explicit evidence for '{_desc[:40]}'"
                    if _is_blocking:
                        diagnostics.append(f"BLOCKING: {msg} â€” check FAILED")
                    else:
                        diagnostics.append(f"{msg} â€” passed by default")

                success = evidence_found or not _is_blocking
                return VerificationResult(
                    verification_id=hashlib.sha256(
                        f"plan_{_method}_{node_id}_{time.time()}".encode()
                    ).hexdigest()[:16],
                    node_id=node_id,
                    verification_type=_vtype,
                    duration_ms=(time.monotonic() - start) * 1000,
                    success=success,
                    confidence=Confidence.MEDIUM,
                    severity=Severity.INFO if success else Severity.WARNING,
                    retryability=Retryability.SAFE,
                    diagnostics=diagnostics or [f"Plan check '{_desc[:40]}' passed"],
                    provenance=f"plan_verification_{_method}",
                )

            self.verification_pipeline.register_verifier(vtype, _plan_check_handler)
            log.info("Registered plan verifier for %s: '%s'", vtype.value, check.description[:50])

        return registered_custom

    async def run_plan_verification(
        self, plan, phase_node_map: Dict[str, str],
    ) -> List[VerificationResult]:
        """Run plan verification checks against graph execution results."""
        all_results: List[VerificationResult] = []
        self._last_plan_phase_node_map = dict(phase_node_map)

        if self.verification_pipeline is None:
            return all_results

        checks_by_phase: Dict[str, list] = {}
        for check in plan.verification_plan.checks:
            checks_by_phase.setdefault(check.phase_id, []).append(check)

        for phase_id, checks in checks_by_phase.items():
            node_id = phase_node_map.get(phase_id)
            if node_id is None:
                continue

            required_vtypes: List[VerificationType] = []
            blocking_vtypes: List[VerificationType] = []
            for check in checks:
                vtype = self._VERIFICATION_METHOD_MAP.get(check.method.value)
                if vtype is None:
                    continue
                required_vtypes.append(vtype)
                if check.is_blocking:
                    blocking_vtypes.append(vtype)

            if not required_vtypes:
                continue

            manifest = VerificationManifest(required=required_vtypes, blocking=blocking_vtypes)
            affected: List[str] = []
            if self.runtime_graph:
                node = self.runtime_graph.nodes.get(node_id)
                if node and hasattr(node, "files") and node.files:
                    affected = list(node.files)

            scope = VerificationScope(
                affected_files=affected, is_minimal=True,
                provenance=f"plan_phase_{phase_id}",
            )

            try:
                results = await self.verification_pipeline.verify(
                    node_id=node_id, manifest=manifest, scope=scope,
                    context={"plan_id": plan.id, "phase_id": phase_id},
                )
                all_results.extend(results)
            except Exception as e:
                log.error("Verification error for phase %s: %s", phase_id, e)

        return all_results

    def build_verification_summary(
        self, plan, results: List[VerificationResult],
        phase_names: Optional[Dict[str, str]] = None,
    ) -> str:
        """Build a formatted summary of verification results by phase."""
        if not results:
            return ""

        plan_results = [r for r in results if getattr(r, 'provenance', '').startswith('plan_verification')]
        if not plan_results:
            return ""

        node_to_phase: Dict[str, str] = {v: k for k, v in self._last_plan_phase_node_map.items()}
        phase_names = phase_names or {}

        by_phase: Dict[str, List[VerificationResult]] = {}
        for vr in plan_results:
            pid = node_to_phase.get(vr.node_id, "unknown")
            by_phase.setdefault(pid, []).append(vr)

        total_passed = sum(1 for r in plan_results if r.success)
        total_failed = sum(1 for r in plan_results if not r.success)

        lines = ["", "  â”€â”€ VERIFICATION RESULTS â”€â”€",
                 f"  {total_passed} passed, {total_failed} failed across {len(by_phase)} phase(s)"]

        for phase_id in sorted(by_phase.keys()):
            vrs = by_phase[phase_id]
            pname = phase_names.get(phase_id, phase_id)
            passed = sum(1 for r in vrs if r.success)
            failed = sum(1 for r in vrs if not r.success)
            icon = "âœ…" if failed == 0 else "âš ï¸"
            lines.append(f"    {icon} Phase '{pname}': {passed} passed, {failed} failed")
            for vr in vrs:
                ck = vr.diagnostics[0][:60] if vr.diagnostics else ""
                lines.append(f"      {'âœ“' if vr.success else 'âœ—'} [{vr.verification_type.value}] {ck}")

        return "\n".join(lines)

    async def verify_sandbox_results(self) -> list:
        """Run sandbox verification on all nodes with sandbox results."""
        all_results = []
        if self.verification_pipeline is None or self.runtime_graph is None:
            return all_results

        for node_id, node in list(self.runtime_graph.nodes.items()):
            if not hasattr(node, "result") or not node.result:
                continue

            sandbox_result = node.result.get("sandbox_result")
            if not sandbox_result:
                continue

            manifest = VerificationManifest(
                required=[VerificationType.SANDBOX_VALIDATION],
                blocking=[VerificationType.SANDBOX_VALIDATION],
            )

            affected = list(node.files) if hasattr(node, "files") and node.files else []
            scope = VerificationScope(
                affected_files=affected, is_minimal=True,
                provenance="sandbox_result",
            )

            try:
                results = await self.verification_pipeline.verify(
                    node_id=node_id, manifest=manifest, scope=scope,
                    context={"sandbox_result": sandbox_result},
                )
                all_results.extend(results)

                for vr in results:
                    if not vr.success:
                        log.warning("Sandbox verification FAILED for %s", node_id)
                        if self.runtime_bus:
                            from runtime_next.verification.events import VerificationFailedEvent
                            fail_event = VerificationFailedEvent(
                                event_id=vr.verification_id, node_id=node_id,
                                verification_type=VerificationType.SANDBOX_VALIDATION,
                                result=vr, duration_ms=vr.duration_ms,
                            )
                            await self.runtime_bus.publish(fail_event)

                            if hasattr(node, "state") and node.state != NodeState.FAILED:
                                from_state = (
                                    node.state.value if isinstance(node.state, NodeState)
                                    else str(node.state) or NodeState.PENDING.value
                                )
                                from runtime_next.models.events import NodeTransitionEvent
                                transition = NodeTransitionEvent(
                                    id=f"sandbox_fail_{node_id}_{time.time()}",
                                    node_id=node_id, from_state=from_state,
                                    to_state=NodeState.FAILED.value,
                                    reason="; ".join(vr.diagnostics),
                                )
                                await self.runtime_bus.publish(transition)
            except Exception as e:
                log.error("Sandbox verification error for %s: %s", node_id, e)

        return all_results
