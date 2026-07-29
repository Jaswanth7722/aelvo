"""
driven_recovery.py — Phase 11: Verification-Driven Recovery Pipeline

Orchestrates the full recovery lifecycle as a first-class pipeline:

  Verify → Classify → Assess Safety → Govern → Recover → Re-verify → Record

Unlike the lower-level RecoveryEngine which handles individual node failures,
this pipeline:
- Produces a comprehensive PipelineResult with full traceability
- Integrates with the planning subsystem's PlanEvolutionEngine
- Supports re-verification after recovery to confirm the fix
- Tracks success rates per failure type for learning
- Provides a clean API for the orchestrator and CLI

Flow:
  1. Classify: Use FailureClassifier to identify the failure type
  2. Strategy: Look up the recovery strategy for the classified failure
  3. Govern: Check governance (autonomy boundary enforcement)
  4. Safety: Evaluate retry safety via RetrySafetyEngine
  5. Recover: Execute the recovery strategy
  6. Re-Verify: Run relevant verifications to confirm recovery succeeded
  7. Record: Persist the outcome to LearnedRecoveryMemory
  8. Evolve: Optionally notify PlanEvolutionEngine of systemic failures
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Callable, Awaitable

from .types import (
    FailureClassification,
    VerificationType,
    ClassificationResult,
    RecoveryStrategy,
    RecoveryAction,
    RetryDecision,
    GovernanceDecision,
    VerificationResult,
    VerificationScope,
    VerificationManifest,
    Confidence,
    Severity,
    Retryability,
    DEFAULT_RECOVERY_MAP,
)
from .classifier import FailureClassifier
from .recovery import RecoveryStrategyEngine
from .retry_safety import RetrySafetyEngine
from .injector import RecoveryNodeInjector
from .governance import RecoveryGovernance
from .memory import LearnedRecoveryMemory
from .pipeline import VerificationPipeline

log = logging.getLogger("aelvo.runtime.verification.driven_recovery")


# ---------------------------------------------------------------------------
# Pipeline Phase Enum
# ---------------------------------------------------------------------------


class RecoveryPipelinePhase(str, Enum):
    """Phases of the verification-driven recovery pipeline."""
    INITIATED = "initiated"
    CLASSIFYING = "classifying"
    CLASSIFIED = "classified"
    GOVERNING = "governing"
    GOVERNED = "governed"
    ASSESSING_SAFETY = "assessing_safety"
    SAFETY_ASSESSED = "safety_assessed"
    RECOVERING = "recovering"
    RECOVERED = "recovered"
    REVERIFYING = "reverifying"
    REVERIFIED = "reverified"
    RECORDING = "recording"
    RECORDED = "recorded"
    EVOLVING = "evolving"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"
    ABORTED = "aborted"


# ---------------------------------------------------------------------------
# Pipeline Result
# ---------------------------------------------------------------------------


@dataclass
class RecoveryPipelineResult:
    """Complete result of a verification-driven recovery pipeline run.

    Contains full traceability of every phase, decision, and action.
    """
    pipeline_id: str
    node_id: str
    status: RecoveryPipelinePhase
    started_at: float = 0.0
    completed_at: float = 0.0
    error_message: str = ""

    # Phase outputs
    classification: Optional[ClassificationResult] = None
    strategy: Optional[RecoveryStrategy] = None
    governance: Optional[GovernanceDecision] = None
    retry_decision: Optional[RetryDecision] = None
    recovery_action: Optional[RecoveryAction] = None
    pre_recovery_verifications: List[VerificationResult] = field(default_factory=list)
    post_recovery_verifications: List[VerificationResult] = field(default_factory=list)
    recovery_memory_entry_id: Optional[str] = None
    plan_evolution_notified: bool = False

    @property
    def duration_ms(self) -> float:
        if self.completed_at and self.started_at:
            return (self.completed_at - self.started_at) * 1000.0
        return 0.0

    @property
    def recovery_success(self) -> Optional[bool]:
        """Whether the recovery itself was successful."""
        if self.recovery_action is not None:
            return self.recovery_action.success
        return None

    @property
    def reverify_passed(self) -> Optional[bool]:
        """Whether post-recovery verification passed."""
        if not self.post_recovery_verifications:
            return None
        return all(v.success for v in self.post_recovery_verifications)

    @property
    def overall_success(self) -> bool:
        """Overall pipeline success: recovery + re-verification both passed."""
        if self.recovery_success is None or self.status in (
            RecoveryPipelinePhase.FAILED,
            RecoveryPipelinePhase.BLOCKED,
            RecoveryPipelinePhase.ABORTED,
        ):
            return False
        if self.recovery_success and self.reverify_passed is None:
            return True  # No re-verification needed
        return bool(self.recovery_success and self.reverify_passed)

    @property
    def failure_type(self) -> Optional[str]:
        if self.classification is not None:
            return self.classification.primary.value
        return None

    def to_summary(self) -> str:
        """Compact human-readable summary."""
        lines = [
            f"Recovery Pipeline: {self.pipeline_id[:12]}",
            f"  Node: {self.node_id[:24]}",
            f"  Status: {self.status.value}",
            f"  Duration: {self.duration_ms:.0f}ms",
        ]
        if self.failure_type:
            lines.append(f"  Failure: {self.failure_type}")
        if self.recovery_action:
            lines.append(f"  Action: {self.recovery_action.action_type} ({'success' if self.recovery_action.success else 'failed'})")
        if self.post_recovery_verifications:
            passed = sum(1 for v in self.post_recovery_verifications if v.success)
            lines.append(f"  Re-verify: {passed}/{len(self.post_recovery_verifications)} passed")
        if self.error_message:
            lines.append(f"  Error: {self.error_message[:100]}")
        return "\n".join(lines)

    def format_report(self) -> Dict[str, Any]:
        """Structured report for integration points."""
        return {
            "pipeline_id": self.pipeline_id,
            "node_id": self.node_id,
            "status": self.status.value,
            "duration_ms": self.duration_ms,
            "overall_success": self.overall_success,
            "failure_type": self.failure_type,
            "recovery_action": self.recovery_action.action_type if self.recovery_action else None,
            "recovery_success": self.recovery_success,
            "reverify_passed": self.reverify_passed,
            "governance_verdict": self.governance.verdict if self.governance else None,
            "retry_safe": self.retry_decision.can_retry if self.retry_decision else None,
            "plan_evolution_notified": self.plan_evolution_notified,
            "error": self.error_message or None,
        }


# ---------------------------------------------------------------------------
# Recovery Pipeline Configuration
# ---------------------------------------------------------------------------


@dataclass
class RecoveryPipelineConfig:
    """Configuration for the VerificationDrivenRecoveryPipeline."""
    max_retries_per_failure: int = 3
    enable_reverify: bool = True
    enable_plan_evolution: bool = True
    reverify_types: List[VerificationType] = field(default_factory=lambda: [
        VerificationType.LINT,
        VerificationType.TYPECHECK,
    ])
    track_success_rates: bool = True


# ---------------------------------------------------------------------------
# Verification-Driven Recovery Pipeline
# ---------------------------------------------------------------------------


class VerificationDrivenRecoveryPipeline:
    """Orchestrates the full verification-driven recovery lifecycle.

    The pipeline wraps the existing verification subsystem components
    (FailureClassifier, RecoveryStrategyEngine, RetrySafetyEngine,
    RecoveryGovernance, RecoveryNodeInjector, LearnedRecoveryMemory)
    into a single, observable, replayable flow.

    It optionally integrates with the planning subsystem's
    PlanEvolutionEngine to trigger plan revisions when recovery
    patterns indicate systemic issues.
    """

    def __init__(
        self,
        classifier: Optional[FailureClassifier] = None,
        recovery_strategies: Optional[RecoveryStrategyEngine] = None,
        retry_safety: Optional[RetrySafetyEngine] = None,
        governance: Optional[RecoveryGovernance] = None,
        injector: Optional[RecoveryNodeInjector] = None,
        recovery_memory: Optional[LearnedRecoveryMemory] = None,
        verification_pipeline: Optional[VerificationPipeline] = None,
        plan_evolution_engine: Any = None,
        config: Optional[RecoveryPipelineConfig] = None,
    ):
        self._classifier = classifier or FailureClassifier()
        self._recovery_strategies = recovery_strategies or RecoveryStrategyEngine()
        self._retry_safety = retry_safety or RetrySafetyEngine()
        self._governance = governance or RecoveryGovernance()
        self._injector = injector or RecoveryNodeInjector()
        self._recovery_memory = recovery_memory or LearnedRecoveryMemory()
        self._verification_pipeline = verification_pipeline or VerificationPipeline()
        self._plan_evolution_engine = plan_evolution_engine
        self._config = config or RecoveryPipelineConfig()

        # Track success rates per failure type for learning
        self._success_rates: Dict[str, Dict[str, Any]] = {}
        self._pipeline_history: List[RecoveryPipelineResult] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(
        self,
        node_id: str,
        error_message: str = "",
        stderr: str = "",
        stdout: str = "",
        exit_code: Optional[int] = None,
        graph_state: Optional[Dict[str, Any]] = None,
        capability_state: Optional[Dict[str, Any]] = None,
        verification_results: Optional[List[Dict[str, Any]]] = None,
        execution_history: Optional[List[Dict[str, Any]]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> RecoveryPipelineResult:
        """Run the full verification-driven recovery pipeline.

        Args:
            node_id: The execution node that failed.
            error_message: Primary error message from the failure.
            stderr: Raw stderr output from the failure.
            stdout: Raw stdout output from the failure.
            exit_code: Process exit code (if applicable).
            graph_state: Execution graph state snapshot.
            capability_state: Capability registry snapshot.
            verification_results: Prior verification results.
            execution_history: Prior execution history for the node.
            context: Additional context dict for recovery executors.

        Returns:
            RecoveryPipelineResult with full phase traceability.
        """
        pipeline_id = hashlib.sha256(
            f"driven_recovery_{node_id}_{time.time()}".encode()
        ).hexdigest()[:16]

        ctx = context or {}

        result = RecoveryPipelineResult(
            pipeline_id=pipeline_id,
            node_id=node_id,
            status=RecoveryPipelinePhase.INITIATED,
            started_at=time.time(),
        )

        log.info(
            "Recovery pipeline started for node=%s (id=%s)",
            node_id, pipeline_id[:12],
        )

        # Phase 1: Classify the failure
        result = await self._phase_classify(
            result, error_message, stderr, stdout, exit_code,
            graph_state, capability_state, verification_results,
            execution_history,
        )
        if result.status in (RecoveryPipelinePhase.FAILED, RecoveryPipelinePhase.ABORTED):
            return self._finalize(result)

        # Phase 2: Get recovery strategy
        result = self._phase_get_strategy(result)
        if result.status in (RecoveryPipelinePhase.FAILED, RecoveryPipelinePhase.BLOCKED):
            return self._finalize(result)

        # Phase 3: Governance check
        result = await self._phase_govern(result, ctx)
        if result.status in (RecoveryPipelinePhase.BLOCKED, RecoveryPipelinePhase.ABORTED):
            return self._finalize(result)

        # Phase 4: Retry safety assessment
        result = await self._phase_assess_safety(
            result, graph_state, capability_state, ctx,
        )
        if result.status == RecoveryPipelinePhase.BLOCKED:
            return self._finalize(result)

        # Phase 5: Execute recovery
        result = await self._phase_recover(result, ctx)
        if result.status == RecoveryPipelinePhase.FAILED:
            return self._finalize(result)

        # Phase 6: Re-verify after recovery
        if self._config.enable_reverify:
            result = await self._phase_reverify(result, graph_state or {})
            # Don't abort on re-verify failure — recovery may still be partial

        # Phase 7: Record to memory
        result = await self._phase_record(result, ctx)

        # Phase 8: Trigger plan evolution (if configured)
        if self._config.enable_plan_evolution:
            result = await self._phase_evolve(result)

        # Finalize
        return self._finalize(result)

    # ------------------------------------------------------------------
    # Phase 1: Classify
    # ------------------------------------------------------------------

    async def _phase_classify(
        self,
        result: RecoveryPipelineResult,
        error_message: str,
        stderr: str,
        stdout: str,
        exit_code: Optional[int],
        graph_state: Optional[Dict[str, Any]],
        capability_state: Optional[Dict[str, Any]],
        verification_results: Optional[List[Dict[str, Any]]],
        execution_history: Optional[List[Dict[str, Any]]],
    ) -> RecoveryPipelineResult:
        """Classify the failure using all available signals."""
        result.status = RecoveryPipelinePhase.CLASSIFYING

        try:
            classification = await self._classifier.classify(
                error_message=error_message,
                stderr=stderr,
                stdout=stdout,
                exit_code=exit_code,
                graph_state=graph_state,
                capability_state=capability_state,
                verification_results=verification_results,
                execution_history=execution_history,
            )
            result.classification = classification
            result.status = RecoveryPipelinePhase.CLASSIFIED

            log.info(
                "Phase 1 [Classify]: %s (confidence=%s, score=%.2f)",
                classification.primary.value,
                classification.confidence.value,
                classification.confidence_score,
            )

        except Exception as exc:
            result.status = RecoveryPipelinePhase.FAILED
            result.error_message = f"Classification failed: {exc}"
            log.error("Phase 1 [Classify] failed: %s", exc)

        return result

    # ------------------------------------------------------------------
    # Phase 2: Get Strategy
    # ------------------------------------------------------------------

    def _phase_get_strategy(
        self,
        result: RecoveryPipelineResult,
    ) -> RecoveryPipelineResult:
        """Look up the recovery strategy for the classified failure."""
        if result.classification is None:
            result.status = RecoveryPipelinePhase.FAILED
            result.error_message = "Cannot get strategy: no classification"
            return result

        result.status = RecoveryPipelinePhase.GOVERNING

        strategy = self._recovery_strategies.get_strategy(
            result.classification.primary,
        )
        result.strategy = strategy

        if strategy is None:
            result.status = RecoveryPipelinePhase.BLOCKED
            result.error_message = (
                f"No recovery strategy for "
                f"{result.classification.primary.value}"
            )
            log.warning("Phase 2 [Strategy]: No strategy for %s", result.classification.primary.value)
        else:
            log.info(
                "Phase 2 [Strategy]: %s → %s (max_retries=%d)",
                result.classification.primary.value,
                strategy.name,
                strategy.max_retries,
            )

        return result

    # ------------------------------------------------------------------
    # Phase 3: Governance
    # ------------------------------------------------------------------

    async def _phase_govern(
        self,
        result: RecoveryPipelineResult,
        context: Dict[str, Any],
    ) -> RecoveryPipelineResult:
        """Check governance — should autonomous recovery proceed."""
        if result.strategy is None or result.classification is None:
            result.status = RecoveryPipelinePhase.FAILED
            result.error_message = "Cannot govern: no strategy or classification"
            return result

        result.status = RecoveryPipelinePhase.GOVERNING

        try:
            governance = await self._governance.decide(
                failure_type=result.classification.primary,
                strategy=result.strategy,
                action_type="retry",
                context={
                    "retry_count": self._retry_safety.get_retry_count(result.node_id),
                    "error_message": result.error_message or "",
                    **context,
                },
            )
            result.governance = governance
            result.status = RecoveryPipelinePhase.GOVERNED

            if governance.should_stop_autonomy():
                result.status = RecoveryPipelinePhase.ABORTED
                result.error_message = governance.reason
                log.warning("Phase 3 [Govern]: Blocked — %s", governance.reason)
            else:
                log.info(
                    "Phase 3 [Govern]: %s (danger=%s, intervene=%s)",
                    governance.verdict,
                    governance.danger_assessment,
                    governance.requires_user_intervention,
                )

        except Exception as exc:
            result.status = RecoveryPipelinePhase.ABORTED
            result.error_message = f"Governance check failed: {exc}"
            log.error("Phase 3 [Govern] failed: %s", exc)

        return result

    # ------------------------------------------------------------------
    # Phase 4: Retry Safety
    # ------------------------------------------------------------------

    async def _phase_assess_safety(
        self,
        result: RecoveryPipelineResult,
        graph_state: Optional[Dict[str, Any]],
        capability_state: Optional[Dict[str, Any]],
        context: Dict[str, Any],
    ) -> RecoveryPipelineResult:
        """Assess whether retry is safe."""
        if result.classification is None:
            result.status = RecoveryPipelinePhase.FAILED
            result.error_message = "Cannot assess safety: no classification"
            return result

        result.status = RecoveryPipelinePhase.ASSESSING_SAFETY

        try:
            strategy = result.strategy
            retryability = Retryability.SAFE
            if strategy is not None:
                retryability = (
                    Retryability.SAFE if strategy.max_retries > 0
                    else Retryability.NEVER
                )

            retry_decision = await self._retry_safety.evaluate(
                node_id=result.node_id,
                classification=result.classification.primary,
                retryability=retryability,
                graph_state=graph_state,
                capability_state=capability_state,
            )
            result.retry_decision = retry_decision
            result.status = RecoveryPipelinePhase.SAFETY_ASSESSED

            if not retry_decision.can_retry:
                result.status = RecoveryPipelinePhase.BLOCKED
                result.error_message = retry_decision.reason
                log.warning(
                    "Phase 4 [Safety]: Blocked — %s",
                    retry_decision.reason,
                )
            else:
                log.info(
                    "Phase 4 [Safety]: OK (backoff=%.1fs, replay_risk=%.2f)",
                    retry_decision.suggested_backoff,
                    retry_decision.replay_divergence_risk,
                )

        except Exception as exc:
            result.status = RecoveryPipelinePhase.BLOCKED
            result.error_message = f"Safety assessment failed: {exc}"
            log.error("Phase 4 [Safety] failed: %s", exc)

        return result

    # ------------------------------------------------------------------
    # Phase 5: Execute Recovery
    # ------------------------------------------------------------------

    async def _phase_recover(
        self,
        result: RecoveryPipelineResult,
        context: Dict[str, Any],
    ) -> RecoveryPipelineResult:
        """Execute the recovery strategy."""
        if result.classification is None:
            result.status = RecoveryPipelinePhase.FAILED
            result.error_message = "Cannot recover: no classification"
            return result

        result.status = RecoveryPipelinePhase.RECOVERING

        try:
            action = await self._recovery_strategies.execute_recovery(
                node_id=result.node_id,
                failure_type=result.classification.primary,
                classification_result=result.classification,
                context={
                    "retry_count": self._retry_safety.get_retry_count(result.node_id),
                    **context,
                },
            )
            result.recovery_action = action

            if action is None:
                result.status = RecoveryPipelinePhase.FAILED
                result.error_message = "Recovery strategy returned no action"
                log.warning("Phase 5 [Recover]: No action returned")
            else:
                result.status = RecoveryPipelinePhase.RECOVERED
                log.info(
                    "Phase 5 [Recover]: %s → %s (%s)",
                    action.action_type,
                    "SUCCESS" if action.success else "FAILED",
                    action.description[:80],
                )

                # If a recovery node was injected, track it
                if action.injected_node_id:
                    log.info(
                        "  Injected recovery node: %s",
                        action.injected_node_id,
                    )

        except Exception as exc:
            result.status = RecoveryPipelinePhase.FAILED
            result.error_message = f"Recovery execution failed: {exc}"
            log.error("Phase 5 [Recover] failed: %s", exc)

        return result

    # ------------------------------------------------------------------
    # Phase 6: Re-verify After Recovery
    # ------------------------------------------------------------------

    async def _phase_reverify(
        self,
        result: RecoveryPipelineResult,
        graph_state: Dict[str, Any],
    ) -> RecoveryPipelineResult:
        """Run verifications after recovery to confirm the fix."""
        result.status = RecoveryPipelinePhase.REVERIFYING

        try:
            # Build a verification manifest with the configured re-verify types
            manifest = VerificationManifest(
                required=self._config.reverify_types,
                blocking=self._config.reverify_types,
            )

            # Determine scope from graph state
            affected_files = graph_state.get("affected_files", [])
            scope = VerificationScope(
                affected_files=affected_files,
                provenance="recovery_pipeline",
            )

            reverify_results = await self._verification_pipeline.verify(
                node_id=result.node_id,
                manifest=manifest,
                scope=scope,
                context={"pipeline_id": result.pipeline_id},
            )
            result.post_recovery_verifications = reverify_results
            result.status = RecoveryPipelinePhase.REVERIFIED

            passed = sum(1 for v in reverify_results if v.success)
            total = len(reverify_results)
            log.info(
                "Phase 6 [Re-verify]: %d/%d passed",
                passed, total,
            )

        except Exception as exc:
            # Re-verify failure is not fatal — recovery may have succeeded
            # even if re-verification couldn't run
            log.warning("Phase 6 [Re-verify] failed: %s", exc)
            result.status = RecoveryPipelinePhase.REVERIFIED  # Continue

        return result

    # ------------------------------------------------------------------
    # Phase 7: Record to Recovery Memory
    # ------------------------------------------------------------------

    async def _phase_record(
        self,
        result: RecoveryPipelineResult,
        context: Dict[str, Any],
    ) -> RecoveryPipelineResult:
        """Record the recovery outcome to learned recovery memory."""
        result.status = RecoveryPipelinePhase.RECORDING

        try:
            if result.recovery_action is not None:
                entry = await self._recovery_memory.record(
                    action=result.recovery_action,
                    strategy=result.strategy,
                    context={
                        "node_description": context.get("node_description", ""),
                        **context,
                    },
                )
                result.recovery_memory_entry_id = entry.id
                result.status = RecoveryPipelinePhase.RECORDED
                log.info(
                    "Phase 7 [Record]: Stored recovery memory (id=%s)",
                    entry.id[:12],
                )

                # Update success rate tracking
                if self._config.track_success_rates and result.classification:
                    fc = result.classification.primary.value
                    if fc not in self._success_rates:
                        self._success_rates[fc] = {"attempts": 0, "successes": 0}
                    self._success_rates[fc]["attempts"] += 1
                    if result.recovery_action.success:
                        self._success_rates[fc]["successes"] += 1
            else:
                result.status = RecoveryPipelinePhase.RECORDED  # No action to record

        except Exception as exc:
            # Recording failure is non-fatal
            log.warning("Phase 7 [Record] failed: %s", exc)
            result.status = RecoveryPipelinePhase.RECORDED

        return result

    # ------------------------------------------------------------------
    # Phase 8: Trigger Plan Evolution
    # ------------------------------------------------------------------

    async def _phase_evolve(
        self,
        result: RecoveryPipelineResult,
    ) -> RecoveryPipelineResult:
        """Trigger plan evolution if recovery indicates systemic issues.

        Calls the PlanEvolutionEngine when:
        - Recovery failed entirely
        - The same failure type occurs repeatedly
        - A CRITICAL severity failure was encountered
        """
        result.status = RecoveryPipelinePhase.EVOLVING

        if self._plan_evolution_engine is None:
            result.status = RecoveryPipelinePhase.COMPLETED
            return result

        if result.classification is None:
            result.status = RecoveryPipelinePhase.COMPLETED
            return result

        try:
            should_evolve = False
            failure_type = result.classification.primary
            fc_key = failure_type.value

            # Check 1: Recovery failed entirely
            if result.recovery_action is not None and not result.recovery_action.success:
                should_evolve = True
                log.info("Evolution trigger: recovery failed for %s", fc_key)

            # Check 2: Same failure type occurs repeatedly (multiple attempts)
            if self._config.track_success_rates and fc_key in self._success_rates:
                stats = self._success_rates[fc_key]
                if stats["attempts"] >= 2 and stats["successes"] == 0:
                    should_evolve = True
                    log.info(
                        "Evolution trigger: %s failed %d times without success",
                        fc_key, stats["attempts"],
                    )

            # Check 3: Critical severity (sandbox escape, architecture violation)
            if failure_type in (
                FailureClassification.SANDBOX_ESCAPE,
                FailureClassification.ARCHITECTURE_VIOLATION,
            ):
                should_evolve = True
                log.info("Evolution trigger: critical failure %s", fc_key)

            if should_evolve:
                # Notify plan evolution engine
                if hasattr(self._plan_evolution_engine, "process_verification_failure"):
                    await self._call_evolution_async(
                        self._plan_evolution_engine,
                        failure_type,
                        result,
                    )
                    result.plan_evolution_notified = True
                    log.info(
                        "Phase 8 [Evolve]: Notified PlanEvolutionEngine for %s",
                        fc_key,
                    )
                else:
                    log.debug(
                        "PlanEvolutionEngine has no process_verification_failure "
                        "method — skipping evolution trigger"
                    )

            result.status = RecoveryPipelinePhase.COMPLETED

        except Exception as exc:
            log.warning("Phase 8 [Evolve] failed: %s", exc)
            result.status = RecoveryPipelinePhase.COMPLETED

        return result

    async def _call_evolution_async(
        self,
        engine: Any,
        failure_type: FailureClassification,
        result: RecoveryPipelineResult,
    ) -> None:
        """Safely call the plan evolution engine's verification failure handler."""
        try:
            handler = engine.process_verification_failure
            outcome = handler(
                milestone_id=result.node_id,
                check_name=f"driven_recovery_{failure_type.value}",
                failure_summary=result.error_message or f"Recovery pipeline: {failure_type.value}",
            )
            # Handle both sync and async handlers
            if hasattr(outcome, "__await__"):
                await outcome
        except Exception as exc:
            log.warning("Plan evolution notification failed: %s", exc)

    # ------------------------------------------------------------------
    # Finalization
    # ------------------------------------------------------------------

    def _finalize(
        self,
        result: RecoveryPipelineResult,
    ) -> RecoveryPipelineResult:
        """Finalize the pipeline result and store in history."""
        result.completed_at = time.time()

        # Ensure status is set to a terminal state.
        # Any non-terminal state (in-progress phases, intermediate states)
        # transitions to COMPLETED. Terminal states (FAILED, BLOCKED, ABORTED)
        # are preserved.
        terminal = {
            RecoveryPipelinePhase.COMPLETED,
            RecoveryPipelinePhase.FAILED,
            RecoveryPipelinePhase.BLOCKED,
            RecoveryPipelinePhase.ABORTED,
        }
        if result.status not in terminal:
            result.status = RecoveryPipelinePhase.COMPLETED

        self._pipeline_history.append(result)

        log.info(
            "Recovery pipeline %s: %s (%.0fms, success=%s)",
            result.pipeline_id[:12],
            result.status.value,
            result.duration_ms,
            result.overall_success,
        )

        return result

    # ------------------------------------------------------------------
    # Accessors & Statistics
    # ------------------------------------------------------------------

    @property
    def pipeline_history(self) -> List[RecoveryPipelineResult]:
        """Read-only access to all pipeline runs."""
        return list(self._pipeline_history)

    def get_pipeline_count(self) -> int:
        return len(self._pipeline_history)

    def get_success_rate(self, failure_type: Optional[str] = None) -> float:
        """Get overall or per-type success rate."""
        if failure_type and failure_type in self._success_rates:
            stats = self._success_rates[failure_type]
            if stats["attempts"] == 0:
                return 0.0
            return stats["successes"] / stats["attempts"]

        if not self._pipeline_history:
            return 0.0
        successes = sum(1 for p in self._pipeline_history if p.overall_success)
        return successes / len(self._pipeline_history)

    def get_success_rates_by_type(self) -> Dict[str, Dict[str, Any]]:
        """Get per-failure-type success rate statistics."""
        return {
            fc: {
                "attempts": stats["attempts"],
                "successes": stats["successes"],
                "rate": round(
                    stats["successes"] / max(1, stats["attempts"]), 4
                ),
            }
            for fc, stats in self._success_rates.items()
        }

    def get_recent_pipelines(self, n: int = 10) -> List[RecoveryPipelineResult]:
        """Get the most recent pipeline results."""
        return self._pipeline_history[-n:]

    def snapshot(self) -> Dict[str, Any]:
        """Quick state snapshot for monitoring."""
        return {
            "pipeline": "VerificationDrivenRecoveryPipeline",
            "total_runs": len(self._pipeline_history),
            "overall_success_rate": round(self.get_success_rate(), 4),
            "per_type_rates": self.get_success_rates_by_type(),
            "success_rates_tracked": self._config.track_success_rates,
            "plan_evolution_enabled": self._config.enable_plan_evolution,
        }
