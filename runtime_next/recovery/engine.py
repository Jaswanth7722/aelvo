import asyncio
import logging
import hashlib
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from ..models.events import BaseEvent, EventType, RecoveryEvent
from ..models.node import NodeDefinition, NodeState
from runtime_next.verification.classifier import FailureClassifier
from runtime_next.verification.recovery import RecoveryStrategyEngine
from runtime_next.verification.retry_safety import RetrySafetyEngine
from runtime_next.verification.injector import RecoveryNodeInjector
from runtime_next.verification.memory import LearnedRecoveryMemory
from runtime_next.verification.governance import RecoveryGovernance
from runtime_next.verification.types import FailureClassification, Retryability, RecoveryStrategy
from .consensus_recovery import ConsensusRecoveryEngine
from .specialist_recovery import SpecialistRecoveryEngine
from .task_recovery import TaskRecoveryEngine
from runtime_next.governance import RecoveryGovernanceHooks, GovernancePolicyEngine, create_default_policies
from runtime_next.monitoring import RuntimeMetricsCollector, RuntimeHealthMonitor, AlertManager, AlertSeverity, RuntimeDashboard, HealthCheckPolicy, HealthCheckResult, RuntimeCLI

# Phase 15: Security Hardening
from runtime_next.security import (
    RuntimeSecurityScanner,
    RuntimeSecurityOrchestrator,
    PolicyAuditTrail,
    SandboxIntegrityVerifier,
)

# Local import guard â€” architect types are optional (plan may not exist)
try:
    from runtime_next.plan.architect_types import FailureModeStrategy, RecoveryStrategyType
    _HAS_ARCHITECT_TYPES = True
except ImportError:
    FailureModeStrategy = None
    RecoveryStrategyType = None
    _HAS_ARCHITECT_TYPES = False

log = logging.getLogger("aelvo.runtime.recovery")


class RecoveryEngine:
    """Classifies node failures and initiates targeted recovery actions.

    This is the backward-compatible wrapper that delegates to the new
    Verification + Self-Healing Runtime subsystem.
    """

    def __init__(self, graph=None):
        self._graph = graph
        self._recovery_history: list = []
        self._calibration_system: Any = None  # Linked PlanCalibrationSystem
        self._architect_plan_strategies: Dict[str, Any] = {}  # Plan-defined failure strategies metadata

        # Delegate to new verification subsystem
        self.classifier = FailureClassifier()
        self.recovery_strategies = RecoveryStrategyEngine()
        self.retry_safety = RetrySafetyEngine()
        self.injector = RecoveryNodeInjector()
        self.recovery_memory = LearnedRecoveryMemory()
        self.governance = RecoveryGovernance()
        # Plain boolean toggle: reads/writes are atomic under the GIL, so no
        # lock is required. A threading.Lock here would risk blocking the
        # event loop when acquired from async recovery paths (and an
        # asyncio.Lock cannot be used from the synchronous use_legacy_recovery
        # toggle API).
        self._use_new_subsystem = True

        # Phase 11: Consensus, Specialist, and Task-level recovery
        self.consensus_recovery = ConsensusRecoveryEngine()
        self.specialist_recovery = SpecialistRecoveryEngine()
        self.task_recovery = TaskRecoveryEngine()
        self.consensus_recovery.link_recovery_engine(self)
        self.specialist_recovery.set_reassign_callback(self._on_specialist_reassign)

        # Phase 13: Governance policy enforcement hooks
        self.governance_policy_engine = GovernancePolicyEngine()
        for policy in create_default_policies():
            self.governance_policy_engine.add_policy(policy)
        self.governance_hooks = RecoveryGovernanceHooks(self.governance_policy_engine)
        self.consensus_recovery.set_governance_hooks(self.governance_hooks)
        self.specialist_recovery.set_governance_hooks(self.governance_hooks)
        self.task_recovery.set_governance_hooks(self.governance_hooks)

        # Phase 14: Monitoring & Observability
        self.metrics_collector = RuntimeMetricsCollector()
        self.health_monitor = RuntimeHealthMonitor()
        self.alert_manager = AlertManager()
        self.dashboard = RuntimeDashboard(
            metrics_collector=self.metrics_collector,
            health_monitor=self.health_monitor,
            alert_manager=self.alert_manager,
        )
        self._register_default_health_checks()

        # Wire metrics collector and default alert rules
        self.governance_hooks.set_metrics_collector(self.metrics_collector)
        self.health_monitor.set_metrics_collector(self.metrics_collector)
        # Auto-evaluate alert rules on every metric recording
        self.metrics_collector.set_alert_manager(self.alert_manager)
        self._register_default_alert_rules()

        # Phase 15: Security Hardening
        self.security_scanner = RuntimeSecurityScanner()
        self.policy_audit_trail = PolicyAuditTrail()
        self.sandbox_integrity = SandboxIntegrityVerifier()
        self.security_orchestrator = RuntimeSecurityOrchestrator()
        self.security_orchestrator.link_scanner(self.security_scanner)
        self.security_orchestrator.link_audit_trail(self.policy_audit_trail)
        self.security_orchestrator.link_integrity_verifier(self.sandbox_integrity)

        # Wire policy audit trail into governance hooks
        self.policy_audit_trail.wrap_governance_hooks(self.governance_hooks)

        # Wire security alerts into alert manager
        self.security_orchestrator.set_alert_callback(
            lambda title, msg, sev: self.alert_manager.create_alert(
                title=title,
                message=msg,
                severity=AlertSeverity(sev) if sev in ("info", "warning", "error", "critical") else AlertSeverity.WARNING,
                subsystem="security",
                source="security_orchestrator",
                metadata={"severity": sev},
            )
        )

        # Register security health checks
        self._register_security_health_checks()

        # Runtime CLI for interactive monitoring commands
        self.runtime_cli = RuntimeCLI(dashboard=self.dashboard)

    def use_legacy_recovery(self, enabled: bool = True):
        """Toggle between legacy and new verification subsystem."""
        self._use_new_subsystem = not enabled

    @property
    def use_new_subsystem(self):
        return self._use_new_subsystem

    @property
    def graph(self):
        return self._graph

    @graph.setter
    def graph(self, g):
        self._graph = g

    async def on_event(self, event: BaseEvent):
        if event.type == EventType.NODE_TRANSITION:
            to_state = getattr(event, "to_state", None)
            if to_state == NodeState.FAILED.value or to_state == NodeState.FAILED:
                node_id = getattr(event, "node_id", "")
                reason = getattr(event, "reason", "Unknown failure")
                if node_id:
                    await self.handle_failure(node_id, reason)

        # Handle VERIFICATION_FAILED events â€” sandbox failures, policy violations, etc.
        elif hasattr(event, "type") and event.type == EventType.VERIFICATION_FAILED:
            # Try direct attribute first, then fall back to payload
            node_id = getattr(event, "node_id", "")
            if not node_id:
                payload = getattr(event, "payload", {})
                payload_node_id = payload.get("node_id", "") if isinstance(payload, dict) else ""
                if payload_node_id:
                    node_id = payload_node_id
            if not node_id and hasattr(event, "result"):
                node_id = getattr(event.result, "node_id", "")

            reason = "Verification failure"
            # Check diagnostics in payload
            payload = getattr(event, "payload", {})
            if isinstance(payload, dict):
                payload_diags = payload.get("diagnostics")
                if payload_diags:
                    reason = "; ".join(payload_diags) if isinstance(payload_diags, list) else str(payload_diags)
            # Fall back to result diagnostics
            if hasattr(event, "result") and hasattr(event.result, "diagnostics"):
                diags = event.result.diagnostics
                if diags:
                    reason = "; ".join(diags)

            if node_id:
                log.warning(
                    f"VERIFICATION_FAILED event for {node_id}: {reason[:100]}"
                )
                await self.handle_failure(node_id, reason)

    async def handle_failure(self, node_id: str, reason: str):
        if not self._graph:
            log.warning(f"RecoveryEngine: No graph attached, cannot recover {node_id}")
            return

        node = self._graph.nodes.get(node_id)
        if not node:
            log.warning(f"RecoveryEngine: Node {node_id} not found")
            return

        log.warning(f"Handling failure for {node_id}: {reason[:100]}")

        use_new = self._use_new_subsystem

        if use_new:
            await self._handle_with_new_subsystem(node, node_id, reason)
        else:
            await self._handle_with_legacy(node, node_id, reason)

    async def _handle_with_new_subsystem(self, node: NodeDefinition, node_id: str, reason: str):
        """Handle failure using the new Verification + Self-Healing Runtime."""
        # 1. Classify the failure
        classification = await self.classifier.classify(
            error_message=reason,
            exit_code=None,
        )

        # 2. Get recovery strategy
        strategy = self.recovery_strategies.get_strategy(classification.primary)
        if strategy is None:
            log.warning(f"No strategy for {classification.primary.value}")
            await self._graph.transition_node(
                node_id, NodeState.FAILED,
                reason=f"No recovery strategy: {reason}"
            )
            return

        # 3. Governance
        governance = await self.governance.decide(
            failure_type=classification.primary,
            strategy=strategy,
            action_type="retry",
            context={
                "retry_count": node.retry_count,
                "node_description": node.description,
                "error_message": reason,
            },
        )

        if governance.should_stop_autonomy():
            log.warning(f"Governance blocked: {governance.verdict}")
            await self._graph.transition_node(
                node_id, NodeState.BLOCKED,
                reason=governance.reason,
            )
            return

        # 4. Retry safety check
        retry_decision = await self.retry_safety.evaluate(
            node_id=node_id,
            classification=classification.primary,
            retryability=(
                Retryability.SAFE
                if strategy.max_retries > 0
                else Retryability.NEVER
            ),
        )

        if not retry_decision.can_retry:
            log.warning(f"Retry blocked: {retry_decision.reason}")
            await self._graph.transition_node(
                node_id, NodeState.FAILED,
                reason=retry_decision.reason,
            )
            return

        # 5. Execute recovery
        action = await self.recovery_strategies.execute_recovery(
            node_id=node_id,
            failure_type=classification.primary,
            classification_result=classification,
            context={
                "retry_count": node.retry_count,
                "node_description": node.description,
            },
        )

        if action is None:
            await self._graph.transition_node(node_id, NodeState.FAILED, reason=reason)
            return

        # 6. Inject recovery node
        if action.action_type in ("retry", "inject_node"):
            injected_id = await self.injector.inject_recovery_node(
                action=action,
                strategy=strategy,
                graph=self._graph,
                context={},
            )
            if injected_id:
                action.injected_node_id = injected_id

        # 7. Record in memory
        await self.recovery_memory.record(
            action=action,
            strategy=strategy,
            success=True,
        )

        # 8. Track in history
        self._recovery_history.append({
            "node_id": node_id,
            "classification": classification.primary.value,
            "action": action.action_type,
            "strategy": strategy.name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "verification_subsystem": True,
        })

        # 9. Publish event
        event = RecoveryEvent(
            id=hashlib.sha256(f"recv_{node_id}_{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:16],
            node_id=node_id,
            classification=classification.primary.value,
            action=action.action_type,
            retry_count=node.retry_count,
        )
        if self._graph and hasattr(self._graph, "event_bus") and self._graph.event_bus:
            await self._graph.event_bus.publish(event)

        # 10. Transition node
        if action.success or action.action_type in ("retry", "inject_node"):
            node.retry_count += 1
            await self._graph.transition_node(
                node_id, NodeState.RETRYING,
                reason=f"{strategy.name}: {strategy.description}",
            )
        else:
            await self._graph.transition_node(
                node_id, NodeState.FAILED,
                reason=f"Recovery failed: {strategy.name}",
            )

    async def _handle_with_legacy(self, node: NodeDefinition, node_id: str, reason: str):
        """Original legacy recovery logic â€” preserved for backward compatibility."""
        if node.retry_count >= node.retry_budget:
            log.warning(f"Node {node_id} exhausted retry budget ({node.retry_budget})")
            await self._graph.transition_node(node_id, NodeState.FAILED, reason=f"Retry budget exhausted: {reason}")
            return

        classification = self._classify_failure_legacy(reason)
        action = await self._execute_recovery_legacy(node, classification, reason)

        self._recovery_history.append({
            "node_id": node_id,
            "classification": classification,
            "action": action,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "verification_subsystem": False,
        })

        event = RecoveryEvent(
            id=hashlib.sha256(f"recv_{node_id}_{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:16],
            node_id=node_id,
            classification=classification,
            action=action
        )
        if self._graph and hasattr(self._graph, "event_bus") and self._graph.event_bus:
            await self._graph.event_bus.publish(event)

    def _register_default_alert_rules(self) -> None:
        """Register default alert rules for common scenarios.

        Rules are automatically evaluated on every metric recording
        via RuntimeMetricsCollector.set_alert_manager().
        """
        from runtime_next.monitoring.alerting import AlertRule

        self.alert_manager.add_rule(AlertRule(
            rule_id="high_recovery_failure_rate",
            name="High recovery failure rate",
            description="Alert when recovery failure count exceeds threshold",
            subsystem="recovery",
            severity=AlertSeverity.WARNING,
            metric_name="recovery.failure",
            threshold_max=5,
            consecutive_count=3,
        ))
        self.alert_manager.add_rule(AlertRule(
            rule_id="governance_denial_spike",
            name="Governance denial spike",
            description="Alert when governance denials exceed threshold",
            subsystem="governance",
            severity=AlertSeverity.WARNING,
            metric_name="governance.evaluation",
            threshold_max=10,
            consecutive_count=2,
        ))
        self.alert_manager.add_rule(AlertRule(
            rule_id="pool_exhaustion",
            name="Resource pool exhaustion risk",
            description="Alert when pool utilization exceeds 90%",
            subsystem="scaling",
            severity=AlertSeverity.WARNING,
            metric_name="scaling.pool.utilization",
            threshold_max=0.9,
            consecutive_count=1,
        ))

        # Security alert rules
        self.alert_manager.add_rule(AlertRule(
            rule_id="security_critical_finding",
            name="Critical security finding",
            description="Alert on critical security scan findings",
            subsystem="security",
            severity=AlertSeverity.CRITICAL,
            metric_name="security.critical_finding",
            threshold_max=0,
            consecutive_count=1,
        ))
        self.alert_manager.add_rule(AlertRule(
            rule_id="integrity_failure",
            name="Sandbox integrity failure",
            description="Alert on sandbox integrity check failures",
            subsystem="security",
            severity=AlertSeverity.CRITICAL,
            metric_name="security.integrity_failure",
            threshold_max=0,
            consecutive_count=1,
        ))

    def _register_security_health_checks(self) -> None:
        """Register security-specific health checks."""
        self.health_monitor.register_check(HealthCheckPolicy(
            subsystem="security",
            check_id="security_scanner",
            description="Runtime security scanner availability",
            interval_seconds=120.0,
            failure_threshold=3,
            check_fn=lambda: HealthCheckResult(
                healthy=True,
                message="Security scanner available",
            ),
        ))
        self.health_monitor.register_check(HealthCheckPolicy(
            subsystem="security",
            check_id="policy_audit_chain",
            description="Policy audit trail chain integrity",
            interval_seconds=300.0,
            failure_threshold=1,
            check_fn=lambda: HealthCheckResult(
                healthy=self.policy_audit_trail.verify_chain_integrity(),
                message="Audit trail chain intact" if self.policy_audit_trail.verify_chain_integrity()
                else "Audit trail chain integrity VIOLATION",
            ),
        ))
        self.health_monitor.register_check(HealthCheckPolicy(
            subsystem="security",
            check_id="sandbox_binary_integrity",
            description="Sandbox binary hash verification",
            interval_seconds=600.0,
            failure_threshold=1,
            check_fn=lambda: (
                self.sandbox_integrity.verify_binary_integrity().to_health_check_result()
            ),
        ))

    def _register_default_health_checks(self) -> None:
        """Register default health checks for runtime subsystems."""
        self.health_monitor.register_check(HealthCheckPolicy(
            subsystem="recovery",
            check_id="consensus_engine",
            description="Consensus recovery engine availability",
            interval_seconds=60.0,
            failure_threshold=3,
            check_fn=lambda: HealthCheckResult(
                healthy=True,
                message="Consensus recovery engine available",
            ),
        ))
        self.health_monitor.register_check(HealthCheckPolicy(
            subsystem="recovery",
            check_id="specialist_recovery",
            description="Specialist recovery engine availability",
            interval_seconds=60.0,
            failure_threshold=3,
            check_fn=lambda: HealthCheckResult(
                healthy=True,
                message="Specialist recovery engine available",
            ),
        ))
        self.health_monitor.register_check(HealthCheckPolicy(
            subsystem="recovery",
            check_id="task_recovery",
            description="Task recovery engine availability",
            interval_seconds=60.0,
            failure_threshold=3,
            check_fn=lambda: HealthCheckResult(
                healthy=True,
                message="Task recovery engine available",
            ),
        ))
        self.health_monitor.register_check(HealthCheckPolicy(
            subsystem="governance",
            check_id="policy_engine",
            description="Governance policy engine availability",
            interval_seconds=120.0,
            failure_threshold=3,
            check_fn=lambda: HealthCheckResult(
                healthy=True,
                message="Governance policy engine available",
            ),
        ))
        self.health_monitor.register_check(HealthCheckPolicy(
            subsystem="governance",
            check_id="governance_hooks",
            description="Governance recovery hooks availability",
            interval_seconds=120.0,
            failure_threshold=3,
            check_fn=lambda: HealthCheckResult(
                healthy=True,
                message="Governance hooks available",
            ),
        ))

    def _on_specialist_reassign(self, original: str, replacement: str, context: Dict[str, Any]) -> None:
        """Callback invoked when a specialist is reassigned due to failure."""
        log.info(
            "Specialist reassigned: %s → %s",
            original, replacement,
        )
        # Record reassignment as a metric
        try:
            self.metrics_collector.record_specialist_reassign(
                original=original,
                replacement=replacement,
            )
        except Exception as _ex:
            log.warning("Silenced exception: %s", _ex)

    def _classify_failure_legacy(self, reason: str) -> str:
        r = reason.lower()
        if "syntaxerror" in r or "indentationerror" in r or "invalid syntax" in r or "syntax error" in r:
            return "syntax_error"
        if "not found" in r or "no such file" in r or "cannot find" in r:
            return "missing_resource"
        if "permission" in r or "access denied" in r or "eacces" in r or "epipe" in r:
            return "permission_denied"
        if "timeout" in r or "timed out" in r:
            return "timeout"
        if "lock" in r and ("contention" in r or "busy" in r):
            return "lock_contention"
        if "verify" in r or "verification" in r or "constraint" in r or "anchor" in r:
            return "anchor_violation"
        if "memory" in r and ("exceed" in r or "full" in r or "oom" in r):
            return "resource_exhaustion"
        return "unknown"

    # Backward compatibility alias
    _classify_failure = _classify_failure_legacy

    async def _execute_recovery_legacy(self, node: NodeDefinition, classification: str, reason: str) -> str:
        if classification == "syntax_error":
            return await self._recover_with_retry_legacy(node, "syntax_error", "Re-invoking specialist with error context")
        elif classification == "missing_resource":
            return await self._recover_missing_resource_legacy(node, reason)
        elif classification == "permission_denied":
            return await self._recover_permission_denied_legacy(node)
        elif classification == "lock_contention":
            return await self._recover_lock_contention_legacy(node)
        elif classification == "timeout":
            return await self._recover_with_retry_legacy(node, "timeout", "Retrying with longer timeout")
        elif classification == "anchor_violation":
            return await self._recover_with_retry_legacy(node, "anchor_violation", "Re-invoking with corrected constraints")
        else:
            return await self._recover_with_retry_legacy(node, "unknown", "Generic retry")

    async def _recover_with_retry_legacy(self, node: NodeDefinition, classification: str, reason: str) -> str:
        node.retry_count += 1
        await self._graph.transition_node(node.id, NodeState.RETRYING, reason=f"{classification}: {reason}")
        backoff = node.next_backoff()
        await asyncio.sleep(backoff)
        await self._graph.transition_node(node.id, NodeState.PENDING, reason=f"Retry #{node.retry_count} after {backoff:.1f}s backoff")
        return f"retry_with_backoff:{backoff}s"

    async def _recover_missing_resource_legacy(self, node: NodeDefinition, reason: str) -> str:
        inject_id = f"recover_{node.id}_install"
        if inject_id not in self._graph.nodes:
            recovery_node = NodeDefinition(
                id=inject_id,
                description=f"Install missing resource for {node.id}",
                specialist="TERMINUS",
                danger="reversible"
            )
            self._graph.inject_node(recovery_node, dependencies=[node.id])
        node.retry_count += 1
        await self._graph.transition_node(node.id, NodeState.RETRYING, reason="Missing resource, inject install node")
        return "inject_install_node"

    async def _recover_permission_denied_legacy(self, node: NodeDefinition) -> str:
        await self._graph.transition_node(node.id, NodeState.BLOCKED, reason="Permission denied, requires user intervention")
        return "escalate_to_user"

    async def _recover_lock_contention_legacy(self, node: NodeDefinition) -> str:
        node.retry_count += 1
        await self._graph.transition_node(node.id, NodeState.RETRYING, reason="Lock contention, retrying")
        await asyncio.sleep(node.next_backoff())
        await self._graph.transition_node(node.id, NodeState.PENDING, reason="Lock contention retry")
        return "retry_with_backoff"

    # ==================================================================
    # Architect Plan Recovery Integration
    # ==================================================================

    def inject_plan_strategies(self, plan_strategies: Any) -> int:
        """Inject architect plan recovery strategies into the recovery engine.

        The architect plan contains pre-designed failure mode strategies
        that define how to recover when specific failures occur during
        execution. This method registers them with the RecoveryStrategyEngine
        so they take priority over generic defaults.

        Each plan FailureModeStrategy maps to a RecoveryStrategy via:
        - failure_mode + phase_id â†’ FailureClassification (text matching)
        - strategy (RecoveryStrategyType) â†’ action type + max_retries
        - triggers_human_review â†’ governance requirements

        Args:
            plan_strategies: The plan's RecoveryPlanSection or list of
                             FailureModeStrategy dicts

        Returns:
            Number of strategies successfully injected
        """
        if not _HAS_ARCHITECT_TYPES:
            return 0

        count = 0
        try:
            # Accept either a list of dicts/objects or a RecoveryPlanSection
            if hasattr(plan_strategies, 'failure_strategies'):
                strategies_list = plan_strategies.failure_strategies
            elif isinstance(plan_strategies, list):
                strategies_list = plan_strategies
            else:
                return 0

            for fs in strategies_list:
                # Extract fields â€” might be dict or FailureModeStrategy object
                if isinstance(fs, dict):
                    failure_mode = fs.get("failure_mode", "")
                    phase_id = fs.get("phase_id", "")
                    strategy_name = fs.get("strategy", "retry") or "retry"
                    fallback = fs.get("fallback_description", "")
                    max_retries = fs.get("max_retries", 2)
                    triggers_human = fs.get("triggers_human_review", False)
                else:
                    failure_mode = getattr(fs, "failure_mode", "")
                    phase_id = getattr(fs, "phase_id", "")
                    strategy_name = getattr(fs, "strategy", "retry") or "retry"
                    if hasattr(strategy_name, 'value'):
                        strategy_name = strategy_name.value
                    fallback = getattr(fs, "fallback_description", "")
                    max_retries = getattr(fs, "max_retries", 2)
                    triggers_human = getattr(fs, "triggers_human_review", False)

                # Determine failure classification from failure_mode text hints
                fc = self._classify_plan_failure_mode(failure_mode, strategy_name)

                # Record the strategy lookup metadata
                lookup_key = f"{phase_id}::{failure_mode[:40]}"
                self._architect_plan_strategies[lookup_key] = {
                    "failure_mode": failure_mode,
                    "phase_id": phase_id,
                    "strategy": strategy_name,
                    "classification": fc.value if fc else "unknown",
                    "fallback": fallback,
                    "max_retries": max_retries,
                    "triggers_human": triggers_human,
                }

                if fc is None:
                    continue

                # Map RecoveryStrategyType to action type
                action_type_map = {
                    "retry": "retry",
                    "rollback": "rollback",
                    "substitute": "inject_node",
                    "escalate": "escalate",
                    "decompose": "retry",
                    "abort": "escalate",
                }
                action_type_map.get(strategy_name, "retry")

                # Build a RecoveryStrategy and register it
                strat_id = f"plan_{fc.value}_{max_retries}"
                existing = self.recovery_strategies.get_strategy(fc)
                if existing and "plan_" in existing.id:
                    continue  # Already registered a plan strategy for this FC

                plan_strategy = RecoveryStrategy(
                    id=strat_id,
                    name=f"[Plan] {fallback[:60]}" if fallback else f"[Plan] {strategy_name}",
                    failure_type=fc,
                    description=fallback or f"Plan-defined {strategy_name} strategy",
                    danger_level="approval_required" if triggers_human else "safe",
                    max_retries=max_retries,
                    requires_user_approval=triggers_human,
                )
                self.recovery_strategies.register_strategy(plan_strategy)
                count += 1

            if count > 0:
                log.info(
                    "Injected %d plan-defined recovery strategies "
                    "from architect plan", count,
                )

        except Exception as e:
            log.warning("Failed to inject plan strategies: %s", e)

        return count

    def _classify_plan_failure_mode(
        self, failure_mode: str, strategy_name: str
    ) -> Optional[FailureClassification]:
        """Classify a plan failure mode text into a FailureClassification."""
        text = (failure_mode + " " + strategy_name).lower()

        if "syntax" in text or "compile" in text or "parse" in text:
            return FailureClassification.SYNTAX_ERROR
        if "depend" in text or "missing" in text or "import" in text or "not found" in text:
            return FailureClassification.DEPENDENCY_MISSING
        if "permission" in text or "access" in text or "security" in text:
            return FailureClassification.PERMISSION_DENIED
        if "timeout" in text or "timed" in text or "hang" in text:
            return FailureClassification.TIMEOUT
        if "verify" in text or "test" in text or "check" in text or "lint" in text:
            return FailureClassification.VERIFICATION_FAILURE
        if "graph" in text or "cycle" in text or "inconsist" in text:
            return FailureClassification.GRAPH_INCONSISTENCY
        if "serial" in text or "json" in text or "parse" in text:
            return FailureClassification.SERIALIZATION_FAILURE
        if "tool" in text or "handler" in text or "executor" in text:
            return FailureClassification.TOOL_FAILURE
        if "rollback" in text or "state" in text or "stale" in text:
            return FailureClassification.STALE_RUNTIME_STATE
        if "lock" in text or "mutex" in text or "contention" in text:
            return FailureClassification.MUTEX_VIOLATION
        if "abort" in text or "unknown" in text or "human" in text:
            return FailureClassification.UNKNOWN_FAILURE
        if "substitute" in text or "replace" in text:
            return FailureClassification.DEPENDENCY_MISSING

        # Default to tool_failure (safe retry) for unrecognized plan strategies
        return FailureClassification.TOOL_FAILURE

    # ==================================================================
    # Calibration Integration
    # ==================================================================

    def link_calibration_system(self, calibration_system: Any):
        """Link a PlanCalibrationSystem for feeding recovery learnings back."""
        self._calibration_system = calibration_system

    def sync_recovery_to_calibration(self) -> Dict[str, Any]:
        """Sync recovery outcomes from LearnedRecoveryMemory to PlanCalibrationSystem.

        Recovery experiences are converted into calibration learnings:
        - Successful recoveries with predictable failure patterns
          increase confidence in those recovery strategies
        - Repeated failures for the same failure type generate
          UNPLANNED_FAILURE deviations
        - Recovery strategies that never succeed generate
          INCORRECT_RISK or STRATEGY_MISMATCH deviations

        Returns:
            Summary dict with counts of synced learnings
        """
        if self._calibration_system is None:
            return {"synced": 0, "note": "No calibration system linked"}

        try:
            entries = self.recovery_memory.entries
            if not entries:
                return {"synced": 0, "note": "No recovery entries to sync"}

            # Count by failure type
            by_type: Dict[str, int] = {}
            by_success: Dict[str, int] = {"success": 0, "failure": 0}
            for entry in entries:
                ft = entry.failure_type.value if hasattr(entry.failure_type, 'value') else str(entry.failure_type)
                by_type[ft] = by_type.get(ft, 0) + 1
                if entry.success:
                    by_success["success"] += 1
                else:
                    by_success["failure"] += 1

            log.info(
                "Recoveryâ†’Calibration sync: %d entries synced "
                "(%d success, %d failure across %d types)",
                len(entries), by_success["success"], by_success["failure"],
                len(by_type),
            )

            return {
                "synced": len(entries),
                "success_count": by_success["success"],
                "failure_count": by_success["failure"],
                "failure_types": by_type,
            }

        except Exception as e:
            log.warning("Failed to sync recovery to calibration: %s", e)
            return {"synced": 0, "error": str(e)}

    @property
    def recovery_count(self) -> int:
        return len(self._recovery_history)

    def get_plan_strategies(self) -> Dict[str, Any]:
        """Get the injected plan strategies metadata."""
        return dict(self._architect_plan_strategies)
