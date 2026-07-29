# runtime_next/governance/policy_engine.py
# Phase 13: Governance policy engine for recovery action enforcement.
#
# Defines governance policies that control which recovery actions are
# allowed, denied, or require approval at each recovery level.
#
# Policies are evaluated before recovery actions execute (pre-hook)
# and can be used to enforce organizational rules, security constraints,
# and operational boundaries.

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set
from enum import Enum
from dataclasses import dataclass, field

log = logging.getLogger("aelvo.runtime.governance.policy")


class PolicyEffect(str, Enum):
    """Effect of a policy rule on a recovery action."""
    ALLOW = "allow"
    """Recovery action is permitted to proceed."""
    DENY = "deny"
    """Recovery action is blocked."""
    REQUIRE_APPROVAL = "require_approval"
    """Recovery action requires explicit approval before proceeding."""
    LOG_ONLY = "log_only"
    """Recovery action is allowed but must be logged for audit."""


class PolicyScope(str, Enum):
    """Which recovery level a policy applies to."""
    ALL = "all"
    CONSENSUS = "consensus"
    SPECIALIST = "specialist"
    TASK = "task"


class PolicySeverity(str, Enum):
    """Severity of policy violation or action."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class PolicyRule:
    """A single governance policy rule.

    Each rule defines:
    - What recovery level it applies to (scope)
    - Which specific action types it governs
    - Conditions under which it applies (via matcher function or context filter)
    - What effect it has (allow, deny, require_approval, log_only)
    """

    policy_id: str
    name: str
    description: str
    effect: PolicyEffect
    scope: PolicyScope = PolicyScope.ALL
    action_types: List[str] = field(default_factory=list)
    """If empty, applies to all action types within scope."""
    specialists: List[str] = field(default_factory=list)
    """If empty, applies to all specialists."""
    failure_types: List[str] = field(default_factory=list)
    """If empty, applies to all failure types."""
    consensus_types: List[str] = field(default_factory=list)
    """If empty, applies to all consensus failure types."""
    task_triggers: List[str] = field(default_factory=list)
    """If empty, applies to all task recovery triggers."""
    priority: int = 0
    """Higher priority rules are evaluated first."""
    enabled: bool = True
    reason_template: str = ""
    """Template for the reason message (can include {action}, {specialist}, etc.)."""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def matches(
        self,
        scope: PolicyScope,
        action_type: str,
        specialist: Optional[str] = None,
        failure_type: Optional[str] = None,
        consensus_type: Optional[str] = None,
        task_trigger: Optional[str] = None,
    ) -> bool:
        """Check if this rule matches the given context."""
        if not self.enabled:
            return False
        if self.scope != PolicyScope.ALL and self.scope != scope:
            return False
        if self.action_types and action_type not in self.action_types:
            return False
        if self.specialists and specialist and specialist.upper() not in [s.upper() for s in self.specialists]:
            return False
        if self.failure_types and failure_type and failure_type not in self.failure_types:
            return False
        if self.consensus_types and consensus_type and consensus_type not in self.consensus_types:
            return False
        if self.task_triggers and task_trigger and task_trigger not in self.task_triggers:
            return False
        return True

    def format_reason(self, **kwargs: Any) -> str:
        """Format the reason template with context variables."""
        if self.reason_template:
            try:
                return self.reason_template.format(**kwargs)
            except KeyError:
                return self.reason_template
        return f"Policy '{self.name}': {self.effect.value}"


@dataclass
class PolicyResult:
    """Result of evaluating a policy rule against a recovery action."""

    policy_id: str
    policy_name: str
    effect: PolicyEffect
    reason: str
    severity: PolicySeverity = PolicySeverity.WARNING
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PolicyEvaluation:
    """Complete evaluation result across all matching rules.

    The most restrictive matching rule wins (DENY > REQUIRE_APPROVAL > LOG_ONLY > ALLOW).
    """

    overall_effect: PolicyEffect
    reason: str
    matching_rules: List[PolicyResult] = field(default_factory=list)
    evaluated_count: int = 0

    @property
    def is_allowed(self) -> bool:
        return self.overall_effect in (PolicyEffect.ALLOW, PolicyEffect.LOG_ONLY)

    @property
    def requires_approval(self) -> bool:
        return self.overall_effect == PolicyEffect.REQUIRE_APPROVAL

    @property
    def is_denied(self) -> bool:
        return self.overall_effect == PolicyEffect.DENY


class GovernancePolicyEngine:
    """Manages and evaluates governance policies for recovery actions.

    Features:
    - Rule registry with priority ordering
    - Multi-level scope matching (consensus, specialist, task, all)
    - Most-restrictive-wins evaluation
    - Policy enable/disable lifecycle
    - Audit logging of policy evaluations
    """

    def __init__(self):
        self._rules: Dict[str, PolicyRule] = {}
        self._evaluation_history: List[PolicyEvaluation] = []
        self._approval_pending: Dict[str, Dict[str, Any]] = {}

    # ── Policy Management ────────────────────────────────────────────────

    def add_policy(self, rule: PolicyRule) -> None:
        """Register a policy rule."""
        self._rules[rule.policy_id] = rule
        log.info(
            "Policy added: '%s' (%s) — effect=%s, scope=%s",
            rule.name, rule.policy_id[:8], rule.effect.value, rule.scope.value,
        )

    def remove_policy(self, policy_id: str) -> bool:
        """Remove a policy rule by ID."""
        if policy_id in self._rules:
            del self._rules[policy_id]
            log.info("Policy removed: %s", policy_id[:8])
            return True
        return False

    def enable_policy(self, policy_id: str) -> bool:
        """Enable a policy rule."""
        if policy_id in self._rules:
            self._rules[policy_id].enabled = True
            return True
        return False

    def disable_policy(self, policy_id: str) -> bool:
        """Disable a policy rule (without removing it)."""
        if policy_id in self._rules:
            self._rules[policy_id].enabled = False
            return True
        return False

    def get_policy(self, policy_id: str) -> Optional[PolicyRule]:
        """Get a policy rule by ID."""
        return self._rules.get(policy_id)

    def get_policies(self, scope: Optional[PolicyScope] = None) -> List[PolicyRule]:
        """Get all policy rules, optionally filtered by scope."""
        rules = list(self._rules.values())
        if scope:
            rules = [r for r in rules if r.scope in (PolicyScope.ALL, scope)]
        return sorted(rules, key=lambda r: r.priority, reverse=True)

    # ── Evaluation ──────────────────────────────────────────────────────

    def evaluate(
        self,
        scope: PolicyScope,
        action_type: str,
        specialist: Optional[str] = None,
        failure_type: Optional[str] = None,
        consensus_type: Optional[str] = None,
        task_trigger: Optional[str] = None,
    ) -> PolicyEvaluation:
        """Evaluate all matching policies against a recovery action.

        Returns a PolicyEvaluation with the most restrictive matching effect.
        Priority order: DENY > REQUIRE_APPROVAL > LOG_ONLY > ALLOW.
        """
        matching_results: List[PolicyResult] = []
        evaluated_count = 0

        for rule in sorted(self._rules.values(), key=lambda r: r.priority, reverse=True):
            if not rule.enabled:
                continue
            evaluated_count += 1

            if rule.matches(
                scope=scope,
                action_type=action_type,
                specialist=specialist,
                failure_type=failure_type,
                consensus_type=consensus_type,
                task_trigger=task_trigger,
            ):
                result = PolicyResult(
                    policy_id=rule.policy_id,
                    policy_name=rule.name,
                    effect=rule.effect,
                    reason=rule.format_reason(
                        action=action_type,
                        specialist=specialist or "unknown",
                        failure_type=failure_type or "unknown",
                        scope=scope.value,
                    ),
                    severity=self._effect_to_severity(rule.effect),
                )
                matching_results.append(result)

        # Most restrictive wins
        overall = self._resolve_conflict(matching_results)
        evaluation = PolicyEvaluation(
            overall_effect=overall,
            reason=matching_results[0].reason if matching_results else "No matching policies",
            matching_rules=matching_results,
            evaluated_count=evaluated_count,
        )

        self._evaluation_history.append(evaluation)
        return evaluation

    def evaluate_consensus_action(
        self,
        action_type: str,
        consensus_type: str,
        specialist: Optional[str] = None,
    ) -> PolicyEvaluation:
        """Evaluate a consensus recovery action against policies."""
        return self.evaluate(
            scope=PolicyScope.CONSENSUS,
            action_type=action_type,
            specialist=specialist,
            consensus_type=consensus_type,
        )

    def evaluate_specialist_action(
        self,
        action_type: str,
        specialist: str,
        failure_type: Optional[str] = None,
    ) -> PolicyEvaluation:
        """Evaluate a specialist recovery action against policies."""
        return self.evaluate(
            scope=PolicyScope.SPECIALIST,
            action_type=action_type,
            specialist=specialist,
            failure_type=failure_type,
        )

    def evaluate_task_action(
        self,
        action_type: str,
        task_trigger: str,
        specialist: Optional[str] = None,
        failure_type: Optional[str] = None,
    ) -> PolicyEvaluation:
        """Evaluate a task recovery action against policies."""
        return self.evaluate(
            scope=PolicyScope.TASK,
            action_type=action_type,
            specialist=specialist,
            failure_type=failure_type,
            task_trigger=task_trigger,
        )

    # ── Approval Management ─────────────────────────────────────────────

    def request_approval(self, evaluation: PolicyEvaluation, context: Dict[str, Any]) -> str:
        """Request approval for a policy-required approval.

        Returns a token that can be used to approve/reject.
        """
        token = hashlib.sha256(
            f"gov_approval_{time.time()}_{evaluation.overall_effect.value}".encode()
        ).hexdigest()[:16]
        self._approval_pending[token] = {
            "evaluation": evaluation,
            "context": context,
            "created_at": time.time(),
            "approved": None,
        }
        log.info("Approval requested: token=%s, policy=%s", token[:8], evaluation.reason[:50])
        return token

    def approve(self, token: str) -> bool:
        """Approve a pending request."""
        if token in self._approval_pending and self._approval_pending[token]["approved"] is None:
            self._approval_pending[token]["approved"] = True
            log.info("Approval granted: token=%s", token[:8])
            return True
        return False

    def reject(self, token: str) -> bool:
        """Reject a pending request."""
        if token in self._approval_pending and self._approval_pending[token]["approved"] is None:
            self._approval_pending[token]["approved"] = False
            log.info("Approval rejected: token=%s", token[:8])
            return True
        return False

    # ── History / Stats ─────────────────────────────────────────────────

    def get_evaluation_history(
        self, limit: int = 50
    ) -> List[PolicyEvaluation]:
        """Get recent policy evaluations."""
        return self._evaluation_history[-limit:]

    def get_pending_approvals(self) -> Dict[str, Dict[str, Any]]:
        """Get all pending approvals."""
        return {
            k: v for k, v in self._approval_pending.items()
            if v["approved"] is None
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get policy engine statistics."""
        total_evaluations = len(self._evaluation_history)
        denied = sum(
            1 for e in self._evaluation_history if e.overall_effect == PolicyEffect.DENY
        )
        approved = sum(
            1 for e in self._evaluation_history if e.overall_effect == PolicyEffect.REQUIRE_APPROVAL
        )
        allowed = sum(
            1 for e in self._evaluation_history if e.overall_effect == PolicyEffect.ALLOW
        )

        return {
            "total_policies": len(self._rules),
            "enabled_policies": sum(1 for r in self._rules.values() if r.enabled),
            "total_evaluations": total_evaluations,
            "denied_count": denied,
            "approval_required_count": approved,
            "allowed_count": allowed,
            "pending_approvals": len(self.get_pending_approvals()),
        }

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _effect_to_severity(effect: PolicyEffect) -> PolicySeverity:
        mapping = {
            PolicyEffect.ALLOW: PolicySeverity.INFO,
            PolicyEffect.LOG_ONLY: PolicySeverity.INFO,
            PolicyEffect.REQUIRE_APPROVAL: PolicySeverity.WARNING,
            PolicyEffect.DENY: PolicySeverity.ERROR,
        }
        return mapping.get(effect, PolicySeverity.WARNING)

    @staticmethod
    def _resolve_conflict(results: List[PolicyResult]) -> PolicyEffect:
        """Most restrictive effect wins: DENY > REQUIRE_APPROVAL > LOG_ONLY > ALLOW."""
        if not results:
            return PolicyEffect.ALLOW
        for effect in (PolicyEffect.DENY, PolicyEffect.REQUIRE_APPROVAL, PolicyEffect.LOG_ONLY):
            if any(r.effect == effect for r in results):
                return effect
        return PolicyEffect.ALLOW


# ── Default Policies ──────────────────────────────────────────────────────

def create_default_policies() -> List[PolicyRule]:
    """Create a set of sensible default governance policies."""
    return [
        PolicyRule(
            policy_id="gov_deny_destructive_consensus",
            name="Deny destructive consensus actions",
            description="Prevent destructive recovery actions during consensus recovery",
            effect=PolicyEffect.DENY,
            scope=PolicyScope.CONSENSUS,
            action_types=["escalate_to_user"],
            reason_template="Destructive consensus action '{action}' is blocked by policy",
            priority=100,
        ),
        PolicyRule(
            policy_id="gov_log_specialist_failover",
            name="Log specialist failover events",
            description="Specialist failovers are logged for audit without blocking normal operations",
            effect=PolicyEffect.LOG_ONLY,
            scope=PolicyScope.SPECIALIST,
            action_types=["failover"],
            reason_template="Specialist failover '{action}' for {specialist} logged for audit",
            priority=10,
        ),
        PolicyRule(
            policy_id="gov_deny_abort_without_notification",
            name="Deny silent task aborts",
            description="Task aborts must be accompanied by notification — prevent silent failures",
            effect=PolicyEffect.DENY,
            scope=PolicyScope.TASK,
            action_types=["abort_task"],
            reason_template="Task abort '{action}' is denied — must notify user first",
            priority=100,
        ),
        PolicyRule(
            policy_id="gov_log_consensus_escalation",
            name="Log consensus escalations",
            description="All consensus escalations must be logged for audit",
            effect=PolicyEffect.LOG_ONLY,
            scope=PolicyScope.CONSENSUS,
            action_types=["escalate_to_user", "use_architect_decision"],
            reason_template="Consensus action '{action}' logged for audit",
            priority=10,
        ),
        PolicyRule(
            policy_id="gov_log_task_replan",
            name="Log task replan events",
            description="All task replanning events must be logged for audit",
            effect=PolicyEffect.LOG_ONLY,
            scope=PolicyScope.TASK,
            action_types=["replan"],
            reason_template="Task replan '{action}' logged for audit",
            priority=10,
        ),
        PolicyRule(
            policy_id="gov_deny_specialist_escalation_sentinel",
            name="Deny SENTINEL escalation",
            description="SENTINEL should never be autonomously escalated — always requires Architect review",
            effect=PolicyEffect.DENY,
            scope=PolicyScope.SPECIALIST,
            specialists=["SENTINEL"],
            action_types=["escalate_to_architect"],
            reason_template="SENTINEL escalation '{action}' is denied — requires manual Architect review",
            priority=100,
        ),
    ]
