"""Phase 13 — Governance Integration Tests.

Covers:
- PolicyRule creation, matching, and lifecycle (enable/disable)
- GovernancePolicyEngine: evaluation, conflict resolution, approval flow, default policies
- RecoveryGovernanceHooks: pre/post hooks for all three recovery levels
- RecoveryEngine integration: hooks wired, default policies loaded
"""


from runtime_next.governance.policy_engine import (
    GovernancePolicyEngine,
    PolicyRule,
    PolicyEffect,
    PolicyScope,
    create_default_policies,
)
from runtime_next.governance.recovery_hooks import (
    RecoveryGovernanceHooks,
    HookResult,
)


# ═══════════════════════════════════════════════════════════════════════════════
# PolicyRule Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestPolicyRule:
    def test_create_rule(self):
        """Basic PolicyRule creation."""
        rule = PolicyRule(
            policy_id="pol_test",
            name="Test policy",
            description="Test description",
            effect=PolicyEffect.DENY,
            scope=PolicyScope.CONSENSUS,
            action_types=["escalate_to_user"],
        )
        assert rule.policy_id == "pol_test"
        assert rule.name == "Test policy"
        assert rule.effect == PolicyEffect.DENY
        assert rule.enabled is True

    def test_matches_scope(self):
        """Rule matches correct scope."""
        rule = PolicyRule(
            policy_id="pol_match",
            name="Match test",
            description="",
            effect=PolicyEffect.DENY,
            scope=PolicyScope.SPECIALIST,
        )
        assert rule.matches(scope=PolicyScope.SPECIALIST, action_type="failover")
        assert not rule.matches(scope=PolicyScope.CONSENSUS, action_type="anything")
        assert not rule.matches(scope=PolicyScope.TASK, action_type="anything")

    def test_matches_action_type(self):
        """Rule matches specific action type."""
        rule = PolicyRule(
            policy_id="pol_action",
            name="Action filter",
            description="",
            effect=PolicyEffect.DENY,
            scope=PolicyScope.ALL,
            action_types=["abort_task", "rollback"],
        )
        assert rule.matches(scope=PolicyScope.TASK, action_type="abort_task")
        assert rule.matches(scope=PolicyScope.TASK, action_type="rollback")
        assert not rule.matches(scope=PolicyScope.TASK, action_type="retry_phase")

    def test_matches_specialist(self):
        """Rule matches specific specialist."""
        rule = PolicyRule(
            policy_id="pol_sp",
            name="Specialist filter",
            description="",
            effect=PolicyEffect.DENY,
            scope=PolicyScope.SPECIALIST,
            specialists=["SENTINEL"],
            action_types=["escalate_to_architect"],
        )
        assert rule.matches(
            scope=PolicyScope.SPECIALIST,
            action_type="escalate_to_architect",
            specialist="SENTINEL",
        )
        assert not rule.matches(
            scope=PolicyScope.SPECIALIST,
            action_type="escalate_to_architect",
            specialist="FORGE",
        )

    def test_disabled_rule_never_matches(self):
        """Disabled rules never match."""
        rule = PolicyRule(
            policy_id="pol_disabled",
            name="Disabled",
            description="",
            effect=PolicyEffect.DENY,
            scope=PolicyScope.ALL,
            enabled=False,
        )
        assert not rule.matches(scope=PolicyScope.ALL, action_type="anything")

    def test_empty_action_types_matches_all(self):
        """Empty action_types list matches all actions."""
        rule = PolicyRule(
            policy_id="pol_all_actions",
            name="All actions",
            description="",
            effect=PolicyEffect.DENY,
            scope=PolicyScope.TASK,
        )
        assert rule.matches(scope=PolicyScope.TASK, action_type="retry_phase")
        assert rule.matches(scope=PolicyScope.TASK, action_type="abort_task")

    def test_format_reason(self):
        """Reason template is formatted with context."""
        rule = PolicyRule(
            policy_id="pol_reason",
            name="Reason test",
            description="",
            effect=PolicyEffect.DENY,
            scope=PolicyScope.ALL,
            reason_template="Action '{action}' denied for {specialist}",
        )
        reason = rule.format_reason(action="failover", specialist="FORGE")
        assert reason == "Action 'failover' denied for FORGE"

    def test_priority_ordering(self):
        """Higher priority rules sort first."""
        low = PolicyRule(
            policy_id="pol_low", name="Low",
            description="", effect=PolicyEffect.ALLOW,
            scope=PolicyScope.ALL, priority=10,
        )
        high = PolicyRule(
            policy_id="pol_high", name="High",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.ALL, priority=100,
        )
        sorted_rules = sorted([low, high], key=lambda r: r.priority, reverse=True)
        assert sorted_rules[0].policy_id == "pol_high"


# ═══════════════════════════════════════════════════════════════════════════════
# GovernancePolicyEngine Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestGovernancePolicyEngine:
    def test_add_and_get_policy(self):
        """Policies can be added and retrieved."""
        engine = GovernancePolicyEngine()
        rule = PolicyRule(
            policy_id="pol_1", name="Policy 1",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.ALL,
        )
        engine.add_policy(rule)
        assert engine.get_policy("pol_1") is rule
        assert engine.get_policy("nonexistent") is None

    def test_remove_policy(self):
        """Policies can be removed."""
        engine = GovernancePolicyEngine()
        rule = PolicyRule(
            policy_id="pol_rm", name="Remove me",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.ALL,
        )
        engine.add_policy(rule)
        assert engine.remove_policy("pol_rm") is True
        assert engine.get_policy("pol_rm") is None

    def test_enable_disable_policy(self):
        """Policies can be enabled and disabled."""
        engine = GovernancePolicyEngine()
        rule = PolicyRule(
            policy_id="pol_toggle", name="Toggle",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.ALL,
        )
        engine.add_policy(rule)
        assert rule.enabled is True

        engine.disable_policy("pol_toggle")
        assert rule.enabled is False

        engine.enable_policy("pol_toggle")
        assert rule.enabled is True

    def test_evaluate_no_matching_policies(self):
        """No matching policies results in ALLOW."""
        engine = GovernancePolicyEngine()
        evaluation = engine.evaluate(
            scope=PolicyScope.TASK,
            action_type="retry_phase",
        )
        assert evaluation.overall_effect == PolicyEffect.ALLOW
        assert evaluation.is_allowed is True
        assert evaluation.matching_rules == []

    def test_evaluate_deny_wins(self):
        """DENY is more restrictive than LOG_ONLY."""
        engine = GovernancePolicyEngine()
        engine.add_policy(PolicyRule(
            policy_id="pol_deny", name="Deny test",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.TASK, action_types=["abort_task"],
        ))
        engine.add_policy(PolicyRule(
            policy_id="pol_log", name="Log test",
            description="", effect=PolicyEffect.LOG_ONLY,
            scope=PolicyScope.TASK, action_types=["abort_task"],
        ))

        evaluation = engine.evaluate(
            scope=PolicyScope.TASK,
            action_type="abort_task",
        )
        assert evaluation.overall_effect == PolicyEffect.DENY
        assert evaluation.is_denied is True
        assert len(evaluation.matching_rules) == 2

    def test_evaluate_require_approval(self):
        """REQUIRE_APPROVAL blocks unless approved."""
        engine = GovernancePolicyEngine()
        engine.add_policy(PolicyRule(
            policy_id="pol_approve", name="Approval needed",
            description="", effect=PolicyEffect.REQUIRE_APPROVAL,
            scope=PolicyScope.SPECIALIST, action_types=["failover"],
        ))

        evaluation = engine.evaluate(
            scope=PolicyScope.SPECIALIST,
            action_type="failover",
        )
        assert evaluation.overall_effect == PolicyEffect.REQUIRE_APPROVAL
        assert evaluation.requires_approval is True
        assert evaluation.is_allowed is False

    def test_approval_flow(self):
        """Approval request, grant, and reject workflow."""
        engine = GovernancePolicyEngine()
        engine.add_policy(PolicyRule(
            policy_id="pol_app_flow", name="Approval flow",
            description="", effect=PolicyEffect.REQUIRE_APPROVAL,
            scope=PolicyScope.TASK, action_types=["replan"],
        ))

        evaluation = engine.evaluate(
            scope=PolicyScope.TASK,
            action_type="replan",
        )
        token = engine.request_approval(evaluation, {"entity_id": "task_1"})

        # Should be pending
        pending = engine.get_pending_approvals()
        assert token in pending

        # Approve
        assert engine.approve(token) is True
        assert engine.approve("invalid_token") is False

        # Should no longer be pending
        assert token not in engine.get_pending_approvals()

    def test_approval_reject(self):
        """Approval rejection works."""
        engine = GovernancePolicyEngine()
        engine.add_policy(PolicyRule(
            policy_id="pol_rej", name="Reject test",
            description="", effect=PolicyEffect.REQUIRE_APPROVAL,
            scope=PolicyScope.ALL, action_types=["rollback"],
        ))

        evaluation = engine.evaluate(
            scope=PolicyScope.TASK,
            action_type="rollback",
        )
        token = engine.request_approval(evaluation, {"entity_id": "task_2"})

        assert engine.reject(token) is True
        assert engine.reject("invalid") is False

    def test_evaluate_specialist_action(self):
        """Specialist actions evaluate correctly."""
        engine = GovernancePolicyEngine()
        engine.add_policy(PolicyRule(
            policy_id="pol_sp_escalate", name="No SENTINEL escalate",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.SPECIALIST,
            specialists=["SENTINEL"],
            action_types=["escalate_to_architect"],
        ))

        # SENTINEL escalation should be denied
        evaluation = engine.evaluate_specialist_action(
            action_type="escalate_to_architect",
            specialist="SENTINEL",
        )
        assert evaluation.is_denied is True

        # FORGE escalation should be allowed
        evaluation = engine.evaluate_specialist_action(
            action_type="escalate_to_architect",
            specialist="FORGE",
        )
        assert evaluation.is_allowed is True

    def test_evaluate_consensus_action(self):
        """Consensus actions evaluate correctly."""
        engine = GovernancePolicyEngine()
        engine.add_policy(PolicyRule(
            policy_id="pol_cons_deny", name="Deny escalate",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.CONSENSUS,
            action_types=["escalate_to_user"],
        ))

        evaluation = engine.evaluate_consensus_action(
            action_type="escalate_to_user",
            consensus_type="deadlocked",
        )
        assert evaluation.is_denied is True

        evaluation = engine.evaluate_consensus_action(
            action_type="add_architect",
            consensus_type="deadlocked",
        )
        assert evaluation.is_allowed is True

    def test_evaluate_task_action(self):
        """Task actions evaluate correctly."""
        engine = GovernancePolicyEngine()
        engine.add_policy(PolicyRule(
            policy_id="pol_task_deny", name="Deny silent abort",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.TASK,
            action_types=["abort_task"],
        ))

        evaluation = engine.evaluate_task_action(
            action_type="abort_task",
            task_trigger="phase_failure",
        )
        assert evaluation.is_denied is True

    def test_default_policies(self):
        """Default policies are created and functional."""
        policies = create_default_policies()
        assert len(policies) >= 5

        engine = GovernancePolicyEngine()
        for p in policies:
            engine.add_policy(p)

        # Default policy: log consensus escalation
        eval_consensus = engine.evaluate_consensus_action(
            action_type="escalate_to_user",
            consensus_type="deadlocked",
        )
        assert eval_consensus.overall_effect == PolicyEffect.DENY  # More restrictive than LOG_ONLY

        # Default policy: deny SENTINEL escalation
        eval_sentinel = engine.evaluate_specialist_action(
            action_type="escalate_to_architect",
            specialist="SENTINEL",
        )
        assert eval_sentinel.is_denied is True

    def test_get_policies_filtered(self):
        """get_policies filters by scope."""
        engine = GovernancePolicyEngine()
        engine.add_policy(PolicyRule(
            policy_id="pol_all", name="All scope",
            description="", effect=PolicyEffect.ALLOW,
            scope=PolicyScope.ALL,
        ))
        engine.add_policy(PolicyRule(
            policy_id="pol_task", name="Task scope",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.TASK,
        ))

        all_policies = engine.get_policies()
        assert len(all_policies) == 2

        task_policies = engine.get_policies(scope=PolicyScope.TASK)
        assert len(task_policies) == 2  # ALL scope + TASK scope

        consensus_policies = engine.get_policies(scope=PolicyScope.CONSENSUS)
        assert len(consensus_policies) == 1  # Only ALL scope

    def test_get_stats(self):
        """Stats reflect engine state."""
        engine = GovernancePolicyEngine()
        assert engine.get_stats()["total_policies"] == 0
        assert engine.get_stats()["total_evaluations"] == 0

        engine.add_policy(PolicyRule(
            policy_id="pol_stat", name="Stat test",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.ALL, action_types=["abort"],
        ))

        # Run some evaluations
        engine.evaluate(scope=PolicyScope.TASK, action_type="retry")
        engine.evaluate(scope=PolicyScope.TASK, action_type="abort")

        stats = engine.get_stats()
        assert stats["total_policies"] == 1
        assert stats["total_evaluations"] == 2
        assert stats["denied_count"] == 1
        assert stats["allowed_count"] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# RecoveryGovernanceHooks Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestRecoveryGovernanceHooks:
    def test_pre_consensus_hook_allowed(self):
        """Consensus pre-hook allows actions not covered by policies."""
        hooks = RecoveryGovernanceHooks()

        outcome = hooks.pre_consensus_recovery(
            consensus_id="cons_001",
            action_type="add_architect",
            consensus_type="deadlocked",
        )
        assert outcome.result == HookResult.ALLOWED

    def test_pre_consensus_hook_denied(self):
        """Consensus pre-hook denies actions blocked by policies."""
        hooks = RecoveryGovernanceHooks()
        hooks.policy_engine.add_policy(PolicyRule(
            policy_id="hook_deny_cons", name="Deny consensus escalate",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.CONSENSUS,
            action_types=["escalate_to_user"],
        ))

        outcome = hooks.pre_consensus_recovery(
            consensus_id="cons_002",
            action_type="escalate_to_user",
            consensus_type="vetoed",
        )
        assert outcome.result == HookResult.DENIED
        assert outcome.policy_id == "hook_deny_cons"

    def test_pre_specialist_hook_approval(self):
        """Specialist pre-hook requires approval when policy demands it."""
        hooks = RecoveryGovernanceHooks()
        hooks.policy_engine.add_policy(PolicyRule(
            policy_id="hook_approve_sp", name="Approve failover",
            description="", effect=PolicyEffect.REQUIRE_APPROVAL,
            scope=PolicyScope.SPECIALIST,
            action_types=["failover"],
        ))

        outcome = hooks.pre_specialist_recovery(
            task_id="task_001",
            action_type="failover",
            specialist="FORGE",
        )
        assert outcome.result == HookResult.APPROVAL_PENDING
        assert outcome.approval_token is not None

    def test_pre_task_hook_allowed(self):
        """Task pre-hook allows safe actions."""
        hooks = RecoveryGovernanceHooks()

        outcome = hooks.pre_task_recovery(
            task_id="task_002",
            action_type="retry_phase",
            task_trigger="phase_failure",
        )
        assert outcome.result == HookResult.ALLOWED

    def test_pre_task_hook_denied_by_default_policy(self):
        """Default policies deny silent task aborts."""
        hooks = RecoveryGovernanceHooks()
        # Load default policies
        for policy in create_default_policies():
            hooks.policy_engine.add_policy(policy)

        outcome = hooks.pre_task_recovery(
            task_id="task_003",
            action_type="abort_task",
            task_trigger="resource_exhaustion",
        )
        # Default policy denies abort_task
        assert outcome.result == HookResult.DENIED

    def test_pre_hooks_recorded(self):
        """Pre-hooks record history."""
        hooks = RecoveryGovernanceHooks()

        hooks.pre_consensus_recovery("cons_003", "add_architect", "deadlocked")
        hooks.pre_specialist_recovery("task_004", "retry_same", "FORGE")
        hooks.pre_task_recovery("task_005", "skip_phase", "phase_failure")

        history = hooks.get_hook_history()
        assert len(history) == 3

    def test_get_hook_history_filtered(self):
        """Hook history filters by level."""
        hooks = RecoveryGovernanceHooks()

        hooks.pre_consensus_recovery("c1", "add_architect", "deadlocked")
        hooks.pre_specialist_recovery("s1", "retry_same", "FORGE")
        hooks.pre_task_recovery("t1", "retry_phase", "phase_failure")

        cons_history = hooks.get_hook_history(level="consensus")
        assert len(cons_history) == 1

        sp_history = hooks.get_hook_history(level="specialist")
        assert len(sp_history) == 1

        task_history = hooks.get_hook_history(level="task")
        assert len(task_history) == 1

    def test_get_hook_stats(self):
        """Hook stats are accurate."""
        hooks = RecoveryGovernanceHooks()
        hooks.policy_engine.add_policy(PolicyRule(
            policy_id="hook_stats_d", name="Deny stats",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.TASK, action_types=["abort_task"],
        ))

        hooks.pre_task_recovery("t_s1", "retry_phase", "phase_failure")
        hooks.pre_task_recovery("t_s2", "abort_task", "resource_exhaustion")

        stats = hooks.get_hook_stats()
        assert stats["total_hooks"] == 2
        assert stats["denied"] == 1
        assert stats["allowed"] == 1

    def test_evaluate_recovery_action_all_levels(self):
        """evaluate_recovery_action dispatches to the correct hook."""
        hooks = RecoveryGovernanceHooks()
        hooks.policy_engine.add_policy(PolicyRule(
            policy_id="hook_all_sp", name="Deny SENTINEL",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.SPECIALIST,
            specialists=["SENTINEL"],
            action_types=["failover"],
        ))

        # SENTINEL failover should be denied at specialist level
        outcome = hooks.evaluate_recovery_action(
            scope=PolicyScope.SPECIALIST,
            action_type="failover",
            entity_id="task_006",
            specialist="SENTINEL",
        )
        assert outcome.result == HookResult.DENIED

        # FORGE failover should be allowed
        outcome = hooks.evaluate_recovery_action(
            scope=PolicyScope.SPECIALIST,
            action_type="failover",
            entity_id="task_007",
            specialist="FORGE",
        )
        assert outcome.result == HookResult.ALLOWED


# ═══════════════════════════════════════════════════════════════════════════════
# RecoveryEngine Integration Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestRecoveryEngineGovernanceIntegration:
    def test_engine_has_governance(self):
        """RecoveryEngine has governance hooks wired."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        assert hasattr(engine, "governance_policy_engine")
        assert hasattr(engine, "governance_hooks")
        assert engine.governance_policy_engine is not None
        assert engine.governance_hooks is not None

    def test_engine_has_default_policies(self):
        """RecoveryEngine loads default governance policies."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        stats = engine.governance_policy_engine.get_stats()
        assert stats["total_policies"] >= 5
        assert stats["enabled_policies"] >= 5

    def test_governance_hooks_integrated_with_consensus(self):
        """Governance hooks can be used alongside consensus recovery."""
        from runtime_next.recovery.engine import RecoveryEngine

        engine = RecoveryEngine()

        # Add a policy that allows us to test
        engine.governance_policy_engine.add_policy(PolicyRule(
            policy_id="integ_cons", name="Deny escalate_consensus",
            description="", effect=PolicyEffect.DENY,
            scope=PolicyScope.CONSENSUS,
            action_types=["escalate_to_user"],
        ))

        # Pre-hook should deny escalation
        outcome = engine.governance_hooks.pre_consensus_recovery(
            consensus_id="integ_c1",
            action_type="escalate_to_user",
            consensus_type="vetoed",
        )
        assert outcome.result == HookResult.DENIED

        # Non-escalation action should be allowed
        outcome = engine.governance_hooks.pre_consensus_recovery(
            consensus_id="integ_c2",
            action_type="add_architect",
            consensus_type="deadlocked",
        )
        assert outcome.result == HookResult.ALLOWED

    def test_governance_hooks_integrated_with_specialist(self):
        """Governance hooks work with specialist recovery."""
        from runtime_next.recovery.engine import RecoveryEngine

        engine = RecoveryEngine()

        # Specialist action not covered by policies should be allowed
        outcome = engine.governance_hooks.pre_specialist_recovery(
            task_id="integ_sp1",
            action_type="retry_same",
            specialist="FORGE",
        )
        assert outcome.result == HookResult.ALLOWED

    def test_governance_hooks_integrated_with_task(self):
        """Governance hooks work with task recovery."""
        from runtime_next.recovery.engine import RecoveryEngine

        engine = RecoveryEngine()

        # Task action not covered by policies should be allowed
        outcome = engine.governance_hooks.pre_task_recovery(
            task_id="integ_t1",
            action_type="retry_phase",
            task_trigger="phase_failure",
        )
        assert outcome.result == HookResult.ALLOWED

    def test_default_policy_blocks_sentinel_escalation(self):
        """Default policy blocks SENTINEL escalation to Architect."""
        from runtime_next.recovery.engine import RecoveryEngine

        engine = RecoveryEngine()

        # Default policy: deny SENTINEL escalation
        outcome = engine.governance_hooks.pre_specialist_recovery(
            task_id="integ_sentinel",
            action_type="escalate_to_architect",
            specialist="SENTINEL",
        )
        assert outcome.result == HookResult.DENIED

    def test_governance_independent_from_recovery_flow(self):
        """Governance hooks don't interfere with normal recovery operations."""
        from runtime_next.recovery.engine import RecoveryEngine
        from runtime_next.recovery.consensus_recovery import ConsensusFailureType
        from runtime_next.recovery.task_recovery import TaskRecoveryTrigger

        engine = RecoveryEngine()

        # Consensus recovery should work normally
        cons_result = engine.consensus_recovery.handle_consensus_failure(
            consensus_id="gov_integ_cons",
            failure_type=ConsensusFailureType.DEADLOCKED,
            context={"participants": ["FORGE", "SENTINEL"]},
        )
        assert cons_result["recovered"] is True

        # Specialist recovery should work normally
        engine.specialist_recovery.start_task("gov_integ_sp", "FORGE")
        sp_result = engine.specialist_recovery.handle_failure("gov_integ_sp", "FORGE")
        assert sp_result["recovered"] is True

        # Task recovery should work normally
        task_result = engine.task_recovery.handle_task_failure(
            "gov_integ_task",
            TaskRecoveryTrigger.PHASE_FAILURE,
            context={"phase_id": "p1"},
        )
        assert task_result["recovered"] is True

        # Governance hooks fire during recovery operations (wired via __init__)
        stats = engine.governance_hooks.get_hook_stats()
        # Consensus recovery attempted ADD_ARCHITECT which passes governance
        # Specialist recovery attempted RETRY_SAME which passes governance
        # So hooks should have been recorded
        assert stats["total_hooks"] >= 3,\
            f"Expected at least 3 hook evaluations, got {stats['total_hooks']}"
