"""Tests for Layer 10 — Autonomous Recovery Governance."""

import pytest


class TestRecoveryGovernance:
    """Tests for the RecoveryGovernance layer."""

    def test_unknown_failure_aborts(self):
        from runtime_next.verification.governance import RecoveryGovernance
        from runtime_next.verification.types import (
            FailureClassification, RecoveryStrategy,
        )

        governance = RecoveryGovernance()
        strategy = RecoveryStrategy(
            id="strat_test",
            name="Test strategy",
            failure_type=FailureClassification.UNKNOWN_FAILURE,
            max_retries=0,
            requires_user_approval=True,
        )

        import asyncio

        async def run():
            decision = await governance.decide(
                failure_type=FailureClassification.UNKNOWN_FAILURE,
                strategy=strategy,
                action_type="escalate",
            )
            assert decision.verdict == "abort"
            assert decision.should_stop_autonomy() is True
            assert decision.requires_user_intervention is True

        asyncio.run(run())

    def test_safe_autonomous_recovery(self):
        from runtime_next.verification.governance import RecoveryGovernance
        from runtime_next.verification.types import (
            FailureClassification, RecoveryStrategy,
        )

        governance = RecoveryGovernance()
        strategy = RecoveryStrategy(
            id="strat_safe",
            name="Safe retry",
            failure_type=FailureClassification.TIMEOUT,
            danger_level="safe",
            max_retries=3,
        )

        import asyncio

        async def run():
            decision = await governance.decide(
                failure_type=FailureClassification.TIMEOUT,
                strategy=strategy,
                action_type="retry",
                context={"retry_count": 0},
            )
            assert decision.verdict == "auto_recover"
            assert decision.requires_user_intervention is False

        asyncio.run(run())

    def test_approval_required_strategy(self):
        from runtime_next.verification.governance import RecoveryGovernance
        from runtime_next.verification.types import (
            FailureClassification, RecoveryStrategy,
        )

        governance = RecoveryGovernance()
        strategy = RecoveryStrategy(
            id="strat_approval",
            name="Requires approval",
            failure_type=FailureClassification.PERMISSION_DENIED,
            danger_level="safe",
            max_retries=0,
            requires_user_approval=True,
        )

        import asyncio

        async def run():
            decision = await governance.decide(
                failure_type=FailureClassification.PERMISSION_DENIED,
                strategy=strategy,
                action_type="escalate",
            )
            assert decision.verdict == "require_approval"
            assert decision.requires_user_intervention is True

        asyncio.run(run())

    def test_budget_exhausted_notifies_user(self):
        from runtime_next.verification.governance import RecoveryGovernance
        from runtime_next.verification.types import (
            FailureClassification, RecoveryStrategy,
        )

        governance = RecoveryGovernance()
        strategy = RecoveryStrategy(
            id="strat_timeout",
            name="Timeout retry",
            failure_type=FailureClassification.TIMEOUT,
            danger_level="safe",
            max_retries=2,
        )

        import asyncio

        async def run():
            decision = await governance.decide(
                failure_type=FailureClassification.TIMEOUT,
                strategy=strategy,
                action_type="retry",
                context={
                    "retry_count": 3,
                    "max_retries": 2,
                },
            )
            assert decision.verdict == "notify_user"
            assert decision.requires_user_intervention is True
            assert decision.suggested_message is not None

        asyncio.run(run())

    def test_permission_denied_always_escalates(self):
        from runtime_next.verification.governance import RecoveryGovernance
        from runtime_next.verification.types import (
            FailureClassification, RecoveryStrategy,
        )

        governance = RecoveryGovernance()
        strategy = RecoveryStrategy(
            id="strat_perm",
            name="Permission handler",
            failure_type=FailureClassification.PERMISSION_DENIED,
            max_retries=0,
            requires_user_approval=True,
        )

        import asyncio

        async def run():
            decision = await governance.decide(
                failure_type=FailureClassification.PERMISSION_DENIED,
                strategy=strategy,
                action_type="escalate",
                context={
                    "node_description": "writing to config file",
                },
            )
            assert decision.requires_user_intervention is True
            assert decision.suggested_message is not None

        asyncio.run(run())

    def test_architecture_violation_escalates(self):
        from runtime_next.verification.governance import RecoveryGovernance
        from runtime_next.verification.types import (
            FailureClassification, RecoveryStrategy,
        )

        governance = RecoveryGovernance()
        strategy = RecoveryStrategy(
            id="strat_arch",
            name="Architecture handler",
            failure_type=FailureClassification.ARCHITECTURE_VIOLATION,
            max_retries=0,
            requires_user_approval=True,
        )

        import asyncio

        async def run():
            decision = await governance.decide(
                failure_type=FailureClassification.ARCHITECTURE_VIOLATION,
                strategy=strategy,
                action_type="escalate",
            )
            assert decision.requires_user_intervention is True

        asyncio.run(run())

    def test_dangerous_action_requires_approval(self):
        from runtime_next.verification.governance import RecoveryGovernance
        from runtime_next.verification.types import (
            FailureClassification, RecoveryStrategy,
        )

        governance = RecoveryGovernance()
        strategy = RecoveryStrategy(
            id="strat_dangerous",
            name="Dangerous rollback",
            failure_type=FailureClassification.SERIALIZATION_FAILURE,
            danger_level="destructive",
            max_retries=1,
        )

        import asyncio

        async def run():
            decision = await governance.decide(
                failure_type=FailureClassification.SERIALIZATION_FAILURE,
                strategy=strategy,
                action_type="rollback",
            )
            assert decision.verdict == "require_approval"
            assert decision.danger_assessment == "destructive"

        asyncio.run(run())

    def test_approval_tracking(self):
        from runtime_next.verification.governance import RecoveryGovernance
        from runtime_next.verification.types import (
            FailureClassification, RecoveryStrategy,
        )

        governance = RecoveryGovernance()
        strategy = RecoveryStrategy(
            id="strat_approval",
            name="Needs approval",
            failure_type=FailureClassification.PERMISSION_DENIED,
            max_retries=0,
            requires_user_approval=True,
        )

        import asyncio

        async def run():
            decision = await governance.decide(
                failure_type=FailureClassification.PERMISSION_DENIED,
                strategy=strategy,
                action_type="escalate",
            )

            # Simulate asking user
            governance.mark_approval_pending(decision.reason)

            assert len(governance.pending_approvals) >= 1

            # Approve
            assert governance.approve(decision.reason) is True

            # Already approved
            assert governance.approve(decision.reason) is False

        asyncio.run(run())

    def test_auto_recovery_count(self):
        from runtime_next.verification.governance import RecoveryGovernance
        from runtime_next.verification.types import (
            FailureClassification, RecoveryStrategy,
        )

        governance = RecoveryGovernance()
        safe_strategy = RecoveryStrategy(
            id="strat_safe",
            name="Safe retry",
            failure_type=FailureClassification.TIMEOUT,
            danger_level="safe",
            max_retries=3,
        )

        import asyncio

        async def run():
            for _ in range(3):
                await governance.decide(
                    failure_type=FailureClassification.TIMEOUT,
                    strategy=safe_strategy,
                    action_type="retry",
                    context={"retry_count": 0},
                )
            assert governance.auto_recovery_count == 3
            assert governance.intervention_count == 0

        asyncio.run(run())

    def test_intervention_count(self):
        from runtime_next.verification.governance import RecoveryGovernance
        from runtime_next.verification.types import (
            FailureClassification, RecoveryStrategy,
        )

        governance = RecoveryGovernance()
        unknown_strategy = RecoveryStrategy(
            id="strat_unknown",
            name="Unknown handler",
            failure_type=FailureClassification.UNKNOWN_FAILURE,
            max_retries=0,
            requires_user_approval=True,
        )

        import asyncio

        async def run():
            await governance.decide(
                failure_type=FailureClassification.UNKNOWN_FAILURE,
                strategy=unknown_strategy,
                action_type="escalate",
            )
            assert governance.intervention_count == 1

        asyncio.run(run())
