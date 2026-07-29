"""tests/test_execution_sandbox.py — Phase 17: Security Hardening for Execution

Tests ExecutionSandbox, SandboxCapability, RollbackApprovalGate, and SecurityIntegration.
"""

import pytest
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, AsyncMock, patch

from core.execution.tool_registry import (
    ToolExecutionRegistry,
    ToolSpec,
    ToolResult,
    ToolCategory,
    RetryPolicy,
)
from core.execution.sandbox_session import (
    PersistentSandboxSession,
    SessionStatus,
)
from core.security.execution_sandbox import (
    ExecutionSandbox,
    SandboxCapability,
    SandboxPolicyAction,
    SandboxAuditRecord,
    RollbackApprovalGate,
    RollbackApprovalRequest,
    RollbackRiskLevel,
    SecurityIntegration,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_fs():
    fs = MagicMock()
    fs.read_file.return_value = {"status": "success", "data": "content"}
    return fs


@pytest.fixture
def session(mock_fs) -> PersistentSandboxSession:
    return PersistentSandboxSession(filesystem=mock_fs, workspace_root="/workspace")


@pytest.fixture
def sandbox() -> ExecutionSandbox:
    return ExecutionSandbox()


@pytest.fixture
def rollback_gate() -> RollbackApprovalGate:
    return RollbackApprovalGate(
        moderate_threshold=3,
        destructive_threshold=10,
        auto_approve_safe=True,
    )


@pytest.fixture
def registry() -> ToolExecutionRegistry:
    reg = ToolExecutionRegistry()
    spec = ToolSpec(
        name="read_file",
        category=ToolCategory.FILE_OPERATION,
        timeout=10.0,
    )
    async def handler(): return {"status": "success", "data": "content"}
    reg.register(spec, handler)

    spec2 = ToolSpec(
        name="bash_exec",
        category=ToolCategory.COMMAND_EXECUTION,
        timeout=30.0,
    )
    async def handler2(): return {"status": "success", "logs": "done"}
    reg.register(spec2, handler2)

    spec3 = ToolSpec(
        name="network_tool",
        category=ToolCategory.UTILITY,
        timeout=15.0,
    )
    async def handler3(): return {"status": "success", "output": "data"}
    reg.register(spec3, handler3)
    return reg


# ============================================================================
# SandboxCapability Tests
# ============================================================================


class TestSandboxCapability:
    """SandboxCapability allows_tool and requires_approval."""

    def test_allows_tool_by_name(self):
        caps = SandboxCapability(allowed_tools={"read_file", "write_file"})
        assert caps.allows_tool("read_file") is True
        assert caps.allows_tool("bash_exec") is False

    def test_allows_tool_by_category(self):
        caps = SandboxCapability(
            allowed_categories={ToolCategory.FILE_OPERATION},
        )
        spec = ToolSpec(name="read_file", category=ToolCategory.FILE_OPERATION)
        assert caps.allows_tool("read_file", spec) is True
        assert caps.allows_tool("bash_exec") is False

    def test_blocked_takes_precedence(self):
        caps = SandboxCapability(
            allowed_tools={"read_file", "bash_exec"},
            blocked_tools={"bash_exec"},
        )
        assert caps.allows_tool("read_file") is True
        assert caps.allows_tool("bash_exec") is False  # Blocked overrides allowed

    def test_empty_allows_all(self):
        caps = SandboxCapability()
        assert caps.allows_tool("anything") is True
        assert caps.allows_tool("read_file") is True

    def test_requires_approval(self):
        caps = SandboxCapability(
            require_approval_for={"bash_exec", "network_tool"},
        )
        assert caps.requires_approval("bash_exec") is True
        assert caps.requires_approval("read_file") is False

    def test_bypass_approval(self):
        caps = SandboxCapability(
            require_approval_for={"bash_exec"},
            bypass_approval=True,
        )
        assert caps.requires_approval("bash_exec") is False  # Bypassed

    def test_to_dict(self):
        caps = SandboxCapability(
            allowed_tools={"read_file"},
            allowed_categories={ToolCategory.FILE_OPERATION},
            blocked_tools={"bash_exec"},
        )
        d = caps.to_dict()
        assert "read_file" in d["allowed_tools"]
        assert "file_operation" in d["allowed_categories"]
        assert "bash_exec" in d["blocked_tools"]


# ============================================================================
# ExecutionSandbox Tests
# ============================================================================


class TestExecutionSandbox:
    """ExecutionSandbox capability management and tool checks."""

    def test_initialize(self):
        sandbox = ExecutionSandbox()
        stats = sandbox.get_statistics()
        assert stats["total_checks"] == 0
        assert stats["active_sessions"] == 0

    def test_default_capability(self, sandbox):
        """Default capability allows FILE_OPERATION and CODE_ANALYSIS."""
        spec = ToolSpec(name="read_file", category=ToolCategory.FILE_OPERATION)
        caps = sandbox.get_session_capabilities("unknown_session")
        assert caps.allows_tool("read_file", spec) is True
        assert caps.allows_tool("bash_exec") is False  # Not in default categories

    def test_set_session_capabilities(self, sandbox):
        """Capabilities can be set per session."""
        caps = SandboxCapability(allowed_tools={"read_file"})
        sandbox.set_session_capabilities("session_1", caps)

        retrieved = sandbox.get_session_capabilities("session_1")
        assert retrieved.allowed_tools == {"read_file"}

    def test_remove_session(self, sandbox):
        """Removing a session restores default capabilities."""
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"bash_exec"},
        ))
        sandbox.remove_session("s1")

        caps = sandbox.get_session_capabilities("s1")
        assert caps.allows_tool("bash_exec") is False  # Back to default

    @pytest.mark.asyncio
    async def test_check_blocked_tool(self, sandbox):
        """Blocked tool returns BLOCK action."""
        sandbox.set_session_capabilities("s1", SandboxCapability(
            blocked_tools={"bash_exec"},
        ))
        action, reason, _ = await sandbox.check_tool("bash_exec", "s1")
        assert action == SandboxPolicyAction.BLOCK
        assert "blocked" in reason.lower()

    @pytest.mark.asyncio
    async def test_check_allowed_tool(self, sandbox, registry):
        """Allowed tool returns ALLOW action."""
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"read_file"},
        ))
        spec = registry.get_spec("read_file")
        action, reason, _ = await sandbox.check_tool("read_file", "s1", spec=spec)
        assert action == SandboxPolicyAction.ALLOW

    @pytest.mark.asyncio
    async def test_check_not_allowed_tool(self, sandbox):
        """Tool not in allowed list returns BLOCK."""
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"read_file"},
        ))
        action, reason, _ = await sandbox.check_tool("bash_exec", "s1")
        assert action == SandboxPolicyAction.BLOCK
        assert "not in allowed" in reason.lower()

    @pytest.mark.asyncio
    async def test_check_approval_required(self, sandbox):
        """Tool requiring approval returns REQUIRE_APPROVAL."""
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"bash_exec"},
            require_approval_for={"bash_exec"},
        ))
        action, reason, _ = await sandbox.check_tool("bash_exec", "s1")
        assert action == SandboxPolicyAction.REQUIRE_APPROVAL

    @pytest.mark.asyncio
    async def test_check_bypass_approval(self, sandbox):
        """Bypass approval allows tool directly."""
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"bash_exec"},
            require_approval_for={"bash_exec"},
            bypass_approval=True,
        ))
        action, reason, _ = await sandbox.check_tool("bash_exec", "s1")
        assert action == SandboxPolicyAction.ALLOW

    @pytest.mark.asyncio
    async def test_check_concurrent_limit(self, sandbox):
        """Concurrent execution limit blocks excess."""
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"read_file"},
            max_concurrent_executions=2,
        ))

        # Acquire 2 slots
        assert sandbox.acquire_execution_slot("s1") is True
        assert sandbox.acquire_execution_slot("s1") is True
        # Third should fail
        assert sandbox.acquire_execution_slot("s1") is False

        # Release one
        sandbox.release_execution_slot("s1")
        assert sandbox.acquire_execution_slot("s1") is True

    def test_acquire_execution_slot_unlimited(self, sandbox):
        """Unlimited concurrent executions always succeed."""
        for _ in range(100):
            assert sandbox.acquire_execution_slot("s1") is True

    def test_audit_records(self, sandbox):
        """Audit records are tracked."""
        sandbox._record_audit(
            tool_name="bash_exec",
            session_id="s1",
            action=SandboxPolicyAction.BLOCK,
            reason="Testing",
            capabilities=SandboxCapability(),
        )
        records = sandbox.get_audit_records()
        assert len(records) == 1
        assert records[0].tool_name == "bash_exec"

    def test_audit_records_filtered(self, sandbox):
        """Audit records can be filtered by session and action."""
        sandbox._record_audit("tool_a", "s1", SandboxPolicyAction.ALLOW, "ok", SandboxCapability())
        sandbox._record_audit("tool_b", "s1", SandboxPolicyAction.BLOCK, "no", SandboxCapability())
        sandbox._record_audit("tool_c", "s2", SandboxPolicyAction.ALLOW, "ok", SandboxCapability())

        assert len(sandbox.get_audit_records(session_id="s1")) == 2
        assert len(sandbox.get_audit_records(action=SandboxPolicyAction.BLOCK)) == 1
        assert len(sandbox.get_audit_records(session_id="s2")) == 1

    def test_statistics(self, sandbox):
        """Statistics reflect audit records."""
        sandbox._record_audit("a", "s1", SandboxPolicyAction.ALLOW, "ok", SandboxCapability())
        sandbox._record_audit("b", "s1", SandboxPolicyAction.BLOCK, "no", SandboxCapability())
        sandbox._record_audit("c", "s1", SandboxPolicyAction.REQUIRE_APPROVAL, "maybe", SandboxCapability())

        stats = sandbox.get_statistics()
        assert stats["total_checks"] == 3
        assert stats["allowed"] == 1
        assert stats["blocked"] == 1
        assert stats["approval_required"] == 1

    def test_update_default_capabilities(self, sandbox):
        """Default capabilities can be updated."""
        sandbox.update_default_capabilities(SandboxCapability(
            allowed_tools={"custom_tool"},
        ))
        caps = sandbox.get_session_capabilities("new_session")
        assert caps.allows_tool("custom_tool") is True
        assert caps.allows_tool("read_file") is False

    def test_to_terminal_display(self, sandbox):
        """to_terminal_display returns human-readable output."""
        display = sandbox.to_terminal_display()
        assert "EXECUTION SANDBOX" in display

    @pytest.mark.asyncio
    async def test_check_with_sec_orchestrator(self):
        """SecurityOrchestrator integration allows/denies based on policy."""
        orchestrator = MagicMock()
        orchestrator.check_execution = AsyncMock()
        sec_ctx = MagicMock()
        sec_ctx.is_blocked = False
        sec_ctx.is_approved = True
        sec_ctx.decision.reason = "Approved by policy"
        orchestrator.check_execution.return_value = sec_ctx

        sandbox = ExecutionSandbox(security_orchestrator=orchestrator)
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"bash_exec"},
        ))

        action, reason, ctx = await sandbox.check_tool("bash_exec", "s1")
        assert action == SandboxPolicyAction.ALLOW
        orchestrator.check_execution.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_blocks_with_orchestrator(self):
        """SecurityOrchestrator can block a tool."""
        orchestrator = MagicMock()
        orchestrator.check_execution = AsyncMock()
        sec_ctx = MagicMock()
        sec_ctx.is_blocked = True
        sec_ctx.decision.reason = "Blocked by policy"
        orchestrator.check_execution.return_value = sec_ctx

        sandbox = ExecutionSandbox(security_orchestrator=orchestrator)
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"bash_exec"},
        ))

        action, reason, ctx = await sandbox.check_tool("bash_exec", "s1")
        assert action == SandboxPolicyAction.BLOCK
        assert "Blocked" in reason


# ============================================================================
# RollbackApprovalGate Tests
# ============================================================================


class TestRollbackApprovalGate:
    """RollbackApprovalGate risk evaluation and approval workflow."""

    def test_evaluate_safe_rollback(self, session, rollback_gate):
        """Few file changes → SAFE risk → auto-approved."""
        session.state.files_created.add("/tmp/test.py")
        request = rollback_gate.evaluate_rollback(
            session, "ckpt_1", reason="Test rollback",
        )
        assert request.risk_level == RollbackRiskLevel.SAFE
        assert request.approved is True
        assert request.file_count == 1

    def test_evaluate_moderate_rollback(self, session, rollback_gate):
        """3-9 file changes → MODERATE risk → auto-approved."""
        for i in range(5):
            session.state.files_created.add(f"/tmp/file_{i}.py")
        request = rollback_gate.evaluate_rollback(
            session, "ckpt_2", reason="Moderate rollback",
        )
        assert request.risk_level == RollbackRiskLevel.MODERATE
        assert request.approved is True  # MODERATE is auto-approved

    def test_evaluate_destructive_rollback(self, session, rollback_gate):
        """10+ file changes → DESTRUCTIVE risk → requires approval."""
        for i in range(15):
            session.state.files_created.add(f"/tmp/file_{i}.py")
        request = rollback_gate.evaluate_rollback(
            session, "ckpt_3", reason="Large rollback",
        )
        assert request.risk_level == RollbackRiskLevel.DESTRUCTIVE
        assert request.approved is False  # Requires explicit approval

    def test_check_rollback_allowed_safe(self, session, rollback_gate):
        """Safe rollbacks are allowed without explicit approval."""
        session.state.files_modified.add("/tmp/test.py")
        allowed, message, request = rollback_gate.check_rollback_allowed(
            session, "ckpt_1", reason="Test",
        )
        assert allowed is True
        assert "auto-approved" in message

    def test_check_rollback_allowed_destructive(self, session, rollback_gate):
        """Destructive rollbacks are NOT allowed without approval."""
        for i in range(10):
            session.state.files_created.add(f"/tmp/file_{i}.py")

        allowed, message, request = rollback_gate.check_rollback_allowed(
            session, "ckpt_3", reason="Large rollback",
        )
        assert allowed is False
        assert "requires approval" in message

    def test_approve_rollback(self, session, rollback_gate):
        """Pending rollback can be approved."""
        for i in range(10):
            session.state.files_created.add(f"/tmp/file_{i}.py")

        # First evaluate — should go to pending
        rollback_gate.evaluate_rollback(session, "ckpt_3", reason="Large")

        # Approve
        request = rollback_gate.approve_rollback(
            session.session_id, approved_by="operator",
        )
        assert request is not None
        assert request.approved is True
        assert request.approved_by == "operator"

    def test_deny_rollback(self, session, rollback_gate):
        """Pending rollback can be denied."""
        for i in range(10):
            session.state.files_created.add(f"/tmp/file_{i}.py")

        rollback_gate.evaluate_rollback(session, "ckpt_3", reason="Large")
        request = rollback_gate.deny_rollback(
            session.session_id, denied_by="operator",
        )
        assert request is not None

        # Should be removed from pending
        assert len(rollback_gate.get_pending_requests()) == 0

    def test_approve_nonexistent(self, rollback_gate):
        """Approving nonexistent rollback returns None."""
        request = rollback_gate.approve_rollback("nonexistent")
        assert request is None

    def test_get_pending_requests(self, session, rollback_gate):
        """Pending destructive rollbacks appear in pending list."""
        for i in range(10):
            session.state.files_created.add(f"/tmp/file_{i}.py")

        rollback_gate.evaluate_rollback(session, "ckpt_3", reason="Large")

        pending = rollback_gate.get_pending_requests()
        assert len(pending) == 1
        assert pending[0].session_id == session.session_id

    def test_get_history(self, session, rollback_gate):
        """All rollback evaluations are recorded in history."""
        session.state.files_modified.add("/tmp/a.py")
        rollback_gate.evaluate_rollback(session, "ckpt_1")

        session2 = MagicMock()
        session2.session_id = "session_2"
        session2.state.files_created = set()
        session2.state.files_modified = set()
        session2.state.files_deleted = set()

        for i in range(10):
            session2.state.files_created.add(f"/tmp/f{i}.py")

        # Need to make session2 a proper mock with the right attributes
        class FakeSession:
            def __init__(self):
                self.session_id = "session_2"

        fake = FakeSession()
        fake.state = MagicMock()
        fake.state.files_created = {f"/tmp/f{i}.py" for i in range(10)}
        fake.state.files_modified = set()
        fake.state.files_deleted = set()

        rollback_gate.evaluate_rollback(fake, "ckpt_3", reason="Bulk")

        history = rollback_gate.get_history()
        assert len(history) == 2

    def test_statistics(self, session, rollback_gate):
        """Statistics reflect all rollback evaluations."""
        session.state.files_created.add("/tmp/a.py")
        rollback_gate.evaluate_rollback(session, "ckpt_1")

        stats = rollback_gate.get_statistics()
        assert stats["total_rollbacks"] == 1
        assert stats["safe"] == 1
        assert stats["approved"] == 1

    def test_statistics_with_denied(self, session, rollback_gate):
        """Statistics track denied rollbacks."""
        for i in range(15):
            session.state.files_created.add(f"/tmp/f{i}.py")
        rollback_gate.evaluate_rollback(session, "ckpt_3")

        stats = rollback_gate.get_statistics()
        assert stats["destructive"] == 1
        # Not yet denied

        rollback_gate.deny_rollback(session.session_id)
        # After deny, it's still in history but not pending
        stats = rollback_gate.get_statistics()
        assert stats["pending"] == 0

    def test_to_terminal_display(self, rollback_gate):
        """to_terminal_display returns human-readable output."""
        display = rollback_gate.to_terminal_display()
        assert "ROLLBACK APPROVAL GATE" in display

    def test_auto_approve_disabled(self, session):
        """When auto_approve_safe=False, safe rollbacks still require approval."""
        gate = RollbackApprovalGate(auto_approve_safe=False)
        session.state.files_created.add("/tmp/test.py")

        request = gate.evaluate_rollback(session, "ckpt_1")
        assert request.risk_level == RollbackRiskLevel.SAFE
        assert request.approved is False  # Auto-approve disabled

    def test_custom_thresholds(self, session):
        """Custom thresholds work correctly."""
        gate = RollbackApprovalGate(
            moderate_threshold=1,
            destructive_threshold=3,
        )

        session.state.files_created.add("/tmp/a.py")
        request = gate.evaluate_rollback(session, "ckpt_1")
        assert request.risk_level == RollbackRiskLevel.MODERATE  # 1 >= 1

        for i in range(3):
            session.state.files_created.add(f"/tmp/f{i}.py")
        request = gate.evaluate_rollback(session, "ckpt_2")
        assert request.risk_level == RollbackRiskLevel.DESTRUCTIVE  # 4 >= 3


# ============================================================================
# SecurityIntegration Tests
# ============================================================================


class TestSecurityIntegration:
    """SecurityIntegration hooks and session configuration."""

    def test_create_pre_execution_hook_allows(self, sandbox, registry):
        """Pre-execution hook allows permitted tools."""
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"read_file"},
        ))
        integration = SecurityIntegration(sandbox, MagicMock())
        hook = integration.create_pre_execution_hook("s1")

        # Should not raise
        hook("read_file", {})

    def test_create_pre_execution_hook_blocks(self, sandbox):
        """Pre-execution hook raises PermissionError for blocked tools."""
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"read_file"},
            blocked_tools={"bash_exec"},
        ))
        integration = SecurityIntegration(sandbox, MagicMock())
        hook = integration.create_pre_execution_hook("s1")

        with pytest.raises(PermissionError):
            hook("bash_exec", {})

    def test_create_pre_execution_hook_blocked_not_allowed(self, sandbox):
        """Pre-execution hook blocks tools not in allowed list."""
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"read_file"},
        ))
        integration = SecurityIntegration(sandbox, MagicMock())
        hook = integration.create_pre_execution_hook("s1")

        with pytest.raises(PermissionError):
            hook("network_tool", {})

    def test_post_execution_hook_releases_slot(self, sandbox):
        """Post-execution hook releases concurrent slot."""
        integration = SecurityIntegration(sandbox, MagicMock())
        hook = integration.create_post_execution_hook("s1")

        # Acquire a slot — must use finite limit for counter to increment
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"read_file"},
            max_concurrent_executions=5,
        ))
        assert sandbox.acquire_execution_slot("s1") is True
        assert sandbox._concurrent_counts.get("s1", 0) == 1

        # Release via hook
        hook(None)
        assert sandbox._concurrent_counts.get("s1", 0) == 0

    def test_check_rollback_allows_safe(self, sandbox, session):
        """SecurityIntegration.check_rollback allows safe rollbacks."""
        gate = RollbackApprovalGate()
        integration = SecurityIntegration(sandbox, gate)

        session.state.files_modified.add("/tmp/test.py")
        allowed, message = integration.check_rollback(session, "ckpt_1", reason="Test")
        assert allowed is True

    def test_check_rollback_blocks_destructive(self, sandbox, session):
        """SecurityIntegration.check_rollback blocks destructive rollbacks."""
        gate = RollbackApprovalGate()
        integration = SecurityIntegration(sandbox, gate)

        for i in range(15):
            session.state.files_created.add(f"/tmp/f{i}.py")

        allowed, message = integration.check_rollback(session, "ckpt_3", reason="Bulk")
        assert allowed is False
        assert "requires approval" in message

    def test_configure_session(self, sandbox):
        """configure_session sets capabilities and registers hook."""
        gate = RollbackApprovalGate()
        integration = SecurityIntegration(sandbox, gate)

        caps = SandboxCapability(allowed_tools={"read_file"})
        integration.configure_session("s1", capabilities=caps)

        # Check capabilities were set
        assert sandbox.get_session_capabilities("s1").allowed_tools == {"read_file"}

    def test_configure_session_no_caps(self, sandbox):
        """configure_session works without explicit capabilities."""
        gate = RollbackApprovalGate()
        integration = SecurityIntegration(sandbox, gate)

        integration.configure_session("s1")
        # Uses default capabilities
        caps = sandbox.get_session_capabilities("s1")
        assert caps is not None

    def test_get_system_status(self, sandbox, rollback_gate):
        """get_system_status returns combined component stats."""
        integration = SecurityIntegration(sandbox, rollback_gate)
        status = integration.get_system_status()
        assert "sandbox" in status
        assert "rollback_gate" in status

    def test_to_terminal_display(self, sandbox, rollback_gate):
        """to_terminal_display returns combined output."""
        integration = SecurityIntegration(sandbox, rollback_gate)
        display = integration.to_terminal_display()
        assert "SECURITY INTEGRATION" in display
        assert "EXECUTION SANDBOX" in display
        assert "ROLLBACK APPROVAL GATE" in display

    def test_pre_execution_hook_with_sec_orchestrator(self, sandbox):
        """Pre-execution hook works with SecurityOrchestrator integration."""
        integration = SecurityIntegration(sandbox, MagicMock())
        sandbox.set_session_capabilities("s1", SandboxCapability(
            allowed_tools={"read_file"},
        ))

        hook = integration.create_pre_execution_hook("s1")
        # Should not raise — read_file is allowed
        hook("read_file", {})

    @pytest.mark.asyncio
    async def test_check_rollback_with_orchestrator(self, session):
        """check_rollback integrates with SecurityOrchestrator."""
        orchestrator = MagicMock()
        orchestrator.check_execution = AsyncMock()
        sec_ctx = MagicMock()
        sec_ctx.is_blocked = False
        sec_ctx.is_approved = True
        orchestrator.check_execution.return_value = sec_ctx

        sandbox = ExecutionSandbox(security_orchestrator=orchestrator)
        gate = RollbackApprovalGate(security_orchestrator=orchestrator)
        integration = SecurityIntegration(sandbox, gate, orchestrator)

        for i in range(10):
            session.state.files_created.add(f"/tmp/f{i}.py")

        allowed, message = integration.check_rollback(session, "ckpt_3", reason="Test")
        # DESTRUCTIVE without explicit approval → denied
        assert allowed is False
