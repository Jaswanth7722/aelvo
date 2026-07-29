"""Integration tests for sandbox → verification pipeline → recovery engine flow.

Tests the complete chain:
1. Sandbox error produced by sandbox execution
2. SandboxVerifier classifies the error into a VerificationResult
3. VerificationPipeline emits events through the event bus
4. RecoveryEngine catches VERIFICATION_FAILED events and triggers recovery
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from runtime_next.verification.types import (
    VerificationType,
    FailureClassification,
    VerificationScope,
    VerificationManifest,
    Severity,
    Confidence,
    Retryability,
)
from runtime_next.verification.pipeline import VerificationPipeline
from runtime_next.verification.sandbox_verifier import (
    SandboxVerifier,
    register_sandbox_verifier,
    classify_sandbox_error,
)
from runtime_next.verification.events import (
    VerificationCompletedEvent,
    VerificationFailedEvent,
)
from runtime_next.recovery.engine import RecoveryEngine
from runtime_next.models.events import BaseEvent, EventType


# ============================================================================
# SandboxError Classification Tests
# ============================================================================


class TestSandboxErrorClassification:
    """Tests for the classify_sandbox_error function mapping."""

    def test_sandbox_denied_classification(self):
        result = classify_sandbox_error("sandbox_denied", "Dangerous command blocked: rm")
        assert result["classification"] == FailureClassification.PERMISSION_DENIED
        assert result["severity"] == Severity.CRITICAL
        assert result["retryability"] == Retryability.NEVER

    def test_timeout_classification(self):
        result = classify_sandbox_error("timeout", "Process timed out after 30s")
        assert result["classification"] == FailureClassification.TIMEOUT
        assert result["retryability"] == Retryability.CONDITIONAL

    def test_resource_limit_classification(self):
        result = classify_sandbox_error("resource_limit", "Memory limit exceeded")
        assert result["classification"] == FailureClassification.ENVIRONMENT_FAILURE
        assert result["severity"] == Severity.ERROR

    def test_path_traversal_in_detail(self):
        result = classify_sandbox_error("unknown", "Path traversal blocked: ../etc/passwd")
        assert result["classification"] == FailureClassification.PERMISSION_DENIED
        assert result["severity"] == Severity.CRITICAL

    def test_invalid_action_default(self):
        result = classify_sandbox_error("invalid_action", "Unknown action type")
        assert result["classification"] == FailureClassification.TOOL_FAILURE
        assert result["severity"] == Severity.ERROR

    def test_unknown_error_defaults_to_tool_failure(self):
        result = classify_sandbox_error("weird_error", "Something strange happened")
        assert result["classification"] == FailureClassification.TOOL_FAILURE
        assert result["confidence"] == Confidence.MEDIUM

    def test_workspace_escape_classification(self):
        result = classify_sandbox_error("workspace_escape", "Process attempted to write outside workspace")
        assert result["classification"] == FailureClassification.SANDBOX_ESCAPE
        assert result["severity"] == Severity.CRITICAL


# ============================================================================
# SandboxVerifier Unit Tests
# ============================================================================


class TestSandboxVerifier:
    """Tests for the SandboxVerifier class."""

    def setup_method(self):
        self.verifier = SandboxVerifier()
        self.handler = self.verifier.create_handler()

    async def _verify(self, sandbox_result, node_id="test_node_001"):
        """Helper to run verification synchronously."""
        scope = VerificationScope(affected_files=["test.txt"], provenance="test")
        context = {"sandbox_result": sandbox_result}
        return await self.handler(node_id, scope, context)

    def test_successful_sandbox_operation(self):
        """A successful sandbox execution should produce a successful verification."""
        result = asyncio.run(self._verify({
            "status": "success",
            "action": "bash",
            "stdout": "Hello World",
            "stderr": "",
            "exit_code": 0,
        }))
        assert result.success is True
        assert result.verification_type == VerificationType.SANDBOX_VALIDATION
        assert result.severity == Severity.INFO
        assert result.provenance == "sandbox_verifier"
        assert result.node_id == "test_node_001"

    def test_blocked_command_verification(self):
        """A blocked command should produce a critical failure verification."""
        result = asyncio.run(self._verify({
            "status": "error",
            "error_type": "sandbox_denied",
            "error_detail": "Dangerous command blocked: rm -rf /",
            "action": "bash",
            "exit_code": -1,
        }))
        assert result.success is False
        assert result.severity == Severity.CRITICAL
        assert result.retryability == Retryability.NEVER
        assert any("blocked" in d.lower() for d in result.diagnostics)
        assert result.artifacts["failure_classification"] == "permission_denied"

    def test_timeout_verification(self):
        """A timeout should produce an error verification with conditional retry."""
        result = asyncio.run(self._verify({
            "status": "error",
            "error_type": "timeout",
            "error_detail": "Process timed out after 30 seconds",
            "action": "bash",
            "exit_code": -1,
        }))
        assert result.success is False
        assert result.severity == Severity.ERROR
        assert result.retryability == Retryability.CONDITIONAL
        assert any("timeout" in d.lower() for d in result.diagnostics)
        assert result.artifacts["failure_classification"] == "timeout"

    def test_path_traversal_verification(self):
        """Path traversal should be classified as permission_denied."""
        result = asyncio.run(self._verify({
            "status": "error",
            "error_type": "sandbox_denied",
            "error_detail": "Path traversal blocked: ../../etc/passwd resolves outside jail",
            "action": "read_file",
            "exit_code": -1,
        }))
        assert result.success is False
        assert result.severity == Severity.CRITICAL
        assert result.artifacts["failure_classification"] == "permission_denied"
        assert len(result.runtime_implications) > 0
        assert "blocked" in result.runtime_implications[0].lower()

    def test_resource_limit_verification(self):
        """Resource limit should be classified as environment_failure."""
        result = asyncio.run(self._verify({
            "status": "error",
            "error_type": "resource_limit",
            "error_detail": "Memory limit exceeded: 512 MB",
            "action": "bash",
            "exit_code": -1073740790,  # STATUS_PROCESS_IS_TERMINATING
        }))
        assert result.success is False
        assert result.artifacts["failure_classification"] == "environment_failure"
        assert result.retryability == Retryability.CONDITIONAL

    def test_no_result_provided(self):
        """Missing sandbox result should produce a no-data verification."""
        result = asyncio.run(self._verify(None))
        assert result.success is False
        assert result.severity == Severity.WARNING
        assert "no sandbox result" in result.diagnostics[0].lower()

    def test_workspace_escape_verification(self):
        """Workspace escape should produce a SANDBOX_ESCAPE classification."""
        result = asyncio.run(self._verify({
            "status": "error",
            "error_type": "workspace_escape",
            "error_detail": "Process attempted to write to C:\\outside\\file.txt",
            "action": "write_file",
            "exit_code": -1,
        }))
        assert result.success is False
        assert result.severity == Severity.CRITICAL
        assert result.artifacts["failure_classification"] == "sandbox_escape"
        assert any("escape" in d.lower() for d in result.runtime_implications)

    def test_stderr_warning_on_success(self):
        """Success with stderr output should still be success but have diagnostics."""
        result = asyncio.run(self._verify({
            "status": "success",
            "action": "bash",
            "stdout": "output",
            "stderr": "warning: deprecated flag used",
            "exit_code": 0,
        }))
        assert result.success is True
        assert len(result.diagnostics) > 0
        assert "stderr" in result.diagnostics[0].lower()


# ============================================================================
# VerificationPipeline + SandboxVerifier Integration Tests
# ============================================================================


class TestPipelineSandboxIntegration:
    """Tests for SandboxVerifier registered on the VerificationPipeline."""

    def setup_method(self):
        self.pipeline = VerificationPipeline()
        self.verifier = register_sandbox_verifier(self.pipeline)
        self.events = []
        async def capture(event):
            self.events.append(event)
        self.pipeline.on_event(capture)

    def test_pipeline_routes_sandbox_verification(self):
        """Pipeline should correctly route SANDBOX_VALIDATION to the verifier."""
        manifest = VerificationManifest(
            required=[VerificationType.SANDBOX_VALIDATION],
            blocking=[VerificationType.SANDBOX_VALIDATION],
        )
        scope = VerificationScope(affected_files=["secret.txt"])
        context = {
            "sandbox_result": {
                "status": "error",
                "error_type": "sandbox_denied",
                "error_detail": "Blocked: rm -rf",
                "action": "bash",
                "exit_code": -1,
            }
        }

        results = asyncio.run(self.pipeline.verify(
            node_id="test_secure",
            manifest=manifest,
            scope=scope,
            context=context,
        ))

        assert len(results) == 1
        assert results[0].success is False
        assert results[0].verification_type == VerificationType.SANDBOX_VALIDATION

        # Check events were emitted
        assert len(self.events) > 0
        assert any(
            isinstance(e, VerificationFailedEvent)
            and e.node_id == "test_secure"
            for e in self.events
        )

    def test_pipeline_successful_sandbox(self):
        """Successful sandbox operations should emit VerificationCompletedEvent."""
        manifest = VerificationManifest(
            required=[VerificationType.SANDBOX_VALIDATION],
        )
        scope = VerificationScope()
        context = {
            "sandbox_result": {
                "status": "success",
                "action": "read_file",
                "stdout": "file content",
                "stderr": "",
                "exit_code": 0,
            }
        }

        results = asyncio.run(self.pipeline.verify(
            node_id="test_safe",
            manifest=manifest,
            scope=scope,
            context=context,
        ))

        assert len(results) == 1
        assert results[0].success is True
        assert any(
            isinstance(e, VerificationCompletedEvent)
            for e in self.events
        )


# ============================================================================
# RecoveryEngine + Sandbox Verification Integration Tests
# ============================================================================


class TestRecoverySandboxIntegration:
    """Tests for RecoveryEngine handling sandbox verification failures."""

    def setup_method(self):
        self.graph = AsyncMock()
        self.graph.nodes = {}
        self.graph.event_bus = AsyncMock()
        self.engine = RecoveryEngine(self.graph)

    def _make_verification_failed_base_event(
        self, node_id="node_sandbox_fail", diagnostics=None
    ):
        """Create a BaseEvent with type=VERIFICATION_FAILED.

        This matches what the orchestrator produces when it converts
        a VerificationFailedEvent from the pipeline to a runtime BaseEvent
        for the event bus.
        """
        if diagnostics is None:
            diagnostics = [
                "Sandbox bash failed: sandbox_denied",
                "Detail: Dangerous command blocked: rm",
            ]
        return BaseEvent(
            id="vf_event_001",
            type=EventType.VERIFICATION_FAILED,
            payload={
                "node_id": node_id,
                "verification_source": "sandbox",
                "diagnostics": diagnostics,
            },
        )

    def test_recovery_engine_handles_verification_failure_event(self):
        """RecoveryEngine should handle VERIFICATION_FAILED events and trigger recovery."""
        event = self._make_verification_failed_base_event()

        # Mock the graph to have the node
        node_mock = MagicMock()
        node_mock.id = "node_sandbox_fail"
        node_mock.retry_count = 0
        node_mock.retry_budget = 3
        node_mock.description = "Test sandbox operation"
        node_mock.danger = "safe"
        self.graph.nodes = {"node_sandbox_fail": node_mock}
        self.graph.transition_node = AsyncMock()

        # Process the event
        asyncio.run(self.engine.on_event(event))

        # Recovery should have been triggered
        assert self.graph.transition_node.called
        # Should have transitioned to RETRYING or FAILED depending on strategy
        call_args = self.graph.transition_node.call_args
        assert call_args is not None

    def test_recovery_engine_skips_non_verification_events(self):
        """RecoveryEngine should not trigger recovery for non-relevant events."""
        event = BaseEvent(
            id="irrelevant",
            type=EventType.LOG_MESSAGE,
            payload={"msg": "Nothing to see here"},
        )

        # Should process without error and without triggering recovery
        asyncio.run(self.engine.on_event(event))

        # No recovery actions should have been taken
        assert self.engine.recovery_count == 0


# ============================================================================
# Full End-to-End Integration Tests
# ============================================================================


class TestFullSandboxVerificationRecoveryFlow:
    """End-to-end test: sandbox result → verification → recovery.

    These tests replicate the actual runtime flow:
    1. Pipeline verifies a sandbox result → emits VerificationFailedEvent
    2. Orchestrator converts VerificationFailedEvent → BaseEvent(VERIFICATION_FAILED)
    3. RecoveryEngine receives the BaseEvent and triggers recovery
    """

    def test_full_flow_with_conversion(self):
        """Verify the full flow: sandbox error → verification → BaseEvent conversion → recovery."""
        # Step 1: Create pipeline with sandbox verifier
        pipeline = VerificationPipeline()
        register_sandbox_verifier(pipeline)

        # Capture pipeline events for conversion (like the orchestrator does)
        converted_events = []
        async def converter(event):
            """Replicate what the orchestrator's _verification_event_to_bus does."""
            node_id = getattr(event, "node_id", "")
            if isinstance(event, VerificationFailedEvent):
                bus_event = BaseEvent(
                    id=getattr(event, "event_id", ""),
                    type=EventType.VERIFICATION_FAILED,
                    payload={
                        "verification_source": "sandbox",
                        "node_id": node_id,
                    },
                )
                converted_events.append(bus_event)
        pipeline.on_event(converter)

        # Step 2: Create recovery engine with mock graph
        graph = AsyncMock()
        graph.nodes = {}
        graph.transition_node = AsyncMock()
        graph.event_bus = AsyncMock()
        recovery = RecoveryEngine(graph)

        # Step 3: Create a mock node in the graph
        node_mock = MagicMock()
        node_mock.id = "test_full_flow_node"
        node_mock.retry_count = 0
        node_mock.retry_budget = 3
        node_mock.description = "E2E sandbox test"
        node_mock.danger = "safe"
        graph.nodes = {"test_full_flow_node": node_mock}
        graph.transition_node = AsyncMock()

        # Step 4: Run sandbox verification
        manifest = VerificationManifest(
            required=[VerificationType.SANDBOX_VALIDATION],
            blocking=[VerificationType.SANDBOX_VALIDATION],
        )
        scope = VerificationScope(affected_files=["secret.txt"])
        context = {
            "sandbox_result": {
                "status": "error",
                "error_type": "sandbox_denied",
                "error_detail": "Blocked: command not in allowlist",
                "action": "bash",
                "exit_code": -1,
            }
        }

        results = asyncio.run(pipeline.verify(
            node_id="test_full_flow_node",
            manifest=manifest,
            scope=scope,
            context=context,
        ))

        # Step 5: Assert verification failed
        assert len(results) == 1
        assert results[0].success is False
        assert results[0].verification_type == VerificationType.SANDBOX_VALIDATION

        # Step 6: Assert a BaseEvent was converted
        assert len(converted_events) == 1
        assert converted_events[0].type == EventType.VERIFICATION_FAILED

        # Step 7: Feed the converted event to the recovery engine
        asyncio.run(recovery.on_event(converted_events[0]))

        # Step 8: Assert recovery was triggered
        assert graph.transition_node.called

    def test_recovery_after_sandbox_failure(self):
        """Verify the RecoveryEngine handles sandbox verification failures properly."""
        graph = MagicMock()
        graph.nodes = {}
        graph.transition_node = AsyncMock()
        # event_bus.publish is awaited in _handle_with_new_subsystem
        graph.event_bus = AsyncMock()
        graph.event_bus.publish = AsyncMock()
        recovery = RecoveryEngine(graph)

        # Simulate a sandbox failure event (as BaseEvent, matching runtime flow)
        node_mock = MagicMock()
        node_mock.id = "sandbox_node_001"
        node_mock.retry_count = 0
        node_mock.retry_budget = 3
        node_mock.description = "Sandbox E2E"
        graph.nodes = {"sandbox_node_001": node_mock}

        event = BaseEvent(
            id="vf_e2e_001",
            type=EventType.VERIFICATION_FAILED,
            payload={
                "node_id": "sandbox_node_001",
                "verification_source": "sandbox",
                "diagnostics": ["Sandbox failed: timeout", "Process exceeded 30s limit"],
            },
        )

        asyncio.run(recovery.on_event(event))

        # Recovery should have transitioned the node (FAILED, RETRYING, or BLOCKED)
        assert graph.transition_node.called
        call_args = graph.transition_node.call_args
        assert call_args is not None
        # The second positional arg should be a state like FAILED, RETRYING, or BLOCKED
        args, _ = call_args
        assert len(args) >= 2
        # Node should have transitioned to some state
        assert args[1] in ("failed", "retrying", "blocked") or hasattr(args[1], "value")
