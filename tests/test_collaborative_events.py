"""
tests/test_collaborative_events.py — Phase 9: EventBus Integration

Tests the new collaborative/Phase 8 event models:
  1. ArchitectDecisionEvent — fields, defaults, to_summary
  2. ModeSelectionEvent — fields, defaults, to_summary
  3. TaskBoardTransitionEvent — fields, defaults, to_summary
  4. ConsensusEvent — fields, defaults, to_summary
  5. BlackboardPublicationEvent — fields, defaults, to_summary
  6. EventType enum values — all new types present
  7. EventBus publish/subscribe for new event types
  8. EventBus replay type_map for new event types
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from runtime_next.events.bus import EventBus
from runtime_next.models.events import (
    EventType,
    BaseEvent,
    ArchitectDecisionEvent,
    ModeSelectionEvent,
    TaskBoardTransitionEvent,
    ConsensusEvent,
    BlackboardPublicationEvent,
)
import logging

log = logging.getLogger(__name__)



# ===========================================================================
# EventType Enum Tests
# ===========================================================================


class TestEventTypeValues:
    """All new EventType values must be present."""

    def test_architect_decision_type(self):
        assert EventType.ARCHITECT_DECISION == "architect_decision"

    def test_mode_selected_type(self):
        assert EventType.MODE_SELECTED == "mode_selected"

    def test_task_board_transition_type(self):
        assert EventType.TASK_BOARD_TRANSITION == "task_board_transition"

    def test_consensus_formed_type(self):
        assert EventType.CONSENSUS_FORMED == "consensus_formed"

    def test_blackboard_publication_type(self):
        assert EventType.BLACKBOARD_PUBLICATION == "blackboard_publication"

    def test_all_types_unique(self):
        """No duplicate values across all EventTypes."""
        values = [e.value for e in EventType]
        assert len(values) == len(set(values))


# ===========================================================================
# ArchitectDecisionEvent Tests
# ===========================================================================


class TestArchitectDecisionEvent:
    """ArchitectDecisionEvent — emitted when Architect makes a decision."""

    def test_default_type(self):
        event = ArchitectDecisionEvent(
            id="dec_001", type=EventType.ARCHITECT_DECISION,
            decision_id="dec_abc123",
            outcome="approve",
        )
        assert event.type == EventType.ARCHITECT_DECISION

    def test_minimal_fields(self):
        event = ArchitectDecisionEvent(
            id="dec_001", type=EventType.ARCHITECT_DECISION,
        )
        assert event.decision_id == ""
        assert event.outcome == ""
        assert event.target_type == ""
        assert event.reason == ""

    def test_all_fields(self):
        event = ArchitectDecisionEvent(
            id="dec_002",
            type=EventType.ARCHITECT_DECISION,
            decision_id="dec_xyz789",
            outcome="approve",
            target_type="consensus",
            target_id="con_001",
            reason="Strong consensus with high confidence",
            conditions=["tests must pass", "security review required"],
            assigned_to="FORGE",
            overridden_recommendation="reject",
            override_rationale="Task is safe to proceed",
            replan_trigger="tied_consensus",
            replan_scope="partial",
        )
        assert event.decision_id == "dec_xyz789"
        assert event.outcome == "approve"
        assert event.target_type == "consensus"
        assert len(event.conditions) == 2
        assert event.assigned_to == "FORGE"
        assert event.replan_scope == "partial"

    def test_to_summary_approve(self):
        event = ArchitectDecisionEvent(
            id="dec_003", type=EventType.ARCHITECT_DECISION,
            decision_id="dec_abc123456789",
            outcome="approve",
            target_type="plan",
            target_id="plan_001",
            reason="Approved by architect",
        )
        summary = event.to_summary()
        assert summary["outcome"] == "APPROVE"
        assert summary["reason"] == "Approved by architect"
        assert summary["decision_id"] == "dec_abc12345"  # [:12] truncation

    def test_to_summary_reject(self):
        event = ArchitectDecisionEvent(
            id="dec_004", type=EventType.ARCHITECT_DECISION,
            decision_id="dec_reject_001",
            outcome="reject",
            target_type="task",
            target_id="task_001",
            reason="Insufficient security review",
            assigned_to="FORGE",
        )
        summary = event.to_summary()
        assert summary["outcome"] == "REJECT"
        assert summary["assigned_to"] == "FORGE"

    def test_is_base_event(self):
        event = ArchitectDecisionEvent(
            id="dec_005", type=EventType.ARCHITECT_DECISION,
        )
        assert isinstance(event, BaseEvent)
        assert event.id == "dec_005"
        assert hasattr(event, "timestamp")

    def test_serialize_roundtrip(self):
        event = ArchitectDecisionEvent(
            id="dec_006", type=EventType.ARCHITECT_DECISION,
            decision_id="dec_roundtrip",
            outcome="override",
            overridden_recommendation="reject",
            override_rationale="Low confidence consensus",
        )
        data = event.model_dump()
        restored = ArchitectDecisionEvent(**data)
        assert restored.decision_id == "dec_roundtrip"
        assert restored.outcome == "override"
        assert restored.override_rationale == "Low confidence consensus"


# ===========================================================================
# ModeSelectionEvent Tests
# ===========================================================================


class TestModeSelectionEvent:
    """ModeSelectionEvent — emitted when execution mode is selected."""

    def test_default_type(self):
        event = ModeSelectionEvent(
            id="mode_001", type=EventType.MODE_SELECTED,
            mode="consolidated",
        )
        assert event.type == EventType.MODE_SELECTED

    def test_mode_a_fields(self):
        event = ModeSelectionEvent(
            id="mode_002", type=EventType.MODE_SELECTED,
            mode="consolidated",
            rationale="Low risk, low complexity",
            task_preview="fix typo in readme",
            risk_profile="low",
            complexity=1,
            has_explicit_prefix=True,
            triggers=["@MODE_A prefix used"],
        )
        assert event.mode == "consolidated"
        assert event.rationale == "Low risk, low complexity"
        assert event.complexity == 1
        assert event.has_explicit_prefix is True

    def test_mode_b_fields(self):
        event = ModeSelectionEvent(
            id="mode_003", type=EventType.MODE_SELECTED,
            mode="collaborative",
            rationale="High risk + security concerns",
            task_preview="deploy database migration",
            risk_profile="high",
            complexity=6,
            has_explicit_prefix=False,
            triggers=["risk=high", "security_concerns"],
        )
        assert event.mode == "collaborative"
        assert event.risk_profile == "high"
        assert event.complexity == 6
        assert len(event.triggers) == 2

    def test_defaults(self):
        event = ModeSelectionEvent(
            id="mode_004", type=EventType.MODE_SELECTED,
            mode="consolidated",
        )
        assert event.rationale == ""
        assert event.task_preview == ""
        assert event.risk_profile == ""
        assert event.complexity == 0
        assert event.has_explicit_prefix is False
        assert event.triggers == []

    def test_to_summary_mode_a(self):
        event = ModeSelectionEvent(
            id="mode_005", type=EventType.MODE_SELECTED,
            mode="consolidated",
            rationale="Low risk task",
            has_explicit_prefix=False,
        )
        summary = event.to_summary()
        assert "Mode A" in summary["mode"]
        assert summary["explicit"] is False

    def test_to_summary_mode_b(self):
        event = ModeSelectionEvent(
            id="mode_006", type=EventType.MODE_SELECTED,
            mode="collaborative",
            rationale="Security concerns",
            risk_profile="high",
            complexity=7,
        )
        summary = event.to_summary()
        assert "Mode B" in summary["mode"]
        assert summary["risk"] == "high"
        assert summary["complexity"] == 7

    def test_is_base_event(self):
        event = ModeSelectionEvent(
            id="mode_007", type=EventType.MODE_SELECTED,
            mode="consolidated",
        )
        assert isinstance(event, BaseEvent)


# ===========================================================================
# TaskBoardTransitionEvent Tests
# ===========================================================================


class TestTaskBoardTransitionEvent:
    """TaskBoardTransitionEvent — emitted on task state transitions."""

    def test_default_type(self):
        event = TaskBoardTransitionEvent(
            id="task_001", type=EventType.TASK_BOARD_TRANSITION,
            task_id="t1",
        )
        assert event.type == EventType.TASK_BOARD_TRANSITION

    def test_all_fields(self):
        event = TaskBoardTransitionEvent(
            id="task_002", type=EventType.TASK_BOARD_TRANSITION,
            task_id="task_abc123",
            task_type="research",
            from_status="pending",
            to_status="in_progress",
            specialist="ORACLE",
            reason="ORACLE picked up research task",
            session_id="session_001",
        )
        assert event.task_id == "task_abc123"
        assert event.task_type == "research"
        assert event.from_status == "pending"
        assert event.to_status == "in_progress"
        assert event.specialist == "ORACLE"
        assert event.session_id == "session_001"

    def test_to_summary_completed(self):
        event = TaskBoardTransitionEvent(
            id="task_003", type=EventType.TASK_BOARD_TRANSITION,
            task_id="task_complete",
            task_type="implement",
            from_status="reviewing",
            to_status="completed",
            specialist="FORGE",
        )
        summary = event.to_summary()
        assert summary["icon"] == "done"
        assert "implement" in summary["task"]

    def test_to_summary_failed(self):
        event = TaskBoardTransitionEvent(
            id="task_004", type=EventType.TASK_BOARD_TRANSITION,
            task_id="task_fail",
            task_type="security_review",
            from_status="in_progress",
            to_status="failed",
            specialist="SENTINEL",
            reason="Vulnerability found",
        )
        summary = event.to_summary()
        assert summary["icon"] == "err"
        assert summary["reason"] == "Vulnerability found"

    def test_to_summary_blocked(self):
        event = TaskBoardTransitionEvent(
            id="task_005", type=EventType.TASK_BOARD_TRANSITION,
            task_id="task_block",
            task_type="execute",
            from_status="assigned",
            to_status="blocked",
            specialist="TERMINUS",
        )
        summary = event.to_summary()
        assert summary["icon"] == "wait"

    def test_is_base_event(self):
        event = TaskBoardTransitionEvent(
            id="task_006", type=EventType.TASK_BOARD_TRANSITION,
            task_id="t1",
        )
        assert isinstance(event, BaseEvent)


# ===========================================================================
# ConsensusEvent Tests
# ===========================================================================


class TestConsensusEvent:
    """ConsensusEvent — emitted when consensus is formed."""

    def test_default_type(self):
        event = ConsensusEvent(
            id="con_001", type=EventType.CONSENSUS_FORMED,
        )
        assert event.type == EventType.CONSENSUS_FORMED

    def test_all_fields(self):
        event = ConsensusEvent(
            id="con_002", type=EventType.CONSENSUS_FORMED,
            consensus_id="con_abc123",
            target_id="task_001",
            recommendation="Approve implementation",
            confidence=0.85,
            positions={
                "ORACLE": "yes",
                "FORGE": "yes",
                "SENTINEL": "no",
                "HERALD": "yes",
            },
            method="majority",
        )
        assert event.consensus_id == "con_abc123"
        assert event.recommendation == "Approve implementation"
        assert event.confidence == 0.85
        assert len(event.positions) == 4
        assert event.method == "majority"

    def test_defaults(self):
        event = ConsensusEvent(
            id="con_003", type=EventType.CONSENSUS_FORMED,
        )
        assert event.consensus_id == ""
        assert event.target_id == ""
        assert event.confidence == 0.0
        assert event.positions == {}

    def test_to_summary(self):
        event = ConsensusEvent(
            id="con_004", type=EventType.CONSENSUS_FORMED,
            consensus_id="con_xyz789",
            recommendation="Proceed with caution",
            confidence=0.72,
            positions={
                "ORACLE": "yes",
                "FORGE": "yes",
                "SENTINEL": "no",
                "HERALD": "yes",
                "TERMINUS": "yes",
            },
            method="super_majority",
        )
        summary = event.to_summary()
        assert summary["recommendation"] == "Proceed with caution"
        assert summary["confidence"] == "0.72"
        assert summary["for"] == 4
        assert summary["against"] == 1
        assert summary["method"] == "super_majority"

    def test_to_summary_no_positions(self):
        event = ConsensusEvent(
            id="con_005", type=EventType.CONSENSUS_FORMED,
            consensus_id="con_empty",
            recommendation="No consensus needed",
        )
        summary = event.to_summary()
        assert summary["for"] == 0
        assert summary["against"] == 0

    def test_is_base_event(self):
        event = ConsensusEvent(
            id="con_006", type=EventType.CONSENSUS_FORMED,
        )
        assert isinstance(event, BaseEvent)


# ===========================================================================
# BlackboardPublicationEvent Tests
# ===========================================================================


class TestBlackboardPublicationEvent:
    """BlackboardPublicationEvent — emitted when specialist publishes."""

    def test_default_type(self):
        event = BlackboardPublicationEvent(
            id="bb_001", type=EventType.BLACKBOARD_PUBLICATION,
            specialist="ORACLE",
        )
        assert event.type == EventType.BLACKBOARD_PUBLICATION

    def test_all_fields(self):
        event = BlackboardPublicationEvent(
            id="bb_002", type=EventType.BLACKBOARD_PUBLICATION,
            specialist="FORGE",
            entry_type="implementation",
            summary="Implemented login handler",
            tags=["auth", "security", "phase_2"],
            session_id="session_001",
        )
        assert event.specialist == "FORGE"
        assert event.entry_type == "implementation"
        assert event.summary == "Implemented login handler"
        assert len(event.tags) == 3
        assert event.session_id == "session_001"

    def test_defaults(self):
        event = BlackboardPublicationEvent(
            id="bb_003", type=EventType.BLACKBOARD_PUBLICATION,
            specialist="SENTINEL",
        )
        assert event.entry_type == ""
        assert event.summary == ""
        assert event.tags == []
        assert event.session_id == ""

    def test_to_summary(self):
        event = BlackboardPublicationEvent(
            id="bb_004", type=EventType.BLACKBOARD_PUBLICATION,
            specialist="HERMES",
            entry_type="analysis",
            summary="Analyzed user intent and risk profile",
            tags=["cognition", "analysis"],
        )
        summary = event.to_summary()
        assert summary["specialist"] == "HERMES"
        assert summary["entry_type"] == "analysis"
        assert summary["tags"] == 2

    def test_is_base_event(self):
        event = BlackboardPublicationEvent(
            id="bb_005", type=EventType.BLACKBOARD_PUBLICATION,
            specialist="HERALD",
        )
        assert isinstance(event, BaseEvent)

    def test_serialize_roundtrip(self):
        event = BlackboardPublicationEvent(
            id="bb_006", type=EventType.BLACKBOARD_PUBLICATION,
            specialist="ORACLE",
            entry_type="finding",
            summary="Found relevant API documentation",
            tags=["research", "docs"],
            session_id="s1",
        )
        data = event.model_dump()
        restored = BlackboardPublicationEvent(**data)
        assert restored.specialist == "ORACLE"
        assert restored.summary == "Found relevant API documentation"
        assert restored.session_id == "s1"


# ===========================================================================
# EventBus Integration Tests
# ===========================================================================


class TestEventBusCollaborativeEvents:
    """EventBus publish/subscribe for new collaborative event types."""

    @pytest.fixture
    def event_bus(self):
        bus = EventBus()
        return bus

    @pytest.mark.asyncio
    async def test_publish_architect_decision(self, event_bus):
        callback = AsyncMock()
        event_bus.subscribe(EventType.ARCHITECT_DECISION, callback)

        event = ArchitectDecisionEvent(
            id="bus_dec_001", type=EventType.ARCHITECT_DECISION,
            decision_id="dec_bus_001",
            outcome="approve",
            reason="Approved via EventBus",
        )
        await event_bus.publish(event)
        await event_bus._process_events()

        callback.assert_called_once()
        called_event = callback.call_args[0][0]
        assert called_event.decision_id == "dec_bus_001"
        assert called_event.outcome == "approve"

    @pytest.mark.asyncio
    async def test_publish_mode_selection(self, event_bus):
        callback = AsyncMock()
        event_bus.subscribe(EventType.MODE_SELECTED, callback)

        event = ModeSelectionEvent(
            id="bus_mode_001", type=EventType.MODE_SELECTED,
            mode="collaborative",
            rationale="Architect selected Mode B",
            risk_profile="high",
            complexity=5,
        )
        await event_bus.publish(event)
        await event_bus._process_events()

        callback.assert_called_once()
        called_event = callback.call_args[0][0]
        assert called_event.mode == "collaborative"
        assert called_event.risk_profile == "high"

    @pytest.mark.asyncio
    async def test_publish_task_board_transition(self, event_bus):
        callback = AsyncMock()
        event_bus.subscribe(EventType.TASK_BOARD_TRANSITION, callback)

        event = TaskBoardTransitionEvent(
            id="bus_task_001", type=EventType.TASK_BOARD_TRANSITION,
            task_id="task_001",
            task_type="research",
            from_status="pending",
            to_status="assigned",
            specialist="ORACLE",
        )
        await event_bus.publish(event)
        await event_bus._process_events()

        callback.assert_called_once()
        called_event = callback.call_args[0][0]
        assert called_event.task_id == "task_001"
        assert called_event.to_status == "assigned"

    @pytest.mark.asyncio
    async def test_publish_consensus(self, event_bus):
        callback = AsyncMock()
        event_bus.subscribe(EventType.CONSENSUS_FORMED, callback)

        event = ConsensusEvent(
            id="bus_con_001", type=EventType.CONSENSUS_FORMED,
            consensus_id="con_bus_001",
            recommendation="Proceed with implementation",
            confidence=0.9,
            positions={"ORACLE": "yes", "FORGE": "yes"},
            method="unanimous",
        )
        await event_bus.publish(event)
        await event_bus._process_events()

        callback.assert_called_once()
        called_event = callback.call_args[0][0]
        assert called_event.recommendation == "Proceed with implementation"
        assert called_event.method == "unanimous"

    @pytest.mark.asyncio
    async def test_publish_blackboard_publication(self, event_bus):
        callback = AsyncMock()
        event_bus.subscribe(EventType.BLACKBOARD_PUBLICATION, callback)

        event = BlackboardPublicationEvent(
            id="bus_bb_001", type=EventType.BLACKBOARD_PUBLICATION,
            specialist="FORGE",
            entry_type="implementation",
            summary="Created new auth module",
            tags=["auth"],
        )
        await event_bus.publish(event)
        await event_bus._process_events()

        callback.assert_called_once()
        called_event = callback.call_args[0][0]
        assert called_event.specialist == "FORGE"
        assert called_event.entry_type == "implementation"

    @pytest.mark.asyncio
    async def test_global_subscriber_receives_all(self, event_bus):
        """Global subscriber should receive all new event types."""
        callback = AsyncMock()
        event_bus.subscribe_all(callback)

        events = [
            ArchitectDecisionEvent(
                id="g1", type=EventType.ARCHITECT_DECISION,
                decision_id="d1", outcome="approve",
            ),
            ModeSelectionEvent(
                id="g2", type=EventType.MODE_SELECTED,
                mode="collaborative",
            ),
            TaskBoardTransitionEvent(
                id="g3", type=EventType.TASK_BOARD_TRANSITION,
                task_id="t1",
            ),
            ConsensusEvent(
                id="g4", type=EventType.CONSENSUS_FORMED,
            ),
            BlackboardPublicationEvent(
                id="g5", type=EventType.BLACKBOARD_PUBLICATION,
                specialist="ORACLE",
            ),
        ]

        for event in events:
            await event_bus.publish(event)
        await event_bus._process_events()

        assert callback.call_count == 5
        # Verify all 5 event types were received
        received_types = {call[0][0].type for call in callback.call_args_list}
        expected_types = {
            EventType.ARCHITECT_DECISION,
            EventType.MODE_SELECTED,
            EventType.TASK_BOARD_TRANSITION,
            EventType.CONSENSUS_FORMED,
            EventType.BLACKBOARD_PUBLICATION,
        }
        assert received_types == expected_types


# ===========================================================================
# EventBus Replay Tests
# ===========================================================================


class TestEventBusReplayNewTypes:
    """EventBus.replay() should handle the new collaborative event types."""

    @pytest.fixture
    def log_path(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False,
            encoding="utf-8",
        ) as f:
            f.write(
                json.dumps({
                    "id": "r1",
                    "type": "architect_decision",
                    "decision_id": "dec_replay",
                    "outcome": "approve",
                    "reason": "Replayed decision",
                    "timestamp": "2025-01-01T00:00:00",
                    "payload": {},
                }) + "\n"
            )
            f.write(
                json.dumps({
                    "id": "r2",
                    "type": "mode_selected",
                    "mode": "collaborative",
                    "rationale": "Replayed mode",
                    "risk_profile": "high",
                    "complexity": 6,
                    "timestamp": "2025-01-01T00:00:00",
                    "payload": {},
                }) + "\n"
            )
            f.write(
                json.dumps({
                    "id": "r3",
                    "type": "task_board_transition",
                    "task_id": "task_replay",
                    "task_type": "implement",
                    "from_status": "pending",
                    "to_status": "completed",
                    "timestamp": "2025-01-01T00:00:00",
                    "payload": {},
                }) + "\n"
            )
            f.write(
                json.dumps({
                    "id": "r4",
                    "type": "consensus_formed",
                    "consensus_id": "con_replay",
                    "recommendation": "Replayed consensus",
                    "confidence": 0.8,
                    "timestamp": "2025-01-01T00:00:00",
                    "payload": {},
                }) + "\n"
            )
            f.write(
                json.dumps({
                    "id": "r5",
                    "type": "blackboard_publication",
                    "specialist": "FORGE",
                    "entry_type": "implementation",
                    "summary": "Replayed publication",
                    "timestamp": "2025-01-01T00:00:00",
                    "payload": {},
                }) + "\n"
            )
            log_path = f.name

        yield log_path
        try:
            Path(log_path).unlink(missing_ok=True)
        except Exception as _ex:
            log.warning("Silenced exception: %s", _ex)

    @pytest.mark.asyncio
    async def test_replay_architect_decision(self, log_path):
        bus = EventBus()
        callback = AsyncMock()
        await bus.replay(log_path, callback)

        # Find the architect_decision event
        for call_args in callback.call_args_list:
            event = call_args[0][0]
            if event.type == EventType.ARCHITECT_DECISION:
                assert isinstance(event, ArchitectDecisionEvent)
                assert event.decision_id == "dec_replay"
                assert event.outcome == "approve"
                break
        else:
            pytest.fail("ArchitectDecisionEvent not replayed")

    @pytest.mark.asyncio
    async def test_replay_mode_selection(self, log_path):
        bus = EventBus()
        callback = AsyncMock()
        await bus.replay(log_path, callback)

        for call_args in callback.call_args_list:
            event = call_args[0][0]
            if event.type == EventType.MODE_SELECTED:
                assert isinstance(event, ModeSelectionEvent)
                assert event.mode == "collaborative"
                assert event.risk_profile == "high"
                break
        else:
            pytest.fail("ModeSelectionEvent not replayed")

    @pytest.mark.asyncio
    async def test_replay_task_board_transition(self, log_path):
        bus = EventBus()
        callback = AsyncMock()
        await bus.replay(log_path, callback)

        for call_args in callback.call_args_list:
            event = call_args[0][0]
            if event.type == EventType.TASK_BOARD_TRANSITION:
                assert isinstance(event, TaskBoardTransitionEvent)
                assert event.task_id == "task_replay"
                assert event.task_type == "implement"
                assert event.from_status == "pending"
                assert event.to_status == "completed"
                break
        else:
            pytest.fail("TaskBoardTransitionEvent not replayed")

    @pytest.mark.asyncio
    async def test_replay_consensus(self, log_path):
        bus = EventBus()
        callback = AsyncMock()
        await bus.replay(log_path, callback)

        for call_args in callback.call_args_list:
            event = call_args[0][0]
            if event.type == EventType.CONSENSUS_FORMED:
                assert isinstance(event, ConsensusEvent)
                assert event.consensus_id == "con_replay"
                assert event.confidence == 0.8
                break
        else:
            pytest.fail("ConsensusEvent not replayed")

    @pytest.mark.asyncio
    async def test_replay_blackboard_publication(self, log_path):
        bus = EventBus()
        callback = AsyncMock()
        await bus.replay(log_path, callback)

        for call_args in callback.call_args_list:
            event = call_args[0][0]
            if event.type == EventType.BLACKBOARD_PUBLICATION:
                assert isinstance(event, BlackboardPublicationEvent)
                assert event.specialist == "FORGE"
                assert event.entry_type == "implementation"
                assert event.summary == "Replayed publication"
                break
        else:
            pytest.fail("BlackboardPublicationEvent not replayed")

    @pytest.mark.asyncio
    async def test_replay_all_new_types(self, log_path):
        bus = EventBus()
        callback = AsyncMock()
        await bus.replay(log_path, callback)

        received_types = {call[0][0].type for call in callback.call_args_list}
        expected_types = {
            EventType.ARCHITECT_DECISION,
            EventType.MODE_SELECTED,
            EventType.TASK_BOARD_TRANSITION,
            EventType.CONSENSUS_FORMED,
            EventType.BLACKBOARD_PUBLICATION,
        }
        # Should contain all new types
        assert expected_types.issubset(received_types)
