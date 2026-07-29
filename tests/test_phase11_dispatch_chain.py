"""
test_phase11_dispatch_chain.py -- Verify Phase 11 UI Event Dispatch Chain

Tests the complete dispatch chain:
    bridge.emit_event() -> dispatcher.dispatch() -> widget.handle_ui_event()

This runs without Textual (widget rendering) to verify the dispatch
logic is correct.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List

from ui.core.ui_event import UIEvent, UIEventType
from ui.core.ui_dispatcher import UIEventDispatcher


# -- Mock Widgets -------------------------------------------------------

class MockConversationFeed:
    def __init__(self):
        self.events: List[UIEvent] = []
    def handle_ui_event(self, event: UIEvent) -> None:
        self.events.append(event)


class MockOmegaOverview:
    def __init__(self):
        self.events: List[UIEvent] = []
        self.overview = None
        self.agent_profiles = {}
        self.agents = {}
    def handle_ui_event(self, event: UIEvent) -> None:
        self.events.append(event)
        if event.type == UIEventType.OVERVIEW_UPDATED:
            self.overview = event.data.get("overview")
            if event.data.get("agent_profiles"):
                self.agent_profiles = event.data["agent_profiles"]
        elif event.type in (UIEventType.SPECIALIST_ACTIVATED,
                           UIEventType.SPECIALIST_DEACTIVATED,
                           UIEventType.SPECIALIST_THINKING,
                           UIEventType.SPECIALIST_ACTION):
            self.agents[event.specialist] = {
                "state": event.data.get("state", "active"),
                "action": event.action,
            }


class MockWorkQueue:
    def __init__(self):
        self.snapshot_data = {}
    def handle_ui_event(self, event: UIEvent) -> None:
        if event.type == UIEventType.WORK_QUEUE_UPDATED:
            self.snapshot_data = event.data


class MockConsensusPanel:
    def __init__(self):
        self.data = {}
    def handle_ui_event(self, event: UIEvent) -> None:
        if event.type == UIEventType.CONSENSUS_UPDATED:
            self.data = event.data


class MockRecoveryPanel:
    def __init__(self):
        self.data = {}
    def handle_ui_event(self, event: UIEvent) -> None:
        if event.type == UIEventType.RECOVERY_UPDATED:
            self.data = event.data


class MockHeaderBar:
    def __init__(self):
        self.current_task = ""
        self.status = ""
    def handle_ui_event(self, event: UIEvent) -> None:
        if event.type == UIEventType.TASK_STARTED:
            self.current_task = event.data.get("task_name", "")
        elif event.type == UIEventType.SYSTEM_ONLINE:
            self.status = event.data.get("status", "READY")


# -- Tests --------------------------------------------------------------

def test_dispatcher_basics():
    d = UIEventDispatcher()
    received = []
    def handler(ev):
        received.append(ev)
    d.subscribe(UIEventType.FINDING_PUBLISHED, handler)
    d.dispatch(UIEvent(type=UIEventType.FINDING_PUBLISHED, specialist="ORACLE", action="test"))
    assert len(received) == 1
    assert received[0].action == "test"


def test_dispatcher_error_isolation():
    d = UIEventDispatcher()
    good = []
    def bad(ev):
        raise RuntimeError("Boom")
    def good_handler(ev):
        good.append(ev)
    d.subscribe(UIEventType.FINDING_PUBLISHED, bad)
    d.subscribe(UIEventType.FINDING_PUBLISHED, good_handler)
    d.dispatch(UIEvent(type=UIEventType.FINDING_PUBLISHED))
    assert len(good) == 1


def test_dispatcher_multiple_types():
    d = UIEventDispatcher()
    finding_events = []
    task_events = []
    d.subscribe(UIEventType.FINDING_PUBLISHED, lambda e: finding_events.append(e))
    d.subscribe(UIEventType.TASK_CREATED, lambda e: task_events.append(e))
    d.dispatch(UIEvent(type=UIEventType.FINDING_PUBLISHED))
    d.dispatch(UIEvent(type=UIEventType.TASK_CREATED))
    d.dispatch(UIEvent(type=UIEventType.FINDING_PUBLISHED))
    assert len(finding_events) == 2
    assert len(task_events) == 1


def test_subscribe_all():
    d = UIEventDispatcher()
    count = []
    def handler(ev):
        count.append(1)
    d.subscribe_all([UIEventType.TASK_CREATED, UIEventType.TASK_COMPLETED], handler)
    d.dispatch(UIEvent(type=UIEventType.TASK_CREATED))
    d.dispatch(UIEvent(type=UIEventType.TASK_COMPLETED))
    assert len(count) == 2


def test_unsubscribe():
    d = UIEventDispatcher()
    received = []
    def handler(ev):
        received.append(ev)
    d.subscribe(UIEventType.FINDING_PUBLISHED, handler)
    d.dispatch(UIEvent(type=UIEventType.FINDING_PUBLISHED))
    assert len(received) == 1
    d.unsubscribe(UIEventType.FINDING_PUBLISHED, handler)
    d.dispatch(UIEvent(type=UIEventType.FINDING_PUBLISHED))
    assert len(received) == 1


def test_clear():
    d = UIEventDispatcher()
    received = []
    def handler(ev):
        received.append(ev)
    d.subscribe(UIEventType.TASK_CREATED, handler)
    d.subscribe(UIEventType.TASK_FAILED, handler)
    assert d.subscriber_count == 2
    d.clear()
    assert d.subscriber_count == 0
    d.dispatch(UIEvent(type=UIEventType.TASK_CREATED))
    assert len(received) == 0


def test_conversation_feed_receives_all_event_types():
    dispatcher = UIEventDispatcher()
    feed = MockConversationFeed()
    feed_types = [
        UIEventType.FINDING_PUBLISHED, UIEventType.EVIDENCE_CONSUMED, UIEventType.CHALLENGE_RAISED,
        UIEventType.CONSENSUS_OUTCOME, UIEventType.DECISION_APPROVED, UIEventType.EXECUTION_STARTED,
        UIEventType.TASK_CREATED, UIEventType.TASK_ASSIGNED, UIEventType.TASK_COMPLETED,
        UIEventType.SPECIALIST_ACTIVATED, UIEventType.TOOL_STARTED, UIEventType.VERIFICATION_PASSED,
        UIEventType.REPORT_GENERATED, UIEventType.RECOVERY_SUCCEEDED,
        UIEventType.SYSTEM_ONLINE, UIEventType.USER_MESSAGE,
    ]
    for etype in feed_types:
        dispatcher.subscribe(etype, feed.handle_ui_event)
    for i, etype in enumerate(feed_types):
        dispatcher.dispatch(UIEvent(type=etype, action=f"event_{i}"))
    assert len(feed.events) == len(feed_types)


def test_overview_updated():
    dispatcher = UIEventDispatcher()
    overview = MockOmegaOverview()
    dispatcher.subscribe(UIEventType.OVERVIEW_UPDATED, overview.handle_ui_event)
    dispatcher.dispatch(UIEvent(
        type=UIEventType.OVERVIEW_UPDATED,
        data={"overview": {"session_state": "active"}, "agent_profiles": {"ORACLE": "mock"}},
    ))
    assert overview.overview is not None
    assert overview.agent_profiles.get("ORACLE") == "mock"


def test_specialist_updates_overview():
    dispatcher = UIEventDispatcher()
    overview = MockOmegaOverview()
    dispatcher.subscribe(UIEventType.SPECIALIST_ACTION, overview.handle_ui_event)
    dispatcher.dispatch(UIEvent(
        type=UIEventType.SPECIALIST_ACTION, specialist="ORACLE",
        action="analyzing", data={"state": "acting", "score": 0.85},
    ))
    assert overview.agents.get("ORACLE") is not None
    assert overview.agents["ORACLE"]["state"] == "acting"


def test_work_queue_panel():
    dispatcher = UIEventDispatcher()
    wq = MockWorkQueue()
    dispatcher.subscribe(UIEventType.WORK_QUEUE_UPDATED, wq.handle_ui_event)
    dispatcher.dispatch(UIEvent(
        type=UIEventType.WORK_QUEUE_UPDATED,
        data={"active": [{"title": "Test", "status": "running"}]},
    ))
    assert wq.snapshot_data["active"][0]["title"] == "Test"


def test_consensus_panel():
    dispatcher = UIEventDispatcher()
    cp = MockConsensusPanel()
    dispatcher.subscribe(UIEventType.CONSENSUS_UPDATED, cp.handle_ui_event)
    dispatcher.dispatch(UIEvent(
        type=UIEventType.CONSENSUS_UPDATED,
        data={"active": [{"topic": "Deploy", "positions": []}]},
    ))
    assert cp.data["active"][0]["topic"] == "Deploy"


def test_recovery_panel():
    dispatcher = UIEventDispatcher()
    rp = MockRecoveryPanel()
    dispatcher.subscribe(UIEventType.RECOVERY_UPDATED, rp.handle_ui_event)
    dispatcher.dispatch(UIEvent(
        type=UIEventType.RECOVERY_UPDATED,
        data={"recent": [{"event_type": "recovery_successful"}], "summary": {}},
    ))
    assert rp.data["recent"][0]["event_type"] == "recovery_successful"


def test_header_bar():
    dispatcher = UIEventDispatcher()
    header = MockHeaderBar()
    dispatcher.subscribe(UIEventType.TASK_STARTED, header.handle_ui_event)
    dispatcher.subscribe(UIEventType.SYSTEM_ONLINE, header.handle_ui_event)
    dispatcher.dispatch(UIEvent(type=UIEventType.TASK_STARTED, action="Scan",
                                data={"task_id": "t1", "task_name": "Scan repo"}))
    dispatcher.dispatch(UIEvent(type=UIEventType.SYSTEM_ONLINE, data={"status": "ACTIVE"}))
    assert header.current_task == "Scan repo"
    assert header.status == "ACTIVE"


def test_ui_event_properties():
    ev = UIEvent(type=UIEventType.FINDING_PUBLISHED, specialist="ORACLE", action="Found 3 vulns")
    assert ev.icon
    assert ev.color
    assert "ORACLE" in ev.to_display_line()
    assert "Found 3 vulns" in ev.to_display_line()


def test_ui_event_defaults():
    ev = UIEvent(type=UIEventType.SYSTEM_ONLINE)
    assert ev.action == ""
    assert ev.source == ""
    assert ev.specialist == ""
    assert ev.data == {}


def test_full_dispatch_chain():
    """Simulate the full bridge -> dispatcher -> widget chain."""
    dispatcher = UIEventDispatcher()
    feed = MockConversationFeed()
    overview = MockOmegaOverview()
    wq = MockWorkQueue()
    cp = MockConsensusPanel()
    rp = MockRecoveryPanel()
    header = MockHeaderBar()

    # Wire like app.py does
    for etype in [UIEventType.FINDING_PUBLISHED, UIEventType.EVIDENCE_CONSUMED,
                  UIEventType.CHALLENGE_RAISED, UIEventType.CONSENSUS_OUTCOME,
                  UIEventType.EXECUTION_STARTED, UIEventType.TASK_CREATED,
                  UIEventType.TASK_ASSIGNED, UIEventType.REPORT_GENERATED,
                  UIEventType.HERALD_NARRATIVE, UIEventType.RECOVERY_SUCCEEDED,
                  UIEventType.SYSTEM_ONLINE]:
        dispatcher.subscribe(etype, feed.handle_ui_event)

    for etype in [UIEventType.OVERVIEW_UPDATED, UIEventType.AGENT_METRICS_UPDATED,
                  UIEventType.TASK_CREATED, UIEventType.TASK_ASSIGNED,
                  UIEventType.SPECIALIST_ACTIVATED, UIEventType.FINDING_PUBLISHED,
                  UIEventType.EXECUTION_STARTED]:
        dispatcher.subscribe(etype, overview.handle_ui_event)

    dispatcher.subscribe(UIEventType.WORK_QUEUE_UPDATED, wq.handle_ui_event)
    dispatcher.subscribe(UIEventType.CONSENSUS_UPDATED, cp.handle_ui_event)
    dispatcher.subscribe(UIEventType.RECOVERY_UPDATED, rp.handle_ui_event)
    dispatcher.subscribe(UIEventType.TASK_STARTED, header.handle_ui_event)
    dispatcher.subscribe(UIEventType.SYSTEM_ONLINE, header.handle_ui_event)

    # Simulate bridge emit events
    events = [
        UIEvent(type=UIEventType.TASK_CREATED, source="orchestrator", action="Scan repo",
                data={"task_id": "t1", "task_name": "Scan repo"}),
        UIEvent(type=UIEventType.TASK_STARTED, source="orchestrator", specialist="ORACLE",
                action="Scan repo", data={"task_id": "t1", "task_name": "Scan repo"}),
        UIEvent(type=UIEventType.FINDING_PUBLISHED, source="blackboard", specialist="ORACLE",
                action="Found 3 vulns", data={"confidence": 0.92}),
        UIEvent(type=UIEventType.SPECIALIST_ACTIVATED, source="orchestrator", specialist="FORGE",
                action="implementing", data={"state": "active", "score": 0.85}),
        UIEvent(type=UIEventType.WORK_QUEUE_UPDATED, source="orchestrator",
                data={"active": [{"title": "Scan repo", "status": "running"}]}),
        UIEvent(type=UIEventType.OVERVIEW_UPDATED, source="orchestrator",
                data={"overview": {"session_state": "active"}}),
        UIEvent(type=UIEventType.CONSENSUS_UPDATED, source="consensus",
                data={"active": [{"topic": "Deploy", "positions": []}]}),
        UIEvent(type=UIEventType.RECOVERY_UPDATED, source="recovery",
                data={"recent": [{"event_type": "recovery_successful"}], "summary": {}}),
        UIEvent(type=UIEventType.EXECUTION_STARTED, source="terminus", specialist="TERMINUS",
                action="npm run build", data={"status": "running"}),
        UIEvent(type=UIEventType.REPORT_GENERATED, source="herald", specialist="HERALD",
                action="Summary", data={"evidence_count": 3}),
        UIEvent(type=UIEventType.SYSTEM_ONLINE, source="system", action="online",
                data={"status": "ACTIVE"}),
    ]

    for ev in events:
        dispatcher.dispatch(ev)

    assert header.current_task == "Scan repo"
    assert header.status == "ACTIVE"
    assert len(wq.snapshot_data.get("active", [])) == 1
    assert cp.data["active"][0]["topic"] == "Deploy"
    assert rp.data["recent"][0]["event_type"] == "recovery_successful"
    assert overview.overview is not None
    assert len(overview.agents) > 0  # FORGE was activated
    assert len(feed.events) > 0


if __name__ == "__main__":
    tests = [
        test_dispatcher_basics,
        test_dispatcher_error_isolation,
        test_dispatcher_multiple_types,
        test_subscribe_all,
        test_unsubscribe,
        test_clear,
        test_conversation_feed_receives_all_event_types,
        test_overview_updated,
        test_specialist_updates_overview,
        test_work_queue_panel,
        test_consensus_panel,
        test_recovery_panel,
        test_header_bar,
        test_ui_event_properties,
        test_ui_event_defaults,
        test_full_dispatch_chain,
    ]

    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1

    print()
    print("=" * 50)
    print(f"  {passed} passed, {failed} failed")
    print("=" * 50)
    sys.exit(0 if failed == 0 else 1)
