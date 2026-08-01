import asyncio
import logging
import time
from typing import Optional, Callable
from ui.events import EventBus, Event, EventType, get_event_bus
from ui.events.event_factory import create_collaboration_event

log = logging.getLogger("aelvo.ui.bridge")


from ui.models.system_overview import SystemOverviewAggregator
from ui.models.agent_status import AgentStatusTracker
from ui.models.work_queue import WorkQueueTracker
from ui.models.consensus_visibility import ConsensusVisibilityTracker
from ui.models.recovery_tracker import RecoveryTracker
from ui.models.herald_narrative import HeraldNarrativeEngine
from ui.core.ui_event import UIEvent, UIEventType
from ui.core.ui_dispatcher import UIEventDispatcher
# Phase 10: Agent analytics — guarded import to avoid breaking the TUI
# when the learning subsystem is not initialized.
_AgentMetricsTracker = None
try:
    from learning.agent_metrics import AgentMetricsTracker as _AgentMetricsTracker
except ImportError as _ex:
    log.warning("Silenced exception: %s", _ex)

# Lazy helper to create an AgentMetricsTracker if available
_AGENT_METRICS_TRACKER_INSTANCE = None
def _get_agent_metrics():
    global _AGENT_METRICS_TRACKER_INSTANCE
    if _AGENT_METRICS_TRACKER_INSTANCE is None and _AgentMetricsTracker is not None:
        _AGENT_METRICS_TRACKER_INSTANCE = _AgentMetricsTracker()
    return _AGENT_METRICS_TRACKER_INSTANCE


class UIBridge:
    def __init__(self, event_bus: Optional[EventBus] = None):
        self.bus = event_bus or get_event_bus()
        self._on_user_input: Optional[Callable] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        # Phase 1: Real-time system overview aggregation
        self.overview = SystemOverviewAggregator()
        # Phase 2: Per-agent status tracking
        self.agent_tracker = AgentStatusTracker()
        # Phase 5: Live work queue tracking
        self.work_queue = WorkQueueTracker()
        # Phase 7: Consensus visibility tracking
        self.consensus_tracker = ConsensusVisibilityTracker()
        # Phase 8: Recovery event tracking
        self.recovery_tracker = RecoveryTracker()
        # Phase 9: Herald narrative engine
        self.herald_narrative = HeraldNarrativeEngine()
        # Phase 9: Narrative throttle — generate at most once per 10s
        self._last_narrative_time: float = 0.0
        self._narrative_throttle: float = 10.0
        # Phase 10: Agent analytics (specialist-specific metrics)
        self._agent_metrics = None
        # Phase 11: UI Event dispatcher — routes UIEvents to widgets
        self.dispatcher = UIEventDispatcher()
        # Phase 3+11: Deduplicate task_assigned events per unique task
        self._assigned_task_ids: set = set()

    def emit_event(self, etype: UIEventType, source: str = "", specialist: str = "",
                   action: str = "", data: dict = None) -> None:
        """Create and dispatch a UIEvent.

        This is the single point of emission for all visible actions.
        Widgets subscribe to UIEventType values through the dispatcher
        and handle events in their handle_ui_event() methods.
        """
        event = UIEvent(
            type=etype,
            source=source,
            specialist=specialist,
            action=action,
            data=data or {},
        )
        self.dispatcher.dispatch(event)

    def _push_overview(self) -> None:
        """Push the latest SystemOverview, AgentStatus, and metrics as UIEvents."""
        self.emit_event(
            UIEventType.OVERVIEW_UPDATED,
            source="orchestrator",
            data={
                "overview": self.overview.snapshot(),
                "agent_profiles": {
                    a.name: a for a in self.agent_tracker.get_ordered()
                },
            },
        )
        # Phase 10: Push agent analytics
        metrics = _get_agent_metrics()
        if metrics is not None:
            self.emit_event(
                UIEventType.AGENT_METRICS_UPDATED,
                source="orchestrator",
                data={"metrics_report": metrics.generate_report()},
            )

    def on_user_input(self, callback: Callable) -> None:
        self._on_user_input = callback

    def send_user_input(self, text: str) -> None:
        if self._on_user_input:
            self._on_user_input(text)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._subscribe()
        await self.bus.start()
        log.info("UI Bridge started")

    async def stop(self) -> None:
        self._running = False
        await self.bus.stop()
        log.info("UI Bridge stopped")

    def _subscribe(self) -> None:
        events = {
            EventType.TASK_CREATED: self._on_task_event,
            EventType.TASK_STARTED: self._on_task_event,
            EventType.TASK_COMPLETED: self._on_task_event,
            EventType.TASK_FAILED: self._on_task_event,
            EventType.TASK_BLOCKED: self._on_task_event,
            EventType.TASK_CANCELLED: self._on_task_event,
            EventType.TASK_PROGRESS: self._on_task_event,
            EventType.SPECIALIST_ACTIVATED: self._on_specialist_event,
            EventType.SPECIALIST_DEACTIVATED: self._on_specialist_event,
            EventType.SPECIALIST_THINKING: self._on_specialist_event,
            EventType.SPECIALIST_ACTION: self._on_specialist_event,
            EventType.TOOL_STARTED: self._on_tool_event,
            EventType.TOOL_COMPLETED: self._on_tool_event,
            EventType.TOOL_FAILED: self._on_tool_event,
            EventType.VERIFICATION_STARTED: self._on_verification_event,
            EventType.VERIFICATION_PASSED: self._on_verification_event,
            EventType.VERIFICATION_FAILED: self._on_verification_event,
            EventType.SYSTEM_STARTUP: self._on_system_event,
            EventType.SYSTEM_SHUTDOWN: self._on_system_event,
            # Collaboration / Mode B events
            EventType.COLLABORATION_FINDING: self._on_collaboration_finding,
            EventType.COLLABORATION_CONSUMED: self._on_collaboration_consumed,
            EventType.COLLABORATION_CHALLENGE: self._on_collaboration_challenge,
            EventType.COLLABORATION_CONSENSUS: self._on_collaboration_consensus,
            EventType.COLLABORATION_DECISION: self._on_collaboration_decision,
            EventType.COLLABORATION_EXECUTION_START: self._on_collaboration_execution,
            EventType.COLLABORATION_EXECUTION_END: self._on_collaboration_execution,
            EventType.COLLABORATION_REPORT: self._on_collaboration_report,
            # Phase 8: Recovery events bridge
            EventType.RECOVERY_INITIATED: self._on_recovery_event,
            EventType.RECOVERY_COMPLETED: self._on_recovery_event,
            EventType.RECOVERY_FAILED: self._on_recovery_event,
        }
        for etype, handler in events.items():
            self.bus.subscribe(etype, handler)

    async def _on_task_event(self, event: Event) -> None:
        data = event.data
        task_id = data.get("task_id", "")
        task_name = data.get("task_name", "")
        specialist = data.get("specialist", "")
        status = data.get("status", "pending")
        progress = data.get("progress", 0.0)

        # Phase 1: Update system overview
        self.overview.on_task_event(task_id, task_name, status, progress)
        # Phase 2: Update agent task tracking
        self.agent_tracker.on_task_event(task_id, task_name, status, specialist)
        # Phase 5: Update work queue
        priority = data.get("priority", "medium")
        task_type = data.get("task_type", "general")
        depends_on = data.get("depends_on", None)
        error = data.get("error", "")
        self.work_queue.on_task_event(
            task_id, task_name, status, progress,
            specialist=specialist, priority=priority,
            task_type=task_type, depends_on=depends_on, error=error,
        )
        self._push_overview()

        # Phase 11: Emit UIEvent for the task lifecycle
        status_lower = status.lower()

        # Deduplicate task_assigned events (emit only once per task)
        is_assigned = specialist and task_name and status_lower not in ("completed", "failed", "blocked", "cancelled")
        if is_assigned and task_id and task_id not in self._assigned_task_ids:
            self._assigned_task_ids.add(task_id)
            self.emit_event(UIEventType.TASK_ASSIGNED, source="orchestrator", specialist=specialist,
                            action=task_name,
                            data={"task_id": task_id, "task_name": task_name,
                                  "assignee": specialist, "status": status})

        # Map status to UIEventType
        if status_lower == "created":
            etype = UIEventType.TASK_CREATED
        elif status_lower == "assigned":
            etype = None  # Already emitted above
        elif status_lower in ("running", "processing"):
            etype = UIEventType.TASK_STARTED
        elif status_lower == "completed":
            etype = UIEventType.TASK_COMPLETED
        elif status_lower == "failed":
            etype = UIEventType.TASK_FAILED
        elif status_lower == "blocked":
            etype = UIEventType.TASK_BLOCKED
        elif status_lower == "cancelled":
            etype = UIEventType.TASK_CANCELLED
        else:
            etype = UIEventType.TASK_PROGRESS

        if etype is not None:
            self.emit_event(etype, source="orchestrator", specialist=specialist,
                            action=task_name,
                            data={
                                "task_id": task_id,
                                "task_name": task_name,
                                "status": status,
                                "progress": progress,
                                "priority": priority,
                            })

        # Push work queue snapshot (always, regardless of event type)
        self.emit_event(UIEventType.WORK_QUEUE_UPDATED, source="orchestrator",
                        data=self.work_queue.snapshot())

    async def _on_specialist_event(self, event: Event) -> None:
        data = event.data
        specialist = data.get("specialist", "")
        action = data.get("action", "")
        score = data.get("score", 0.0)
        task_id = data.get("task_id", "")

        state_map = {
            EventType.SPECIALIST_ACTIVATED: "active",
            EventType.SPECIALIST_DEACTIVATED: "inactive",
            EventType.SPECIALIST_THINKING: "thinking",
            EventType.SPECIALIST_ACTION: "acting",
        }
        state = state_map.get(event.event_type, "active")

        # Phase 1: Update system overview
        self.overview.on_specialist_event(specialist, state, action, score)
        # Phase 2: Update agent status tracking
        self.agent_tracker.on_specialist_event(specialist, state, action, score)
        # Phase 5: Update work queue with specialist confidence
        self.work_queue.on_specialist_event(specialist, task_id=task_id, confidence=score)
        self._push_overview()

        # Phase 11: Emit specialist lifecycle event
        ui_type_map = {
            EventType.SPECIALIST_ACTIVATED: UIEventType.SPECIALIST_ACTIVATED,
            EventType.SPECIALIST_DEACTIVATED: UIEventType.SPECIALIST_DEACTIVATED,
            EventType.SPECIALIST_THINKING: UIEventType.SPECIALIST_THINKING,
            EventType.SPECIALIST_ACTION: UIEventType.SPECIALIST_ACTION,
        }
        ui_type = ui_type_map.get(event.event_type, UIEventType.SPECIALIST_ACTION)
        self.emit_event(ui_type, source="orchestrator", specialist=specialist,
                        action=action,
                        data={"state": state, "score": score, "task_id": task_id})

        # Push work queue snapshot
        self.emit_event(UIEventType.WORK_QUEUE_UPDATED, source="orchestrator",
                        data=self.work_queue.snapshot())

    async def _on_tool_event(self, event: Event) -> None:
        data = event.data
        tool = data.get("tool_name", "")
        command = data.get("command", "")
        status = data.get("status", "running")
        exit_code = data.get("exit_code")
        duration = data.get("duration")

        # Phase 11: Emit tool event
        ui_type_map = {
            "started": UIEventType.TOOL_STARTED,
            "running": UIEventType.TOOL_STARTED,
            "completed": UIEventType.TOOL_COMPLETED,
            "failed": UIEventType.TOOL_FAILED,
        }
        ui_type = ui_type_map.get(status, UIEventType.TOOL_STARTED)
        msg = f"{tool}: {command[:40]}"
        if status == "completed":
            msg += " [success]"
        elif status == "failed":
            msg += " [failed]"
        self.emit_event(ui_type, source="terminus", specialist="TERMINUS",
                        action=msg,
                        data={"tool_name": tool, "command": command, "status": status,
                              "exit_code": exit_code, "duration": duration})

    async def _on_verification_event(self, event: Event) -> None:
        data = event.data
        vtype = data.get("verification_type", "")
        target = data.get("target", "")
        status = data.get("status", "pending")
        confidence = data.get("confidence", 0.0)
        details = data.get("details", "")

        # Phase 1: Update system overview
        self.overview.on_verification_event(vtype, target, status, confidence)
        self._push_overview()

        # Phase 11: Emit verification event
        ui_type_map = {
            "passed": UIEventType.VERIFICATION_PASSED,
            "failed": UIEventType.VERIFICATION_FAILED,
            "running": UIEventType.VERIFICATION_RUNNING,
            "pending": UIEventType.VERIFICATION_RUNNING,
        }
        ui_type = ui_type_map.get(status, UIEventType.VERIFICATION_RUNNING)
        self.emit_event(ui_type, source="sentinel", specialist="SENTINEL",
                        action=f"{vtype} on {target[:30]} [{status}]",
                        data={"verification_type": vtype, "target": target,
                              "status": status, "confidence": confidence, "details": details})

    async def _on_system_event(self, event: Event) -> None:
        msg = event.data.get("message", "")
        # Phase 1: Update system overview
        self.overview.on_system_event(msg)
        self._push_overview()

        # Phase 11: Emit system event
        ui_type = event.event_type
        etype = UIEventType.SYSTEM_ONLINE
        if ui_type.value == "system_shutdown":
            etype = UIEventType.SYSTEM_ERROR
        self.emit_event(etype, source="system", action=msg)

    # ── Collaboration Event Handlers ────────────────────────────

    async def _on_collaboration_finding(self, event: Event) -> None:
        data = event.data
        specialist = data.get("specialist", "")
        action = data.get("action", "")
        entry_type = data.get("entry_type", "finding")
        confidence = data.get("confidence", 0.0)

        # Phase 6: Extract trust metadata from event data
        source = data.get("source", "")
        verification_status = data.get("verification_status", "pending")
        challenged = data.get("challenged", False)
        challenge_count = data.get("challenge_count", 0)
        affected_files = data.get("affected_files", None)
        lifecycle_status = data.get("lifecycle_status", "created")
        finding_timestamp = data.get("timestamp", 0.0)

        # Phase 2: Track agent findings
        self.agent_tracker.on_collaboration_finding(specialist, action, entry_type, confidence)
        # Phase 10: Track Oracle/Forge/Sentinel metrics (guarded)
        metrics = _get_agent_metrics()
        if metrics is not None and specialist:
            spec_upper = specialist.upper()
            if spec_upper == "ORACLE":
                metrics.record_oracle_finding(
                    consumed=False,
                    challenged="challenged" in action.lower() or challenge_count > 0,
                )
            elif spec_upper == "FORGE":
                metrics.record_forge_implementation(
                    success=verification_status == "verified",
                    approved=verification_status == "verified",
                    revision="revision" in action.lower(),
                )
            elif spec_upper == "SENTINEL":
                metrics.record_sentinel_review(
                    challenged=challenge_count > 0,
                    approved=verification_status == "verified",
                )

        # Phase 11: Emit finding published event — widgets handle display via handle_ui_event
        trust_data = {
            "source": source,
            "verification_status": verification_status,
            "challenged": challenged,
            "challenge_count": challenge_count,
            "affected_files": affected_files,
            "lifecycle_status": lifecycle_status,
            "timestamp": finding_timestamp or time.time(),
        }
        self.emit_event(
            UIEventType.FINDING_PUBLISHED,
            source=source or "blackboard",
            specialist=specialist,
            action=action,
            data={
                "entry_type": entry_type,
                "confidence": confidence,
                **trust_data,
            },
        )
        # Push agent status + trigger metrics push
        self._push_overview()

    async def _on_collaboration_consumed(self, event: Event) -> None:
        data = event.data
        consumer = data.get("specialist", "")
        entry_owner = data.get("entry_owner", "")
        entry_type = data.get("entry_type", "finding")

        # Phase 11: Emit evidence consumed event
        self.emit_event(
            UIEventType.EVIDENCE_CONSUMED,
            source="blackboard",
            specialist=consumer,
            action=f"Read {entry_type} from {entry_owner[:8]}",
            data={"owner": entry_owner, "entry_type": entry_type,
                  "entry_id": data.get("entry_id", "")},
        )

    async def _on_collaboration_challenge(self, event: Event) -> None:
        data = event.data
        specialist = data.get("specialist", "")
        reason = data.get("reason", "")
        entry_id = data.get("entry_id", "")
        challenge_id = data.get("challenge_id", f"ch_{int(time.time())}")
        consensus_id = data.get("consensus_id", "")

        # Phase 7: Track challenge in consensus visibility
        self.consensus_tracker.on_challenge_raised(
            challenge_id, specialist, reason, entry_id, consensus_id,
        )
        self._push_consensus_snapshot()

        # Phase 11: Emit challenge event
        self.emit_event(
            UIEventType.CHALLENGE_RAISED,
            source="blackboard",
            specialist=specialist,
            action=reason,
            data={"entry_id": entry_id, "challenge_id": challenge_id,
                  "consensus_id": consensus_id},
        )

    async def _on_collaboration_consensus(self, event: Event) -> None:
        data = event.data
        topic = data.get("topic", "")
        action = data.get("action", "")
        confidence = data.get("confidence", 0.0)
        participants = data.get("participants", [])
        consensus_id = data.get("consensus_id", "")
        strategy = data.get("strategy", "MAJORITY")
        positions = data.get("positions", [])
        dissenting = data.get("dissenting", [])
        conditions = data.get("conditions", [])
        is_timeout = data.get("is_timeout", False)
        timeout_participants = data.get("timeout_participants", [])
        event_name = data.get("event_name", "")

        # Phase 1: Update system overview
        self.overview.on_consensus_event(topic, action, confidence, participants)
        # Phase 2: Track consensus participation
        self.agent_tracker.on_consensus_event(topic, action, confidence, participants)

        # Phase 7: Track consensus lifecycle
        # Normalize outcome/action field — engine sends 'outcome', TUISession sends 'action'
        outcome_text = action or data.get("outcome", "")
        # Phase 10: Track consensus metrics (guarded)
        metrics = _get_agent_metrics()
        if metrics is not None:
            metrics.record_consensus_outcome(outcome_text or action)

        # Determine event subtype
        is_start = event_name in ("CONSENSUS_STARTED", "consensus_started")
        is_position = event_name in ("CONSENSUS_POSITION_SUBMITTED", "consensus_position_submitted")

        if is_start:
            self.consensus_tracker.on_consensus_started(
                consensus_id, topic, participants, strategy,
            )
            self._push_consensus_snapshot()
            self._push_overview()
            return

        if is_position:
            specialist = data.get("specialist", "")
            position = data.get("position", "NEUTRAL")
            pos_confidence = data.get("confidence", 0.5)
            pos_conditions = data.get("conditions", [])
            self.consensus_tracker.on_consensus_position(
                consensus_id, specialist, position, pos_confidence, pos_conditions,
            )
            self._push_consensus_snapshot()
            self._push_overview()
            return

        # Outcome event
        self.consensus_tracker.on_consensus_outcome(
            consensus_id, outcome_text, confidence,
            conditions=conditions,
            dissenting=dissenting,
            is_timeout=is_timeout,
            timeout_participants=timeout_participants,
            strategy=strategy,
        )

        self._push_overview()
        self._push_consensus_snapshot()

        # Phase 11: Emit consensus outcome event
        self.emit_event(
            UIEventType.CONSENSUS_OUTCOME,
            source="consensus",
            specialist="CONSENSUS",
            action=outcome_text,
            data={
                "topic": topic,
                "confidence": confidence,
                "participants": participants,
                "consensus_id": consensus_id,
                "strategy": strategy,
                "positions": positions,
                "dissenting": dissenting,
                "conditions": conditions,
                "is_timeout": is_timeout,
                "timeout_participants": timeout_participants,
            },
        )

    async def _on_collaboration_decision(self, event: Event) -> None:
        data = event.data
        specialist = data.get("specialist", "")
        outcome = data.get("outcome", "")
        reason = data.get("reason", "")
        target_id = data.get("target_id", "")
        decision_conditions = data.get("conditions", [])

        # Phase 1: Update system overview
        self.overview.on_collaboration_decision(specialist, outcome)
        # Phase 2: Track agent decisions
        self.agent_tracker.on_collaboration_decision(specialist, outcome)
        # Phase 10: Track Architect metrics (guarded)
        metrics = _get_agent_metrics()
        if metrics is not None:
            metrics.record_architect_decision(outcome.lower())

        # Phase 7: Track architect decision in consensus visibility
        self.consensus_tracker.on_consensus_decision(
            target_id or reason, outcome, reason, specialist,
            conditions=decision_conditions,
        )
        self._push_consensus_snapshot()
        self._push_overview()

        # Phase 11: Emit decision event
        outcome_upper = outcome.upper()
        if "APPROV" in outcome_upper:
            ui_type = UIEventType.DECISION_APPROVED
        elif "REJECT" in outcome_upper:
            ui_type = UIEventType.DECISION_REJECTED
        elif "REPLAN" in outcome_upper:
            ui_type = UIEventType.DECISION_REPLAN
        else:
            ui_type = UIEventType.DECISION_OVERRIDE

        self.emit_event(
            ui_type,
            source="architect",
            specialist=specialist,
            action=outcome,
            data={
                "reason": reason,
                "target_id": target_id,
                "conditions": decision_conditions,
            },
        )

    async def _on_collaboration_execution(self, event: Event) -> None:
        data = event.data
        specialist = data.get("specialist", "TERMINUS")
        action = data.get("action", "")
        status = "running"
        if event.event_type == EventType.COLLABORATION_EXECUTION_END:
            status = data.get("status", "success")
        # Phase 1: Update system overview
        self.overview.on_execution_event(specialist, action, status)
        self._push_overview()

        # Phase 11: Emit execution event
        ui_type = UIEventType.EXECUTION_STARTED if status == "running" else UIEventType.EXECUTION_COMPLETED
        self.emit_event(
            ui_type,
            source="terminus",
            specialist=specialist,
            action=action,
            data={"status": status},
        )

    # ── Consensus Visibility Push ───────────────────────────────────

    def _push_consensus_snapshot(self) -> None:
        """Push the latest consensus visibility snapshot as a UIEvent."""
        self.emit_event(
            UIEventType.CONSENSUS_UPDATED,
            source="consensus",
            data=self.consensus_tracker.snapshot(),
        )

    async def _on_collaboration_report(self, event: Event) -> None:
        data = event.data
        action = data.get("action", "")
        evidence_count = data.get("evidence_count", 0)
        challenge_count = data.get("challenge_count", 0)

        # Phase 11: Emit report event
        self.emit_event(
            UIEventType.REPORT_GENERATED,
            source="herald",
            specialist="HERALD",
            action=action,
            data={"evidence_count": evidence_count, "challenge_count": challenge_count},
        )
        # Phase 9: Generate structured narratives from runtime state
        self._generate_herald_narratives()

    # ── Phase 8: Recovery Event Handler ─────────────────────────────

    async def _on_recovery_event(self, event: Event) -> None:
        """Handle recovery lifecycle events."""
        data = event.data
        event_type_map = {
            EventType.RECOVERY_INITIATED: "retry_started",
            EventType.RECOVERY_COMPLETED: "recovery_successful",
            EventType.RECOVERY_FAILED: "recovery_failed",
        }
        mapped_type = event_type_map.get(event.event_type, "recovery_failed")
        specialist = data.get("specialist", "")
        summary = data.get("summary", data.get("action", "Recovery event"))
        detail = data.get("detail", data.get("reason", ""))
        node_id = data.get("node_id", "")
        classification = data.get("classification", "")
        action = data.get("action", "")
        retry_count = data.get("retry_count", 0)
        success = mapped_type == "recovery_successful"

        # Also check provider_failure patterns
        if "provider" in summary.lower() or "failover" in summary.lower() or "fallback" in summary.lower():
            mapped_type = "fallback_activated" if "fallback" in summary.lower() else "provider_failure"

        # Track in recovery tracker
        self.recovery_tracker.on_recovery_event(
            event_type=mapped_type,
            specialist=specialist or "SYSTEM",
            summary=summary[:60],
            detail=detail[:80],
            node_id=node_id,
            classification=classification,
            action=action,
            retry_count=retry_count,
            success=success,
        )

        # Update system overview
        self.overview.on_recovery_event()
        self._push_overview()
        self._push_recovery_snapshot()

        # Phase 11: Emit recovery event
        ui_type_map = {
            "provider_failure": UIEventType.PROVIDER_FAILURE,
            "fallback_activated": UIEventType.FALLBACK_ACTIVATED,
            "retry_started": UIEventType.RETRY_STARTED,
            "recovery_successful": UIEventType.RECOVERY_SUCCEEDED,
            "recovery_failed": UIEventType.RECOVERY_FAILED,
            "specialist_reassigned": UIEventType.SPECIALIST_REASSIGNED,
        }
        ui_type = ui_type_map.get(mapped_type, UIEventType.RECOVERY_SUCCEEDED if success else UIEventType.RECOVERY_FAILED)
        self.emit_event(
            ui_type,
            source="recovery",
            specialist=specialist or "SYSTEM",
            action=summary,
            data={
                "detail": detail,
                "node_id": node_id,
                "classification": classification,
                "action": action,
                "retry_count": retry_count,
                "success": success,
                "mapped_type": mapped_type,
            },
        )

    # ── Phase 8: Recovery Snapshot Push ─────────────────────────────

    def _push_recovery_snapshot(self) -> None:
        """Push the latest recovery snapshot as a UIEvent."""
        self.emit_event(
            UIEventType.RECOVERY_UPDATED,
            source="recovery",
            data=self.recovery_tracker.snapshot(),
        )

    # ── Phase 9: Herald Narrative Generation ────────────────────────

    def _generate_herald_narratives(self) -> None:
        """Generate structured narratives from current runtime state.

        Throttled to generate at most once per _narrative_throttle seconds
        to avoid flooding the feed with redundant summaries.
        """
        now = time.time()
        if now - self._last_narrative_time < self._narrative_throttle:
            return
        self._last_narrative_time = now

        # Task summary — use public get_active() API
        ov = self.overview.snapshot()
        task_counts = {
            "active": ov.tasks_active,
            "pending": ov.tasks_pending,
            "completed": ov.tasks_completed,
            "failed": ov.tasks_failed,
        }
        active_entries = self.work_queue.get_active()
        active_tasks = [{"title": e.title} for e in active_entries[:5]]
        narrative = self.herald_narrative.generate_task_summary(
            task_counts, active_tasks, ov.tasks_completed, ov.tasks_failed,
        )
        self.emit_event(UIEventType.HERALD_NARRATIVE, source="herald", action=narrative.to_feed_display())

        # Evidence summary — only if there are challenges
        consensus_data = self.consensus_tracker.snapshot()
        challenge_count = len(consensus_data.get("challenges", []))
        if challenge_count > 0:
            evidence_narrative = self.herald_narrative.generate_evidence_summary(
                findings_count=0,
                consumed_count=0,
                challenged_count=challenge_count,
                specialists_with_findings=[],
            )
            self.emit_event(UIEventType.HERALD_NARRATIVE, source="herald",
                            action=evidence_narrative.to_feed_display())

        # Consensus summary (only if active topics exist)
        active_topics = len(consensus_data.get("active", []))
        resolved_topics = len(consensus_data.get("resolved", []))
        if active_topics > 0 or resolved_topics > 0:
            total_positions = sum(
                len(t.get("positions", [])) for t in consensus_data.get("active", [])
            )
            conf_values = [t.get("outcome_confidence", 0.0) for t in consensus_data.get("active", [])]
            avg_conf = sum(conf_values) / len(conf_values) if conf_values else 0.0
            cons_narrative = self.herald_narrative.generate_consensus_summary(
                active_topics, resolved_topics, total_positions, avg_conf,
            )
            self.emit_event(UIEventType.HERALD_NARRATIVE, source="herald",
                            action=cons_narrative.to_feed_display())

        # Recovery summary (only if events exist)
        recovery_snap = self.recovery_tracker.snapshot()
        rec_summary = recovery_snap.get("summary", {})
        if rec_summary.get("total", 0) > 0:
            rec_narrative = self.herald_narrative.generate_recovery_summary(
                total_events=rec_summary.get("total", 0),
                succeeded=rec_summary.get("succeeded", 0),
                failed=rec_summary.get("failed", 0),
                events_by_type=rec_summary.get("by_type", {}),
            )
            self.emit_event(UIEventType.HERALD_NARRATIVE, source="herald",
                            action=rec_narrative.to_feed_display())

        # Contribution summary
        agent_profiles = {a.name: a for a in self.agent_tracker.get_ordered()}
        agent_stats = {}
        for name, profile in agent_profiles.items():
            has_active = hasattr(profile, 'is_active')
            has_contrib = hasattr(profile, 'contribution_score')
            has_tasks = hasattr(profile, 'task_count')
            agent_stats[name] = {
                "is_active": profile.is_active if has_active else False,
                "contribution_score": profile.contribution_score if has_contrib else 0.0,
                "task_count": profile.task_count if has_tasks else 0,
            }
        contrib_narrative = self.herald_narrative.generate_contribution_summary(agent_stats)
        self.emit_event(UIEventType.HERALD_NARRATIVE, source="herald",
                        action=contrib_narrative.to_feed_display())


class RuntimeToUIBridge:
    """Bridges runtime EventBus events to the UI EventBus for TUI visibility.

    Subscribes to specific runtime event types and maps them to
    UI collaboration events that the CollaborationView widget displays.
    """

    def __init__(self, runtime_bus, ui_event_bus: EventBus):
        self._runtime_bus = runtime_bus
        self._ui_bus = ui_event_bus
        self._running = False

    async def start(self) -> None:
        """Subscribe to runtime events and start bridging."""
        if self._running:
            return
        self._running = True

        # Subscribe to all relevant runtime event types using subscribe_all
        # and filter by event type for simplicity
        self._runtime_bus.subscribe_all(self._on_runtime_event)

        log.info("RuntimeToUIBridge started — forwarding runtime events to UI")

    async def stop(self) -> None:
        self._running = False
        log.info("RuntimeToUIBridge stopped")

    async def _on_runtime_event(self, runtime_event) -> None:
        """Receive a runtime event and forward it to the UI EventBus if relevant."""
        if not self._running:
            return

        try:
            etype = getattr(runtime_event, "type", None)
            if etype is None:
                return

            etype_str = str(etype.value) if hasattr(etype, "value") else str(etype)

            # Map runtime event types to UI events
            mapped = self._map_event(etype_str, runtime_event)
            if mapped:
                ui_type, specialist, action, details = mapped
                event = create_collaboration_event(ui_type, specialist, action, details)
                await self._ui_bus.publish(event)
        except Exception as e:
            log.debug(f"RuntimeToUIBridge skipped event: {e}")

    def _map_event(self, etype_str: str, event) -> Optional[tuple]:
        """Map a runtime event to a UI collaboration event.

        Returns (UIEventType, specialist, action, details) or None.
        """
        # BLACKBOARD_PUBLICATION -> COLLABORATION_FINDING
        if etype_str == "blackboard_publication":
            summary = getattr(event, "summary", "")
            specialist = getattr(event, "specialist", "")
            entry_type = getattr(event, "entry_type", "finding")
            tags = getattr(event, "tags", [])
            # Phase 6: Pass real trust metadata from runtime event
            confidence = getattr(event, "confidence", 0.0)
            source = getattr(event, "source", "")
            verification_status = getattr(event, "verification_status", "pending")
            challenged = getattr(event, "challenged", False)
            challenge_count = getattr(event, "challenge_count", 0)
            lifecycle_status = getattr(event, "lifecycle_status", "created")
            return (
                EventType.COLLABORATION_FINDING,
                specialist,
                summary[:60],
                {
                    "entry_type": entry_type,
                    "confidence": confidence,
                    "tags": tags,
                    "source": source,
                    "verification_status": verification_status,
                    "challenged": challenged,
                    "challenge_count": challenge_count,
                    "lifecycle_status": lifecycle_status,
                },
            )

        # FINDING_CONSUMED -> COLLABORATION_CONSUMED
        if etype_str == "finding_consumed":
            consumer = getattr(event, "consumer", "")
            entry_owner = getattr(event, "entry_owner", "")
            entry_type = getattr(event, "entry_type", "finding")
            return (
                EventType.COLLABORATION_CONSUMED,
                consumer,
                f"Consumed {entry_type} from {entry_owner[:8]}",
                {"entry_owner": entry_owner, "entry_type": entry_type, "entry_id": getattr(event, "entry_id", "")},
            )

        # CHALLENGE_RAISED -> COLLABORATION_CHALLENGE
        if etype_str == "challenge_raised":
            challenger = getattr(event, "challenger", "")
            challenged_claim = getattr(event, "challenged_claim", "")
            return (
                EventType.COLLABORATION_CHALLENGE,
                challenger,
                challenged_claim[:60],
                {"entry_id": getattr(event, "entry_id", ""), "reason": challenged_claim},
            )

        # CONSENSUS_FORMED -> COLLABORATION_CONSENSUS
        if etype_str == "consensus_formed":
            recommendation = getattr(event, "recommendation", "")
            confidence = getattr(event, "confidence", 0.0)
            positions = getattr(event, "positions", {})
            participants = list(positions.keys()) if positions else []
            return (
                EventType.COLLABORATION_CONSENSUS,
                "CONSENSUS",
                recommendation[:60],
                {
                    "topic": getattr(event, "target_id", ""),
                    "confidence": confidence,
                    "participants": participants,
                    "consensus_id": getattr(event, "consensus_id", ""),
                    "strategy": getattr(event, "resolution_strategy", "MAJORITY"),
                    "event_name": "consensus_formed",
                },
            )

        # ARCHITECT_DECISION -> COLLABORATION_DECISION
        if etype_str == "architect_decision":
            outcome = getattr(event, "outcome", "")
            reason = getattr(event, "reason", "")
            return (
                EventType.COLLABORATION_DECISION,
                "ARCHITECT",
                f"Decision: {outcome}",
                {
                    "outcome": outcome,
                    "reason": reason[:80],
                    "target_id": getattr(event, "target_id", ""),
                },
            )

        # EXECUTION_STARTED -> COLLABORATION_EXECUTION_START
        if etype_str == "execution_started":
            command = getattr(event, "command", "")
            return (
                EventType.COLLABORATION_EXECUTION_START,
                getattr(event, "specialist", "TERMINUS"),
                command[:60],
                {"task_id": getattr(event, "task_id", ""), "command": command},
            )

        # EXECUTION_COMPLETED -> COLLABORATION_EXECUTION_END
        if etype_str == "execution_completed":
            exit_code = getattr(event, "exit_code", 0)
            return (
                EventType.COLLABORATION_EXECUTION_END,
                getattr(event, "specialist", "TERMINUS"),
                f"Exit code: {exit_code}",
                {
                    "task_id": getattr(event, "task_id", ""),
                    "exit_code": exit_code,
                    "status": "success" if exit_code == 0 else "failed",
                },
            )

        # REPORT_GENERATED -> COLLABORATION_REPORT
        if etype_str == "report_generated":
            return (
                EventType.COLLABORATION_REPORT,
                "HERALD",
                getattr(event, "session_title", "")[:60],
                {
                    "report_id": getattr(event, "report_id", ""),
                    "evidence_count": getattr(event, "evidence_count", 0),
                    "challenge_count": getattr(event, "challenge_count", 0),
                },
            )

        # Phase 8: RECOVERY_INITIATED/COMPLETED -> UI Recovery event
        if etype_str in ("recovery_initiated", "recovery_completed"):
            ui_type = EventType.RECOVERY_INITIATED if etype_str == "recovery_initiated" else EventType.RECOVERY_COMPLETED
            return (
                ui_type,
                getattr(event, "node_id", "SYSTEM"),
                f"{getattr(event, 'action', '')}",
                {
                    "summary": getattr(event, "classification", etype_str),
                    "node_id": getattr(event, "node_id", ""),
                    "classification": getattr(event, "classification", ""),
                    "action": getattr(event, "action", ""),
                    "retry_count": getattr(event, "retry_count", 0),
                    "reason": getattr(event, "payload", {}).get("reason", ""),
                },
            )

        return None
