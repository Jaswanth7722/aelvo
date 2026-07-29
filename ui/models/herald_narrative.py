"""ui/models/herald_narrative.py — Herald Narrative Engine

Phase 9: Herald becomes the narrator of the system.

Generates structured narrative summaries that power the conversation
feed: task summaries, evidence summaries, decision summaries,
consensus summaries, recovery summaries, and contribution summaries.

These are produced from real runtime events — not synthetic dialogue.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class HeraldNarrative:
    """A structured narrative generated from runtime events."""

    narrative_type: str                    # task_summary | evidence_summary | decision_summary |
                                           # consensus_summary | recovery_summary | contribution_summary
    title: str                             # Short title
    body: str                              # The narrative text
    specialist: str = "HERALD"             # Always HERALD
    source_events: int = 0                 # Number of source events this summarizes
    confidence: float = 1.0               # Narrative confidence
    timestamp: float = field(default_factory=time.time)

    def to_feed_display(self) -> str:
        """Format as a feed-ready display string."""
        return f"{self.title}: {self.body}"


class HeraldNarrativeEngine:
    """Generates structured narratives from tracked runtime state.

    Pulls from AgentStatusTracker, WorkQueueTracker, ConsensusVisibilityTracker,
    RecoveryTracker, and SystemOverviewAggregator to produce narratives.

    Each narrative type maps to a specific aspect of system activity:
    - task_summary: What tasks are active, assigned, completed
    - evidence_summary: What findings have been published and consumed
    - decision_summary: What the Architect decided
    - consensus_summary: What positions were collected and outcome
    - recovery_summary: What resilience events occurred
    - contribution_summary: Which agents contributed and how
    """

    def __init__(self):
        self._narratives: List[HeraldNarrative] = []
        self._last_state: Dict[str, Any] = {}

    def generate_task_summary(
        self,
        task_counts: Dict[str, int],
        active_tasks: List[Dict[str, Any]],
        completed_tasks: int,
        failed_tasks: int,
    ) -> HeraldNarrative:
        """Generate a task summary narrative."""
        active = task_counts.get("active", 0)
        pending = task_counts.get("pending", 0)

        if active == 0 and pending == 0 and completed_tasks == 0:
            body = "No tasks yet."
        elif active == 0 and pending == 0:
            body = f"All tasks complete. {completed_tasks} completed, {failed_tasks} failed."
        elif active > 0:
            active_names = [t.get("title", "")[:30] for t in active_tasks[:3]]
            task_list = ", ".join(active_names) if active_names else f"{active} active tasks"
            body = f"{active} active, {pending} pending. {task_list}."
            if completed_tasks:
                body += f" {completed_tasks} completed."
            if failed_tasks:
                body += f" {failed_tasks} failed."
        else:
            body = f"{pending} pending, {completed_tasks} completed, {failed_tasks} failed."

        narrative = HeraldNarrative(
            narrative_type="task_summary",
            title="Task Summary",
            body=body[:120],
            source_events=sum(task_counts.values()),
        )
        self._narratives.append(narrative)
        return narrative

    def generate_evidence_summary(
        self,
        findings_count: int,
        consumed_count: int,
        challenged_count: int,
        specialists_with_findings: List[str],
    ) -> HeraldNarrative:
        """Generate an evidence summary narrative."""
        if findings_count == 0:
            body = "No findings yet."
        else:
            parts = [f"{findings_count} findings"]
            if consumed_count:
                parts.append(f"{consumed_count} consumed")
            if challenged_count:
                parts.append(f"{challenged_count} challenged")
            if specialists_with_findings:
                parts.append(f"from {', '.join(specialists_with_findings)}")
            body = ", ".join(parts) + "."

        narrative = HeraldNarrative(
            narrative_type="evidence_summary",
            title="Evidence Summary",
            body=body[:120],
            source_events=findings_count + consumed_count + challenged_count,
        )
        self._narratives.append(narrative)
        return narrative

    def generate_decision_summary(
        self,
        decisions: List[Dict[str, Any]],
        approve_count: int,
        reject_count: int,
        override_count: int,
    ) -> HeraldNarrative:
        """Generate a decision summary narrative."""
        if not decisions:
            body = "No decisions yet."
        else:
            parts = []
            if approve_count:
                parts.append(f"{approve_count} approved")
            if reject_count:
                parts.append(f"{reject_count} rejected")
            if override_count:
                parts.append(f"{override_count} overrides")
            latest = decisions[-1]
            latest_detail = f" Latest: {latest.get('outcome', '')} — {latest.get('reason', '')[:40]}"
            body = ", ".join(parts) + "." + latest_detail if parts else "Decisions recorded."

        narrative = HeraldNarrative(
            narrative_type="decision_summary",
            title="Decision Summary",
            body=body[:120],
            source_events=len(decisions),
        )
        self._narratives.append(narrative)
        return narrative

    def generate_consensus_summary(
        self,
        active_topics: int,
        resolved_topics: int,
        total_positions: int,
        average_confidence: float,
    ) -> HeraldNarrative:
        """Generate a consensus summary narrative."""
        if active_topics == 0 and resolved_topics == 0:
            body = "No consensus activity."
        else:
            parts = []
            if active_topics:
                parts.append(f"{active_topics} active")
            if resolved_topics:
                parts.append(f"{resolved_topics} resolved")
            conf_str = f" avg confidence {average_confidence:.0%}" if average_confidence > 0 else ""
            body = ", ".join(parts) + f" topics{conf_str}, {total_positions} positions collected."

        narrative = HeraldNarrative(
            narrative_type="consensus_summary",
            title="Consensus Summary",
            body=body[:120],
            source_events=active_topics + resolved_topics,
            confidence=average_confidence,
        )
        self._narratives.append(narrative)
        return narrative

    def generate_recovery_summary(
        self,
        total_events: int,
        succeeded: int,
        failed: int,
        events_by_type: Dict[str, int],
    ) -> HeraldNarrative:
        """Generate a recovery summary narrative."""
        if total_events == 0:
            body = "No recovery events."
        else:
            rate = (succeeded / total_events * 100) if total_events > 0 else 0
            parts = [f"{total_events} events"]
            if succeeded:
                parts.append(f"{succeeded} succeeded ({rate:.0f}% success rate)")
            if failed:
                parts.append(f"{failed} failed")
            type_parts = [f"{k.replace('_', ' ')}: {v}" for k, v in sorted(events_by_type.items(), key=lambda x: -x[1])[:3]]
            if type_parts:
                parts.append(" (" + ", ".join(type_parts) + ")")
            body = " ".join(parts) + "."

        narrative = HeraldNarrative(
            narrative_type="recovery_summary",
            title="Recovery Summary",
            body=body[:120],
            source_events=total_events,
        )
        self._narratives.append(narrative)
        return narrative

    def generate_contribution_summary(
        self,
        agent_stats: Dict[str, Dict[str, Any]],
    ) -> HeraldNarrative:
        """Generate a contribution summary narrative."""
        if not agent_stats:
            body = "No agent activity yet."
        else:
            active = [name for name, s in agent_stats.items() if s.get("is_active", False)]
            top_contributor = max(agent_stats.items(), key=lambda x: x[1].get("contribution_score", 0)) if agent_stats else None
            parts = []
            if active:
                parts.append(f"{', '.join(active[:3])} active")
            if top_contributor:
                name, stats = top_contributor
                cs = stats.get("contribution_score", 0)
                parts.append(f"top: {name} ({cs:.0f}% contribution)")
            body = ". ".join(parts) + "." if parts else "Agents idle."

        narrative = HeraldNarrative(
            narrative_type="contribution_summary",
            title="Contribution Summary",
            body=body[:120],
            source_events=len(agent_stats),
        )
        self._narratives.append(narrative)
        return narrative

    def get_recent(self, limit: int = 10) -> List[HeraldNarrative]:
        """Get the most recent narratives."""
        return self._narratives[-limit:]

    def clear(self) -> None:
        """Clear all narratives."""
        self._narratives.clear()
