"""conversation_feed.py — Lightweight collaboration feed

Displays only the latest 20 collaboration events.
No debug logs, provider logs, tool traces, verification noise.

Keeps: assignments, findings, challenges, decisions, executions,
reports, recovery actions, user messages, responses.
"""

import textwrap
import time
from typing import Any, Dict, List, Optional

from rich.markup import escape
from textual.containers import VerticalScroll
from textual.reactive import reactive
from textual.widgets import Static

from ui.models.collaboration_event import (
    CollaborationEvent,
    CollaborationEventType,
    EVENT_ICONS,
    EVENT_COLORS,
    SPECIALIST_COLORS,
)
from ui.models.trust_indicator import TrustIndicator
from ui.core.ui_event import UIEvent, UIEventType

MAX_VISIBLE_EVENTS = 20


class ConversationFeed(VerticalScroll):
    """Lightweight collaboration feed — latest 20 events only."""

    collaboration_events: reactive[list] = reactive([], always_update=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pending_count = 0

    def compose(self):
        yield Static(id="feed-content")

    @property
    def _content(self) -> Static:
        return self.query_one("#feed-content", Static)

    def on_mount(self) -> None:
        self.styles.border = ("none", "#030712")
        self.styles.padding = (0, 1)
        self.styles.background = "#030712"
        self.render_content(self.collaboration_events)

    def watch_collaboration_events(self, events: list) -> None:
        self.render_content(events)

    # ── Public API ──────────────────────────────────────────────

    def handle_ui_event(self, event: UIEvent) -> None:
        """Handle a UIEvent — only processes collaboration-relevant events.

        Silently ignores: tool events, specialist lifecycle, verification,
        and system noise events.
        """
        etype: UIEventType = event.type
        specialist: str = event.specialist
        action: str = event.action
        data: Dict[str, Any] = event.data

        # ── Collaboration findings ──
        if etype == UIEventType.FINDING_PUBLISHED:
            self.add_collaboration_event(
                CollaborationEvent.finding_published(
                    specialist=specialist,
                    summary=action,
                    confidence=data.get("confidence", 0.0),
                    source=data.get("source", ""),
                    verification_status=data.get("verification_status", "pending"),
                    challenged=data.get("challenged", False),
                    challenge_count=data.get("challenge_count", 0),
                    affected_files=data.get("affected_files", None),
                    evidence_type=data.get("entry_type", "finding"),
                    lifecycle_status=data.get("lifecycle_status", "created"),
                )
            )

        elif etype == UIEventType.EVIDENCE_CONSUMED:
            self.add_collaboration_event(
                CollaborationEvent.evidence_consumed(
                    consumer=specialist,
                    owner=data.get("owner", ""),
                    entry_type=data.get("entry_type", "finding"),
                )
            )

        elif etype == UIEventType.CHALLENGE_RAISED:
            self.add_collaboration_event(
                CollaborationEvent.challenge_raised(
                    specialist=specialist,
                    reason=action,
                )
            )

        # ── Consensus ──
        elif etype == UIEventType.CONSENSUS_OUTCOME:
            self.add_collaboration_event(
                CollaborationEvent.consensus_outcome(
                    topic=data.get("topic", ""),
                    outcome=action,
                    confidence=data.get("confidence", 0.0),
                    participants=data.get("participants", []),
                )
            )

        # ── Decisions ──
        elif etype == UIEventType.DECISION_APPROVED:
            self.add_collaboration_event(
                CollaborationEvent.decision_approved(
                    specialist=specialist,
                    outcome=action,
                    reason=data.get("reason", ""),
                )
            )

        elif etype in (UIEventType.DECISION_REJECTED, UIEventType.DECISION_OVERRIDE, UIEventType.DECISION_REPLAN):
            self.add_collaboration_event(
                CollaborationEvent.system(f"Decision: {specialist} {action}")
            )

        # ── Execution ──
        elif etype in (UIEventType.EXECUTION_STARTED, UIEventType.EXECUTION_COMPLETED):
            status = data.get("status", "running")
            self.add_collaboration_event(
                CollaborationEvent.execution_action(
                    specialist=specialist,
                    command=action,
                    status=status,
                )
            )

        # ── Task lifecycle (assignments only, not creation/progress noise) ──
        elif etype == UIEventType.TASK_ASSIGNED:
            task_name = data.get("task_name", action)
            self.add_collaboration_event(
                CollaborationEvent.task_assigned(
                    specialist=specialist,
                    task_name=task_name,
                )
            )

        # ── Reporting ──
        elif etype == UIEventType.REPORT_GENERATED:
            self.add_collaboration_event(
                CollaborationEvent.report_generated(
                    specialist=specialist,
                    title=action,
                    evidence_count=data.get("evidence_count", 0),
                    challenge_count=data.get("challenge_count", 0),
                )
            )

        elif etype == UIEventType.HERALD_NARRATIVE:
            self.add_collaboration_event(
                CollaborationEvent.system(f"HERALD: {action}")
            )

        # ── Recovery (only meaningful outcomes, not retry noise) ──
        elif etype in (UIEventType.RECOVERY_SUCCEEDED, UIEventType.RECOVERY_FAILED):
            label = etype.value.replace("_", " ").title()
            self.add_collaboration_event(
                CollaborationEvent.system(f"Recovery: {label} \u2014 {action[:45]}")
            )

        # ── User messages ──
        elif etype == UIEventType.USER_MESSAGE:
            self.add_message("You", "AELVO", action)
        elif etype == UIEventType.RESPONSE_MESSAGE:
            self.add_message("AELVO", "You", action)

        # Silently ignore: tool events, specialist lifecycle, verification,
        # system online/error/warning, task created/started/progress/completed/failed

    def add_collaboration_event(self, event: CollaborationEvent) -> None:
        """Add a structured collaboration event, keep max 20."""
        current = list(self.collaboration_events)
        current.append(event)
        if len(current) > MAX_VISIBLE_EVENTS:
            current = current[-MAX_VISIBLE_EVENTS:]
        self.collaboration_events = current

    def add_message(self, sender: str, recipient: str, content: str) -> None:
        """Legacy wrapper."""
        if sender.lower() == "you":
            event = CollaborationEvent.user_message(content)
        else:
            event = CollaborationEvent.response_message(content)
        self.add_collaboration_event(event)

    # ── Scroll Helpers ──────────────────────────────────────────

    def _is_at_bottom(self) -> bool:
        return self.max_scroll_y <= 0 or self.scroll_y >= self.max_scroll_y

    def _update_scroll_indicator(self) -> None:
        try:
            indicator = self.screen.query_one("#scroll-indicator")
            if self.collaboration_events and not self._is_at_bottom():
                indicator.styles.display = "block"
                indicator.update(
                    f"  [bold #1f8fff]\u2193[/] [#52627f]{self._pending_count} new[/]"
                    if self._pending_count > 0
                    else "  [bold #1f8fff]\u2193[/] [#52627f]scroll to bottom[/]"
                )
            else:
                indicator.styles.display = "none"
                indicator.update("")
                self._pending_count = 0
        except AttributeError:
            pass

    # ── Content Rendering ───────────────────────────────────────

    def render_content(self, events: list) -> None:
        was_at_bottom = self._is_at_bottom()

        lines: list[str] = []

        if not events:
            lines.append("[#52627f]collaboration feed[/]")
            lines.append("")
            lines.append("[#8c5cff]AELVO[/]")
            lines.append("  Ready.")
            self._content.update("\n".join(lines))
            self._update_scroll_indicator()
            return

        lines.append("[#52627f]collaboration feed[/]")
        lines.append("")

        for item in events:
            if isinstance(item, CollaborationEvent):
                lines.extend(self._render_collaboration_event(item))
            lines.append("")

        self._content.update("\n".join(lines))

        if was_at_bottom:
            self._pending_count = 0
            self.call_after_refresh(self.scroll_end, animate=False)
            self.call_after_refresh(self._update_scroll_indicator)
        else:
            self.call_after_refresh(self._update_scroll_indicator)

    def _render_collaboration_event(self, ev: CollaborationEvent) -> list[str]:
        etype = ev.event_type
        icon = EVENT_ICONS.get(etype, "\u2500")
        color = EVENT_COLORS.get(etype, "#52627f")
        timestamp = self._time_label(ev.timestamp)
        specialist = ev.specialist.upper() if ev.specialist else ""
        spec_color = SPECIALIST_COLORS.get(specialist, "#52627f")

        result: list[str] = []

        if etype == CollaborationEventType.TASK_ASSIGNED:
            result.append(
                f"[#52627f]{timestamp}[/] [{color}]{icon}[/] "
                f"[{spec_color}]{specialist}[/] [#8fa0c5]assigned[/] "
                f"[#00d889]{ev.details[:48]}[/]"
            )

        elif etype == CollaborationEventType.FINDING_PUBLISHED:
            conf_str = f" [#52627f]conf:{ev.confidence:.2f}[/]" if ev.confidence > 0 else ""
            result.append(
                f"[#52627f]{timestamp}[/] [{color}]{icon}[/] "
                f"[{spec_color}]{specialist}[/] [#8fa0c5]published[/] "
                f"{escape(ev.summary[:48])}{conf_str}"
            )

        elif etype == CollaborationEventType.EVIDENCE_CONSUMED:
            owner = ev.metadata.get("owner", "")
            entry_type = ev.metadata.get("entry_type", "evidence")
            result.append(
                f"[#52627f]{timestamp}[/] [{color}]{icon}[/] "
                f"[{spec_color}]{specialist}[/] [#8fa0c5]consumed[/] "
                f"[#52627f]{entry_type}[/] from [{spec_color}]{owner}[/]"
            )

        elif etype == CollaborationEventType.CHALLENGE_RAISED:
            result.append(
                f"[#52627f]{timestamp}[/] [{color}]{icon}[/] "
                f"[{spec_color}]{specialist}[/] [#ff5c7a]challenged[/]: "
                f"{escape(ev.summary[:60])}"
            )

        elif etype == CollaborationEventType.CONSENSUS_OUTCOME:
            outcome_lower = ev.summary.lower()
            if "revision" in outcome_lower or "reject" in outcome_lower:
                label, label_color = "revision", "#f7b731"
            elif "escalat" in outcome_lower:
                label, label_color = "escalated", "#ff5c7a"
            else:
                label, label_color = "approved", "#00e38c"
            conf_str = f" [#52627f]conf:{ev.confidence:.2f}[/]" if ev.confidence > 0 else ""
            result.append(
                f"[#52627f]{timestamp}[/] [{color}]{icon}[/] "
                f"[#19f5a5]CONSENSUS[/] [{label_color}]{label}[/]: "
                f"{escape(ev.summary[:48])}{conf_str}"
            )

        elif etype == CollaborationEventType.DECISION_APPROVED:
            detail = f" \u2014 {escape(ev.details[:40])}" if ev.details else ""
            result.append(
                f"[#52627f]{timestamp}[/] [{color}]{icon}[/] "
                f"[{spec_color}]{specialist}[/] [#19f5a5]approved[/]: "
                f"{escape(ev.summary[:40])}{detail}"
            )

        elif etype == CollaborationEventType.EXECUTION_ACTION:
            status = ev.metadata.get("status", "running")
            status_color = "#00e38c" if status == "success" else "#f7b731"
            result.append(
                f"[#52627f]{timestamp}[/] [{color}]{icon}[/] "
                f"[{spec_color}]{specialist}[/] [#8fa0c5]executed[/] "
                f"[#8fa0c5]{escape(ev.summary[:55])}[/]"
            )

        elif etype == CollaborationEventType.REPORT_GENERATED:
            ec = ev.metadata.get("evidence_count", 0)
            cc = ev.metadata.get("challenge_count", 0)
            meta = f" [#52627f]({ec} evidence, {cc} challenge)[/]"
            result.append(
                f"[#52627f]{timestamp}[/] [{color}]{icon}[/] "
                f"[{spec_color}]{specialist}[/] [#8fa0c5]generated[/] "
                f"{escape(ev.summary[:40])}{meta}"
            )

        elif etype == CollaborationEventType.USER_MESSAGE:
            wrapped = self._wrap_content(escape(ev.summary), width=88)
            result.append(f"[#1f8fff]YOU[/] asked [#52627f]{timestamp}[/]:")
            for chunk in wrapped[:5]:
                result.append(f"  {chunk}")
            if len(wrapped) > 5:
                result.append("  [#52627f]...[/]")

        elif etype == CollaborationEventType.RESPONSE_MESSAGE:
            wrapped = self._wrap_content(escape(ev.summary), width=88)
            result.append(f"[#8c5cff]AELVO[/] responded [#52627f]{timestamp}[/]:")
            for chunk in wrapped[:5]:
                result.append(f"  {chunk}")
            if len(wrapped) > 5:
                result.append("  [#52627f]...[/]")

        else:
            result.append(
                f"[#52627f]{timestamp}[/] [{color}]{icon}[/] "
                f"[#52627f]{escape(ev.summary[:70])}[/]"
            )

        return result

    # ── Legacy wrapper (called from app.py) ───────────────────────

    def add_entry(self, category: str, message: str) -> None:
        """Legacy wrapper — converts to a system CollaborationEvent.

        Called from app.py for startup, error, and warning messages.
        """
        event = CollaborationEvent.system(f"[{category}] {message}")
        self.add_collaboration_event(event)

    # ── Utilities ───────────────────────────────────────────────

    def _time_label(self, ts: float) -> str:
        if not ts:
            return "--:--"
        local_t = time.localtime(ts)
        return f"{local_t.tm_hour:02d}:{local_t.tm_min:02d}"

    def _wrap_content(self, content: str, width: int) -> list[str]:
        wrapped: list[str] = []
        for raw_line in content.splitlines() or [""]:
            if not raw_line:
                wrapped.append("")
                continue
            wrapped.extend(textwrap.wrap(raw_line, width=width, replace_whitespace=False) or [""])
        return wrapped
