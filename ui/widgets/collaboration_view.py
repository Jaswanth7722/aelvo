"""
CollaborationView — real-time specialist collaboration dashboard.

Displays:
- Specialist activity states (active / thinking / acting / inactive)
- Live finding publications (ORACLE → blackboard)
- Evidence consumption trail (who consumed what)
- Challenge activity (SENTINEL challenging low-confidence findings)
- Consensus outcomes (positions, confidence)
- Architect decisions (approve / reject / escalate / override)
- Execution activity (commands, results)
- Report generation
"""

from typing import Optional, Dict, List, Any
from textual.widgets import Static
from textual.reactive import reactive
from rich.text import Text
from collections import deque
import time

from ui.models.trust_indicator import TrustIndicator, confidence_color


SPECIALIST_TREE = ["Architect", "Oracle", "Forge", "Sentinel", "Terminus", "Herald"]

# Color scheme matching the dark TUI
COLOR_SPECIALIST = {
    "ORACLE": "#9370db",
    "FORGE": "#2e8b57",
    "SENTINEL": "#cd5c5c",
    "ARCHITECT": "#dda0dd",
    "TERMINUS": "#f5deb3",
    "HERALD": "#6495ed",
    "HERMES": "#4682b4",
    "CONSENSUS": "#ffd700",
}

COLOR_FINDING = "#9370db"
COLOR_CONSUMED = "#2e8b57"
COLOR_CHALLENGE = "#cd5c5c"
COLOR_CONSENSUS = "#ffd700"
COLOR_DECISION_APPROVE = "#2e8b57"
COLOR_DECISION_REJECT = "#cd5c5c"
COLOR_DECISION_ESCALATE = "#ff8c00"
COLOR_EXECUTION = "#6495ed"
COLOR_REPORT = "#dda0dd"
COLOR_MUTED = "#666666"

MAX_ACTIVITY_LOG = 50


class CollaborationView(Static):
    """Widget displaying real-time specialist collaboration activity."""

    specialists: reactive[dict] = reactive({}, always_update=True)
    consensus: reactive[Optional[dict]] = reactive(None)
    activity_log: reactive[list] = reactive([], always_update=True)
    evidence_count: reactive[int] = reactive(0)
    challenge_count: reactive[int] = reactive(0)
    decision_count: reactive[int] = reactive(0)

    def on_mount(self) -> None:
        self.styles.border = ("solid", "#9370db")
        self.styles.padding = (0, 1)
        self.styles.background = "#1a1a2e"
        self.styles.overflow_y = "auto"

    # ── Reactive watchers ───────────────────────────────────────

    def watch_specialists(self, val: dict) -> None:
        self._update_content()

    def watch_consensus(self, val: Optional[dict]) -> None:
        self._update_content()

    def watch_activity_log(self, val: list) -> None:
        self._update_content()

    def watch_evidence_count(self, val: int) -> None:
        self._update_content()

    # ── Specialist state methods ────────────────────────────────

    def update_specialist(self, name: str, state: str, activity: str = "") -> None:
        data = dict(self.specialists)
        data[name] = {"state": state, "activity": activity}
        self.specialists = data

    def remove_specialist(self, name: str) -> None:
        data = dict(self.specialists)
        data.pop(name, None)
        self.specialists = data

    # ── Consensus display ──────────────────────────────────────

    def set_consensus(self, topic: str, participants: list, outcome: str, confidence: float = 0.0) -> None:
        self.consensus = {
            "topic": topic,
            "participants": participants,
            "outcome": outcome,
            "confidence": confidence,
        }

    def clear_consensus(self) -> None:
        self.consensus = None

    # ── Activity log methods ────────────────────────────────────

    def add_activity(self, category: str, icon: str, text: str, color: str = "#6495ed") -> None:
        """Add an entry to the scrolling activity log."""
        current = list(self.activity_log)
        current.append({
            "category": category,
            "icon": icon,
            "text": text[:55],
            "color": color,
            "time": time.time(),
        })
        if len(current) > MAX_ACTIVITY_LOG:
            current = current[-MAX_ACTIVITY_LOG:]
        self.activity_log = current

    def log_finding(self, specialist: str, summary: str, entry_type: str = "finding", confidence: float = 0.0,
                    trust: Optional[TrustIndicator] = None) -> None:
        """Log a finding publication with optional trust metadata."""
        icon = "◈"
        text = f"[{specialist[:8]}] {summary[:40]}"
        if confidence:
            text += f" ({confidence:.2f})"
        # Append trust indicators
        if trust:
            ver_color = trust.verification_color
            ver_label = trust.verification_label
            text += f" [{ver_label}]"
            if trust.source:
                text += f" src:{trust.source[:12]}"
            if trust.challenged:
                text += f" ⚡{trust.challenge_count}"
        self.add_activity("finding", icon, text[:55], COLOR_FINDING)
        self.evidence_count = self.evidence_count + 1

    def log_consumed(self, consumer: str, owner: str, entry_type: str = "finding") -> None:
        """Log an evidence consumption."""
        icon = "◈"
        text = f"{consumer[:8]} ← {owner[:8]} ({entry_type})"
        self.add_activity("consumed", icon, text, COLOR_CONSUMED)

    def log_challenge(self, challenger: str, reason: str, entry_id: str = "") -> None:
        """Log a challenge raised."""
        icon = "⚡"
        text = f"{challenger[:8]}: {reason[:45]}"
        self.add_activity("challenge", icon, text, COLOR_CHALLENGE)
        self.challenge_count = self.challenge_count + 1

    def log_consensus(self, outcome: str, confidence: float, participant_count: int) -> None:
        """Log a consensus formed."""
        icon = "◎"
        text = f"{outcome[:20]} ({confidence:.2f}, {participant_count} voters)"
        self.add_activity("consensus", icon, text, COLOR_CONSENSUS)

    def log_decision(self, specialist: str, outcome: str, reason: str = "") -> None:
        """Log an architect decision."""
        icon = "◆"
        text = f"{specialist[:8]}: {outcome[:15]}"
        if reason:
            text += f" — {reason[:35]}"
        outcome_upper = outcome.upper()
        color = (
            COLOR_DECISION_APPROVE if "APPROV" in outcome_upper
            else COLOR_DECISION_REJECT if "REJECT" in outcome_upper or "DENIED" in outcome_upper
            else COLOR_DECISION_ESCALATE if "ESCAL" in outcome_upper
            else COLOR_MUTED
        )
        self.add_activity("decision", icon, text, color)
        self.decision_count = self.decision_count + 1

    def log_execution(self, specialist: str, command: str, status: str = "running") -> None:
        """Log an execution start/end."""
        icon = "▸" if status == "running" else "✓" if status == "success" else "✗"
        text = f"{specialist[:8]}: {command[:45]}"
        color = COLOR_EXECUTION if status == "running" else COLOR_CONSUMED if status == "success" else COLOR_CHALLENGE
        self.add_activity("execution", icon, text, color)

    def log_report(self, title: str, evidence_count: int = 0, challenge_count: int = 0) -> None:
        """Log a report generation."""
        icon = "■"
        text = f"{title[:45]} ({evidence_count} ev, {challenge_count} ch)"
        self.add_activity("report", icon, text, COLOR_REPORT)

    # ── Content rendering ──────────────────────────────────────

    def _is_at_bottom(self) -> bool:
        """Check if the scroll position is at the bottom."""
        return self.max_scroll_y <= 0 or self.scroll_y >= self.max_scroll_y

    def _update_content(self) -> None:
        """Rebuild the full widget content from reactive state."""
        was_at_bottom = self._is_at_bottom()

        lines: List[str] = []

        # ── Specialist status section ──
        lines.append(" agents")
        for name in SPECIALIST_TREE:
            info = self.specialists.get(name, {})
            state = info.get("state", "inactive")
            activity = info.get("activity", "")

            state_color = "#2e8b57" if state == "active" else "#6495ed" if state == "thinking" else "#cd5c5c" if state == "acting" else "#a9a9a9"
            state_sym = "●" if state == "active" else "◐" if state == "thinking" else "►" if state == "acting" else "○"

            if state != "inactive":
                line = f"  [{state_color}]{state_sym}[/] {name.lower()}"
                if activity:
                    line += f" — {activity[:28]}"
                lines.append(line)
            else:
                lines.append(f"  [#a9a9a9]○[/] {name.lower()}")

        # ── Stats bar ──
        ev = self.evidence_count
        ch = self.challenge_count
        dc = self.decision_count
        if ev or ch or dc:
            lines.append("")
            parts = []
            if ev:
                parts.append(f"[#9370db]{ev} findings[/]")
            if ch:
                parts.append(f"[#cd5c5c]{ch} challenges[/]")
            if dc:
                parts.append(f"[#dda0dd]{dc} decisions[/]")
            lines.append(f"  {' · '.join(parts)}")

        # ── Active consensus ──
        c = self.consensus
        if c:
            lines.append("")
            lines.append(f"  [#ffd700]◎[/] consensus: {c.get('topic', '')[:25]}")
            outcome = c.get("outcome", "")
            ocolor = "#2e8b57" if "APPROV" in outcome else "#cd5c5c" if "REJECT" in outcome else "#6495ed"
            conf = c.get("confidence", 0.0)
            participants = c.get("participants", [])
            part_str = ", ".join(p[:6] for p in participants) if participants else ""
            lines.append(f"  [{ocolor}]{outcome.lower()}[/] [{COLOR_MUTED}]conf={conf:.2f}[/] [{COLOR_MUTED}]{part_str}[/]")

        # ── Activity log ──
        log_entries = self.activity_log
        if log_entries:
            lines.append("")
            lines.append(" activity")
            for entry in log_entries[-12:]:
                icon = entry.get("icon", "·")
                text = entry.get("text", "")
                color = entry.get("color", "#6495ed")
                lines.append(f"  [{color}]{icon}[/] {text}")

        self.update("\n".join(lines))

        # Auto-scroll to bottom only if user was already at the bottom
        if was_at_bottom:
            self.call_after_refresh(self.scroll_end, animate=False)
