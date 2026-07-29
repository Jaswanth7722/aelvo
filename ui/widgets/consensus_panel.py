"""
consensus_panel.py — Consensus Visibility Panel

Displays the full consensus lifecycle as a first-class UI concept:
- Active consensus topics with all positions (who voted FOR/AGAINST/NEUTRAL)
- Vote breakdown with confidence per specialist
- Dissenting positions and conditions
- Challenge activity linked to consensus
- Consensus outcomes and architect decisions
- Historical consensus records
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from rich.text import Text
from rich.style import Style
from textual.reactive import reactive
from textual.widgets import Static

from ui.core.ui_event import UIEvent, UIEventType


# ── Helpers ─────────────────────────────────────────────────────

SPECIALIST_COLORS: Dict[str, str] = {
    "HERMES": "#8b5cf6",
    "ARCHITECT": "#3b82f6",
    "ORACLE": "#10b981",
    "FORGE": "#f59e0b",
    "SENTINEL": "#ef4444",
    "TERMINUS": "#06b6d4",
    "HERALD": "#ec4899",
    "CONSENSUS": "#19f5a5",
}

DEFAULT_SPECIALIST_COLOR = "#64748b"

POSITION_COLORS: Dict[str, str] = {
    "FOR": "#00e38c",
    "AGAINST": "#ff5c7a",
    "NEUTRAL": "#f7b731",
    "ABSTAIN": "#52627f",
}

OUTCOME_COLORS: Dict[str, str] = {
    "APPROVED": "#00e38c",
    "APPROVED W/ RISK": "#f7b731",
    "NEEDS REVISION": "#f7b731",
    "REJECTED": "#ff5c7a",
    "ESCALATED": "#ff5c7a",
}

DECISION_COLORS: Dict[str, str] = {
    "approve": "#00e38c",
    "reject": "#ff5c7a",
    "escalate": "#ff5c7a",
    "replan": "#f7b731",
    "override": "#a565ff",
}

STATUS_COLORS: Dict[str, str] = {
    "proposed": "#52627f",
    "gathering": "#3b82f6",
    "resolved": "#00e38c",
    "reviewed": "#a565ff",
}


def _specialist_color(name: str) -> str:
    return SPECIALIST_COLORS.get(name.upper(), DEFAULT_SPECIALIST_COLOR)


def _time_label(ts: float) -> str:
    if not ts:
        return "--:--"
    lt = time.localtime(ts)
    return f"{lt.tm_hour:02d}:{lt.tm_min:02d}"


def _progress_bar(pct: int, width: int = 14) -> Text:
    """Render a small progress bar for confidence."""
    filled = max(0, min(width, int(pct / 100 * width)))
    empty = width - filled
    color = "green" if pct >= 80 else "yellow" if pct >= 50 else "red"
    segments = Text()
    segments.append("█" * filled, style=color)
    segments.append("░" * empty, style="dim")
    return segments


def _render_position_row(pos: Dict[str, Any]) -> Text:
    """Render a single position row: specialist | position | confidence bar."""
    t = Text()
    specialist = pos.get("specialist", "")
    position = pos.get("position", "NEUTRAL")
    pos_color = pos.get("position_color", "#52627f")
    conf_pct = pos.get("confidence_pct", 50)

    # Specialist name
    spec_color = _specialist_color(specialist)
    t.append(f"  {specialist:<10}", style=f"bold {spec_color}")

    # Position badge
    t.append(f" [{position:<7}]", style=Style(bgcolor=pos_color, color="white", bold=True))
    t.append(" ")

    # Confidence bar
    t.append(_progress_bar(conf_pct))
    t.append(f" {conf_pct:>2}%", style="bold")

    # Conditions
    conditions = pos.get("conditions", [])
    if conditions:
        t.append(f"  ⚠ {conditions[0][:30]}", style="dim italic")

    return t


def _outcome_badge(topic: Dict[str, Any]) -> Text:
    """Render the outcome badge for a resolved consensus."""
    outcome_label = topic.get("outcome_label", "")
    outcome_color = topic.get("outcome_color", "#52627f")
    t = Text()
    if outcome_label:
        t.append(f" [{outcome_label}] ", style=Style(bgcolor=outcome_color, color="white", bold=True))
    else:
        t.append(" [PENDING] ", style=Style(bgcolor="#52627f", color="white", bold=True))
    return t


def _decision_badge(topic: Dict[str, Any]) -> Text:
    """Render the architect decision badge."""
    decision = topic.get("decision", "")
    if not decision:
        return Text()
    dec_color = topic.get("decision_color", "#52627f")
    t = Text()
    t.append(f" [{decision.upper()}] ", style=Style(bgcolor=dec_color, color="white", bold=True))
    return t


def _status_badge(topic: Dict[str, Any]) -> Text:
    """Render the consensus lifecycle status badge."""
    status = topic.get("status", "proposed")
    color = STATUS_COLORS.get(status, "#52627f")
    t = Text()
    t.append(f" [{status}] ", style=Style(bgcolor=color, color="white", bold=True))
    return t


def _render_topic_card(topic: Dict[str, Any], now: float) -> Text:
    """Render a complete consensus topic card."""
    t = Text()

    # ── Header line ──
    t.append(_status_badge(topic))
    t.append(" ")
    t.append(_outcome_badge(topic))
    t.append(" ")

    # Strategy
    strategy = topic.get("strategy_label", "")
    if strategy:
        t.append(f"[{strategy}]", style="dim")
    t.append(" ")

    # Decision badge if present
    decision = topic.get("decision", "")
    if decision:
        t.append(_decision_badge(topic))
        t.append(" ")

    # Age
    age = topic.get("display_age", "")
    if age:
        t.append(f"  {age}", style="dim")

    t.append("\n")

    # ── Topic line ──
    topic_name = topic.get("topic", "")[:48]
    t.append(f"  {topic_name}", style="bold white")

    # Vote counts
    f_count = topic.get("for_count", 0)
    a_count = topic.get("against_count", 0)
    n_count = topic.get("neutral_count", 0)
    parts = []
    if f_count:
        parts.append(f"[#00e38c]{f_count} for[/]")
    if a_count:
        parts.append(f"[#ff5c7a]{a_count} against[/]")
    if n_count:
        parts.append(f"[#f7b731]{n_count} neutral[/]")
    if parts:
        t.append("  (")
        first = True
        for part in parts:
            if not first:
                t.append("  ", style="dim")
            first = False
            t.append(part)
        t.append(")")

    # Confidence
    conf = topic.get("outcome_confidence", 0.0)
    if conf > 0:
        pct = int(conf * 100)
        t.append(f"  conf:{pct}%", style="dim")

    t.append("\n")

    # ── Positions ──
    positions = topic.get("positions", [])
    if positions:
        for pos in positions:
            t.append("\n")
            t.append(_render_position_row(pos))
    else:
        t.append(f"\n  [#52627f]awaiting positions...[/]")

    # ── Timeout warning ──
    if topic.get("is_timeout", False):
        timeout_parts = topic.get("timeout_participants", [])
        if timeout_parts:
            t.append("\n")
            t.append(f"  ⏱ timed out: {', '.join(timeout_parts)}", style="red italic")

    # ── Dissenting conditions ──
    conditions = topic.get("conditions", [])
    if conditions:
        t.append("\n")
        for c in conditions[:3]:
            t.append(f"  ⚠ {c[:48]}", style="dim italic")

    # ── Challenge links ──
    challenge_ids = topic.get("challenge_ids", [])
    if challenge_ids:
        t.append(f"\n  ⚡ {len(challenge_ids)} challenge(s)", style="dim")

    # ── Decision reason ──
    decision_reason = topic.get("decision_reason", "")
    if decision_reason:
        dec_by = topic.get("decision_by", "ARCHITECT")
        t.append("\n")
        t.append(f"  ▲ {dec_by}: {decision_reason[:55]}", style=Style(color="#a565ff", italic=True))

    return t


class ConsensusPanel(Static):
    """Consensus Visibility Panel.

    Displays active and recent consensus topics with full position
    breakdowns, outcomes, challenges, and architect decisions.
    """

    consensus_data: reactive[dict] = reactive({}, always_update=True)

    def on_mount(self) -> None:
        self.styles.border = ("solid", "#19f5a5")
        self.styles.padding = (0, 1)
        self.styles.background = "#1a1a2e"
        self.styles.height = "1fr"
        self.styles.overflow_y = "auto"

    def watch_consensus_data(self, data: dict) -> None:
        self.refresh_content(data)

    def update_from_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Update from a ConsensusVisibilityTracker snapshot."""
        self.consensus_data = snapshot

    # ── Phase 11: UIEvent handler ─────────────────────────────────

    def handle_ui_event(self, event: UIEvent) -> None:
        """Handle a standardized UIEvent to update consensus display.

        Responds to CONSENSUS_UPDATED events by refreshing the
        consensus panel with the latest snapshot data.

        Args:
            event: The UIEvent; only CONSENSUS_UPDATED is processed.
        """
        if event.type == UIEventType.CONSENSUS_UPDATED:
            self.consensus_data = event.data

    def refresh_content(self, data: dict) -> None:
        """Render the consensus panel content."""
        active = data.get("active", [])
        resolved = data.get("resolved", [])
        challenges = data.get("challenges", [])

        if not active and not resolved:
            lines = Text()
            lines.append(" consensus", style="bold #19f5a5")
            lines.append("  (0 active)", style="dim")
            lines.append("\n")
            lines.append("\n  ⏳ awaiting consensus activity", style="dim italic")
            self.update(lines)
            return

        now = time.time()
        lines = Text()

        # ── Header ──
        active_count = len(active)
        resolved_count = len(resolved)
        challenge_count = len(challenges)
        lines.append(" consensus", style="bold #19f5a5")
        parts = []
        if active_count:
            parts.append(f"[#3b82f6]{active_count} active[/]")
        if resolved_count:
            parts.append(f"[#00e38c]{resolved_count} resolved[/]")
        if challenge_count:
            parts.append(f"[#ff5c7a]{challenge_count} challenges[/]")
        if parts:
            lines.append("  (")
            for i, part in enumerate(parts):
                if i > 0:
                    lines.append("  ", style="dim")
                lines.append(part)
            lines.append(")", style="dim")
        lines.append("\n")

        # ── Active topics ──
        if active:
            lines.append("\n[#3b82f6]ACTIVE[/]", style="bold")
            lines.append("\n")
            for topic in active:
                lines.append("\n")
                lines.append(_render_topic_card(topic, now))
                lines.append("\n")

        # ── Resolved topics (latest 5) ──
        if resolved:
            if active:
                lines.append("\n")
            lines.append("\n[#00e38c]RESOLVED[/]", style="bold")
            lines.append("\n")
            for topic in resolved[:5]:
                lines.append("\n")
                lines.append(_render_topic_card(topic, now))
                lines.append("\n")

        # ── Footer ──
        lines.append("\n")
        lines.append(
            f"  {len(active)} active · {len(resolved)} resolved · "
            f"{len(challenges)} challenges",
            style="dim",
        )

        self.update(lines)
