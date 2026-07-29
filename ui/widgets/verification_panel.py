"""
VerificationPanel — real-time verification results display.

Shows:
- Current verification status per type (lint, typecheck, test, security, etc.)
- Pass/fail counts
- Recent verification results with confidence scores
- Retry attempts
"""

from textual.widgets import Static
from textual.reactive import reactive
from collections import Counter


VERIFICATION_TYPES = ["lint", "typecheck", "test", "security", "sandbox", "consistency"]

VERIFICATION_COLORS = {
    "passed": "#2e8b57",
    "failed": "#cd5c5c",
    "running": "#6495ed",
    "pending": "#666666",
    "retry": "#ff8c00",
    "skipped": "#444444",
    "blocked": "#cd5c5c",
}

VERIFICATION_ICONS = {
    "passed": "✓",
    "failed": "✗",
    "running": "◐",
    "pending": "○",
    "retry": "↻",
    "skipped": "—",
    "blocked": "◍",
}

MAX_RESULTS = 20


class VerificationPanel(Static):
    """Widget showing verification events with pass/fail status and result history."""

    verifications: reactive[dict] = reactive({}, always_update=True)
    recent_results: reactive[list] = reactive([], always_update=True)
    pass_count: reactive[int] = reactive(0)
    fail_count: reactive[int] = reactive(0)

    def on_mount(self) -> None:
        self.styles.border = ("solid", "#2e8b57")
        self.styles.padding = (0, 1)
        self.styles.background = "#1a1a2e"
        self.styles.overflow_y = "auto"

    def watch_verifications(self, val: dict) -> None:
        self._update_content()

    def watch_recent_results(self, val: list) -> None:
        self._update_content()

    def add_result(self, vtype: str, target: str, status: str, confidence: float = 0.0, details: str = "") -> None:
        """Add or update a verification result.

        Args:
            vtype: Verification type (lint, typecheck, test, security, sandbox)
            target: What was verified (file name, task id, etc.)
            status: passed, failed, running, pending, retry, skipped, blocked
            confidence: Confidence score (0.0 - 1.0)
            details: Additional details
        """
        # Update current status per type
        statuses = dict(self.verifications)
        statuses[vtype] = {"status": status, "target": target[:30], "confidence": confidence}
        self.verifications = statuses

        # Track pass/fail counts
        if status == "passed":
            self.pass_count = self.pass_count + 1
        elif status == "failed":
            self.fail_count = self.fail_count + 1

        # Add to recent results log
        results = list(self.recent_results)
        results.append({
            "type": vtype,
            "target": target[:30],
            "status": status,
            "confidence": confidence,
            "details": details[:40],
        })
        if len(results) > MAX_RESULTS:
            results = results[-MAX_RESULTS:]
        self.recent_results = results

    def _is_at_bottom(self) -> bool:
        """Check if the scroll position is at the bottom."""
        return self.max_scroll_y <= 0 or self.scroll_y >= self.max_scroll_y

    def _update_content(self) -> None:
        was_at_bottom = self._is_at_bottom()

        lines = [" verification"]

        # Current status for each verification type
        for vtype in VERIFICATION_TYPES:
            info = self.verifications.get(vtype, {})
            status = info.get("status", "pending")
            target = info.get("target", "")
            confidence = info.get("confidence", 0.0)

            icon = VERIFICATION_ICONS.get(status, "○")
            color = VERIFICATION_COLORS.get(status, "#666666")

            if status in ("passed", "failed", "running", "retry"):
                line = f"  [{color}]{icon}[/] {vtype}"
                if target:
                    line += f" — {target}"
                if confidence:
                    line += f" [#555555]({confidence:.2f})[/]"
            else:
                line = f"  [#444444]{icon}[/] [#666666]{vtype}[/]"
            lines.append(line)

        # Stats bar
        passed = self.pass_count
        failed = self.fail_count
        total = passed + failed
        if total > 0:
            lines.append("")
            stats_parts = []
            if passed:
                stats_parts.append(f"[#2e8b57]{passed} passed[/]")
            if failed:
                stats_parts.append(f"[#cd5c5c]{failed} failed[/]")
            stats_parts.append(f"[#555555]{total} total[/]")
            lines.append(f"  {' · '.join(stats_parts)}")

        # Recent results
        results = self.recent_results
        if results:
            lines.append("")
            lines.append("  [#555555]recent:[/]")
            for r in results[-6:]:
                vtype = r.get("type", "")
                target = r.get("target", "")
                status = r.get("status", "")
                color = VERIFICATION_COLORS.get(status, "#666666")
                icon = VERIFICATION_ICONS.get(status, "·")
                lines.append(f"    [{color}]{icon}[/] {vtype}[#555555]:[/] {target}")

        self.update("\n".join(lines))

        # Auto-scroll to bottom only if user was already at the bottom
        if was_at_bottom:
            self.call_after_refresh(self.scroll_end, animate=False)
