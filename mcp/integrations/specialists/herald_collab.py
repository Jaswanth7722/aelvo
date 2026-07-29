"""herald_collab.py — HERALD Collaborative Behaviors

HERALD gains the ability to:
1. Generate structured collaboration summaries from the task board history
2. Submit summaries to Architect for review (REVIEW task on the board)
3. Report capability gaps and trust state changes for human operators
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from cognition.blackboard import CognitiveBlackboard
from cognition.types import EntryType, Provenance, ProvenanceType
from shared_task_board import (
    SharedTaskBoard,
    Task,
    TaskSpec,
    TaskType,
    TaskStatus,
    SpecialistName,
    BoardState,
)

log = logging.getLogger("aelvo.collab.herald")


class HeraldCollaborativeBehavior:
    """Augments HERALD with collaboration summarization and reporting."""

    def __init__(
        self,
        task_board: SharedTaskBoard,
        blackboard: CognitiveBlackboard,
    ):
        self.task_board = task_board
        self.blackboard = blackboard

    # ------------------------------------------------------------------
    # Collaboration Summary Generation
    # ------------------------------------------------------------------

    async def generate_collaboration_summary(self, session_title: str = "Session") -> Dict[str, Any]:
        """Generate a structured human-readable narrative of what the
        collaborative system accomplished.

        Reads the complete task board for the current session — every task,
        every review, every consensus outcome, every challenge — and produces
        a structured summary.

        Args:
            session_title: Title for the session being summarized

        Returns:
            A dict with narrative sections: overview, task_breakdown,
            reviews, challenges, consensus, key_decisions, recommendations
        """
        board_state = await self.task_board.get_board_state()
        tasks = self.task_board.get_all_tasks()

        # --- Overview ---
        completed = [t for t in tasks if t.status == TaskStatus.COMPLETED]
        active = [t for t in tasks if t.status == TaskStatus.ACTIVE]
        blocked = [t for t in tasks if t.status == TaskStatus.BLOCKED]
        under_review = [t for t in tasks if t.status == TaskStatus.UNDER_REVIEW]

        overview = (
            f"## {session_title} — Collaboration Summary\n\n"
            f"**Total tasks:** {board_state.total_tasks} | "
            f"**Completed:** {len(completed)} | "
            f"**Active:** {len(active)} | "
            f"**Blocked:** {len(blocked)} | "
            f"**Under review:** {len(under_review)} | "
            f"**Pending reviews:** {board_state.pending_reviews}\n"
        )

        # --- Task Breakdown by Type ---
        task_breakdown = "### Tasks by Type\n\n"
        for ttype, count in sorted(board_state.by_type.items()):
            task_breakdown += f"- **{ttype}:** {count}\n"

        # --- Completed Tasks Details ---
        task_details = "### Completed Tasks\n\n"
        if completed:
            for t in completed:
                task_details += (
                    f"- **{t.title}** [{t.task_type.value}] "
                    f"(owner: {t.owner.value if t.owner else 'N/A'}, "
                    f"priority: {t.priority})\n"
                )
                if t.results:
                    result_summary = str(t.results)[:150]
                    task_details += f"  - Result: {result_summary}\n"
        else:
            task_details += "No tasks completed yet.\n"

        # --- Active / Blocked Tasks ---
        active_blocked = "### Active & Blocked Tasks\n\n"
        if active:
            for t in active:
                active_blocked += (
                    f"- **ACTIVE:** {t.title} [{t.task_type.value}] "
                    f"— {t.owner.value if t.owner else 'unassigned'}\n"
                )
        if blocked:
            for t in blocked:
                last_event = t.events[-1] if t.events else None
                block_reason = last_event.detail if last_event else "Unknown"
                active_blocked += (
                    f"- **BLOCKED:** {t.title} [{t.task_type.value}] "
                    f"— {t.owner.value if t.owner else 'unassigned'}\n"
                    f"  - Reason: {block_reason[:120]}\n"
                )
        if not active and not blocked:
            active_blocked += "No active or blocked tasks.\n"

        # --- Review Activity ---
        reviews = "### Review Activity\n\n"
        review_count = 0
        for t in tasks:
            for review in t.review_requests:
                review_count += 1
                status = "✅ Approved" if review.approved else ("❌ Rejected" if review.approved is False else "⏳ Pending")
                reviews += (
                    f"- **{t.title[:40]}** {status}\n"
                    f"  - {review.requesting_specialist.value} → {review.reviewing_specialist.value}\n"
                    f"  - Q: {review.question[:80]}\n"
                )
        if review_count == 0:
            reviews += "No reviews requested.\n"

        # --- Consensus Activity ---
        consensus = "### Consensus Activity\n\n"
        consensus += "Consensus tracking available via engine metrics.\n"

        # --- Key Decisions ---
        decisions = "### Key Decisions\n\n"
        decision_entries = []
        for slot_name in ("security_escalations", "execution_approvals", "decisions"):
            slot = self.blackboard._slots.get(slot_name)
            if slot:
                for entry in slot.entries[-5:]:
                    decision_entries.append(f"- [{slot_name}] {entry.content[:120]}")
        if decision_entries:
            decisions += "\n".join(decision_entries) + "\n"
        else:
            decisions += "No key decisions recorded.\n"

        # --- Recommendations ---
        recommendations = "### Recommendations\n\n"
        if blocked:
            recommendations += "- **Unblock blocked tasks:** "
            recommendations += ", ".join(f"`{t.title[:30]}`" for t in blocked[:3]) + "\n"
        if under_review:
            recommendations += "- **Review pending submissions:** "
            recommendations += ", ".join(f"`{t.title[:30]}`" for t in under_review[:3]) + "\n"
        if not blocked and not under_review and not active:
            recommendations += "All tasks completed. Consider generating a final report.\n"

        # Compile
        summary = {
            "overview": overview,
            "task_breakdown": task_breakdown,
            "task_details": task_details,
            "active_blocked": active_blocked,
            "reviews": reviews,
            "consensus": consensus,
            "key_decisions": decisions,
            "recommendations": recommendations,
            "full_narrative": "\n\n".join([
                overview, task_breakdown, task_details,
                active_blocked, reviews, consensus,
                decisions, recommendations,
            ]),
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "total_tasks": board_state.total_tasks,
                "completed_count": len(completed),
                "active_count": len(active),
                "blocked_count": len(blocked),
                "pending_reviews": board_state.pending_reviews,
            },
        }

        return summary

    async def submit_summary_for_review(
        self,
        summary: Dict[str, Any],
        task_id: str = "",
    ) -> None:
        """Submit a collaboration summary to Architect for review.

        This creates a REVIEW task on the board. Architect can request
        revisions if the summary misrepresents what happened.
        """
        # Publish the summary to the blackboard
        entry = self.blackboard.publish(
            slot_name="collaboration_summaries",
            content=summary.get("full_narrative", ""),
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="HERALD",
            ),
            confidence=0.8,
            tags=["collaboration-summary", "herald"],
        )

        # If there's an existing synthesis task, add a review request
        if task_id:
            await self.task_board.request_review(
                task_id=task_id,
                from_specialist=SpecialistName.HERALD,
                to_specialist=SpecialistName.ARCHITECT,
                question="Please review the collaboration summary for accuracy and completeness.",
            )

        log.info(
            "HERALD submitted collaboration summary for review (entry=%s)",
            entry.id[:8],
        )

    # ------------------------------------------------------------------
    # Capability & Reliability Reporting
    # ------------------------------------------------------------------

    async def generate_capability_report(
        self,
        server_summaries: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Generate a report on MCP server capabilities, reliability trends,
        and trust state changes for human operators.

        Args:
            server_summaries: Optional list of MCP server status dicts

        Returns:
            A structured report with server status, capability counts,
            reliability scores, and recommendations
        """
        server_summaries = server_summaries or []

        report_lines = ["## MCP Platform Report\n"]
        report_lines.append(f"**Servers tracked:** {len(server_summaries)}\n")

        for server in server_summaries:
            name = server.get("name", "unknown")
            trust = server.get("trust_level", "UNKNOWN")
            health = server.get("health", "UNKNOWN")
            tools = server.get("tool_count", 0)
            reliability = server.get("reliability", 0.0)

            report_lines.append(f"- **{name}** [{trust}] [{health}]")
            report_lines.append(f"  - Tools: {tools} | Reliability: {reliability:.2f}")

        report = {
            "report": "\n".join(report_lines),
            "servers": server_summaries,
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source": "HERALD",
            },
        }
        return report

    async def generate_session_summary_for_user(
        self,
        session_title: str = "",
        user_request: str = "",
    ) -> str:
        """Generate a concise human-readable session summary suitable for
        presenting to the human operator.

        This is shorter and more narrative than the full collaboration
        summary. It focuses on what was accomplished, what decisions were
        made, and what remains to be done.
        """
        summary = await self.generate_collaboration_summary(session_title)
        meta = summary["metadata"]

        narrative = (
            f"# {'Session Summary' if not session_title else session_title}\n\n"
        )

        if user_request:
            narrative += f"**Original request:** {user_request}\n\n"

        narrative += (
            f"## What Was Accomplished\n\n"
            f"The collaborative system completed **{meta['completed_count']} tasks**, "
            f"with **{meta['active_count']} still active** "
            f"and **{meta['blocked_count']} blocked**.\n\n"
        )

        narrative += (
            f"## Current Status\n\n"
            f"- **Pending reviews:** {meta['pending_reviews']}\n"
            f"- **Active specialists:** "
        )

        # Determine active specialists from task board
        active_specialists = set()
        for task in self.task_board.get_all_tasks():
            if task.owner and task.status in (TaskStatus.ACTIVE, TaskStatus.ASSIGNED):
                active_specialists.add(task.owner.value)
        if active_specialists:
            narrative += ", ".join(sorted(active_specialists)) + "\n\n"
        else:
            narrative += "None\n\n"

        # Publish to blackboard
        self.blackboard.publish(
            slot_name="user_reports",
            content=narrative,
            entry_type=EntryType.FACT,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="HERALD",
            ),
            confidence=0.85,
            tags=["session-summary", "user-facing"],
        )

        return narrative
