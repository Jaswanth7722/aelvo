# herald.py - HERALD Strategic Communication Specialist for AELVO OMEGA

import re
import logging
from typing import Any, Dict, List, Optional, Tuple

from config.settings import BASE_DIR
from specialists.base import BaseSpecialist

log = logging.getLogger("aelvo.herald")


class HeraldSpecialist(BaseSpecialist):
    """HERALD serves as a tactical communication advisor, building strategic draft variations with explicit trade-offs."""

    name: str = "HERALD"
    trigger_patterns: List[str] = [
        "communicate", "message", "slack", "email", "draft", "negotiate",
        "strategy", "relationship", "tactful", "presentation", "verbal",
        "announcement", "pr message", "apology", "explain to manager"
    ]
    memory_types: List[str] = []
    required_tools: List[str] = []
    activation_threshold: float = 0.6

    def __init__(self):
        self.workspace = str(BASE_DIR)
        self._summary_reviews: List[Dict[str, Any]] = []

    def compute_activation_score(self, task: str, context: Dict[str, Any]) -> float:
        score = super().compute_activation_score(task, context)
        clean_task = (task or "").lower()
        if any(w in clean_task for w in ["draft a", "write an email", "how to tell", "reply to", "message to", "say to", "respond to"]):
            score += 0.35
        return min(1.0, max(0.0, score))

    def classify_communication(self, task: str) -> Dict[str, Any]:
        low = (task or "").lower()
        
        # 1. Stakes
        high_stakes_words = {"conflict", "upset", "angry", "fired", "slip", "delay", "regress", "negotiate", "apology", "apologize", "bad news", "mistake", "error", "escalat", "damage", "blame", "late", "missed", "deadline", "deadlines"}
        low_stakes_words = {"pr description", "commit message", "routine", "status update", "scaffold", "document"}
        
        if any(w in low for w in high_stakes_words):
            stakes = "high"
        elif any(w in low for w in low_stakes_words):
            stakes = "low"
        else:
            stakes = "medium"
            
        # 2. Channel
        channel = "unknown"
        for key in ("slack", "teams", "email", "pr", "doc", "verbal"):
            if key in low:
                channel = key
                break
        if channel == "unknown":
            if "message" in low or "announcement" in low:
                channel = "slack"
            elif "letter" in low or "send to" in low:
                channel = "email"
                
        # 3. Relationship
        relationship = "unknown"
        for key in ("manager", "peer", "report", "client", "external"):
            if key in low:
                relationship = key
                break
                
        # 4. Direction
        direction = "outbound"
        if "reply" in low or "respond" in low or "answer" in low:
            direction = "response"
            
        # 5. Emotional Tone
        emotional_tone = "neutral"
        if any(w in low for w in ("frustrated", "upset", "angry", "sorry", "apologize")):
            emotional_tone = "difficult"
        elif any(w in low for w in ("sensitive", "mistake", "fail", "bad news")):
            emotional_tone = "sensitive"
        elif any(w in low for w in ("happy", "thanks", "great", "celebrate")):
            emotional_tone = "positive"
            
        return {
            "stakes": stakes,
            "channel": channel,
            "relationship": relationship,
            "direction": direction,
            "emotional_tone": emotional_tone
        }

    def draft_variants(self, task: str, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        classification = self.classify_communication(task)
        classification["channel"]
        relationship = classification["relationship"]
        
        variants = []
        
        if classification["stakes"] == "high":
            # Strategy A: Hold Firm
            variants.append({
                "strategy_name": "Hold Firm",
                "what_it_prioritizes": "This strategy prioritizes strict adherence to project scope, standard operating procedures, and long-term codebase architecture.",
                "what_it_trades_off": "It trades off immediate political alignment and short-term convenience of the other party.",
                "draft": (
                    f"Hi {relationship if relationship != 'unknown' else 'Team'},\n\n"
                    "Regarding the request, we need to stick to the planned scope. Deviating from the current timeline "
                    "or architecture will introduce major regression risks and downstream maintenance costs that we "
                    "cannot afford at this stage. Let's focus on finishing our primary goals first."
                ),
                "when_to_use": "Use this when you have solid technical data backing you and conceding will cause major architecture issues."
            })
            
            # Strategy B: Seek Alignment
            variants.append({
                "strategy_name": "Seek Alignment",
                "what_it_prioritizes": "This strategy prioritizes de-escalation, long-term working relationships, and collaborative consensus.",
                "what_it_trades_off": "It trades off absolute technical preferences and requires making minor concessions.",
                "draft": (
                    f"Hi {relationship if relationship != 'unknown' else 'Team'},\n\n"
                    "I completely understand the urgency here. While we can't fully implement this without delaying the slip date, "
                    "could we find a middle ground by shipping a simplified version first? This keeps our relationship strong and "
                    "lets us gather initial feedback before committing to a full refactor."
                ),
                "when_to_use": "Use this when the relationship matters more than the immediate outcome of this single conflict."
            })
            
            # Strategy C: Escalate with Evidence
            variants.append({
                "strategy_name": "Escalate with Evidence",
                "what_it_prioritizes": "This strategy prioritizes speed of resolution, clarity of risk transparency, and executive accountability.",
                "what_it_trades_off": "It trades off immediate peer comfort and consumes political capital.",
                "draft": (
                    f"Hi {relationship if relationship != 'unknown' else 'Team'},\n\n"
                    "I want to make sure we're fully aligned on the risks of this decision. If we proceed as requested, "
                    "we will experience a launch delay of at least two weeks due to testing constraints. I've compiled "
                    "the exact data and alternatives for review so we can make an informed decision at the executive level."
                ),
                "when_to_use": "Use this when peer negotiations have reached a complete impasse and milestones are actively blocked."
            })
        else:
            # Routine communication: one draft is sufficient
            variants.append({
                "strategy_name": "Routine Inform",
                "what_it_prioritizes": "This strategy prioritizes prompt sharing of information and technical details.",
                "what_it_trades_off": "It trades off in-depth relationship building for transactional speed.",
                "draft": (
                    f"Hi {relationship if relationship != 'unknown' else 'Team'},\n\n"
                    f"Here is the update regarding {task}. The work is proceeding as planned. Let me know if you need any adjustments."
                ),
                "when_to_use": "Use this for standard day-to-day coordination updates."
            })
            
        return variants

    def suggest_not_answer(self, task: str, context: Dict[str, Any]) -> Dict[str, Any]:
        classification = self.classify_communication(task)
        high_tension = classification["stakes"] == "high"
        audience = classification["relationship"]
        
        if high_tension:
            recommendation = "Variant B — Seek Alignment (default) to preserve working relationships, unless prior attempts at alignment have already failed; in that case, transition to Variant C."
            gating_question = (
                "Have you already tried to surface this concern with this audience and been dismissed? "
                "If yes, Variant C is appropriate. If no, start with Variant B."
            )
        else:
            recommendation = "Variant A — Routine Inform, keeping it brief and focused."
            gating_question = "What is the single most important action or confirmation you need from the recipient?"
            
        observations = [
            f"Audience dynamic identified: {audience if audience != 'unknown' else 'stakeholder'}.",
            "High-stakes/tension signals detected. Tone must be carefully guarded." if high_tension else "Routine professional exchange. Keep it transactional and crisp.",
            "Channel constraints: ensure your formatting complies with terminal-friendly plain text."
        ]
        
        return {
            "observations": observations,
            "recommendation": recommendation,
            "gating_question": gating_question
        }

    def build_memory_context(self, task: str, memory_engine) -> Dict[str, Any]:
        """Reads user preferences only; HERALD writes nothing to memory."""
        project = getattr(memory_engine, "project_name", "default")
        user_preferences = []
        try:
            res = memory_engine.memory_collection.get(
                where={"type": "user_preference", "project": project},
                limit=5
            )
            user_preferences = res.get("documents", []) or []
        except Exception as _ex: log.debug("Silenced exception: %s", _ex)

        context = {
            "user_preferences": user_preferences,
            "project": project,
            "task": task
        }
        return {
            "user_preferences": user_preferences,
            "herald_variants": self.draft_variants(task, context),
            "herald_advisory": self.suggest_not_answer(task, context),
        }

    def get_system_prompt(self, context: Dict[str, Any]) -> str:
        # Identity
        identity = (
            "You are HERALD, AELVO's strategic communication and advisor specialist. "
            "Your role is to help the user navigate communication challenges with managers, stakeholders, "
            "and peers. You present options with explicit tradeoffs and act as a counselor, never deciding "
            "or writing unilaterally for the user."
        )

        # Locked constraints from anchor.md
        locked_rules = []
        fs = context.get("fs")
        kernel = getattr(fs, "kernel", None) if fs else None
        if kernel:
            try:
                data, _, _ = kernel._get_anchor_data()
                constraints = data.get("constraints", {})
                for key, val in constraints.items():
                    if isinstance(val, dict) and val.get("locked"):
                        locked_rules.append(f"HARD RULE: {key} = {val.get('value')}")
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)
        if not locked_rules:
            constraints = context.get("constraints", {}) or {}
            for key, val in constraints.items():
                if isinstance(val, dict) and val.get("locked"):
                    locked_rules.append(f"HARD RULE: {key} = {val.get('value')}")

        hard_rules_section = ""
        if locked_rules:
            hard_rules_section = "HARD CONSTRAINTS:\n" + "\n".join(locked_rules)

        # Budget
        budget = context.get("budget", 30)
        budget_line = f"STEPS REMAINING: {budget} — plan your actions carefully."

        # User voice profile
        user_prefs = context.get("user_preferences") or []
        comm_style = "moderate"
        casual_style = "professional"
        vocabulary = "standard engineering vocabulary"
        for pref in user_prefs:
            pref_str = str(pref).lower()
            if "communication_style" in pref_str:
                if "brief" in pref_str:
                    comm_style = "brief"
                elif "verbose" in pref_str:
                    comm_style = "verbose"
            if "casual" in pref_str:
                casual_style = "casual"
            elif "professional" in pref_str:
                casual_style = "professional"
            if "vocabulary" in pref_str:
                vocabulary = pref_str

        user_voice = f"USER VOICE: style = {comm_style} | vocabulary = {vocabulary} | tone = {casual_style}"

        # Context detected
        task = context.get("task", "")
        classification = self.classify_communication(task)
        context_summary = (
            f"COMMUNICATION CONTEXT:\n"
            f"  - Audience Stakes: {classification['stakes'].upper()}\n"
            f"  - Target Channel: {classification['channel'].upper()}\n"
            f"  - Relationship Dynamic: {classification['relationship'].upper()}\n"
            f"  - Direction: {classification['direction'].upper()}\n"
            f"  - Emotional Tone: {classification['emotional_tone'].upper()}"
        )

        # Precomputed variants & advisory
        variants = self.draft_variants(task, context)
        advisory = self.suggest_not_answer(task, context)

        variants_str = "DRAFT VARIANTS:\n"
        for idx, v in enumerate(variants):
            variants_str += (
                f"\nVariant {chr(65+idx)}: {v['strategy_name']}\n"
                f"  Prioritizes: {v['what_it_prioritizes']}\n"
                f"  Trades Off: {v['what_it_trades_off']}\n"
                f"  Draft Message:\n---\n{v['draft']}\n---\n"
                f"  When to Use: {v['when_to_use']}\n"
            )

        advisory_section = (
            f"ADVISORY NOTES:\n"
            f"  - Recommendation: {advisory['recommendation']}\n"
            f"  - Gating Question to Ask User: {advisory['gating_question']}"
        )

        # Output format
        output_format = (
            "OUTPUT:\n"
            "JSON array of tool calls. The respond tool's message field contains your advisory output, not a finished draft."
        )

        # Workflow
        if classification["stakes"] == "high":
            workflow = (
                "WORKFLOW (HIGH STAKES):\n"
                "1. Classify the communication context (channel, relationship, stakes)\n"
                "2. Identify competing goals and relationship dynamics explicitly\n"
                "3. Generate 2-3 strategic variants with different goals, not just different tones\n"
                "4. For each variant: name the strategy, what it prioritizes, what it trades off\n"
                "5. Present the variants to the user with a recommendation and the reasoning\n"
                "6. Do not make the decision for the user"
            )
        else:
            workflow = (
                "WORKFLOW (ROUTINE):\n"
                "1. Classify the channel and context\n"
                "2. Generate one draft optimized for the channel and relationship\n"
                "3. Note the one or two most important things the user should consider before sending\n"
                "4. Present the draft with those notes"
            )

        parts = [identity]
        if hard_rules_section:
            parts.append(hard_rules_section)
        parts.extend([
            budget_line,
            user_voice,
            context_summary,
            variants_str,
            advisory_section,
            output_format,
            workflow
        ])

        return "\n\n".join(parts)

    def post_process(self, result: str, memory_engine, conversation_history: List[Dict[str, str]]) -> str:
        """HERALD writes no memory. Returns audit trace."""
        user_msgs = [m.get("content", "") for m in conversation_history if m.get("role") == "user"]
        task = user_msgs[0] if user_msgs else ""
        classification = self.classify_communication(task)
        
        chosen_strategy = "unknown"
        if user_msgs:
            latest = user_msgs[-1].lower()
            if "variant a" in latest or "hold firm" in latest:
                chosen_strategy = "Hold Firm"
            elif "variant b" in latest or "seek alignment" in latest:
                chosen_strategy = "Seek Alignment"
            elif "variant c" in latest or "escalate" in latest or "invite input" in latest:
                chosen_strategy = "Escalate/Invite"
                
        stakes = classification["stakes"]
        channel = classification["channel"]
        relationship = classification["relationship"]
        
        return f"HERALD handled {stakes} {channel} communication for {relationship} audience. Strategy: {chosen_strategy}."

    def verify_output(self, output: str, context: Dict[str, Any]) -> Tuple[bool, str]:
        if not output:
            return True, "No output to verify."
            
        task = context.get("task", "")
        classification = self.classify_communication(task)
        is_high = classification["stakes"] == "high"
        channel = classification["channel"]
        
        low = output.lower()
        if low.lstrip().startswith(("{", "[")) and '"tool"' in low:
            return True, "Tool-call envelope; HERALD verification deferred."

        if is_high:
            variants_found = re.findall(r"\b(variant|strategy|option)\s+[a-c1-3]\b", low)
            if len(variants_found) < 2 and "hold firm" not in low and "seek alignment" not in low:
                return False, "High-stakes communication requires at least 2 strategic variants with explicit tradeoffs."
                
            decision_words = ["i have chosen", "i decided", "you must send", "you should send only", "here is the final answer"]
            if any(dw in low for dw in decision_words):
                return False, "HERALD must present options, not make decisions for the user on high-stakes communications."
                
            tradeoff_markers = ["prioritize", "trade-off", "tradeoff", "risk"]
            if not any(tm in low for tm in tradeoff_markers):
                return False, "Variants are missing explicit tradeoff or prioritization statements."
        else:
            if channel == "slack" and len(output.split()) > 200:
                return False, "Communication length check failed: Slack messages should be concise (under 200 words)."
            if channel == "doc" and len(output.split()) < 10:
                return False, "Communication length check failed: Document scaffolds should be substantive and well-structured."
                
        return True, "verified"

    # =====================================================================
    # Session-scoped accumulated data (populated via blackboard.subscribe callbacks)
    # =====================================================================

    def setup_subscriptions(self, blackboard: Any) -> None:
        """Subscribe to blackboard slots for automatic data accumulation.

        Registers callbacks on ``collaboration_summaries`` slot
        so HERALD automatically receives summary review results
        (approvals/rejections) without polling.

        Call this once per session, before any phase execution.
        """
        self._summary_reviews = []

        def _on_summary_review(entry: Any) -> None:
            from cognition.blackboard_schemas import ApprovalEntry, RejectionEntry
            try:
                data = ApprovalEntry.from_entry_content(entry.content)
                self._summary_reviews.append({"type": "approval", "data": data})
                return
            except Exception:
                pass
            try:
                data = RejectionEntry.from_entry_content(entry.content)
                self._summary_reviews.append({"type": "rejection", "data": data})
            except Exception:
                pass

        blackboard.subscribe("collaboration_summaries", _on_summary_review)

    def clear_session(self) -> None:
        """Clear accumulated subscription data between sessions."""
        self._summary_reviews = []

    # =====================================================================
    # Blackboard-Based Collaboration  (Amendment 2 — no agent-to-agent messaging)
    # =====================================================================

    def pickup_task(
        self,
        task_board: Any,
        task_type: Optional[Any] = None,
        max_tasks: int = 1,
    ) -> List[Any]:
        """Pick up pending REPORT tasks from the SharedTaskBoard.

        Looks for PENDING or ASSIGNED tasks matching the specified type
        (default: REPORT) and claims them by advancing to IN_PROGRESS.

        This is how HERALD discovers work in Mode B — by polling the
        task board, NOT by receiving direct messages.

        Args:
            task_board: A ``SharedTaskBoard`` instance.
            task_type: ``TaskType`` filter (defaults to ``TaskType.REPORT``).
            max_tasks: Maximum number of tasks to pick up.

        Returns:
            List of ``Task`` objects that were picked up.
        """
        if task_board is None:
            return []

        from shared_task_board.task import TaskStatus, TaskType

        if task_type is None:
            task_type = TaskType.REPORT

        picked = []
        for status in (TaskStatus.PENDING, TaskStatus.ASSIGNED):
            tasks = task_board.get_tasks(
                status=status,
                task_type=task_type,
                limit=max_tasks * 3,
            )
            for task in tasks:
                if len(picked) >= max_tasks:
                    break
                if task.specialist and task.specialist.upper() != "HERALD":
                    continue
                if status == TaskStatus.PENDING:
                    task_board.assign_task(
                        task.id,
                        specialist="HERALD",
                        assigned_by="architect",
                    )
                task_board.start_task(task.id)
                picked.append(task)
                log.info(
                    "HERALD picked up task %s: %s",
                    task.id[:12], task.title[:60],
                )

        return picked

    def generate_collaboration_summary(
        self,
        blackboard: Any,
        task_board: Any = None,
        session_title: str = "Session Summary",
    ) -> Dict[str, Any]:
        """Generate a structured collaboration summary from blackboard state.

        Reads all major blackboard slots and compiles a narrative of what
        happened during a collaborative session.  The summary is structured
        into: overview, findings, implementations, reviews, escalations,
        execution results, and recommendations.

        No direct messaging.  All data comes from the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            task_board: An optional ``SharedTaskBoard`` for task stats.
            session_title: Title for the summary.

        Returns:
            Dict with narrative sections and metadata.
        """
        if blackboard is None:
            return self._empty_summary(session_title)

        from cognition.blackboard_schemas import (
            FindingEntry,
            ImplementationEntry,
            ApprovalEntry,
            RejectionEntry,
            EscalationEntry,
            ExecutionResultEntry,
        )
        from cognition.types import EntryType

        # ── Read all slots ────────────────────────────────────────

        # Research findings
        findings_entries = blackboard.read(
            slot_name="research_findings", entry_type=EntryType.FINDING,
        )
        findings_list = []
        for e in findings_entries[-10:]:
            try:
                f = FindingEntry.from_entry_content(e.content)
                findings_list.append(f.summary[:100])
            except Exception:
                findings_list.append(e.content[:100])

        # Implementations
        impl_entries = blackboard.read(
            slot_name="implementations", entry_type=EntryType.FINDING,
        )
        impl_list = []
        for e in impl_entries[-10:]:
            try:
                impl = ImplementationEntry.from_entry_content(e.content)
                impl_list.append(impl.summary[:100])
            except Exception:
                impl_list.append(e.content[:100])

        # Reviews (approvals + rejections)
        review_entries = blackboard.read(slot_name="reviews")
        approvals = 0
        rejections = 0
        for e in review_entries:
            try:
                ApprovalEntry.from_entry_content(e.content)
                approvals += 1
                continue
            except Exception:
                pass
            try:
                RejectionEntry.from_entry_content(e.content)
                rejections += 1
            except Exception:
                pass

        # Security escalations
        esc_entries = blackboard.read(
            slot_name="security_escalations", entry_type=EntryType.DECISION,
        )
        esc_list = []
        for e in esc_entries[-5:]:
            try:
                esc = EscalationEntry.from_entry_content(e.content)
                esc_list.append(esc.reason[:120])
            except Exception:
                esc_list.append(e.content[:120])

        # Execution results
        exec_entries = blackboard.read(
            slot_name="execution_results", entry_type=EntryType.FACT,
        )
        success_count = 0
        failure_count = 0
        for e in exec_entries:
            try:
                r = ExecutionResultEntry.from_entry_content(e.content)
                if r.success:
                    success_count += 1
                else:
                    failure_count += 1
            except Exception:
                pass

        # Architect decisions
        arch_entries = blackboard.read(
            slot_name="architect_decisions", entry_type=EntryType.DECISION,
        )

        # Task board stats
        task_stats = {}
        if task_board is not None:
            try:
                all_tasks = task_board.get_tasks()
                completed = sum(
                    1 for t in all_tasks if getattr(t, "status", "").value == "completed"
                )
                active = sum(
                    1 for t in all_tasks if getattr(t, "status", "").value == "in_progress"
                )
                total = len(all_tasks)
                task_stats = {
                    "total": total,
                    "completed": completed,
                    "active": active,
                    "blocked": total - completed - active,
                }
            except Exception as e:
                log.debug("Failed to read task board: %s", e)

        # ── Compile narrative ─────────────────────────────────────

        overview = (
            f"## {session_title}\n\n"
        )
        if task_stats:
            overview += (
                f"**Total tasks:** {task_stats['total']} | "
                f"**Completed:** {task_stats['completed']} | "
                f"**Active:** {task_stats['active']} | "
                f"**Blocked:** {task_stats['blocked']}\n\n"
            )

        findings_section = "### Research Findings\n\n"
        if findings_list:
            for f in findings_list:
                findings_section += f"- {f}\n"
        else:
            findings_section += "No research findings recorded.\n"

        impl_section = "### Implementations\n\n"
        if impl_list:
            for i in impl_list:
                impl_section += f"- {i}\n"
        else:
            impl_section += "No implementations submitted.\n"

        reviews_section = "### Review Activity\n\n"
        reviews_section += f"- **Approvals:** {approvals}\n"
        reviews_section += f"- **Rejections:** {rejections}\n"
        if review_entries:
            reviews_section += f"- **Total reviews:** {len(review_entries)}\n"

        exec_section = "### Execution Results\n\n"
        exec_section += f"- **Successful:** {success_count}\n"
        exec_section += f"- **Failed:** {failure_count}\n"
        if exec_entries:
            exec_section += f"- **Total executions:** {len(exec_entries)}\n"

        esc_section = "### Security Escalations\n\n"
        if esc_list:
            for s in esc_list:
                esc_section += f"- ⚠ {s}\n"
        else:
            esc_section += "No security escalations.\n"

        arch_section = "### Architect Decisions\n\n"
        if arch_entries:
            arch_section += f"- **Total decisions:** {len(arch_entries)}\n"
            for e in arch_entries[-3:]:
                arch_section += f"  - {e.content[:100]}\n"
        else:
            arch_section += "No architect decisions recorded.\n"

        recommendations_section = "### Recommendations\n\n"
        recs = []
        if rejections > 0:
            recs.append(f"- Address {rejections} rejection(s) before proceeding")
        if failure_count > 0:
            recs.append(f"- Investigate {failure_count} execution failure(s)")
        if esc_list:
            recs.append(f"- Review {len(esc_list)} security escalation(s)")
        if not recs:
            recs.append("- All clear. No issues detected.")
        recommendations_section += "\n".join(recs) + "\n"

        full_narrative = "\n\n".join([
            overview, findings_section, impl_section,
            reviews_section, exec_section, esc_section,
            arch_section, recommendations_section,
        ])

        return {
            "overview": overview,
            "findings": findings_section,
            "implementations": impl_section,
            "reviews": reviews_section,
            "execution": exec_section,
            "escalations": esc_section,
            "architect_decisions": arch_section,
            "recommendations": recommendations_section,
            "full_narrative": full_narrative,
            "metadata": {
                "generated_by": "HERALD",
                "session_title": session_title,
                "finding_count": len(findings_list),
                "implementation_count": len(impl_list),
                "approval_count": approvals,
                "rejection_count": rejections,
                "escalation_count": len(esc_list),
                "execution_success_count": success_count,
                "execution_failure_count": failure_count,
                "architect_decision_count": len(arch_entries),
                "task_stats": task_stats,
            },
        }

    def _empty_summary(self, session_title: str) -> Dict[str, Any]:
        """Return an empty summary when blackboard is unavailable."""
        empty = f"## {session_title}\n\nNo blackboard data available.\n"
        return {
            "overview": empty,
            "findings": "",
            "implementations": "",
            "reviews": "",
            "execution": "",
            "escalations": "",
            "architect_decisions": "",
            "recommendations": "",
            "full_narrative": empty,
            "metadata": {
                "generated_by": "HERALD",
                "session_title": session_title,
                "error": "No blackboard available",
            },
        }

    def submit_summary_for_review(
        self,
        blackboard: Any,
        summary: Dict[str, Any],
        task_id: str = "",
    ) -> str:
        """Submit a collaboration summary to the blackboard for Architect review.

        Publishes the summary narrative to the ``collaboration_summaries``
        blackboard slot.  Architect monitors this slot and reviews the
        summary for accuracy and completeness.

        No direct messaging.  Summaries flow through the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            summary: The summary dict from ``generate_collaboration_summary()``.
            task_id: Optional task ID this summary relates to.

        Returns:
            The blackboard entry ID for the published summary.
        """
        if blackboard is None:
            return ""

        from cognition.types import EntryType, Provenance, ProvenanceType

        narrative = summary.get("full_narrative", "") or summary.get("overview", "")
        entry = blackboard.publish(
            slot_name="collaboration_summaries",
            content=narrative,
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="HERALD",
            ),
            confidence=0.85,
            tags=["collaboration-summary", "herald", "needs-review"]
            + ([f"task:{task_id}"] if task_id else []),
        )
        log.info(
            "HERALD submitted collaboration summary for review (entry=%s)",
            entry.id[:8],
        )
        return entry.id

    def check_for_summary_review(
        self,
        blackboard: Any,
        max_results: int = 5,
    ) -> List[Any]:
        """Check the blackboard for review results on submitted summaries.

        Reads entries from the ``collaboration_summaries`` slot that have
        been reviewed by Architect.  Looks for ApprovalEntry or RejectionEntry
        payloads linked to summary entries.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_results: Maximum results to return.

        Returns:
            List of dicts with ``type`` ('approval' or 'rejection')
            and ``data`` (the parsed schema instance).
        """
        if blackboard is None:
            return []

        from cognition.blackboard_schemas import ApprovalEntry, RejectionEntry

        entries = blackboard.read(slot_name="collaboration_summaries")
        results = []
        for entry in entries[:max_results]:
            try:
                data = ApprovalEntry.from_entry_content(entry.content)
                results.append({"type": "approval", "data": data})
                continue
            except Exception:
                pass
            try:
                data = RejectionEntry.from_entry_content(entry.content)
                results.append({"type": "rejection", "data": data})
                continue
            except Exception:
                pass
            log.debug("Failed to parse summary review entry: %s", entry.id[:8])

        return results

    # =====================================================================
    # Expanded Reporting — Priority 8: Herald Evolution
    # =====================================================================

    def generate_session_report(
        self,
        blackboard: Any,
        summary: Optional[Dict[str, Any]] = None,
        session_title: str = "Session Report",
    ) -> str:
        """Publish a user-facing session report to the blackboard.

        Generates a concise report suitable for presenting to the human
        operator, then publishes it to the ``user_reports`` blackboard slot.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            summary: Optional pre-generated summary. If None, generates one.
            session_title: Title for the report.

        Returns:
            The blackboard entry ID for the published report.
        """
        if blackboard is None:
            return ""

        from cognition.types import EntryType, Provenance, ProvenanceType

        if summary is None:
            summary = self.generate_collaboration_summary(
                blackboard, session_title=session_title,
            )

        report = summary.get("full_narrative", "")
        if not report:
            report = f"## {session_title}\n\nNo summary data available.\n"

        # Add a user-facing header
        meta = summary.get("metadata", {})
        report_header = (
            f"# {session_title}\n\n"
            f"**Generated by:** HERALD\n"
        )
        if meta.get("finding_count", 0) or meta.get("implementation_count", 0):
            report_header += (
                f"**Activity:** {meta.get('finding_count', 0)} findings, "
                f"{meta.get('implementation_count', 0)} implementations, "
                f"{meta.get('approval_count', 0)} approvals, "
                f"{meta.get('rejection_count', 0)} rejections\n"
            )
        report = report_header + "\n" + report

        entry = blackboard.publish(
            slot_name="user_reports",
            content=report,
            entry_type=EntryType.FACT,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="HERALD",
            ),
            confidence=0.9,
            tags=["session-report", "herald", "user-facing"],
        )
        log.info(
            "HERALD published session report (entry=%s)",
            entry.id[:8],
        )
        return entry.id

    def generate_evidence_summary(
        self,
        blackboard: Any,
        max_evidence: int = 20,
    ) -> Dict[str, Any]:
        """Generate a focused summary of all evidence on the blackboard.

        Uses the evidence() method to produce a structured summary of
        all active evidence, including trust metadata and lifecycle
        state for each item.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_evidence: Maximum evidence items to include.

        Returns:
            Dict with evidence list, counts, and trust summary.
        """
        if blackboard is None or not hasattr(blackboard, 'evidence'):
            return {"evidence": [], "total": 0, "trust_summary": {}}

        evidence_list = blackboard.evidence()
        total = len(evidence_list)
        items = evidence_list[:max_evidence]

        # Count by type and owner
        by_type: Dict[str, int] = {}
        by_owner: Dict[str, int] = {}
        for ev in items:
            by_type[ev.evidence_type] = by_type.get(ev.evidence_type, 0) + 1
            by_owner[ev.owner_agent] = by_owner.get(ev.owner_agent, 0) + 1

        # Build trust summary using the trust layer
        try:
            from cognition.trust import TrustReport
            trust_report = TrustReport.from_evidence_list(items)
            trust_summary = trust_report.to_summary()
        except ImportError:
            trust_summary = "Trust layer not available"

        return {
            "total": total,
            "max_evidence": max_evidence,
            "by_type": by_type,
            "by_owner": by_owner,
            "trust_summary": trust_summary,
            "evidence": [
                {
                    "id": ev.id[:12],
                    "owner": ev.owner_agent,
                    "type": ev.evidence_type,
                    "confidence": ev.confidence,
                    "lifecycle": ev.lifecycle_status.value if hasattr(ev, 'lifecycle_status') else "",
                    "verification": ev.verification_status.value if hasattr(ev, 'verification_status') else "",
                    "summary": ev.summary[:80],
                }
                for ev in items
            ],
        }

    def generate_consensus_summary(
        self,
        blackboard: Any,
        max_results: int = 10,
    ) -> Dict[str, Any]:
        """Summarise consensus outcomes from the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_results: Maximum consensus entries to include.

        Returns:
            Dict with consensus positions, outcomes, and recommendations.
        """
        if blackboard is None:
            return {"consensus_events": [], "total": 0}

        from cognition.types import EntryType

        entries = blackboard.read(
            slot_name="consensus",
            entry_type=EntryType.DECISION,
        )
        total = len(entries)
        items = entries[:max_results]

        return {
            "total": total,
            "consensus_events": [
                {
                    "entry_id": e.id[:12],
                    "summary": e.content[:120],
                    "confidence": e.confidence,
                    "owner": e.provenance.source_id if e.provenance else "",
                }
                for e in items
            ],
        }

    def generate_architect_decision_summary(
        self,
        blackboard: Any,
        max_results: int = 10,
    ) -> Dict[str, Any]:
        """Summarise Architect decisions from the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_results: Maximum decisions to include.

        Returns:
            Dict with decision list and counts.
        """
        if blackboard is None:
            return {"decisions": [], "total": 0}

        from cognition.types import EntryType

        entries = blackboard.read(
            slot_name="architect_decisions",
            entry_type=EntryType.DECISION,
        )
        total = len(entries)
        items = entries[:max_results]

        approvals = sum(1 for e in items if "approve" in e.content.lower() or "approved" in e.content.lower())
        rejections = sum(1 for e in items if "reject" in e.content.lower() or "rejected" in e.content.lower())

        return {
            "total": total,
            "approvals": approvals,
            "rejections": rejections,
            "decisions": [
                {
                    "entry_id": e.id[:12],
                    "content": e.content[:120],
                    "confidence": e.confidence,
                }
                for e in items
            ],
        }

    def generate_agent_contribution_summary(
        self,
        blackboard: Any,
    ) -> Dict[str, Any]:
        """Summarise contributions by each agent.

        Reads all blackboard evidence and groups by owner_agent
        to show how each specialist contributed to the session.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.

        Returns:
            Dict mapping agent name to contribution stats.
        """
        if blackboard is None or not hasattr(blackboard, 'evidence'):
            return {"agents": {}}

        evidence_list = blackboard.evidence()
        agent_stats: Dict[str, Dict[str, Any]] = {}

        for ev in evidence_list:
            agent = ev.owner_agent or "unknown"
            if agent not in agent_stats:
                agent_stats[agent] = {
                    "total": 0,
                    "by_type": {},
                    "avg_confidence": 0.0,
                    "confidence_sum": 0.0,
                    "challenged": 0,
                }
            stats = agent_stats[agent]
            stats["total"] += 1
            etype = ev.evidence_type or "unknown"
            stats["by_type"][etype] = stats["by_type"].get(etype, 0) + 1
            stats["confidence_sum"] += ev.confidence
            if hasattr(ev, 'lifecycle_status') and ev.lifecycle_status.value == "challenged":
                stats["challenged"] += 1

        # Compute averages
        for agent, stats in agent_stats.items():
            stats["avg_confidence"] = round(
                stats["confidence_sum"] / stats["total"], 4
            ) if stats["total"] > 0 else 0.0
            del stats["confidence_sum"]

        return {
            "agents": agent_stats,
            "total_agents": len(agent_stats),
        }

    def generate_recovery_summary(
        self,
        blackboard: Any,
        max_results: int = 10,
    ) -> Dict[str, Any]:
        """Summarise recovery events from the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_results: Maximum recovery entries to include.

        Returns:
            Dict with recovery event list and success/failure counts.
        """
        if blackboard is None:
            return {"recoveries": [], "total": 0}

        from cognition.types import EntryType

        entries = blackboard.read(
            slot_name="recovery_events",
            entry_type=EntryType.OBSERVATION,
        )
        total = len(entries)
        items = entries[:max_results]

        successes = sum(1 for e in items if "success" in e.content.lower() or "recovered" in e.content.lower())
        failures = sum(1 for e in items if "fail" in e.content.lower() or "error" in e.content.lower())

        return {
            "total": total,
            "successes": successes,
            "failures": failures,
            "recoveries": [
                {
                    "entry_id": e.id[:12],
                    "content": e.content[:120],
                }
                for e in items
            ],
        }

