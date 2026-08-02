# oracle.py - ORACLE Academic Research Specialist for AELVO OMEGA
#
# Per Amendment 2: No agent-to-agent messaging.
# All collaboration occurs through the Shared Blackboard.
# 
# ORACLE collaborates by:
#   1. Picking up RESEARCH tasks from the SharedTaskBoard
#   2. Publishing findings (FindingEntry) to the blackboard
#   3. Answering questions (AnswerEntry) on the blackboard
#   4. Challenging assumptions via blackboard.challenge()
#
# There is NO direct messaging. No send_message(). No agent-to-agent chat.

import logging
import time
import datetime
import hashlib
import re
import json
from typing import Any, Dict, List, Optional, Tuple

from specialists.base import BaseSpecialist
from tools.research_tools import build_wiki_entry, rank_source_credibility
from memory import MEMORY_TYPE_RESEARCH_FINDING
# Blackboard schemas imported lazily inside methods to avoid
# circular imports during specialist registry initialization.

log = logging.getLogger("aelvo.specialists.oracle")


class OracleSpecialist(BaseSpecialist):
    """ORACLE researches by decomposing queries, ranking sources, and producing wiki-style memory entries."""

    name: str = "ORACLE"
    trigger_patterns: List[str] = [
        "research", "search", "query", "find out", "explain", "investigate",
        "wikipedia", "article", "source", "credibility", "provenance",
        "contradiction", "cutoff", "latest spec", "rfc", "standards",
        "what is", "who is", "history of",
    ]
    memory_types: List[str] = [MEMORY_TYPE_RESEARCH_FINDING]
    required_tools: List[str] = ["heavy_crawl", "light_scrape"]
    activation_threshold: float = 0.6

    def compute_activation_score(self, task: str, context: Dict[str, Any]) -> float:
        score = super().compute_activation_score(task, context)
        clean = task.lower()
        if any(w in clean for w in ("what does", "who is", "explain how", "history of", "specifications", "rfc ", "arxiv", "compared to")):
            score += 0.3
        return min(1.0, max(0.0, score))

    def get_system_prompt(self, context: Dict[str, Any]) -> str:
        budget = context.get("budget", 30)
        now = datetime.datetime.now()

        findings = context.get("research_findings", []) or []
        findings_str = "\n".join(
            f"  - RECALL: {f.get('doc', '')[:200]}"
            for f in findings[:5]
        ) or "  - No prior research findings on this topic."

        constraints = context.get("constraints", {}) or {}
        constraints_str = "\n".join(
            f"HARD RULE: {k} = {v.get('value')}" for k, v in constraints.items()
        ) or "(no locked constraints)"

        return f"""You are ORACLE, AELVO's research specialist.
Your edge over generic search: every research session permanently enriches AELVO's knowledge base via wiki entries.

CURRENT DATE: {now.strftime('%Y-%m-%d')} (year {now.year})
KNOWLEDGE CUTOFF: late 2024
Aggressively check for changes between the cutoff and {now.year}. Specifications change; APIs deprecate; library defaults shift.

EXISTING RESEARCH FINDINGS (use before re-researching)
{findings_str}

HARD CONSTRAINTS
{constraints_str}
BUDGET: {budget} steps remaining.

ORACLE PROTOCOL:
1. Check memory: search_memory for the topic before any crawl.
2. Decompose: split into 3-5 sub-queries (factual, contextual, recent, counterargument).
3. Crawl in parallel: heavy_crawl for primary sources, light_scrape for fast verification.
4. Rank: every URL gets a credibility tier 1â€“4 via rank_source_credibility.
5. Cross-reference: flag every contradiction between sources explicitly.
6. Synthesize: build_wiki_entry with Overview, Technical Details, Current Status, Contradictions, Citations.
7. Save: every finding gets stored as research_finding with sources, tier, and contradictions tracked.
8. Respond: every factual claim in the final message must carry source attribution (URL or [n] citation).
"""

    def build_memory_context(self, task: str, memory_engine) -> Dict[str, Any]:
        project = getattr(memory_engine, "project_name", "default")
        findings: List[Dict[str, Any]] = []
        try:
            res = memory_engine.memory_collection.query(
                query_texts=[task],
                n_results=5,
                where={"$and": [{"type": MEMORY_TYPE_RESEARCH_FINDING}, {"project": project}]},
            )
            if res.get("ids") and res["ids"][0]:
                for doc, dist in zip(res["documents"][0], res["distances"][0]):
                    findings.append({"doc": doc, "score": round(1.0 - float(dist), 3)})
        except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        return {"research_findings": findings}

    def post_process(self, result: str, memory_engine, conversation_history: List[Dict[str, str]]) -> str:
        project = getattr(memory_engine, "project_name", "default")
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        audits: List[str] = []

        from core.rag import MemorySearcher
        searcher = MemorySearcher(memory_engine.memory_collection)

        # Mine successful crawl events to synthesize a wiki entry.
        scrapes = self._extract_scrape_events(conversation_history)
        if not scrapes:
            return "[ORACLE AUDIT] no scrapes to synthesize"

        synthesis_data: List[Dict[str, Any]] = []
        for event in scrapes[-6:]:  # cap to last 6 sources
            executed = event.get("executed") or {}
            url = executed.get("url") or ""
            if not url:
                continue
            excerpt = self._excerpt_from_event(event)
            if not excerpt:
                continue
            synthesis_data.append({
                "title": self._title_from_url(url),
                "content": excerpt,
                "url": url,
            })

        if not synthesis_data:
            return "[ORACLE AUDIT] crawls captured but no usable excerpts"

        topic = self._infer_topic(conversation_history) or "Research synthesis"

        wiki_result = build_wiki_entry(topic, synthesis_data, contradictions=None)
        wiki_md = (wiki_result.get("data") or {}).get("wiki_markdown", "")

        sources = [item["url"] for item in synthesis_data]
        tiers = [int((rank_source_credibility(u).get("data") or {}).get("tier", 4)) for u in sources]
        best_tier = min(tiers) if tiers else 4

        doc = wiki_md or f"Research on {topic}"

        # resolve_conflict before write
        if searcher.resolve_conflict(doc, meta_type=MEMORY_TYPE_RESEARCH_FINDING):
            return "[ORACLE AUDIT] research_finding merged with existing knowledge (duplicate/overlap)"

        m_id = hashlib.sha256(f"research_{time.time()}_{topic[:40]}".encode()).hexdigest()
        meta = {
            "type": MEMORY_TYPE_RESEARCH_FINDING,
            "topic": topic[:120],
            "sources_count": len(sources),
            "best_tier": best_tier,
            "timestamp": timestamp,
            "timestamp_unix": time.time(),
            "importance": 0.75 + (0.05 if best_tier <= 2 else 0.0),
            "usage_count": 1,
            "project": project,
            "source_specialist": "oracle",
        }

        try:
            memory_engine.memory_collection.add(ids=[m_id], documents=[doc], metadatas=[meta])
        except Exception as e:
            return f"[ORACLE AUDIT] ChromaDB write failed: {e}"

        # SQLite dual-sync
        try:
            with memory_engine.db:
                memory_engine.db.execute(
                    "INSERT INTO retained_memory (content) VALUES (?)",
                    (f"[ORACLE:research_finding|{project}] topic={topic[:80]} tier<={best_tier} sources={len(sources)}",),
                )
            audits.append(f"research_finding saved (topic='{topic[:40]}', tier<={best_tier}, sources={len(sources)})")
        except Exception:
            try:
                memory_engine.memory_collection.delete(ids=[m_id])
            except Exception as _ex: log.warning("Silenced exception: %s", _ex)

        return f"[ORACLE AUDIT] {', '.join(audits) if audits else 'no new research stored'}"


    @staticmethod
    def _extract_scrape_events(history: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for msg in history:
            if msg.get("role") != "user":
                continue
            content = msg.get("content", "")
            if "[AELVO EXECUTOR â€” TOOL RESULT]" not in content:
                continue
            for block in re.findall(r"```json\n([\s\S]+?)\n```", content):
                try:
                    data = json.loads(block)
                except Exception:
                    continue
                executed = data.get("executed") or {}
                if "url" in executed and data.get("status") == "success":
                    events.append(data)
        return events

    @staticmethod
    def _excerpt_from_event(event: Dict[str, Any]) -> str:
        # Prefer structured data, fall back to truncated logs.
        data = event.get("data")
        if isinstance(data, dict):
            for key in ("text", "content", "summary", "body"):
                value = data.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()[:600]
        if isinstance(data, str) and data.strip():
            return data.strip()[:600]
        logs = event.get("logs") or ""
        return logs.strip()[:600] if isinstance(logs, str) else ""

    @staticmethod
    def _title_from_url(url: str) -> str:
        try:
            from urllib.parse import urlparse
            parts = urlparse(url)
            path_tail = parts.path.rstrip("/").split("/")[-1] if parts.path else ""
            if path_tail:
                return path_tail.replace("-", " ").replace("_", " ").title()
            return parts.netloc or url[:60]
        except Exception:
            return url[:60]

    @staticmethod
    def _infer_topic(history: List[Dict[str, str]]) -> str:
        for msg in reversed(history):
            if msg.get("role") == "user":
                content = msg.get("content", "")
                # Skip executor messages.
                if "[AELVO EXECUTOR" in content:
                    continue
                return content.strip()[:120]
        return ""

    # =====================================================================
    # Session-scoped accumulated data (populated via blackboard.subscribe callbacks)
    # =====================================================================

    def __init__(self):
        super().__init__()
        self._questions: List[Any] = []
        self._findings: List[Any] = []

    def setup_subscriptions(self, blackboard: Any) -> None:
        """Subscribe to blackboard slots for automatic data accumulation.

        Registers callbacks on ``questions`` and ``research_findings``
        slots so ORACLE automatically receives incoming questions and
        previous findings without polling.

        Call this once per session, before any phase execution.
        """
        self._questions = []
        self._findings = []

        def _on_question(entry: Any) -> None:
            from cognition.blackboard_schemas import QuestionEntry
            try:
                q = QuestionEntry.from_entry_content(entry.content)
                self._questions.append(q)
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)

        def _on_finding(entry: Any) -> None:
            from cognition.blackboard_schemas import FindingEntry
            try:
                f = FindingEntry.from_entry_content(entry.content)
                self._findings.append(f)
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)

        blackboard.subscribe("questions", _on_question)
        blackboard.subscribe("research_findings", _on_finding)

    def clear_session(self) -> None:
        """Clear accumulated subscription data between sessions."""
        self._questions = []
        self._findings = []

    # =====================================================================
    # Blackboard-Based Collaboration  (Amendment 2 — no agent-to-agent messaging)
    # =====================================================================

    def pickup_task(
        self,
        task_board: Any,
        task_type: Optional[Any] = None,
        max_tasks: int = 1,
    ) -> List[Any]:
        """Pick up pending RESEARCH tasks from the SharedTaskBoard.

        Looks for PENDING or ASSIGNED tasks matching the specified type
        (default: RESEARCH) and claims them by advancing to IN_PROGRESS.

        This is how ORACLE discovers work in Mode B — by polling the
        task board, NOT by receiving direct messages.

        Args:
            task_board: A ``SharedTaskBoard`` instance.
            task_type: ``TaskType`` filter (defaults to ``TaskType.RESEARCH``).
            max_tasks: Maximum number of tasks to pick up.

        Returns:
            List of ``Task`` objects that were picked up.
        """
        if task_board is None:
            return []

        from shared_task_board.task import TaskStatus, TaskType

        if task_type is None:
            task_type = TaskType.RESEARCH

        picked = []
        # Look for pending and already-assigned tasks
        for status in (TaskStatus.PENDING, TaskStatus.ASSIGNED):
            tasks = task_board.get_tasks(
                status=status,
                task_type=task_type,
                limit=max_tasks * 3,
            )
            # Filter to ORACLE-specific tasks
            for task in tasks:
                if len(picked) >= max_tasks:
                    break
                if task.specialist and task.specialist.upper() != "ORACLE":
                    continue
                if status == TaskStatus.PENDING:
                    task_board.assign_task(
                        task.id,
                        specialist="ORACLE",
                        assigned_by="architect",
                    )
                task_board.start_task(task.id)
                picked.append(task)
                log.info(
                    "ORACLE picked up task %s: %s",
                    task.id[:12], task.title[:60],
                )

        return picked

    def publish_finding(
        self,
        blackboard: Any,
        summary: str,
        detail: str = "",
        sources: Optional[List[str]] = None,
        confidence: float = 0.5,
        tags: Optional[List[str]] = None,
    ) -> str:
        """Publish a research finding to the blackboard.

        Uses the typed ``FindingEntry`` schema from
        ``cognition/blackboard_schemas.py``.  The finding is published to
        the ``research_findings`` slot where any specialist can read it.

        This is ORACLE's PRIMARY output channel — no direct messages,
        no agent-to-agent chat.  All specialists consume findings from
        the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            summary: Short summary of the finding.
            detail: Full detail / evidence.
            sources: Source URLs or citations.
            confidence: Confidence level (0.0-1.0).
            tags: Optional tags for categorization.

        Returns:
            The blackboard entry ID for the published finding.
        """
        if blackboard is None:
            return ""

        from cognition.blackboard_schemas import FindingEntry
        from cognition.types import EntryType, Provenance, ProvenanceType

        finding = FindingEntry(
            summary=summary,
            detail=detail,
            sources=sources or [],
            confidence=confidence,
            tags=tags or [],
        )
        entry = blackboard.publish(
            slot_name="research_findings",
            content=finding.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="ORACLE",
            ),
            confidence=confidence,
            tags=["research", "oracle"] + (tags or []),
        )
        log.info(
            "ORACLE published finding to blackboard "
            "(slot=research_findings, confidence=%.2f)",
            confidence,
        )
        return entry.id

    def respond_to_question(
        self,
        blackboard: Any,
        question_entry: Any,
        answer: str,
        evidence: Optional[List[str]] = None,
        confidence: float = 0.5,
    ) -> str:
        """Answer a question by publishing an AnswerEntry to the blackboard.

        Questions arrive as ``QuestionEntry`` payloads in blackboard
        entries.  ORACLE reads them from the ``questions`` slot and
        responds by publishing an ``AnswerEntry`` to the ``answers``
        slot.  No direct messaging — all communication through the
        blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            question_entry: The ``QuestionEntry`` being answered.
            answer: The answer text.
            evidence: Optional list of evidence strings.
            confidence: Confidence level (0.0-1.0).

        Returns:
            The blackboard entry ID for the published answer.
        """
        if blackboard is None:
            return ""

        from cognition.blackboard_schemas import AnswerEntry
        from cognition.types import EntryType, Provenance, ProvenanceType

        answer_entry = AnswerEntry(
            question_id=getattr(question_entry, "question_id", "") or "",
            answered_by="ORACLE",
            answer=answer,
            evidence=evidence or [],
            confidence=confidence,
        )
        entry = blackboard.publish(
            slot_name="answers",
            content=answer_entry.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="ORACLE",
            ),
            confidence=confidence,
            tags=["answer", "oracle", question_entry.asked_by],
        )
        log.info(
            "ORACLE answered question %s (confidence=%.2f)",
            question_entry.question_id[:12] if question_entry.question_id else "?",
            confidence,
        )
        return entry.id

    def check_for_questions(
        self,
        blackboard: Any,
        max_questions: int = 5,
    ) -> List[Any]:
        """Check the blackboard for unanswered questions from other specialists.

        Reads ``QuestionEntry`` payloads from the ``questions`` blackboard
        slot.  These are questions published by other specialists (e.g.,
        FORGE asking about a library API).  ORACLE should process them
        via ``respond_to_question()``.

        No direct messaging.  Questions arrive as blackboard entries.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_questions: Maximum number of questions to return.

        Returns:
            List of ``QuestionEntry`` instances from unanswered questions.
        """
        if blackboard is None:
            return []

        from cognition.blackboard_schemas import QuestionEntry
        from cognition.types import EntryType

        entries = blackboard.read(
            slot_name="questions",
            entry_type=EntryType.QUERY,
        )
        questions = []
        for entry in entries[:max_questions]:
            try:
                q = QuestionEntry.from_entry_content(entry.content)
                questions.append(q)
            except Exception as e:
                log.debug("Failed to parse question entry: %s", e)
                continue

        return questions

    def read_findings(
        self,
        blackboard: Any,
        max_results: int = 10,
    ) -> List[Any]:
        """Read research findings from the blackboard.

        Useful when ORACLE needs to build on previous findings or
        check if a question has already been answered.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_results: Maximum findings to return.

        Returns:
            List of ``FindingEntry`` instances.
        """
        if blackboard is None:
            return []

        from cognition.blackboard_schemas import FindingEntry
        from cognition.types import EntryType

        entries = blackboard.read(
            slot_name="research_findings",
            entry_type=EntryType.FINDING,
        )
        findings = []
        for entry in entries[:max_results]:
            try:
                f = FindingEntry.from_entry_content(entry.content)
                findings.append(f)
            except Exception as e:
                log.debug("Failed to parse finding entry: %s", e)
                continue

        return findings

    def verify_output(self, output: str, context: Dict[str, Any]) -> Tuple[bool, str]:
        # Citations required for substantive answers; short answers are exempt.
        if len(output) < 500:
            return True, "Short output exempt from citation requirement."
        if not re.search(r"\[\d+\]|https?://", output):
            return False, "Output is substantial but contains no citations or URLs."
        return True, "Citations present."
