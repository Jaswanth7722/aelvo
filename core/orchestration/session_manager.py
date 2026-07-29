"""session_manager.py â€” Session summarization for AELVO OMEGA."""
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger("aelvo.session_manager")

SESSION_SUMMARY_INTERVAL = 50


class SessionManager:
    """Manages periodic session summarization and memory consolidation.

    Responsibilities:
    - Periodically persist compact session summaries to long-term memory
    - Consolidate execution traces across turns
    """

    def __init__(self, memory_engine=None):
        self.memory_engine = memory_engine
        self._turn_counter: int = 0

    @property
    def turn_counter(self) -> int:
        return self._turn_counter

    def increment_turn(self):
        self._turn_counter += 1

    def maybe_summarize_session(self, agent) -> Optional[str]:
        """Every SESSION_SUMMARY_INTERVAL turns, persist a session summary."""
        history = getattr(agent, "conversation_history", []) or []
        if len(history) == 0 or self._turn_counter == 0:
            return None
        if self._turn_counter % SESSION_SUMMARY_INTERVAL != 0:
            return None

        slice_size = SESSION_SUMMARY_INTERVAL * 2
        recent = history[-slice_size:]
        user_msgs = [m.get("content", "") for m in recent if m.get("role") == "user"]
        assistant_msgs = [m.get("content", "") for m in recent if m.get("role") == "assistant"]

        topics_blob = " ".join(user_msgs)[:1500]
        actions_blob = " ".join(assistant_msgs)[:1500]
        summary_text = (
            f"SESSION SUMMARY (turn {self._turn_counter}):\n"
            f"User topics: {topics_blob[:500]}\n"
            f"Assistant actions: {actions_blob[:500]}"
        )

        if self.memory_engine is None:
            return summary_text

        project = getattr(self.memory_engine, "project_name", "default")

        # Check for conflicts before writing
        from core.rag import MemorySearcher
        searcher = MemorySearcher(self.memory_engine.memory_collection)
        if not searcher.resolve_conflict(summary_text, meta_type="session_summary"):
            m_id = hashlib.sha256(f"sess_{project}_{time.time()}".encode()).hexdigest()
            meta = {
                "type": "session_summary",
                "turn": self._turn_counter,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp_unix": time.time(),
                "importance": 0.6,
                "usage_count": 1,
                "project": project,
                "source_specialist": "orchestrator",
            }
            chroma_success = False
            try:
                self.memory_engine.memory_collection.add(
                    ids=[m_id], documents=[summary_text], metadatas=[meta]
                )
                chroma_success = True
                if hasattr(self.memory_engine, "db"):
                    with self.memory_engine.db:
                        self.memory_engine.db.execute(
                            "INSERT INTO retained_memory (content) VALUES (?)",
                            (f"[ORCHESTRATOR:session_summary|{project}] turn={self._turn_counter}",),
                        )
            except Exception as e:
                log.warning("Failed to persist session summary: %s", e)
                if chroma_success:
                    try:
                        self.memory_engine.memory_collection.delete(ids=[m_id])
                    except Exception as _ex:
                        log.debug("Silenced exception on delete rollback: %s", _ex)

        return summary_text
