# user_model.py - HERMES Persistent User Preference Profiler for AELVO OMEGA

import time
import re
import hashlib
import threading
from typing import List, Dict, Any, Tuple
from config.settings import (
    IMPORTANCE_USER_PREFERENCE,
    CONFLICT_SIMILARITY_DUPLICATE,
    CONFLICT_SIMILARITY_OVERRIDE,
)
from memory import MEMORY_TYPE_USER_PREFERENCE
import logging

log = logging.getLogger(__name__)



# Heuristic vocabulary for signal extraction.
_FRUSTRATION_TERMS = (
    "useless", "stupid", "wrong again", "broken", "fail", "garbage",
    "waste", "nonsense", "ridiculous", "terrible", "awful", "doesn't work",
)
_BUILDING_TERMS = ("build", "write", "implement", "code", "scaffold", "create", "generate", "ship")
_DEBUGGING_TERMS = ("bug", "error", "fail", "broken", "traceback", "fix", "exception", "crash")
_REVIEWING_TERMS = ("review", "check", "verify", "lint", "inspect", "audit", "validate")
_COMMUNICATING_TERMS = ("email", "slack", "message", "draft", "tell him", "tell her", "tell them")
_CASUAL_PATTERNS = (r"\bhey\b", r"\bhi\b", r"\bbro\b", r"\blol\b", r"\byeah\b", r"\byep\b", r"ðŸ˜Š", r"ðŸ‘")
_DOMAIN_TERMS = (
    "python", "typescript", "javascript", "rust", "go", "devops", "git",
    "security", "architecture", "react", "node", "docker", "kubernetes",
    "sql", "ml", "llm", "ai", "vector",
)


class UserModelManager:
    """Persistent user-style profiler that writes only to its own project namespace."""

    def __init__(self, memory_collection, db_conn):
        self.collection = memory_collection
        self.db = db_conn
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # SIGNAL EXTRACTION
    # ------------------------------------------------------------------
    def extract_signals_from_history(self, conversation_history: List[Dict[str, str]]) -> Dict[str, Any]:
        user_msgs = [m.get("content", "") for m in conversation_history if m.get("role") == "user"]

        if not user_msgs:
            return self._default_signals()

        joined = " ".join(user_msgs).lower()

        # 1. Verbosity from average word count.
        avg_len = sum(len(m.split()) for m in user_msgs) / max(1, len(user_msgs))
        if avg_len < 10:
            verbosity = "brief"
        elif avg_len > 40:
            verbosity = "detailed"
        else:
            verbosity = "moderate"

        # 2. Formality from casual signals.
        casual_score = sum(1 for pat in _CASUAL_PATTERNS if re.search(pat, joined))
        formality = "casual" if casual_score >= 2 else "professional"

        # 3. Expertise per domain (frequency-weighted).
        expertise: Dict[str, str] = {}
        for term in _DOMAIN_TERMS:
            count = len(re.findall(rf"\b{re.escape(term)}\b", joined))
            if count >= 4:
                expertise[term] = "expert"
            elif count >= 2:
                expertise[term] = "high"
            elif count == 1:
                expertise[term] = "mid"

        # 4. Response format preference.
        code_first_c = len(re.findall(r"\b(code|implementation|just the code|directly|show me the)\b", joined))
        explain_c = len(re.findall(r"\b(explain|why|how does|concept|details|walk me through)\b", joined))
        if code_first_c > explain_c + 1:
            format_pref = "code_first"
        elif explain_c > code_first_c + 1:
            format_pref = "explanation_first"
        else:
            format_pref = "balanced"

        # 5. Recommendation mode.
        if any(w in joined for w in ("compare", "alternatives", "options", "trade-off", "tradeoff")):
            recommendation = "options"
        elif any(w in joined for w in ("best way", "recommend", "what should i", "just tell me")):
            recommendation = "direct"
        else:
            recommendation = "options"

        # 6. Workflow mode.
        workflow = "exploring"
        if any(w in joined for w in _DEBUGGING_TERMS):
            workflow = "debugging"
        elif any(w in joined for w in _BUILDING_TERMS):
            workflow = "building"
        elif any(w in joined for w in _REVIEWING_TERMS):
            workflow = "reviewing"
        elif any(w in joined for w in _COMMUNICATING_TERMS):
            workflow = "communicating"

        # 7. Frustration detection.
        bang_count = sum(m.count("!") for m in user_msgs[-3:])
        frustration = (
            any(term in joined for term in _FRUSTRATION_TERMS)
            or bang_count > 2
        )

        # 8. Vocabulary map: track user-specific phrases that resemble project nouns.
        vocab: Dict[str, str] = {}
        for match in re.finditer(r"\b([A-Z][a-z]+[A-Z]\w*)\b", " ".join(user_msgs)):
            vocab[match.group(1).lower()] = match.group(1)

        # 9. Pushback: detect explicit corrections.
        pushback: List[str] = []
        for msg in user_msgs:
            low = msg.lower()
            if any(p in low for p in ("not what i asked", "no, i meant", "you misunderstood", "that's wrong", "stop doing")):
                pushback.append(msg[:160])

        return {
            "verbosity_preference": verbosity,
            "formality_level": formality,
            "expertise_by_domain": expertise,
            "response_format_preference": format_pref,
            "recommendation_preference": recommendation,
            "current_workflow_mode": workflow,
            "frustration_signals": frustration,
            "vocabulary_map": vocab,
            "pushback_history": pushback,
        }

    @staticmethod
    def _default_signals() -> Dict[str, Any]:
        return {
            "verbosity_preference": "moderate",
            "formality_level": "professional",
            "expertise_by_domain": {},
            "response_format_preference": "balanced",
            "recommendation_preference": "options",
            "current_workflow_mode": "exploring",
            "frustration_signals": False,
            "vocabulary_map": {},
            "pushback_history": [],
        }

    # ------------------------------------------------------------------
    # MODEL UPDATES (PROJECT-SCOPED)
    # ------------------------------------------------------------------
    def update_model(self, project: str, signals: Dict[str, Any]) -> None:
        """Persist signals as user_preference entries, scoped to this project."""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

        traits: List[Tuple[str, str]] = [
            ("communication_style",
             f"User prefers a {signals['verbosity_preference']} and {signals['formality_level']} communication style."),
            ("workflow_mode",
             f"User is currently in a {signals['current_workflow_mode']} workflow mode."),
            ("response_format",
             f"User responds best to a {signals['response_format_preference']} response format."),
            ("recommendation",
             f"User prefers AELVO to present {signals['recommendation_preference']} when navigating trade-offs."),
        ]

        if signals["frustration_signals"]:
            traits.append((
                "frustration",
                "User is showing frustration signals; cut verbosity by 40% and lead with the direct answer.",
            ))

        for domain, lvl in signals.get("expertise_by_domain", {}).items():
            traits.append((
                "expertise_domain",
                f"User has {lvl} expertise in the '{domain}' domain.",
            ))

        for phrase in signals.get("pushback_history", []):
            traits.append((
                "avoid",
                f"User has previously pushed back: '{phrase}'. Avoid repeating that pattern.",
            ))

        with self._lock:
            for category, text in traits:
                self._upsert_trait(project, category, text, timestamp)

    def _upsert_trait(self, project: str, category: str, text: str, timestamp: str) -> None:
        """Insert a single trait while honoring deduplicate/override thresholds within the project namespace."""
        with self._lock:
            try:
                results = self.collection.query(
                    query_texts=[text],
                    n_results=1,
                    where={"type": MEMORY_TYPE_USER_PREFERENCE, "project": project},
                    include=["documents", "metadatas", "distances"],
                )
            except Exception:
                results = {"ids": [[]], "metadatas": [[]], "distances": [[]]}

            duplicate = False
            existing_id: str = ""

            if results.get("ids") and results["ids"] and results["ids"][0]:
                try:
                    dist = results["distances"][0][0]
                    similarity = 1.0 - float(dist)
                    existing_id = results["ids"][0][0]

                    if similarity >= CONFLICT_SIMILARITY_DUPLICATE:
                        duplicate = True
                        meta = dict(results["metadatas"][0][0]) if results["metadatas"] and results["metadatas"][0] else {}
                        meta["usage_count"] = int(meta.get("usage_count", 0)) + 1
                        meta["importance"] = min(1.0, float(meta.get("importance", IMPORTANCE_USER_PREFERENCE)) + 0.05)
                        self.collection.update(ids=[existing_id], metadatas=[meta])
                    elif similarity >= CONFLICT_SIMILARITY_OVERRIDE:
                        self.collection.delete(ids=[existing_id])
                except Exception as _ex: log.warning("Silenced exception: %s", _ex)

            if duplicate:
                return

            m_id = hashlib.sha256(
                f"user_pref_{time.time()}_{project}_{category}_{text}".encode("utf-8")
            ).hexdigest()

            # ChromaDB write first
            try:
                self.collection.add(
                    ids=[m_id],
                    documents=[text],
                    metadatas=[{
                        "type": MEMORY_TYPE_USER_PREFERENCE,
                        "preference_category": category,
                        "timestamp": timestamp,
                        "timestamp_unix": time.time(),
                        "importance": IMPORTANCE_USER_PREFERENCE,
                        "usage_count": 1,
                        "project": project,
                        "source_specialist": "hermes",
                    }],
                )
            except Exception:
                try:
                    self.collection.delete(ids=[m_id])
                except Exception as _ex: log.warning("Silenced exception: %s", _ex)
                return

            # SQLite dual-sync
            try:
                with self.db:
                    self.db.execute(
                        "INSERT INTO retained_memory (content) VALUES (?)",
                        (f"[USER PREFERENCE - {category.upper()} | {project}] {text}",),
                    )
            except Exception:
                # Rollback ChromaDB if SQLite write fails to restore sync
                try:
                    self.collection.delete(ids=[m_id])
                except Exception as _ex: log.warning("Silenced exception: %s", _ex)

    # ------------------------------------------------------------------
    # PROMPT INJECTION
    # ------------------------------------------------------------------
    def build_prompt_injection(self, project: str) -> str:
        """Return formatted user-model guidance scoped to project."""
        with self._lock:
            try:
                results = self.collection.get(
                    where={"type": MEMORY_TYPE_USER_PREFERENCE, "project": project},
                    include=["documents"],
                    limit=20,
                )
                docs = results.get("documents", [])
            except Exception:
                docs = []

            if not docs:
                return (
                    "[USER MODEL PROFILE]\n"
                    "- Verbosity: moderate\n"
                    "- Formality: professional\n"
                    "- Format: balanced\n"
                    "- Recommendation Mode: present options with explicit tradeoffs\n"
                    "- Proactively verify logic and surface improvements without being asked.\n"
                )

            bullets = "\n".join(f"- {doc}" for doc in docs)
            return (
                "[USER MODEL PROFILE]\n"
                "Calibrate every response to the following persistent user attributes. "
                "These are durable preferences mined across past turns; respect them unless contradicted by the latest message.\n"
                f"{bullets}\n"
            )
