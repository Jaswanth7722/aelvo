# hermes.py - HERMES Personalized User Calibration Specialist for AELVO OMEGA

import time
import hashlib
import logging
import re
from typing import Dict, List, Tuple, Any

from config.settings import BASE_DIR
from specialists.base import BaseSpecialist
from memory import MEMORY_TYPE_USER_PREFERENCE

# UI Integration (direct)
try:
    from ui.events import get_event_bus, create_specialist_event, EventType
    UI_AVAILABLE = True
except ImportError:
    UI_AVAILABLE = False

log = logging.getLogger("aelvo.hermes")


class HermesSpecialist(BaseSpecialist):
    """HERMES analyzes communication signals and calibrates responses to the user's actual style."""

    name: str = "HERMES"
    trigger_patterns: List[str] = [
        "preference", "style", "calibrate", "tone", "how i", "know me",
        "customise", "customize", "frustrated", "explain to me", "briefly",
        "in detail", "concise", "shorter", "longer",
    ]
    memory_types: List[str] = [MEMORY_TYPE_USER_PREFERENCE]
    required_tools: List[str] = []
    activation_threshold: float = 0.5

    def __init__(self):
        self.workspace = str(BASE_DIR)
        
        # UI Integration (direct)
        self.event_bus = None
        if UI_AVAILABLE:
            self.event_bus = get_event_bus()
    
    def _notify_ui_action(self, action: str):
        """Direct UI notification when HERMES performs an action."""
        if self.event_bus:
            import asyncio
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.event_bus.publish(
                    create_specialist_event(EventType.SPECIALIST_ACTION, "HERMES", action)
                ))
            except RuntimeError:
                pass
    


    def compute_activation_score(self, task: str, context: Dict[str, Any]) -> float:
        score = super().compute_activation_score(task, context)
        clean = (task or "").lower()
        if any(w in clean for w in ("concise", "shorten", "wordy", "verbose", "calibrate", "formal", "casual", "tone")):
            score += 0.35
        return min(1.0, max(0.0, score))

    def build_user_model(self, conversation_history: List[Dict[str, str]]) -> Dict[str, Any]:
        # UI Integration: Notify when HERMES is analyzing user model
        self._notify_ui_action("Analyzing user communication patterns")
        
        user_msgs = [m.get("content", "") for m in conversation_history if m.get("role") == "user"]
        joined_content = " ".join(user_msgs).lower()
        
        if not user_msgs:
            return {
                "communication_style": "moderate",
                "question_frequency": "directing",
                "expertise": "medium",
                "workflow_mode": "exploring",
                "emoji_casual": "neutral",
                "code_preference": "balanced",
                "frustration": False
            }
            
        # 1. Message length distribution
        avg_words = sum(len(m.split()) for m in user_msgs) / len(user_msgs)
        if avg_words < 20:
            comm_style = "brief_direct"
        elif avg_words > 60:
            comm_style = "verbose"
        else:
            comm_style = "moderate"
            
        # 2. Question frequency
        q_words = {"why", "how", "what", "who", "where", "when", "?"}
        q_count = sum(1 for m in user_msgs if any(qw in m.lower() for qw in q_words))
        q_fraction = q_count / len(user_msgs)
        q_freq = "exploring" if q_fraction > 0.3 else "directing"
        
        # 3. Technical vocabulary density
        tech_words = {"python", "typescript", "javascript", "rust", "go", "api", "database", "git", "auth", "class", "function", "linter", "docker", "yaml", "sql", "migration"}
        tech_count = sum(1 for m in user_msgs for word in m.lower().split() if word in tech_words)
        tech_density = tech_count / len(user_msgs)
        expertise = "high" if tech_density > 1.5 else "low"
        
        # 4. Correction signal
        corrections = {"no", "that's not what i asked", "wrong", "again", "still", "incorrect"}
        correction_detected = any(any(c in m.lower() for c in corrections) for m in user_msgs[-3:])
        
        # 5. Emoji and casual
        casual_indicators = {"lol", "yeah", "bro", "yep", "ðŸ˜Š", "ðŸ‘", "thanks", "hey", "hi"}
        casual_count = sum(1 for m in user_msgs if any(c in m.lower() for c in casual_indicators))
        casual_style = "casual" if casual_count > 0 else "neutral"
        
        # 6. Code preference
        code_indicators = {"```", "code", "syntax", "traceback", "def ", "class ", "function ", "import "}
        code_count = sum(1 for m in user_msgs if any(c in m.lower() for c in code_indicators))
        code_pref = "code_first" if code_count > len(user_msgs) * 0.4 else "explanation_first"
        
        # 7. Trajectory and frustration
        frustration_terms = {"useless", "stupid", "wrong again", "broken", "fail", "garbage", "waste", "nonsense", "ridiculous", "terrible", "awful", "doesn't work", "frustrat"}
        bang_count = sum(m.count("!") for m in user_msgs)
        frustration = (
            any(term in joined_content for term in frustration_terms)
            or bang_count > 2
        )
                
        # Workflow mode
        workflow_mode = "exploring"
        if any(w in joined_content for w in ("bug", "error", "broken", "traceback", "fix", "crash", "failed", "fail", "failure")):
            workflow_mode = "debugging"
        elif any(w in joined_content for w in ("build", "implement", "create", "write")):
            workflow_mode = "build"
            
        model = {
            "communication_style": comm_style,
            "question_frequency": q_freq,
            "expertise": expertise,
            "workflow_mode": workflow_mode,
            "emoji_casual": casual_style,
            "code_preference": code_pref,
            "frustration": frustration
        }
        return model

    def build_memory_context(self, task: str, memory_engine) -> Dict[str, Any]:
        project = getattr(memory_engine, "project_name", "default")
        
        # Query all user preference records sorted by recency - no noise filter
        preferences = []
        try:
            results = memory_engine.memory_collection.get(
                where={"type": "user_preference", "project": project},
                include=["documents", "metadatas"]
            )
            # Sort by timestamp_unix descending
            zipped = list(zip(results.get("documents") or [], results.get("metadatas") or []))
            zipped.sort(key=lambda x: float(x[1].get("timestamp_unix", 0.0)), reverse=True)
            preferences = [x[0] for x in zipped]
        except Exception as _ex: log.debug("Silenced exception: %s", _ex)
            
        return {
            "user_preferences": preferences,
            "user_model": self.build_user_model([]) # will be calculated dynamically on history
        }

    def get_system_prompt(self, context: Dict[str, Any]) -> str:
        project = context.get("project", "default")
        
        # Query user preferences sorted by recency
        preferences = context.get("user_preferences") or []
        
        # Section 1 - Identity
        if preferences:
            identity = f"You are HERMES, AELVO's personalized reasoning specialist. You have access to this user's profile based on {len(preferences)} historical calibration signals."
        else:
            identity = "You are HERMES, AELVO's personalized reasoning specialist. This is the first session, so you will build a user model as the conversation progresses."

        # Section 2 - User model summary
        user_model = context.get("user_model") or {}
        summary_lines = [
            f"  - Communication Style: {user_model.get('communication_style', 'moderate')}",
            f"  - Question Frequency: {user_model.get('question_frequency', 'directing')}",
            f"  - Technical Expertise: {user_model.get('expertise', 'medium')}",
            f"  - Current Workflow: {user_model.get('workflow_mode', 'exploring')}",
            f"  - Casual/Formal Style: {user_model.get('emoji_casual', 'neutral')}",
            f"  - Code Preference: {user_model.get('code_preference', 'balanced')}",
            f"  - Frustration Detected: {'Yes' if user_model.get('frustration') else 'No'}"
        ]
        summary_section = "KNOWN USER PROFILE:\n" + "\n".join(summary_lines)

        # Section 3 - Calibration instructions
        cal_lines = []
        if user_model.get("communication_style") == "brief_direct":
            cal_lines.append("Lead with the direct answer. Skip the analogy. No explanatory preamble.")
        elif user_model.get("communication_style") == "verbose":
            cal_lines.append("Provide a thorough structural explanation with code blocks and rationales.")
            
        if user_model.get("expertise") == "high":
            cal_lines.append("Lead with technical recommendations, avoid tutorial-like explanations, and bypass introductory overviews.")
        else:
            cal_lines.append("User is learning â€” explain the decision, not just the result. Comment the code inline.")
            
        if user_model.get("frustration"):
            cal_lines.append("Cut responses by 40%. Deliver precise diagnostic answers first and omit pleasantries.")

        calibration_instructions = "CALIBRATION RULES:\n" + "\n".join(f"  - {c}" for c in cal_lines) if cal_lines else "CALIBRATION RULES:\n  - Maintain standard balanced professional tone."

        # Section 4 - Proactive context injection
        proactive_injection = ""
        memory_engine = context.get("memory_engine")
        if memory_engine:
            try:
                # Check for architect system decision mappings
                res = memory_engine.memory_collection.get(
                    where={"type": "system_decision", "project": project},
                    limit=1
                )
                if res and res.get("documents"):
                    doc = res["documents"][0]
                    meta = res["metadatas"][0]
                    proactive_injection = f"NOTE: Surface the system decision '{doc[:120]}' from {meta.get('timestamp', 'past session')} if it is relevant to this task."
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)
                
        # Section 5 - Output format
        output_format = (
            "OUTPUT:\n"
            "JSON array of tool calls. Calibrate the respond tool's message field according to the user model above."
        )

        # Section 6 - Workflow
        workflow = (
            "WORKFLOW:\n"
            "1. Query memory for user preferences and relevant past context\n"
            "2. Analyze the current message for new signals to add to the user model\n"
            "3. Build the response calibrated to the detected user profile\n"
            "4. In the respond tool call, lead with what the user actually needs, in the format they actually want\n"
            "5. In post_process, save any new preference signals discovered in this turn"
        )

        parts = [identity, summary_section, calibration_instructions]
        if proactive_injection:
            parts.append(proactive_injection)
        parts.extend([output_format, workflow])

        return "\n\n".join(parts)

    def post_process(self, result: str, memory_engine, conversation_history: List[Dict[str, str]]) -> str:
        project = getattr(memory_engine, "project_name", "default")
        signals = self.build_user_model(conversation_history)

        # Use UserModelManager for rich, categorized signal extraction and persistence
        try:
            from memory.user_model import UserModelManager
            umm = UserModelManager(memory_engine.memory_collection, memory_engine.db)
            # Extract richer signals from full conversation history
            rich_signals = umm.extract_signals_from_history(conversation_history)
            umm.update_model(project, rich_signals)
        except Exception as e:
            log.error("UserModelManager update failed: %s", e)

        num_saved = 0
        num_unchanged = 0

        # Also persist the fast heuristic signals for immediate availability
        traits = [
            ("communication_style", signals["communication_style"]),
            ("workflow_mode", signals["workflow_mode"]),
            ("expertise", signals["expertise"]),
            ("code_preference", signals["code_preference"]),
            ("frustration", "frustrated" if signals["frustration"] else "normal"),
        ]

        for category, value in traits:
            saved = self._write_preference(memory_engine, category, value, 0.85)
            if saved:
                num_saved += 1
            else:
                num_unchanged += 1

        return f"HERMES updated user model: {num_saved} signals saved, {num_unchanged} unchanged (rich model via UserModelManager also updated)."


    def _write_preference(self, memory_engine, signal_type: str, signal_value: Any, confidence: float) -> bool:
        content = f"user_preference: {signal_type} = {signal_value} | confidence={confidence}"
        project = getattr(memory_engine, "project_name", "default")
        
        # 1. Resolve conflict
        from core.rag import MemorySearcher
        searcher = MemorySearcher(memory_engine.memory_collection)
        if searcher.resolve_conflict(content, meta_type="user_preference"):
            return False
            
        entry_id = hashlib.sha256(f"user_pref_{signal_type}_{time.time()}".encode()).hexdigest()
        
        # 2. Build metadata
        metadata = {
            "type": "user_preference",
            "importance": 0.60,
            "timestamp_unix": time.time(),
            "usage_count": 0,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "project": project,
            "source_specialist": "HERMES",
            "signal_type": signal_type,
            "confidence": str(confidence)
        }
        
        # 3. ChromaDB Add
        try:
            memory_engine.memory_collection.add(
                ids=[entry_id],
                documents=[content],
                metadatas=[metadata]
            )
        except Exception as exc:
            log.error("ChromaDB add failed in HERMES: %s", exc)
            return False
            
        # 4. SQLite dual-sync
        try:
            with memory_engine.db:
                memory_engine.db.execute(
                    "INSERT INTO retained_memory (content) VALUES (?)",
                    (f"[HERMES:user_preference] {content}",)
                )
        except Exception as exc:
            log.error("SQLite sync failed in HERMES, rolling back Chroma: %s", exc)
            try:
                memory_engine.memory_collection.delete(ids=[entry_id])
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)
            return False
            
        return True

    # ------------------------------------------------------------------
    # HermesContext — Global Cognition Output
    # ------------------------------------------------------------------

    async def create_hermes_context(
        self,
        task: str,
        conversation_history: List[Dict[str, str]],
        memory_engine=None,
        session_id: str = "",
    ) -> "HermesContext":
        """Produce the immutable HermesContext for this turn.

        This is the PRIMARY output of HERMES. The context is created
        ONCE at the start of every request and consumed immutably
        by every component in the system.

        HermesContext contains:
        - Intent, goals, constraints inferred from the task
        - Risk profile and complexity assessment
        - User model (communication style, expertise, workflow)
        - Relevant memory context
        - Execution permissions
        - Enriched analysis from HERMES

        Per Amendment 4: Hermes is NOT a preprocessing step.
        Hermes remains active throughout every workflow.
        """
        # Lazy import to break circular: specialists → cognition → specialists
        from cognition.hermes_context import HermesContext

        log.info("Creating HermesContext for task: %s", task[:80])
        self._notify_ui_action("Creating global cognition context")

        # 1. Build the user model from conversation history
        user_model = self.build_user_model(conversation_history)

        # 2. Build memory context
        memory_ctx = {}
        if memory_engine:
            try:
                memory_ctx = self.build_memory_context(task, memory_engine)
            except Exception as e:
                log.debug("Memory context build failed: %s", e)

        # 3. Infer risk profile from task content
        risk_profile = self._infer_risk_profile(task)

        # 4. Estimate complexity
        complexity = self._estimate_complexity(task)

        # 5. Build enriched analysis
        hermes_analysis = self._build_hermes_analysis(
            task, user_model, memory_ctx,
        )

        # 6. Determine execution permissions
        permissions = self._determine_permissions(task, risk_profile)

        # 7. Extract constraints from task
        constraints = self._extract_constraints(task)

        # 8. Create the immutable context
        context = HermesContext.create(
            task=task,
            intent=self._infer_intent_from_model(task, user_model),
            goals=self._decompose_goals_for_context(task),
            constraints=constraints,
            risk_profile=risk_profile,
            complexity=complexity,
            memory_context=memory_ctx,
            user_model=user_model,
            execution_permissions=permissions,
            session_id=session_id,
            hermes_analysis=hermes_analysis,
        )

        log.info(
            "HermesContext created: intent=%s, risk=%s, complexity=%d/10, goals=%d",
            context.intent[:40], context.risk_profile,
            context.complexity, len(context.goals),
        )
        return context

    # ------------------------------------------------------------------
    # HermesContext Analysis Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _infer_risk_profile(task: str) -> str:
        """Infer risk profile from task content."""
        lower = task.lower()

        high_risk = (
            "delete", "drop", "truncate", "rm -rf", "format",
            "production", "prod", "deploy", "release", "publish",
            "database", "migration", "rollback", "credentials",
            "secret", "password", "api_key", "token",
            "chmod 777", "sudo", "root",
        )
        medium_risk = (
            "security", "auth", "permission", "firewall",
            "network", "config", "configuration",
            "docker", "kubernetes", "k8s",
            "commit", "push", "merge",
            "refactor", "rewrite",
        )

        for keyword in high_risk:
            if keyword in lower:
                return "high"

        for keyword in medium_risk:
            if keyword in lower:
                return "medium"

        return "low"

    @staticmethod
    def _estimate_complexity(task: str) -> int:
        """Estimate task complexity on a 1-10 scale."""
        lower = task.lower()
        score = 1

        # Length factor
        words = len(task.split())
        if words > 50:
            score += 1
        if words > 100:
            score += 1
        if words > 200:
            score += 1

        # Multiple requirements
        separators = sum(1 for sep in (" and ", ", ", ". ", "; ") if sep in lower)
        if separators > 3:
            score += 1
        if separators > 6:
            score += 1

        # Technical complexity
        tech_terms = (
            "database", "migration", "refactor", "architecture",
            "design pattern", "dependency", "integration",
            "multi-thread", "async", "concurrent", "distributed",
            "docker", "kubernetes", "ci/cd", "pipeline",
            "api", "graphql", "rest", "websocket", "grpc",
        )
        tech_count = sum(1 for t in tech_terms if t in lower)
        if tech_count >= 2:
            score += 1
        if tech_count >= 4:
            score += 1

        # Multi-file or system-wide scope
        scope_terms = ("all files", "everywhere", "entire", "global", "system", "full")
        if any(t in lower for t in scope_terms):
            score += 1

        return min(10, max(1, score))

    @staticmethod
    def _infer_intent_from_model(
        task: str, user_model: Dict[str, Any],
    ) -> str:
        """Infer intent, enriched by user model context."""
        # Lazy import to break circular: specialists -> cognition -> specialists
        from cognition.hermes_context import HermesContext
        base_intent = HermesContext._infer_intent(task)

        # Enrich with workflow mode
        workflow = user_model.get("workflow_mode", "")
        if workflow == "debugging" and base_intent != "debug_and_fix":
            return "debug_and_fix"
        if workflow == "build" and base_intent == "general_assistance":
            return "implement_feature"

        return base_intent

    @staticmethod
    def _decompose_goals_for_context(task: str) -> List[str]:
        """Decompose task into goals for HermesContext."""
        from cognition.hermes_context import HermesContext
        return HermesContext._decompose_goals(task)

    def _build_hermes_analysis(
        self,
        task: str,
        user_model: Dict[str, Any],
        memory_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build HERMES's enriched analysis of the task."""
        lower = task.lower()
        analysis: Dict[str, Any] = {
            "concerns": [],
            "approach_suggestions": [],
            "key_terms": [],
        }

        # Identify concerns
        if user_model.get("frustration"):
            analysis["concerns"].append(
                "User frustration detected — prioritize quick, accurate response"
            )
        if self._infer_risk_profile(task) == "high":
            analysis["concerns"].append(
                "High-risk task — require security review before execution"
            )
        if len(task.split()) < 5:
            analysis["concerns"].append(
                "Very brief request — may need clarification"
            )

        # Approach suggestions based on task type
        intent = self._infer_intent_from_model(task, user_model)
        approach_map = {
            "debug_and_fix": [
                "First reproduce and diagnose the issue",
                "Check error logs and stack traces",
                "Apply minimal fix — avoid scope creep",
            ],
            "implement_feature": [
                "Review existing patterns and conventions",
                "Check for similar implementations in codebase",
                "Implement incrementally with testing",
            ],
            "refactor_code": [
                "Understand current behavior first",
                "Ensure test coverage before refactoring",
                "Refactor in small, verifiable steps",
            ],
            "research_and_explain": [
                "Search codebase for relevant context",
                "Provide concrete examples and evidence",
            ],
            "security_audit": [
                "Review for common vulnerability patterns",
                "Check secret exposure risks",
                "Prioritize findings by severity",
            ],
        }
        analysis["approach_suggestions"] = approach_map.get(intent, [])

        # Extract key technical terms
        tech_terms = {
            "python", "typescript", "javascript", "rust", "go",
            "react", "vue", "angular", "django", "flask", "fastapi",
            "sql", "nosql", "redis", "postgres", "mysql", "sqlite",
            "docker", "kubernetes", "aws", "azure", "gcp",
            "api", "graphql", "rest", "grpc", "websocket",
        }
        analysis["key_terms"] = [
            t for t in tech_terms if t in lower
        ]

        return analysis

    @staticmethod
    def _determine_permissions(
        task: str, risk_profile: str,
    ) -> List[str]:
        """Determine allowed execution operations."""
        permissions = ["read", "search"]
        lower = task.lower()

        # Write permissions for code changes
        if any(w in lower for w in (
            "write", "create", "edit", "implement", "fix",
            "refactor", "update", "modify", "change", "add",
        )):
            permissions.append("write")

        # Execute permissions for operations
        if any(w in lower for w in (
            "run", "execute", "deploy", "test", "bash",
            "terminal", "command", "install", "build", "compile",
        )):
            permissions.append("execute")

        # Restrict execute in high-risk scenarios
        if risk_profile == "high":
            if "execute" in permissions:
                permissions.remove("execute")
            permissions.append("requires_security_review")

        return permissions

    @staticmethod
    def _extract_constraints(task: str) -> Dict[str, Any]:
        """Extract explicit constraints from the task."""
        constraints = {}
        lower = task.lower()

        # Time constraints
        import re
        time_patterns = [
            r"by (\w+day|\w+ month|tomorrow|next week)",
            r"in (\d+) (minute|hour|day)",
            r"urgent|asap|immediately",
        ]
        for pattern in time_patterns:
            match = re.search(pattern, lower)
            if match:
                constraints["timeframe"] = match.group(0)
                break

        # Budget constraints
        if "budget" in lower or "cost" in lower or "cheap" in lower:
            constraints["cost_sensitive"] = True

        # Compatibility constraints
        if any(w in lower for w in ("python", "node", "typescript", "rust", "go")):
            constraints["language"] = next(
                (w for w in ("python", "node", "typescript", "rust", "go") if w in lower),
                "",
            )

        # Platform constraints
        if any(w in lower for w in ("windows", "linux", "macos", "docker", "browser")):
            constraints["platform"] = next(
                (w for w in ("windows", "linux", "macos", "docker", "browser") if w in lower),
                "",
            )

        return constraints

    def verify_output(self, output: str, context: Dict[str, Any]) -> Tuple[bool, str]:
        if not output.strip():
            return False, "Output is empty."
        if "TODO" in output or "FIXME:" in output:
            return False, "Output contains developer placeholders (TODO/FIXME)."
        return True, "Calibrated reasoning checks out."

    def calibrate_response(self, text: str, user_model: Dict[str, Any]) -> str:
        if not text:
            return text

        expertise = user_model.get("expertise", "medium")
        workflow = user_model.get("workflow_mode", "exploring")
        comm_style = user_model.get("communication_style", "moderate")
        frustration = user_model.get("frustration", False)
        code_pref = user_model.get("code_preference", "balanced")
        
        calibrated = text
        
        # 1. Clean hedges and fillers
        calibrated = self._strip_preamble(calibrated)
        calibrated = self._strip_hedging(calibrated)
        
        # 2. Expertise: High
        if expertise == "high":
            calibrated = re.sub(r"\b(basically|essentially|in other words|simply put|as you know)\b,?\s*", "", calibrated, flags=re.IGNORECASE)
            
        # 3. Workflow Mode: build
        if workflow == "build" or code_pref == "code_first":
            calibrated = self._hoist_first_code_block(calibrated)
            
        # 4. Workflow Mode: debugging
        if workflow == "debugging":
            if "diagnosis:" not in calibrated.lower() and "cause:" not in calibrated.lower():
                calibrated = f"DIAGNOSIS: Root cause analysis of reported issue.\n\n{calibrated}"

        # 5. Brief or Frustrated length cap
        word_count = len(calibrated.split())
        if frustration or comm_style == "brief_direct":
            if word_count > 150:
                calibrated = self._trim_to_word_budget(calibrated, 120) + "\n\n(Summarized for brief REPL stream)"
        elif comm_style == "verbose":
            pass
        elif word_count > 400:
            calibrated = self._trim_to_word_budget(calibrated, 300) + "\n\n(Summarized for terminal display)"
            
        # 6. Hard cap on avoid:long_explanations
        if user_model.get("avoid:long_explanations") or "avoid:long_explanations" in str(user_model):
            paragraphs = calibrated.split("\n\n")
            shortened = []
            for p in paragraphs:
                if "```" in p:
                    shortened.append(p)
                else:
                    sentences = re.split(r"(?<=[.!?])\s+", p)
                    shortened.append(" ".join(sentences[:3]))
            calibrated = "\n\n".join(shortened)
            
        return calibrated.strip()

    @staticmethod
    def _strip_preamble(text: str) -> str:
        lines = text.splitlines()
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            chatter = ("sure!", "absolutely", "of course", "great question", "happy to", "i'd be happy",
                       "let me", "i can help", "no problem")
            if any(line.lower().startswith(c) for c in chatter):
                i += 1
                continue
            break
        return "\n".join(lines[i:]) if i < len(lines) else text

    @staticmethod
    def _strip_hedging(text: str) -> str:
        hedges = (
            r"\bi think (that\s+)?",
            r"\bi believe (that\s+)?",
            r"\bit seems like ",
            r"\bperhaps ",
            r"\bjust to clarify, ",
            r"\bif i understand correctly, ",
        )
        out = text
        for pattern in hedges:
            out = re.sub(pattern, "", out, flags=re.IGNORECASE)
        return out

    @staticmethod
    def _trim_to_word_budget(text: str, word_budget: int) -> str:
        parts = re.split(r"(```[\s\S]+?```)", text)
        out = []
        used = 0
        for part in parts:
            if part.startswith("```"):
                out.append(part)
                continue
            words = part.split()
            if used + len(words) <= word_budget:
                out.append(part)
                used += len(words)
            else:
                remaining = max(0, word_budget - used)
                if remaining > 0:
                    out.append(" ".join(words[:remaining]))
                    used = word_budget
                break
        return "".join(out)

    @staticmethod
    def _hoist_first_code_block(text: str) -> str:
        match = re.search(r"```[\s\S]+?```", text)
        if not match:
            return text
        block = match.group(0)
        if text.strip().startswith("```"):
            return text
        before = text[:match.start()].strip()
        after = text[match.end():].strip()
        rest = "\n\n".join(p for p in (before, after) if p)
        return f"{block}\n\n{rest}".strip()
