# forge.py - FORGE Coding Intelligence Specialist for AELVO OMEGA

import ast
import hashlib
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config.settings import (
    BASE_DIR,
    FORGE_ACTION_BUDGET,
    FORGE_ACTIVATION_THRESHOLD,
    FORGE_MAX_PATTERN_DESCRIPTION_CHARS,
    FORGE_MAX_TREE_CHARS_IN_PROMPT,
    FORGE_MIN_PATTERN_LINES,
    FORGE_PROJECT_TREE_CACHE_TTL,
    FORGE_NOISE_FLOOR,
)
from memory import MEMORY_TYPE_CODE_PATTERN, MEMORY_TYPE_ERROR_RECOVERY
from memory.forge_memory import ForgeMemory, MEMORY_TYPE_CONVENTION
from specialists.base import BaseSpecialist


log = logging.getLogger("aelvo.forge")

WRITE_TOOLS = frozenset({
    "write_file", "write_atomic", "edit_file", "edit_file_block",
})
VERIFY_TOOLS = frozenset({
    "run_linter", "run_tests", "run_type_checker",
})

_EXT_LANGUAGE = {
    ".py": "python", ".pyx": "python", ".pyi": "python",
    ".ts": "typescript", ".tsx": "typescript",
    ".js": "javascript", ".jsx": "javascript", ".mjs": "javascript", ".cjs": "javascript",
    ".rs": "rust", ".go": "go", ".java": "java", ".rb": "ruby",
    ".php": "php", ".cs": "csharp", ".cpp": "cpp", ".cc": "cpp", ".c": "c",
    ".swift": "swift", ".kt": "kotlin", ".kts": "kotlin",
}


class ForgeSpecialist(BaseSpecialist):
    """FORGE governs code generation, refactoring, and verification with institutional memory."""

    name: str = "FORGE"
    trigger_patterns: List[str] = [
        "code", "write", "refactor", "bug", "implement", "fix", "linter",
        "mypy", "pytest", "test", "symbol", "graph", "syntax", "python",
        "typescript", "javascript", "rust", "go", "class", "function",
        "build", "ts", "py", "worker", "runtime", "bridge", "ipc",
        "debug", "exception", "traceback", "format", "typecheck", "eslint",
    ]
    memory_types: List[str] = [MEMORY_TYPE_CODE_PATTERN, MEMORY_TYPE_ERROR_RECOVERY, MEMORY_TYPE_CONVENTION]
    required_tools: List[str] = [
        "read_file", "write_file", "edit_file", "list_files",
        "run_linter", "run_type_checker", "run_tests",
    ]
    activation_threshold: float = FORGE_ACTIVATION_THRESHOLD

    def __init__(self):
        self.forge_memory: Optional[ForgeMemory] = None
        self._tree_cache: Optional[str] = None
        self._tree_cache_time: float = 0.0
        self._symbol_cache: Optional[Dict[str, Any]] = None
        self._symbol_cache_time: float = 0.0
        self.workspace = str(BASE_DIR)
        self.fs = None
        # Subscription-accumulated data (populated via blackboard.subscribe callbacks)
        self._findings: List[Any] = []
        self._revisions: List[Dict[str, Any]] = []
        log.info("FORGE initialized (direct CLI tool mode â€” ruff/mypy/pytest/tsc/eslint/cargo/go)")


    def _ensure_memory(self, memory_engine) -> ForgeMemory:
        if self.forge_memory is None:
            project = getattr(memory_engine, "project_name", "default_project")
            self.forge_memory = ForgeMemory(memory_engine, project)
        return self.forge_memory

    def compute_activation_score(self, task: str, context: Dict[str, Any]) -> float:
        clean = (task or "").lower()

        # 1. Keyword signal (0.0 - 0.5)
        matches = sum(1 for p in self.trigger_patterns if p.lower() in clean)
        keyword_signal = min(0.5, matches * 0.1) if matches > 0 else 0.0

        # 2. Filesystem signal (0.0 - 0.3)
        workspace = context.get("workspace_path") or self.workspace
        source_exts = {".py", ".ts", ".tsx", ".js", ".jsx", ".rs", ".go"}
        has_source = False
        try:
            for root, dirs, files in os.walk(workspace):
                dirs[:] = [d for d in dirs if d not in (".git", "node_modules", "__pycache__", "chroma_db", "dist", ".venv")]
                for f in files:
                    if Path(f).suffix.lower() in source_exts:
                        has_source = True
                        break
                if has_source:
                    break
        except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        filesystem_signal = 0.3 if has_source else 0.0

        # 3. Memory signal (0.0 - 0.2)
        memory_engine = context.get("memory_engine")
        has_memory = False
        if memory_engine:
            try:
                fm = self._ensure_memory(memory_engine)
                hits = fm.query_patterns(task, 1)
                if hits and any(h.get("score", 0.0) >= FORGE_NOISE_FLOOR for h in hits):
                    has_memory = True
            except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        memory_signal = 0.2 if has_memory else 0.0

        return min(1.0, max(0.0, keyword_signal + filesystem_signal + memory_signal))

    def build_memory_context(self, task: str, memory_engine) -> Dict[str, Any]:
        fm = self._ensure_memory(memory_engine)
        patterns = fm.query_patterns(task, 5)
        recoveries = fm.query_by_type(task, MEMORY_TYPE_ERROR_RECOVERY, 3)
        conventions = fm.query_by_type(task, MEMORY_TYPE_CONVENTION, 5)
        return {
            "code_patterns": patterns,
            "error_recoveries": recoveries,
            "conventions": conventions,
        }

    def get_system_prompt(self, context: Dict[str, Any]) -> str:
        # Identity
        identity = (
            "You are FORGE, AELVO's principal coding intelligence specialist. "
            "Your capabilities match and exceed Cursor Composer and Claude Code through "
            "rigorous multi-file changeset planning, non-destructive surgical edits, "
            "recursive Run & Fix diagnostics loops, and deep symbol graph analysis. "
            "You never write code blind."
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
            except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        if not locked_rules:
            constraints = context.get("constraints", {}) or {}
            for key, val in constraints.items():
                if isinstance(val, dict) and val.get("locked"):
                    locked_rules.append(f"HARD RULE: {key} = {val.get('value')}")

        hard_rules_section = ""
        if locked_rules:
            hard_rules_section = "HARD CONSTRAINTS:\n" + "\n".join(locked_rules)

        # Action Budget
        budget = context.get("budget", FORGE_ACTION_BUDGET)
        budget_line = f"STEPS REMAINING: {budget} â€” plan your actions, do not waste steps on reads you do not need."

        # User profile
        user_profile_section = ""
        user_model = context.get("user_model")
        if user_model and isinstance(user_model, dict):
            profile_lines = []
            expertise_by_domain = user_model.get("expertise_by_domain", {})
            workflow = user_model.get("current_workflow_mode", "exploring")
            
            has_high_expertise = any(lvl in ("high", "expert") for lvl in expertise_by_domain.values())
            has_low_expertise = any(lvl == "low" for lvl in expertise_by_domain.values())

            if has_high_expertise:
                profile_lines.append("Lead with code directly and skip elementary explanations.")
            elif has_low_expertise:
                profile_lines.append("Explain technical decisions clearly and comment code inline.")

            if workflow == "debugging":
                profile_lines.append("Lead with diagnostic findings and error root cause before presenting fixes.")

            if profile_lines:
                user_profile_section = "USER CALIBRATION PROFILE:\n" + "\n".join(f"- {l}" for l in profile_lines)

        # Prior Knowledge
        prior_section = ""
        memory_engine = context.get("memory_engine")
        if memory_engine:
            try:
                fm = self._ensure_memory(memory_engine)
                task = context.get("task", "")
                
                patterns = (context.get("code_patterns") or fm.query_patterns(task, 5))
                recoveries = (context.get("error_recoveries") or fm.query_by_type(task, MEMORY_TYPE_ERROR_RECOVERY, 3))
                conventions = (context.get("conventions") or fm.query_by_type(task, MEMORY_TYPE_CONVENTION, 5))

                known_lines = []
                for p in patterns:
                    if p.get("score", 0.0) >= FORGE_NOISE_FLOOR:
                        desc = p.get("doc", "").split("\n")[0]
                        file_path = p.get("metadata", {}).get("file_path", "unknown")
                        known_lines.append(f"  pattern: {desc} [{file_path}]")

                error_lines = []
                for r in recoveries:
                    if r.get("score", 0.0) >= FORGE_NOISE_FLOOR:
                        desc = r.get("doc", "").split("\n")[0]
                        sig = r.get("metadata", {}).get("error_signature", "unknown")
                        error_lines.append(f"  error: {sig} â†’ fix: {desc}")

                convention_lines = []
                for c in conventions:
                    if c.get("score", 0.0) >= FORGE_NOISE_FLOOR:
                        desc = c.get("doc", "").split("\n")[0]
                        convention_lines.append(f"  â†’ {desc}")

                blocks = []
                if known_lines:
                    blocks.append("KNOWN IN THIS CODEBASE:\n" + "\n".join(known_lines))
                if error_lines:
                    blocks.append("PAST ERRORS FIXED:\n" + "\n".join(error_lines))
                if convention_lines:
                    blocks.append("ESTABLISHED CONVENTIONS:\n" + "\n".join(convention_lines))

                if blocks:
                    prior_section = "\n\n".join(blocks)
            except Exception as _ex: log.warning("Silenced exception: %s", _ex)

        if not prior_section:
            prior_section = "No prior knowledge for this project â€” extract and save patterns as you work."

        # Project tree structure
        workspace = context.get("workspace_path") or self.workspace
        tree = self._get_project_tree(workspace)
        
        truncated_msg = ""
        if len(tree) > FORGE_MAX_TREE_CHARS_IN_PROMPT:
            tree_content = tree[:FORGE_MAX_TREE_CHARS_IN_PROMPT]
            truncated_msg = "\n[Tree truncated. Use the 'project_tree' tool for the full workspace structure.]"
        else:
            tree_content = tree

        lang_info = self._detect_language_from_tree(tree)
        primary_lang = lang_info["primary_language"]
        if primary_lang == "python":
            toolchain = "ruff + mypy --strict + pytest -x --tb=short"
        elif primary_lang == "typescript":
            toolchain = "eslint + tsc --strict + vitest"
        else:
            toolchain = lang_info.get("toolchain", "ruff + mypy --strict + pytest -x --tb=short")

        project_structure_section = (
            f"PROJECT STRUCTURE:\n{tree_content}{truncated_msg}\n\n"
            f"DETECTED LANGUAGE: {primary_lang} | TOOLCHAIN: {toolchain}"
        )

        # State section (non-runtime)
        state = context.get("state", {}) or {}
        non_runtime_keys = [f"  {k}: {v}" for k, v in state.items() if not k.startswith("runtime:")]
        non_runtime_section = "NON-RUNTIME STATE:\n" + "\n".join(non_runtime_keys) if non_runtime_keys else "NON-RUNTIME STATE:\n  (empty)"

        # Output format specification
        output_format_section = (
            "OUTPUT:\n"
            "JSON array of tool calls only. No prose. No explanation outside the rationale field.\n\n"
            "Example call shape:\n"
            "[\n"
            "  {\n"
            '    "rationale": "Searching memory for similar code patterns to maintain style consistency.",\n'
            '    "tool": "search_memory",\n'
            '    "args": {"query": "auth handler"}\n'
            "  }\n"
            "]"
        )

        # Workflow sequence
        workflow_section = (
            "WORKFLOW & ENGINEERING CONTRACT (Enforce strictly):\n"
            "1. **Query search_memory**: Search prior patterns or failures before reading any files.\n"
            "2. **Use project_tree**: Map the structural workspace layout if not already in context.\n"
            "3. **Navigate Symbol Graphs**: Trace symbol definitions using `build_symbol_graph` and `search_code` before reading files.\n"
            "4. **Use read_file_range**: Bounded reading (never `read_file` for files over 100 lines).\n"
            "5. **Plan Multi-File Changesets**: Map all file changesets, dependencies, and execution order in your planning block.\n"
            "6. **Write in Dependency Order**: Edit base classes, interfaces, and shared types before implementing concrete classes.\n"
            "7. **Non-Destructive Surgical Edits**: Exclusively prioritize `edit_file_block` over full file writes (`write_file`) to keep comments and spacing intact.\n"
            "8. **Iterative 'Run & Fix' Loop**: If a test, linter, or compiler fails, read the line numbers, use `read_file_range` to pull context, fix surgically, and re-verify recursively.\n"
            "9. **Register Conventions & Lock**: Save style repetitions as conventions and execute `#lock` commands to prevent regression.\n"
            "10. **Respond last**: Call `respond` only after all edits are verified and codebase patterns are successfully saved."
        )

        # Build final prompt
        parts = [identity]
        if hard_rules_section:
            parts.append(hard_rules_section)
        parts.append(budget_line)
        if user_profile_section:
            parts.append(user_profile_section)
        parts.extend([
            prior_section,
            project_structure_section,
            non_runtime_section,
            output_format_section,
            workflow_section
        ])

        return "\n\n".join(parts)

    # =====================================================================
    # Session-scoped accumulated data (populated via blackboard.subscribe callbacks)
    # =====================================================================

    def setup_subscriptions(self, blackboard: Any) -> None:
        """Subscribe to blackboard slots for automatic data accumulation.

        Registers callbacks on ``research_findings`` and ``reviews`` slots
        so FORGE automatically receives findings and review results without
        polling.

        Call this once per session, before any phase execution.
        """
        self._findings = []
        self._revisions = []

        def _on_finding(entry: Any) -> None:
            from cognition.blackboard_schemas import FindingEntry
            try:
                f = FindingEntry.from_entry_content(entry.content)
                self._findings.append(f)
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)

        def _on_review(entry: Any) -> None:
            from cognition.blackboard_schemas import ApprovalEntry, RejectionEntry
            try:
                data = ApprovalEntry.from_entry_content(entry.content)
                self._revisions.append({"type": "approval", "data": data})
                return
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)
            try:
                data = RejectionEntry.from_entry_content(entry.content)
                self._revisions.append({"type": "rejection", "data": data})
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)

        blackboard.subscribe("research_findings", _on_finding)
        blackboard.subscribe("reviews", _on_review)

    def clear_session(self) -> None:
        """Clear accumulated subscription data between sessions."""
        self._findings = []
        self._revisions = []

    # =====================================================================
    # Blackboard-Based Collaboration  (Amendment 2 — no agent-to-agent messaging)
    # =====================================================================

    def pickup_task(
        self,
        task_board: Any,
        task_type: Optional[Any] = None,
        max_tasks: int = 1,
    ) -> List[Any]:
        """Pick up pending IMPLEMENT tasks from the SharedTaskBoard.

        Looks for PENDING or ASSIGNED tasks matching the specified type
        (default: IMPLEMENT) and claims them by advancing to IN_PROGRESS.

        This is how FORGE discovers work in Mode B — by polling the
        task board, NOT by receiving direct messages.

        Args:
            task_board: A ``SharedTaskBoard`` instance.
            task_type: ``TaskType`` filter (defaults to ``TaskType.IMPLEMENT``).
            max_tasks: Maximum number of tasks to pick up.

        Returns:
            List of ``Task`` objects that were picked up.
        """
        if task_board is None:
            return []

        from shared_task_board.task import TaskStatus, TaskType

        if task_type is None:
            task_type = TaskType.IMPLEMENT

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
                if task.specialist and task.specialist.upper() != "FORGE":
                    continue
                if status == TaskStatus.PENDING:
                    task_board.assign_task(
                        task.id,
                        specialist="FORGE",
                        assigned_by="architect",
                    )
                task_board.start_task(task.id)
                picked.append(task)
                log.info(
                    "FORGE picked up task %s: %s",
                    task.id[:12], task.title[:60],
                )

        return picked

    def request_research(
        self,
        blackboard: Any,
        question: str,
        task_id: str = "",
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Request research from ORACLE by publishing a QuestionEntry to the blackboard.

        When FORGE encounters an unfamiliar API or library, it publishes
        a question to the ``questions`` blackboard slot.  ORACLE monitors
        this slot and responds with an AnswerEntry.

        No direct messaging.  Questions flow through the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            question: The research question text.
            task_id: Optional task ID this question relates to.
            context: Optional supporting context dict.

        Returns:
            The blackboard entry ID for the published question.
        """
        if blackboard is None:
            return ""

        from cognition.blackboard_schemas import QuestionEntry
        from cognition.types import EntryType, Provenance, ProvenanceType

        context_data = context or {}
        question_entry = QuestionEntry(
            asked_by="FORGE",
            question=question,
            context=context_data,
            directed_to="ORACLE",
            tags=["research-request", "forge"] + (
                [task_id] if task_id else []
            ),
        )
        entry = blackboard.publish(
            slot_name="questions",
            content=question_entry.to_entry_content(),
            entry_type=EntryType.QUERY,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="FORGE",
            ),
            tags=["research-request", "forge"],
        )
        log.info(
            "FORGE requested research on: %s",
            question[:80],
        )
        return entry.id

    def submit_for_review(
        self,
        blackboard: Any,
        summary: str,
        files_changed: Optional[List[str]] = None,
        files_created: Optional[List[str]] = None,
        changes_description: str = "",
        test_summary: str = "",
        security_review_requested: bool = True,
    ) -> str:
        """Submit an implementation for security review by publishing to the blackboard.

        Publishes an ``ImplementationEntry`` to the ``implementations``
        blackboard slot.  SENTINEL monitors this slot and reviews the
        implementation, publishing an ApprovalEntry or RejectionEntry.

        No direct messaging.  Submissions flow through the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            summary: Summary of what was implemented.
            files_changed: Files that were created or modified.
            files_created: New files created.
            changes_description: Detailed description of changes.
            test_summary: Summary of test results.
            security_review_requested: Whether security review is needed.

        Returns:
            The blackboard entry ID for the published implementation.
        """
        if blackboard is None:
            return ""

        from cognition.blackboard_schemas import ImplementationEntry
        from cognition.types import EntryType, Provenance, ProvenanceType

        impl_entry = ImplementationEntry(
            summary=summary,
            files_changed=files_changed or [],
            files_created=files_created or [],
            changes_description=changes_description,
            test_summary=test_summary,
            security_review_requested=security_review_requested,
        )
        entry = blackboard.publish(
            slot_name="implementations",
            content=impl_entry.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="FORGE",
            ),
            tags=["implementation", "forge", "needs-review"] if security_review_requested
                else ["implementation", "forge"],
        )
        log.info(
            "FORGE submitted implementation for review: %s",
            summary[:80],
        )
        return entry.id

    def check_for_revisions(
        self,
        blackboard: Any,
        max_results: int = 10,
    ) -> List[Any]:
        """Check the blackboard for review results on FORGE's implementations.

        Reads ``RejectionEntry`` and ``ApprovalEntry`` payloads from
        the ``reviews`` blackboard slot.  These are published by SENTINEL
        after reviewing FORGE's implementations.

        No direct messaging.  Reviews arrive as blackboard entries.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_results: Maximum number of entries to return.

        Returns:
            List of dicts with ``type`` ('approval' or 'rejection')
            and ``data`` (the parsed schema instance).
        """
        if blackboard is None:
            return []

        from cognition.blackboard_schemas import ApprovalEntry, RejectionEntry
        from cognition.types import EntryType

        entries = blackboard.read(
            slot_name="reviews",
            entry_type=EntryType.FINDING,
        )
        results = []
        for entry in entries[:max_results]:
            # Try parsing as RejectionEntry first (more specific fields)
            try:
                data = RejectionEntry.from_entry_content(entry.content)
                results.append({"type": "rejection", "data": data})
                continue
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)
            # Try parsing as ApprovalEntry
            try:
                data = ApprovalEntry.from_entry_content(entry.content)
                results.append({"type": "approval", "data": data})
                continue
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)
            log.debug("Failed to parse review entry: %s", entry.id[:8])

        return results

    def read_findings(
        self,
        blackboard: Any,
        max_results: int = 10,
    ) -> List[Any]:
        """Read research findings from the blackboard.

        Useful when FORGE needs to consume ORACLE's research results
        while implementing.  Findings are published by ORACLE as
        ``FindingEntry`` payloads.

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
        calls = self._parse_tool_calls(output)
        if not calls:
            return True, "No tool calls detected."

        write_idx = -1
        verify_idx = -1
        respond_idx = -1

        for idx, call in enumerate(calls):
            tool = (call.get("tool") or "").lower()
            if tool in ("write_file", "write_atomic", "edit_file", "edit_file_block"):
                write_idx = idx
            elif tool in ("run_linter", "run_tests", "run_type_checker"):
                verify_idx = idx
            elif tool == "respond":
                respond_idx = idx

        if write_idx != -1:
            if verify_idx == -1:
                return False, "FORGE verification error: A write tool was used, but no verification tool (run_linter, run_tests, run_type_checker) was called."
            if verify_idx < write_idx:
                return False, "FORGE verification error: A verification tool was called, but a write tool was called after it. Verification must occur after all writes."
            if respond_idx != -1 and respond_idx < verify_idx:
                return False, "FORGE verification error: The respond tool was called before verification completed."
            return True, "verified"

        return True, "read-only turn"

    def post_process(self, result: str, memory_engine, conversation_history: List[Dict[str, str]]) -> str:
        fm = self._ensure_memory(memory_engine)
        num_patterns = 0
        num_recoveries = 0
        num_conventions = 0

        calls = self._extract_tool_calls_from_history(conversation_history)

        failed_verifications = []
        linter_rules = {}

        for idx, call in enumerate(calls):
            tool = (call.get("tool") or "").lower()
            args = call.get("args") or {}
            result_data = call.get("_result") or {}
            status = result_data.get("status", "")

            # 1. Patterns
            if tool in ("write_file", "write_atomic", "edit_file", "edit_file_block"):
                content = args.get("content") or args.get("new_block") or ""
                path = args.get("path", "")
                if isinstance(content, str) and content.count("\n") + 1 >= FORGE_MIN_PATTERN_LINES:
                    lang = self._detect_language_from_extension(path)
                    pattern = self._analyze_content_for_pattern(content, lang)
                    if pattern:
                        saved = fm.save_code_pattern(
                            description=pattern.get("description", "")[:FORGE_MAX_PATTERN_DESCRIPTION_CHARS],
                            file_path=path,
                            language=lang,
                            pattern_type=pattern.get("pattern_type", "structural"),
                            signature=pattern.get("signature", ""),
                            context=pattern.get("context", ""),
                        )
                        if saved:
                            num_patterns += 1
        
            # 2. Track failed verifications
            if tool in ("run_linter", "run_tests", "run_type_checker") and status == "error":
                logs = result_data.get("logs", "") or ""
                failed_verifications.append({
                    "idx": idx,
                    "tool": tool,
                    "logs": logs,
                    "args": args
                })
                # Parse rule violations for conventions
                if tool == "run_linter":
                    rule_matches = re.findall(r"\b([A-Z]\d{3}|[a-z-]+/[a-z-]+)\b", logs)
                    for r_code in rule_matches:
                        linter_rules[r_code] = linter_rules.get(r_code, 0) + 1

            # 3. Check for resolved fixes
            if tool in ("run_linter", "run_tests", "run_type_checker") and status == "success":
                if failed_verifications:
                    last_failure = failed_verifications[-1]
                    failure_idx = last_failure["idx"]
                    
                    has_write = False
                    fix_desc = "Code correction applied"
                    fix_applied = ""
                    for j in range(failure_idx + 1, idx):
                        mid_call = calls[j]
                        mid_tool = (mid_call.get("tool") or "").lower()
                        if mid_tool in ("write_file", "write_atomic", "edit_file", "edit_file_block"):
                            has_write = True
                            mid_args = mid_call.get("args") or {}
                            fix_applied = mid_args.get("content") or mid_args.get("new_block") or ""
                            fix_desc = f"Fixed issues in {mid_args.get('path', 'code')}"
                            break
                            
                    if has_write:
                        error_sig = hashlib.sha256(f"{last_failure['tool']}:{last_failure['logs']}".encode()).hexdigest()[:12]
                        saved = fm.save_error_recovery(
                            error_signature=error_sig,
                            fix_description=fix_desc,
                            file_path=last_failure["args"].get("path", "unknown"),
                            language=self._detect_language_from_extension(last_failure["args"].get("path", "")),
                            error_output=last_failure["logs"][:400],
                            fix_applied=fix_applied[:400]
                        )
                        if saved:
                            num_recoveries += 1

                        failed_verifications.clear()

        # 4. Conventions (3 or more fires)
        # NOTE: post_process() has no `context` parameter, so look up the
        # filesystem/kernel off memory_engine if present (safe no-op otherwise).
        fs = getattr(memory_engine, "fs", None)
        kernel = getattr(fs, "kernel", None) if fs else None
        for rule, count in linter_rules.items():
            if count >= 3:
                desc = f"Linter Rule {rule} must be satisfied without violations."
                saved = fm.save_convention(
                    convention_description=desc,
                    source_file="linter",
                    language="all"
                )
                if saved:
                    num_conventions += 1
                    if kernel:
                        try:
                            kernel.parse_and_execute(f"#lock LINTER_{rule} locked")
                        except Exception as _ex: log.warning("Silenced exception: %s", _ex)

        return f"FORGE extracted {num_patterns} patterns, {num_recoveries} error recoveries, {num_conventions} conventions this turn."


    def _parse_tool_calls(self, output: str) -> List[Dict[str, Any]]:
        text = (output or "").strip()
        candidates: List[str] = []
        if "```json" in text:
            try:
                block = text.split("```json", 1)[1].split("```", 1)[0].strip()
                candidates.append(block)
            except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        candidates.append(text)
        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, list):
                    return [c for c in parsed if isinstance(c, dict)]
                if isinstance(parsed, dict):
                    return [parsed]
            except Exception:
                continue
        return []

    def _extract_tool_calls_from_history(self, history: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        calls = []
        assistant_calls = []
        for i, msg in enumerate(history):
            role = msg.get("role", "")
            content = (msg.get("content") or "").strip()
            if role == "assistant":
                turn_calls = self._parse_tool_calls(content)
                assistant_calls.append((i, turn_calls))

        for idx, (ast_idx, turn_calls) in enumerate(assistant_calls):
            user_idx = ast_idx + 1
            if user_idx < len(history):
                user_msg = history[user_idx]
                if user_msg.get("role") == "user":
                    user_content = user_msg.get("content") or ""
                    results = []
                    for block in re.findall(r"```json\s*([\s\S]*?)\s*```", user_content):
                        try:
                            parsed = json.loads(block.strip())
                            if isinstance(parsed, list):
                                results.extend(parsed)
                            elif isinstance(parsed, dict):
                                results.append(parsed)
                        except Exception as _ex: log.warning("Silenced exception: %s", _ex)
                    if not results:
                        try:
                            clean_json = user_content
                            for prefix in ("[RESULT]", "[BATCH RESULTS]", "[AELVO EXECUTOR â€” TOOL RESULT]"):
                                if clean_json.startswith(prefix):
                                    clean_json = clean_json[len(prefix):].strip()
                            parsed = json.loads(clean_json)
                            if isinstance(parsed, list):
                                results = parsed
                            elif isinstance(parsed, dict):
                                results = [parsed]
                        except Exception as _ex: log.warning("Silenced exception: %s", _ex)

                    for c_idx, call in enumerate(turn_calls):
                        call_copy = dict(call)
                        if c_idx < len(results):
                            call_copy["_result"] = results[c_idx]
                        else:
                            call_copy["_result"] = None
                        calls.append(call_copy)
            else:
                for call in turn_calls:
                    call_copy = dict(call)
                    call_copy["_result"] = None
                    calls.append(call_copy)
        return calls

    def _analyze_content_for_pattern(self, content: str, language: str) -> Optional[Dict[str, str]]:
        lines = content.splitlines()
        if len(lines) < FORGE_MIN_PATTERN_LINES:
            return None

        if content.strip().startswith(("{", "[")) and content.strip().endswith(("}", "]")):
            return None

        if language == "python":
            try:
                tree = ast.parse(content)
            except SyntaxError:
                return None

            classes = []
            functions = []
            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.ClassDef):
                    bases = [getattr(b, "id", "") for b in node.bases]
                    bases_str = f"({', '.join(bases)})" if bases else ""
                    doc = ast.get_docstring(node) or ""
                    classes.append((node.name, bases_str, doc))
                elif isinstance(node, ast.FunctionDef):
                    args = [a.arg for a in node.args.args]
                    doc = ast.get_docstring(node) or ""
                    ret = ""
                    if node.returns:
                        if isinstance(node.returns, ast.Name):
                            ret = f" -> {node.returns.id}"
                    functions.append((node.name, args, ret, doc))

            if not classes and not functions:
                return None

            if classes:
                name, bases, doc = classes[0]
                desc = f"Class {name}{bases} pattern"
                sig = f"class {name}{bases}"
                ctx = doc if doc else "No docstring provided."
            else:
                name, args, ret, doc = functions[0]
                desc = f"Function {name}({', '.join(args)}){ret} pattern"
                sig = f"def {name}({', '.join(args)}){ret}"
                ctx = doc if doc else "No docstring provided."

            return {
                "description": desc,
                "signature": sig,
                "pattern_type": "ast_extracted",
                "context": ctx[:200]
            }

        elif language in ("typescript", "javascript"):
            class_match = re.search(r"\bclass\s+(\w+)(?:\s+extends\s+(\w+))?", content)
            func_match = re.search(r"\b(?:async\s+)?function\s+(\w+)\s*\(([^)]*)\)", content)
            arrow_match = re.search(r"\bconst\s+(\w+)\s*=\s*\(([^)]*)\)\s*=>", content)

            if class_match:
                name = class_match.group(1)
                extends = class_match.group(2)
                extends_str = f" extends {extends}" if extends else ""
                return {
                    "description": f"{language} class {name}{extends_str} definition",
                    "signature": f"class {name}{extends_str}",
                    "pattern_type": "regex_class",
                    "context": content[:200]
                }
            elif func_match:
                name = func_match.group(1)
                args = func_match.group(2)
                return {
                    "description": f"{language} function {name}({args}) definition",
                    "signature": f"function {name}({args})",
                    "pattern_type": "regex_function",
                    "context": content[:200]
                }
            elif arrow_match:
                name = arrow_match.group(1)
                args = arrow_match.group(2)
                return {
                    "description": f"{language} arrow function {name}({args}) definition",
                    "signature": f"const {name} = ({args}) =>",
                    "pattern_type": "regex_arrow_function",
                    "context": content[:200]
                }

        elif language == "rust":
            struct_match = re.search(r"\bstruct\s+(\w+)", content)
            fn_match = re.search(r"\bfn\s+(\w+)\s*\(([^)]*)\)", content)
            trait_match = re.search(r"\btrait\s+(\w+)", content)
            impl_match = re.search(r"\bimpl\s+(\w+)", content)

            if struct_match:
                name = struct_match.group(1)
                return {
                    "description": f"Rust struct {name} definition",
                    "signature": f"struct {name}",
                    "pattern_type": "rust_struct",
                    "context": content[:200]
                }
            elif fn_match:
                name = fn_match.group(1)
                args = fn_match.group(2)
                return {
                    "description": f"Rust function fn {name}({args}) definition",
                    "signature": f"fn {name}({args})",
                    "pattern_type": "rust_fn",
                    "context": content[:200]
                }
            elif trait_match:
                name = trait_match.group(1)
                return {
                    "description": f"Rust trait {name} definition",
                    "signature": f"trait {name}",
                    "pattern_type": "rust_trait",
                    "context": content[:200]
                }
            elif impl_match:
                name = impl_match.group(1)
                return {
                    "description": f"Rust impl {name} block",
                    "signature": f"impl {name}",
                    "pattern_type": "rust_impl",
                    "context": content[:200]
                }

        else:
            if len(lines) > 20:
                for keyword in ("class", "interface", "struct", "fn", "function", "def", "module"):
                    if re.search(rf"\b{keyword}\b", content):
                        return {
                            "description": f"Generic structural pattern using keyword '{keyword}'",
                            "signature": f"Generic {keyword} block",
                            "pattern_type": "generic_structural",
                            "context": content[:200]
                        }
        return None

    def _get_project_tree(self, workspace: str) -> str:
        now = time.time()
        if self._tree_cache and now - self._tree_cache_time < FORGE_PROJECT_TREE_CACHE_TTL:
            return self._tree_cache

        if self.fs and hasattr(self.fs, "project_tree"):
            result = self.fs.project_tree(max_depth=2, max_entries=200)
            tree = result.get("logs", "") if isinstance(result, dict) else str(result)
        else:
            tree = context_tree_fallback(workspace)

        self._tree_cache = tree
        self._tree_cache_time = now
        return tree

    def _detect_language_from_tree(self, tree_text: str) -> Dict[str, Any]:
        counts: Dict[str, int] = {}
        for line in tree_text.splitlines():
            for ext, lang in _EXT_LANGUAGE.items():
                if line.strip().endswith(ext) or ext[1:] + "." in line:
                    if line.rstrip().endswith(ext):
                        counts[lang] = counts.get(lang, 0) + 1

        for match in re.finditer(r"\.([a-zA-Z0-9]+)(?:\s|$)", tree_text):
            ext = "." + match.group(1).lower()
            if ext in _EXT_LANGUAGE:
                lang = _EXT_LANGUAGE[ext]
                counts[lang] = counts.get(lang, 0) + 1

        all_langs = sorted(counts.keys(), key=lambda k: counts[k], reverse=True)
        primary = all_langs[0] if all_langs else "python"
        toolchains = {
            "python": "ruff + mypy --strict + pytest",
            "typescript": "eslint + tsc --strict + vitest",
            "javascript": "eslint + prettier + vitest",
            "rust": "cargo clippy + rustfmt + cargo test",
            "go": "go vet + gofmt + go test",
        }
        return {
            "primary_language": primary,
            "all_languages": all_langs,
            "toolchain": toolchains.get(primary, "ruff + mypy + pytest"),
            "all_toolchains": {lang: toolchains.get(lang, "generic") for lang in all_langs},
        }

    def _detect_language_from_extension(self, file_path: str) -> str:
        ext = Path(file_path).suffix.lower()
        return _EXT_LANGUAGE.get(ext, "unknown")


def context_tree_fallback(workspace: str) -> str:
    lines = [Path(workspace).name + "/"]
    skip = {".git", "node_modules", "__pycache__", "chroma_db", "dist", ".venv"}
    count = 0
    try:
        for root, dirs, files in os.walk(workspace):
            dirs[:] = [d for d in sorted(dirs) if d not in skip]
            rel = Path(root).relative_to(workspace)
            depth = len(rel.parts)
            if depth > 2:
                continue
            indent = "  " * depth
            for d in dirs[:20]:
                if count >= 200:
                    break
                lines.append(f"{indent}  {d}/")
                count += 1
            for f in sorted(files)[:30]:
                if count >= 200:
                    break
                lines.append(f"{indent}  {f}")
                count += 1
    except Exception as exc:
        lines.append(f"  (tree error: {exc})")
    return "\n".join(lines)
