# terminus.py - TERMINUS DevOps & Pipeline Specialist for AELVO OMEGA

import logging
import time
import shutil
import re
import json
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from specialists.base import BaseSpecialist
from tools.git_tools import get_git_state
from memory import MEMORY_TYPE_DEVOPS_PATTERN


log = logging.getLogger("aelvo.specialists.terminus")


class ExecutionBlockedError(Exception):
    """Raised when TERMINUS cannot execute because Architect has not approved.

    Per Amendment 3: TERMINUS is a hard gate that checks the Architect Decision
    before any state-modifying command. If the decision is missing, not APPROVE,
    or conditions are unsatisfied, this error is raised with full context.
    """

    def __init__(
        self,
        reason: str = "",
        target_id: str = "",
        command: str = "",
        decision_outcome: Optional[str] = None,
    ):
        self.reason = reason
        self.target_id = target_id
        self.command = command
        self.decision_outcome = decision_outcome
        super().__init__(self.__str__())

    def __str__(self) -> str:
        parts = ["ExecutionBlocked"]
        if self.reason:
            parts.append(f"reason={self.reason}")
        if self.target_id:
            parts.append(f"target={self.target_id[:12]}")
        if self.command:
            parts.append(f"command={self.command[:60]}")
        if self.decision_outcome:
            parts.append(f"decision={self.decision_outcome}")
        return " | ".join(parts)


# Patterns that mark a command as state-modifying or destructive.
_DESTRUCTIVE_PATTERNS = (
    re.compile(r"\brm\b", re.IGNORECASE),
    re.compile(r"\bdel\b", re.IGNORECASE),
    re.compile(r"\brmdir\b", re.IGNORECASE),
    re.compile(r"\bdrop\s+(table|database|index)\b", re.IGNORECASE),
    re.compile(r"\bgit\s+push\b", re.IGNORECASE),
    re.compile(r"\bgit\s+reset\s+--hard\b", re.IGNORECASE),
    re.compile(r"\bgit\s+clean\b", re.IGNORECASE),
    re.compile(r"\bdocker\s+system\s+prune\b", re.IGNORECASE),
    re.compile(r"\bkubectl\s+delete\b", re.IGNORECASE),
    re.compile(r"\bterraform\s+destroy\b", re.IGNORECASE),
    re.compile(r"\bshutdown\b", re.IGNORECASE),
    re.compile(r"\bformat\s+[a-z]:", re.IGNORECASE),
    re.compile(r"\bmkfs\b", re.IGNORECASE),
    re.compile(r"\bdd\s+if=", re.IGNORECASE),
)

_NETWORK_PATTERNS = (
    re.compile(r"\bcurl\b", re.IGNORECASE),
    re.compile(r"\bwget\b", re.IGNORECASE),
    re.compile(r"\bgit\s+(push|pull|fetch|clone)\b", re.IGNORECASE),
    re.compile(r"\b(npm|pip|cargo)\s+(install|publish)\b", re.IGNORECASE),
)


class TerminusSpecialist(BaseSpecialist):
    """TERMINUS plans and executes terminal pipelines, git workflows, and deployment sequences."""

    name: str = "TERMINUS"
    trigger_patterns: List[str] = [
        "bash", "shell", "terminal", "run", "execute", "command", "git",
        "commit", "push", "checkout", "stash", "pull", "docker", "compose",
        "npm", "cargo", "pip", "env", "subprocess", "deployment", "script",
        "ci", "cd", "kubectl", "terraform",
    ]
    memory_types: List[str] = [MEMORY_TYPE_DEVOPS_PATTERN]
    required_tools: List[str] = ["bash_exec"]
    activation_threshold: float = 0.6

    def compute_activation_score(self, task: str, context: Dict[str, Any]) -> float:
        score = super().compute_activation_score(task, context)
        clean = task.lower()
        if any(w in clean for w in ("npm ", "pip ", "cargo ", "git ", "docker", "cd ", "mkdir", "mv ", "cp ", "ls ", "kubectl", "terraform")):
            score += 0.35
        if "|" in task or ">" in task or "&&" in task:
            score += 0.15
        return min(1.0, max(0.0, score))

    def get_system_prompt(self, context: Dict[str, Any]) -> str:
        budget = context.get("budget", 30)

        patterns = context.get("devops_patterns", []) or []
        patterns_str = "\n".join(
            f"  - PATTERN: {p.get('doc', '')[:200]} | Score: {p.get('score', 0.5)}"
            for p in patterns[:5]
        ) or "  - No prior devops patterns recorded."

        # Live tool availability snapshot.
        available_tools = [
            tool for tool in (
                "git", "docker", "docker-compose", "npm", "pnpm", "yarn", "pip", "uv",
                "cargo", "go", "gh", "aws", "gcloud", "az", "kubectl", "terraform", "ansible", "helm",
            ) if shutil.which(tool) is not None
        ]
        tools_str = ", ".join(available_tools) if available_tools else "None"

        # Live git state.
        workspace = context.get("workspace_path", ".")
        git_str = "NOT_A_GIT_REPOSITORY"
        try:
            git_state = get_git_state(workspace)
            if git_state.get("status") == "success":
                gd = git_state["data"]
                git_str = (
                    f"Branch: {gd.get('branch')}\n"
                    f"Staged: {len(gd.get('staged_files', []))}, "
                    f"Unstaged: {len(gd.get('unstaged_files', []))}, "
                    f"Untracked: {len(gd.get('untracked_files', []))}, "
                    f"Stash: {gd.get('stash_count', 0)}\n"
                    f"Recent: " + " | ".join(gd.get("recent_commits", [])[:3])
                )
        except Exception as _ex: print("Silenced exception: %s", _ex)

        constraints = context.get("constraints", {}) or {}
        constraints_str = "\n".join(
            f"HARD RULE: {k} = {v.get('value')}" for k, v in constraints.items()
        ) or "(no locked constraints)"

        return f"""You are TERMINUS, AELVO's DevOps and pipeline specialist.
Your edge is plan-before-execute: every destructive command is dry-run, justified, and recovery-ready.

AVAILABLE TOOLS: {tools_str}

LIVE GIT STATE
{git_str}

HARD CONSTRAINTS
{constraints_str}
BUDGET: {budget} steps remaining.

HISTORICAL DEVOPS PATTERNS
{patterns_str}

TERMINUS RULES:
1. For any sequence of bash commands, classify each as read-only, state-modifying, or destructive.
2. Destructive or network-touching commands require an explicit user confirmation step before execution.
3. For git commits, generate the message from the actual staged diff via 'generate_commit_message'.
4. For docker/k8s/terraform, always show the dry-run / plan output before applying.
5. For dependency installs, prefer the project's existing manager (pnpm/yarn/uv) detected from lockfiles.
6. On failure, capture the exact error signature and store it as a devops_pattern with severity tags.
7. Never wildcard-delete. Never `rm -rf /`. Never push to main without an explicit instruction.
8. Output planned actions as a JSON tool-call array; respond at the end with a clean summary.
"""

    def build_memory_context(self, task: str, memory_engine) -> Dict[str, Any]:
        project = getattr(memory_engine, "project_name", "default")
        patterns: List[Dict[str, Any]] = []
        try:
            res = memory_engine.memory_collection.query(
                query_texts=[task],
                n_results=5,
                where={"type": MEMORY_TYPE_DEVOPS_PATTERN, "project": project},
            )
            if res.get("ids") and res["ids"][0]:
                for doc, dist in zip(res["documents"][0], res["distances"][0]):
                    patterns.append({"doc": doc, "score": round(1.0 - float(dist), 3)})
        except Exception as _ex: print("Silenced exception: %s", _ex)
        return {"devops_patterns": patterns}

    # ------------------------------------------------------------------
    # PLAN COMMAND SEQUENCE
    # ------------------------------------------------------------------
    def plan_command_sequence(self, commands: List[str]) -> Dict[str, Any]:
        """Classify and order shell commands; flag destructive ones for confirmation."""
        plan: List[Dict[str, Any]] = []
        requires_confirmation = False

        for idx, cmd in enumerate(commands, start=1):
            classification = "read_only"
            recovery: List[str] = []

            if any(p.search(cmd) for p in _DESTRUCTIVE_PATTERNS):
                classification = "destructive"
                requires_confirmation = True
                recovery = self._recovery_for(cmd)
            elif any(p.search(cmd) for p in _NETWORK_PATTERNS):
                classification = "network"
                requires_confirmation = True
            elif re.search(r"\b(git\s+(add|commit|stash)|mkdir|touch|cp|mv|chmod|chown|export)\b", cmd, re.IGNORECASE):
                classification = "state_modifying"

            plan.append({
                "step": idx,
                "command": cmd,
                "classification": classification,
                "recovery_steps": recovery,
                "blocks_remaining_on_failure": classification != "read_only",
            })

        return {
            "status": "success",
            "logs": (
                f"Plan compiled for {len(commands)} command(s); "
                f"{'requires confirmation' if requires_confirmation else 'no destructive ops detected'}."
            ),
            "executed": {
                "command_count": len(commands),
                "requires_confirmation": requires_confirmation,
            },
            "data": {"plan": plan, "requires_confirmation": requires_confirmation},
        }

    @staticmethod
    def _recovery_for(cmd: str) -> List[str]:
        if re.search(r"\bgit\s+reset\s+--hard\b", cmd, re.IGNORECASE):
            return ["git reflog", "git reset --hard <previous-sha>"]
        if re.search(r"\bgit\s+push\b", cmd, re.IGNORECASE):
            return ["git push --force-with-lease origin <branch>", "or revert with git revert <sha>"]
        if re.search(r"\brm\b", cmd, re.IGNORECASE):
            return ["restore from backup or git checkout HEAD -- <path>"]
        if re.search(r"\bdocker\s+system\s+prune\b", cmd, re.IGNORECASE):
            return ["pull required images again; rebuild from Dockerfile"]
        if re.search(r"\bkubectl\s+delete\b", cmd, re.IGNORECASE):
            return ["kubectl apply -f <manifest>"]
        if re.search(r"\bterraform\s+destroy\b", cmd, re.IGNORECASE):
            return ["terraform apply against the previous plan"]
        return ["no automated recovery; restore from backup"]

    # ------------------------------------------------------------------
    # POST PROCESS
    # ------------------------------------------------------------------
    def post_process(self, result: str, memory_engine, conversation_history: List[Dict[str, str]]) -> str:
        project = getattr(memory_engine, "project_name", "default")
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        audits: List[str] = []

        from core.rag import MemorySearcher
        searcher = MemorySearcher(memory_engine.memory_collection)

        bash_events = self._extract_bash_events(conversation_history)

        # Collect successful non-trivial sequences (pipeline operators, git, docker).
        successful = [ev for ev in bash_events if ev.get("status") == "success"]
        nontrivial = [
            ev for ev in successful
            if len((ev.get("executed") or {}).get("command", "")) > 0 and (
                "&&" in ev["executed"]["command"]
                or "|" in ev["executed"]["command"]
                or "git " in ev["executed"]["command"]
                or "docker" in ev["executed"]["command"]
            )
        ]

        for ev in nontrivial[-3:]:
            cmd = ev["executed"]["command"]
            doc = f"DevOps pattern: {cmd[:200]}"
            # resolve_conflict before write
            if searcher.resolve_conflict(doc, meta_type=MEMORY_TYPE_DEVOPS_PATTERN):
                continue
            m_id = hashlib.sha256(f"devops_{time.time()}_{cmd}".encode()).hexdigest()
            meta = {
                "type": MEMORY_TYPE_DEVOPS_PATTERN,
                "command_signature": cmd[:200],
                "timestamp": timestamp,
                "timestamp_unix": time.time(),
                "importance": 0.75,
                "usage_count": 1,
                "project": project,
                "source_specialist": "terminus",
            }
            try:
                memory_engine.memory_collection.add(ids=[m_id], documents=[doc], metadatas=[meta])
            except Exception:
                continue
            # SQLite dual-sync
            try:
                with memory_engine.db:
                    memory_engine.db.execute(
                        "INSERT INTO retained_memory (content) VALUES (?)",
                        (f"[TERMINUS:devops_pattern|{project}] {doc[:800]}",),
                    )
                audits.append("devops_pattern saved")
            except Exception:
                try:
                    memory_engine.memory_collection.delete(ids=[m_id])
                except Exception as _ex: print("Silenced exception: %s", _ex)

        # Capture failures + their fixes (failure â†’ success on similar command).
        for i, ev in enumerate(bash_events):
            if ev.get("status") != "error":
                continue
            cmd = (ev.get("executed") or {}).get("command", "")
            for later in bash_events[i + 1:]:
                if later.get("status") == "success" and self._command_similarity(
                    cmd, (later.get("executed") or {}).get("command", "")
                ) > 0.6:
                    failure_doc = (
                        f"DevOps recovery: failed `{cmd[:120]}` -> succeeded with "
                        f"`{(later.get('executed') or {}).get('command', '')[:120]}`. "
                        f"Error excerpt: {(ev.get('logs') or '')[:120]}"
                    )
                    if searcher.resolve_conflict(failure_doc, meta_type=MEMORY_TYPE_DEVOPS_PATTERN):
                        break
                    m_id = hashlib.sha256(f"devops_rec_{time.time()}_{cmd[:30]}".encode()).hexdigest()
                    meta = {
                        "type": MEMORY_TYPE_DEVOPS_PATTERN,
                        "tag": "error_recovery",
                        "timestamp": timestamp,
                        "timestamp_unix": time.time(),
                        "importance": 0.8,
                        "usage_count": 1,
                        "project": project,
                        "source_specialist": "terminus",
                    }
                    try:
                        memory_engine.memory_collection.add(ids=[m_id], documents=[failure_doc], metadatas=[meta])
                    except Exception:
                        break
                    try:
                        with memory_engine.db:
                            memory_engine.db.execute(
                                "INSERT INTO retained_memory (content) VALUES (?)",
                                (f"[TERMINUS:devops_recovery|{project}] {failure_doc[:800]}",),
                            )
                        audits.append("devops error_recovery saved")
                    except Exception:
                        try:
                            memory_engine.memory_collection.delete(ids=[m_id])
                        except Exception as _ex: print("Silenced exception: %s", _ex)
                    break

        return f"[TERMINUS AUDIT] {', '.join(audits) if audits else 'no new devops patterns'}"


    @staticmethod
    def _extract_bash_events(history: List[Dict[str, str]]) -> List[Dict[str, Any]]:
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
                if "command" in executed:
                    events.append(data)
        return events

    @staticmethod
    def _command_similarity(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        ta = set(re.findall(r"[\w\-]+", a.lower()))
        tb = set(re.findall(r"[\w\-]+", b.lower()))
        if not ta or not tb:
            return 0.0
        return len(ta & tb) / len(ta | tb)

    def verify_output(self, output: str, context: Dict[str, Any]) -> Tuple[bool, str]:
        # Extract bash commands embedded in the response and reject hard-blocks.
        for block in re.findall(r"```(?:bash|sh|shell)?\n([\s\S]+?)\n```", output):
            for line in block.splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                # Hard block: literal `rm -rf /`, mkfs, dd if=/dev/sda
                if re.search(r"\brm\s+-rf\s+/(?:\s|$)", stripped) or re.search(r"\bmkfs\b", stripped) or re.search(r"\bdd\s+if=/dev/", stripped):
                    return False, f"Output contains a hard-blocked destructive command: '{stripped}'"
        return True, "Pipeline validations passed."

    # =====================================================================
    # Session-scoped accumulated data (populated via blackboard.subscribe callbacks)
    # =====================================================================

    def __init__(self):
        super().__init__()
        self._execution_results: List[Any] = []

    def setup_subscriptions(self, blackboard: Any) -> None:
        """Subscribe to blackboard slots for automatic data accumulation.

        Registers callbacks on ``execution_results`` slot
        so TERMINUS automatically receives execution results without polling.

        Call this once per session, before any phase execution.
        """
        self._execution_results = []

        def _on_execution_result(entry: Any) -> None:
            from cognition.blackboard_schemas import ExecutionResultEntry
            try:
                r = ExecutionResultEntry.from_entry_content(entry.content)
                self._execution_results.append(r)
            except Exception:
                pass

        blackboard.subscribe("execution_results", _on_execution_result)

    def clear_session(self) -> None:
        """Clear accumulated subscription data between sessions."""
        self._execution_results = []

    # =====================================================================
    # Blackboard-Based Collaboration  (Amendment 2 — no agent-to-agent messaging)
    # =====================================================================

    def pickup_task(
        self,
        task_board: Any,
        task_type: Optional[Any] = None,
        max_tasks: int = 1,
    ) -> List[Any]:
        """Pick up pending EXECUTION tasks from the SharedTaskBoard.

        Looks for PENDING or ASSIGNED tasks matching the specified type
        (default: EXECUTION) and claims them by advancing to IN_PROGRESS.

        This is how TERMINUS discovers work in Mode B — by polling the
        task board, NOT by receiving direct messages.

        Args:
            task_board: A ``SharedTaskBoard`` instance.
            task_type: ``TaskType`` filter (defaults to ``TaskType.EXECUTION``).
            max_tasks: Maximum number of tasks to pick up.

        Returns:
            List of ``Task`` objects that were picked up.
        """
        if task_board is None:
            return []

        from shared_task_board.task import TaskStatus, TaskType

        if task_type is None:
            task_type = TaskType.EXECUTE

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
                if task.specialist and task.specialist.upper() != "TERMINUS":
                    continue
                if status == TaskStatus.PENDING:
                    task_board.assign_task(
                        task.id,
                        specialist="TERMINUS",
                        assigned_by="architect",
                    )
                task_board.start_task(task.id)
                picked.append(task)
                log.info(
                    "TERMINUS picked up task %s: %s",
                    task.id[:12], task.title[:60],
                )

        return picked

    def check_architect_decision(
        self,
        blackboard: Any,
        target_id: str = "",
        command: str = "",
    ) -> Dict[str, Any]:
        """Check the blackboard for an ArchitectDecision approving execution.

        Per Amendment 3: TERMINUS is a hard execution gate. Before any
        state-modifying command, this method checks the ``architect_decisions``
        blackboard slot for a matching APPROVE decision.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            target_id: The plan ID or task ID to check for approval.
            command: The command being checked (for error context).

        Returns:
            Dict with:
            - ``approved`` (bool): Whether execution is allowed
            - ``decision`` (ArchitectDecision or None): The matching decision
            - ``conditions`` (list): Any conditions from the decision

        Raises:
            ExecutionBlockedError: If no matching APPROVE decision exists.
        """
        if blackboard is None:
            raise ExecutionBlockedError(
                reason="No blackboard available — cannot verify Architect decision",
                command=command,
            )

        from cognition.architect_decision import ArchitectDecision, ArchitectDecisionOutcome
        from cognition.types import EntryType

        entries = blackboard.read(
            slot_name="architect_decisions",
            entry_type=EntryType.DECISION,
        )

        for entry in entries:
            try:
                decision = ArchitectDecision(**json.loads(entry.content))
            except Exception as e:
                log.debug("Failed to parse architect decision entry: %s", e)
                continue

            # If a target_id is specified, match against it
            if target_id and decision.target_id != target_id:
                continue

            if decision.outcome == ArchitectDecisionOutcome.APPROVE:
                log.info(
                    "TERMINUS gate: Architect APPROVED execution for target %s",
                    target_id[:12] if target_id else "any",
                )
                return {
                    "approved": True,
                    "decision": decision,
                    "conditions": decision.conditions,
                }

            # Decision exists but is not APPROVE — block execution
            raise ExecutionBlockedError(
                reason=f"Architect decision is {decision.outcome.value}, not APPROVE",
                target_id=target_id,
                command=command,
                decision_outcome=decision.outcome.value,
            )

        # No matching decision found — block execution
        raise ExecutionBlockedError(
            reason=f"No Architect decision found for target {target_id[:20] if target_id else '(any)'}",
            target_id=target_id,
            command=command,
        )

    def publish_execution_result(
        self,
        blackboard: Any,
        command: str = "",
        exit_code: int = 0,
        stdout: str = "",
        stderr: str = "",
        success: bool = True,
        duration_ms: float = 0.0,
        task_id: str = "",
    ) -> str:
        """Publish an execution result to the blackboard.

        Publishes an ``ExecutionResultEntry`` to the ``execution_results``
        blackboard slot.  All specialists can consume these results.

        No direct messaging.  Results flow through the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            command: The command that was executed.
            exit_code: The exit code.
            stdout: Stdout summary.
            stderr: Stderr summary.
            success: Whether execution succeeded.
            duration_ms: Execution duration in milliseconds.
            task_id: Optional task ID this result belongs to.

        Returns:
            The blackboard entry ID for the published result.
        """
        if blackboard is None:
            return ""

        from cognition.blackboard_schemas import ExecutionResultEntry
        from cognition.types import EntryType, Provenance, ProvenanceType

        result = ExecutionResultEntry(
            specialist="TERMINUS",
            command=command,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            success=success,
            duration_ms=duration_ms,
        )
        entry = blackboard.publish(
            slot_name="execution_results",
            content=result.to_entry_content(),
            entry_type=EntryType.FACT,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="TERMINUS",
            ),
            confidence=0.95 if success else 0.5,
            tags=[
                "execution",
                "result",
                "terminus",
                "success" if success else "failure",
            ] + ([f"task:{task_id}"] if task_id else []),
        )
        log.info(
            "TERMINUS published execution result (exit=%d, success=%s, duration=%.0fms)",
            exit_code, success, duration_ms,
        )
        return entry.id

    def publish_failure_report(
        self,
        blackboard: Any,
        command: str = "",
        error_message: str = "",
        exit_code: int = -1,
        recovery_suggestion: str = "",
        task_id: str = "",
    ) -> str:
        """Publish a failure report to the blackboard.

        Publishes failure details to the ``execution_failures`` slot.
        All specialists (especially RecoveryEngine) can consume these
        reports and take action.

        No direct messaging.  Failures flow through the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            command: The command that failed.
            error_message: The error description.
            exit_code: The exit code.
            recovery_suggestion: Suggested recovery action.
            task_id: Optional task ID this failure belongs to.

        Returns:
            The blackboard entry ID for the published failure.
        """
        if blackboard is None:
            return ""

        from cognition.types import EntryType, Provenance, ProvenanceType

        failure_payload = json.dumps({
            "specialist": "TERMINUS",
            "command": command,
            "error": error_message,
            "exit_code": exit_code,
            "recovery_suggestion": recovery_suggestion,
            "task_id": task_id,
        })
        entry = blackboard.publish(
            slot_name="execution_failures",
            content=failure_payload,
            entry_type=EntryType.CONSTRAINT,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="TERMINUS",
            ),
            confidence=1.0,
            tags=[
                "execution",
                "failure",
                "terminus",
                "recovery-needed",
            ] + ([f"task:{task_id}"] if task_id else []),
        )
        log.error(
            "TERMINUS reported failure (exit=%d): %s",
            exit_code, error_message[:80],
        )
        return entry.id

    def read_execution_results(
        self,
        blackboard: Any,
        max_results: int = 10,
    ) -> List[Any]:
        """Read execution results from the blackboard.

        Reads ``ExecutionResultEntry`` payloads from the
        ``execution_results`` slot.  Useful for monitoring whether
        previous executions succeeded or failed.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_results: Maximum results to return.

        Returns:
            List of ``ExecutionResultEntry`` instances.
        """
        if blackboard is None:
            return []

        from cognition.blackboard_schemas import ExecutionResultEntry
        from cognition.types import EntryType

        entries = blackboard.read(
            slot_name="execution_results",
            entry_type=EntryType.FACT,
        )
        results = []
        for entry in entries[:max_results]:
            try:
                r = ExecutionResultEntry.from_entry_content(entry.content)
                results.append(r)
            except Exception as e:
                log.debug("Failed to parse execution result entry: %s", e)
                continue

        return results
