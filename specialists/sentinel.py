# sentinel.py - SENTINEL Hardened Security Specialist for AELVO OMEGA

import time
import os
import json
import hashlib
import re
import logging
from typing import Dict, List, Tuple, Any, Optional
from specialists.base import BaseSpecialist
from tools.security_tools import SECRET_PATTERNS
from memory import MEMORY_TYPE_SECURITY_RULE


log = logging.getLogger("aelvo.specialists.sentinel")


class SentinelSpecialist(BaseSpecialist):
    """SENTINEL detects secrets, vulnerabilities, and produces ready-to-paste remediated code."""

    name: str = "SENTINEL"
    trigger_patterns: List[str] = [
        "security", "vulnerability", "secret", "private key", "scan", "token",
        "aws_key", "password", "exploit", "attack", "sqli", "xss", "ssrf",
        "traversal", "leak", "audit", "cve", "auth", "harden", "patch",
        "credential", "encrypt", "decrypt",
    ]
    memory_types: List[str] = [MEMORY_TYPE_SECURITY_RULE]
    required_tools: List[str] = ["read_file", "write_file"]
    activation_threshold: float = 0.5

    def compute_activation_score(self, task: str, context: Dict[str, Any]) -> float:
        score = super().compute_activation_score(task, context)
        clean = task.lower()
        if any(w in clean for w in ("sqli", "xss", "ssrf", "injection", "exposed", "hardcoded", "leak", "compromised", "cve-", "rce")):
            score += 0.4
        if any(w in clean for w in ("security", "scan", "audit", "secret", "vulnerability", "harden", "credential")):
            score += 0.25
        return min(1.0, max(0.0, score))

    def get_system_prompt(self, context: Dict[str, Any]) -> str:
        budget = context.get("budget", 30)

        rules = context.get("security_rules", []) or []
        rules_str = "\n".join(
            f"  - SECURITY RULE: {r.get('doc', '')[:200]}"
            for r in rules[:5]
        ) or "  - No prior security rules recorded for this project."

        # Dependency inventory.
        workspace = context.get("workspace_path", ".")
        dep_files = []
        for filename in ("requirements.txt", "package.json", "Cargo.toml", "go.mod", "Gemfile", "pom.xml", "pyproject.toml"):
            full = os.path.join(workspace, filename)
            if os.path.exists(full):
                dep_files.append(filename)
        deps_str = ", ".join(dep_files) if dep_files else "None detected"

        constraints = context.get("constraints", {}) or {}
        constraints_str = "\n".join(
            f"HARD RULE: {k} = {v.get('value')}" for k, v in constraints.items()
        ) or "(no locked constraints)"

        return f"""You are SENTINEL, AELVO's security specialist.
You think like an attacker. Every scan you run becomes a permanent guardrail for FORGE.

DEPENDENCY FILES DETECTED: {deps_str}

EXISTING SECURITY RULES (apply as hard guardrails)
{rules_str}

HARD CONSTRAINTS
{constraints_str}
BUDGET: {budget} steps remaining.

SENTINEL EXECUTION RULES:
1. Before declaring code clean, run scan_for_secrets and scan_for_vulnerabilities on every file you touched.
2. For every finding, use simulate_attack to demonstrate the exact exploit payload that would succeed.
3. Provide a remediated_code block â€” copy-paste ready â€” for every vulnerability.
4. Severity ratings: secrets/RCE=CRITICAL, SQLi/SSRF=HIGH, XSS/path traversal=MEDIUM unless context elevates them.
5. Never output a raw secret. Always redact: first 4 + last 2 chars only.
6. Save every finding as a security_rule so future FORGE writes inherit the guardrail automatically.
7. Respond only after every finding has either been fixed or surfaced with a clear remediation plan.
"""

    def build_memory_context(self, task: str, memory_engine) -> Dict[str, Any]:
        project = getattr(memory_engine, "project_name", "default")
        rules: List[Dict[str, Any]] = []
        try:
            res = memory_engine.memory_collection.query(
                query_texts=[task],
                n_results=5,
                where={"$and": [{"type": MEMORY_TYPE_SECURITY_RULE}, {"project": project}]},
            )
            if res.get("ids") and res["ids"][0]:
                for doc, dist in zip(res["documents"][0], res["distances"][0]):
                    rules.append({"doc": doc, "score": round(1.0 - float(dist), 3)})
        except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        return {"security_rules": rules}

    def post_process(self, result: str, memory_engine, conversation_history: List[Dict[str, str]]) -> str:
        project = getattr(memory_engine, "project_name", "default")
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        audits: List[str] = []

        from core.rag import MemorySearcher
        searcher = MemorySearcher(memory_engine.memory_collection)

        # Mine real findings from tool results in conversation history AND the current result.
        all_events = self._extract_security_events(conversation_history)
        all_events.extend(self._extract_security_events_from_text(result))

        for event in all_events:
            data = event.get("data") or []
            executed = event.get("executed") or {}
            file_path = executed.get("file") or ""

            if not isinstance(data, list):
                continue

            for finding in data[:10]:
                if not isinstance(finding, dict):
                    continue
                vuln_type = (finding.get("type") or "unknown").lower().replace(" ", "_").replace("-", "_")
                severity = finding.get("severity", "HIGH")
                line = finding.get("line", 0)
                description = finding.get("description") or finding.get("redacted_value") or ""

                doc = (
                    f"SECURITY [{severity}] {vuln_type} in {file_path}:{line} â€” "
                    f"{str(description)[:200]}"
                )

                # resolve_conflict before write â€” security_rule importance is high,
                # so override threshold (0.85) prunes stale and fresh takes precedence
                if searcher.resolve_conflict(doc, meta_type=MEMORY_TYPE_SECURITY_RULE):
                    continue

                m_id = hashlib.sha256(f"sec_{time.time()}_{file_path}_{vuln_type}_{line}".encode()).hexdigest()
                importance = 1.0 if severity == "CRITICAL" else (0.9 if severity == "HIGH" else 0.75)
                meta = {
                    "type": MEMORY_TYPE_SECURITY_RULE,
                    "vulnerability_type": vuln_type,
                    "severity": severity,
                    "file_path": file_path,
                    "pattern_to_avoid": str(description)[:200],
                    "remediated_example": "",
                    "timestamp": timestamp,
                    "timestamp_unix": time.time(),
                    "importance": importance,
                    "usage_count": 1,
                    "project": project,
                    "source_specialist": "sentinel",
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
                            (f"[SENTINEL:security_rule|{project}] {doc[:800]}",),
                        )
                    audits.append(f"security_rule:{vuln_type}@{file_path}:{line}")
                except Exception:
                    try:
                        memory_engine.memory_collection.delete(ids=[m_id])
                    except Exception as _ex: log.warning("Silenced exception: %s", _ex)


        return f"[SENTINEL AUDIT] {', '.join(audits) if audits else 'no new security rules'}"


    @staticmethod
    def _extract_security_events_from_text(content: str) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        if not content:
            return events

        for block in re.findall(r"```json\n([\s\S]+?)\n```", content):
            try:
                data = json.loads(block)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            logs = (data.get("logs") or "").lower()
            executed = data.get("executed") or {}
            if (
                "secret" in logs
                or "vulnerability" in logs
                or "secrets_found" in executed
                or "flaws_found" in executed
            ):
                events.append(data)

        # Also, check if there are direct structured SECURITY rules in the text
        # of the form: SECURITY [severity] type in file:line â€” description
        # or similar structured lines.
        rule_pattern = r"SECURITY\s+\[(CRITICAL|HIGH|MEDIUM|LOW)\]\s+(\S+?)\s+in\s+(\S+?):(\d+)\s*[â€”-]\s*(.*)"
        for match in re.finditer(rule_pattern, content, re.IGNORECASE):
            severity = match.group(1).upper()
            vuln_type = match.group(2).lower()
            file_path = match.group(3)
            try:
                line = int(match.group(4))
            except ValueError:
                line = 0
            description = match.group(5).strip()

            # Synthesize an event dictionary that post_process expects
            events.append({
                "executed": {"file": file_path},
                "data": [{
                    "type": vuln_type,
                    "severity": severity,
                    "line": line,
                    "description": description
                }]
            })
        return events


    @staticmethod
    def _extract_security_events(history: List[Dict[str, str]]) -> List[Dict[str, Any]]:
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
                logs = (data.get("logs") or "").lower()
                executed = data.get("executed") or {}
                if (
                    "secret" in logs
                    or "vulnerability" in logs
                    or "secrets_found" in executed
                    or "flaws_found" in executed
                ):
                    events.append(data)
        return events

    # =====================================================================
    # Session-scoped accumulated data (populated via blackboard.subscribe callbacks)
    # =====================================================================

    def __init__(self):
        """Initialize SENTINEL specialist."""
        super().__init__()
        self._implementations: List[Any] = []
        self._escalations: List[Any] = []

    def setup_subscriptions(self, blackboard: Any) -> None:
        """Subscribe to blackboard slots for automatic data accumulation.

        Registers callbacks on ``implementations`` and ``security_escalations``
        slots so SENTINEL automatically receives implementations to review
        and escalation results without polling.

        Call this once per session, before any phase execution.
        """
        self._implementations = []
        self._escalations = []

        def _on_implementation(entry: Any) -> None:
            from cognition.blackboard_schemas import ImplementationEntry
            try:
                impl = ImplementationEntry.from_entry_content(entry.content)
                self._implementations.append(impl)
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)

        def _on_escalation(entry: Any) -> None:
            from cognition.blackboard_schemas import EscalationEntry
            try:
                e = EscalationEntry.from_entry_content(entry.content)
                self._escalations.append(e)
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)

        blackboard.subscribe("implementations", _on_implementation)
        blackboard.subscribe("security_escalations", _on_escalation)

    def clear_session(self) -> None:
        """Clear accumulated subscription data between sessions."""
        self._implementations = []
        self._escalations = []

    # =====================================================================
    # Blackboard-Based Collaboration  (Amendment 2 — no agent-to-agent messaging)
    # =====================================================================

    def pickup_task(
        self,
        task_board: Any,
        task_type: Optional[Any] = None,
        max_tasks: int = 1,
    ) -> List[Any]:
        """Pick up pending SECURITY_REVIEW tasks from the SharedTaskBoard.

        Looks for PENDING or ASSIGNED tasks matching the specified type
        (default: SECURITY_REVIEW) and claims them by advancing to
        IN_PROGRESS.

        This is how SENTINEL discovers work in Mode B — by polling the
        task board, NOT by receiving direct messages.

        Args:
            task_board: A ``SharedTaskBoard`` instance.
            task_type: ``TaskType`` filter (defaults to ``TaskType.SECURITY_REVIEW``).
            max_tasks: Maximum number of tasks to pick up.

        Returns:
            List of ``Task`` objects that were picked up.
        """
        if task_board is None:
            return []

        from shared_task_board.task import TaskStatus, TaskType

        if task_type is None:
            task_type = TaskType.SECURITY_REVIEW

        picked = []
        # Look for pending and already-assigned tasks
        for status in (TaskStatus.PENDING, TaskStatus.ASSIGNED):
            tasks = task_board.get_tasks(
                status=status,
                task_type=task_type,
                limit=max_tasks * 3,
            )
            # Filter to SENTINEL-specific tasks
            for task in tasks:
                if len(picked) >= max_tasks:
                    break
                if task.specialist and task.specialist.upper() != "SENTINEL":
                    continue
                if status == TaskStatus.PENDING:
                    task_board.assign_task(
                        task.id,
                        specialist="SENTINEL",
                        assigned_by="architect",
                    )
                task_board.start_task(task.id)
                picked.append(task)
                log.info(
                    "SENTINEL picked up task %s: %s",
                    task.id[:12], task.title[:60],
                )

        return picked

    def read_implementations(
        self,
        blackboard: Any,
        max_results: int = 10,
    ) -> List[Any]:
        """Read implementations pending review from the blackboard.

        FORGE publishes ``ImplementationEntry`` payloads to the
        ``implementations`` slot.  SENTINEL reads them to perform
        security review.

        No direct messaging.  Submissions arrive as blackboard entries.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_results: Maximum implementations to return.

        Returns:
            List of ``ImplementationEntry`` instances.
        """
        if blackboard is None:
            return []

        from cognition.blackboard_schemas import ImplementationEntry
        from cognition.types import EntryType

        entries = blackboard.read(
            slot_name="implementations",
            entry_type=EntryType.FINDING,
        )
        impls = []
        for entry in entries[:max_results]:
            try:
                impl = ImplementationEntry.from_entry_content(entry.content)
                impls.append(impl)
            except Exception as e:
                log.debug("Failed to parse implementation entry: %s", e)
                continue

        return impls

    def approve_implementation(
        self,
        blackboard: Any,
        summary: str,
        entry_id: str = "",
        conditions: Optional[List[str]] = None,
        confidence: float = 0.85,
    ) -> str:
        """Approve an implementation by publishing an ApprovalEntry to the blackboard.

        Publishes to the ``reviews`` slot where FORGE monitors for
        results.  This is SENTINEL's approval authority — it means the
        implementation passes security review.

        No direct messaging.  Approvals flow through the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            summary: Approval summary / reason.
            entry_id: ID of the implementation entry being approved.
            conditions: Optional conditions that must be met.
            confidence: Confidence level (0.0-1.0).

        Returns:
            The blackboard entry ID for the published approval.
        """
        if blackboard is None:
            return ""

        from cognition.blackboard_schemas import ApprovalEntry
        from cognition.types import EntryType, Provenance, ProvenanceType

        approval = ApprovalEntry(
            approved_by="SENTINEL",
            entry_id=entry_id,
            reason=summary,
            conditions=conditions or [],
            confidence=confidence,
        )
        entry = blackboard.publish(
            slot_name="reviews",
            content=approval.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="SENTINEL",
            ),
            confidence=confidence,
            tags=["security", "approval", "sentinel"],
        )
        log.info(
            "SENTINEL approved implementation %s (confidence=%.2f, %d conditions)",
            entry_id[:8] if entry_id else "?",
            confidence,
            len(conditions or []),
        )
        return entry.id

    def reject_implementation(
        self,
        blackboard: Any,
        entry_id: str = "",
        reason: str = "",
        findings: Optional[List[str]] = None,
        remediations: Optional[List[str]] = None,
        severity: str = "medium",
    ) -> str:
        """Reject an implementation by publishing a RejectionEntry to the blackboard.

        Publishes to the ``reviews`` slot.  REJECTION means the
        implementation has security issues that must be fixed before
        it can proceed.

        No direct messaging.  Rejections flow through the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            entry_id: ID of the implementation entry being rejected.
            reason: Why it was rejected.
            findings: Specific issues found during review.
            remediations: Suggested fixes / remediations.
            severity: Severity level (low, medium, high, critical).

        Returns:
            The blackboard entry ID for the published rejection.
        """
        if blackboard is None:
            return ""

        from cognition.blackboard_schemas import RejectionEntry
        from cognition.types import EntryType, Provenance, ProvenanceType

        rejection = RejectionEntry(
            rejected_by="SENTINEL",
            entry_id=entry_id,
            reason=reason,
            findings=findings or [],
            remediations=remediations or [],
            severity=severity,
        )
        entry = blackboard.publish(
            slot_name="reviews",
            content=rejection.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="SENTINEL",
            ),
            confidence=0.9,
            tags=["security", "rejection", "sentinel", severity],
        )
        log.info(
            "SENTINEL REJECTED implementation %s (severity=%s, %d findings)",
            entry_id[:8] if entry_id else "?",
            severity,
            len(findings or []),
        )
        return entry.id

    def escalate_to_architect(
        self,
        blackboard: Any,
        reason: str,
        context: Optional[Dict[str, Any]] = None,
        suggested_action: str = "",
        urgency: str = "medium",
    ) -> str:
        """Escalate a security issue to ARCHITECT by publishing an EscalationEntry.

        When SENTINEL identifies a security issue that requires
        architectural changes rather than implementation fixes, it
        escalates to the ``security_escalations`` blackboard slot.
        ARCHITECT monitors this slot and decides on action.

        No direct messaging.  Escalations flow through the blackboard.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            reason: Why escalation is needed.
            context: Supporting context dict.
            suggested_action: What SENTINEL suggests Architect do.
            urgency: Urgency level (low, medium, high, critical).

        Returns:
            The blackboard entry ID for the published escalation.
        """
        if blackboard is None:
            return ""

        from cognition.blackboard_schemas import EscalationEntry
        from cognition.types import EntryType, Provenance, ProvenanceType

        escalation = EscalationEntry(
            escalated_by="SENTINEL",
            reason=reason,
            context=context or {},
            suggested_action=suggested_action,
            urgency=urgency,
        )
        entry = blackboard.publish(
            slot_name="security_escalations",
            content=escalation.to_entry_content(),
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.SPECIALIST,
                source_id="SENTINEL",
            ),
            confidence=0.85,
            tags=["security", "escalation", "sentinel", urgency],
        )
        log.warning(
            "SENTINEL escalated to ARCHITECT (urgency=%s): %s",
            urgency, reason[:80],
        )
        return entry.id

    def check_for_escalations(
        self,
        blackboard: Any,
        max_results: int = 10,
    ) -> List[Any]:
        """Check the blackboard for unresolved escalations.

        Reads ``EscalationEntry`` payloads from the
        ``security_escalations`` blackboard slot.  Useful for monitoring
        whether previous escalations have been addressed.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_results: Maximum escalations to return.

        Returns:
            List of ``EscalationEntry`` instances.
        """
        if blackboard is None:
            return []

        from cognition.blackboard_schemas import EscalationEntry
        from cognition.types import EntryType

        entries = blackboard.read(
            slot_name="security_escalations",
            entry_type=EntryType.DECISION,
        )
        escalations = []
        for entry in entries[:max_results]:
            try:
                e = EscalationEntry.from_entry_content(entry.content)
                escalations.append(e)
            except Exception as exc:
                log.debug("Failed to parse escalation entry: %s", exc)
                continue

        return escalations

    # =====================================================================
    # Challenge System (Phase 6 — Real Challenge Workflow)
    # =====================================================================

    def challenge_low_confidence(
        self,
        blackboard: Any,
        entry_id: str,
        challenger: str = "SENTINEL",
        confidence_threshold: float = 0.7,
    ) -> Optional[str]:
        """Challenge a blackboard entry if its confidence is below threshold.

        Reviews an entry's confidence level and, if insufficient, raises
        a formal challenge through the blackboard's challenge system.
        The challenge includes SENTINEL's rationale and triggers the
        consensus workflow.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            entry_id: The ID of the entry to challenge.
            challenger: Who is raising the challenge (default: SENTINEL).
            confidence_threshold: Minimum acceptable confidence (0.0-1.0).

        Returns:
            The challenge ID if a challenge was raised, None if the entry
            meets the confidence threshold or was not found.
        """
        if blackboard is None:
            return None

        # Find the entry across all slots
        entry = None
        for entry_list in [
            blackboard.read(slot_name="research_findings"),
            blackboard.read(slot_name="implementations"),
        ]:
            for e in entry_list:
                if e.id == entry_id:
                    entry = e
                    break
            if entry:
                break

        if entry is None:
            log.warning("SENTINEL challenge failed: entry %s not found", entry_id[:8])
            return None

        # Check confidence
        if entry.confidence >= confidence_threshold:
            log.debug(
                "SENTINEL review: entry %s confidence %.2f >= %.2f — no challenge needed",
                entry_id[:8], entry.confidence, confidence_threshold,
            )
            return None

        # Raise the challenge
        challenged_claim = (
            f"Insufficient confidence: entry {entry_id[:8]} has confidence "
            f"{entry.confidence:.2f} which is below the threshold of {confidence_threshold:.2f}. "
            f"Content: {entry.content[:100]}"
        )
        challenge_entry = blackboard.challenge(
            slot_type=entry.slot_name,
            entry_id=entry_id,
            challenger=challenger,
            challenged_claim=challenged_claim,
            evidence=f"SENTINEL confidence review: entry confidence={entry.confidence:.2f}, threshold={confidence_threshold:.2f}",
            proposed_alternative=f"Requesting additional evidence to raise confidence above {confidence_threshold:.2f}",
        )

        log.warning(
            "SENTINEL challenged entry %s (confidence=%.2f < threshold=%.2f) — challenge_id=%s",
            entry_id[:8], entry.confidence, confidence_threshold,
            challenge_entry.challenge_id[:8],
        )
        return challenge_entry.challenge_id

    def review_findings(
        self,
        blackboard: Any,
        max_results: int = 10,
        confidence_threshold: float = 0.7,
    ) -> List[Dict[str, Any]]:
        """Review ORACLE's findings and challenge low-confidence ones.

        Scans the ``research_findings`` slot for entries below the
        confidence threshold and raises challenges for each.

        Args:
            blackboard: A ``CognitiveBlackboard`` instance.
            max_results: Maximum number of findings to review.
            confidence_threshold: Minimum acceptable confidence.

        Returns:
            List of dicts with 'entry_id', 'confidence', 'challenge_id'
            for each challenged finding.
        """
        if blackboard is None:
            return []

        from cognition.types import EntryType

        findings = blackboard.read(
            slot_name="research_findings",
            entry_type=EntryType.FINDING,
        )
        results = []
        for entry in findings[:max_results]:
            challenge_id = self.challenge_low_confidence(
                blackboard=blackboard,
                entry_id=entry.id,
                confidence_threshold=confidence_threshold,
            )
            if challenge_id:
                results.append({
                    "entry_id": entry.id,
                    "confidence": entry.confidence,
                    "challenge_id": challenge_id,
                })
        return results

    def verify_output(self, output: str, context: Dict[str, Any]) -> Tuple[bool, str]:
        """Reject any response that leaks raw credentials."""
        for name, regex in SECRET_PATTERNS.items():
            if regex.search(output):
                return False, f"Output verification failed: raw {name} pattern detected in response."
        return True, "Security verification passed."

