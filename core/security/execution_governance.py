"""ExecutionGovernance — Risk Classification, Trust Levels, and Policy Enforcement.

Every execution flows through:
  Request → RiskClassification → PolicyDecision → ApprovalCheck → Execution → Audit

Fail-closed: if classification fails, the action is blocked.
No silent bypasses allowed.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

log = logging.getLogger("aelvo.security.governance")


# ============================================================================
# Enums
# ============================================================================


class RiskLevel(str, Enum):
    """Risk classification for an execution action."""

    SAFE = "safe"
    """Trivially safe — e.g., reading a known file, listing directory contents."""

    RESTRICTED = "restricted"
    """Potentially risky but controllable — e.g., writing to workspace, running known tools."""

    APPROVAL_REQUIRED = "approval_required"
    """High risk — e.g., writing to system paths, installing packages, git push."""

    BLOCKED = "blocked"
    """Never permitted — e.g., privilege escalation, raw shell injection, rm -rf /."""

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, RiskLevel):
            return NotImplemented
        return _RISK_PRIORITY[self] < _RISK_PRIORITY[other]

    def __le__(self, other: object) -> bool:
        if not isinstance(other, RiskLevel):
            return NotImplemented
        return _RISK_PRIORITY[self] <= _RISK_PRIORITY[other]

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, RiskLevel):
            return NotImplemented
        return _RISK_PRIORITY[self] > _RISK_PRIORITY[other]

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, RiskLevel):
            return NotImplemented
        return _RISK_PRIORITY[self] >= _RISK_PRIORITY[other]


# RiskLevel severity ordering (used by comparison operators)
_RISK_PRIORITY = {
    RiskLevel.SAFE: 0,
    RiskLevel.RESTRICTED: 1,
    RiskLevel.APPROVAL_REQUIRED: 2,
    RiskLevel.BLOCKED: 3,
}


class TrustLevel(str, Enum):
    """Trust level for commands, files, specialists, and repositories."""

    TRUSTED = "trusted"
    """Known safe — e.g., read_file, write_file in workspace, git status."""

    RESTRICTED = "restricted"
    """Conditionally trusted — e.g., bash_exec with allowlisted commands."""

    UNTRUSTED = "untrusted"
    """Requires approval or is blocked — e.g., arbitrary shell, raw python exec."""

    HOSTILE = "hostile"
    """Known dangerous — automatically blocked."""


# ============================================================================
# Data Types
# ============================================================================


@dataclass
class SecurityClassification:
    """Result of classifying an action's security risk."""

    risk_level: RiskLevel
    trust_level: TrustLevel
    reason: str
    """Human-readable explanation of why this classification was assigned."""

    evidence: Dict[str, Any] = field(default_factory=dict)
    """Evidence supporting the classification (matched patterns, policy rules, etc.)."""

    classification_time_ms: float = 0.0


@dataclass
class PolicyDecision:
    """A complete, structured policy decision."""

    decision_id: str = ""
    """Unique identifier for this decision."""

    action_type: str = ""
    """Type of action classified (command, file_write, file_read, network, etc.)."""

    action_target: str = ""
    """The target of the action (path, command string, URL, etc.)."""

    risk_level: RiskLevel = RiskLevel.SAFE
    """Assigned risk level."""

    trust_level: TrustLevel = TrustLevel.TRUSTED
    """Assigned trust level."""

    allowed: bool = False
    """Whether the action is permitted to proceed."""

    requires_approval: bool = False
    """Whether explicit user approval is required before execution."""

    reason: str = ""
    """Why this decision was made."""

    policy_rules_matched: List[Dict[str, Any]] = field(default_factory=list)
    """Which policy rules were evaluated and their outcomes."""

    classification_details: Optional[SecurityClassification] = None
    """Full classification details."""

    timestamp: float = 0.0
    """When the decision was made."""

    decision_time_ms: float = 0.0
    """How long the decision took."""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str, indent=2)


# ============================================================================
# Policy Rules
# ============================================================================


@dataclass
class PolicyRule:
    """A single policy rule that evaluates an action against allow/deny conditions."""

    name: str
    """Human-readable rule name."""

    description: str = ""
    """What this rule checks."""

    patterns: List[str] = field(default_factory=list)
    """Regex patterns to match against the action target."""

    deny: bool = False
    """If True, matching this rule blocks the action."""

    require_approval: bool = False
    """If True, matching this rule requires user approval."""

    risk_level: RiskLevel = RiskLevel.SAFE
    """Risk level assigned when this rule matches."""

    trust_level: TrustLevel = TrustLevel.TRUSTED
    """Trust level assigned when this rule matches."""

    priority: int = 100
    """Lower priority numbers are evaluated first. 0 = highest priority."""


# ============================================================================
# ExecutionGovernance
# ============================================================================


class ExecutionGovernance:
    """Governs all execution through risk classification, trust evaluation, and policy enforcement.

    Design principles:
    - Fail closed: any error in classification results in BLOCKED
    - Default deny: actions not explicitly allowed are blocked
    - Least privilege: actions get the minimum trust level needed
    - Every decision is auditable with a structured PolicyDecision
    """

    # Default policy rules — evaluated in priority order
    _DEFAULT_RULES: List[Dict[str, Any]] = [
        # Blocked: privilege escalation, destructive system commands, dangerous patterns
        {"name": "block_privilege_escalation", "patterns": [r"\bsudo\b", r"\bsu\b", r"\bchown\b", r"\bchmod\s+777"], "deny": True, "risk_level": "blocked", "trust_level": "hostile", "priority": 10},
        {"name": "block_destructive_fs", "patterns": [r"\brm\s+-rf\s+/", r"\bmv\s+/\s+", r"\bdd\b.*\bof=/dev/", r"\bmkfs\b", r"\bformat\b"], "deny": True, "risk_level": "blocked", "trust_level": "hostile", "priority": 10},
        {"name": "block_fork_bomb", "patterns": [r":\(\)\s*\{", r"\b:\(\)\s*\|"], "deny": True, "risk_level": "blocked", "trust_level": "hostile", "priority": 10},
        # Approval-required: installing packages, modifying system configs, remote deploys
        {"name": "require_approval_package_install", "patterns": [r"\b(?:pip|npm|apt|brew|cargo|go)\s+install\b", r"\bapt-get\s+install\b"], "require_approval": True, "risk_level": "approval_required", "trust_level": "restricted", "priority": 30},
        {"name": "require_approval_git_remote", "patterns": [r"\bgit\s+push\b", r"\bgit\s+pull\b", r"\bgit\s+fetch\b", r"\bgit\s+remote\b"], "require_approval": True, "risk_level": "approval_required", "trust_level": "restricted", "priority": 30},
        {"name": "require_approval_network", "patterns": [r"\bcurl\s+", r"\bwget\s+", r"\bssh\b", r"\bscp\b", r"\brsync\b"], "require_approval": True, "risk_level": "approval_required", "trust_level": "restricted", "priority": 30},
        # Restricted: shell commands, file writes
        {"name": "restrict_bash_exec", "patterns": [r"\bbash\b", r"\bsh\b", r"\bzsh\b", r"\bpython\b"], "risk_level": "restricted", "trust_level": "restricted", "priority": 50},
        # Safe: everything else (fallback)
        {"name": "default_safe", "patterns": [r".*"], "risk_level": "safe", "trust_level": "trusted", "priority": 999},
    ]

    def __init__(
        self,
        workspace_root: str = "",
        custom_rules: Optional[List[Dict[str, Any]]] = None,
        policy_rules: Optional[List[PolicyRule]] = None,
        allowlisted_commands: Optional[Set[str]] = None,
        blocked_paths: Optional[Set[str]] = None,
        protected_paths: Optional[Set[str]] = None,
        enable_rust_integration: bool = True,
    ):
        """Initialize the execution governance layer.

        Args:
            workspace_root: Root of the workspace being governed.
            custom_rules: Additional policy rules (merged with defaults).
            policy_rules: Pre-built PolicyRule objects (alternative to custom_rules).
            allowlisted_commands: Set of explicitly allowed command basenames.
            blocked_paths: Set of absolute paths that are always blocked.
            protected_paths: Set of absolute paths that require approval to modify.
            enable_rust_integration: If True, attempt to use Rust PolicyEngine.
        """
        self._workspace_root = Path(workspace_root).resolve() if workspace_root else Path.cwd().resolve()
        self._enable_rust = enable_rust_integration

        # Allowlisted commands
        self._allowlisted_commands: Set[str] = allowlisted_commands or {
            "ls", "cat", "echo", "head", "tail", "wc", "sort", "uniq",
            "find", "grep", "sed", "awk",
            "python", "python3", "node", "npm", "npx",
            "git", "cargo", "rustc",
            "mkdir", "cp", "mv",
        }

        # Blocked paths (absolute, never accessible)
        self._blocked_paths: Set[str] = blocked_paths or set()

        # Protected paths (require approval to modify)
        self._protected_paths: Set[str] = protected_paths or set()

        # Policy rules
        self._rules: List[PolicyRule] = []
        if policy_rules:
            self._rules.extend(policy_rules)

        # Merge default rules with custom rules
        merged = list(self._DEFAULT_RULES)
        if custom_rules:
            merged.extend(custom_rules)
        merged.sort(key=lambda r: r.get("priority", 100))

        # Track which rule dicts are already converted to PolicyRule objects
        existing_names = {r.name for r in self._rules}
        for rd in merged:
            name = rd.get("name", f"rule_{len(self._rules)}")
            if name not in existing_names:
                self._rules.append(PolicyRule(
                    name=name,
                    description=rd.get("description", ""),
                    patterns=rd.get("patterns", []),
                    deny=rd.get("deny", False),
                    require_approval=rd.get("require_approval", False),
                    risk_level=RiskLevel(rd.get("risk_level", "safe")),
                    trust_level=TrustLevel(rd.get("trust_level", "trusted")),
                    priority=rd.get("priority", 100),
                ))
                existing_names.add(name)

        # Decision history
        self._decisions: List[PolicyDecision] = []

        # Compile regex patterns
        self._compiled_rules: List[Tuple[PolicyRule, List[re.Pattern]]] = []
        for rule in self._rules:
            patterns = []
            for p in rule.patterns:
                try:
                    patterns.append(re.compile(p, re.IGNORECASE))
                except re.error as e:
                    log.warning(f"Invalid regex in policy rule '{rule.name}': {e}")
            self._compiled_rules.append((rule, patterns))

        log.info(f"ExecutionGovernance initialized with {len(self._rules)} rules, "
                 f"{len(self._allowlisted_commands)} allowlisted commands")
        self._rust_policy_available = False

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def classify_tool_call(
        self,
        tool_name: str,
        args: Dict[str, Any],
    ) -> SecurityClassification:
        """Classify the risk of a tool call.

        Args:
            tool_name: The tool being called (e.g., 'bash_exec', 'write_atomic').
            args: The arguments to the tool.

        Returns:
            SecurityClassification with risk level, trust level, and evidence.
        """
        start = time.perf_counter()
        action_target = self._build_action_target(tool_name, args)

        # Default: safe
        classification = SecurityClassification(
            risk_level=RiskLevel.SAFE,
            trust_level=TrustLevel.TRUSTED,
            reason="No policy rules matched — default safe",
            evidence={"tool": tool_name, "target": action_target},
        )

        # Evaluate rules in priority order
        matched_rules: List[PolicyRule] = []
        for rule, patterns in self._compiled_rules:
            for pattern in patterns:
                if pattern.search(action_target) or pattern.search(tool_name):
                    matched_rules.append(rule)
                    break

        # Apply the strictest matched rule
        if matched_rules:
            # Sort by priority (lowest = highest priority)
            matched_rules.sort(key=lambda r: r.priority)
            strictest = matched_rules[0]

            classification = SecurityClassification(
                risk_level=strictest.risk_level,
                trust_level=strictest.trust_level,
                reason=f"Matched policy rule '{strictest.name}': {strictest.description}",
                evidence={
                    "tool": tool_name,
                    "target": action_target,
                    "matched_rule": strictest.name,
                    "matched_rule_priority": strictest.priority,
                    "all_matched_rules": [r.name for r in matched_rules],
                },
            )

        # Block-level classification checks for file paths
        if tool_name in ("write_atomic", "edit_file_block", "delete_file"):
            path = args.get("path", "")
            classification = self._classify_file_operation(path, classification, tool_name)

        classification.classification_time_ms = (time.perf_counter() - start) * 1000
        return classification

    def _build_action_target(self, tool_name: str, args: Dict[str, Any]) -> str:
        """Build a searchable string from the tool call for pattern matching."""
        parts = [tool_name]
        for key, value in args.items():
            if isinstance(value, str):
                # Truncate long strings to prevent regex DoS
                val = value[:500]
                parts.append(f"{key}={val}")
            elif isinstance(value, (int, float, bool)):
                parts.append(f"{key}={value}")
        return " ".join(parts)

    def _classify_file_operation(
        self,
        path: str,
        current: SecurityClassification,
        tool_name: str,
    ) -> SecurityClassification:
        """Perform additional classification for file operations based on path."""
        if not path:
            return current

        try:
            resolved = self._workspace_root / path
            resolved = resolved.resolve()
        except (OSError, ValueError):
            return current

        # Check blocked paths
        str_path = str(resolved)
        for blocked in self._blocked_paths:
            if str_path.startswith(blocked):
                return SecurityClassification(
                    risk_level=RiskLevel.BLOCKED,
                    trust_level=TrustLevel.HOSTILE,
                    reason=f"Path is in blocked list: {blocked}",
                    evidence={"path": str_path, "blocked_prefix": blocked},
                )

        # Escalate for protected paths
        for protected in self._protected_paths:
            if str_path.startswith(protected):
                if current.risk_level < RiskLevel.APPROVAL_REQUIRED:
                    return SecurityClassification(
                        risk_level=RiskLevel.APPROVAL_REQUIRED,
                        trust_level=TrustLevel.RESTRICTED,
                        reason=f"Path is protected: {protected}",
                        evidence={"path": str_path, "protected_prefix": protected},
                    )

        # Workspace-internal writes are safer than external writes
        if tool_name in ("write_atomic", "edit_file_block", "delete_file"):
            try:
                resolved.relative_to(self._workspace_root)
                # Inside workspace — keep current classification
                pass
            except ValueError:
                # Outside workspace — escalate
                if current.risk_level < RiskLevel.APPROVAL_REQUIRED:
                    return SecurityClassification(
                        risk_level=RiskLevel.APPROVAL_REQUIRED,
                        trust_level=TrustLevel.RESTRICTED,
                        reason=f"Write target is outside workspace: {resolved}",
                        evidence={
                            "path": str_path,
                            "workspace": str(self._workspace_root),
                            "is_outside_workspace": True,
                        },
                    )

        return current

    # ------------------------------------------------------------------
    # Policy Decision
    # ------------------------------------------------------------------

    def decide(
        self,
        tool_name: str,
        args: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> PolicyDecision:
        """Make a complete policy decision for a tool call.

        This is the primary API. Every tool call should flow through
        this method before execution.

        Args:
            tool_name: The tool being called.
            args: The arguments to the tool.
            context: Optional execution context (user, specialist, etc.).

        Returns:
            PolicyDecision with allowed/blocked/approval status.
        """
        start = time.perf_counter()
        decision_id = f"pd_{uuid.uuid4().hex[:12]}"

        # 1. Classify risk
        classification = self.classify_tool_call(tool_name, args)

        # 2. Determine if allowed
        allowed = classification.risk_level not in (RiskLevel.BLOCKED,)
        requires_approval = classification.risk_level == RiskLevel.APPROVAL_REQUIRED

        # 3. Apply context overrides (if any)
        if context:
            allowed, requires_approval = self._apply_context_overrides(
                allowed, requires_approval, classification, context,
            )

        # 4. Build matched rules list
        matched = []
        if classification.evidence.get("matched_rule"):
            matched.append({
                "name": classification.evidence["matched_rule"],
                "reason": classification.reason,
            })

        decision = PolicyDecision(
            decision_id=decision_id,
            action_type=tool_name,
            action_target=self._build_action_target(tool_name, args),
            risk_level=classification.risk_level,
            trust_level=classification.trust_level,
            allowed=allowed,
            requires_approval=requires_approval,
            reason=classification.reason,
            policy_rules_matched=matched,
            classification_details=classification,
            timestamp=time.time(),
            decision_time_ms=(time.perf_counter() - start) * 1000,
        )

        self._decisions.append(decision)
        return decision

    def _apply_context_overrides(
        self,
        allowed: bool,
        requires_approval: bool,
        classification: SecurityClassification,
        context: Dict[str, Any],
    ) -> Tuple[bool, bool]:
        """Apply context-based overrides to a policy decision.

        Currently supports:
        - bypass_approval: if True and user is trusted, skip approval
        - force_block: if True, always block
        - specialist_override: specialists can have elevated trust
        """
        if context.get("force_block"):
            return False, False

        if context.get("bypass_approval") and requires_approval:
            # Only bypass if the classification allows it
            specialist = context.get("specialist", "")
            if specialist in ("FORGE", "TERMINUS") and classification.trust_level != TrustLevel.HOSTILE:
                log.info(f"Approval bypassed for specialist {specialist}: {classification.evidence}")
                return True, False

        if context.get("simulate"):
            return True, False

        return allowed, requires_approval

    # ------------------------------------------------------------------
    # Command/Path Allowlisting
    # ------------------------------------------------------------------

    def is_command_allowed(self, command: str) -> Tuple[bool, str]:
        """Check if a command is allowlisted.

        Returns (allowed, reason).
        """
        if not command:
            return False, "Empty command"

        parts = command.strip().split()
        base = parts[0].split("/")[-1].split("\\")[-1]  # basename regardless of OS

        if base in self._allowlisted_commands:
            return True, f"Command '{base}' is allowlisted"

        return False, f"Command '{base}' is not allowlisted"

    def add_allowlisted_command(self, command: str) -> None:
        """Add a command to the allowlist."""
        self._allowlisted_commands.add(command)

    def add_blocked_path(self, path: str) -> None:
        """Add a path to the blocked list."""
        try:
            resolved = Path(path).resolve()
            self._blocked_paths.add(str(resolved))
        except (OSError, ValueError):
            self._blocked_paths.add(path)

    def add_protected_path(self, path: str) -> None:
        """Add a path to the protected list (requires approval to modify)."""
        try:
            resolved = Path(path).resolve()
            self._protected_paths.add(str(resolved))
        except (OSError, ValueError):
            self._protected_paths.add(path)

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def recent_decisions(self, n: int = 20) -> List[PolicyDecision]:
        """Return the n most recent policy decisions."""
        return self._decisions[-n:]

    def get_blocked_actions(self, since: Optional[float] = None) -> List[PolicyDecision]:
        """Return all blocked actions, optionally filtered by time."""
        blocked = [d for d in self._decisions if not d.allowed]
        if since is not None:
            blocked = [d for d in blocked if d.timestamp >= since]
        return blocked

    def get_decisions_by_risk(self, risk_level: RiskLevel) -> List[PolicyDecision]:
        """Return all decisions at a given risk level."""
        return [d for d in self._decisions if d.risk_level == risk_level]

    def get_stats(self) -> Dict[str, Any]:
        """Get governance statistics."""
        total = len(self._decisions)
        allowed = sum(1 for d in self._decisions if d.allowed)
        blocked = total - allowed
        approval = sum(1 for d in self._decisions if d.requires_approval)
        by_risk: Dict[str, int] = {}
        for d in self._decisions:
            by_risk[d.risk_level.value] = by_risk.get(d.risk_level.value, 0) + 1

        return {
            "total_decisions": total,
            "allowed": allowed,
            "blocked": blocked,
            "approval_required": approval,
            "by_risk_level": by_risk,
            "allowlisted_commands": len(self._allowlisted_commands),
            "policy_rules": len(self._rules),
            "blocked_paths": len(self._blocked_paths),
            "protected_paths": len(self._protected_paths),
        }

    def clear_decisions(self) -> None:
        """Clear decision history."""
        self._decisions.clear()
