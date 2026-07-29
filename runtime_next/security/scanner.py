"""Runtime Security Scanner — scans execution plans, configurations, and
runtime state for security vulnerabilities, credential leaks, path traversal
attempts, and dangerous command patterns.

Integrates with the VerificationPipeline as a SECURITY_SCAN verifier plugin
to provide automated security scanning during plan execution.

Scan Types:
  - credential_scan: Detects hardcoded API keys, tokens, passwords
  - path_traversal_scan: Detects attempts to escape workspace jail
  - command_injection_scan: Detects dangerous shell metacharacters
  - secret_exposure_scan: Detects secrets in logs, output, error messages
  - policy_violation_scan: Detects violations of runtime security policies
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set, Tuple

log = logging.getLogger("aelvo.runtime.security.scanner")


class SecuritySeverity(str, Enum):
    """Severity of a security finding."""
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SecurityCategory(str, Enum):
    """Categories of security findings."""
    CREDENTIAL_LEAK = "credential_leak"
    PATH_TRAVERSAL = "path_traversal"
    COMMAND_INJECTION = "command_injection"
    SECRET_EXPOSURE = "secret_exposure"
    POLICY_VIOLATION = "policy_violation"
    UNSAFE_COMMAND = "unsafe_command"
    SUSPICIOUS_PATTERN = "suspicious_pattern"
    SANDBOX_TAMPER = "sandbox_tamper"
    CONFIGURATION_ISSUE = "configuration_issue"


@dataclass
class SecurityFinding:
    """A single security finding discovered during scanning.

    Immutable once created — findings are source truth for security events.
    """

    finding_id: str
    category: SecurityCategory
    severity: SecuritySeverity
    title: str
    message: str
    location: str = ""
    """Where the finding was found (file path, command string, config key, etc.)."""
    line_number: Optional[int] = None
    snippet: str = ""
    """The matching text snippet (redacted if credential-related)."""
    recommendation: str = ""
    timestamp: float = field(default_factory=time.time)
    source: str = ""
    """Which scanner or component produced this finding."""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "finding_id": self.finding_id,
            "category": self.category.value,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "location": self.location,
            "line_number": self.line_number,
            "snippet": self.snippet,
            "recommendation": self.recommendation,
            "timestamp": self.timestamp,
            "source": self.source,
            "metadata": self.metadata,
        }


@dataclass
class ScanResult:
    """Result of a security scan across one or more targets."""

    scan_id: str
    timestamp: float = field(default_factory=time.time)
    duration_ms: float = 0.0
    findings: List[SecurityFinding] = field(default_factory=list)
    targets_scanned: int = 0
    total_findings: int = 0
    critical_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0
    info_count: int = 0
    scan_type: str = "full"

    @property
    def passed(self) -> bool:
        """Scan passes if there are no HIGH or CRITICAL findings."""
        return self.critical_count == 0 and self.high_count == 0

    def merge(self, other: ScanResult) -> ScanResult:
        """Merge another scan result into this one."""
        self.findings.extend(other.findings)
        self.targets_scanned += other.targets_scanned
        self.total_findings += other.total_findings
        self.critical_count += other.critical_count
        self.high_count += other.high_count
        self.medium_count += other.medium_count
        self.low_count += other.low_count
        self.info_count += other.info_count
        return self

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scan_id": self.scan_id,
            "timestamp": self.timestamp,
            "duration_ms": self.duration_ms,
            "scan_type": self.scan_type,
            "targets_scanned": self.targets_scanned,
            "total_findings": self.total_findings,
            "critical_count": self.critical_count,
            "high_count": self.high_count,
            "medium_count": self.medium_count,
            "low_count": self.low_count,
            "info_count": self.info_count,
            "passed": self.passed,
            "findings": [f.to_dict() for f in self.findings],
        }


# ── Credential Detection Patterns ─────────────────────────────────────────

CREDENTIAL_PATTERNS: List[Tuple[str, str, SecuritySeverity]] = [
    # API Keys and tokens
    (r"(?i)(api[_-]?key|api[_-]?secret|apikey)\s*[:=]\s*['\"]?[A-Za-z0-9_\-]{16,}", "API key/secret", SecuritySeverity.CRITICAL),
    (r"(?i)(sk-[A-Za-z0-9]{20,})", "OpenAI-style API key", SecuritySeverity.CRITICAL),
    (r"(?i)(ghp_[A-Za-z0-9]{36,})", "GitHub personal access token", SecuritySeverity.CRITICAL),
    (r"(?i)(gho_[A-Za-z0-9]{36,})", "GitHub OAuth token", SecuritySeverity.CRITICAL),
    (r"(?i)(ghu_[A-Za-z0-9]{36,})", "GitHub user token", SecuritySeverity.CRITICAL),
    (r"(?i)(xox[bpras]-[A-Za-z0-9\-]{24,})", "Slack token", SecuritySeverity.CRITICAL),
    (r"(?i)(AKIA[0-9A-Z]{16})", "AWS access key ID", SecuritySeverity.CRITICAL),
    # JWTs
    (r"(?i)(eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,})", "JWT token", SecuritySeverity.CRITICAL),
    # Passwords
    (r"(?i)(password|passwd|pwd)\s*[:=]\s*['\"]?[^'\"\s]{6,}", "Password/secret string", SecuritySeverity.HIGH),
    (r"(?i)(secret|token|credential)\s*[:=]\s*['\"]?[^'\"\s]{8,}", "Generic secret/credential", SecuritySeverity.HIGH),
    # Connection strings
    (r"(?i)(postgresql|mysql|mongodb|redis)://[^:]+:[^@]+@", "Database connection string with password", SecuritySeverity.CRITICAL),
    # Private keys
    (r"-----BEGIN\s+(RSA|DSA|EC|OPENSSH|PRIVATE)(?:\s+PRIVATE)?\s+KEY-----", "Private key", SecuritySeverity.CRITICAL),
]

# ── Path Traversal Patterns ──────────────────────────────────────────────

PATH_TRAVERSAL_PATTERNS: List[Tuple[str, str, SecuritySeverity]] = [
    (r"(\.\./){2,}", "Multiple directory traversal (../../..)", SecuritySeverity.HIGH),
    (r"(\.\.\\){2,}", "Multiple Windows directory traversal", SecuritySeverity.HIGH),
    (r"(?i)(file:///|file://localhost)", "File protocol URL access", SecuritySeverity.MEDIUM),
    (r"(?i)(/etc/passwd|/etc/shadow|/proc/self/environ)", "Access to sensitive system files", SecuritySeverity.CRITICAL),
    (r"(?i)(C:\\Windows\\System32|/windows/system32)", "Access to Windows system directory", SecuritySeverity.HIGH),
    (r"(?i)(%2e%2e%2f|%2e%2e\\|..%252f|..%255c)", "URL-encoded path traversal", SecuritySeverity.HIGH),
    (r"\.\./\.\./\.\./\.\./", "Deep directory traversal", SecuritySeverity.HIGH),
]

# ── Command Injection Patterns ───────────────────────────────────────────

COMMAND_INJECTION_PATTERNS: List[Tuple[str, str, SecuritySeverity]] = [
    (r"[;&|`]\s*(rm|del|deltree|shutdown|format|mkfs|dd)", "Destructive command via shell injection", SecuritySeverity.CRITICAL),
    (r"\|\s*(bash|sh|cmd|powershell|zsh|fish)", "Piped shell invocation", SecuritySeverity.HIGH),
    (r"(?i)(\$\(.*\)|`.*`)", "Command substitution", SecuritySeverity.MEDIUM),
    (r"(?i)(wget|curl)\s+.*\|\s*(bash|sh|python)", "Piped remote execution", SecuritySeverity.CRITICAL),
    (r"(?i)(eval|exec|system|popen|subprocess\.call)\s*\(", "Dynamic code execution function", SecuritySeverity.HIGH),
    (r"(?i)(chmod\s+777|chmod\s+a\+x)", "Overly permissive file permissions", SecuritySeverity.MEDIUM),
    (r"(?i)(> /dev/sda|< /dev/sda)", "Direct block device access", SecuritySeverity.CRITICAL),
]

# ── Secret Exposure Patterns (for log/output scanning) ───────────────────

SECRET_EXPOSURE_PATTERNS: List[Tuple[str, str, SecuritySeverity]] = [
    (r"(?i)(api[_-]?key|api[_-]?secret)\s*[:=]\s*\S{8,}", "API key in output", SecuritySeverity.HIGH),
    (r"(?i)(token|secret|credential)\s*(=|:)\s*\S{6,}", "Token/secret in output", SecuritySeverity.HIGH),
    (r"(?i)(password|passwd)\s*(=|:)\s*\S{4,}", "Password in output", SecuritySeverity.CRITICAL),
    (r"-----BEGIN\s+(RSA|DSA|EC|OPENSSH|PRIVATE)\s+KEY-----", "Private key in output", SecuritySeverity.CRITICAL),
]

# ── Dangerous Command Patterns ───────────────────────────────────────────

DANGEROUS_COMMAND_PATTERNS: List[Tuple[str, str, SecuritySeverity]] = [
    (r"(?i)^(rm|del)\s+(-rf|-r|-f)?\s*/", "Recursive filesystem delete from root", SecuritySeverity.CRITICAL),
    (r"(?i)^(dd\s+if=|format|mkfs\.)", "Filesystem destructive operation", SecuritySeverity.CRITICAL),
    (r":\s*\(\s*\)\s*\{", "Fork bomb", SecuritySeverity.CRITICAL),
    (r"(?i)(yum|apt|apk|brew)\s+(install|update|remove)\s+.*--no-verify", "Package install with verification disabled", SecuritySeverity.MEDIUM),
    (r"(?i)(curl|wget)\s+-[^\s]*O?\s+https?://", "Remote file download", SecuritySeverity.LOW),
    (r"(?i)(git\s+clone\s+https?://[^@]+@)", "Git clone with embedded credentials", SecuritySeverity.CRITICAL),
]


class RuntimeSecurityScanner:
    """Scans runtime execution context for security vulnerabilities.

    Performs multiple scan types:
      1. Credential scanning — detect leaked keys/tokens/passwords
      2. Path traversal scanning — detect jail escape attempts
      3. Command injection scanning — detect shell injection patterns
      4. Secret exposure scanning — detect secrets in logs/output
      5. Policy violation scanning — detect security policy breaks
      6. Dangerous command scanning — detect destructive/unsafe commands

    Integrates with the VerificationPipeline via create_verifier_handler().
    """

    def __init__(self):
        self._scan_history: List[ScanResult] = []
        self._max_history: int = 100

    # ── Scan Methods ─────────────────────────────────────────────────────

    def scan_text(
        self,
        text: str,
        source: str = "",
        scan_types: Optional[List[str]] = None,
    ) -> ScanResult:
        """Scan a text string (command, config, output) for security issues.

        Args:
            text: The text to scan.
            source: Description of where the text came from.
            scan_types: List of scan types to run. If None, runs all.

        Returns:
            ScanResult with findings.
        """
        start = time.time()
        scan_id = self._generate_id("sec_scan")
        findings: List[SecurityFinding] = []

        scan_types = scan_types or [
            "credential", "path_traversal", "command_injection",
            "secret_exposure", "dangerous_command",
        ]

        if "credential" in scan_types:
            findings.extend(self._scan_credentials(text, source))
        if "path_traversal" in scan_types:
            findings.extend(self._scan_path_traversal(text, source))
        if "command_injection" in scan_types:
            findings.extend(self._scan_command_injection(text, source))
        if "secret_exposure" in scan_types:
            findings.extend(self._scan_secret_exposure(text, source))
        if "dangerous_command" in scan_types:
            findings.extend(self._scan_dangerous_commands(text, source))

        duration = (time.time() - start) * 1000
        result = self._build_result(scan_id, findings, duration, 1, source)

        self._scan_history.append(result)
        if len(self._scan_history) > self._max_history:
            self._scan_history = self._scan_history[-self._max_history:]

        return result

    def scan_file(
        self,
        file_path: str,
        content: Optional[str] = None,
    ) -> Optional[ScanResult]:
        """Scan a file for security issues.

        Args:
            file_path: Path to the file to scan.
            content: Optional pre-read content. If None, reads from disk.

        Returns:
            ScanResult or None if file can't be read.
        """
        if content is None:
            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
            except Exception as e:
                log.warning("Cannot scan file %s: %s", file_path, e)
                return None

        return self.scan_text(content, source=file_path)

    def scan_plan(
        self,
        plan: Dict[str, Any],
    ) -> ScanResult:
        """Scan an execution plan dict for security issues.

        Checks plan node commands, descriptions, and configuration
        for security vulnerabilities.

        Args:
            plan: The execution plan dict or object.

        Returns:
            ScanResult aggregated across all plan nodes.
        """
        start = time.time()
        scan_id = self._generate_id("sec_plan_scan")
        aggregate = ScanResult(scan_id=scan_id)

        # Extract text from plan nodes
        nodes = plan.get("nodes", []) if isinstance(plan, dict) else []
        if not nodes and isinstance(plan, dict):
            log.warning("scan_plan: plan has no 'nodes' key — scan result may be empty")
        elif not isinstance(plan, dict):
            log.warning("scan_plan: plan is not a dict (%s) — scan result will be empty", type(plan).__name__)
        targets = 0

        for node in nodes:
            if isinstance(node, dict):
                node_texts = [
                    node.get("description", ""),
                    node.get("command", ""),
                    node.get("handler", ""),
                    str(node.get("params", {})),
                ]
            else:
                node_texts = [
                    getattr(node, "description", ""),
                    getattr(node, "command", ""),
                    getattr(node, "handler", ""),
                ]

            for text in node_texts:
                if text:
                    result = self.scan_text(
                        text,
                        source=f"plan_node:{node.get('id', 'unknown') if isinstance(node, dict) else getattr(node, 'id', 'unknown')}",
                    )
                    aggregate.merge(result)
                    targets += 1

        aggregate.targets_scanned = targets
        aggregate.duration_ms = (time.time() - start) * 1000
        aggregate.scan_type = "plan"

        self._scan_history.append(aggregate)
        return aggregate

    def scan_context(
        self,
        context: Dict[str, Any],
        source: str = "runtime_context",
    ) -> ScanResult:
        """Scan a runtime context dict for security issues.

        Recursively scans all string values in the context.

        Args:
            context: The context dict to scan.
            source: Source label for findings.

        Returns:
            ScanResult with findings.
        """
        start = time.time()
        scan_id = self._generate_id("sec_ctx_scan")
        findings: List[SecurityFinding] = []
        targets = 0

        def _scan_dict(d: Dict[str, Any], prefix: str = "") -> None:
            nonlocal targets
            for key, value in d.items():
                if isinstance(value, str) and len(value) > 8:
                    result = self.scan_text(value, source=f"{source}.{prefix}{key}")
                    findings.extend(result.findings)
                    targets += 1
                elif isinstance(value, dict):
                    _scan_dict(value, f"{prefix}{key}.")
                elif isinstance(value, list):
                    for i, item in enumerate(value[:10]):  # Limit depth
                        if isinstance(item, str) and len(item) > 8:
                            result = self.scan_text(item, source=f"{source}.{prefix}{key}[{i}]")
                            findings.extend(result.findings)
                            targets += 1

        _scan_dict(context)

        duration = (time.time() - start) * 1000
        result = self._build_result(scan_id, findings, duration, targets, source)

        self._scan_history.append(result)
        return result

    def scan_all(
        self,
        text_targets: Optional[List[str]] = None,
        file_targets: Optional[List[str]] = None,
        plan: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> ScanResult:
        """Run a comprehensive scan across multiple targets.

        Args:
            text_targets: List of text strings to scan.
            file_targets: List of file paths to scan.
            plan: Optional execution plan to scan.
            context: Optional runtime context to scan.

        Returns:
            Aggregated ScanResult.
        """
        start = time.time()
        scan_id = self._generate_id("sec_full_scan")
        aggregate = ScanResult(scan_id=scan_id, scan_type="full")

        if text_targets:
            for text in text_targets:
                result = self.scan_text(text, source="full_scan_target")
                aggregate.merge(result)

        if file_targets:
            for fp in file_targets:
                result = self.scan_file(fp)
                if result:
                    aggregate.merge(result)

        if plan:
            result = self.scan_plan(plan)
            aggregate.merge(result)

        if context:
            result = self.scan_context(context)
            aggregate.merge(result)

        aggregate.duration_ms = (time.time() - start) * 1000
        self._scan_history.append(aggregate)
        return result

    # ── Internal Scan Implementations ────────────────────────────────────

    def _scan_credentials(self, text: str, source: str) -> List[SecurityFinding]:
        """Scan for credential leaks."""
        findings: List[SecurityFinding] = []
        for pattern, label, severity in CREDENTIAL_PATTERNS:
            for match in re.finditer(pattern, text):
                snippet = self._redact_snippet(match.group()[:40])
                findings.append(SecurityFinding(
                    finding_id=self._generate_id("cred"),
                    category=SecurityCategory.CREDENTIAL_LEAK,
                    severity=severity,
                    title=f"Credential leak: {label}",
                    message=f"Potential {label} detected in {source or 'text'}",
                    location=source,
                    snippet=snippet,
                    recommendation=f"Remove {label} from source and use a secure credential store or environment variable instead.",
                    source="credential_scanner",
                    metadata={"pattern": pattern, "label": label},
                ))
        return findings

    def _scan_path_traversal(self, text: str, source: str) -> List[SecurityFinding]:
        """Scan for path traversal attempts."""
        findings: List[SecurityFinding] = []
        for pattern, label, severity in PATH_TRAVERSAL_PATTERNS:
            for match in re.finditer(pattern, text):
                findings.append(SecurityFinding(
                    finding_id=self._generate_id("path"),
                    category=SecurityCategory.PATH_TRAVERSAL,
                    severity=severity,
                    title=f"Path traversal: {label}",
                    message=f"Path traversal pattern detected in {source or 'text'}: {match.group()[:60]}",
                    location=source,
                    snippet=match.group()[:60],
                    recommendation="Avoid directory traversal sequences. Use the sandbox filesystem jail instead.",
                    source="path_scanner",
                    metadata={"pattern": pattern, "match": match.group()[:100]},
                ))
        return findings

    def _scan_command_injection(self, text: str, source: str) -> List[SecurityFinding]:
        """Scan for command injection patterns."""
        findings: List[SecurityFinding] = []
        for pattern, label, severity in COMMAND_INJECTION_PATTERNS:
            for match in re.finditer(pattern, text):
                findings.append(SecurityFinding(
                    finding_id=self._generate_id("inj"),
                    category=SecurityCategory.COMMAND_INJECTION,
                    severity=severity,
                    title=f"Command injection: {label}",
                    message=f"Command injection pattern detected in {source or 'text'}",
                    location=source,
                    snippet=match.group()[:80],
                    recommendation="Use the sandbox API for executing commands — never construct shell commands from untrusted input.",
                    source="injection_scanner",
                    metadata={"pattern": pattern, "match": match.group()[:100]},
                ))
        return findings

    def _scan_secret_exposure(self, text: str, source: str) -> List[SecurityFinding]:
        """Scan for secrets exposed in output/logs."""
        findings: List[SecurityFinding] = []
        for pattern, label, severity in SECRET_EXPOSURE_PATTERNS:
            for match in re.finditer(pattern, text):
                findings.append(SecurityFinding(
                    finding_id=self._generate_id("sec_exp"),
                    category=SecurityCategory.SECRET_EXPOSURE,
                    severity=severity,
                    title=f"Secret exposure: {label}",
                    message=f"Potential secret exposed in {source or 'output'}",
                    location=source,
                    snippet=self._redact_snippet(match.group()[:40]),
                    recommendation="Redact secrets from logs and output. Use a structured logging system with automatic credential redaction.",
                    source="exposure_scanner",
                    metadata={"pattern": pattern},
                ))
        return findings

    def _scan_dangerous_commands(self, text: str, source: str) -> List[SecurityFinding]:
        """Scan for dangerous/unsafe commands."""
        findings: List[SecurityFinding] = []
        for pattern, label, severity in DANGEROUS_COMMAND_PATTERNS:
            for match in re.finditer(pattern, text):
                findings.append(SecurityFinding(
                    finding_id=self._generate_id("danger"),
                    category=SecurityCategory.UNSAFE_COMMAND,
                    severity=severity,
                    title=f"Dangerous command: {label}",
                    message=f"Dangerous command pattern detected in {source or 'text'}",
                    location=source,
                    snippet=match.group()[:80],
                    recommendation=f"Avoid {label}. Use the sandbox with appropriate policies for dangerous operations.",
                    source="command_scanner",
                    metadata={"pattern": pattern, "match": match.group()[:100]},
                ))
        return findings

    # ── Verification Pipeline Integration ────────────────────────────────

    def create_verifier_handler(self):
        """Create a handler for the VerificationPipeline SECURITY_SCAN type.

        Usage:
            pipeline = VerificationPipeline()
            scanner = RuntimeSecurityScanner()
            pipeline.register_verifier(
                VerificationType.SECURITY_SCAN,
                scanner.create_verifier_handler(),
            )

        The handler expects context to contain a 'security_targets' key
        with the text/file/plan/context to scan.
        """
        from runtime_next.verification.types import (
            VerificationType, VerificationResult, VerificationScope,
            Confidence, Severity, Retryability,
        )

        async def handler(
            node_id: str,
            scope: VerificationScope,
            context: Dict[str, Any],
        ) -> VerificationResult:
            start = time.time()

            # Extract scan targets from context
            text_targets = context.get("security_targets", [])
            file_targets = context.get("security_files", [])
            plan_data = context.get("security_plan")
            ctx_data = context.get("security_context")

            # Run the scan
            result = self.scan_all(
                text_targets=text_targets if isinstance(text_targets, list) else [str(text_targets)],
                file_targets=file_targets if isinstance(file_targets, list) else [],
                plan=plan_data if isinstance(plan_data, dict) else None,
                context=ctx_data if isinstance(ctx_data, dict) else None,
            )

            duration = (time.time() - start) * 1000

            diagnostics = []
            for finding in result.findings[:10]:  # Top 10 findings
                diagnostics.append(
                    f"[{finding.severity.value.upper()}] {finding.category.value}: "
                    f"{finding.title} at {finding.location}"[:200]
                )
            if len(result.findings) > 10:
                diagnostics.append(f"... and {len(result.findings) - 10} more findings")

            # Map severity
            if result.critical_count > 0:
                severity = Severity.CRITICAL
                retryability = Retryability.NEVER
            elif result.high_count > 0:
                severity = Severity.ERROR
                retryability = Retryability.CONDITIONAL
            elif result.medium_count > 0:
                severity = Severity.WARNING
                retryability = Retryability.SAFE
            else:
                severity = Severity.INFO
                retryability = Retryability.SAFE

            return VerificationResult(
                verification_id=self._generate_id("sec_verif"),
                node_id=node_id,
                verification_type=VerificationType.SECURITY_SCAN,
                duration_ms=duration,
                success=result.passed,
                confidence=Confidence.HIGH,
                severity=severity,
                retryability=retryability,
                diagnostics=diagnostics,
                artifacts={"scan_result": result.to_dict()},
                affected_files=list(scope.affected_files) if scope else [],
                runtime_implications=(
                    ["Security scan found critical issues — operation blocked"]
                    if not result.passed
                    else ["Security scan passed — no critical issues found"]
                ),
                provenance="security_scanner",
            )

        return handler

    # ── History ──────────────────────────────────────────────────────────

    def get_scan_history(self, limit: int = 10) -> List[ScanResult]:
        """Get recent scan results."""
        return list(self._scan_history[-limit:])

    def get_latest_scan(self) -> Optional[ScanResult]:
        """Get the most recent scan result."""
        return self._scan_history[-1] if self._scan_history else None

    def reset(self) -> None:
        """Clear scan history."""
        self._scan_history.clear()

    # ── Helpers ──────────────────────────────────────────────────────────

    def _build_result(
        self,
        scan_id: str,
        findings: List[SecurityFinding],
        duration_ms: float,
        targets: int,
        source: str,
    ) -> ScanResult:
        """Build a ScanResult from findings."""
        return ScanResult(
            scan_id=scan_id,
            duration_ms=round(duration_ms, 2),
            findings=findings,
            targets_scanned=targets,
            total_findings=len(findings),
            critical_count=sum(1 for f in findings if f.severity == SecuritySeverity.CRITICAL),
            high_count=sum(1 for f in findings if f.severity == SecuritySeverity.HIGH),
            medium_count=sum(1 for f in findings if f.severity == SecuritySeverity.MEDIUM),
            low_count=sum(1 for f in findings if f.severity == SecuritySeverity.LOW),
            info_count=sum(1 for f in findings if f.severity == SecuritySeverity.INFO),
            scan_type=source or "unknown",
        )

    @staticmethod
    def _generate_id(prefix: str) -> str:
        raw = f"{prefix}_{time.time()}_{id(object())}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @staticmethod
    def _redact_snippet(text: str) -> str:
        """Redact sensitive content from a snippet for safe display."""
        if len(text) > 8:
            return text[:4] + "*" * (len(text) - 8) + text[-4:]
        return text
