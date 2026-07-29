"""Runtime Security Orchestrator — coordinates runtime security scanning,
policy audit trails, and sandbox integrity verification into a unified
security posture management system.

Features:
- Scheduled and on-demand security scanning
- Policy audit trail management with hash-chain integrity
- Sandbox integrity verification with tamper detection
- Security event alerting (integrates with AlertManager)
- Health check registration (integrates with RuntimeHealthMonitor)
- Auto-remediation for common security issues
- Security posture reporting
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .scanner import (
    RuntimeSecurityScanner,
    SecurityFinding,
    SecuritySeverity,
    SecurityCategory,
    ScanResult,
)
from .policy_audit import (
    PolicyAuditTrail,
    AuditRecord,
    AuditAction,
    AuditDecision,
    AuditQuery,
)
from .sandbox_integrity import (
    SandboxIntegrityVerifier,
    IntegrityCheckResult,
    BinaryVerificationStatus,
    AuditLogIntegrityStatus,
)

log = logging.getLogger("aelvo.runtime.security.orchestrator")


@dataclass
class SecurityScanSchedule:
    """Configuration for periodic security scans."""

    enabled: bool = True
    interval_seconds: float = 300.0
    scan_types: List[str] = field(default_factory=lambda: [
        "credential", "path_traversal", "command_injection",
        "secret_exposure", "dangerous_command",
    ])
    auto_remediate: bool = False
    alert_on_findings: bool = True
    alert_threshold: str = "medium"
    """Minimum severity to trigger alerts."""


@dataclass
class SecurityPosture:
    """Overall runtime security posture summary."""

    overall_status: str = "unknown"
    """healthy, attention_needed, critical, unknown"""

    total_scans: int = 0
    total_findings: int = 0
    critical_findings: int = 0
    high_findings: int = 0
    medium_findings: int = 0
    low_findings: int = 0

    audit_records_count: int = 0
    audit_chain_valid: bool = True

    integrity_checks_passed: int = 0
    integrity_checks_failed: int = 0

    last_scan_time: float = 0.0
    last_integrity_check_time: float = 0.0

    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "overall_status": self.overall_status,
            "total_scans": self.total_scans,
            "total_findings": self.total_findings,
            "critical_findings": self.critical_findings,
            "high_findings": self.high_findings,
            "medium_findings": self.medium_findings,
            "low_findings": self.low_findings,
            "audit_records_count": self.audit_records_count,
            "audit_chain_valid": self.audit_chain_valid,
            "integrity_checks_passed": self.integrity_checks_passed,
            "integrity_checks_failed": self.integrity_checks_failed,
            "last_scan_time": self.last_scan_time,
            "last_integrity_check_time": self.last_integrity_check_time,
            "recommendations": self.recommendations,
        }


class RuntimeSecurityOrchestrator:
    """Coordinates all runtime security activities.

    Usage:
        orchestrator = RuntimeSecurityOrchestrator()
        orchestrator.link_scanner(scanner)
        orchestrator.link_audit_trail(audit)
        orchestrator.link_integrity_verifier(verifier)

        # Run a security scan
        result = await orchestrator.run_security_scan()

        # Check overall posture
        posture = orchestrator.get_posture()
    """

    def __init__(self):
        self._scanner: Optional[RuntimeSecurityScanner] = None
        self._audit_trail: Optional[PolicyAuditTrail] = None
        self._integrity_verifier: Optional[SandboxIntegrityVerifier] = None
        self._alert_callback: Optional[Callable[[str, str, str], None]] = None
        """Callback for alerting: fn(title, message, severity)"""

        self._scan_schedule = SecurityScanSchedule()
        self._scan_count: int = 0
        self._integrity_check_count: int = 0
        self._last_scan_result: Optional[ScanResult] = None
        self._last_integrity_results: Dict[str, IntegrityCheckResult] = {}

    # ── Linking Components ───────────────────────────────────────────────

    def link_scanner(self, scanner: RuntimeSecurityScanner) -> None:
        """Link a RuntimeSecurityScanner instance."""
        self._scanner = scanner
        log.info("Security orchestrator linked to scanner")

    def link_audit_trail(self, audit_trail: PolicyAuditTrail) -> None:
        """Link a PolicyAuditTrail instance."""
        self._audit_trail = audit_trail
        log.info("Security orchestrator linked to audit trail")

    def link_integrity_verifier(self, verifier: SandboxIntegrityVerifier) -> None:
        """Link a SandboxIntegrityVerifier instance."""
        self._integrity_verifier = verifier
        log.info("Security orchestrator linked to integrity verifier")

    def set_alert_callback(
        self, callback: Callable[[str, str, str], None],
    ) -> None:
        """Set callback for alerting.

        The callback receives (title, message, severity).
        """
        self._alert_callback = callback

    def set_scan_schedule(self, schedule: SecurityScanSchedule) -> None:
        """Configure the security scan schedule."""
        self._scan_schedule = schedule

    # ── Security Scanning ────────────────────────────────────────────────

    def run_security_scan(
        self,
        text_targets: Optional[List[str]] = None,
        file_targets: Optional[List[str]] = None,
        plan: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[ScanResult]:
        """Run a comprehensive security scan.

        Args:
            text_targets: Text strings to scan.
            file_targets: File paths to scan.
            plan: Execution plan to scan.
            context: Runtime context to scan.

        Returns:
            ScanResult, or None if no scanner is linked.
        """
        if not self._scanner:
            log.warning("Cannot run security scan: no scanner linked")
            return None

        self._scan_count += 1
        start = time.time()
        log.info("Running security scan #%d...", self._scan_count)

        result = self._scanner.scan_all(
            text_targets=text_targets,
            file_targets=file_targets,
            plan=plan,
            context=context,
        )

        # Record the scan in the audit trail
        if self._audit_trail:
            self._audit_trail.record(
                action=AuditAction.SECURITY_SCAN,
                decision=AuditDecision.ALLOWED if result.passed else AuditDecision.BLOCKED,
                actor="security_orchestrator",
                subsystem="security",
                resource=f"scan:{result.scan_id}",
                reason=f"Security scan {'passed' if result.passed else 'failed'}: "
                       f"{result.critical_count} critical, {result.high_count} high",
                message=f"Scan #{self._scan_count}: {result.total_findings} findings in {result.duration_ms:.0f}ms",
                severity="critical" if result.critical_count > 0
                else "warning" if result.high_count > 0
                else "info",
                metadata={
                    "scan_id": result.scan_id,
                    "duration_ms": result.duration_ms,
                    "total_findings": result.total_findings,
                    "critical_count": result.critical_count,
                    "high_count": result.high_count,
                },
            )

            # Record individual findings
            for finding in result.findings:
                self._audit_trail.record_security_finding(
                    finding_id=finding.finding_id,
                    severity=finding.severity.value,
                    category=finding.category.value,
                    message=finding.message,
                    metadata={"location": finding.location, "title": finding.title},
                )

        # Alert on high-severity findings
        self._alert_on_findings(result)

        # Auto-remediate simple issues
        if self._scan_schedule.auto_remediate and not result.passed:
            self._auto_remediate(result)

        duration = (time.time() - start) * 1000
        log.info(
            "Security scan #%d complete: %d findings in %.0fms (%d critical, %d high)",
            self._scan_count, result.total_findings, duration,
            result.critical_count, result.high_count,
        )

        self._last_scan_result = result
        return result

    # ── Integrity Checks ─────────────────────────────────────────────────

    def run_integrity_checks(
        self,
        workspace_path: Optional[str] = None,
    ) -> Dict[str, IntegrityCheckResult]:
        """Run all sandbox integrity checks.

        Args:
            workspace_path: Optional workspace path for filesystem check.

        Returns:
            Dict mapping check names to IntegrityCheckResult.
        """
        if not self._integrity_verifier:
            log.warning("Cannot run integrity checks: no verifier linked")
            return {}

        self._integrity_check_count += 1
        start = time.time()
        log.info("Running integrity checks #%d...", self._integrity_check_count)

        results = self._integrity_verifier.run_all_checks(
            workspace_path=workspace_path,
        )

        # Record in audit trail
        if self._audit_trail:
            for name, result in results.items():
                self._audit_trail.record_integrity_check(
                    check_id=result.check_id,
                    passed=result.passed,
                    details=result.message,
                    metadata={"check_name": name},
                )

        # Alert on failures
        for name, result in results.items():
            if not result.passed:
                self._alert(
                    f"Integrity check failed: {name}",
                    result.message,
                    "critical" if result.status in ("mismatch", "tampered") else "warning",
                )

        duration = (time.time() - start) * 1000
        passed = sum(1 for r in results.values() if r.passed)
        failed = sum(1 for r in results.values() if not r.passed)
        log.info(
            "Integrity checks #%d complete: %d passed, %d failed in %.0fms",
            self._integrity_check_count, passed, failed, duration,
        )

        self._last_integrity_results = results
        return results

    # ── Audit Trail Management ──────────────────────────────────────────

    def get_audit_trail(self) -> Optional[PolicyAuditTrail]:
        """Get the linked policy audit trail."""
        return self._audit_trail

    def query_audit(self, query: AuditQuery) -> List[AuditRecord]:
        """Query audit records."""
        if not self._audit_trail:
            return []
        return self._audit_trail.query(query)

    def verify_audit_chain(self) -> bool:
        """Verify the integrity of the entire audit chain."""
        if not self._audit_trail:
            return True
        return self._audit_trail.verify_chain_integrity()

    # ── Posture Reporting ────────────────────────────────────────────────

    def get_posture(self) -> SecurityPosture:
        """Get the overall runtime security posture.

        Aggregates data from scanner, audit trail, and integrity verifier
        to produce an overall security posture assessment.
        """
        posture = SecurityPosture()

        # From scanner
        scan = self._last_scan_result
        if scan:
            posture.total_scans = self._scan_count
            posture.total_findings = scan.total_findings
            posture.critical_findings = scan.critical_count
            posture.high_findings = scan.high_count
            posture.medium_findings = scan.medium_count
            posture.low_findings = scan.low_count
            posture.last_scan_time = scan.timestamp

        # From audit trail
        if self._audit_trail:
            stats = self._audit_trail.get_stats()
            posture.audit_records_count = stats.get("total_records", 0)
            posture.audit_chain_valid = stats.get("chain_valid", True)

        # From integrity verifier
        if self._integrity_verifier:
            summary = self._integrity_verifier.get_summary()
            posture.integrity_checks_passed = summary.get("passed_checks", 0)
            posture.integrity_checks_failed = summary.get("failed_checks", 0)

        # Determine overall status
        if posture.critical_findings > 0 or posture.integrity_checks_failed > 0:
            posture.overall_status = "critical"
        elif posture.high_findings > 0:
            posture.overall_status = "attention_needed"
        elif posture.total_scans > 0:
            posture.overall_status = "healthy"
        else:
            posture.overall_status = "unknown"

        # Generate recommendations
        if posture.critical_findings > 0:
            posture.recommendations.append(
                f"Address {posture.critical_findings} critical security finding(s) immediately"
            )
        if posture.high_findings > 0:
            posture.recommendations.append(
                f"Review {posture.high_findings} high-severity security finding(s)"
            )
        if not posture.audit_chain_valid:
            posture.recommendations.append(
                "Audit trail hash chain integrity violation detected — investigate immediately"
            )
        if posture.integrity_checks_failed > 0:
            posture.recommendations.append(
                f"Investigate {posture.integrity_checks_failed} failed sandbox integrity check(s)"
            )

        return posture

    # ── Health Check Integration ─────────────────────────────────────────

    def create_health_check_fns(self) -> Dict[str, Any]:
        """Create health check functions for RuntimeHealthMonitor.

        Returns dict mapping check names to callables returning
        HealthCheckResult.
        """
        from runtime_next.monitoring.health import HealthCheckResult

        def _scan_check() -> HealthCheckResult:
            posture = self.get_posture()
            if posture.critical_findings > 0:
                return HealthCheckResult(
                    healthy=False,
                    message=f"Security scan found {posture.critical_findings} critical issues",
                )
            return HealthCheckResult(
                healthy=True,
                message=f"Security posture: {posture.overall_status}",
            )

        def _audit_check() -> HealthCheckResult:
            if self._audit_trail:
                chain_valid = self._audit_trail.verify_chain_integrity()
                return HealthCheckResult(
                    healthy=chain_valid,
                    message="Audit trail chain intact" if chain_valid else "Audit trail chain integrity VIOLATION",
                )
            return HealthCheckResult(healthy=True, message="No audit trail linked")

        def _integrity_check() -> HealthCheckResult:
            if not self._integrity_verifier:
                return HealthCheckResult(healthy=True, message="No verifier linked")
            all_passed = self._integrity_verifier.all_passed()
            return HealthCheckResult(
                healthy=all_passed,
                message="All integrity checks passed" if all_passed
                else "Some integrity checks failed",
            )

        return {
            "security_scan": _scan_check,
            "audit_chain": _audit_check,
            "integrity_status": _integrity_check,
        }

    # ── Alerting ─────────────────────────────────────────────────────────

    def _alert_on_findings(self, result: ScanResult) -> None:
        """Alert on findings that meet the threshold."""
        if not self._scan_schedule.alert_on_findings:
            return

        threshold_map = {
            "info": SecuritySeverity.INFO,
            "low": SecuritySeverity.LOW,
            "medium": SecuritySeverity.MEDIUM,
            "high": SecuritySeverity.HIGH,
            "critical": SecuritySeverity.CRITICAL,
        }
        threshold = threshold_map.get(self._scan_schedule.alert_threshold, SecuritySeverity.MEDIUM)

        # Map SecuritySeverity to comparable int
        severity_order = {
            SecuritySeverity.INFO: 0,
            SecuritySeverity.LOW: 1,
            SecuritySeverity.MEDIUM: 2,
            SecuritySeverity.HIGH: 3,
            SecuritySeverity.CRITICAL: 4,
        }
        min_severity = severity_order.get(threshold, 2)

        for finding in result.findings:
            if severity_order.get(finding.severity, 0) >= min_severity:
                self._alert(
                    f"Security: {finding.title}",
                    f"[{finding.severity.value.upper()}] {finding.message[:120]}",
                    finding.severity.value,
                )

    def _alert(self, title: str, message: str, severity: str) -> None:
        """Dispatch an alert through the callback or log."""
        if self._alert_callback:
            try:
                self._alert_callback(title, message, severity)
            except Exception as e:
                log.warning("Alert callback failed: %s", e)
        else:
            log.log(
                logging.CRITICAL if severity == "critical"
                else logging.ERROR if severity == "high"
                else logging.WARNING,
                "SECURITY ALERT [%s] %s: %s",
                severity.upper(), title, message[:200],
            )

    # ── Auto-Remediation ────────────────────────────────────────────────

    def _auto_remediate(self, result: ScanResult) -> int:
        """Attempt auto-remediation for common security issues.

        Currently handles:
        - Credential leaks in text: recommend using env vars (logging only)

        Returns:
            Number of finding types auto-remediated.
        """
        remediated = 0
        for finding in result.findings:
            if finding.category == SecurityCategory.CREDENTIAL_LEAK:
                log.warning(
                    "Auto-remediation recommended for credential leak at %s: use environment variables instead",
                    finding.location,
                )
                remediated += 1
            elif finding.category == SecurityCategory.UNSAFE_COMMAND:
                log.warning(
                    "Auto-remediation recommended for unsafe command at %s: use sandbox API",
                    finding.location,
                )
                remediated += 1

        return remediated

    # ── Reset ────────────────────────────────────────────────────────────

    def reset(self) -> None:
        """Reset all security state."""
        self._scan_count = 0
        self._integrity_check_count = 0
        self._last_scan_result = None
        self._last_integrity_results.clear()
        if self._scanner:
            self._scanner.reset()
        if self._audit_trail:
            self._audit_trail.reset()
        log.info("Security orchestrator reset")
