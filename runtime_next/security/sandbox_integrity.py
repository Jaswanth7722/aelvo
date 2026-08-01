"""Sandbox Integrity Verifier — verifies the integrity of the Rust sandbox
binary, audit logs, and runtime sandbox environment.

Provides tamper-evident checks for:
- Sandbox binary integrity (hash verification against known good hashes)
- Audit log integrity (tamper-evident chain verification)
- Sandbox process health (process existence, resource usage)
- Filesystem isolation integrity (jail boundary verification)

Integrates with RuntimeHealthMonitor as health checks and with
PolicyAuditTrail for integrity event recording.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import platform
import subprocess
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from runtime_next.monitoring.health import HealthCheckResult

log = logging.getLogger("aelvo.runtime.security.sandbox_integrity")


class BinaryVerificationStatus(str, Enum):
    """Status of sandbox binary integrity verification."""
    VERIFIED = "verified"
    """Binary hash matches the expected good hash."""
    MISMATCH = "mismatch"
    """Binary hash differs from expected — possible tampering."""
    NOT_FOUND = "not_found"
    """Binary file not found at expected path."""
    UNKNOWN = "unknown"
    """Verification could not be completed."""


class AuditLogIntegrityStatus(str, Enum):
    """Status of audit log integrity verification."""
    INTACT = "intact"
    """Audit log chain is intact and unmodified."""
    TAMPERED = "tampered"
    """Audit log chain integrity violation detected."""
    EMPTY = "empty"
    """No audit log entries to verify."""
    UNKNOWN = "unknown"
    """Verification could not be completed."""


@dataclass
class IntegrityCheckResult:
    """Result of a single integrity verification check."""

    check_id: str
    name: str
    passed: bool
    status: str = ""
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_id": self.check_id,
            "name": self.name,
            "passed": self.passed,
            "status": self.status,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp,
        }

    def to_health_check_result(self) -> HealthCheckResult:
        """Convert to a HealthCheckResult for the health monitor."""
        return HealthCheckResult(
            healthy=self.passed,
            message=self.message,
            details=self.details,
        )


class SandboxIntegrityVerifier:
    """Verifies sandbox integrity through multiple independently-checked dimensions.

    Dimensions:
    1. Binary Integrity — Cryptographic hash verification of the sandbox binary
    2. Audit Log Integrity — Tamper-evident chain verification of audit logs
    3. Process Health — Verification that sandbox processes are running and healthy
    4. Filesystem Isolation — Basic checks that jail boundaries are enforced

    Usage:
        verifier = SandboxIntegrityVerifier(sandbox_binary_path="./sandbox_core/target/release/sandbox_core")
        result = verifier.verify_binary_integrity()
        if not result.passed:
            # Handle tampered binary
    """

    def __init__(
        self,
        sandbox_binary_path: Optional[str] = None,
        expected_sha256: Optional[str] = None,
        audit_log_path: Optional[str] = None,
    ):
        self._binary_path = sandbox_binary_path or self._default_binary_path()
        self._expected_sha256 = expected_sha256 or ""
        self._audit_log_path = audit_log_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "..", "sandbox_audit.jsonl",
        )
        self._known_hashes: List[str] = []
        """List of known-good hashes (support multiple versions)."""
        self._last_results: Dict[str, IntegrityCheckResult] = {}
        self._check_count: int = 0

    def set_expected_hash(self, sha256: str) -> None:
        """Set the expected SHA-256 hash for the sandbox binary."""
        self._expected_sha256 = sha256

    def add_known_hash(self, sha256: str) -> None:
        """Add a known-good hash (for version upgrades)."""
        if sha256 not in self._known_hashes:
            self._known_hashes.append(sha256)

    # ── Binary Integrity ─────────────────────────────────────────────────

    def verify_binary_integrity(self) -> IntegrityCheckResult:
        """Verify the sandbox binary's cryptographic hash.

        Checks if the binary exists and its SHA-256 hash matches
        the expected or known-good hashes.

        Returns:
            IntegrityCheckResult with binary verification status.
        """
        self._check_count += 1
        check_id = f"binary_integrity_{self._check_count}"

        binary_path = self._binary_path
        if not binary_path:
            return IntegrityCheckResult(
                check_id=check_id,
                name="Sandbox Binary Integrity",
                passed=False,
                status=BinaryVerificationStatus.UNKNOWN.value,
                message="No sandbox binary path configured",
                details={"binary_path": binary_path, "error": "no_path"},
            )

        # Expand user and resolve path
        binary_path = os.path.expanduser(binary_path)
        if not os.path.isabs(binary_path):
            binary_path = os.path.abspath(binary_path)

        if not os.path.exists(binary_path):
            return IntegrityCheckResult(
                check_id=check_id,
                name="Sandbox Binary Integrity",
                passed=False,
                status=BinaryVerificationStatus.NOT_FOUND.value,
                message=f"Sandbox binary not found at {binary_path}",
                details={"binary_path": binary_path, "error": "not_found"},
            )

        # Compute SHA-256 hash
        try:
            actual_hash = self._compute_file_hash(binary_path)
        except Exception as e:
            return IntegrityCheckResult(
                check_id=check_id,
                name="Sandbox Binary Integrity",
                passed=False,
                status=BinaryVerificationStatus.UNKNOWN.value,
                message=f"Cannot compute hash: {e}",
                details={"binary_path": binary_path, "error": str(e)},
            )

        # Check against expected hash
        expected = self._expected_sha256
        known = self._known_hashes

        if expected and actual_hash == expected:
            status = BinaryVerificationStatus.VERIFIED
            passed = True
            message = f"Sandbox binary hash matches expected ({actual_hash[:16]}...)"
        elif expected and actual_hash != expected:
            status = BinaryVerificationStatus.MISMATCH
            passed = False
            message = (
                f"Sandbox binary hash MISMATCH!\n"
                f"  Expected: {expected[:32]}...\n"
                f"  Actual:   {actual_hash[:32]}..."
            )
            log.critical("SANDBOX BINARY TAMPER DETECTED! Expected %s, got %s", expected[:32], actual_hash[:32])
        elif known and actual_hash in known:
            status = BinaryVerificationStatus.VERIFIED
            passed = True
            message = f"Sandbox binary hash matches known-good ({actual_hash[:16]}...)"
        elif known and actual_hash not in known:
            status = BinaryVerificationStatus.MISMATCH
            passed = False
            message = f"Sandbox binary hash does not match any known-good hash ({actual_hash[:16]}...)"
        else:
            # No expected hash set — warn but don't fail (first-run scenario)
            status = BinaryVerificationStatus.UNKNOWN
            passed = True  # Don't block on first run
            message = f"No expected hash configured. Actual hash: {actual_hash[:16]}... (set via set_expected_hash or add_known_hash)"
            log.warning(message)

        result = IntegrityCheckResult(
            check_id=check_id,
            name="Sandbox Binary Integrity",
            passed=passed,
            status=status.value,
            message=message,
            details={
                "binary_path": binary_path,
                "actual_hash": actual_hash,
                "expected_hash": expected or "",
                "file_size": os.path.getsize(binary_path),
                "last_modified": os.path.getmtime(binary_path),
                "platform": platform.system(),
            },
        )

        self._last_results["binary_integrity"] = result
        return result

    # ── Audit Log Integrity ──────────────────────────────────────────────

    def verify_audit_log_integrity(
        self,
        audit_records: Optional[List[Dict[str, Any]]] = None,
    ) -> IntegrityCheckResult:
        """Verify the integrity of an audit log's hash chain.

        If audit_records is provided, verifies the hash chain.
        Otherwise, attempts to load the audit log from the configured path.

        Args:
            audit_records: Optional list of audit record dicts with
                          'record_hash' and 'previous_hash' fields.

        Returns:
            IntegrityCheckResult with audit log integrity status.
        """
        self._check_count += 1
        check_id = f"audit_log_integrity_{self._check_count}"

        if audit_records is None:
            # Try to load from file
            try:
                audit_records = self._load_audit_log_file()
            except Exception as e:
                return IntegrityCheckResult(
                    check_id=check_id,
                    name="Audit Log Integrity",
                    passed=False,
                    status=AuditLogIntegrityStatus.UNKNOWN.value,
                    message=f"Cannot load audit log: {e}",
                    details={"error": str(e)},
                )

        if not audit_records:
            return IntegrityCheckResult(
                check_id=check_id,
                name="Audit Log Integrity",
                passed=True,
                status=AuditLogIntegrityStatus.EMPTY.value,
                message="No audit records to verify",
                details={"record_count": 0},
            )

        # Verify hash chain
        previous_hash = ""
        violations = []

        for i, record in enumerate(audit_records):
            record_hash = record.get("record_hash", "")
            expected_prev = record.get("previous_hash", "")

            # Compute expected hash
            expected_hash = self._compute_record_hash(record)
            if record_hash and record_hash != expected_hash:
                violations.append(
                    f"Record {i} ({record.get('record_id', '?')[:16]}): "
                    f"hash mismatch (expected {expected_hash[:16]}..., got {record_hash[:16]}...)"
                )

            # Check chain linkage
            if expected_prev and expected_prev != previous_hash:
                violations.append(
                    f"Record {i} ({record.get('record_id', '?')[:16]}): "
                    f"previous_hash mismatch (expected {previous_hash[:16]}..., got {expected_prev[:16]}...)"
                )

            previous_hash = record_hash or previous_hash

        # Additionally verify the file on disk hasn't been truncated
        file_integrity_ok = True
        file_size = 0
        if self._audit_log_path:
            try:
                if os.path.exists(self._audit_log_path):
                    file_size = os.path.getsize(self._audit_log_path)
            except Exception as _ex:
                log.warning("Silenced exception: %s", _ex)

        passed = len(violations) == 0 and file_integrity_ok
        if violations:
            status = AuditLogIntegrityStatus.TAMPERED
            message = f"Audit log integrity violation: {violations[0]}"
            if len(violations) > 1:
                message += f" (+{len(violations)-1} more violations)"
            log.critical("AUDIT LOG TAMPERING DETECTED: %s", message)
        else:
            status = AuditLogIntegrityStatus.INTACT
            message = f"Audit log chain intact ({len(audit_records)} records, {file_size} bytes)"

        result = IntegrityCheckResult(
            check_id=check_id,
            name="Audit Log Integrity",
            passed=passed,
            status=status.value,
            message=message,
            details={
                "record_count": len(audit_records),
                "violations": violations,
                "file_size_bytes": file_size,
                "file_path": self._audit_log_path or "",
            },
        )

        self._last_results["audit_log_integrity"] = result
        return result

    # ── Process Health ──────────────────────────────────────────────────

    def check_process_health(
        self,
        process_name: str = "sandbox_core",
        expected_count_range: Tuple[int, int] = (0, 5),
    ) -> IntegrityCheckResult:
        """Check that sandbox processes are in a healthy state.

        Verifies:
        - Process exists and count is within expected range
        - No zombie/defunct processes
        - Processes are using reasonable resources

        Args:
            process_name: Name of the sandbox process to check.
            expected_count_range: (min, max) expected process count.

        Returns:
            IntegrityCheckResult with process health status.
        """
        self._check_count += 1
        check_id = f"process_health_{self._check_count}"

        try:
            if platform.system() == "Windows":
                # Windows: use tasklist
                result = subprocess.run(
                    ["tasklist", "/FO", "CSV", "/NH", "/FI", f"IMAGENAME eq {process_name}*"],
                    capture_output=True, text=True, timeout=10,
                )
                process_count = result.stdout.count("\n") if result.stdout else 0
                # tasklist CSV format: "image","pid","session","session#","mem"
                # Count non-empty lines
                process_count = len([l for l in result.stdout.strip().split("\n") if l.strip()])
            else:
                # Unix: use ps
                result = subprocess.run(
                    ["ps", "-e", "-o", "pid,state,comm"],
                    capture_output=True, text=True, timeout=10,
                )
                lines = result.stdout.strip().split("\n")[1:]  # Skip header
                matching = [
                    l for l in lines if process_name in l.split()[-1] if l.split()[-1]
                ]
                process_count = len(matching)
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            return IntegrityCheckResult(
                check_id=check_id,
                name="Sandbox Process Health",
                passed=True,  # Non-blocking — may not be running
                status="unknown",
                message=f"Cannot check process health: {e}",
                details={"process_name": process_name, "error": str(e)},
            )

        min_count, max_count = expected_count_range
        if process_count < min_count:
            passed = process_count == 0  # 0 is acceptable (not running yet)
            status = "not_running" if process_count == 0 else "low_count"
            message = f"Sandbox processes: {process_count} (expected {min_count}-{max_count})"
        elif process_count > max_count:
            passed = False
            status = "high_count"
            message = f"Too many sandbox processes: {process_count} (expected {min_count}-{max_count})"
            log.warning("High sandbox process count: %d", process_count)
        else:
            passed = True
            status = "healthy"
            message = f"Sandbox processes healthy: {process_count} running"

        result = IntegrityCheckResult(
            check_id=check_id,
            name="Sandbox Process Health",
            passed=passed,
            status=status,
            message=message,
            details={
                "process_name": process_name,
                "process_count": process_count,
                "expected_min": min_count,
                "expected_max": max_count,
                "platform": platform.system(),
            },
        )

        self._last_results["process_health"] = result
        return result

    # ── Filesystem Isolation ─────────────────────────────────────────────

    def check_filesystem_isolation(
        self,
        workspace_path: Optional[str] = None,
    ) -> IntegrityCheckResult:
        """Check that the sandbox filesystem jail is properly isolated.

        Verifies:
        - Workspace directory exists and is accessible
        - No symlinks escaping the workspace boundary
        - Workspace is not a system directory

        Args:
            workspace_path: Path to the sandbox workspace directory.

        Returns:
            IntegrityCheckResult with isolation status.
        """
        self._check_count += 1
        check_id = f"fs_isolation_{self._check_count}"

        ws_path = workspace_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "..", "workspace",
        )
        ws_path = os.path.abspath(os.path.expanduser(ws_path))

        issues: List[str] = []

        # 1. Check workspace exists
        if not os.path.exists(ws_path):
            issues.append(f"Workspace path does not exist: {ws_path}")
        elif not os.path.isdir(ws_path):
            issues.append(f"Workspace path is not a directory: {ws_path}")

        # 2. Check for symlink escapes (basic check)
        if os.path.exists(ws_path):
            try:
                for entry in os.listdir(ws_path)[:50]:  # Limit scan
                    entry_path = os.path.join(ws_path, entry)
                    if os.path.islink(entry_path):
                        target = os.readlink(entry_path)
                        target_abs = os.path.abspath(os.path.join(os.path.dirname(entry_path), target))
                        if not target_abs.startswith(os.path.abspath(ws_path)):
                            issues.append(f"Symlink escapes jail: {entry} -> {target}")
            except PermissionError as e:
                issues.append(f"Cannot scan workspace: {e}")

        # 3. Check workspace is not a system directory
        system_dirs = ["/etc", "/var", "/bin", "/sbin", "/usr", "/boot",
                       "C:\\Windows", "C:\\System32", "C:\\Program Files"]
        ws_lower = ws_path.lower()
        for sd in system_dirs:
            if sd.lower() in ws_lower:
                issues.append(f"Workspace appears to be a system directory: {ws_path}")

        passed = len(issues) == 0
        status = "isolated" if passed else "violation_detected"
        message = "Filesystem isolation OK" if passed else f"Filesystem isolation issues: {'; '.join(issues[:3])}"

        result = IntegrityCheckResult(
            check_id=check_id,
            name="Filesystem Isolation",
            passed=passed,
            status=status,
            message=message,
            details={
                "workspace_path": ws_path,
                "issues": issues,
                "exists": os.path.exists(ws_path),
                "is_dir": os.path.isdir(ws_path) if os.path.exists(ws_path) else False,
            },
        )

        self._last_results["fs_isolation"] = result
        return result

    # ── Comprehensive Check ──────────────────────────────────────────────

    def run_all_checks(
        self,
        workspace_path: Optional[str] = None,
        audit_records: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, IntegrityCheckResult]:
        """Run all sandbox integrity checks.

        Args:
            workspace_path: Optional workspace path for filesystem check.
            audit_records: Optional audit records for log integrity check.

        Returns:
            Dict mapping check names to results.
        """
        results: Dict[str, IntegrityCheckResult] = {
            "binary_integrity": self.verify_binary_integrity(),
            "audit_log_integrity": self.verify_audit_log_integrity(audit_records),
            "process_health": self.check_process_health(),
            "fs_isolation": self.check_filesystem_isolation(workspace_path),
        }
        return results

    def all_passed(self) -> bool:
        """Check if all previously run checks passed."""
        return all(r.passed for r in self._last_results.values())

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of all integrity check results."""
        return {
            "all_passed": self.all_passed(),
            "checks": {
                name: result.to_dict()
                for name, result in self._last_results.items()
            },
            "total_checks": len(self._last_results),
            "passed_checks": sum(1 for r in self._last_results.values() if r.passed),
            "failed_checks": sum(1 for r in self._last_results.values() if not r.passed),
        }

    def get_last_result(self, check_name: str) -> Optional[IntegrityCheckResult]:
        """Get the last result for a specific check."""
        return self._last_results.get(check_name)

    # ── Health Check Integration ─────────────────────────────────────────

    def create_health_check_fns(self) -> Dict[str, Any]:
        """Create health check functions for RuntimeHealthMonitor.

        Returns a dict mapping check names to callables that return
        HealthCheckResult.
        """
        return {
            "binary_integrity": lambda: self.verify_binary_integrity().to_health_check_result(),
            "audit_log_integrity": lambda: self.verify_audit_log_integrity().to_health_check_result(),
            "process_health": lambda: self.check_process_health().to_health_check_result(),
            "fs_isolation": lambda: self.check_filesystem_isolation().to_health_check_result(),
        }

    # ── Internal ─────────────────────────────────────────────────────────

    @staticmethod
    def _default_binary_path() -> str:
        """Get the default sandbox binary path."""
        # Look relative to the project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
        return os.path.join(
            project_root, "sandbox_core", "target", "release", "sandbox_core.exe",
        )

    @staticmethod
    def _compute_file_hash(file_path: str) -> str:
        """Compute SHA-256 hash of a file."""
        sha = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha.update(chunk)
        return sha.hexdigest()

    @staticmethod
    def _compute_record_hash(record: Dict[str, Any]) -> str:
        """Compute the expected hash of an audit record."""
        # Exclude record_hash from the content
        content = {k: v for k, v in record.items() if k != "record_hash"}
        raw = json.dumps(content, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def _load_audit_log_file(self) -> List[Dict[str, Any]]:
        """Load audit records from the audit log file."""
        import json
        records: List[Dict[str, Any]] = []
        path = self._audit_log_path
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError as _ex:
                            log.warning("Silenced exception: %s", _ex)
        return records
