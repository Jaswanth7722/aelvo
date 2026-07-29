use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use serde_json::{Value, json};
use regex::Regex;
use crate::resource::ResourceKind;

// ═══════════════════════════════════════════════════════════════════════
// Security Event Types
// ═══════════════════════════════════════════════════════════════════════

/// All possible security events that the auditor can emit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SecurityEvent {
    /// Sandbox started processing a request
    SandboxStarted,
    /// Request was denied by policy
    SandboxDenied,
    /// Request was allowed by policy
    SandboxAllowed,
    /// A policy violation was detected
    PolicyViolation,
    /// A process execution timed out
    TimeoutTriggered,
    /// A resource limit was hit
    ResourceLimitHit,
    /// A workspace escape attempt was detected
    WorkspaceEscapeAttempt,
    /// An audit record was created
    AuditRecordCreated,
    /// Command execution completed
    CommandCompleted,
    /// A path traversal attempt was blocked
    PathTraversalBlocked,
}

impl SecurityEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            SecurityEvent::SandboxStarted => "sandbox_started",
            SecurityEvent::SandboxDenied => "sandbox_denied",
            SecurityEvent::SandboxAllowed => "sandbox_allowed",
            SecurityEvent::PolicyViolation => "policy_violation",
            SecurityEvent::TimeoutTriggered => "timeout_triggered",
            SecurityEvent::ResourceLimitHit => "resource_limit_hit",
            SecurityEvent::WorkspaceEscapeAttempt => "workspace_escape_attempt",
            SecurityEvent::AuditRecordCreated => "audit_record_created",
            SecurityEvent::CommandCompleted => "command_completed",
            SecurityEvent::PathTraversalBlocked => "path_traversal_blocked",
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Secrets Redaction
// ═══════════════════════════════════════════════════════════════════════

/// Known patterns for secrets that must be redacted from audit logs.
const SECRET_PATTERNS: &[&str] = &[
    r"sk_live_[a-zA-Z0-9]{24,}",       // Stripe live secret key
    r"sk_test_[a-zA-Z0-9]{24,}",       // Stripe test secret key
    r"pk_live_[a-zA-Z0-9]{24,}",       // Stripe live publishable key
    r"gh[pousr]_[A-Za-z0-9_]{36,}",    // GitHub tokens (ghp_, gho_, ghu_, ghs_, ghr_)
    r"ghp_[a-zA-Z0-9]{36,}",           // GitHub personal access token
    r"xox[bapr]-[a-zA-Z0-9-]{24,}",    // Slack tokens
    r"AKIA[0-9A-Z]{16}",               // AWS access key ID
    r"eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}", // JWT tokens
    r"Bearer\s+[a-zA-Z0-9._-]{20,}",  // Bearer tokens
    r"api[_-]?key[=:]\s*[a-zA-Z0-9]{16,}",  // API keys in key=value format
    r"token[=:]\s*[a-zA-Z0-9]{16,}",   // Tokens in key=value format
    r"secret[=:]\s*[a-zA-Z0-9]{16,}",  // Secrets in key=value format
    r"password[=:]\s*\S+",             // Passwords
    r"-----BEGIN (RSA |EC |)PRIVATE KEY-----",  // Private keys (start)
];

// ═══════════════════════════════════════════════════════════════════════
// Audit Record
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct AuditRecord {
    pub timestamp: String,
    pub event: String,
    pub action: String,
    pub decision: String,
    pub duration_ms: u64,
    pub exit_code: Option<i32>,
    pub details: Value,
}

impl AuditRecord {
    pub fn to_json(&self) -> Value {
        json!({
            "timestamp": self.timestamp,
            "event": self.event,
            "action": self.action,
            "decision": self.decision,
            "duration_ms": self.duration_ms,
            "exit_code": self.exit_code,
            "details": self.details,
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Auditor — writes structured audit logs to a JSONL file
// ═══════════════════════════════════════════════════════════════════════

pub struct Auditor {
    log_file: PathBuf,
    start_time: std::time::Instant,
    redaction_regexes: Vec<Regex>,
    /// Number of audit entries written (for diagnostics)
    entry_count: u64,
    /// Number of write failures (silent counter, never eprintln!)
    write_failures: u64,
}

impl Auditor {
    /// Create a new auditor that writes audit logs to the workspace.
    pub fn new(workspace_root: &str) -> Self {
        let log_file = Path::new(workspace_root).join("sandbox_audit.jsonl");

        // Compile all redaction patterns
        let redaction_regexes: Vec<Regex> = SECRET_PATTERNS
            .iter()
            .filter_map(|p| Regex::new(p).ok())
            .collect();

        Auditor {
            log_file,
            start_time: std::time::Instant::now(),
            redaction_regexes,
            entry_count: 0,
            write_failures: 0,
        }
    }

    /// Get the path to the audit log file.
    pub fn log_path(&self) -> &Path {
        &self.log_file
    }

    /// Get the total number of audit entries written.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Get the total number of audit write failures.
    pub fn write_failures(&self) -> u64 {
        self.write_failures
    }

    /// Log the start of a sandbox request.
    pub fn log_start(&mut self, action: &str, params: &Value) {
        self.emit_event(
            SecurityEvent::SandboxStarted,
            action,
            "started",
            json!({
                "action": action,
                "params": self.redact_params(params),
            }),
        );
    }

    /// Log a policy decision.
    pub fn log_policy_decision(&mut self, action: &str, allowed: bool, reason: &str) {
        let event_type = if allowed {
            SecurityEvent::SandboxAllowed
        } else {
            SecurityEvent::SandboxDenied
        };

        self.emit_event(
            event_type,
            action,
            if allowed { "allowed" } else { "denied" },
            json!({
                "action": action,
                "allowed": allowed,
                "reason": self.redact_string(reason),
            }),
        );
    }

    /// Log a policy violation.
    pub fn log_policy_violation(&mut self, action: &str, detail: &str) {
        self.emit_event(
            SecurityEvent::PolicyViolation,
            action,
            "violation",
            json!({
                "action": action,
                "detail": self.redact_string(detail),
            }),
        );
    }

    /// Log a command execution attempt.
    pub fn log_command(&mut self, command: &str, status: &str, exit_code: Option<i32>) {
        let event = match status {
            "TIMEOUT" => SecurityEvent::TimeoutTriggered,
            s if s.starts_with("POLICY_DENIED") => SecurityEvent::PolicyViolation,
            _ => SecurityEvent::CommandCompleted,
        };

        self.emit_event(
            event,
            "execute_command",
            status,
            json!({
                "command": self.redact_string(command),
                "exit_code": exit_code,
                "status": status,
            }),
        );
    }

    /// Log a path traversal blocked event.
    pub fn log_traversal_blocked(&mut self, path: &str, resolved: &str) {
        self.emit_event(
            SecurityEvent::PathTraversalBlocked,
            "path_operation",
            "denied",
            json!({
                "original_path": path,
                "resolved_path": resolved,
                "reason": "Path traversal detected: resolved path is outside jail boundary",
            }),
        );
    }

    /// Log a resource limit hit event.
    pub fn log_resource_limit(&mut self, kind: ResourceKind, limit: u64, actual: u64) {
        let resource_name = kind.as_str();
        self.emit_event(
            SecurityEvent::ResourceLimitHit,
            resource_name,
            "limit_exceeded",
            json!({
                "resource": resource_name,
                "limit": limit,
                "actual": actual,
            }),
        );
    }

    /// Log the completion of a sandbox request.
    pub fn log_complete(&mut self, result: &Result<super::RunResult, String>) {
        let (status, message) = match result {
            Ok(r) => ("success", &r.logs),
            Err(e) => ("error", e),
        };

        let elapsed = self.start_time.elapsed().as_millis() as u64;

        self.emit_event(
            SecurityEvent::AuditRecordCreated,
            "request_complete",
            status,
            json!({
                "status": status,
                "message": self.redact_string(message),
                "elapsed_ms": elapsed,
            }),
        );
    }

    /// Log a generic security event.
    pub fn emit_security_event(&mut self, event: SecurityEvent, action: &str, detail: &str) {
        self.emit_event(
            event,
            action,
            "security_event",
            json!({
                "action": action,
                "detail": self.redact_string(detail),
            }),
        );
    }

    // ═══════════════════════════════════════════════════════════════════
    // Internal methods
    // ═══════════════════════════════════════════════════════════════════

    /// Emit a structured security event to the audit log.
    fn emit_event(&mut self, event: SecurityEvent, action: &str, decision: &str, details: Value) {
        let elapsed = self.start_time.elapsed().as_millis() as u64;
        let timestamp = {
            let dur = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            // Format as ISO-8601 / RFC 3339
            let secs = dur.as_secs();
            let nanos = dur.subsec_nanos();
            let micros = nanos / 1000;
            // Convert to datetime components
            let days = secs / 86400;
            let time_secs = secs % 86400;
            let hours = time_secs / 3600;
            let minutes = (time_secs % 3600) / 60;
            let seconds = time_secs % 60;
            // Date from days since epoch (simplified Gregorian)
            let (year, month, day) = days_to_date(days);
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
                year, month, day, hours, minutes, seconds, micros / 1000
            )
        };

        let record = json!({
            "timestamp": timestamp,
            "event": event.as_str(),
            "action": action,
            "decision": decision,
            "elapsed_ms": elapsed,
            "details": details,
        });

        if let Err(_e) = self.write_entry(&record) {
            self.write_failures += 1;
        } else {
            self.entry_count += 1;
        }
    }

    /// Write a JSON entry to the audit log file.
    fn write_entry(&self, entry: &Value) -> Result<(), std::io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file)?;

        let mut writer = std::io::BufWriter::new(file);
        let line = serde_json::to_string(entry)?;
        writeln!(writer, "{}", line)?;
        writer.flush()?;
        Ok(())
    }

    /// Redact secrets from a string value.
    pub fn redact_string(&self, input: &str) -> String {
        let mut result = input.to_string();
        for regex in &self.redaction_regexes {
            result = regex.replace_all(&result, "[REDACTED]").to_string();
        }
        result
    }

    /// Redact secrets from a JSON value (recursively).
    pub fn redact_params(&self, val: &Value) -> Value {
        match val {
            Value::String(s) => {
                Value::String(self.redact_string(s))
            }
            Value::Object(map) => {
                let mut new_map = serde_json::Map::new();
                for (k, v) in map {
                    new_map.insert(k.clone(), self.redact_params(v));
                }
                Value::Object(new_map)
            }
            Value::Array(arr) => {
                Value::Array(arr.iter().map(|v| self.redact_params(v)).collect())
            }
            other => other.clone(),
        }
    }

    /// Reset the auditor's timer (for testing).
    pub fn reset_timer(&mut self) {
        self.start_time = std::time::Instant::now();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Standalone audit functions for backward compatibility
// ═══════════════════════════════════════════════════════════════════════

/// Convert days since Unix epoch to (year, month, day) using Gregorian calendar.
fn days_to_date(days: u64) -> (u64, u64, u64) {
    // Days since 1970-01-01
    let mut y = 1970i64;
    let mut d = days as i64;

    loop {
        let days_in_year = if is_leap(y) { 366 } else { 365 };
        if d < days_in_year {
            break;
        }
        d -= days_in_year;
        y += 1;
    }

    let mut month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if is_leap(y) {
        month_days[1] = 29;
    }

    let mut m = 0;
    for md in &month_days {
        if d < *md {
            break;
        }
        d -= *md;
        m += 1;
    }

    (y as u64, (m + 1) as u64, (d + 1) as u64)
}

/// Check if a year is a leap year.
fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Read all audit records from a workspace's audit log.
pub fn read_audit_log(workspace_root: &str) -> Result<Vec<Value>, String> {
    let path = Path::new(workspace_root).join("sandbox_audit.jsonl");
    if !path.exists() {
        return Ok(Vec::new());
    }

    let content = fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read audit log: {}", e))?;

    let mut records = Vec::new();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(line) {
            Ok(record) => records.push(record),
            Err(_e) => {
                // Silently skip malformed records
            }
        }
    }

    Ok(records)
}

/// Verify the integrity of an audit log (all entries are valid JSON).
pub fn verify_audit_integrity(workspace_root: &str) -> Result<(usize, Vec<String>), String> {
    let records = read_audit_log(workspace_root)?;
    let mut errors = Vec::new();

    for record in &records {
        // Validate required fields
        if record.get("timestamp").is_none() {
            errors.push("Missing timestamp field".into());
        }
        if record.get("event").is_none() {
            errors.push("Missing event field".into());
        }
        if record.get("action").is_none() {
            errors.push("Missing action field".into());
        }

        // Check for unredacted secrets
        let serialized = serde_json::to_string(record).unwrap_or_default();
        for pattern in SECRET_PATTERNS {
            if let Ok(re) = Regex::new(pattern) {
                if re.is_match(&serialized) {
                    errors.push(format!("Unredacted secret detected matching pattern: {}", pattern));
                }
            }
        }
    }

    Ok((records.len(), errors))
}
