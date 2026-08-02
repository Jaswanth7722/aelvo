use std::io::{self, Read};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod fs_jail;
pub mod process;
pub mod policy;
pub mod audit;
pub mod resource;
pub mod threat_detection;
pub mod workspace_intelligence;
pub mod checkpoint;

// ═══════════════════════════════════════════════════════════════════════
// Data Types
// ═══════════════════════════════════════════════════════════════════════

/// Incoming JSON-RPC request from the Python orchestration layer.
#[derive(Deserialize, Debug)]
pub struct Request {
    /// The action to perform (e.g., "read_file", "write_atomic", "execute_command")
    pub action: String,
    /// The workspace root path — the jail boundary
    pub workspace_root: String,
    /// The repo root path (may be subdirectory of workspace)
    pub repo_root: String,
    /// Whether write operations are allowed
    pub write_mode: bool,
    /// Action-specific parameters
    pub params: Value,
}

/// Structured response sent back to the Python layer.
#[derive(Serialize)]
struct Response {
    status: String,
    data: Option<Value>,
    logs: String,
    audit: Option<Value>,
    success: bool,
}

/// Result of a successful sandbox operation.
pub struct RunResult {
    pub data: Option<Value>,
    pub logs: String,
    pub audit: Option<Value>,
}

// ═══════════════════════════════════════════════════════════════════════
// Entry Point
// ═══════════════════════════════════════════════════════════════════════

fn main() {
    // Read full stdin as JSON
    let mut input = String::new();
    if let Err(e) = io::stdin().read_to_string(&mut input) {
        respond_error(&format!("Failed to read stdin: {}", e));
        return;
    }

    // Parse request
    let req: Request = match serde_json::from_str(&input) {
        Ok(r) => r,
        Err(e) => {
            respond_error(&format!("Invalid JSON request: {}", e));
            return;
        }
    };

    // Handle the request through the full pipeline
    let result = handle_request(&req);

    match result {
        Ok(res) => respond_success(res),
        Err(e) => respond_error(&e),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Request Handler — complete execution pipeline
// ═══════════════════════════════════════════════════════════════════════

fn handle_request(req: &Request) -> Result<RunResult, String> {
    // ── 1. Setup workspace boundaries (fail closed) ─────────────────
    let jail = fs_jail::FsJail::new(&req.workspace_root, &req.repo_root)?;

    // ── 2. Initialize audit session ─────────────────────────────────
    let mut auditor = audit::Auditor::new(&req.workspace_root);
    auditor.log_start(&req.action, &req.params);

    // ── 3. Create policy engine ─────────────────────────────────────
    let policy = policy::PolicyEngine::new();

    // ── 4. Initialize threat detector ───────────────────────────────
    let threat_detector = threat_detection::ThreatDetector::new();

    // ── 5. Initialize workspace intelligence ────────────────────────
    let workspace_intel = match workspace_intelligence::WorkspaceIntelligence::new(&req.workspace_root) {
        Ok(wi) => wi,
        Err(e) => {
            return Err(format!("Workspace intelligence init failed: {}", e));
        }
    };

    // ── 6. Initialize checkpoint manager ────────────────────────────
    let mut checkpoint_mgr = match checkpoint::CheckpointManager::new(&req.workspace_root, None) {
        Ok(cm) => cm,
        Err(e) => {
            return Err(format!("Checkpoint manager init failed: {}", e));
        }
    };

    // ── 7. Policy evaluation ────────────────────────────────────────
    // Evaluate the complete request (including path checks for file ops)
    let decision = policy.evaluate(req, &jail);
    auditor.log_policy_decision(
        &req.action,
        decision.is_allowed(),
        decision.reason(),
    );

    if !decision.is_allowed() {
        // Log the denial as a policy violation
        auditor.emit_security_event(
            audit::SecurityEvent::PolicyViolation,
            &req.action,
            &format!("Policy denied: {}", decision.reason()),
        );
        return Err(format!("Policy denied: {}", decision.reason()));
    }

    // ── 8. Threat detection for command execution ───────────────────
    if req.action == "execute_command" || req.action == "bash_exec" {
        let command = req.params
            .get("command")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if !command.is_empty() {
            let threat_findings = threat_detector.analyze_command(command);
            if !threat_findings.is_empty() {
                let (blocked, severity, summary) = threat_detection::ThreatDetector::summarize_threats(&threat_findings);
                auditor.emit_security_event(
                    audit::SecurityEvent::PolicyViolation,
                    &format!("{}/threat_detection", req.action),
                    &summary,
                );
                if blocked {
                    return Err(format!(
                        "Threat detection blocked command [severity={}]: {}",
                        severity.as_str(),
                        summary
                    ));
                }
            }
        }
    }

    // ── 9. Workspace intelligence path check for file operations ────
    if let Some(path) = req.params.get("path").and_then(|v| v.as_str()) {
        match req.action.as_str() {
            "write_atomic" | "edit_file_block" | "delete_file" => {
                let (allowed, reason) = workspace_intel.can_write(path);
                if !allowed {
                    auditor.emit_security_event(
                        audit::SecurityEvent::WorkspaceEscapeAttempt,
                        &req.action,
                        &reason,
                    );
                    return Err(format!("Workspace intelligence blocked write: {}", reason));
                }
            }
            "read_file" | "read_file_range" => {
                let (allowed, reason) = workspace_intel.can_read(path);
                if !allowed {
                    auditor.emit_security_event(
                        audit::SecurityEvent::WorkspaceEscapeAttempt,
                        &req.action,
                        &reason,
                    );
                    return Err(format!("Workspace intelligence blocked read: {}", reason));
                }
            }
            _ => {}
        }
    }

    // ── 10. Create auto-checkpoint for write operations ──────────────
    let is_write = matches!(
        req.action.as_str(),
        "write_atomic" | "edit_file_block" | "delete_file"
    );
    if is_write && !checkpoint_mgr.has_active_checkpoint() {
        let _ = checkpoint_mgr.create_checkpoint(&format!("auto_{}", req.action));
    }

    // ── 5. Route to handler and execute ─────────────────────────────
    let result = match req.action.as_str() {        // Path resolution (validation only, no read)
            "resolve_path" => fs_jail::resolve_path(req, &jail),

            // File read operations
            "read_file" => fs_jail::read_file(req, &jail),
            "read_file_range" => fs_jail::read_range(req, &jail),

        // File write operations
        "write_atomic" => fs_jail::write_file(req, &jail),
        "edit_file_block" => fs_jail::edit_file_block(req, &jail),
        "delete_file" => fs_jail::delete_file(req, &jail),

        // Directory operations
        "list_directory" => fs_jail::list_directory(req, &jail),

        // Code search & file discovery
        "grep_file" => fs_jail::grep_file(req, &jail),
        "search_code" => fs_jail::search_code(req, &jail),
        "find_files" => fs_jail::find_files(req, &jail),
        "project_tree" => fs_jail::project_tree(req, &jail),

        // Command execution
        "execute_command" | "bash_exec" => {
            process::bash_exec(req, &jail, &mut auditor, &policy)
        }

        // Unknown action
        _ => Err(format!(
            "Unknown action '{}'. Supported actions: resolve_path, read_file, read_file_range, write_atomic, edit_file_block, delete_file, list_directory, grep_file, search_code, find_files, project_tree, execute_command, bash_exec",
            req.action
        )),
    };

    // ── 6. Audit completion ─────────────────────────────────────────
    auditor.log_complete(&result);

    result
}

// ═══════════════════════════════════════════════════════════════════════
// Response Helpers
// ═══════════════════════════════════════════════════════════════════════

/// Serialize any value to a single JSON line for stdout.
///
/// Never fails silently: the Python orchestrator blocks on stdout
/// until it sees a JSON line. If serialization errors, emit a valid
/// error payload instead so the orchestrator always gets a response.
fn render_serializable<T: serde::Serialize>(value: &T) -> String {
    match serde_json::to_string(value) {
        Ok(json) => json,
        Err(_) => serde_json::json!({
            "status": "error",
            "data": null,
            "logs": "response serialization failed",
            "audit": null,
            "success": false
        })
        .to_string(),
    }
}

/// Send an error response to stdout (must be valid JSON for Python parser).
fn respond_error(msg: &str) {
    let res = Response {
        status: "error".to_string(),
        data: None,
        logs: msg.to_string(),
        audit: None,
        success: false,
    };
    println!("{}", render_serializable(&res));
}

/// Send a success response to stdout.
fn respond_success(res: RunResult) {
    let resp = Response {
        status: "success".to_string(),
        data: res.data,
        logs: res.logs,
        audit: res.audit,
        success: true,
    };
    println!("{}", render_serializable(&resp));
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde::ser::Error as _; // for S::Error::custom

    #[test]
    fn render_serializable_emits_valid_json_for_success() {
        let res = Response {
            status: "success".to_string(),
            data: Some(serde_json::json!({"ok": true})),
            logs: "done".to_string(),
            audit: None,
            success: true,
        };
        let line = render_serializable(&res);
        let v: Value = serde_json::from_str(&line).expect("must be valid JSON");
        assert_eq!(v["status"], "success");
        assert_eq!(v["success"], true);
        assert_eq!(v["data"]["ok"], true);
        assert_eq!(v["logs"], "done");
    }

    #[test]
    fn render_serializable_emits_valid_json_for_error() {
        let res = Response {
            status: "error".to_string(),
            data: None,
            logs: "boom".to_string(),
            audit: None,
            success: false,
        };
        let line = render_serializable(&res);
        let v: Value = serde_json::from_str(&line).expect("must be valid JSON");
        assert_eq!(v["status"], "error");
        assert_eq!(v["logs"], "boom");
        assert_eq!(v["success"], false);
        assert!(v["data"].is_null());
    }

    /// A type whose Serialize impl always fails, forcing the fallback.
    struct AlwaysFailsToSerialize;

    impl serde::Serialize for AlwaysFailsToSerialize {
        fn serialize<S: serde::Serializer>(
            &self,
            _serializer: S,
        ) -> Result<S::Ok, S::Error> {
            Err(S::Error::custom("intentional failure"))
        }
    }

    #[test]
    fn render_serializable_falls_back_to_valid_error_json_on_failure() {
        let line = render_serializable(&AlwaysFailsToSerialize);
        let v: Value = serde_json::from_str(&line).expect("fallback must be valid JSON");
        assert_eq!(v["status"], "error");
        assert_eq!(v["logs"], "response serialization failed");
        assert_eq!(v["success"], false);
        assert!(v["data"].is_null());
        assert!(v["audit"].is_null());
    }
}
