use std::collections::HashSet;
use regex::Regex;

use crate::{Request, fs_jail::FsJail};

// ═══════════════════════════════════════════════════════════════════════
// Policy Decision
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyDecision {
    /// Action is explicitly allowed
    Allowed,
    /// Action is denied with an explanation
    Denied(String),
}

impl PolicyDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, PolicyDecision::Allowed)
    }

    pub fn reason(&self) -> &str {
        match self {
            PolicyDecision::Allowed => "allowed",
            PolicyDecision::Denied(reason) => reason.as_str(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Policy Engine
// ═══════════════════════════════════════════════════════════════════════

/// Default set of explicitly allowed command base names.
/// Only commands whose base name is in this set may be executed.
/// Any command not in this set is denied by default.
const DEFAULT_ALLOWED_COMMANDS: &[&str] = &[
    // Shell built-ins and utilities
    "echo", "cat", "type", "dir", "more", "sort", "find", "findstr",
    "where", "which", "head", "tail", "wc", "tee",
    // Navigation and inspection
    "ls", "pwd", "cd",
    // File operations (safe subset)
    "cp", "copy", "move", "ren", "mkdir", "rmdir",
    "touch", "chmod", "attrib",
    // Development tools
    "python", "python3", "node", "npm", "npx", "pnpm", "yarn",
    "cargo", "rustc", "rustup",
    "gcc", "g++", "clang", "make", "cmake",
    "go", "deno", "bun",
    "pip", "pip3", "poetry",
    // Version control
    "git",
    // Text processing
    "grep", "findstr", "awk", "sed",
    // Compression
    "tar", "gzip", "gunzip", "zip", "unzip",
    // Network (safe inspection)
    "ping", "curl", "wget", "nslookup",
    // Process inspection
    "ps", "tasklist", "top", "htop",
    // Rust/Cargo build chain
    "cmd", "sh", "bash", "powershell",
    // Windows-specific
    "xcopy", "robocopy",
    // File content search
    "rg", "ripgrep",
];

/// Command patterns that are always blocked regardless of the allowlist.
/// These patterns match command *content* (substrings in the full command string).
const BLOCKED_COMMAND_PATTERNS: &[&str] = &[
    r"rm\s+-rf\s+/",           // Recursive delete from root
    r"rm\s+-rf\s+--no-preserve-root",
    r"mkfs\.",                  // Format filesystem
    r"dd\s+if=",                // Dangerous dd operations
    r":\(\)\s*\{",              // Fork bomb
    r">\s*/dev/sda",            // Direct device writes
    r"format\s+\w:\s*\/q",     // Windows format with quiet
    r"del\s+/[fFsS].*\\\*\.\*", // Dangerous recursive delete on Windows
    r"rd\s+/[sSqQ]\s+\\\\",    // Dangerous rmdir on Windows
    r"shutdown\s+/[rsp]",      // Shutdown/restart
    r"taskkill\s+/[fF]\s+/[iI][mM]", // Force kill by image name
    r"reg\s+(delete|add)\s+",  // Registry modification
    r"chmod\s+777\s+/",        // World-writable root
    r"chown\s+-[Rr]\s+",       // Recursive chown
];

/// Common dangerous shell metacharacters and constructs that may indicate injection.
const SHELL_INJECTION_PATTERNS: &[&str] = &[
    r"\$\(.*\)",      // Command substitution
    r"`[^`]+`",       // Backtick command substitution
    r";\s*rm\s",      // Semicolon + rm
    r"\|\s*sh\b",     // Pipe to shell
    r"\|\s*bash\b",   // Pipe to bash
    r"\|\s*zsh\b",    // Pipe to zsh
    r">>\s*/etc/",    // Append to system files
    r">\s*/etc/",     // Write to system files
];

pub struct PolicyEngine {
    /// Set of explicitly allowed command base names.
    allowed_commands: HashSet<String>,
    /// Compiled regex patterns for blocked commands.
    blocked_patterns: Vec<Regex>,
    /// Compiled regex patterns for shell injection detection.
    injection_patterns: Vec<Regex>,
}

impl PolicyEngine {
    pub fn new() -> Self {
        let allowed: HashSet<String> = DEFAULT_ALLOWED_COMMANDS
            .iter()
            .map(|s| s.to_lowercase())
            .collect();

        let blocked: Vec<Regex> = BLOCKED_COMMAND_PATTERNS
            .iter()
            .map(|p| Regex::new(p).expect("Invalid blocked command pattern"))
            .collect();

        let injection: Vec<Regex> = SHELL_INJECTION_PATTERNS
            .iter()
            .map(|p| Regex::new(p).expect("Invalid injection pattern"))
            .collect();

        PolicyEngine {
            allowed_commands: allowed,
            blocked_patterns: blocked,
            injection_patterns: injection,
        }
    }

    /// Extend the allowlist with additional commands (called from Python layer).
    pub fn add_allowed_command(&mut self, cmd: &str) {
        self.allowed_commands.insert(cmd.to_lowercase());
    }

    /// Evaluate a complete action request against policy.
    pub fn evaluate(&self, req: &Request, jail: &FsJail) -> PolicyDecision {
        match req.action.as_str() {
            "read_file" | "read_file_range" | "grep_file" => {
                // Read operations: path must be within jail
                let path = req.params.get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                self.evaluate_path_operation(path, jail, false)
            }
            "search_code" | "find_files" | "project_tree" => {
                // Workspace-wide read operations: always allowed (jail boundary
                // is enforced by the handler functions using jail.workspace_root())
                PolicyDecision::Allowed
            }
            "write_atomic" | "edit_file_block" | "delete_file" => {
                // Write operations: must have write_mode, path within jail
                if !req.write_mode {
                    return PolicyDecision::Denied("Write mode is disabled by the caller. Set write_mode=true to enable writes.".into());
                }
                let path = req.params.get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                self.evaluate_path_operation(path, jail, true)
            }
            "execute_command" | "bash_exec" => {
                let command = req.params.get("command")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                self.evaluate_command(command)
            }
            _ => {
                PolicyDecision::Denied(format!("Unknown action '{}'. No policy rule applies.", req.action))
            }
        }
    }

    /// Evaluate a command string against the command allowlist and blocklist.
    pub fn evaluate_command(&self, command: &str) -> PolicyDecision {
        if command.is_empty() {
            return PolicyDecision::Denied("Empty command is not allowed.".into());
        }

        // Check against blocked patterns first
        for pattern in &self.blocked_patterns {
            if pattern.is_match(command) {
                return PolicyDecision::Denied(
                    format!("Command contains blocked pattern: '{}'", pattern.as_str())
                );
            }
        }

        // Check for shell injection indicators
        for pattern in &self.injection_patterns {
            if pattern.is_match(command) {
                return PolicyDecision::Denied(
                    format!("Command contains shell injection indicator: '{}'", pattern.as_str())
                );
            }
        }

        // Extract the command base name (first word, stripping path prefixes)
        let trimmed = command.trim();
        let first_word = trimmed.split_whitespace().next().unwrap_or("");
        let base_name = std::path::Path::new(first_word)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(first_word)
            .to_lowercase();

        // Allowlist check
        if self.allowed_commands.contains(&base_name) {
            PolicyDecision::Allowed
        } else {
            PolicyDecision::Denied(
                format!("Command '{}' is not in the allowed command list. Base name: '{}'.", command, base_name)
            )
        }
    }

    /// Evaluate a path operation against jail boundaries and permissions.
    pub fn evaluate_path_operation(&self, path: &str, jail: &FsJail, _is_write: bool) -> PolicyDecision {
        if path.is_empty() {
            return PolicyDecision::Denied("Path is empty.".into());
        }

        // Trim leading/trailing whitespace and normalize
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return PolicyDecision::Denied("Path is empty after trimming.".into());
        }

        // Check for path traversal components
        let lower = trimmed.to_lowercase();
        if lower.contains("..") || lower.contains("%2e%2e") {
            return PolicyDecision::Denied(
                "Path traversal detected ('..' component in path).".into()
            );
        }

        // Check for null bytes (injection attempt)
        if trimmed.contains('\0') {
            return PolicyDecision::Denied("Path contains null byte (injection attempt).".into());
        }

        // Try to resolve and verify the path is within the jail
        match jail.resolve_and_verify(trimmed) {
            Ok(_resolved) => {
                // Path resolved successfully and is within jail
                PolicyDecision::Allowed
            }
            Err(e) => PolicyDecision::Denied(e),
        }
    }
}

impl Default for PolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Standalone evaluate function (backward compatibility).
/// Evaluates the request against a default policy engine and the filesystem jail.
pub fn evaluate(req: &Request, jail: &FsJail) -> Result<(), String> {
    let engine = PolicyEngine::new();
    match engine.evaluate(req, jail) {
        PolicyDecision::Allowed => Ok(()),
        PolicyDecision::Denied(reason) => Err(reason),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn engine() -> PolicyEngine {
        PolicyEngine::new()
    }

    /// RAII guard: holds the jail plus its temp root, removed on drop so
    /// repeated test runs don't litter the system temp directory.
    struct TestJail {
        jail: FsJail,
        root: PathBuf,
    }

    impl Drop for TestJail {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    /// Build an isolated FsJail over a fresh temp directory.
    fn make_jail() -> TestJail {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "aelvo_policy_test_{}_{}",
            std::process::id(),
            seq
        ));
        std::fs::create_dir_all(&root).expect("create temp workspace");
        let root_str = root.to_str().expect("utf8 temp path");
        let jail = FsJail::new(root_str, root_str).expect("jail construction");
        TestJail { jail, root }
    }

    #[test]
    fn policy_decision_api() {
        assert!(PolicyDecision::Allowed.is_allowed());
        assert_eq!(PolicyDecision::Allowed.reason(), "allowed");
        let denied = PolicyDecision::Denied("nope".to_string());
        assert!(!denied.is_allowed());
        assert_eq!(denied.reason(), "nope");
    }

    #[test]
    fn empty_command_is_denied() {
        assert!(!engine().evaluate_command("").is_allowed());
        assert!(!engine().evaluate_command("   ").is_allowed());
    }

    #[test]
    fn allowlisted_commands_are_allowed() {
        let e = engine();
        for cmd in [
            "echo hello",
            "ls -la",
            "git status",
            "python script.py",
            "curl https://api.example.com",
            "grep -r foo src/",
        ] {
            assert!(e.evaluate_command(cmd).is_allowed(), "expected allowed: {cmd}");
        }
    }

    #[test]
    fn allowlist_is_case_insensitive() {
        let e = engine();
        assert!(e.evaluate_command("ECHO hi").is_allowed());
        assert!(e.evaluate_command("Git status").is_allowed());
    }

    #[test]
    fn unknown_commands_are_denied() {
        let e = engine();
        let d = e.evaluate_command("evilbinary --flag");
        assert!(!d.is_allowed());
        assert!(d.reason().contains("not in the allowed command list"));
    }

    #[test]
    fn blocked_patterns_are_denied() {
        let e = engine();
        for cmd in [
            "rm -rf /",
            "rm -rf --no-preserve-root /etc",
            "mkfs.ext4 /dev/sda1",
            "dd if=/dev/zero of=/dev/sda",
            ":(){ :|:& };:",
            "chmod 777 /etc/passwd",
            "shutdown /r",
        ] {
            let d = e.evaluate_command(cmd);
            assert!(!d.is_allowed(), "expected blocked: {cmd}");
            assert!(
                d.reason().contains("blocked pattern"),
                "reason: {}",
                d.reason()
            );
        }
    }

    #[test]
    fn shell_injection_indicators_are_denied() {
        let e = engine();
        for cmd in [
            "echo $(whoami)",
            "echo `whoami`",
            "cat /etc/passwd; rm -rf /",
            "ls | sh",
            "echo >> /etc/hosts",
        ] {
            let d = e.evaluate_command(cmd);
            assert!(!d.is_allowed(), "expected injection denied: {cmd}");
        }
    }

    #[test]
    fn add_allowed_command_extends_allowlist() {
        let mut e = engine();
        assert!(!e.evaluate_command("mycustomtool --x").is_allowed());
        e.add_allowed_command("mycustomtool");
        assert!(e.evaluate_command("mycustomtool --x").is_allowed());
    }

    #[test]
    fn path_traversal_is_denied() {
        let tj = make_jail();
        let jail = &tj.jail;
        let e = engine();
        for p in ["../etc/passwd", "..\\..\\windows\\system32", "%2e%2e/etc/passwd"] {
            let d = e.evaluate_path_operation(p, &jail, false);
            assert!(!d.is_allowed(), "expected traversal denied: {p}");
        }
    }

    #[test]
    fn empty_and_null_paths_are_denied() {
        let tj = make_jail();
        let jail = &tj.jail;
        let e = engine();
        assert!(!e.evaluate_path_operation("", &jail, false).is_allowed());
        assert!(!e.evaluate_path_operation("  ", &jail, false).is_allowed());
        assert!(!e.evaluate_path_operation("file\0name", &jail, false).is_allowed());
    }

    #[test]
    fn in_jail_path_is_allowed_outside_is_denied() {
        let tj = make_jail();
        let jail = &tj.jail;
        let e = engine();
        // Non-existent in-jail relative path resolves to workspace_root/name.
        assert!(e.evaluate_path_operation("notes.txt", &jail, false).is_allowed());
        // A path in the parent dir is genuinely outside the jail on every
        // platform (unlike a Windows drive path, which is relative on Unix).
        let outside = jail
            .workspace_root()
            .parent()
            .expect("temp root has a parent")
            .join("evil.exe");
        let d = e.evaluate_path_operation(outside.to_str().unwrap(), jail, false);
        assert!(!d.is_allowed(), "outside path must be denied");
    }
}
