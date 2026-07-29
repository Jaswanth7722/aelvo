use std::process::{Command, Stdio, Child};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use std::thread;
use crate::{Request, RunResult, fs_jail::FsJail, audit::Auditor, policy::PolicyEngine};
use crate::resource::{self, Limits, ResourceGovernorImpl, ResourceKind};
use serde_json::json;

// Maximum bytes to collect from stdout/stderr per process (10 MB total)
const MAX_OUTPUT_BYTES: usize = 10 * 1024 * 1024;

// Poll interval for process wait (50ms — balances responsiveness vs CPU)
const POLL_INTERVAL_MS: u64 = 50;

// Interval for checking handle counts (every 500ms)
const HANDLE_CHECK_INTERVAL_MS: u64 = 500;

// Number of consecutive handle limit violations before terminating the process
const HANDLE_VIOLATION_THRESHOLD: u32 = 2;

// ═══════════════════════════════════════════════════════════════════════
// Globally tracked child processes for cleanup on shutdown
// ═══════════════════════════════════════════════════════════════════════

fn child_processes() -> &'static Mutex<Vec<u32>> {
    static PROCESSES: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();
    PROCESSES.get_or_init(|| Mutex::new(Vec::new()))
}

/// Register a child process PID for global cleanup tracking.
pub fn track_child(pid: u32) {
    if let Ok(mut children) = child_processes().lock() {
        children.push(pid);
    }
}

/// Clean up all tracked child processes. Call this on graceful shutdown.
pub fn cleanup_all_children() {
    if let Ok(mut children) = child_processes().lock() {
        let pids: Vec<u32> = children.drain(..).collect();
        for pid in pids {
            kill_process_tree(pid);
        }
    }
}

/// Kill a process and its children. Uses platform-appropriate mechanisms.
fn kill_process_tree(pid: u32) {
    #[cfg(target_os = "windows")]
    {
        // Use taskkill to kill the entire process tree
        let _ = Command::new("taskkill")
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
    }

    #[cfg(not(target_os = "windows"))]
    {
        // Send SIGKILL to the entire process group
        let _ = Command::new("kill")
            .args(["-9", &format!("-{}", pid)])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Process Execution
// ═══════════════════════════════════════════════════════════════════════

/// Execute a shell command with strict timeout enforcement and resource limits.
pub fn bash_exec(
    req: &Request,
    jail: &FsJail,
    auditor: &mut Auditor,
    policy: &PolicyEngine,
) -> Result<RunResult, String> {
    let cmd_str = req.params
        .get("command")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required 'command' parameter".to_string())?;

    if cmd_str.trim().is_empty() {
        return Err("Empty command is not allowed.".into());
    }

    let timeout = req.params
        .get("timeout_seconds")
        .and_then(|v| v.as_u64())
        .unwrap_or(60)
        .min(300); // Hard cap at 5 minutes

    // ── 1. Policy evaluation ────────────────────────────────────────
    let decision = policy.evaluate_command(cmd_str);
    if !decision.is_allowed() {
        auditor.log_command(
            cmd_str,
            &format!("POLICY_DENIED: {}", decision.reason()),
            None,
        );
        return Err(format!(
            "Command blocked by security policy: {}",
            decision.reason()
        ));
    }

    auditor.log_command(cmd_str, "POLICY_ALLOWED", None);

    // ── 2. Create resource governor with appropriate limits ─────────
    let limits = Limits {
        cpu_time_seconds: timeout.min(120), // CPU time cap (mins between timeout and 2 min)
        memory_bytes: 512 * 1024 * 1024,    // 512 MB per process
        max_processes: 10,                   // max 10 concurrent processes
        max_handles: 2000,                   // max 2000 open handles
    };

    let governor = match resource::create_governor(Some(limits)) {
        Ok(g) => g,
        Err(e) => {
            // Resource governor creation failure is non-fatal — log and proceed
            auditor.log_policy_violation("resource_governor", &format!(
                "Failed to create resource governor: {}", e
            ));
            return Err(format!("Resource governor setup failed: {}", e));
        }
    };

    // ── 3. Spawn process ────────────────────────────────────────────
    #[cfg(target_os = "windows")]
    let mut cmd = {
        let mut c = Command::new("cmd");
        c.arg("/C").arg(cmd_str);
        c
    };

    #[cfg(not(target_os = "windows"))]
    let mut cmd = {
        let mut c = Command::new("sh");
        c.arg("-c").arg(cmd_str);
        // Apply rlimits on Unix via pre_exec (called in child before exec)
        let lims = governor.limits().clone();
        unsafe {
            c.pre_exec(move || {
                // Best-effort: setrlimit can fail if limits exceed system hard limits.
                // In a child process there's no meaningful recovery, so we ignore errors.
                let _ = libc::setrlimit(
                    libc::RLIMIT_CPU,
                    &libc::rlimit {
                        rlim_cur: lims.cpu_time_seconds,
                        rlim_max: lims.cpu_time_seconds,
                    },
                );
                let _ = libc::setrlimit(
                    libc::RLIMIT_AS,
                    &libc::rlimit {
                        rlim_cur: lims.memory_bytes,
                        rlim_max: lims.memory_bytes,
                    },
                );
                let _ = libc::setrlimit(
                    libc::RLIMIT_NPROC,
                    &libc::rlimit {
                        rlim_cur: lims.max_processes as u64,
                        rlim_max: lims.max_processes as u64,
                    },
                );
                let _ = libc::setrlimit(
                    libc::RLIMIT_NOFILE,
                    &libc::rlimit {
                        rlim_cur: lims.max_handles as u64,
                        rlim_max: lims.max_handles as u64,
                    },
                );
                Ok(())
            });
        }
        c
    };

    cmd.current_dir(jail.workspace_root())
       .stdout(Stdio::piped())
       .stderr(Stdio::piped());

    let mut child: Child = cmd.spawn()
        .map_err(|e| format!("Failed to spawn process: {}", e))?;

    let child_pid = child.id();
    track_child(child_pid);

    // Assign process to the resource governor (Windows: Job Object; Unix: no-op)
    if let Err(e) = governor.enforce(child_pid) {
        auditor.log_policy_violation("resource_enforce", &format!(
            "Failed to enforce resource limits on PID {}: {}", child_pid, e
        ));
        // Non-fatal — the process still runs but without full jailing
    }

    // ── 4. Wait loop with timeout + resource enforcement ────────────
    let start = Instant::now();
    let limit = Duration::from_secs(timeout);
    let poll_interval = Duration::from_millis(POLL_INTERVAL_MS);
    let handle_check_interval = Duration::from_millis(HANDLE_CHECK_INTERVAL_MS);
    let mut last_handle_check = Instant::now();
    let mut handle_violations = 0u32;

    loop {
        if start.elapsed() > limit {
            // Timeout — kill the process tree
            kill_process_tree(child_pid);
            let _ = child.kill();
            let _ = child.wait();

            auditor.log_command(cmd_str, "TIMEOUT", Some(-1));

            return Err(format!(
                "Process timed out after {} seconds and was terminated.",
                timeout
            ));
        }

        // Periodically check handle/file-descriptor count
        if last_handle_check.elapsed() >= handle_check_interval {
            last_handle_check = Instant::now();
            match governor.check_handle_limit(child_pid) {
                Ok((true, current, max)) => {
                    handle_violations += 1;
                    if handle_violations >= HANDLE_VIOLATION_THRESHOLD {
                        // Exceeded handle limit — terminate the process
                        kill_process_tree(child_pid);
                        let _ = child.kill();
                        let _ = child.wait();

                        auditor.log_resource_limit(
                            ResourceKind::HandleCount,
                            max as u64,
                            current as u64,
                        );
                        auditor.log_command(
                            cmd_str,
                            &format!("HANDLE_LIMIT_EXCEEDED: {} (max {})", current, max),
                            Some(-1),
                        );

                        return Err(format!(
                            "Process terminated: exceeded handle limit ({} > {})",
                            current, max
                        ));
                    }
                }
                Ok((false, _, _)) => {
                    // Handle count is within limits — reset violation counter
                    handle_violations = 0;
                }
                Err(_) => {
                    // Failed to check handle count (process may have exited) — best-effort
                }
            }
        }

        match child.try_wait() {
            Ok(Some(_status)) => {
                // Process finished — collect output with size limit
                let output = child.wait_with_output()
                    .map_err(|e| format!("Failed to collect output: {}", e))?;

                let exit_code = output.status.code().unwrap_or(-1);

                // Detect if process was killed by resource limits
                let exit_reason = if exit_code == -1073740790i32 {
                    // STATUS_PROCESS_IS_TERMINATING — killed by job object limits
                    "RESOURCE_LIMIT"
                } else if exit_code > 128 && exit_code < 160 {
                    // Unix signal exit (128 + signal number)
                    "SIGNAL"
                } else if exit_code == 0 {
                    "SUCCESS"
                } else {
                    "FAILED"
                };

                // Truncate output to prevent memory exhaustion from chatty processes
                let trunc_msg = format!("\n\n[OUTPUT TRUNCATED at {} bytes]", MAX_OUTPUT_BYTES);
                let stdout = if output.stdout.len() > MAX_OUTPUT_BYTES {
                    String::from_utf8_lossy(&output.stdout[..MAX_OUTPUT_BYTES]).to_string()
                        + &trunc_msg
                } else {
                    String::from_utf8_lossy(&output.stdout).to_string()
                };
                let stderr = if output.stderr.len() > MAX_OUTPUT_BYTES {
                    String::from_utf8_lossy(&output.stderr[..MAX_OUTPUT_BYTES]).to_string()
                        + &trunc_msg
                } else {
                    String::from_utf8_lossy(&output.stderr).to_string()
                };

                auditor.log_command(cmd_str, exit_reason, Some(exit_code));

                return Ok(RunResult {
                    data: Some(json!({
                        "stdout": stdout,
                        "stderr": stderr,
                        "exit_code": exit_code,
                    })),
                    logs: format!(
                        "Command completed (exit code: {}).",
                        exit_code
                    ),
                    audit: Some(json!({
                        "exit_code": exit_code,
                        "command_redacted": auditor.redact_string(cmd_str),
                    })),
                });
            }
            Ok(None) => {
                // Still running, sleep briefly
                thread::sleep(poll_interval);
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("Process error: {}", e));
            }
        }
    }
}
