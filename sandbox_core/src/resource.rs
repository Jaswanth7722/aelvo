// ═══════════════════════════════════════════════════════════════════════
// Resource Governor — enforces CPU, memory, file descriptor, and
// process count limits on sandboxed processes.
//
// Windows: Uses Job Objects (CreateJobObjectW / SetInformationJobObject)
// Unix:    Uses setrlimit via pre_exec hooks
// ═══════════════════════════════════════════════════════════════════════

/// Default resource limits applied to every sandboxed process.
pub const DEFAULT_CPU_TIME_SECS: u64 = 30;
pub const DEFAULT_MEMORY_LIMIT_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_PROCESS_LIMIT: u32 = 10;
pub const DEFAULT_HANDLE_LIMIT: u32 = 2000;

/// Named resource limit categories (for audit events).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResourceKind {
    CpuTime,
    Memory,
    ProcessCount,
    HandleCount,
}

impl ResourceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResourceKind::CpuTime => "cpu_time",
            ResourceKind::Memory => "memory",
            ResourceKind::ProcessCount => "process_count",
            ResourceKind::HandleCount => "handle_count",
        }
    }
}

/// Sanity-checked resource limits for a sandbox execution.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Max CPU time per process (in seconds). Caps at 300.
    pub cpu_time_seconds: u64,
    /// Max memory per process (in bytes). Caps at 4 GB.
    pub memory_bytes: u64,
    /// Max concurrent processes in the job group.
    pub max_processes: u32,
    /// Max open handles (file descriptors) per process.
    pub max_handles: u32,
}

impl Limits {
    pub const fn default() -> Self {
        Limits {
            cpu_time_seconds: DEFAULT_CPU_TIME_SECS,
            memory_bytes: DEFAULT_MEMORY_LIMIT_BYTES,
            max_processes: DEFAULT_PROCESS_LIMIT,
            max_handles: DEFAULT_HANDLE_LIMIT,
        }
    }

    /// Clamp values to safe ranges.
    pub fn clamp(&mut self) {
        self.cpu_time_seconds = self.cpu_time_seconds.min(300);
        self.memory_bytes = self.memory_bytes.min(4 * 1024 * 1024 * 1024);
        self.max_processes = self.max_processes.min(100);
        self.max_handles = self.max_handles.min(10000);
    }
}

/// Platform abstraction for resource governance.
pub trait ResourceGovernorImpl: Send {
    /// Apply resource limits to a process identified by PID.
    fn enforce(&self, pid: u32) -> Result<(), String>;
    fn limits(&self) -> &Limits;
    fn check_handle_limit(&self, pid: u32) -> Result<(bool, u32, u32), String>;
}

// ─────────────────────────────────────────────────────────────────────
// Windows implementation via raw FFI (kernel32)
// ─────────────────────────────────────────────────────────────────────

#[cfg(target_os = "windows")]
#[allow(
    non_camel_case_types,
    non_snake_case,
    dead_code
)]
mod win_impl {
    use super::*;
    use std::ffi::c_void;
    use std::ptr;

    // ── Windows type aliases ────────────────────────────────────────
    type HANDLE = *mut c_void;
    type BOOL = i32;
    type DWORD = u32;
    type LPCWSTR = *const u16;
    type LPVOID = *mut c_void;
    type LPDWORD = *mut u32;
    type ULONG_PTR = usize;
    type SIZE_T = usize;

    const FALSE: BOOL = 0;
    const TRUE: BOOL = 1;

    // ── Process access rights ───────────────────────────────────────
    const PROCESS_TERMINATE: DWORD = 0x0001;
    const PROCESS_QUERY_INFORMATION: DWORD = 0x0400;
    const PROCESS_SET_QUOTA: DWORD = 0x0100;

    // ── JOB_OBJECT_LIMIT flags ──────────────────────────────────────
    const JOB_OBJECT_LIMIT_ACTIVE_PROCESS: DWORD = 0x00000008;
    const JOB_OBJECT_LIMIT_PROCESS_TIME: DWORD = 0x00000002;
    const JOB_OBJECT_LIMIT_PROCESS_MEMORY: DWORD = 0x00000100;
    const JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION: DWORD = 0x00000400;

    // ── JobObjectInfoClass ──────────────────────────────────────────
    const JOB_OBJECT_INFO_CLASS_EXTENDED_LIMITS: DWORD = 9;

    // ── Windows API structs (repr(C) to match C ABI) ────────────────

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct JOBOBJECT_BASIC_LIMIT_INFORMATION {
        PerProcessUserTimeLimit: i64,
        PerJobUserTimeLimit: i64,
        LimitFlags: DWORD,
        MinimumWorkingSetSize: SIZE_T,
        MaximumWorkingSetSize: SIZE_T,
        ActiveProcessLimit: DWORD,
        Affinity: ULONG_PTR,
        PriorityClass: DWORD,
        SchedulingClass: DWORD,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct IO_COUNTERS {
        ReadOperationCount: u64,
        WriteOperationCount: u64,
        OtherOperationCount: u64,
        ReadTransferCount: u64,
        WriteTransferCount: u64,
        OtherTransferCount: u64,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION {
        BasicLimitInformation: JOBOBJECT_BASIC_LIMIT_INFORMATION,
        IoInfo: IO_COUNTERS,
        ProcessMemoryLimit: SIZE_T,
        JobMemoryLimit: SIZE_T,
        PeakProcessMemoryUsed: SIZE_T,
        PeakJobMemoryUsed: SIZE_T,
    }

    // ── FFI function declarations ───────────────────────────────────

    #[link(name = "kernel32")]
    extern "system" {
        fn CreateJobObjectW(
            lpJobAttributes: *const c_void,
            lpName: LPCWSTR,
        ) -> HANDLE;

        fn SetInformationJobObject(
            hJob: HANDLE,
            JobObjectInfoClass: DWORD,
            lpJobObjectInfo: LPVOID,
            cbJobObjectInfoLength: DWORD,
        ) -> BOOL;

        fn AssignProcessToJobObject(
            hJob: HANDLE,
            hProcess: HANDLE,
        ) -> BOOL;

        fn OpenProcess(
            dwDesiredAccess: DWORD,
            bInheritHandle: BOOL,
            dwProcessId: DWORD,
        ) -> HANDLE;

        fn CloseHandle(
            hObject: HANDLE,
        ) -> BOOL;

        fn GetProcessHandleCount(
            hProcess: HANDLE,
            pdwHandleCount: LPDWORD,
        ) -> BOOL;
    }

    // ── Windows ResourceGovernor ────────────────────────────────────

    pub struct WindowsResourceGovernor {
        job_handle: HANDLE,
        limits: Limits,
    }

    // SAFETY: HANDLE is a raw pointer that can be sent across threads.
    unsafe impl Send for WindowsResourceGovernor {}

    impl WindowsResourceGovernor {
        pub fn new(limits: Limits) -> Result<Self, String> {
            unsafe {
                let job_handle = CreateJobObjectW(ptr::null(), ptr::null());
                if job_handle.is_null() {
                    return Err(
                        "CreateJobObjectW failed: could not create Windows Job Object".into(),
                    );
                }

                let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION =
                    std::mem::zeroed();

                // Per-process CPU time limit (in 100-ns intervals)
                info.BasicLimitInformation.PerProcessUserTimeLimit =
                    (limits.cpu_time_seconds as i64) * 10_000_000;

                // Per-process memory limit
                info.ProcessMemoryLimit = limits.memory_bytes as SIZE_T;

                // Active process limit
                info.BasicLimitInformation.ActiveProcessLimit = limits.max_processes;

                // Combine limit flags
                info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_ACTIVE_PROCESS
                    | JOB_OBJECT_LIMIT_PROCESS_TIME
                    | JOB_OBJECT_LIMIT_PROCESS_MEMORY
                    | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION;

                let result = SetInformationJobObject(
                    job_handle,
                    JOB_OBJECT_INFO_CLASS_EXTENDED_LIMITS,
                    &info as *const _ as LPVOID,
                    std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as DWORD,
                );

                if result == FALSE {
                    CloseHandle(job_handle);
                    return Err(
                        "SetInformationJobObject failed: could not apply resource limits"
                            .into(),
                    );
                }

                Ok(WindowsResourceGovernor { job_handle, limits })
            }
        }
    }

    impl ResourceGovernorImpl for WindowsResourceGovernor {
        fn enforce(&self, pid: u32) -> Result<(), String> {
            unsafe {
                let process_handle = OpenProcess(
                    PROCESS_SET_QUOTA | PROCESS_TERMINATE | PROCESS_QUERY_INFORMATION,
                    FALSE,
                    pid,
                );

                if process_handle.is_null() {
                    return Err(format!(
                        "OpenProcess failed: could not open handle for PID {}",
                        pid
                    ));
                }

                let result = AssignProcessToJobObject(self.job_handle, process_handle);
                CloseHandle(process_handle);

                if result == FALSE {
                    return Err(format!(
                        "AssignProcessToJobObject failed: process {} may already be in \
                         another job or has terminated",
                        pid
                    ));
                }

                Ok(())
            }
        }

        fn limits(&self) -> &Limits {
            &self.limits
        }

        fn check_handle_limit(&self, pid: u32) -> Result<(bool, u32, u32), String> {
            unsafe {
                let process_handle = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, pid);
                if process_handle.is_null() {
                    return Ok((false, 0, self.limits.max_handles));
                }

                let mut handle_count: DWORD = 0;
                let result =
                    GetProcessHandleCount(process_handle, &mut handle_count as LPDWORD);
                CloseHandle(process_handle);

                if result == FALSE {
                    return Ok((false, 0, self.limits.max_handles));
                }

                Ok((
                    handle_count > self.limits.max_handles,
                    handle_count,
                    self.limits.max_handles,
                ))
            }
        }
    }

    impl Drop for WindowsResourceGovernor {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.job_handle);
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────
// Unix implementation via setrlimit — limits applied in pre_exec hook
// ─────────────────────────────────────────────────────────────────────

#[cfg(not(target_os = "windows"))]
mod unix_impl {
    use super::*;

    pub struct UnixResourceGovernor {
        limits: Limits,
    }

    unsafe impl Send for UnixResourceGovernor {}

    impl UnixResourceGovernor {
        pub fn new(limits: Limits) -> Result<Self, String> {
            Ok(UnixResourceGovernor { limits })
        }
    }

    impl ResourceGovernorImpl for UnixResourceGovernor {
        fn enforce(&self, _pid: u32) -> Result<(), String> {
            // setrlimit applied in pre_exec hook in process.rs — no-op here
            Ok(())
        }

        fn limits(&self) -> &Limits {
            &self.limits
        }

        fn check_handle_limit(&self, _pid: u32) -> Result<(bool, u32, u32), String> {
            // RLIMIT_NOFILE is enforced at the kernel level on Unix — no polling needed
            Ok((false, 0, self.limits.max_handles))
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Export the correct implementation for the current platform
// ═══════════════════════════════════════════════════════════════════════

#[cfg(target_os = "windows")]
pub use win_impl::WindowsResourceGovernor as PlatformResourceGovernor;

#[cfg(not(target_os = "windows"))]
pub use unix_impl::UnixResourceGovernor as PlatformResourceGovernor;

/// Create a new resource governor with the provided limits.
pub fn create_governor(limits: Option<Limits>) -> Result<PlatformResourceGovernor, String> {
    let mut limits = limits.unwrap_or_else(Limits::default);
    limits.clamp();

    #[cfg(target_os = "windows")]
    {
        PlatformResourceGovernor::new(limits)
    }

    #[cfg(not(target_os = "windows"))]
    {
        PlatformResourceGovernor::new(limits)
    }
}

/// Diagnose whether a process exit code indicates a resource limit violation.
/// Used by process.rs to detect if a job object killed the process.
#[allow(dead_code)]
pub fn diagnose_limit_violation(exit_code: Option<i32>) -> Option<ResourceKind> {
    if let Some(code) = exit_code {
        // 0xC000010A = STATUS_PROCESS_IS_TERMINATING (killed by job object)
        if code == -1073740790i32 || code == 3 {
            return Some(ResourceKind::CpuTime);
        }
    }
    None
}
