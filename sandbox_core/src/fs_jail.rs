use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::fs;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic counter used to make atomic-write temp filenames unique.
static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);
use crate::{Request, RunResult};
use serde_json::json;

/// Cross-platform unique file identifier.
///
/// On Unix: (st_dev, st_ino) from `MetadataExt` (stable).
/// On Windows: (volume_serial_number, file_index_combined) from
/// `GetFileInformationByHandle` via raw FFI.
///
/// This pair uniquely identifies a file across the entire system.
/// Hard links share the same (device, inode) pair, while symlinks
/// do not (each symlink is its own inode).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FileId(u64, u64);

/// Get the unique file identifier for a path on Unix.
#[cfg(unix)]
fn get_file_id(path: &Path) -> Result<FileId, String> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::metadata(path)
        .map_err(|e| format!("Failed to get metadata for '{}': {}", path.display(), e))?;
    Ok(FileId(meta.dev(), meta.ino()))
}

/// Get the unique file identifier for a path on Windows.
/// Uses raw FFI to `GetFileInformationByHandle` from `kernel32.dll`.
#[cfg(windows)]
fn get_file_id(path: &Path) -> Result<FileId, String> {
    use std::os::windows::ffi::OsStrExt;
    #[repr(C)]
    struct BY_HANDLE_FILE_INFORMATION {
        file_attributes: u32,
        creation_time_low: u32,
        creation_time_high: u32,
        last_access_time_low: u32,
        last_access_time_high: u32,
        last_write_time_low: u32,
        last_write_time_high: u32,
        volume_serial_number: u32,
        file_size_high: u32,
        file_size_low: u32,
        n_number_of_links: u32,
        file_index_high: u32,
        file_index_low: u32,
    }

    extern "system" {
        fn GetFileInformationByHandle(
            h_file: isize,
            lp_file_information: *mut BY_HANDLE_FILE_INFORMATION,
        ) -> i32;

        fn CreateFileW(
            lp_file_name: *const u16,
            dw_desired_access: u32,
            dw_share_mode: u32,
            lp_security_attributes: *mut std::ffi::c_void,
            dw_creation_disposition: u32,
            dw_flags_and_attributes: u32,
            h_template_file: isize,
        ) -> isize;

        fn CloseHandle(h_object: isize) -> i32;
    }

    const GENERIC_READ: u32 = 0x80000000;
    const FILE_SHARE_READ: u32 = 0x00000001;
    const FILE_SHARE_WRITE: u32 = 0x00000002;
    const OPEN_EXISTING: u32 = 3;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;
    const INVALID_HANDLE_VALUE: isize = -1;

    // Convert path to wide (UTF-16) for Windows API
    let path_str = path.as_os_str().to_str()
        .ok_or_else(|| format!("Non-UTF-8 path: {}", path.display()))?;

    let wide: Vec<u16> = std::ffi::OsStr::new(path_str)
        .encode_wide()
        .chain(Some(0))
        .collect();

    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null_mut(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            0,
        )
    };

    if handle == INVALID_HANDLE_VALUE {
        return Err(format!(
            "Failed to open file for identity check: {}",
            path.display()
        ));
    }

    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    let result = unsafe {
        GetFileInformationByHandle(handle, &mut info)
    };

    unsafe { CloseHandle(handle); }

    if result == 0 {
        return Err(format!(
            "GetFileInformationByHandle failed for: {}",
            path.display()
        ));
    }

    // Combine the 64-bit file index from two 32-bit halves
    let file_index = ((info.file_index_high as u64) << 32) | (info.file_index_low as u64);
    Ok(FileId(info.volume_serial_number as u64, file_index))
}

/// Name of the persisted inode whitelist file (stored in workspace root).
const INODE_WHITELIST_FILE: &str = ".aelvo_inode_whitelist";

/// Directories that inode whitelist walk will skip (for performance).
const INODE_WALK_IGNORED_DIRS: &[&str] = &[
    ".git", "__pycache__", "chroma_db", "backups", "node_modules",
    ".venv", "dist", "target",
];

/// Load the inode whitelist from the persisted file on disk.
/// Returns `None` if the file doesn't exist or is corrupted.
fn load_whitelist_from_disk(workspace_root: &Path) -> Option<HashSet<FileId>> {
    let path = workspace_root.join(INODE_WHITELIST_FILE);
    if !path.exists() {
        return None;
    }
    let content = fs::read_to_string(&path).ok()?;
    let arr: Vec<[u64; 2]> = serde_json::from_str(&content).ok()?;
    Some(arr.into_iter().map(|[dev, ino]| FileId(dev, ino)).collect())
}

/// Save the inode whitelist to disk atomically (write to temp, then rename).
fn save_whitelist_to_disk(workspace_root: &Path, whitelist: &HashSet<FileId>) -> Result<(), String> {
    let path = workspace_root.join(INODE_WHITELIST_FILE);
    let arr: Vec<[u64; 2]> = whitelist.iter().map(|fid| [fid.0, fid.1]).collect();
    let json = serde_json::to_string(&arr)
        .map_err(|e| format!("Failed to serialize inode whitelist: {}", e))?;

    // Atomic write: write to temp, then rename over the real file
    let temp_ext = format!(
        ".aelvo_tmp_{}_{}",
        std::process::id(),
        TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let temp_path = path.with_extension(temp_ext);
    fs::write(&temp_path, &json)
        .map_err(|e| format!("Failed to write inode whitelist temp file: {}", e))?;
    fs::rename(&temp_path, &path)
        .map_err(|e| format!("Failed to finalize inode whitelist file: {}", e))?;
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
// Filesystem Jail — production-grade path isolation
// ═══════════════════════════════════════════════════════════════════════

/// Security-hardened filesystem jail that enforces workspace boundaries.
/// All paths must be resolved and verified to be within the workspace root.
///
/// In addition to path-based checks, the jail maintains an inode whitelist
/// populated at construction time. Any file whose inode is NOT in the
/// whitelist is blocked — this detects hard links pointing outside the
/// workspace (hard links share inodes but bypass path checks).
pub struct FsJail {
    /// Canonical (real) path to the workspace root.
    workspace_root: PathBuf,
    /// Canonical path to the repo root (may be same as workspace_root).
    _repo_root: PathBuf,
    /// Set of known file (device, inode) pairs within the workspace.
    /// Populated at init time by walking the workspace tree.
    /// Updated after atomic writes create new files.
    inode_whitelist: Mutex<HashSet<FileId>>,
}

impl FsJail {
    /// Create a new filesystem jail with the given workspace and repo roots.
    ///
    /// Both paths are canonicalized immediately. If canonicalization fails,
    /// the jail construction fails — we never fall back to an unverified path.
    ///
    /// After construction, the inode whitelist is populated by walking the
    /// workspace tree. All existing files' (device, inode) pairs are recorded
    /// so that hard links to outside files can be detected later.
    pub fn new(workspace_root: &str, repo_root: &str) -> Result<Self, String> {
        let ws = Path::new(workspace_root)
            .canonicalize()
            .map_err(|e| format!(
                "Failed to canonicalize workspace root '{}': {}",
                workspace_root, e
            ))?;

        let rr = Path::new(repo_root)
            .canonicalize()
            .map_err(|e| format!(
                "Failed to canonicalize repo root '{}': {}",
                repo_root, e
            ))?;

        // Try loading the inode whitelist from disk first. If the file
        // exists (from a previous sandbox initialization), use it.
        // Otherwise, walk the workspace to build it and persist to disk.
        let whitelist = match load_whitelist_from_disk(&ws) {
            Some(loaded) => {
                // Re-use the persisted whitelist (created at first init)
                loaded
            }
            None => {
                // First initialization: walk workspace and persist
                let mut fresh = HashSet::new();
                let tmp_jail = FsJail {
                    workspace_root: ws.clone(),
                    _repo_root: rr.clone(),
                    inode_whitelist: Mutex::new(HashSet::new()),
                };
                tmp_jail.walk_and_collect_inodes(&ws, &mut fresh, 0)?;
                save_whitelist_to_disk(&ws, &fresh)?;
                fresh
            }
        };

        let jail = FsJail {
            workspace_root: ws,
            _repo_root: rr,
            inode_whitelist: Mutex::new(whitelist),
        };

        Ok(jail)
    }

    /// Return a reference to the canonical workspace root.
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Recursively walk a directory and collect all file inodes.
    fn walk_and_collect_inodes(
        &self,
        dir: &Path,
        whitelist: &mut HashSet<FileId>,
        depth: usize,
    ) -> Result<(), String> {
        // Safety guard: max 20 levels deep to prevent infinite recursion
        if depth > 20 {
            return Ok(());
        }

        // Ensure we stay within the jail boundary
        if !dir.starts_with(&self.workspace_root) {
            return Ok(());
        }

        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return Ok(()), // Skip unreadable directories
        };

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let name = entry.file_name().to_string_lossy().to_string();

            // Skip large ignored directories for performance
            if INODE_WALK_IGNORED_DIRS.contains(&name.as_str()) {
                continue;
            }

            let path = entry.path();

            // Use symlink_metadata to get the inode of the symlink itself
            // (not the target). For symlinks, the target will be resolved
            // by resolve_and_verify's canonicalize() call separately.
            let ft = match entry.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };

            if ft.is_dir() {
                // Recurse into subdirectories
                let _ = self.walk_and_collect_inodes(&path, whitelist, depth + 1);
            } else if ft.is_symlink() {
                // For symlinks, follow the link and record the target's inode
                if let Ok(meta) = fs::metadata(&path) {
                    if meta.is_file() {
                        if let Ok(fid) = get_file_id(&path) {
                            whitelist.insert(fid);
                        }
                    }
                }
            } else if ft.is_file() {
                // Regular file — record its inode
                if let Ok(fid) = get_file_id(&path) {
                    whitelist.insert(fid);
                }
            }
        }

        Ok(())
    }

    /// Add a file identity to the inode whitelist.
    ///
    /// Called after atomic writes to register the new file's inode.
    /// Updates both the in-memory HashSet AND the persisted file on disk.
    pub fn add_file_to_whitelist(&self, path: &Path) {
        if let Ok(fid) = get_file_id(path) {
            if let Ok(mut whitelist) = self.inode_whitelist.lock() {
                whitelist.insert(fid);
                // Persist to disk so subsequent sandbox invocations see it
                let _ = save_whitelist_to_disk(&self.workspace_root, &whitelist);
            }
        }
    }

    /// Resolve a user-supplied path SAFELY and verify it is within the jail.
    ///
    /// Security checks performed:
    /// 1. Reject null bytes and empty paths
    /// 2. Resolve all symlinks to their real targets
    /// 3. Normalize path components (handle `..`, `.`)
    /// 4. Verify the final canonical path is a strict descendant of workspace_root
    /// 5. **Inode-level check**: if the resolved path exists as a file, verify
    ///    its (device, inode) pair is in the workspace whitelist. This detects
    ///    hard links that point outside the workspace (same inode, different
    ///    directory entry, not caught by path-based checks).
    ///
    /// Returns the fully canonicalized path on success, or an error describing
    /// the security violation.
    pub fn resolve_and_verify(&self, requested_path: &str) -> Result<PathBuf, String> {
        // ── Pre-checks ──────────────────────────────────────────────
        if requested_path.is_empty() {
            return Err("Path is empty.".into());
        }

        // Null byte injection prevention
        if requested_path.contains('\0') {
            return Err("Path contains null byte (injection attempt).".into());
        }

        // URL-encoded path traversal detection
        let lower = requested_path.to_lowercase();
        if lower.contains("%2e%2e") || lower.contains("..%2f") || lower.contains("..%5c")
            || lower.contains("%252e%252e") || lower.contains("..%252f")
            || lower.contains("..%255c") {
            return Err("URL-encoded path traversal detected.".into());
        }

        // ── Build an absolute candidate path ────────────────────────
        let path = Path::new(requested_path);
        let candidate = if path.is_absolute() {
            // Strip Windows extended-length prefix \\?\\ if present
            let path_str = path.to_string_lossy();
            if cfg!(windows) && path_str.starts_with("\\\\?\\") {
                PathBuf::from(&path_str[4..])
            } else {
                path.to_path_buf()
            }
        } else {
            self.workspace_root.join(path)
        };

        // ── Symlink-aware resolution ────────────────────────────────
        // Use canonicalize() which resolves all symlinks and normalizes
        // path components (.., .). This is the security-critical step.
        // For non-existent paths (e.g., writing a new file), we resolve
        // the nearest existing ancestor and append the remaining components.
        let resolved = match candidate.canonicalize() {
            Ok(p) => p,
            Err(_) => {
                // Path doesn't exist yet — resolve nearest ancestor
                // Walk up from the candidate until we find an existing path,
                // collecting non-existent components to reconstruct later.
                let mut current = candidate.as_path();
                let mut tail: Vec<String> = Vec::new();

                let path_or_err: Result<PathBuf, String> = loop {
                    match current.canonicalize() {
                        Ok(base) => {
                            // Reconstruct the full path from base + tail
                            let mut full = base.to_path_buf();
                            for name in tail.iter().rev() {
                                full.push(name);
                            }
                            break Ok(full);
                        }
                        Err(_) => {
                            // Walk up: pop the last component
                            match current.parent() {
                                Some(parent) => {
                                    if let Some(name) = current.file_name() {
                                        let name_str = name.to_string_lossy().to_string();
                                        // Reject '..' components in non-existent path segments
                                        // to prevent traversal via Path::starts_with bypass
                                        if name_str == ".." {
                                            break Err(format!(
                                                "Path traversal detected: '..' component in non-existent path segment '{}'",
                                                requested_path
                                            ));
                                        }
                                        tail.push(name_str);
                                    }
                                    current = parent;
                                }
                                None => {
                                    break Err(format!(
                                        "Failed to resolve path '{}': no existing ancestor found",
                                        requested_path
                                    ));
                                }
                            }
                        }
                    }
                };

                match path_or_err {
                    Ok(path) => path,
                    Err(e) => return Err(e),
                }
            }
        };

        // ── Boundary enforcement ────────────────────────────────────
        // The resolved path MUST be a strict descendant of workspace_root.
        // We use canonical path comparison to prevent symlink escapes.
        if !resolved.starts_with(&self.workspace_root) {
            return Err(format!(
                "Jail boundary traversal blocked: path '{}' resolves to '{}' which is outside workspace '{}'",
                requested_path,
                resolved.display(),
                self.workspace_root.display()
            ));
        }

        // ── Inode-level check for hard link detection ───────────────
        // Path-based checks can be bypassed by hard links (same inode,
        // different directory entry, not resolved by canonicalize()).
        // If the file exists and is a regular file, verify its
        // unique file identity is in the workspace whitelist.
        if resolved.exists() && resolved.is_file() {
            let fid = get_file_id(&resolved)
                .map_err(|e| format!(
                    "Failed to get file identity for '{}': {}",
                    resolved.display(), e
                ))?;

            let whitelist = self.inode_whitelist.lock()
                .map_err(|e| format!("Inode whitelist lock poisoned: {}", e))?;

            if !whitelist.contains(&fid) {
                return Err(format!(
                    concat!(
                        "Hard link attack blocked: '{}' is not a known workspace file. ",
                        "This file appears to be a hard link to a file outside the sandbox boundary."
                    ),
                    requested_path
                ));
            }
        }

        Ok(resolved)
    }

    /// Check if a path is a file within the jail.
    pub fn is_file(&self, path: &Path) -> bool {
        path.starts_with(&self.workspace_root) && path.is_file()
    }

    /// Check if the given resolved path is the workspace root itself.
    pub fn is_root(&self, path: &Path) -> bool {
        path == &self.workspace_root
    }
}

// ═══════════════════════════════════════════════════════════════════════
// File Operation Helpers
// ═══════════════════════════════════════════════════════════════════════

/// Extract a validated path from request params and resolve it through the jail.
pub fn extract_path<'a>(req: &'a Request, jail: &FsJail) -> Result<(PathBuf, &'a str), String> {
    let path_str = req.params
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required 'path' parameter".to_string())?;

    let safe_path = jail.resolve_and_verify(path_str)?;
    Ok((safe_path, path_str))
}

// ═══════════════════════════════════════════════════════════════════════
// Public API — filesystem operations
// ═══════════════════════════════════════════════════════════════════════

pub fn read_file(req: &Request, jail: &FsJail) -> Result<RunResult, String> {
    let (safe_path, _original) = extract_path(req, jail)?;

    if !safe_path.is_file() {
        return Err(format!("Not a file or does not exist: {}", safe_path.display()));
    }

    // Note: resolve_and_verify() already canonicalizes the path via
    // std::fs::canonicalize(), which resolves all symlinks. The returned
    // path is guaranteed to be within the jail boundary.
    let content = fs::read_to_string(&safe_path)
        .map_err(|e| format!("Failed to read file '{}': {}", safe_path.display(), e))?;

    Ok(RunResult {
        data: Some(json!({"content": content})),
        logs: format!("Read file: {:?}", safe_path),
        audit: Some(json!({
            "resolved_path": safe_path.to_string_lossy(),
            "size_bytes": content.len(),
        })),
    })
}

pub fn read_range(req: &Request, jail: &FsJail) -> Result<RunResult, String> {
    let (safe_path, _original) = extract_path(req, jail)?;

    let start = req.params
        .get("start_line")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as usize;
    let end = req.params
        .get("end_line")
        .and_then(|v| v.as_u64())
        .unwrap_or(usize::MAX as u64) as usize;

    if !safe_path.is_file() {
        return Err(format!("Not a file or does not exist: {}", safe_path.display()));
    }

    let content = fs::read_to_string(&safe_path)
        .map_err(|e| format!("Failed to read file '{}': {}", safe_path.display(), e))?;

    let lines: Vec<&str> = content.lines().collect();
    if lines.is_empty() {
        return Ok(RunResult {
            data: Some(json!({"content": ""})),
            logs: format!("Read file range (empty file): {:?}", safe_path),
            audit: Some(json!({"resolved_path": safe_path.to_string_lossy()})),
        });
    }

    let start_idx = start.saturating_sub(1).min(lines.len() - 1);
    let end_idx = end.min(lines.len());

    if start_idx >= end_idx {
        return Err(format!(
            "Invalid line range: start={} must be less than end={} (file has {} lines)",
            start, end, lines.len()
        ));
    }

    let sliced = lines[start_idx..end_idx].join("\n");

    Ok(RunResult {
        data: Some(json!({"content": sliced, "start_line": start_idx + 1, "end_line": end_idx})),
        logs: format!("Read file range {:?} (lines {}-{})", safe_path, start_idx + 1, end_idx),
        audit: Some(json!({
            "resolved_path": safe_path.to_string_lossy(),
            "start_line": start_idx + 1,
            "end_line": end_idx,
        })),
    })
}

pub fn write_file(req: &Request, jail: &FsJail) -> Result<RunResult, String> {
    if !req.write_mode {
        return Err("Write mode is disabled by the caller.".to_string());
    }

    let (safe_path, _original) = extract_path(req, jail)?;
    let content = req.params
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // Create parent directories if they don't exist
    if let Some(parent) = safe_path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create parent directories for '{}': {}", safe_path.display(), e))?;
        }
    }

    // Atomic write: write to a temp file first, then rename
    let temp_ext = format!(
        ".aelvo_tmp_{}_{}",
        std::process::id(),
        TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let temp_path = safe_path.with_extension(
        format!("{}{}", safe_path.extension().unwrap_or_default().to_string_lossy(), temp_ext)
    );

    fs::write(&temp_path, content)
        .map_err(|e| format!("Failed to write file '{}': {}", safe_path.display(), e))?;

    fs::rename(&temp_path, &safe_path)
        .map_err(|e| format!("Failed to finalize write to '{}': {}", safe_path.display(), e))?;

    // Register the NEW inode in the whitelist (atomic write replaces the
    // directory entry with a new inode via fs::rename)
    jail.add_file_to_whitelist(&safe_path);

    Ok(RunResult {
        data: Some(json!({"path": safe_path.to_string_lossy()})),
        logs: format!("Wrote file: {:?}", safe_path),
        audit: Some(json!({
            "resolved_path": safe_path.to_string_lossy(),
            "size_bytes": content.len(),
        })),
    })
}

pub fn edit_file_block(req: &Request, jail: &FsJail) -> Result<RunResult, String> {
    if !req.write_mode {
        return Err("Write mode is disabled by the caller.".to_string());
    }

    let (safe_path, _original) = extract_path(req, jail)?;

    let target = req.params
        .get("old_block")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required 'old_block' parameter".to_string())?;

    let replacement = req.params
        .get("new_block")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required 'new_block' parameter".to_string())?;

    if target.is_empty() {
        return Err("old_block parameter cannot be empty.".into());
    }

    if !safe_path.is_file() {
        return Err(format!("Not a file or does not exist: {}", safe_path.display()));
    }

    let content = fs::read_to_string(&safe_path)
        .map_err(|e| format!("Failed to read file '{}': {}", safe_path.display(), e))?;

    if !content.contains(target) {
        return Err(format!(
            "The specified old_block was not found in '{}'",
            safe_path.display()
        ));
    }

    let new_content = content.replace(target, replacement);

    fs::write(&safe_path, new_content)
        .map_err(|e| format!("Failed to write file '{}': {}", safe_path.display(), e))?;

    Ok(RunResult {
        data: Some(json!({"path": safe_path.to_string_lossy()})),
        logs: format!("Replaced content in file: {:?}", safe_path),
        audit: Some(json!({
            "resolved_path": safe_path.to_string_lossy(),
            "replacement_made": true,
        })),
    })
}

pub fn delete_file(req: &Request, jail: &FsJail) -> Result<RunResult, String> {
    if !req.write_mode {
        return Err("Write mode is disabled by the caller.".to_string());
    }

    let (safe_path, _original) = extract_path(req, jail)?;

    if jail.is_root(&safe_path) {
        return Err("Cannot delete the workspace root directory.".into());
    }

    if !safe_path.exists() {
        return Err(format!("File does not exist: {}", safe_path.display()));
    }

    if safe_path.is_dir() {
        fs::remove_dir_all(&safe_path)
            .map_err(|e| format!("Failed to delete directory '{}': {}", safe_path.display(), e))?;
    } else {
        fs::remove_file(&safe_path)
            .map_err(|e| format!("Failed to delete file '{}': {}", safe_path.display(), e))?;
    }

    Ok(RunResult {
        data: Some(json!({"path": safe_path.to_string_lossy()})),
        logs: format!("Deleted: {:?}", safe_path),
        audit: Some(json!({"resolved_path": safe_path.to_string_lossy()})),
    })
}

pub fn resolve_path(req: &Request, jail: &FsJail) -> Result<RunResult, String> {
    let (safe_path, _original) = extract_path(req, jail)?;

    Ok(RunResult {
        data: Some(json!({
            "resolved_path": safe_path.to_string_lossy(),
            "exists": safe_path.exists(),
            "is_file": safe_path.is_file(),
            "is_dir": safe_path.is_dir(),
        })),
        logs: format!("Resolved path: {:?}", safe_path),
        audit: Some(json!({"resolved_path": safe_path.to_string_lossy()})),
    })
}

pub fn list_directory(req: &Request, jail: &FsJail) -> Result<RunResult, String> {
    let (safe_path, _original) = extract_path(req, jail)?;

    if !safe_path.is_dir() {
        return Err(format!("Not a directory: {}", safe_path.display()));
    }

    let entries: Vec<String> = fs::read_dir(&safe_path)
        .map_err(|e| format!("Failed to read directory '{}': {}", safe_path.display(), e))?
        .filter_map(|entry| {
            entry.ok().map(|e| e.file_name().to_string_lossy().to_string())
        })
        .collect();

    Ok(RunResult {
        data: Some(json!({
            "path": safe_path.to_string_lossy(),
            "entries": entries,
            "count": entries.len(),
        })),
        logs: format!("Listed directory: {:?} ({} entries)", safe_path, entries.len()),
        audit: Some(json!({"resolved_path": safe_path.to_string_lossy()})),
    })
}

// ═══════════════════════════════════════════════════════════════════════
// Code Search & File Discovery — all go through jail boundary
// ═══════════════════════════════════════════════════════════════════════

/// Source file extensions that search_code will scan.
const SOURCE_EXTENSIONS: &[&str] = &[
    "py", "ts", "tsx", "js", "jsx", "json", "md", "yaml", "yml",
    "toml", "rs", "go", "html", "css", "sql",
];

/// Maximum file size (in bytes) that search_code will read (10 MB).
const MAX_SEARCH_FILE_SIZE: u64 = 10 * 1024 * 1024;

/// Directories that search_code and find_files will skip.
const IGNORED_DIRS: &[&str] = &[
    ".git", "__pycache__", "chroma_db", "backups", "node_modules",
    ".venv", "dist", "target",
];

/// Recursively walk a directory within the jail, returning relative paths of files.
/// Respects IGNORED_DIRS and max_results.
fn walk_workspace(
    jail: &FsJail,
    base: &Path,
    prefix: &str,
    max_results: usize,
    results: &mut Vec<String>,
) -> Result<(), String> {
    if !base.starts_with(jail.workspace_root()) {
        return Ok(()); // Safety: skip paths outside jail
    }

    let entries = fs::read_dir(base)
        .map_err(|e| format!("Failed to read directory '{}': {}", base.display(), e))?;

    for entry in entries {
        if results.len() >= max_results {
            break;
        }
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        let name = entry.file_name().to_string_lossy().to_string();

        // Skip ignored directories
        if IGNORED_DIRS.contains(&name.as_str()) {
            continue;
        }

        let rel_path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{}/{}", prefix, name)
        };

        let ft = entry.file_type().map_err(|e| format!("Failed to get file type: {}", e))?;
        if ft.is_dir() {
            walk_workspace(jail, &entry.path(), &rel_path, max_results, results)?;
        } else {
            results.push(rel_path);
        }
    }
    Ok(())
}

/// Grep a single file within the jail with a regex pattern.
pub fn grep_file(req: &Request, jail: &FsJail) -> Result<RunResult, String> {
    let (safe_path, _original) = extract_path(req, jail)?;

    if !safe_path.is_file() {
        return Err(format!("Not a file or does not exist: {}", safe_path.display()));
    }

    let pattern = req.params
        .get("pattern")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required 'pattern' parameter".to_string())?;

    let case_sensitive = req.params
        .get("case_sensitive")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let max_matches = req.params
        .get("max_matches")
        .and_then(|v| v.as_u64())
        .unwrap_or(100) as usize;

    let content = fs::read_to_string(&safe_path)
        .map_err(|e| format!("Failed to read file '{}': {}", safe_path.display(), e))?;

    let mut matches: Vec<serde_json::Value> = Vec::new();
    // Build regex with optional case-insensitive flag
    let pattern_str = if case_sensitive {
        pattern.to_string()
    } else {
        format!(r"(?i){}", pattern)
    };

    match regex::Regex::new(&pattern_str) {
        Ok(re) => {
            for (line_no, line) in content.lines().enumerate() {
                if matches.len() >= max_matches {
                    break;
                }
                if re.is_match(line) {
                    matches.push(json!({
                        "line": line_no + 1,
                        "text": line,
                    }));
                }
            }
        }
        Err(e) => {
            return Err(format!("Invalid regex pattern '{}': {}", pattern, e));
        }
    }

    Ok(RunResult {
        data: Some(json!({
            "matches": matches,
            "match_count": matches.len(),
            "path": safe_path.to_string_lossy(),
        })),
        logs: format!("Grep '{}' in {:?}: {} match(es)", pattern, safe_path, matches.len()),
        audit: Some(json!({
            "path": safe_path.to_string_lossy(),
            "pattern": pattern,
            "match_count": matches.len(),
        })),
    })
}

/// Search source files across the workspace for a text query.
pub fn search_code(req: &Request, jail: &FsJail) -> Result<RunResult, String> {
    let query = req.params
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing required 'query' parameter".to_string())?;

    let max_matches = req.params
        .get("max_matches")
        .and_then(|v| v.as_u64())
        .unwrap_or(100) as usize;

    // Use case-insensitive search (escaped query for literal search)
    let escaped = regex::escape(query);
    let pattern_str = format!(r"(?i){}", escaped);
    let re = regex::Regex::new(&pattern_str)
        .map_err(|e| format!("Failed to compile search pattern: {}", e))?;

    // Walk workspace collecting source files
    let mut all_files = Vec::new();
    walk_workspace(
        jail,
        jail.workspace_root(),
        "",
        usize::MAX,
        &mut all_files,
    )?;

    let mut matches: Vec<serde_json::Value> = Vec::new();
    for rel_path in &all_files {
        if matches.len() >= max_matches {
            break;
        }

        // Filter by source extension
        let ext = Path::new(rel_path)
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_lowercase();
        if !SOURCE_EXTENSIONS.contains(&ext.as_str()) {
            continue;
        }

        let full_path = jail.workspace_root().join(rel_path);
        // Skip files larger than MAX_SEARCH_FILE_SIZE to prevent memory exhaustion
        if let Ok(meta) = fs::metadata(&full_path) {
            if meta.len() > MAX_SEARCH_FILE_SIZE {
                continue;
            }
        }
        let Ok(content) = fs::read_to_string(&full_path) else {
            continue;
        };

        for (line_no, line) in content.lines().enumerate() {
            if matches.len() >= max_matches {
                break;
            }
            if re.is_match(line) {
                matches.push(json!({
                    "file": rel_path,
                    "line": line_no + 1,
                    "text": line,
                }));
            }
        }
    }

    Ok(RunResult {
        data: Some(json!({
            "matches": matches,
            "match_count": matches.len(),
            "query": query,
        })),
        logs: format!("Search code for '{}': {} match(es) in {} file(s)", query, matches.len(), all_files.len()),
        audit: Some(json!({
            "query": query,
            "match_count": matches.len(),
            "files_scanned": all_files.len(),
        })),
    })
}

/// Find files by glob pattern within the workspace jail.
pub fn find_files(req: &Request, jail: &FsJail) -> Result<RunResult, String> {
    let pattern = req.params
        .get("pattern")
        .and_then(|v| v.as_str())
        .unwrap_or("*");

    let max_results = req.params
        .get("max_results")
        .and_then(|v| v.as_u64())
        .unwrap_or(200) as usize;

    let mut all_files = Vec::new();
    walk_workspace(
        jail,
        jail.workspace_root(),
        "",
        usize::MAX, // Collect all files, filter by pattern below
        &mut all_files,
    )?;

    // Filter by glob pattern (simple fnmatch-style matching)
    let pattern_lower = pattern.to_lowercase();
    let matching: Vec<String> = all_files.into_iter()
        .filter(|f| {
            let f_lower = f.to_lowercase();
            // Match against both the full relative path and the file name
            glob_match(&pattern_lower, &f_lower)
                || Path::new(f).file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| glob_match(&pattern_lower, &n.to_lowercase()))
                    .unwrap_or(false)
        })
        .take(max_results)
        .collect();

    Ok(RunResult {
        data: Some(json!({
            "files": matching,
            "count": matching.len(),
            "pattern": pattern,
        })),
        logs: format!("Find files matching '{}': {} result(s)", pattern, matching.len()),
        audit: Some(json!({
            "pattern": pattern,
            "count": matching.len(),
        })),
    })
}

/// Simple glob matching (supports `*`, `?`, and `**`).
fn glob_match(pattern: &str, name: &str) -> bool {
    let mut pat_chars = pattern.chars().peekable();
    let mut name_chars = name.chars().peekable();

    loop {
        match (pat_chars.next(), name_chars.next()) {
            (None, None) => return true,
            (None, Some(_)) => return false,
            (Some(_), None) => return false,
            (Some('*'), _) => {
                // '*' matches zero or more characters
                // Check for '**' (matches across path separators)
                if pat_chars.peek() == Some(&'*') {
                    // '**' pattern — consume both stars and match everything
                    pat_chars.next(); // consume second '*'
                    // Skip any following '/' in pattern
                    if pat_chars.peek() == Some(&'/') {
                        pat_chars.next();
                    }
                    // Match the rest of the pattern against the rest of the name
                    let rest_pat: String = pat_chars.collect();
                    let rest_name: String = name_chars.collect();
                    // Try matching the rest at any position
                    if rest_pat.is_empty() {
                        return true;
                    }
                    // Check if the rest of the pattern matches anywhere in the name
                    for i in 0..=rest_name.len() {
                        if glob_match(&rest_pat, &rest_name[i..]) {
                            return true;
                        }
                    }
                    return false;
                }
                // Single '*' — match any chars except '/'
                let rest_pat: String = pat_chars.collect();
                if rest_pat.is_empty() {
                    return true;
                }
                let rest_name: String = name_chars.collect();
                // Try matching the rest at each position (excluding path separators in single segment)
                // Use char_indices to safely handle multi-byte UTF-8 characters
                let mut byte_positions: Vec<usize> = rest_name.char_indices().map(|(i, _)| i).collect();
                byte_positions.push(rest_name.len()); // Include end-of-string position
                for byte_i in byte_positions {
                    // If there's a '/' in the slice before byte_i, skip (single * doesn't cross dirs)
                    if rest_name[..byte_i].contains('/') {
                        continue;
                    }
                    if glob_match(&rest_pat, &rest_name[byte_i..]) {
                        return true;
                    }
                }
                return false;
            }
            (Some('?'), Some(c)) => {
                // '?' matches any single character except '/'
                if c == '/' {
                    return false;
                }
                continue;
            }
            (Some(p), Some(c)) => {
                if p.to_ascii_lowercase() != c.to_ascii_lowercase() {
                    return false;
                }
            }
        }
    }
}

/// Generate a project directory tree within the jail.
pub fn project_tree(req: &Request, jail: &FsJail) -> Result<RunResult, String> {
    let max_depth = req.params
        .get("max_depth")
        .and_then(|v| v.as_u64())
        .unwrap_or(2) as usize;

    let max_entries = req.params
        .get("max_entries")
        .and_then(|v| v.as_u64())
        .unwrap_or(300) as usize;

    let mut lines: Vec<String> = Vec::new();
    let mut entry_count = 0;

    // Root directory name
    if let Some(root_name) = jail.workspace_root().file_name() {
        lines.push(format!("{}/", root_name.to_string_lossy()));
    }

    build_tree(
        jail,
        jail.workspace_root(),
        0,
        max_depth,
        max_entries,
        &mut lines,
        &mut entry_count,
    )?;

    if entry_count >= max_entries {
        lines.push("  ...".to_string());
    }

    Ok(RunResult {
        data: Some(json!({
            "tree": lines,
            "entry_count": entry_count,
            "max_depth": max_depth,
        })),
        logs: lines.join("\n"),
        audit: Some(json!({
            "max_depth": max_depth,
            "entry_count": entry_count,
        })),
    })
}

/// Recursively build tree lines for project_tree.
fn build_tree(
    jail: &FsJail,
    dir: &Path,
    depth: usize,
    max_depth: usize,
    max_entries: usize,
    lines: &mut Vec<String>,
    entry_count: &mut usize,
) -> Result<(), String> {
    if !dir.starts_with(jail.workspace_root()) {
        return Ok(());
    }

    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };

    let mut dirs: Vec<String> = Vec::new();
    let mut files: Vec<String> = Vec::new();

    for entry in entries {
        if *entry_count >= max_entries {
            break;
        }
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let name = entry.file_name().to_string_lossy().to_string();

        if IGNORED_DIRS.contains(&name.as_str()) {
            continue;
        }

        if let Ok(ft) = entry.file_type() {
            if ft.is_dir() {
                dirs.push(name);
            } else {
                files.push(name);
            }
        }
    }

    dirs.sort();
    files.sort();

    let indent = "  ".repeat(depth + 1);

    for dirname in &dirs {
        if *entry_count >= max_entries {
            break;
        }
        lines.push(format!("{}{}/", indent, dirname));
        *entry_count += 1;

        if depth + 1 < max_depth {
            let subdir = dir.join(dirname);
            build_tree(
                jail,
                &subdir,
                depth + 1,
                max_depth,
                max_entries,
                lines,
                entry_count,
            )?;
        }
    }

    for filename in &files {
        if *entry_count >= max_entries {
            break;
        }
        // Skip lock/bytecode files
        if filename.ends_with(".aelvo.lock") || filename.ends_with(".pyc") {
            continue;
        }
        lines.push(format!("{}{}", indent, filename));
        *entry_count += 1;
    }

    Ok(())
}
