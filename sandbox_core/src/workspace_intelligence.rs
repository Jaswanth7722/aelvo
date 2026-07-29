// ═══════════════════════════════════════════════════════════════════════
// Workspace Intelligence — tracks trusted workspace roots, mutable and
// immutable zones, protected files/directories, snapshot state and change
// history, detects unexpected workspace drift, prevents path traversal
// and symlink escapes.
//
// Design: Canonical path tracking with zone-based access control.
// All decisions are deterministic and based on path prefix matching
// after canonicalization.
// ═══════════════════════════════════════════════════════════════════════

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

// ═══════════════════════════════════════════════════════════════════════
// Zone Types
// ═══════════════════════════════════════════════════════════════════════

/// Access permissions for a workspace zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZoneAccess {
    /// Read-only — no mutations allowed
    ReadOnly,
    /// Read-write — full access
    ReadWrite,
    /// No access — path is inaccessible
    Blocked,
}

impl ZoneAccess {
    pub fn can_read(&self) -> bool {
        matches!(self, ZoneAccess::ReadOnly | ZoneAccess::ReadWrite)
    }

    pub fn can_write(&self) -> bool {
        matches!(self, ZoneAccess::ReadWrite)
    }
}

/// A named zone within the workspace with specific access permissions.
#[derive(Debug, Clone)]
pub struct Zone {
    /// Canonical path to the zone root.
    pub path: PathBuf,
    /// Access permissions for this zone.
    pub access: ZoneAccess,
    /// Human-readable description of this zone.
    pub description: String,
    /// Whether this zone contains immutable files (cannot be modified).
    pub immutable_files: bool,
}

/// Status returned when checking a path against workspace intelligence.
#[derive(Debug, Clone)]
pub struct ZoneCheckResult {
    /// Whether the path is within a known zone.
    pub in_known_zone: bool,
    /// The zone access level for this path.
    pub access: ZoneAccess,
    /// The zone name/description.
    pub zone_description: String,
    /// Whether the path is protected (immutable).
    pub protected: bool,
    /// Whether the path is on the deny list.
    pub denied: bool,
    /// Human-readable explanation.
    pub explanation: String,
}

// ═══════════════════════════════════════════════════════════════════════
// Drift Detection
// ═══════════════════════════════════════════════════════════════════════

/// A record of a file's known state for drift detection.
#[derive(Debug, Clone)]
pub struct FileSnapshot {
    /// Relative path within the workspace.
    pub relative_path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Last modification time (seconds since epoch).
    pub modified_secs: u64,
    /// Whether the file existed at snapshot time.
    pub existed: bool,
    /// Whether it was a regular file.
    pub is_file: bool,
    /// Whether it was a directory.
    pub is_dir: bool,
}

impl FileSnapshot {
    /// Create a snapshot of a file/directory.
    pub fn from_path(base: &Path, relative: &str) -> Result<Self, String> {
        let full = base.join(relative);
        let meta = fs::metadata(&full).map_err(|e| format!("Failed to stat '{}': {}", relative, e))?;

        let modified = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Ok(FileSnapshot {
            relative_path: relative.to_string(),
            size_bytes: meta.len(),
            modified_secs: modified,
            existed: true,
            is_file: meta.is_file(),
            is_dir: meta.is_dir(),
        })
    }

    /// Create a snapshot for a non-existent file.
    pub fn nonexistent(relative: &str) -> Self {
        FileSnapshot {
            relative_path: relative.to_string(),
            size_bytes: 0,
            modified_secs: 0,
            existed: false,
            is_file: false,
            is_dir: false,
        }
    }

    /// Compare this snapshot with the current state of the file.
    pub fn check_drift(&self, base: &Path) -> DriftResult {
        let full = base.join(&self.relative_path);
        let current = match fs::metadata(&full) {
            Ok(meta) => meta,
            Err(_) => {
                // File disappeared
                return DriftResult {
                    relative_path: self.relative_path.clone(),
                    drifted: true,
                    reason: "FILE_DELETED".to_string(),
                    detail: format!("File existed at snapshot but is now gone"),
                    old_state: Some(format!(
                        "existed={}, size={}, is_file={}",
                        self.existed, self.size_bytes, self.is_file
                    )),
                    new_state: Some("does not exist".to_string()),
                };
            }
        };

        let modified = current
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut reasons = Vec::new();

        if current.len() != self.size_bytes {
            reasons.push(format!(
                "size changed: {} -> {}",
                self.size_bytes, current.len()
            ));
        }

        if modified != self.modified_secs {
            reasons.push("modification time changed".to_string());
        }

        if current.is_file() != self.is_file {
            reasons.push(format!(
                "type changed: is_file={} -> {}",
                self.is_file,
                current.is_file()
            ));
        }

        if reasons.is_empty() {
            DriftResult {
                relative_path: self.relative_path.clone(),
                drifted: false,
                reason: "NO_DRIFT".to_string(),
                detail: "File matches snapshot".to_string(),
                old_state: None,
                new_state: None,
            }
        } else {
            DriftResult {
                relative_path: self.relative_path.clone(),
                drifted: true,
                reason: "FILE_MODIFIED".to_string(),
                detail: reasons.join("; "),
                old_state: Some(format!("snapshot: {} bytes", self.size_bytes)),
                new_state: Some(format!("current: {} bytes", current.len())),
            }
        }
    }
}

/// Result of a drift check.
#[derive(Debug, Clone)]
pub struct DriftResult {
    /// Path of the file that was checked.
    pub relative_path: String,
    /// Whether drift was detected.
    pub drifted: bool,
    /// Machine-readable reason code.
    pub reason: String,
    /// Human-readable detail.
    pub detail: String,
    /// Old state description.
    pub old_state: Option<String>,
    /// New state description.
    pub new_state: Option<String>,
}

// ═══════════════════════════════════════════════════════════════════════
// Workspace Intelligence Engine
// ═══════════════════════════════════════════════════════════════════════

/// Main engine for workspace intelligence — zone tracking, drift detection,
/// protected path enforcement.
pub struct WorkspaceIntelligence {
    /// Canonical workspace root.
    workspace_root: PathBuf,
    /// Defined zones within the workspace.
    zones: Vec<Zone>,
    /// Protected files — paths that cannot be modified or deleted.
    protected_files: HashSet<PathBuf>,
    /// Protected directories — paths that cannot be modified (recursive).
    protected_dirs: HashSet<PathBuf>,
    /// Denied paths — paths that are completely blocked.
    denied_paths: HashSet<PathBuf>,
    /// File snapshots for drift detection.
    snapshots: HashMap<String, FileSnapshot>,
    /// Known mutable zones (for fast lookup).
    mutable_zones: Vec<PathBuf>,
    /// Known immutable zones (for fast lookup).
    immutable_zones: Vec<PathBuf>,
}

impl WorkspaceIntelligence {
    /// Create a new workspace intelligence engine with default zones.
    pub fn new(workspace_root: &str) -> Result<Self, String> {
        let ws = Path::new(workspace_root)
            .canonicalize()
            .map_err(|e| format!(
                "Failed to canonicalize workspace root '{}': {}",
                workspace_root, e
            ))?;

        let mut engine = WorkspaceIntelligence {
            workspace_root: ws.clone(),
            zones: Vec::new(),
            protected_files: HashSet::new(),
            protected_dirs: HashSet::new(),
            denied_paths: HashSet::new(),
            snapshots: HashMap::new(),
            mutable_zones: Vec::new(),
            immutable_zones: Vec::new(),
        };

        // Add default workspace zone (read-write for workspace)
        engine.add_zone(Zone {
            path: ws.clone(),
            access: ZoneAccess::ReadWrite,
            description: "Workspace root".to_string(),
            immutable_files: false,
        });

        // Add default protected zones
        engine.add_protected_dir(".git");

        Ok(engine)
    }

    // ── Zone Management ─────────────────────────────────────────────

    /// Add a named zone with specific access permissions.
    pub fn add_zone(&mut self, zone: Zone) {
        let canonical = match zone.path.canonicalize() {
            Ok(p) => p,
            Err(_) => zone.path.clone(),
        };

        let access = zone.access;
        let description = zone.description.clone();

        self.zones.push(Zone {
            path: canonical.clone(),
            access,
            description,
            immutable_files: zone.immutable_files,
        });

        // Index for fast lookup
        match access {
            ZoneAccess::ReadWrite => self.mutable_zones.push(canonical),
            ZoneAccess::ReadOnly => self.immutable_zones.push(canonical),
            ZoneAccess::Blocked => {}
        }
    }

    /// Add a read-only (immutable) zone — typically system directories.
    pub fn add_readonly_zone(&mut self, path: &str, description: &str) -> Result<(), String> {
        let full_path = self.workspace_root.join(path);
        self.add_zone(Zone {
            path: full_path,
            access: ZoneAccess::ReadOnly,
            description: description.to_string(),
            immutable_files: true,
        });
        Ok(())
    }

    /// Add a blocked zone — paths that cannot be accessed at all.
    pub fn add_blocked_zone(&mut self, path: &str, description: &str) -> Result<(), String> {
        let full_path = self.workspace_root.join(path);
        self.add_zone(Zone {
            path: full_path,
            access: ZoneAccess::Blocked,
            description: description.to_string(),
            immutable_files: false,
        });
        Ok(())
    }

    // ── Protected Path Management ───────────────────────────────────

    /// Mark a file as protected (cannot be modified or deleted).
    pub fn add_protected_file(&mut self, relative_path: &str) {
        self.protected_files.insert(self.workspace_root.join(relative_path));
    }

    /// Mark a directory as protected (cannot be modified recursively).
    pub fn add_protected_dir(&mut self, relative_path: &str) {
        self.protected_dirs.insert(self.workspace_root.join(relative_path));
    }

    /// Add a path to the deny list (completely blocked).
    pub fn add_denied_path(&mut self, relative_path: &str) {
        self.denied_paths.insert(self.workspace_root.join(relative_path));
    }

    // ── Path Checking ───────────────────────────────────────────────

    /// Check a path against workspace intelligence rules.
    /// Returns a ZoneCheckResult with access level and explanation.
    /// Relative paths are resolved against workspace_root.
    pub fn check_path(&self, path: &str) -> ZoneCheckResult {
        let raw_path = Path::new(path);
        let resolved = if raw_path.is_relative() {
            self.workspace_root.join(raw_path)
        } else {
            raw_path.to_path_buf()
        };

        // Walk up to find nearest existing ancestor for non-existent paths
        let canonical = match resolved.canonicalize() {
            Ok(p) => p,
            Err(_) => {
                match self.resolve_nearest_ancestor(&resolved) {
                    Some(ancestor) => {
                        return self.check_path_inner(&ancestor, path);
                    }
                    None => {
                        return ZoneCheckResult {
                            in_known_zone: false,
                            access: ZoneAccess::Blocked,
                            zone_description: "Zone undetermined — cannot resolve path".into(),
                            protected: false,
                            denied: true,
                            explanation: format!("Cannot resolve path '{}'", path),
                        };
                    }
                }
            }
        };

        self.check_path_inner(&canonical, path)
    }

    /// Walk up from a non-existent path until we find an existing ancestor.
    /// Returns the canonical ancestor path, or None if nothing in the chain exists.
    fn resolve_nearest_ancestor(&self, path: &Path) -> Option<PathBuf> {
        let mut current = path;
        for _ in 0..20 {
            // Safety guard: max 20 levels deep
            match current.parent() {
                Some(parent) => {
                    if let Ok(canon) = parent.canonicalize() {
                        return Some(canon);
                    }
                    current = parent;
                }
                None => return None,
            }
        }
        None
    }

    fn check_path_inner(&self, canonical: &Path, original: &str) -> ZoneCheckResult {
        // Check deny list first
        if self.denied_paths.contains(canonical) {
            return ZoneCheckResult {
                in_known_zone: true,
                access: ZoneAccess::Blocked,
                zone_description: "Path is on the deny list".into(),
                protected: false,
                denied: true,
                explanation: format!("Path '{}' is explicitly denied", original),
            };
        }

        // Check if path is a protected file
        let protected = self.protected_files.contains(canonical)
            || self.protected_dirs.iter().any(|d| canonical.starts_with(d));

        // Check against zones — find the MOST SPECIFIC match (longest path prefix wins)
        // This ensures that if a path matches both a parent zone and a child zone,
        // the child zone's rules take precedence.
        let mut best_zone: Option<&Zone> = None;
        for zone in &self.zones {
            if canonical.starts_with(&zone.path) {
                match best_zone {
                    None => best_zone = Some(zone),
                    Some(current) => {
                        // Longer path = more specific zone
                        if zone.path.components().count() > current.path.components().count() {
                            best_zone = Some(zone);
                        }
                    }
                }
            }
        }

        if let Some(zone) = best_zone {
            let access = if protected && zone.access == ZoneAccess::ReadWrite {
                // Protected files within read-write zones are read-only
                ZoneAccess::ReadOnly
            } else {
                zone.access
            };

            return ZoneCheckResult {
                in_known_zone: true,
                access,
                zone_description: zone.description.clone(),
                protected,
                denied: access == ZoneAccess::Blocked,
                explanation: format!(
                    "Path '{}' is in zone '{}' with {:?} access{}",
                    original,
                    zone.description,
                    access,
                    if protected { " (protected)" } else { "" }
                ),
            };
        }

        // Not in any known zone — default behavior depends on workspace containment
        if canonical.starts_with(&self.workspace_root) {
            // Within workspace but not in a specific zone: default read-write
            ZoneCheckResult {
                in_known_zone: false,
                access: if protected { ZoneAccess::ReadOnly } else { ZoneAccess::ReadWrite },
                zone_description: "Workspace (default zone)".into(),
                protected,
                denied: false,
                explanation: format!(
                    "Path '{}' is within workspace but outside defined zones",
                    original
                ),
            }
        } else {
            // Outside workspace — blocked
            ZoneCheckResult {
                in_known_zone: false,
                access: ZoneAccess::Blocked,
                zone_description: "Outside workspace boundary".into(),
                protected: false,
                denied: true,
                explanation: format!("Path '{}' is outside the workspace boundary", original),
            }
        }
    }

    /// Check whether a write operation on a path should be allowed.
    pub fn can_write(&self, path: &str) -> (bool, String) {
        let result = self.check_path(path);
        if result.access.can_write() {
            (true, result.explanation)
        } else if !result.access.can_read() {
            (false, format!("Access denied to path '{}': {}", path, result.explanation))
        } else {
            (false, format!(
                "Write access denied to path '{}': {} (zone: {})",
                path, result.explanation, result.zone_description
            ))
        }
    }

    /// Check whether a read operation on a path should be allowed.
    pub fn can_read(&self, path: &str) -> (bool, String) {
        let result = self.check_path(path);
        if result.access.can_read() {
            (true, result.explanation)
        } else {
            (false, format!("Read access denied to path '{}': {}", path, result.explanation))
        }
    }

    // ── Drift Detection ─────────────────────────────────────────────

    /// Record a snapshot of current workspace state.
    pub fn snapshot(&mut self) -> Result<usize, String> {
        self.snapshots.clear();
        let ws = self.workspace_root.clone();
        let count = self.walk_and_snapshot(&ws, "")?;
        Ok(count)
    }

    fn walk_and_snapshot(&mut self, dir: &Path, prefix: &str) -> Result<usize, String> {
        let mut count = 0;
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return Ok(0),
        };

        let ws_root = self.workspace_root.clone();

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };

            let name = entry.file_name().to_string_lossy().to_string();
            let rel = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", prefix, name)
            };

            // Skip .git to avoid massive snapshot size
            if name == ".git" {
                self.snapshots.insert(
                    rel.clone(),
                    FileSnapshot {
                        relative_path: rel,
                        size_bytes: 0,
                        modified_secs: 0,
                        existed: true,
                        is_file: false,
                        is_dir: true,
                    },
                );
                continue;
            }

            let ft = match entry.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };

            if ft.is_dir() {
                self.snapshots.insert(
                    rel.clone(),
                    FileSnapshot {
                        relative_path: rel.clone(),
                        size_bytes: 0,
                        modified_secs: 0,
                        existed: true,
                        is_file: false,
                        is_dir: true,
                    },
                );
                count += 1;
                count += self.walk_and_snapshot(&entry.path(), &rel)?;
            } else if ft.is_file() {
                let snapshot = FileSnapshot::from_path(&ws_root, &rel).unwrap_or_else(|_| {
                    FileSnapshot::nonexistent(&rel)
                });
                self.snapshots.insert(rel, snapshot);
                count += 1;
            }
        }

        Ok(count)
    }

    /// Check for drift against the last snapshot.
    pub fn check_drift(&self) -> Vec<DriftResult> {
        let mut results = Vec::new();

        for (_path, snapshot) in &self.snapshots {
            // Skip .git snapshots (too large, not meaningful)
            if snapshot.relative_path.starts_with(".git") {
                continue;
            }

            let result = snapshot.check_drift(&self.workspace_root);
            if result.drifted {
                results.push(result);
            }
        }

        results
    }

    /// Check a specific file for drift.
    pub fn check_file_drift(&self, relative_path: &str) -> Option<DriftResult> {
        self.snapshots
            .get(relative_path)
            .map(|snapshot| snapshot.check_drift(&self.workspace_root))
    }

    // ── Workspace Boundary ──────────────────────────────────────────

    /// Check whether a resolved path is within the workspace.
    pub fn is_within_workspace(&self, path: &Path) -> bool {
        match path.canonicalize() {
            Ok(canonical) => canonical.starts_with(&self.workspace_root),
            Err(_) => false,
        }
    }

    /// Get the canonical workspace root.
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Get a summary of the workspace intelligence state.
    pub fn summary(&self) -> serde_json::Value {
        serde_json::json!({
            "workspace_root": self.workspace_root.to_string_lossy(),
            "zones": self.zones.iter().map(|z| serde_json::json!({
                "path": z.path.to_string_lossy(),
                "access": format!("{:?}", z.access),
                "description": z.description,
            })).collect::<Vec<_>>(),
            "protected_files": self.protected_files.len(),
            "protected_dirs": self.protected_dirs.len(),
            "denied_paths": self.denied_paths.len(),
            "snapshots_taken": self.snapshots.len(),
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_default_zones() {
        let dir = std::env::temp_dir().join("ws_test_default");
        let _ = fs::create_dir_all(&dir);
        let engine = WorkspaceIntelligence::new(&dir.to_string_lossy()).unwrap();
        assert_eq!(engine.zones.len(), 1);
        assert_eq!(engine.zones[0].access, ZoneAccess::ReadWrite);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_check_path_within_workspace() {
        let dir = std::env::temp_dir().join("ws_test_check");
        let _ = fs::create_dir_all(&dir);
        let test_file = dir.join("test.txt");
        fs::write(&test_file, b"hello").unwrap();

        let engine = WorkspaceIntelligence::new(&dir.to_string_lossy()).unwrap();
        let result = engine.check_path(&test_file.to_string_lossy());
        assert!(result.access.can_read());
        assert!(result.access.can_write());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_readonly_zone() {
        let dir = std::env::temp_dir().join("ws_test_ro");
        let _ = fs::create_dir_all(&dir.join("config"));
        fs::write(dir.join("config").join("settings.json"), b"{}").unwrap();

        let mut engine = WorkspaceIntelligence::new(&dir.to_string_lossy()).unwrap();
        engine.add_readonly_zone("config", "Configuration files").unwrap();

        let (can_write, _) = engine.can_write("config/settings.json");
        assert!(!can_write, "Should not allow write to readonly zone");

        let (can_read, _) = engine.can_read("config/settings.json");
        assert!(can_read, "Should allow read from readonly zone");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_blocked_zone() {
        let dir = std::env::temp_dir().join("ws_test_blocked");
        let _ = fs::create_dir_all(&dir.join("secrets"));
        fs::write(dir.join("secrets").join("key.txt"), b"secret").unwrap();

        let mut engine = WorkspaceIntelligence::new(&dir.to_string_lossy()).unwrap();
        engine.add_blocked_zone("secrets", "Secret files").unwrap();

        let (can_read, _) = engine.can_read("secrets/key.txt");
        assert!(!can_read, "Should not allow read from blocked zone");

        let (can_write, _) = engine.can_write("secrets/key.txt");
        assert!(!can_write, "Should not allow write to blocked zone");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_protected_file() {
        let dir = std::env::temp_dir().join("ws_test_protected");
        let _ = fs::create_dir_all(&dir);
        fs::write(dir.join("important.txt"), b"important").unwrap();

        let mut engine = WorkspaceIntelligence::new(&dir.to_string_lossy()).unwrap();
        engine.add_protected_file("important.txt");

        let (can_write, _) = engine.can_write("important.txt");
        assert!(!can_write, "Should not allow write to protected file");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_protected_directory() {
        let dir = std::env::temp_dir().join("ws_test_prot_dir");
        let _ = fs::create_dir_all(&dir.join(".git"));
        fs::write(dir.join(".git").join("HEAD"), b"ref: refs/heads/main").unwrap();

        let mut engine = WorkspaceIntelligence::new(&dir.to_string_lossy()).unwrap();
        engine.add_protected_dir(".git");

        let (can_write, _) = engine.can_write(".git/HEAD");
        assert!(!can_write, "Should not allow write to protected dir");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_denied_path() {
        let dir = std::env::temp_dir().join("ws_test_denied");
        let _ = fs::create_dir_all(&dir);
        fs::write(dir.join("evil.exe"), b"fake binary").unwrap();

        let mut engine = WorkspaceIntelligence::new(&dir.to_string_lossy()).unwrap();
        engine.add_denied_path("evil.exe");

        let (can_read, _) = engine.can_read("evil.exe");
        assert!(!can_read, "Denied path should not be readable");

        let result = engine.check_path(&dir.join("evil.exe").to_string_lossy());
        assert!(result.denied);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_outside_workspace_blocked() {
        let dir = std::env::temp_dir().join("ws_test_outside");
        let _ = fs::create_dir_all(&dir);
        let outside = std::env::temp_dir().join("outside_file.txt");
        fs::write(&outside, b"outside").unwrap();

        let engine = WorkspaceIntelligence::new(&dir.to_string_lossy()).unwrap();
        let result = engine.check_path(&outside.to_string_lossy());
        assert!(!result.access.can_read(), "Outside path should be blocked");
        assert!(result.denied);
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_file(&outside);
    }

    #[test]
    fn test_snapshot_and_drift() {
        let dir = std::env::temp_dir().join("ws_test_drift");
        let _ = fs::create_dir_all(&dir);
        fs::write(dir.join("stable.txt"), b"original content").unwrap();

        let mut engine = WorkspaceIntelligence::new(&dir.to_string_lossy()).unwrap();
        engine.snapshot().unwrap();

        // Modify the file (different size to ensure drift detected)
        fs::write(dir.join("stable.txt"), b"short").unwrap();

        // Check drift
        let drifts = engine.check_drift();
        let modified_drift = drifts.iter().find(|d| d.relative_path == "stable.txt");
        assert!(modified_drift.is_some(), "Should detect drift in modified file");
        assert!(modified_drift.unwrap().drifted);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_new_file_not_in_snapshot() {
        let dir = std::env::temp_dir().join("ws_test_newfile");
        let _ = fs::create_dir_all(&dir);
        fs::write(dir.join("existing.txt"), b"existing").unwrap();

        let mut engine = WorkspaceIntelligence::new(&dir.to_string_lossy()).unwrap();
        engine.snapshot().unwrap();

        // Create a new file
        fs::write(dir.join("new.txt"), b"new").unwrap();

        // Check drift — new file is not in the snapshot, so no drift for it
        let drifts = engine.check_drift();
        let new_drift = drifts.iter().find(|d| d.relative_path == "new.txt");
        assert!(new_drift.is_none(), "New file should not be in snapshot");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_deleted_file_drift() {
        let dir = std::env::temp_dir().join("ws_test_deleted");
        let _ = fs::create_dir_all(&dir);
        fs::write(dir.join("will_delete.txt"), b"will be deleted").unwrap();

        let mut engine = WorkspaceIntelligence::new(&dir.to_string_lossy()).unwrap();
        engine.snapshot().unwrap();

        // Delete the file
        fs::remove_file(dir.join("will_delete.txt")).unwrap();

        let drifts = engine.check_drift();
        let deleted = drifts.iter().find(|d| d.relative_path == "will_delete.txt");
        assert!(deleted.is_some());
        assert_eq!(deleted.unwrap().reason, "FILE_DELETED");
        let _ = fs::remove_dir_all(&dir);
    }
}
