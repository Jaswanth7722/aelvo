// ═══════════════════════════════════════════════════════════════════════
// Checkpoint & Rollback System — tracks file mutations, creates execution
// checkpoints, rolls back workspace state when possible, and makes all
// rollback actions visible and auditable.
//
// Design: Copy-on-write file backups stored in a checkpoint directory.
// Each checkpoint records which files changed and stores the original
// content. Rollback restores original content from the backup.
// ═══════════════════════════════════════════════════════════════════════

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

// ═══════════════════════════════════════════════════════════════════════
// Mutation Record
// ═══════════════════════════════════════════════════════════════════════

/// Type of file mutation tracked by the checkpoint system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationType {
    /// File was created (didn't exist before checkpoint)
    Created,
    /// File was modified (content changed)
    Modified,
    /// File was deleted (existed before checkpoint)
    Deleted,
}

impl MutationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MutationType::Created => "created",
            MutationType::Modified => "modified",
            MutationType::Deleted => "deleted",
        }
    }
}

/// A record of a single file mutation within a checkpoint scope.
#[derive(Debug, Clone)]
pub struct MutationRecord {
    /// Path of the mutated file (relative to workspace root).
    pub relative_path: String,
    /// Type of mutation.
    pub mutation_type: MutationType,
    /// Size of the original file (0 for creations).
    pub original_size_bytes: u64,
    /// Size of the new file (0 for deletions).
    pub new_size_bytes: u64,
    /// Whether the backup file still exists for rollback.
    pub backup_available: bool,
    /// Path to the backup file (stored in checkpoint directory).
    pub backup_path: Option<PathBuf>,
}

// ═══════════════════════════════════════════════════════════════════════
// Checkpoint
// ═══════════════════════════════════════════════════════════════════════

/// A checkpoint capturing workspace state at a point in time.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    /// Unique identifier for this checkpoint.
    pub id: String,
    /// Timestamp when the checkpoint was created (seconds since epoch).
    pub timestamp_secs: u64,
    /// Human-readable label for this checkpoint.
    pub label: String,
    /// Workspace root path at checkpoint time.
    pub workspace_root: PathBuf,
    /// Directory where backup files are stored.
    pub backup_dir: PathBuf,
    /// Records of all mutations tracked since this checkpoint.
    pub mutations: Vec<MutationRecord>,
    /// Has this checkpoint been rolled back?
    pub rolled_back: bool,
    /// Timestamp of rollback (if applied).
    pub rolled_back_at: Option<u64>,
}

impl Checkpoint {
    /// Save the original content of a file to the backup directory.
    pub fn save_original(&self, relative_path: &str, content: &[u8]) -> Result<PathBuf, String> {
        let backup_path = self.backup_dir.join(sanitize_path(relative_path));
        if let Some(parent) = backup_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create backup dir: {}", e))?;
        }
        fs::write(&backup_path, content)
            .map_err(|e| format!("Failed to write backup: {}", e))?;
        Ok(backup_path)
    }

    /// Load the original content of a file from the backup.
    pub fn load_original(&self, relative_path: &str) -> Result<Vec<u8>, String> {
        let backup_path = self.backup_dir.join(sanitize_path(relative_path));
        fs::read(&backup_path)
            .map_err(|e| format!("Failed to read backup '{}': {}", relative_path, e))
    }

    /// Check if a backup exists for a given file.
    pub fn has_backup(&self, relative_path: &str) -> bool {
        self.backup_dir.join(sanitize_path(relative_path)).exists()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Checkpoint Manager
// ═══════════════════════════════════════════════════════════════════════

/// Manages checkpoints and rollback operations for workspace state.
pub struct CheckpointManager {
    /// Workspace root path.
    workspace_root: PathBuf,
    /// Base directory where checkpoint data is stored.
    checkpoint_base: PathBuf,
    /// Active checkpoints (most recent at end).
    checkpoints: Vec<Checkpoint>,
    /// Maximum number of checkpoints to retain.
    max_checkpoints: usize,
    /// Current checkpoint being tracked (if any).
    active_checkpoint: Option<usize>,
}

impl CheckpointManager {
    /// Create a new checkpoint manager for the given workspace.
    pub fn new(workspace_root: &str, checkpoint_base: Option<&str>) -> Result<Self, String> {
        let ws = Path::new(workspace_root)
            .canonicalize()
            .map_err(|e| format!("Failed to resolve workspace: {}", e))?;

        let base = checkpoint_base
            .map(|s| PathBuf::from(s))
            .unwrap_or_else(|| ws.join(".aelvo_checkpoints"));

        fs::create_dir_all(&base)
            .map_err(|e| format!("Failed to create checkpoint base: {}", e))?;

        Ok(CheckpointManager {
            workspace_root: ws,
            checkpoint_base: base,
            checkpoints: Vec::new(),
            max_checkpoints: 20,
            active_checkpoint: None,
        })
    }

    // ── Checkpoint Creation ─────────────────────────────────────────

    /// Create a new checkpoint, returning its ID.
    pub fn create_checkpoint(&mut self, label: &str) -> Result<String, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let id = format!("cp_{:x}_{}", timestamp, self.checkpoints.len());
        let cp_dir = self.checkpoint_base.join(&id);

        fs::create_dir_all(&cp_dir)
            .map_err(|e| format!("Failed to create checkpoint dir: {}", e))?;

        let checkpoint = Checkpoint {
            id: id.clone(),
            timestamp_secs: timestamp,
            label: label.to_string(),
            workspace_root: self.workspace_root.clone(),
            backup_dir: cp_dir,
            mutations: Vec::new(),
            rolled_back: false,
            rolled_back_at: None,
        };

        let idx = self.checkpoints.len();
        self.checkpoints.push(checkpoint);
        self.active_checkpoint = Some(idx);

        // Prune old checkpoints if needed
        while self.checkpoints.len() > self.max_checkpoints {
            if let Some(oldest) = self.checkpoints.first() {
                let _ = fs::remove_dir_all(&oldest.backup_dir);
            }
            self.checkpoints.remove(0);
            // Adjust active checkpoint index
            if let Some(active) = self.active_checkpoint {
                if active > 0 {
                    self.active_checkpoint = Some(active - 1);
                }
            }
        }

        Ok(id)
    }

    /// Close the active checkpoint (stop tracking mutations).
    pub fn close_checkpoint(&mut self) -> Result<(), String> {
        self.active_checkpoint = None;
        Ok(())
    }

    // ── Mutation Tracking ───────────────────────────────────────────

    /// Record a file creation in the active checkpoint.
    pub fn record_creation(&mut self, relative_path: &str) -> Result<(), String> {
        let idx = self.active_checkpoint.ok_or_else(|| {
            "No active checkpoint to record mutation".to_string()
        })?;

        let full_path = self.workspace_root.join(relative_path);
        let new_size = fs::metadata(&full_path).ok().map(|m| m.len()).unwrap_or(0);

        self.checkpoints[idx].mutations.push(MutationRecord {
            relative_path: relative_path.to_string(),
            mutation_type: MutationType::Created,
            original_size_bytes: 0,
            new_size_bytes: new_size,
            backup_available: false,
            backup_path: None,
        });

        Ok(())
    }

    /// Record a file modification. Backs up the original content.
    pub fn record_modification(
        &mut self,
        relative_path: &str,
        original_content: &[u8],
    ) -> Result<(), String> {
        let idx = self.active_checkpoint.ok_or_else(|| {
            "No active checkpoint to record mutation".to_string()
        })?;

        let cp = &self.checkpoints[idx];
        let backup_path = cp.save_original(relative_path, original_content)?;

        let full_path = self.workspace_root.join(relative_path);
        let new_size = fs::metadata(&full_path).ok().map(|m| m.len()).unwrap_or(0);

        self.checkpoints[idx].mutations.push(MutationRecord {
            relative_path: relative_path.to_string(),
            mutation_type: MutationType::Modified,
            original_size_bytes: original_content.len() as u64,
            new_size_bytes: new_size,
            backup_available: true,
            backup_path: Some(backup_path),
        });

        Ok(())
    }

    /// Record a file deletion. Backs up the original content.
    pub fn record_deletion(
        &mut self,
        relative_path: &str,
        original_content: &[u8],
    ) -> Result<(), String> {
        let idx = self.active_checkpoint.ok_or_else(|| {
            "No active checkpoint to record mutation".to_string()
        })?;

        let cp = &self.checkpoints[idx];
        let backup_path = cp.save_original(relative_path, original_content)?;

        self.checkpoints[idx].mutations.push(MutationRecord {
            relative_path: relative_path.to_string(),
            mutation_type: MutationType::Deleted,
            original_size_bytes: original_content.len() as u64,
            new_size_bytes: 0,
            backup_available: true,
            backup_path: Some(backup_path),
        });

        Ok(())
    }

    // ── Rollback ────────────────────────────────────────────────────

    /// Rollback the most recent checkpoint, restoring all files to their
    /// pre-checkpoint state.
    pub fn rollback_latest(&mut self) -> Result<RollbackResult, String> {
        let idx = self.checkpoints.len().checked_sub(1).ok_or_else(|| {
            "No checkpoints to rollback".to_string()
        })?;

        self.rollback_checkpoint_by_index(idx)
    }

    /// Rollback a specific checkpoint by its ID.
    pub fn rollback_by_id(&mut self, id: &str) -> Result<RollbackResult, String> {
        let idx = self.checkpoints.iter().position(|cp| cp.id == id).ok_or_else(|| {
            format!("Checkpoint '{}' not found", id)
        })?;

        self.rollback_checkpoint_by_index(idx)
    }

    fn rollback_checkpoint_by_index(&mut self, idx: usize) -> Result<RollbackResult, String> {
        // Clone the checkpoint data we need before mutating self
        let cp_id = self.checkpoints[idx].id.clone();
        let cp_label = self.checkpoints[idx].label.clone();
        let cp_mutations = self.checkpoints[idx].mutations.clone();
        let cp_backup_dir = self.checkpoints[idx].backup_dir.clone();
        let cp_rolled_back = self.checkpoints[idx].rolled_back;

        if cp_rolled_back {
            return Err(format!(
                "Checkpoint '{}' has already been rolled back",
                cp_id
            ));
        }

        let mut restored = 0u64;
        let mut failed = 0u64;
        let mut errors = Vec::new();

        // Process mutations in reverse order (deletions first, then modifications)
        for mutation in cp_mutations.iter().rev() {
            let target = self.workspace_root.join(&mutation.relative_path);

            match mutation.mutation_type {
                MutationType::Created => {
                    // Remove the created file
                    if target.exists() {
                        if target.is_dir() {
                            if let Err(e) = fs::remove_dir_all(&target) {
                                failed += 1;
                                errors.push(format!(
                                    "Failed to remove created dir '{}': {}",
                                    mutation.relative_path, e
                                ));
                                continue;
                            }
                        } else {
                            if let Err(e) = fs::remove_file(&target) {
                                failed += 1;
                                errors.push(format!(
                                    "Failed to remove created file '{}': {}",
                                    mutation.relative_path, e
                                ));
                                continue;
                            }
                        }
                    }
                    restored += 1;
                }
                MutationType::Modified | MutationType::Deleted => {
                    // Restore original content from backup
                    let backup_path = cp_backup_dir.join(sanitize_path(&mutation.relative_path));
                    match fs::read(&backup_path) {
                        Ok(content) => {
                            if let Some(parent) = target.parent() {
                                let _ = fs::create_dir_all(parent);
                            }
                            if let Err(e) = fs::write(&target, &content) {
                                failed += 1;
                                errors.push(format!(
                                    "Failed to restore '{}': {}",
                                    mutation.relative_path, e
                                ));
                                continue;
                            }
                            restored += 1;
                        }
                        Err(e) => {
                            failed += 1;
                            errors.push(format!(
                                "Failed to load backup for '{}': {}",
                                mutation.relative_path, e
                            ));
                        }
                    }
                }
            }
        }

        // Mark checkpoint as rolled back
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.checkpoints[idx].rolled_back = true;
        self.checkpoints[idx].rolled_back_at = Some(now);

        // Clean up backup directory
        let _ = fs::remove_dir_all(&cp_backup_dir);

        Ok(RollbackResult {
            checkpoint_id: cp_id,
            checkpoint_label: cp_label,
            mutations_processed: cp_mutations.len() as u64,
            files_restored: restored,
            files_failed: failed,
            errors,
            rolled_back_at: now,
        })
    }

    // ── Queries ─────────────────────────────────────────────────────

    /// Get a reference to a specific checkpoint by ID.
    pub fn get_checkpoint(&self, id: &str) -> Option<&Checkpoint> {
        self.checkpoints.iter().find(|cp| cp.id == id)
    }

    /// Get the most recent checkpoint.
    pub fn latest_checkpoint(&self) -> Option<&Checkpoint> {
        self.checkpoints.last()
    }

    /// Count the number of checkpoints.
    pub fn checkpoint_count(&self) -> usize {
        self.checkpoints.len()
    }

    /// Check if there's an active checkpoint.
    pub fn has_active_checkpoint(&self) -> bool {
        self.active_checkpoint.is_some()
    }

    /// Get the active checkpoint (if any).
    pub fn active_checkpoint(&self) -> Option<&Checkpoint> {
        self.active_checkpoint
            .and_then(|idx| self.checkpoints.get(idx))
    }

    /// Get mutations for a specific checkpoint.
    pub fn mutations_for_checkpoint(&self, id: &str) -> Option<&[MutationRecord]> {
        self.checkpoints
            .iter()
            .find(|cp| cp.id == id)
            .map(|cp| cp.mutations.as_slice())
    }

    /// Export checkpoint summary for audit reporting.
    pub fn export_summary(&self) -> Vec<serde_json::Value> {
        self.checkpoints.iter().map(|cp| {
            serde_json::json!({
                "id": cp.id,
                "timestamp_secs": cp.timestamp_secs,
                "label": cp.label,
                "mutation_count": cp.mutations.len(),
                "rolled_back": cp.rolled_back,
                "rolled_back_at": cp.rolled_back_at,
                "backup_size_kb": Self::dir_size(&cp.backup_dir) / 1024,
            })
        }).collect()
    }

    fn dir_size(path: &Path) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    total += meta.len();
                    if meta.is_dir() {
                        total += Self::dir_size(&entry.path());
                    }
                }
            }
        }
        total
    }
}

/// Result of a rollback operation.
#[derive(Debug, Clone)]
pub struct RollbackResult {
    /// ID of the checkpoint that was rolled back.
    pub checkpoint_id: String,
    /// Label of the checkpoint that was rolled back.
    pub checkpoint_label: String,
    /// Total mutations processed.
    pub mutations_processed: u64,
    /// Number of files successfully restored.
    pub files_restored: u64,
    /// Number of files that failed to restore.
    pub files_failed: u64,
    /// Error messages from failed restorations.
    pub errors: Vec<String>,
    /// Timestamp of the rollback.
    pub rolled_back_at: u64,
}

impl RollbackResult {
    /// Whether all mutations were restored successfully.
    pub fn success(&self) -> bool {
        self.files_failed == 0
    }
}

/// Sanitize a relative path for use as a backup filename.
fn sanitize_path(path: &str) -> String {
    // Component-wise sanitization. Rebuilding from components (instead
    // of a whole-string replace) ensures no traversal component, such
    // as "..", "....//" lookalikes, or an absolute/rooted prefix, can
    // survive Path::join and escape the backup directory.
    let normalized = path.replace('\\', "/");
    let mut parts: Vec<String> = Vec::new();
    for part in normalized.split('/') {
        match part {
            "" | "." => continue,
            // Strip drive-letter prefixes (e.g. "C:") so Windows absolute
            // paths cannot escape the backup dir via Path::join.
            p if p.len() == 2 && p.as_bytes()[1] == b':' && p.as_bytes()[0].is_ascii_alphabetic() => continue,
            p => parts.push(p.replace("..", "__")),
        }
    }
    parts.join("/")
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_path_neutralizes_traversal() {
        // Ordinary relative paths pass through unchanged.
        assert_eq!(sanitize_path("src/main.rs"), "src/main.rs");
        assert_eq!(sanitize_path("a/b/c.txt"), "a/b/c.txt");
        // Classic ".." traversal components are neutralized.
        assert_eq!(sanitize_path("../etc/passwd"), "__/etc/passwd");
        assert_eq!(sanitize_path("a/../b"), "a/__/b");
        // The "....//" lookalike bypass is closed by component-wise handling.
        assert_eq!(sanitize_path("....//etc/passwd"), "____/etc/passwd");
        // Absolute / rooted prefixes are stripped so Path::join cannot escape.
        assert_eq!(sanitize_path("/etc/passwd"), "etc/passwd");
        assert_eq!(sanitize_path("//etc/passwd"), "etc/passwd");
        // Backslash separators normalize to forward slashes.
        assert_eq!(sanitize_path(r"..\..\win.ini"), "__/__/win.ini");
        // Drive-letter prefixes are stripped (Windows absolute paths).
        assert_eq!(sanitize_path("C:/Windows/system32"), "Windows/system32");
        assert_eq!(sanitize_path(r"C:\Windows\system32"), "Windows/system32");
        // Dot components are dropped.
        assert_eq!(sanitize_path("././a/./b"), "a/b");
    }

    fn setup_test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("checkpoint_test_{}", name));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_create_checkpoint() {
        let dir = setup_test_dir("create");
        let mut mgr = CheckpointManager::new(&dir.to_string_lossy(), None).unwrap();
        let id = mgr.create_checkpoint("test checkpoint").unwrap();
        assert!(!id.is_empty());
        assert_eq!(mgr.checkpoint_count(), 1);
        assert!(mgr.has_active_checkpoint());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_record_modification_and_rollback() {
        let dir = setup_test_dir("mod_rollback");
        let test_file = dir.join("test.txt");
        fs::write(&test_file, b"original content").unwrap();

        let mut mgr = CheckpointManager::new(&dir.to_string_lossy(), None).unwrap();
        mgr.create_checkpoint("mod test").unwrap();

        // Read original and modify
        let original = fs::read(&test_file).unwrap();
        mgr.record_modification("test.txt", &original).unwrap();
        fs::write(&test_file, b"modified content").unwrap();

        // Verify modified
        assert_eq!(fs::read_to_string(&test_file).unwrap(), "modified content");

        // Rollback
        let result = mgr.rollback_latest().unwrap();
        assert!(result.success());
        assert_eq!(result.files_restored, 1);

        // Verify restored
        assert_eq!(fs::read_to_string(&test_file).unwrap(), "original content");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_record_creation_and_rollback() {
        let dir = setup_test_dir("create_rollback");
        let mut mgr = CheckpointManager::new(&dir.to_string_lossy(), None).unwrap();
        mgr.create_checkpoint("create test").unwrap();

        // Create a file
        fs::write(dir.join("new.txt"), b"new file").unwrap();
        mgr.record_creation("new.txt").unwrap();

        // Verify exists
        assert!(dir.join("new.txt").exists());

        // Rollback
        let result = mgr.rollback_latest().unwrap();
        assert!(result.success());

        // Verify removed
        assert!(!dir.join("new.txt").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_record_deletion_and_rollback() {
        let dir = setup_test_dir("delete_rollback");
        let test_file = dir.join("delete.txt");
        fs::write(&test_file, b"will be deleted").unwrap();

        let mut mgr = CheckpointManager::new(&dir.to_string_lossy(), None).unwrap();
        mgr.create_checkpoint("delete test").unwrap();

        // Record and delete
        let original = fs::read(&test_file).unwrap();
        mgr.record_deletion("delete.txt", &original).unwrap();
        fs::remove_file(&test_file).unwrap();

        assert!(!test_file.exists());

        // Rollback
        let result = mgr.rollback_latest().unwrap();
        assert!(result.success());

        // Verify restored
        assert!(test_file.exists());
        assert_eq!(fs::read_to_string(&test_file).unwrap(), "will be deleted");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_multiple_mutations_rollback() {
        let dir = setup_test_dir("multi");
        fs::write(dir.join("a.txt"), b"aaa").unwrap();
        fs::write(dir.join("b.txt"), b"bbb").unwrap();

        let mut mgr = CheckpointManager::new(&dir.to_string_lossy(), None).unwrap();
        mgr.create_checkpoint("multi test").unwrap();

        // Modify a.txt
        let orig_a = fs::read(dir.join("a.txt")).unwrap();
        mgr.record_modification("a.txt", &orig_a).unwrap();
        fs::write(dir.join("a.txt"), b"AAA").unwrap();

        // Delete b.txt
        let orig_b = fs::read(dir.join("b.txt")).unwrap();
        mgr.record_deletion("b.txt", &orig_b).unwrap();
        fs::remove_file(dir.join("b.txt")).unwrap();

        // Create c.txt
        fs::write(dir.join("c.txt"), b"ccc").unwrap();
        mgr.record_creation("c.txt").unwrap();

        // Rollback
        let result = mgr.rollback_latest().unwrap();
        assert!(result.success());
        assert_eq!(result.mutations_processed, 3);
        assert_eq!(result.files_restored, 3);

        // Verify all restored
        assert_eq!(fs::read_to_string(dir.join("a.txt")).unwrap(), "aaa");
        assert_eq!(fs::read_to_string(dir.join("b.txt")).unwrap(), "bbb");
        assert!(!dir.join("c.txt").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_double_rollback_rejected() {
        let dir = setup_test_dir("double");
        let mut mgr = CheckpointManager::new(&dir.to_string_lossy(), None).unwrap();
        mgr.create_checkpoint("double test").unwrap();
        mgr.close_checkpoint().unwrap();

        let result = mgr.rollback_latest().unwrap();
        assert!(result.success());

        let err = mgr.rollback_latest();
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("already been rolled back"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_no_active_checkpoint_error() {
        let dir = setup_test_dir("no_active");
        let mut mgr = CheckpointManager::new(&dir.to_string_lossy(), None).unwrap();
        let err = mgr.record_modification("test.txt", b"data");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("No active checkpoint"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_checkpoint_summary() {
        let dir = setup_test_dir("summary");
        let mut mgr = CheckpointManager::new(&dir.to_string_lossy(), None).unwrap();
        mgr.create_checkpoint("summary test").unwrap();
        mgr.close_checkpoint().unwrap();

        let summary = mgr.export_summary();
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0]["label"], "summary test");
        assert!(!summary[0]["rolled_back"].as_bool().unwrap());
        let _ = fs::remove_dir_all(&dir);
    }
}
