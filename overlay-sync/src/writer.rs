//! Atomic file writer with path containment.
//!
//! Writes are performed via a temporary file and atomic rename to prevent
//! partial reads by concurrent consumers.  Path validation ensures the
//! output target is absolute, is rooted under `/output`, and contains no
//! `..` traversal components.

use std::path::{Component, Path};

use crate::SyncError;

/// Writable root mounted into the overlay-sync container by Forge.
const OUTPUT_ROOT: &str = "/output";

/// Write `content` to `path` atomically via a temporary file and rename.
///
/// Creates parent directories if they do not exist.
///
/// # Errors
///
/// Returns [`SyncError::Io`] if the temporary file write or rename fails.
/// Returns [`SyncError::Write`] if the parent directory cannot be
/// determined.
pub fn write_atomic(path: &str, content: &str) -> Result<(), SyncError> {
    let target = Path::new(path);
    let parent = target
        .parent()
        .ok_or_else(|| SyncError::Write(format!("no parent directory for {path}")))?;
    std::fs::create_dir_all(parent)?;
    let tmp_path = format!("{path}.tmp");
    std::fs::write(&tmp_path, content)?;
    std::fs::rename(&tmp_path, target)?;
    Ok(())
}

/// Validate that the output path is absolute, rooted under `/output`, and
/// contains no `..` traversal components.
///
/// # Errors
///
/// Returns [`SyncError::Write`] if the path is relative, escapes `/output`,
/// or contains parent-directory traversal.
pub fn validate_output_path(path: &str) -> Result<(), SyncError> {
    let p = Path::new(path);
    if !p.is_absolute() {
        return Err(SyncError::Write(format!("output path must be absolute: {path}")));
    }
    if !p.starts_with(OUTPUT_ROOT) {
        return Err(SyncError::Write(format!(
            "output path must be under {OUTPUT_ROOT}: {path}"
        )));
    }
    if path_contains_traversal(p) {
        return Err(SyncError::Write(format!("output path must not contain '..': {path}")));
    }
    Ok(())
}

/// Check whether a path contains any `..` (parent directory) components.
fn path_contains_traversal(path: &Path) -> bool {
    path.components().any(|c| matches!(c, Component::ParentDir))
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Atomic write creates a file with the expected content.
    #[test]
    fn write_atomic_creates_file() -> Result<(), SyncError> {
        let path = format!(
            "{}/overlay-sync-test-write-creates.json",
            std::env::temp_dir().display(),
        );
        write_atomic(&path, "test content")?;
        let content = std::fs::read_to_string(&path)?;
        assert_eq!(content, "test content", "written content should match");
        std::fs::remove_file(&path)?;
        Ok(())
    }

    /// Atomic write replaces an existing file.
    #[test]
    fn write_atomic_replaces_existing() -> Result<(), SyncError> {
        let path = format!(
            "{}/overlay-sync-test-write-replaces.json",
            std::env::temp_dir().display(),
        );
        write_atomic(&path, "first")?;
        write_atomic(&path, "second")?;
        let content = std::fs::read_to_string(&path)?;
        assert_eq!(content, "second", "content should be replaced");
        std::fs::remove_file(&path)?;
        Ok(())
    }

    /// Relative paths are rejected.
    #[test]
    fn validate_rejects_relative_path() {
        assert!(
            validate_output_path("relative/path.json").is_err(),
            "relative path should be rejected",
        );
    }

    /// Paths containing `..` are rejected.
    #[test]
    fn validate_rejects_path_traversal() {
        assert!(
            validate_output_path("/output/../etc/passwd").is_err(),
            "path traversal should be rejected",
        );
    }

    /// Absolute paths outside `/output` are rejected.
    #[test]
    fn validate_rejects_path_outside_output_root() {
        assert!(
            validate_output_path("/etc/passwd").is_err(),
            "paths outside /output should be rejected",
        );
    }

    /// Absolute paths without traversal are accepted.
    #[test]
    fn validate_accepts_clean_absolute_path() -> Result<(), SyncError> {
        validate_output_path("/output/grid-config.json")?;
        Ok(())
    }

    /// Path traversal detection on the component level.
    #[test]
    fn path_traversal_detected() {
        assert!(
            path_contains_traversal(Path::new("/a/../b")),
            ".. component should be detected",
        );
    }

    /// Clean paths do not trigger traversal detection.
    #[test]
    fn clean_path_no_traversal() {
        assert!(
            !path_contains_traversal(Path::new("/a/b/c")),
            "clean path should not trigger traversal",
        );
    }
}
