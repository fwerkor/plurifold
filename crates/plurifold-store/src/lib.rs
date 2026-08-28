use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sha2::{Digest, Sha256};
use thiserror::Error;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Error)]
pub enum StoreError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("unsupported digest {0}")]
    UnsupportedDigest(String),
    #[error("object digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch { expected: String, actual: String },
}

#[derive(Clone, Debug)]
pub struct StoredObject {
    pub digest: String,
    pub size_bytes: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let root = root.into();
        fs::create_dir_all(root.join("sha256"))?;
        Ok(Self { root })
    }

    pub fn put(&self, bytes: &[u8]) -> Result<StoredObject, StoreError> {
        let hex_digest = hex::encode(Sha256::digest(bytes));
        let digest = format!("sha256:{hex_digest}");
        let path = self.root.join("sha256").join(&hex_digest);
        if !path.exists() {
            let temp_id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let tmp = self.root.join("sha256").join(format!(
                ".{hex_digest}.{}.{}.tmp",
                std::process::id(),
                temp_id
            ));
            fs::write(&tmp, bytes)?;
            match fs::rename(&tmp, &path) {
                Ok(()) => {}
                Err(_) if path.exists() => {
                    let _ = fs::remove_file(&tmp);
                }
                Err(error) => return Err(StoreError::Io(error)),
            }
        }
        Ok(StoredObject {
            digest,
            size_bytes: bytes.len() as u64,
            path,
        })
    }

    pub fn get(&self, digest: &str) -> Result<Vec<u8>, StoreError> {
        let path = self.path_for_digest(digest)?;
        let bytes = fs::read(path)?;
        self.verify(digest, &bytes)?;
        Ok(bytes)
    }

    pub fn contains(&self, digest: &str) -> bool {
        self.path_for_digest(digest)
            .map(|path| path.is_file())
            .unwrap_or(false)
    }

    pub fn verify(&self, expected: &str, bytes: &[u8]) -> Result<(), StoreError> {
        let actual = format!("sha256:{}", hex::encode(Sha256::digest(bytes)));
        if actual == expected {
            Ok(())
        } else {
            Err(StoreError::DigestMismatch {
                expected: expected.to_owned(),
                actual,
            })
        }
    }

    pub fn path_for_digest(&self, digest: &str) -> Result<PathBuf, StoreError> {
        let hex_digest = digest
            .strip_prefix("sha256:")
            .ok_or_else(|| StoreError::UnsupportedDigest(digest.to_owned()))?;
        if hex_digest.len() != 64 || !hex_digest.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(StoreError::UnsupportedDigest(digest.to_owned()));
        }
        Ok(self.root.join("sha256").join(hex_digest))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_addressing_deduplicates_and_verifies() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path()).unwrap();
        let first = store.put(b"plurifold").unwrap();
        let second = store.put(b"plurifold").unwrap();
        assert_eq!(first.digest, second.digest);
        assert_eq!(store.get(&first.digest).unwrap(), b"plurifold");
    }

    #[test]
    fn corrupted_bytes_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path()).unwrap();
        let object = store.put(b"correct").unwrap();
        fs::write(&object.path, b"wrong").unwrap();
        assert!(matches!(
            store.get(&object.digest),
            Err(StoreError::DigestMismatch { .. })
        ));
    }

    #[test]
    fn concurrent_writers_converge_on_one_blob() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path()).unwrap();
        let threads = (0..8)
            .map(|_| {
                let store = store.clone();
                std::thread::spawn(move || store.put(b"same-content").unwrap().digest)
            })
            .collect::<Vec<_>>();
        let digests = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert!(digests.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(fs::read_dir(dir.path().join("sha256")).unwrap().count(), 1);
    }
}
