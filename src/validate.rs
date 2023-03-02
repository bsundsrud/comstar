use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use anyhow::Result;
use url::Url;

use crate::{
    manifest::{self, ManifestEntry},
    util,
};

#[derive(Debug, Clone)]
pub enum DifferenceType {
    FileMissing(ManifestEntry),
    HashMismatch {
        upstream: ManifestEntry,
        local: String,
    },
    UnknownFile,
}

#[derive(Debug)]
pub struct ValidationDifference {
    pub ty: DifferenceType,
    pub path: PathBuf,
}

impl ValidationDifference {
    fn missing<P: Into<PathBuf>>(path: P, entry: ManifestEntry) -> Self {
        Self {
            ty: DifferenceType::FileMissing(entry),
            path: path.into(),
        }
    }

    fn hash_mismatch<P: Into<PathBuf>>(path: P, upstream: ManifestEntry, local: String) -> Self {
        Self {
            ty: DifferenceType::HashMismatch { upstream, local },
            path: path.into(),
        }
    }

    fn unknown_file<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            ty: DifferenceType::UnknownFile,
            path: path.into(),
        }
    }
}

pub fn verify_manifest(target: &Url, dir: &Path, force: bool) -> Result<Vec<ValidationDifference>> {
    let manifest = manifest::get_manifest(&target)?;
    let mut differences = Vec::new();
    for e in manifest.entries.iter() {
        let local_path = dir.join(&e.path);
        if !local_path.exists() {
            differences.push(ValidationDifference::missing(&e.path, e.clone()));
            continue;
        }
        let sha512 = util::get_file_hash(&local_path)?;
        if sha512 != e.sha512 {
            differences.push(ValidationDifference::hash_mismatch(
                &e.path,
                e.clone(),
                sha512,
            ))
        }
    }
    if force {
        let fnames: HashSet<PathBuf> = manifest.entries.iter().map(|e| dir.join(&e.path)).collect();
        let walker = util::get_walker(&dir)?;
        for dirent in walker {
            let dirent = dirent?;
            let path = dirent.path();
            if !path.is_file() {
                continue;
            }
            if !fnames.contains(path) {
                differences.push(ValidationDifference::unknown_file(path));
            }
        }
    }

    Ok(differences)
}
