use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use relative_path::{RelativePath, RelativePathBuf};
use tokio::sync::Semaphore;
use url::Url;

use crate::{
    events::{self, Event},
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
    pub path: RelativePathBuf,
}

impl ValidationDifference {
    fn missing<P: Into<RelativePathBuf>>(path: P, entry: ManifestEntry) -> Self {
        Self {
            ty: DifferenceType::FileMissing(entry),
            path: path.into(),
        }
    }

    fn hash_mismatch<P: Into<RelativePathBuf>>(
        path: P,
        upstream: ManifestEntry,
        local: String,
    ) -> Self {
        Self {
            ty: DifferenceType::HashMismatch { upstream, local },
            path: path.into(),
        }
    }

    fn unknown_file<P: Into<RelativePathBuf>>(path: P) -> Self {
        Self {
            ty: DifferenceType::UnknownFile,
            path: path.into(),
        }
    }
}

#[tracing::instrument]
pub async fn diff_manifests(
    authority: &Url,
    other: &Url,
    force: bool,
) -> Result<Option<Vec<ValidationDifference>>> {
    let authority_manifest = manifest::get_manifest(authority)
        .await?
        .ok_or_else(|| anyhow!("Remote manifest not found: {}", &authority))?;
    let local_manifest = manifest::get_manifest(other).await?;

    if let Some(local) = local_manifest {
        let mut differences = Vec::new();
        let authority_entries: HashMap<&RelativePath, &ManifestEntry> = authority_manifest
            .entries
            .iter()
            .map(|e| (e.path.as_ref(), e))
            .collect();
        let local_entries: HashMap<&RelativePath, &ManifestEntry> =
            local.entries.iter().map(|e| (e.path.as_ref(), e)).collect();

        for (k, v) in authority_entries.iter() {
            if let Some(local_entry) = local_entries.get(k) {
                if local_entry.sha512 != v.sha512 {
                    differences.push(ValidationDifference::hash_mismatch(
                        v.path.to_relative_path_buf(),
                        (*v).clone(),
                        local_entry.sha512.to_string(),
                    ));
                }
            } else {
                differences.push(ValidationDifference::missing(
                    v.path.to_relative_path_buf(),
                    (*v).clone(),
                ))
            }
        }
        if force {
            for (k, v) in local_entries {
                if !authority_entries.contains_key(&k) {
                    differences.push(ValidationDifference::unknown_file(v.path.clone()));
                }
            }
        }
        Ok(Some(differences))
    } else {
        Ok(None)
    }
}

#[tracing::instrument]
pub async fn verify_manifest(
    target: &Url,
    dir: &Path,
    force: bool,
) -> Result<Vec<ValidationDifference>> {
    let (tx, rx) = tokio::sync::mpsc::channel(50);
    let manifest = manifest::get_manifest(&target)
        .await?
        .ok_or_else(|| anyhow!("Remote manifest not found: {}", &target))?;
    let mut differences = Vec::new();
    let h = tokio::spawn(events::event_output(
        rx,
        "Validating files".into(),
        manifest.entries.len() as u64,
    ));
    let sem = Arc::new(Semaphore::new(10));
    let mut handles = Vec::new();

    for e in manifest.entries.iter() {
        //let local_path = dir.join(&e.path);
        let local_path = e.path.to_logical_path(&dir);
        let fname = local_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        tx.send(Event::unknown_file_started(fname.clone())).await?;
        let permit = sem.clone().acquire_owned().await?;
        let t = tx.clone();
        let e = e.clone();
        let fut = async move {
            if !local_path.exists() {
                t.send(Event::file_done(fname)).await?;
                return Ok::<Option<ValidationDifference>, anyhow::Error>(Some(
                    ValidationDifference::missing(&e.path, e.clone()),
                ));
            }
            let sha512 = util::get_file_hash(&local_path)?;
            if sha512 != e.sha512 {
                t.send(Event::file_done(fname)).await?;
                return Ok::<Option<ValidationDifference>, anyhow::Error>(Some(
                    ValidationDifference::hash_mismatch(&e.path, e.clone(), sha512),
                ));
            }
            t.send(Event::file_done(fname)).await?;
            drop(permit);
            Ok(None)
        };
        handles.push(tokio::spawn(fut));
    }
    for handle in handles {
        if let Some(d) = handle.await?? {
            differences.push(d);
        }
    }
    tx.send(Event::close()).await?;
    h.await??;
    if force {
        let (tx, rx) = tokio::sync::mpsc::channel(50);
        let fnames: HashSet<PathBuf> = manifest
            .entries
            .iter()
            .map(|e| e.path.to_logical_path(&dir))
            .collect();

        let walker: Vec<ignore::DirEntry> = util::get_walker(&dir)?
            .filter_map(|d| d.ok())
            .filter(|d| d.path().is_file())
            .collect();
        let h = tokio::spawn(events::event_output(
            rx,
            "Searching for untracked files".into(),
            walker.len() as u64,
        ));
        for dirent in walker {
            let path = dirent.path();
            let relative = RelativePathBuf::from_path(path.strip_prefix(dir)?)?;
            let fname = path.file_name().unwrap().to_string_lossy();
            tx.send(Event::unknown_file_started(fname.clone())).await?;
            if !fnames.contains(path) {
                differences.push(ValidationDifference::unknown_file(relative));
            }
            tx.send(Event::file_done(fname.clone())).await?;
        }
        tx.send(Event::close()).await?;
        h.await??;
    }

    Ok(differences)
}
