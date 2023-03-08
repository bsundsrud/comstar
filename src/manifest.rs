use std::{fs::File, io::BufReader, path::Path, sync::Arc};

use anyhow::Result;
use chrono::{DateTime, Utc};
use path_slash::PathExt;
use relative_path::{RelativePath, RelativePathBuf};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::Sender, Semaphore};
use url::Url;

use crate::{
    events::{self, Event},
    util,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Manifest {
    pub source: Url,
    pub generated_at: DateTime<Utc>,
    pub entries: Vec<ManifestEntry>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ManifestEntry {
    pub path: RelativePathBuf,
    pub sha512: String,
    pub source: Url,
}

#[tracing::instrument]
async fn get_manifest_http(target: &Url) -> Result<Manifest> {
    Ok(reqwest::get(target.as_ref()).await?.json().await?)
}

#[tracing::instrument]
async fn get_manifest_file(target: &Url) -> Result<Manifest> {
    let f = target
        .to_file_path()
        .map_err(|_| anyhow::anyhow!("Invalid file URL: {}", target))?;
    let file = File::open(&f)?;
    let br = BufReader::new(file);
    let manifest = serde_json::from_reader(br)?;
    Ok(manifest)
}

#[tracing::instrument]
pub async fn get_manifest(target: &Url) -> Result<Manifest> {
    match target.scheme() {
        "http" | "https" => get_manifest_http(target).await,
        "file" => get_manifest_file(target).await,
        _ => unimplemented!(),
    }
}

#[tracing::instrument]
async fn hash_with_events(p: &Path, tx: Sender<Event>) -> Result<String> {
    let name = p
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("Invalid file name passed to hash_with_events"))?
        .to_string_lossy();
    tx.send(Event::unknown_file_started(name.to_string()))
        .await?;
    let sha512 = util::get_file_hash(&p)?;
    tx.send(Event::file_done(name.to_string())).await?;

    Ok(sha512)
}

#[tracing::instrument]
pub async fn generate_manifest(base_url: Url, dir: &Path) -> Result<Manifest> {
    let (tx, rx) = tokio::sync::mpsc::channel(50);

    let walker = util::get_walker(dir)?;
    let dirents: Vec<ignore::DirEntry> = walker
        .filter_map(|d| d.ok())
        .filter(|d| d.path().is_file())
        .collect();
    let count = dirents.len();
    let h = tokio::spawn(events::event_output(
        rx,
        "Generating manifest".into(),
        count as u64,
    ));
    let mut entries = Vec::new();
    let mut handles = Vec::new();
    let sem = Arc::new(Semaphore::new(10));
    for dirent in dirents {
        let c = dirent.path().to_path_buf();
        // skip dirs, we only care about files and dirs are implied by paths
        if !c.is_file() {
            continue;
        }

        let t = tx.clone();
        let dir = dir.to_path_buf();
        let base = base_url.clone();
        let permit = sem.clone().acquire_owned().await?;
        let fut = async move {
            let stripped_path = c.strip_prefix(dir)?.to_slash_lossy().to_string();
            let relative = RelativePath::from_path(&stripped_path)?;
            let src_url = base.join(relative.as_str())?;
            let sha512 = hash_with_events(&c, t).await?;
            drop(permit);
            Ok::<ManifestEntry, anyhow::Error>(ManifestEntry {
                path: relative.to_owned(),
                sha512,
                source: src_url,
            })
        };

        handles.push(tokio::spawn(fut));
    }

    for handle in handles {
        let e = handle.await??;
        entries.push(e);
    }
    tx.send(Event::close()).await?;
    h.await??;
    let manifest_file = base_url.join("comstar.json")?;
    Ok(Manifest {
        source: manifest_file,
        generated_at: Utc::now(),
        entries,
    })
}
