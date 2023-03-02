use std::{fs::File, io::BufReader, path::Path};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use url::Url;

use crate::{util, events::Event};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Manifest {
    pub source: Url,
    pub generated_at: DateTime<Utc>,
    pub entries: Vec<ManifestEntry>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ManifestEntry {
    pub path: String,
    pub sha512: String,
    pub source: Url,
}

async fn get_manifest_http(target: &Url) -> Result<Manifest> {
    Ok(reqwest::blocking::get(target.as_ref())?.json()?)
}

async fn get_manifest_file(target: &Url) -> Result<Manifest> {
    let f = target
        .to_file_path()
        .map_err(|_| anyhow::anyhow!("Invalid file URL: {}", target))?;
    let file = File::open(&f)?;
    let br = BufReader::new(file);
    let manifest = serde_json::from_reader(br)?;
    Ok(manifest)
}

pub async fn get_manifest(target: &Url) -> Result<Manifest> {
    match target.scheme() {
        "http" | "https" => get_manifest_http(target).await,
        "file" => get_manifest_file(target).await,
        _ => unimplemented!(),
    }
}

async fn hash_with_events(p: &Path, tx: Sender<Event>) -> Result<String> {
    let name = p.file_name().ok_or_else(|| anyhow::anyhow!("Invalid file name passed to hash_with_events"))?.to_string_lossy();
    tx.send(Event::file_started(name.to_string())).await?;
    let sha512 = util::get_file_hash(&p)?;
    tx.send(Event::file_done(name.to_string())).await?;

    Ok(sha512)
}

pub  async fn generate_manifest(base_url: Url, dir: &Path, tx: Sender<Event>) -> Result<Manifest> {
    let walker = util::get_walker(dir)?;

    let mut entries = Vec::new();
    for dirent in walker {
        let dirent = dirent?;
        let c = dirent.path();
        // skip dirs, we only care about files and dirs are implied by paths
        if !c.is_file() {
            continue;
        }
        let relative = c.strip_prefix(dir)?;
        let relative_url = relative.to_str().ok_or_else(|| {
            anyhow::anyhow!("Invalid characters for URL in path: {}", relative.display())
        })?;
        let src_url = base_url.join(relative_url)?;

        let sha512 = hash_with_events(&c, tx.clone()).await?;
        entries.push(ManifestEntry {
            path: relative.to_string_lossy().to_string(),
            sha512,
            source: src_url,
        })
    }
    let manifest_file = base_url.join("comstar.json")?;
    Ok(Manifest {
        source: manifest_file,
        generated_at: Utc::now(),
        entries,
    })
}
