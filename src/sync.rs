use std::{fs, path::Path, sync::Arc};

use anyhow::{anyhow, Result};
use futures::StreamExt;
use tokio::{
    io::AsyncWriteExt,
    sync::{mpsc::Sender, Semaphore},
};
use url::Url;

use crate::{
    events::{self, Event},
    validate,
};

#[tracing::instrument]
async fn get_file_http(src: &Url, dest: &Path, tx: Sender<Event>) -> Result<()> {
    let resp = reqwest::get(src.as_ref()).await?;
    let fname = dest.file_name().unwrap().to_string_lossy().to_string();
    if let Some(p) = dest.parent() {
        fs::create_dir_all(p)?;
    }
    let mut stream = resp.bytes_stream();
    let mut f = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(dest)
        .await?;

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result?;
        let len = chunk.len() as u64;
        f.write_all(&chunk).await?;
        tx.send(Event::file_progress(&fname, len)).await?;
    }
    Ok(())
}

async fn get_file_file(src: &Url, dest: &Path) -> Result<()> {
    let path = src
        .to_file_path()
        .map_err(|_| anyhow!("Could not create path from URL {}", src))?;
    if let Some(p) = dest.parent() {
        fs::create_dir_all(p)?;
    }
    tokio::fs::copy(&path, dest).await?;
    Ok(())
}

#[tracing::instrument]
pub async fn get_file(src: &Url, dest: &Path, t: Sender<Event>) -> Result<()> {
    match src.scheme() {
        "http" | "https" => get_file_http(src, dest, t).await,
        "file" => get_file_file(src, dest).await,
        _ => unimplemented!(),
    }
}

pub async fn delete_file(f: &Path) -> Result<()> {
    tokio::fs::remove_file(f).await?;
    Ok(())
}

#[tracing::instrument]
pub async fn sync_manifest(target: &Url, dir: &Path, force: bool) -> Result<()> {
    // get differences
    let diff = validate::verify_manifest(target, dir, force).await?;
    // return early if there's nothing to do
    if diff.is_empty() {
        return Ok(());
    }
    let (tx, rx) = tokio::sync::mpsc::channel(50);
    let sem = Arc::new(Semaphore::new(10));
    let h = tokio::spawn(events::event_output(
        rx,
        "Synchronizing files".into(),
        diff.len() as u64,
    ));
    // set up async runtime
    let mut handles = Vec::new();
    for d in diff {
        let t = tx.clone();
        let permit = sem.clone().acquire_owned().await?;
        let sync_path = d.path.to_logical_path(dir);

        let fut = async move {
            let fname = &d.path.file_name().unwrap().to_string();
            t.send(Event::unknown_file_started(fname)).await?;
            match d.ty {
                validate::DifferenceType::FileMissing(entry) => {
                    get_file(&entry.source, &sync_path, t.clone()).await?;
                }
                validate::DifferenceType::HashMismatch { upstream, .. } => {
                    get_file(&upstream.source, &sync_path, t.clone()).await?;
                }
                validate::DifferenceType::UnknownFile => {
                    delete_file(&sync_path).await?;
                }
            }
            t.send(Event::file_done(fname)).await?;
            drop(permit);
            Ok::<(), anyhow::Error>(())
        };
        let handle = tokio::spawn(fut);
        handles.push(handle);
    }
    for h in handles {
        h.await??;
    }
    tx.send(Event::close()).await?;
    h.await??;

    Ok(())
}
