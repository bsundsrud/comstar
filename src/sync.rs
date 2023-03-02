use std::path::Path;

use anyhow::{anyhow, Result};
use futures::StreamExt;
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    runtime::{self, Runtime},
    sync::mpsc::Sender,
};
use url::Url;

use crate::{events::Event, validate};

async fn get_file_http(src: &Url, dest: &Path) -> Result<()> {
    let resp = reqwest::get(src.as_ref()).await?;
    let mut stream = resp.bytes_stream();
    let f = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(dest)
        .await?;
    let mut writer = BufWriter::new(f);
    while let Some(v) = stream.next().await {
        tokio::io::copy_buf(&mut v?.as_ref(), &mut writer).await?;
    }
    Ok(())
}

async fn get_file_file(src: &Url, dest: &Path) -> Result<()> {
    let path = src
        .to_file_path()
        .map_err(|_| anyhow!("Could not create path from URL {}", src))?;

    tokio::fs::copy(&path, dest).await?;
    Ok(())
}

pub async fn get_file(src: &Url, dest: &Path) -> Result<()> {
    match src.scheme() {
        "http" | "https" => get_file_http(src, dest).await,
        "file" => get_file_file(src, dest).await,
        _ => unimplemented!(),
    }
}

pub async fn delete_file(f: &Path) -> Result<()> {
    tokio::fs::remove_file(f).await?;
    Ok(())
}

pub async fn sync_manifest(target: &Url, dir: &Path, force: bool) -> Result<()> {
    // get differences
    let diff = validate::verify_manifest(target, dir, force).await?;
    // return early if there's nothing to do
    if diff.is_empty() {
        return Ok(());
    }
    // set up async runtime
    let mut builder = runtime::Builder::new_multi_thread();
    let rt = builder.enable_all().build()?;
    let mut handles = Vec::new();
    for d in diff {
        match d.ty {
            validate::DifferenceType::FileMissing(entry) => {
                let handle = rt.spawn(async move { get_file(&entry.source, &d.path).await });
                handles.push(handle);
            }
            validate::DifferenceType::HashMismatch { upstream, .. } => {
                let handle = rt.spawn(async move { get_file(&upstream.source, &d.path).await });
                handles.push(handle);
            }
            validate::DifferenceType::UnknownFile => {
                let handle = rt.spawn(async move { delete_file(&d.path).await });
                handles.push(handle);
            }
        }
    }
    for h in handles {
        let _ = rt.block_on(h)??;
    }
    todo!()
}
