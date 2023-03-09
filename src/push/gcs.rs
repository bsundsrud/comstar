use std::{path::Path, collections::HashMap, sync::Arc};
use async_compression::tokio::bufread::GzipEncoder;
use google_cloud_storage::{client::{Client, ClientConfig}, http::{objects::{upload::{UploadType, UploadObjectRequest}, Object, delete::DeleteObjectRequest}, storage_client::StorageClient}};
use relative_path::{RelativePathBuf, RelativePath};
use tokio::{fs::File, io::BufReader, sync::Semaphore};
use tokio_util::io::ReaderStream;
use anyhow::Result;

use crate::{manifest::Manifest, events::{Event, self}};
use google_cloud_default::WithAuthExt;

fn make_meta<S: Into<String>>(bucket: S, name: S, content_type: String) -> Object {
    let name = name.into();
    Object {
        bucket: bucket.into(),
        name,
        content_encoding: Some("gzip".to_string()),
        content_type: Some(content_type),

        ..Default::default()
    }
}

pub enum ManifestDiff {
    Update(RelativePathBuf),
    Delete(RelativePathBuf)
}

pub async fn delete_object(client: &StorageClient, bucket: &str, object: &RelativePath) -> Result<()> {
    client.delete_object(&DeleteObjectRequest {
        bucket: bucket.to_string(),
        object: object.to_string(),
        .. Default::default()
    }, None).await?;
    Ok(())
}

pub async fn upload_object(client: &StorageClient, bucket: &str, path: &RelativePath, local_file: &Path) -> Result<Object> {
    let content_type = mime_guess::from_path(&local_file).first().map(|m| m.to_string()).unwrap_or_else(|| "application/octet-stream".to_string());
    let meta = make_meta(bucket, path.as_ref(), content_type);
    let f = File::open(local_file).await?;
    let reader = BufReader::new(f);
    let upload_type = UploadType::Multipart(Box::new(meta));
    let gz_encoder = GzipEncoder::new(reader);
    let stream =ReaderStream::new(gz_encoder);
    let upload = client.upload_streamed_object(&UploadObjectRequest {
        bucket: bucket.to_string(),
        ..Default::default()
    }, stream, &upload_type, None).await?;

    Ok(upload)
}

fn diff_manifests(local: &Manifest, remote: Option<&Manifest>) -> Vec<ManifestDiff> {
    let local_map: HashMap<&RelativePath, &str> = local.entries.iter().map(|e| (e.path.as_relative_path(), e.sha512.as_ref())).collect();
    let remote_map: Option<HashMap<&RelativePath, &str>> = remote.map(|m| m.entries.iter().map(|e| (e.path.as_relative_path(), e.sha512.as_ref())).collect());
    let mut update_list = Vec::new();
    if let Some(remote_map) = remote_map {
        for (k, v) in local_map.iter() {
            if let Some(remote_sha) = remote_map.get(k) {
                if remote_sha != v {
                    update_list.push( ManifestDiff::Update(k.to_relative_path_buf()));
                }
            } else {
                update_list.push(ManifestDiff::Update(k.to_relative_path_buf()));
            }
        }

        for k in remote_map.keys() {
            if !local_map.contains_key(k) {
                update_list.push(ManifestDiff::Delete(k.to_relative_path_buf()));
            }
        }
    } else {
        for k in local_map.keys() {
            update_list.push(ManifestDiff::Update(k.to_relative_path_buf()));
        }
    }

    update_list
}

pub async fn push_dir(base: &Path, local_manifest: &Manifest, remote_manifest: Option<&Manifest>, bucket: &str, bucket_prefix: Option<RelativePathBuf>) -> Result<()> {
    let config = ClientConfig::default().with_auth().await?;
    let client = Client::new(config);

    let mut diffs = diff_manifests(local_manifest, remote_manifest);
    if !diffs.is_empty() {
        diffs.push(ManifestDiff::Update(RelativePathBuf::from("comstar.json")));
    }
    let sem = Arc::new(Semaphore::new(10));

    let (tx, rx) = tokio::sync::mpsc::channel(50);
    let h = tokio::spawn(events::event_output(
        rx,
        "Pushing differences".into(),
        diffs.len() as u64,
    ));
    let mut handles = Vec::new();

    for d in diffs {
        let base = base.to_path_buf();
        let bucket = bucket.to_string();
        let permit = sem.clone().acquire_owned().await?;
        let bucket_prefix = bucket_prefix.clone();
        let t = tx.clone();
        let client = client.clone();
        let fut = async move {
            
            match d {
                ManifestDiff::Update(rel_path) => {
                    let path = if let Some(ref p) = bucket_prefix {
                        p.join(rel_path)
                    } else {
                        rel_path
                    };
                    let local_file = path.to_path(base);
                    t.send(Event::unknown_file_started(&path.to_string())).await?;
                    let _obj = upload_object(&client, &bucket, &path, &local_file).await?;
                    t.send(Event::file_done(&path.to_string())).await?;

                },
                ManifestDiff::Delete(rel_path) => {
                    let path = if let Some(ref p) = bucket_prefix {
                        p.join(rel_path)
                    } else {
                        rel_path
                    };
                    t.send(Event::unknown_file_started(&path.to_string())).await?;
                    delete_object(&client, &bucket, &path).await?;
                    t.send(Event::file_done(&path.to_string())).await?;
                },
            }
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