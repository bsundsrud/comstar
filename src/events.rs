use std::collections::HashMap;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::{sync::mpsc::Receiver, time::Instant};
use anyhow::Result;

#[derive(Debug, Clone)]
pub enum Event {
    CloseStream,
    FileStarted {
        name: String,
    },
    FileProgress {
        name: String,
        bytes: u64,
    },
    FileDone {
        name: String,
    }
}

impl Event {
    pub fn file_started<S: Into<String>>(name: S) -> Self {
        Event::FileStarted { name: name.into() }
    }

    pub fn file_progress<S: Into<String>>(name: S, delta_bytes: u64) -> Self {
        Event::FileProgress { name: name.into(), bytes: delta_bytes }
    }

    pub fn file_done<S: Into<String>>(name: S) -> Self {
        Event::FileDone { name: name.into() }
    }

    pub fn close() -> Self {
        Event::CloseStream
    }
}

fn create_spinner() -> ProgressBar {
    let style = ProgressStyle::with_template("  {spinner} {prefix}: {msg} ({bytes}, {binary_bytes_per_sec} {elapsed})").unwrap();
    let pb = ProgressBar::new_spinner();
    pb.set_style(style);
    pb
}

pub async fn event_output(mut ch: Receiver<Event>, action: String, max_items: Option<u64>) -> Result<()> {
    let mp = MultiProgress::new();
    let mut current_pbs: HashMap<String, ProgressBar> = HashMap::new();
    let mut files_done = 0;
    let mut files_started = 1;
    loop {
        if let Some(e) = ch.recv().await {
            if let Some(max) = max_items {
                mp.println(format!("{} ({}/{})", action, files_done, max))?;
            } else {
                mp.println(format!("{} ({}/?)", action, files_done))?;
            }
            match e {
                Event::CloseStream => break,
                Event::FileStarted { name } => {
                    let pb = mp.add(create_spinner());
                    pb.set_prefix(files_started.to_string());
                    files_started += 1;
                    pb.set_message(name.clone());
                    current_pbs.insert(name.clone(), pb);
                },
                Event::FileProgress { name, bytes } => {
                    if let Some(pb) = current_pbs.get(&name) {
                        pb.inc(bytes);
                    }
                },
                Event::FileDone { name } => {
                    files_done += 1;
                    if let Some(pb) = current_pbs.get(&name) {
                        mp.remove(pb);
                    }
                    current_pbs.remove(&name);
                },
            }   
        } else {
            break;
        }
    }
    Ok(())
}