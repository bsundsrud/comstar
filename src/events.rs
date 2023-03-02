use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::{sync::mpsc::Receiver, time::Instant};

#[derive(Debug, Clone)]
pub enum Event {
    CloseStream,
    FileStarted { name: String, size: Option<u64> },
    FileProgress { name: String, bytes: u64 },
    FileDone { name: String },
}

impl Event {
    pub fn unknown_file_started<S: Into<String>>(name: S) -> Self {
        Event::FileStarted {
            name: name.into(),
            size: None,
        }
    }

    pub fn file_started<S: Into<String>>(name: S, size: u64) -> Self {
        Event::FileStarted {
            name: name.into(),
            size: Some(size),
        }
    }

    pub fn file_progress<S: Into<String>>(name: S, delta_bytes: u64) -> Self {
        Event::FileProgress {
            name: name.into(),
            bytes: delta_bytes,
        }
    }

    pub fn file_done<S: Into<String>>(name: S) -> Self {
        Event::FileDone { name: name.into() }
    }

    pub fn close() -> Self {
        Event::CloseStream
    }
}

fn create_spinner(size: u64) -> ProgressBar {
    let style = ProgressStyle::with_template(
        "  {spinner} {msg} ({bytes}, {binary_bytes_per_sec} {elapsed})",
    )
    .unwrap();
    let pb = ProgressBar::new_spinner();
    pb.set_length(size);
    pb.set_style(style);
    pb
}

fn create_unknown_spinner() -> ProgressBar {
    let style = ProgressStyle::with_template("  {spinner} {msg} ({elapsed})").unwrap();
    let pb = ProgressBar::new_spinner();
    pb.set_style(style);
    pb
}

fn header_spinner() -> ProgressBar {
    let style = ProgressStyle::with_template("{spinner} {msg} ({pos}/? {elapsed})").unwrap();
    let pb = ProgressBar::new_spinner();
    pb.set_style(style);
    pb
}

fn header_progress(max: u64) -> ProgressBar {
    let style = ProgressStyle::with_template("[{bar}] {msg} ({pos}/{len} {elapsed})").unwrap();
    let pb = ProgressBar::new(max);
    pb.set_style(style);
    pb
}

pub async fn event_output(mut ch: Receiver<Event>, action: String, max_items: u64) -> Result<()> {
    let mp = MultiProgress::new();
    let mut current_pbs: HashMap<String, ProgressBar> = HashMap::new();
    let header = mp.add(header_progress(max_items));

    header.enable_steady_tick(Duration::from_millis(100));
    header.set_message(action.clone());
    loop {
        if let Some(e) = ch.recv().await {
            match e {
                Event::CloseStream => break,
                Event::FileStarted { name, size } => {
                    let pb = if let Some(s) = size {
                        mp.add(create_spinner(s))
                    } else {
                        mp.add(create_unknown_spinner())
                    };
                    pb.enable_steady_tick(Duration::from_millis(100));
                    pb.set_message(name.clone());
                    current_pbs.insert(name.clone(), pb);
                }
                Event::FileProgress { name, bytes } => {
                    if let Some(pb) = current_pbs.get(&name) {
                        pb.inc(bytes);
                    }
                }
                Event::FileDone { name } => {
                    if let Some(pb) = current_pbs.get(&name) {
                        pb.finish_and_clear();
                    }
                    header.inc(1);
                    current_pbs.remove(&name);
                }
            }
        } else {
            break;
        }
    }
    mp.clear()?;
    header.finish_with_message(format!("{}: Done.", action));
    Ok(())
}
