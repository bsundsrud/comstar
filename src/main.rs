use std::{fs, io::BufWriter, path::PathBuf};

use anyhow::{bail, Result};
use structopt::StructOpt;
use url::Url;
use validate::DifferenceType;

mod manifest;
mod util;
mod validate;
fn parse_url(s: &str) -> Result<Url> {
    Ok(Url::parse(s)?)
}

#[derive(Debug, StructOpt)]
#[structopt(about = "Sync files from a static source.")]
enum Args {
    #[structopt(about = "Generate manifests for directories.")]
    Generate {
        #[structopt(
            short,
            long,
            parse(from_os_str),
            help = "Directory to generate manifest for. Default is current directory."
        )]
        dir: Option<PathBuf>,
        #[structopt(
            short = "t",
            long = "target",
            help = "URL to write manifest for.  Defaults to local filesystem, current directory.",
            parse(try_from_str = parse_url)
        )]
        target: Option<Url>,
    },
    #[structopt(about = "Sync a directory from a manifest.")]
    Sync {
        #[structopt(
            short,
            long,
            parse(try_from_str = parse_url),
            help = "URI to manifest to sync against.  Defaults to looking for manifest in current dir."
        )]
        manifest: Option<Url>,
        #[structopt(
            short,
            long,
            parse(from_os_str),
            help = "Directory to sync to. Default is current directory."
        )]
        dir: Option<PathBuf>,
        #[structopt(
            short,
            long,
            help = "Ensure that ONLY files in the manifest are at the destination. Deletes any file not in the manifest."
        )]
        force: bool,
    },
    Validate {
        #[structopt(
            short,
            long,
            parse(try_from_str = parse_url),
            help = "URI of manifest to validate against.  Defaults to looking for manifest in current dir."
        )]
        manifest: Option<Url>,
        #[structopt(
            short,
            long,
            parse(from_os_str),
            help = "Directory to validate. Default is current directory."
        )]
        dir: Option<PathBuf>,
        #[structopt(
            short,
            long,
            help = "Ensure that ONLY files in the manifest are at the destination. Complains about any file not in the manifest."
        )]
        force: bool,
    },
}

fn base_dir(d: Option<PathBuf>) -> Result<PathBuf> {
    let dir = if let Some(d) = d {
        d
    } else {
        std::env::current_dir()?
    };
    Ok(dir.canonicalize()?)
}

fn main() -> Result<()> {
    let args = Args::from_args();
    match args {
        Args::Generate { dir, target } => {
            let generate_dir = base_dir(dir)?;
            let default_url = Url::from_directory_path(&generate_dir).map_err(|_| {
                anyhow::format_err!("Cannot make URL from directory {}", &generate_dir.display())
            })?;
            let target_url = target.unwrap_or(default_url);
            let manifest = manifest::generate_manifest(target_url, &generate_dir)?;

            let manifest_file = fs::OpenOptions::new()
                .truncate(true)
                .write(true)
                .create(true)
                .open(&generate_dir.join("comstar.json"))?;
            let writer = BufWriter::new(manifest_file);
            serde_json::to_writer_pretty(writer, &manifest)?;
        }
        Args::Sync { .. } => todo!(),
        Args::Validate {
            manifest,
            dir,
            force,
        } => {
            let validate_dir = base_dir(dir)?;
            let default_manifest = validate_dir.join("comstar.json");
            let default_url = Url::from_file_path(&default_manifest).map_err(|_| {
                anyhow::format_err!(
                    "Cannot make URL from directory {}",
                    &default_manifest.display()
                )
            })?;
            let target_url = manifest.unwrap_or(default_url);
            let differences = validate::verify_manifest(&target_url, &validate_dir, force)?;
            if differences.len() == 0 {
                println!("All files validated.");
            } else {
                let mut missing_count = 0;
                let mut hash_mismatch_count = 0;
                let mut unknown_count = 0;
                println!("DIFFERENCES");
                println!("-----------");
                for diff in differences {
                    let p = diff.path.to_string_lossy();
                    match &diff.ty {
                        DifferenceType::FileMissing => {
                            missing_count += 1;
                            println!("  MISSING FILE: {}", p);
                        }
                        DifferenceType::HashMismatch { .. } => {
                            hash_mismatch_count += 1;
                            println!("  HASH MISMATCH: {}", p);
                        }
                        DifferenceType::UnknownFile => {
                            unknown_count += 1;
                            println!("  UNKNOWN FILE: {}", p);
                        }
                    }
                }
                println!("");
                if force {
                    println!(
                        "Missing items: {}, Desynced items: {}, Untracked items: {}",
                        missing_count, hash_mismatch_count, unknown_count
                    );
                } else {
                    println!(
                        "Missing items: {}, Desynced items: {}",
                        missing_count, hash_mismatch_count
                    );
                }
                bail!("Validation failed.");
            }
        }
    }
    Ok(())
}
