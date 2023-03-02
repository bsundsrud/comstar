use anyhow::Result;
use ignore::{overrides::OverrideBuilder, Walk, WalkBuilder};
use sha2::{Digest, Sha512};
use std::{fs::File, io, path::Path};

pub fn get_walker(dir: &Path) -> Result<Walk> {
    let mut builder = WalkBuilder::new(dir);
    builder.add_custom_ignore_filename(".comstarignore");
    builder.git_ignore(false);
    builder.git_exclude(false);
    builder.git_global(false);
    builder.ignore(false);

    let mut o = OverrideBuilder::new(dir);
    let o = o.add("!comstar.json")?;
    builder.overrides(o.build()?);

    Ok(builder.build())
}

pub fn get_file_hash(path: &Path) -> Result<String> {
    let mut hasher = Sha512::new();
    let mut input = File::open(&path)?;
    let _ = io::copy(&mut input, &mut hasher)?;
    let hash_bytes = hasher.finalize();
    Ok(format!("{:x}", &hash_bytes))
}
