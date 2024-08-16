use std::path::Path;

use fuse::Filesystem;

use crate::config::Config;

#[derive(Debug, Clone)]
pub struct SqliteFuseConfig {
    data_dir: Path,
    mount_dir: Path,
}

#[derive(Debug)]
pub struct SqliteFuse {
    config: Config,
}

impl SqliteFuse {}

impl Filesystem for SqliteFuse {}
