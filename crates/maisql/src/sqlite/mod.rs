use std::path::PathBuf;

use fuse::SqliteFuse;
use fuser::BackgroundSession;
use tokio::sync::mpsc;

use crate::{config::Config, transport::Transport};

pub mod file_format;
pub mod fuse;
pub mod wal;

#[derive(Debug, Clone)]
pub struct SqliteConfig {
    data_dir: PathBuf,
    mount_dir: PathBuf,
    sender: mpsc::Sender<SqliteReq>,
}

struct State {
    fs: BackgroundSession,
}

pub async fn run_sqlite_fuse(
    transport: &impl Transport,
    config: Config,
    client_rx: &mut mpsc::Receiver<SqliteReq>,
) -> anyhow::Result<()> {
    let filesystem = SqliteFuse::new(config.to_owned()).await;
    let filesystem_handle = filesystem.start()?;

    let mut state = State {
        fs: filesystem_handle,
    };

    loop {
        let _ = tokio::select! {
            Some(req) = client_rx.recv() => handle_client_req(req, transport, config.to_owned(), &mut state).await
        };
    }
}

enum SqliteReq {}

async fn handle_client_req(
    req: SqliteReq,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    Ok(())
}

#[derive(Debug, Clone)]
pub struct SqliteClient {
    sender: mpsc::Sender<SqliteReq>,
}

impl From<Config> for SqliteClient {
    fn from(conf: Config) -> Self {
        Self {
            sender: conf.sqlite.sender,
        }
    }
}
