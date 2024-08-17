use std::{path::PathBuf, time::Duration};

use fuse::SqliteFuse;
use fuser::BackgroundSession;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Instant},
};

use crate::{
    config::Config,
    locks::LockHandle,
    transport::{NodeId, Transport, TransportData},
};

pub mod db_registry;
pub mod file_format;
pub mod fuse;
pub mod wal;

#[derive(Debug, Clone)]
pub struct SqliteConfig {
    data_dir: PathBuf,
    mount_dir: PathBuf,
    heartbeat: Duration,
    sender: mpsc::Sender<SqliteClientReq>,
}

struct State {
    fs: BackgroundSession,
    databases: Vec<Database>,
}

#[derive(Debug, Clone)]
pub struct Database {
    id: u64,
    name: String,
    replica_type: DatabaseReplica,
    lock: LockHandle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DatabaseReplica {
    Candidate,
    Replica,
    Remote,
}

pub async fn run_sqlite_fuse(
    transport: &impl Transport,
    config: Config,
    client_rx: &mut mpsc::Receiver<SqliteClientReq>,
) -> anyhow::Result<()> {
    let filesystem = SqliteFuse::new(config.to_owned()).await;
    let filesystem_handle = filesystem.start()?;

    let mut state = State {
        fs: filesystem_handle,
        databases: Vec::new(),
    };

    let mut heartbeat = time::interval(config.sqlite.heartbeat);

    loop {
        let _ = tokio::select! {
            Some(req) = client_rx.recv() => handle_client_req(req, transport, config.to_owned(), &mut state).await,
            Some((bytes, from)) = transport.recv() => handle_message(bytes, from, transport, config.to_owned(), &mut state).await,
            instant = heartbeat.tick() => handle_heartbeat(instant, transport, config.to_owned(), &mut state).await
        };
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SqliteMessage {}

impl TransportData for SqliteMessage {}

async fn handle_message(
    bytes: &[u8],
    from: NodeId,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
}

async fn handle_heartbeat(
    instant: Instant,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
}

enum SqliteClientReq {}

async fn handle_client_req(
    req: SqliteClientReq,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    Ok(())
}

#[derive(Debug, Clone)]
pub struct SqliteClient {
    sender: mpsc::Sender<SqliteClientReq>,
}

impl From<Config> for SqliteClient {
    fn from(conf: Config) -> Self {
        Self {
            sender: conf.sqlite.sender,
        }
    }
}
