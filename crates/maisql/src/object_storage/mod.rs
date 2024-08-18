pub mod fuse;

use std::path::PathBuf;

use fuse::ObjectStorageFuse;
use fuser::BackgroundSession;
use tokio::sync::mpsc;

use crate::{config::Config, transport::Transport};

#[derive(Debug, Clone)]
pub struct ObjectStorageConfig {
    min_replicated_regions: u8,
    /// number of datacenters/region to replicate files to.
    min_replicated_dcs: u8,
    client_sender: mpsc::Sender<ObjectStorageClientReq>,
    mount_dir: PathBuf,
    data_dir: PathBuf,
}

struct State {
    fs: BackgroundSession,
}

pub async fn run_object_storage(
    transport: &impl Transport,
    config: Config,
    client_rx: &mut mpsc::Receiver<ObjectStorageClientReq>,
) -> anyhow::Result<()> {
    let filesystem = ObjectStorageFuse::new(config.to_owned()).await;
    let filesystem_handle = filesystem.start()?;

    let mut state = State {
        fs: filesystem_handle,
    };

    loop {
        let _ = tokio::select! {
            Some(req) = client_rx.recv() => handle_client_req(req, transport, config.to_owned(), &mut state).await,
        };
    }
}

pub enum ObjectStorageClientReq {}

async fn handle_client_req(
    req: ObjectStorageClientReq,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
}

#[derive(Debug, Clone)]
pub struct ObjectStorageClient {
    sender: mpsc::Sender<ObjectStorageClientReq>,
    config: Config,
}

impl From<Config> for ObjectStorageClient {
    fn from(value: Config) -> Self {
        Self {
            sender: value.clone().object_storage.client_sender,
            config: value,
        }
    }
}
