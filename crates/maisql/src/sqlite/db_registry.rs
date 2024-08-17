use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, time::{self, Instant}};

use crate::{config::Config, locks::LockHandle, transport::{NodeId, Transport}};

#[derive(Debug, Clone)]
pub struct DbRegistryConfig {
    leader_candidate: bool,
    heartbeat_interval: Duration,
    client: mpsc::Sender<DbRegistryClientReq>,
}

pub async fn run_registry(transport: &impl Transport, config: Config, client_rx: &mut mpsc::Receiver<DbRegistryClientReq>) -> anyhow::Result<()> {
    let mut state = State {
        db_registry_lock: config.locks_client().lock("internal/db_registry/leader".to_string(), None)
    };

    if config.db_registry.leader_candidate {
        state.db_registry_lock.lock().await;
    }

    let mut heartbeat_interval = time::interval(config.db_registry.heartbeat_interval);

    loop {
        let _  = tokio::select! {
            Some((bytes, from)) = transport.recv() => handle_transport_message(bytes, from, transport, config.to_owned(), &mut state).await,
            Some(req) = client_rx.recv() => handle_client_req(req, transport, config.to_owned(), &mut state).await,
            instant = heartbeat_interval.tick() => handle_heartbeat(instant, transport, config.to_owned(), &mut state).await
        }
    }
}

#[derive(Debug, Clone)]
struct State {
    db_registry_lock: LockHandle
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "db_registry_msg_type")]
enum DbRegistryMessage {}

async fn handle_transport_message(bytes: &[u8], from: NodeId, transport: &impl Transport, config: Config, state: &mut State) -> anyhow::Result<()> {

}

async fn handle_heartbeat(instant: Instant, transport: &impl Transport, config: Config, state: &mut State) -> anyhow::Result<()> {

}

pub enum DbRegistryClientReq {}

async fn handle_client_req(req: DbRegistryClientReq, transport: &impl Transport, config: Config, state: &mut State) -> anyhow::Result<()> {

}

#[derive(Debug, Clone)]
pub struct DbRegistryClient {
    sender: mpsc::Sender<DbRegistryClientReq>,
    config: Config
}

impl DbRegistryClient {}

impl From<Config> for DbRegistryClient {
    fn from(value: Config) -> Self {
        Self {
            sender: value.clone().db_registry.client,
            config: value
        }
    }
}
