use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    time::{self, Instant},
};

use crate::{
    config::Config,
    transport::{Transport, TransportData},
};

#[derive(Debug, Clone)]
pub struct SemaphoreConfig {
    /// How long it takes to revoke a lock.
    pub lock_timeout: Duration,
}

pub async fn run_semaphore(
    transport: &impl Transport,
    config: &mut broadcast::Receiver<Config>,
    client_sender: oneshot::Sender<SemaphoreClient>,
) -> anyhow::Result<()> {
    let (client, mut rx) = SemaphoreClient::new();
    client_sender.send(client).unwrap();

    let config = config.recv().await;

    let mut heartbeat_interval = time::interval(global.config.semaphore_config.lock_timeout);

    loop {
        let _ = tokio::select! {
            instant = heartbeat_interval.tick() => handle_heartbeat(instant, transport, global).await,
            Some((bytes, from)) = transport.recv() => handle_transport_message(bytes, from, transport, global).await
        };
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SemaphoreMessage {}

impl TransportData for SemaphoreMessage {}

impl StateMachine {}

async fn handle_transport_message(
    bytes: &[u8],
    from: NodeId,
    transport: &impl Transport,
    global: &Global,
) -> anyhow::Result<()> {
}

async fn handle_heartbeat(
    instant: Instant,
    transport: &impl Transport,
    global: &Global,
) -> anyhow::Result<()> {
}

#[derive(Debug)]
pub enum SemaphoreClientRequest {}

#[derive(Debug, Clone)]
pub struct SemaphoreClient {
    sender: mpsc::Sender<SemaphoreClientRequest>,
}

impl SemaphoreClient {
    pub fn new() -> (Self, mpsc::Receiver<SemaphoreClientRequest>) {
        let (tx, rx) = mpsc::channel(128);
        return (Self { sender: tx }, rx);
    }
}
