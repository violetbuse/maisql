use std::{
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    time::{self, Instant},
};
use transport::Transport;

pub struct RaftState {
    pub cluster_client: cluster::ClusterClient,
}

pub struct TermInfo {
    pub term_id: u64,
    pub voted: bool,
    pub leader: Option<LeaderInfo>,
}

pub struct LeaderInfo {
    pub addr: SocketAddr,
    pub last_heartbeat: SystemTime,
}

pub struct RaftConfig {
    heartbeat_interval: Duration,
    cluster_client: cluster::ClusterClient,
}

pub async fn run_raft(
    config: RaftConfig,
    transport: &impl Transport,
    requests: &mut mpsc::Receiver<RaftRequest>,
) -> anyhow::Result<()> {
    let mut state = RaftState {
        cluster_client: config.cluster_client,
    };
    let mut interval = time::interval(config.heartbeat_interval);

    loop {
        let _ = tokio::select! {
            Some((bytes, from)) = transport.recv() => handle_transport_message(bytes, from, transport, &mut state).await,
            Some(request) = requests.recv() => handle_request(request, transport, &mut state).await,
            instant = interval.tick() => handle_heartbeat(&instant, transport, &mut state).await
        };
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "raft_transport_message")]
pub enum TransportMessage {}

impl TransportMessage {
    pub fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        if let Ok(parsed) = rmp_serde::from_slice(bytes) {
            return Ok(parsed);
        } else if let Ok(parsed) = serde_json::from_slice(bytes) {
            return Ok(parsed);
        } else {
            return Err(anyhow!(
                "Could not parse raft message from msgpack or json."
            ));
        }
    }
    pub fn serialize_msgpack(&self) -> anyhow::Result<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(|_| anyhow!("Could not serialize raft message to msgpack."))
    }
    pub fn serialize_json(&self) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec_pretty(self)
            .map_err(|_| anyhow!("Could not serialize raft message to json."))
    }
}

async fn handle_transport_message(
    bytes: &[u8],
    from: SocketAddr,
    transport: &impl Transport,
    state: &mut RaftState,
) -> anyhow::Result<()> {
    let message = TransportMessage::parse(bytes)?;

    Ok(())
}

#[derive(Debug, Clone)]
pub struct RaftClient {
    sender: mpsc::Sender<RaftRequest>,
}

impl RaftClient {}

pub enum RaftRequest {}

async fn handle_request(
    request: RaftRequest,
    transport: &impl Transport,
    state: &mut RaftState,
) -> anyhow::Result<()> {
    Ok(())
}

pub async fn handle_heartbeat(
    interval: &Instant,
    transport: &impl Transport,
    state: &mut RaftState,
) -> anyhow::Result<()> {
    Ok(())
}
