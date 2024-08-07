use std::{
    collections::HashMap,
    fmt::Debug,
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use anyhow::anyhow;
use futures::future;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    sync::oneshot,
    time::{self, Instant},
};
use transport::Transport;

struct NodeState {
    pub self_state: NodeData,
    pub seeds: Vec<SocketAddr>,
    pub decomission_wait: Duration,
    pub cluster: HashMap<SocketAddr, NodeData>,
}

impl NodeState {
    pub fn generate_digest(&self) -> HashMap<SocketAddr, u64> {
        self.cluster
            .iter()
            .map(|(addr, data)| (addr.to_owned(), data.version.to_owned()))
            .collect()
    }
    pub fn generate_delta(
        &self,
        digest: HashMap<SocketAddr, u64>,
    ) -> HashMap<SocketAddr, NodeData> {
        let mut delta = HashMap::new();

        for (addr, version) in digest.into_iter() {
            if addr == self.self_state.addr && version < self.self_state.version {
                delta.insert(self.self_state.addr, self.self_state.to_owned());
            }

            if let Some(data) = self.cluster.get(&addr) {
                if version < data.version {
                    delta.insert(addr, data.to_owned());
                }
            }
        }

        return delta;
    }
    pub fn apply_delta(&mut self, delta: HashMap<SocketAddr, NodeData>) {
        for (addr, data) in delta.into_iter() {
            if addr == self.self_state.addr {
                continue;
            }

            self.cluster
                .entry(addr)
                .and_modify(|val| *val = data.to_owned())
                .or_insert(data.to_owned());
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeData {
    pub name: String,
    pub addr: SocketAddr,
    pub last_heartbeat: SystemTime,
    pub metadata: HashMap<String, String>,
    pub to_be_decomissioned: bool,
    pub version: u64,
}

pub struct NodeConfig {
    pub name: String,
    pub heartbeat_interval: Duration,
    /// The minimum time to wait before deleting a `to_be_decomissioned`
    /// node.
    pub wait_decommission: Duration,
    pub seed_nodes: Vec<SocketAddr>,
}

pub async fn run_node(
    config: NodeConfig,
    transport: &impl Transport,
    requests: &mut mpsc::Receiver<ClusterRequest>,
) -> anyhow::Result<()> {
    let self_state = NodeData {
        name: config.name,
        addr: transport.addr(),
        last_heartbeat: SystemTime::now(),
        metadata: HashMap::new(),
        to_be_decomissioned: false,
        version: 1,
    };

    let mut state = NodeState {
        self_state,
        seeds: config.seed_nodes,
        decomission_wait: config.wait_decommission,
        cluster: HashMap::new(),
    };

    let mut interval = time::interval(config.heartbeat_interval);

    loop {
        let _ = tokio::select! {
            Some((bytes, from)) = transport.recv() => handle_transport_message(bytes, from, transport, &mut state).await,
            Some(request) = requests.recv() => handle_cluster_request(request, transport, &mut state).await,
            instant = interval.tick() => handle_heartbeat(instant, transport, &mut state).await
        };
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cluster_transport_message")]
pub enum TransportMessage {
    Syn {
        cluster_digest: HashMap<SocketAddr, u64>,
    },
    SynAck {
        cluster_digest: HashMap<SocketAddr, u64>,
        cluster_delta: HashMap<SocketAddr, NodeData>,
    },
    Ack {
        cluster_delta: HashMap<SocketAddr, NodeData>,
    },
    DecomissionNode {
        addr: SocketAddr,
    },
    RejoinNode {
        addr: SocketAddr,
    },
}

impl TransportMessage {
    pub fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        if let Ok(parsed) = rmp_serde::from_slice(bytes) {
            return Ok(parsed);
        } else if let Ok(parsed) = serde_json::from_slice(bytes) {
            return Ok(parsed);
        } else {
            return Err(anyhow!(
                "Could not parse node_cluster message from msgpack or json."
            ));
        }
    }
    pub fn serialize_msgpack(&self) -> anyhow::Result<Vec<u8>> {
        rmp_serde::to_vec(self)
            .map_err(|_| anyhow!("Could not serialize node_cluster message to msgpack."))
    }
    pub fn serialize_json(&self) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec_pretty(self)
            .map_err(|_| anyhow!("Could not serialize node_cluster message to json."))
    }
}

async fn handle_transport_message(
    bytes: &[u8],
    from: SocketAddr,
    transport: &impl Transport,
    state: &mut NodeState,
) -> anyhow::Result<()> {
    let message = TransportMessage::parse(bytes)?;

    match message {
        TransportMessage::Syn { cluster_digest } => {
            let cluster_delta = state.generate_delta(cluster_digest);
            let cluster_digest = state.generate_digest();
            let message = TransportMessage::SynAck {
                cluster_digest,
                cluster_delta,
            }
            .serialize_msgpack()?;

            transport.send(from, &message).await?;
        }
        TransportMessage::SynAck {
            cluster_digest,
            cluster_delta,
        } => {
            state.apply_delta(cluster_delta);

            let cluster_delta = state.generate_delta(cluster_digest);
            let message = TransportMessage::Ack { cluster_delta }.serialize_msgpack()?;

            transport.send(from, &message).await?;
        }
        TransportMessage::Ack { cluster_delta } => {
            state.apply_delta(cluster_delta);
        }
        TransportMessage::DecomissionNode { addr } => {
            state.cluster.entry(addr).and_modify(|node_data| {
                node_data.to_be_decomissioned = true;
                node_data.version += 1;
            });
        }
        TransportMessage::RejoinNode { addr } => {
            state.cluster.entry(addr).and_modify(|node_data| {
                node_data.to_be_decomissioned = false;
                node_data.version += 1;
            });
        }
    };

    Ok(())
}

#[derive(Debug)]
pub enum ClusterRequest {
    GetNodes {
        client: oneshot::Sender<HashMap<SocketAddr, NodeData>>,
    },
    GetNode {
        addr: SocketAddr,
        client: oneshot::Sender<Option<NodeData>>,
    },
    SetMetadata {
        key: String,
        value: String,
        client: oneshot::Sender<()>,
    },
    DeleteMetadata {
        key: String,
        client: oneshot::Sender<Option<String>>,
    },
}

async fn handle_cluster_request(
    request: ClusterRequest,
    _transport: &impl Transport,
    state: &mut NodeState,
) -> anyhow::Result<()> {
    match request {
        ClusterRequest::GetNodes { client } => {
            let mut result = state.cluster.clone();
            result.insert(state.self_state.addr, state.self_state.to_owned());

            client
                .send(result)
                .map(|_| ())
                .map_err(|_| anyhow!("could not send response to get_nodes request"))
        }
        ClusterRequest::GetNode { addr, client } => {
            let result = state.cluster.get(&addr).map(|addr| addr.to_owned());

            client
                .send(result)
                .map(|_| ())
                .map_err(|_| anyhow!("could not send response to get_node {:?} request", addr))
        }
        ClusterRequest::SetMetadata { key, value, client } => {
            state
                .self_state
                .metadata
                .entry(key.clone())
                .and_modify(|curr| *curr = value.clone())
                .or_insert(value.clone());

            state.self_state.version += 1;

            client
                .send(())
                .map(|_| ())
                .map_err(|_| anyhow!("could not confirm set_metadata {}: {} request", key, value))
        }
        ClusterRequest::DeleteMetadata { key, client } => {
            let prev = state.self_state.metadata.remove(&key);

            state.self_state.version += 1;

            client.send(prev).map(|_| ()).map_err(|_| {
                anyhow!(
                    "could not send previous data for delete_metadata {} request",
                    key
                )
            })
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterClient {
    sender: mpsc::Sender<ClusterRequest>,
}

impl ClusterClient {
    pub async fn list_nodes(&self) -> anyhow::Result<HashMap<SocketAddr, NodeData>> {
        let (tx, rx) = oneshot::channel();
        let request = ClusterRequest::GetNodes { client: tx };

        self.sender.send(request).await?;
        let response = rx.await?;

        return Ok(response);
    }
    pub async fn get_node(&self, addr: SocketAddr) -> anyhow::Result<Option<NodeData>> {
        let (tx, rx) = oneshot::channel();
        let request = ClusterRequest::GetNode { addr, client: tx };

        self.sender.send(request).await?;
        let response = rx.await?;

        return Ok(response);
    }
    pub async fn set_metadata(&self, key: String, value: String) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let request = ClusterRequest::SetMetadata {
            key,
            value,
            client: tx,
        };

        self.sender.send(request).await?;
        let response = rx.await?;

        return Ok(response);
    }
    pub async fn delete_metadata(&self, key: String) -> anyhow::Result<Option<String>> {
        let (tx, rx) = oneshot::channel();
        let request = ClusterRequest::DeleteMetadata { key, client: tx };

        self.sender.send(request).await?;
        let response = rx.await?;

        return Ok(response);
    }
}

async fn handle_heartbeat(
    _when: Instant,
    transport: &impl Transport,
    state: &mut NodeState,
) -> anyhow::Result<()> {
    state.self_state.last_heartbeat = SystemTime::now();
    state.self_state.version += 1;

    let mut candidates: Vec<SocketAddr> = state.seeds.to_owned();

    let mut cluster_nodes: Vec<SocketAddr> =
        state.cluster.keys().map(|addr| addr.to_owned()).collect();

    let mut rng = rand::thread_rng();

    cluster_nodes.shuffle(&mut rng);

    let mut selected_nodes: Vec<SocketAddr> = cluster_nodes
        .iter()
        .take(9)
        .map(|addr| addr.to_owned())
        .collect();

    candidates.append(&mut selected_nodes);

    candidates.shuffle(&mut rng);

    let cluster_digest = state.generate_digest();
    let syn = TransportMessage::Syn { cluster_digest };
    let data = syn.serialize_msgpack()?;

    let messages = candidates.into_iter().map(|addr| {
        return transport.send(addr, &data);
    });

    future::join_all(messages).await;

    let nodes_to_delete: Vec<_> = state
        .cluster
        .iter()
        .filter(|(_, data)| {
            let to_be_decommisioned = data.to_be_decomissioned;
            let fully_waited = SystemTime::now()
                .duration_since(data.last_heartbeat)
                .map(|duration| duration > state.decomission_wait)
                .unwrap_or(false);

            return to_be_decommisioned && fully_waited;
        })
        .map(|(addr, _)| addr.clone())
        .collect();

    for addr in nodes_to_delete {
        state.cluster.remove(&addr);
    }

    Ok(())
}
