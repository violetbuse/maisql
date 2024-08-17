use crate::config::Config;
use crate::transport::{NodeId, Transport, TransportData};
use futures::future;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use std::{fmt::Debug, time::SystemTime};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Instant};

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub client: mpsc::Sender<ClusterClientRequest>,
    pub cluster_heartbeat: Duration,
    pub own_node_id: NodeId,
    pub own_region: String,
    pub own_dc: String,
    pub own_meta: HashMap<String, String>,
    pub seed_nodes: Vec<NodeId>,
    /// How many seconds to wait until a node is considered "sus".
    pub suspicious_timeout: Duration,
    /// How many random seed nodes to ping every heartbeat.
    pub seeds_to_ping: usize,
    /// How many sus nodes to ping every heartbeat.
    pub sus_to_ping: usize,
    /// How many of the other nodes to ping every heartbeat.
    pub nodes_to_ping: usize,
}

struct State {
    local_node: LocalNode,
    nodes: HashMap<NodeId, NodeState>,
}

struct LocalNode {
    id: NodeId,
    state: NodeState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeState {
    last_seen: SystemTime,
    raft_voter: bool,
    region: String,
    dc: String,
    meta: HashMap<String, String>,
    version: u64,
}

pub async fn run_node(
    transport: &impl Transport,
    config: Config,
    client_rx: &mut mpsc::Receiver<ClusterClientRequest>,
) -> anyhow::Result<()> {
    let mut state = State {
        local_node: LocalNode {
            id: config.cluster.own_node_id.to_owned(),
            state: NodeState {
                last_seen: SystemTime::now(),
                raft_voter: config.raft.raft_voter.to_owned(),
                region: config.cluster.own_region.to_owned(),
                dc: config.cluster.own_dc.to_owned(),
                meta: config.cluster.own_meta.to_owned(),
                version: 0,
            },
        },
        nodes: HashMap::new(),
    };

    let mut interval = time::interval(config.cluster.cluster_heartbeat);

    loop {
        let _ = tokio::select! {
            Some((bytes, from)) = transport.recv() => handle_transport_message(bytes, from, transport, config.clone(), &mut state).await,
            Some(req) = client_rx.recv() => handle_client_request(req, transport, config.clone(), &mut state).await,
            instant = interval.tick() => handle_heartbeat(instant, transport, config.clone(), &mut state).await
        };
    }
}

type ClusterDigest = HashMap<NodeId, u64>;
type ClusterDelta = HashMap<NodeId, NodeState>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cluster_msg_type")]
pub enum ClusterMessage {
    Syn {
        cluster_digest: ClusterDigest,
    },
    SynAck {
        cluster_digest: ClusterDigest,
        cluster_delta: ClusterDelta,
    },
    Ack {
        cluster_delta: ClusterDelta,
    },
}

impl TransportData for ClusterMessage {}

impl State {
    pub fn generate_cluster_digest(&self) -> ClusterDigest {
        let mut nodes = self.nodes.clone();
        nodes.insert(
            self.local_node.id.to_owned(),
            self.local_node.state.to_owned(),
        );

        return nodes
            .clone()
            .into_iter()
            .filter_map(|(id, value)| {
                if id == self.local_node.id {
                    return None;
                } else {
                    return Some((id, value.version));
                }
            })
            .collect();
    }
    pub fn generate_cluster_delta(&self, digest: ClusterDigest) -> ClusterDelta {
        let mut nodes = self.nodes.clone();
        nodes.insert(
            self.local_node.id.to_owned(),
            self.local_node.state.to_owned(),
        );

        return nodes
            .into_iter()
            .filter_map(|(id, value)| match digest.get(&id) {
                None => Some((id, value)),
                Some(digest_version) if digest_version < &value.version => Some((id, value)),
                _ => None,
            })
            .collect();
    }
    pub fn apply_cluster_delta(&mut self, delta: ClusterDelta) {
        for (id, node) in delta.iter() {
            if *id != self.local_node.id {
                self.nodes
                    .entry(id.clone())
                    .and_modify(|node_data| *node_data = node.clone())
                    .or_insert(node.clone());
            }
        }
    }
}

async fn handle_transport_message(
    bytes: &[u8],
    from: NodeId,
    transport: &impl Transport,
    _config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let message = ClusterMessage::parse(bytes, "cluster_message")?;

    match message {
        ClusterMessage::Syn { cluster_digest } => {
            let delta = state.generate_cluster_delta(cluster_digest);
            let digest = state.generate_cluster_digest();

            let message = ClusterMessage::SynAck {
                cluster_digest: digest,
                cluster_delta: delta,
            };

            transport
                .send_unreliable(from, &message.serialize_for_send())
                .await;
        }
        ClusterMessage::SynAck {
            cluster_digest,
            cluster_delta,
        } => {
            state.apply_cluster_delta(cluster_delta);
            let delta = state.generate_cluster_delta(cluster_digest);

            let message = ClusterMessage::Ack {
                cluster_delta: delta,
            };

            transport
                .send_unreliable(from, &message.serialize_for_send())
                .await;
        }
        ClusterMessage::Ack { cluster_delta } => {
            state.apply_cluster_delta(cluster_delta);
        }
    };

    Ok(())
}

async fn handle_heartbeat(
    _when: Instant,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    // let messages: Vec<(NodeId, Vec<u8>)> = global.map_state_mut(|state| {
    let mut rng = rand::thread_rng();

    let mut nodes = Vec::new();
    let digest = state.generate_cluster_digest();

    let mut seed_nodes: Vec<_> = config
        .cluster
        .seed_nodes
        .iter()
        .cloned()
        .choose_multiple(&mut rng, config.cluster.seeds_to_ping);

    let mut suspicious_nodes: Vec<_> = state
        .nodes
        .clone()
        .iter()
        .filter_map(|(id, node_data)| {
            let suspicious = SystemTime::now()
                .duration_since(node_data.last_seen)
                .ok()
                .map(|duration| duration.gt(&config.cluster.suspicious_timeout))
                .unwrap_or(true);

            match suspicious {
                false => None,
                true => Some(id.clone()),
            }
        })
        .choose_multiple(&mut rng, config.cluster.sus_to_ping);

    let mut regular_nodes: Vec<_> = state
        .nodes
        .keys()
        .cloned()
        .choose_multiple(&mut rng, config.cluster.nodes_to_ping);

    nodes.append(&mut seed_nodes);
    nodes.append(&mut suspicious_nodes);
    nodes.append(&mut regular_nodes);

    let messages = nodes
        .iter()
        .map(|nodeid| {
            let message = ClusterMessage::Syn {
                cluster_digest: digest.clone(),
            };

            (nodeid.clone(), message.serialize_for_send())
        })
        .collect::<Vec<_>>();

    future::join_all(
        messages
            .iter()
            .map(|(addr, data)| transport.send_unreliable(addr.clone(), data)),
    )
    .await;

    Ok(())
}

#[derive(Debug)]
pub enum ClusterClientRequest {
    ListRaftNodes {
        respond: oneshot::Sender<Vec<NodeId>>,
    },
    ListObserverNodes {
        respond: oneshot::Sender<Vec<NodeId>>,
    },
    ListAllNodes {
        respond: oneshot::Sender<Vec<NodeId>>,
    },
}

async fn handle_client_request(
    req: ClusterClientRequest,
    _transport: &impl Transport,
    _config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match req {
        ClusterClientRequest::ListRaftNodes { respond } => {
            let mut nodes: Vec<NodeId> = state
                .nodes
                .iter()
                .filter_map(|(k, v)| match v.raft_voter {
                    true => Some(k.to_owned()),
                    false => None,
                })
                .collect();

            if state.local_node.state.raft_voter {
                nodes.push(state.local_node.id.to_owned());
            }

            respond.send(nodes);
            Ok(())
        }
        ClusterClientRequest::ListObserverNodes { respond } => {
            let mut nodes: Vec<NodeId> = state
                .nodes
                .iter()
                .filter_map(|(k, v)| match v.raft_voter {
                    true => None,
                    false => Some(k.to_owned()),
                })
                .collect();

            if !state.local_node.state.raft_voter {
                nodes.push(state.local_node.id.to_owned());
            }

            respond.send(nodes);
            Ok(())
        }
        ClusterClientRequest::ListAllNodes { respond } => {
            let mut nodes: Vec<NodeId> = state.nodes.keys().cloned().collect();
            nodes.push(state.local_node.id.to_owned());

            respond.send(nodes);

            Ok(())
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterClient {
    sender: mpsc::Sender<ClusterClientRequest>,
    config: Config,
}

impl ClusterClient {
    pub async fn list_raft_nodes(&self) -> anyhow::Result<Vec<NodeId>> {
        let (tx, rx) = oneshot::channel();
        let req = ClusterClientRequest::ListRaftNodes { respond: tx };

        let _ = self.sender.send(req).await?;
        let response = rx.await?;

        return Ok(response);
    }
    pub async fn list_observer_nodes(&self) -> anyhow::Result<Vec<NodeId>> {
        let (tx, rx) = oneshot::channel();
        let req = ClusterClientRequest::ListObserverNodes { respond: tx };

        let _ = self.sender.send(req).await?;
        let response = rx.await?;

        return Ok(response);
    }
    pub async fn list_all_nodes(&self) -> anyhow::Result<Vec<NodeId>> {
        let (tx, rx) = oneshot::channel();
        let req = ClusterClientRequest::ListAllNodes { respond: tx };

        let _ = self.sender.send(req).await?;
        let response = rx.await?;

        return Ok(response);
    }
}

impl From<Config> for ClusterClient {
    fn from(value: Config) -> Self {
        Self {
            sender: value.clone().cluster.client,
            config: value.clone(),
        }
    }
}
