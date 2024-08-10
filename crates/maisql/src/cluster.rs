use crate::state::{ClusterDelta, ClusterDigest, Global, Node, NodeId, RaftData, State};
use crate::transport::{Transport, TransportData};
use futures::future;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::SystemTime};
use tokio::time::{self, Instant};

pub async fn run_node(transport: &impl Transport, global: &Global) -> anyhow::Result<()> {
    let mut interval = time::interval(global.config.cluster_config.cluster_heartbeat);

    loop {
        let _ = tokio::select! {
            Some((bytes, from)) = transport.recv() => handle_transport_message(bytes, from, transport, global).await,
            instant = interval.tick() => handle_heartbeat(instant, transport, global).await
        };
    }
}

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
    DecomissionNode {
        addr: NodeId,
    },
    RejoinNode {
        addr: NodeId,
    },
}

impl TransportData for ClusterMessage {}

impl State {
    pub fn generate_cluster_digest(&self) -> ClusterDigest {
        return self
            .nodes
            .clone()
            .into_iter()
            .filter_map(|(id, value)| {
                if id == self.local_node.0 {
                    return None;
                } else {
                    return Some((id, value.cluster.version));
                }
            })
            .collect();
    }
    pub fn generate_cluster_delta(&self, digest: ClusterDigest) -> ClusterDelta {
        return self
            .nodes
            .clone()
            .into_iter()
            .filter_map(|(id, value)| match digest.get(&id) {
                None => Some((id, value.cluster)),
                Some(digest_version) if digest_version < &value.cluster.version => {
                    Some((id, value.cluster))
                }
                _ => None,
            })
            .collect();
    }
    pub fn apply_cluster_delta(&mut self, delta: ClusterDelta) {
        for (id, node) in delta.iter() {
            self.nodes
                .entry(id.clone())
                .and_modify(|node_data| node_data.cluster = node.clone())
                .or_insert(Node {
                    cluster: node.clone(),
                    raft: RaftData::default(),
                });
        }
    }
}

async fn handle_transport_message(
    bytes: &[u8],
    from: NodeId,
    transport: &impl Transport,
    global: &Global,
) -> anyhow::Result<()> {
    let message = ClusterMessage::parse(bytes, "cluster_message")?;

    match message {
        ClusterMessage::Syn { cluster_digest } => {
            let (cluster_delta, cluster_digest) = global.map_state(|state| {
                (
                    state.generate_cluster_delta(cluster_digest.clone()),
                    state.generate_cluster_digest(),
                )
            });

            let message = ClusterMessage::SynAck {
                cluster_digest,
                cluster_delta,
            };

            let data = TransportData::serialize(&message);

            transport.send_unreliable(from, &data).await?;
        }
        ClusterMessage::SynAck {
            cluster_digest,
            cluster_delta,
        } => {
            let cluster_delta = global.map_state_mut(|state| {
                state.apply_cluster_delta(cluster_delta.clone());
                state.generate_cluster_delta(cluster_digest.clone())
            });

            let message = ClusterMessage::Ack { cluster_delta };

            let data = TransportData::serialize(&message);

            transport.send_unreliable(from, &data).await?;
        }
        ClusterMessage::Ack { cluster_delta } => {
            global.map_state_mut(|state| state.apply_cluster_delta(cluster_delta.clone()))
        }
        ClusterMessage::DecomissionNode { addr } => global.map_state_mut(|state| {
            state.nodes.entry(addr.clone()).and_modify(|node| {
                node.cluster.map(|cluster_data| {
                    cluster_data.deleted = true;
                })
            });
        }),
        ClusterMessage::RejoinNode { addr } => global.map_state_mut(|state| {
            state.nodes.entry(addr.clone()).and_modify(|node| {
                node.cluster.map(|cluster_data| {
                    cluster_data.deleted = false;
                })
            });
        }),
    };

    Ok(())
}

async fn handle_heartbeat(
    _when: Instant,
    transport: &impl Transport,
    global: &Global,
) -> anyhow::Result<()> {
    let messages: Vec<(NodeId, Vec<u8>)> = global.map_state_mut(|state| {
        let mut rng = rand::thread_rng();

        let mut nodes = Vec::new();
        let digest = state.generate_cluster_digest();

        let mut seed_nodes: Vec<_> = global
            .config
            .cluster_config
            .seed_nodes
            .iter()
            .cloned()
            .choose_multiple(&mut rng, global.config.cluster_config.seeds_to_ping);

        let mut suspicious_nodes: Vec<_> = state
            .nodes
            .clone()
            .iter()
            .filter_map(|(id, node_data)| {
                let suspicious = SystemTime::now()
                    .duration_since(node_data.cluster.last_seen)
                    .ok()
                    .map(|duration| duration.gt(&global.config.cluster_config.suspicious_timeout))
                    .unwrap_or(true);

                match suspicious {
                    false => None,
                    true => Some(id.clone()),
                }
            })
            .choose_multiple(&mut rng, global.config.cluster_config.sus_to_ping);

        let mut regular_nodes: Vec<_> = state
            .nodes
            .keys()
            .cloned()
            .choose_multiple(&mut rng, global.config.cluster_config.nodes_to_ping);

        nodes.append(&mut seed_nodes);
        nodes.append(&mut suspicious_nodes);
        nodes.append(&mut regular_nodes);

        return nodes
            .iter()
            .map(|nodeid| {
                let message = ClusterMessage::Syn {
                    cluster_digest: digest.clone(),
                };

                let data = TransportData::serialize(&message);

                (nodeid.clone(), data)
            })
            .collect();
    });

    future::join_all(
        messages
            .iter()
            .map(|(addr, data)| transport.send_unreliable(addr.clone(), data)),
    )
    .await;

    Ok(())
}
