use crate::state::{ClusterDelta, ClusterDigest, Global, NodeId};
use crate::transport::Transport;
use anyhow::anyhow;
use futures::future;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::SystemTime};
use tokio::time::{self, Instant};

pub async fn run_node(transport: impl Transport, global: &Global) -> anyhow::Result<()> {
    let mut interval = time::interval(global.config.cluster_config.cluster_heartbeat);

    loop {
        let _ = tokio::select! {
            Some((bytes, from)) = transport.recv() => handle_transport_message(bytes, from, transport, global).await,
            instant = interval.tick() => handle_heartbeat(instant, transport, global).await
        };
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cluster_transport_message")]
pub enum TransportMessage {
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

impl TransportMessage {
    pub fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        if let Ok(parsed) = rmp_serde::from_slice(bytes) {
            Ok(parsed)
        } else if let Ok(parsed) = serde_json::from_slice(bytes) {
            Ok(parsed)
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
    from: NodeId,
    transport: impl Transport,
    global: &Global,
) -> anyhow::Result<()> {
    let message = TransportMessage::parse(bytes)?;

    match message {
        TransportMessage::Syn { cluster_digest } => {
            let (cluster_delta, cluster_digest) = global.map_state(|state| {
                (
                    state.generate_cluster_delta(cluster_digest.clone()),
                    state.generate_cluster_digest(),
                )
            });

            let message = TransportMessage::SynAck {
                cluster_digest,
                cluster_delta,
            }
            .serialize_msgpack()?;

            transport.send_unreliable(from, &message).await?;
        }
        TransportMessage::SynAck {
            cluster_digest,
            cluster_delta,
        } => {
            let cluster_delta = global.map_state_mut(|state| {
                state.apply_cluster_delta(cluster_delta.clone());
                state.generate_cluster_delta(cluster_digest.clone())
            });

            let message = TransportMessage::Ack { cluster_delta }.serialize_msgpack()?;

            transport.send_unreliable(from, &message).await?;
        }
        TransportMessage::Ack { cluster_delta } => {
            global.map_state_mut(|state| state.apply_cluster_delta(cluster_delta.clone()))
        }
        TransportMessage::DecomissionNode { addr } => global.map_state_mut(|state| {
            state.nodes.entry(addr.clone()).and_modify(|node| {
                node.cluster.map(|cluster_data| {
                    cluster_data.deleted = true;
                })
            });
        }),
        TransportMessage::RejoinNode { addr } => global.map_state_mut(|state| {
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
    transport: impl Transport,
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
                let data = TransportMessage::Syn {
                    cluster_digest: digest.clone(),
                }
                .serialize_msgpack()
                .unwrap();
                (nodeid.clone(), data)
            })
            .collect();
    });

    future::join_all(
        messages
            .iter()
            .map(|(addr, data)| transport.send_unreliable(addr.clone(), data))
            .collect::<Vec<_>>(),
    )
    .await;

    Ok(())
}
