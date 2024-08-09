use std::{
    collections::HashMap,
    hash::Hash,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId {
    pub addr: SocketAddr,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub local_node_id: NodeId,
    pub cluster_config: ClusterConfig,
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub cluster_heartbeat: Duration,
    pub seed_nodes: Vec<NodeId>,
    pub suspicious_timeout: Duration,
    pub seeds_to_ping: usize,
    pub sus_to_ping: usize,
    pub nodes_to_ping: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub nodes: HashMap<NodeId, Node>,
    pub local_node: (NodeId, Node),
}

pub type ClusterDigest = HashMap<NodeId, u64>;
pub type ClusterDelta = HashMap<NodeId, ClusterData>;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub cluster: ClusterData,
    pub raft: RaftData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterData {
    pub last_seen: SystemTime,
    pub meta: HashMap<String, String>,
    pub deleted: bool,
    pub version: u64,
}

impl ClusterData {
    pub fn map<F, R>(&mut self, fxn: F) -> R
    where
        F: Fn(&mut Self) -> R,
    {
        let result = fxn(self);
        self.version += 1;

        return result;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RaftData {}

#[derive(Debug, Clone)]
pub struct Global {
    pub state: Arc<Mutex<State>>,
    pub config: Config,
}

impl Global {
    pub fn map_state_mut<F, R>(&self, fxn: F) -> R
    where
        F: Fn(&mut State) -> R,
    {
        let mut state = self.state.lock().unwrap();
        return fxn(&mut state);
    }
    pub fn map_state<F, R>(&self, fxn: F) -> R
    where
        F: Fn(&State) -> R,
    {
        let state = self.state.lock().unwrap();
        return fxn(&state);
    }
}
