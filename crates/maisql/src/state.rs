use std::{
    collections::{HashMap, HashSet},
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
    pub raft_config: RaftConfig,
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

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub heartbeat_interval: Duration,
    pub tick_interval: Duration,
    pub min_election_timeout: Duration,
    pub max_election_timeout: Duration,
    pub term_cleanup: u64,
    pub leader_heartbeat_cleanup: Duration,
    /// Value between 0 and 1
    pub min_vote_ratio_to_lead: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub nodes: HashMap<NodeId, Node>,
    pub local_node: (NodeId, Node),
    pub raft_terms: HashMap<u64, TermData>,
}

pub type ClusterDigest = HashMap<NodeId, u64>;
pub type ClusterDelta = HashMap<NodeId, ClusterData>;

impl State {}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftData {
    pub member: bool,
}

impl Default for RaftData {
    fn default() -> Self {
        Self { member: false }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermData {
    pub leader: Option<NodeId>,
    pub leader_heartbeats: Vec<SystemTime>,
    pub voted_for: Option<NodeId>,
    pub votes_received: HashSet<NodeId>,
}

impl Default for TermData {
    fn default() -> Self {
        Self {
            leader: None,
            leader_heartbeats: Vec::new(),
            voted_for: None,
            votes_received: 0,
        }
    }
}

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
