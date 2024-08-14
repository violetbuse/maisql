use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    cluster::{ClusterClient, ClusterConfig},
    locks::{LocksClient, LocksConfig},
    raft::{RaftClient, RaftConfig},
    transport::Transport,
};

#[derive(Debug, Clone)]
pub struct Config {
    pub cluster_config: ClusterConfig,
    pub cluster_client: ClusterClient,
    pub raft_config: RaftConfig,
    pub raft_client: RaftClient,
    pub locks_config: LocksConfig,
    pub locks_client: LocksClient,
}
