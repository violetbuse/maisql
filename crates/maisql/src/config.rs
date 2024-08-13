use crate::{
    cluster::{ClusterClient, ClusterConfig},
    locks::{LocksClient, LocksConfig},
    raft::{RaftClient, RaftConfig},
    semaphore::{SemaphoreClient, SemaphoreConfig},
};

#[derive(Debug, Clone)]
pub struct Config {
    pub cluster_config: ClusterConfig,
    pub cluster_client: ClusterClient,
    pub raft_config: RaftConfig,
    pub raft_client: RaftClient,
    pub semaphore_config: SemaphoreConfig,
    pub semaphore_client: SemaphoreClient,
    pub locks_config: LocksConfig,
    pub locks_client: LocksClient,
}
