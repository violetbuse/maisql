use crate::{
    cluster::{ClusterClient, ClusterConfig},
    raft_leader::{RaftClient, RaftConfig},
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
}
