use tokio::{
    join, select,
    sync::{broadcast, oneshot},
};

use crate::{
    cluster::{self, ClusterConfig},
    config::Config,
    locks::{self, LocksConfig},
    raft::{self, RaftConfig},
    transport::Transport,
};

#[derive(Debug, Clone)]
pub struct ManagerConfig {
    cluster_config: ClusterConfig,
    raft_config: RaftConfig,
    lock_config: LocksConfig,
}

pub async fn start_node(transport: &impl Transport, config: ManagerConfig) {
    let (cfg_sender, cfg_recv) = broadcast::channel(128);
    let (cluster_client_sender, cluster_client_recv) = oneshot::channel();
    let (raft_client_sender, raft_client_recv) = oneshot::channel();
    let (locks_client_sender, locks_client_recv) = oneshot::channel();

    let mut cluster_cfg_recv = cfg_recv.resubscribe();
    let mut raft_cfg_recv = cfg_recv.resubscribe();
    let mut locks_cfg_recv = cfg_recv.resubscribe();

    let cluster_task = cluster::run_node(transport, &mut cluster_cfg_recv, cluster_client_sender);
    let raft_task = raft::run_raft(transport, &mut raft_cfg_recv, raft_client_sender);
    let locks_task = locks::run_locks(transport, &mut locks_cfg_recv, locks_client_sender);

    let _config = tokio::spawn(async move {
        let (cluster_client, raft_client, locks_client) =
            join!(cluster_client_recv, raft_client_recv, locks_client_recv);

        let cluster_client = cluster_client.unwrap();
        let raft_client = raft_client.unwrap();
        let locks_client = locks_client.unwrap();

        let config = Config {
            cluster_config: config.cluster_config,
            cluster_client,
            raft_config: config.raft_config,
            raft_client,
            locks_config: config.lock_config,
            locks_client,
        };

        let _ = cfg_sender.send(config.clone());
        return config;
    })
    .await
    .unwrap();

    select! {
        _ = cluster_task => {},
        _ = raft_task => {},
        _ = locks_task => {}
    }
}
