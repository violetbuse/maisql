use tokio::{
    join, select,
    sync::{broadcast, oneshot},
};

use crate::{
    cluster::{self, ClusterConfig},
    config::Config,
    raft_leader::{self, RaftConfig},
    semaphore::{self, SemaphoreConfig},
    transport::Transport,
};

#[derive(Debug, Clone)]
pub struct ManagerConfig {
    cluster_config: ClusterConfig,
    raft_config: RaftConfig,
    semaphore_config: SemaphoreConfig,
}

pub async fn start_node(transport: &impl Transport, config: ManagerConfig) {
    let (cfg_sender, cfg_recv) = broadcast::channel(128);
    let (cluster_client_sender, cluster_client_recv) = oneshot::channel();
    let (raft_client_sender, raft_client_recv) = oneshot::channel();
    let (semaphore_client_sender, semaphore_client_recv) = oneshot::channel();

    let cluster_task = cluster::run_node(
        transport,
        &mut cfg_recv.resubscribe(),
        cluster_client_sender,
    );
    let raft_task =
        raft_leader::run_raft(transport, &mut cfg_recv.resubscribe(), raft_client_sender);
    let semaphore_task = semaphore::run_semaphore(
        transport,
        &mut cfg_recv.resubscribe(),
        semaphore_client_sender,
    );

    let _config = tokio::spawn(async move {
        let (cluster_client, raft_client, semaphore_client) =
            join!(cluster_client_recv, raft_client_recv, semaphore_client_recv);

        let cluster_client = cluster_client.unwrap();
        let raft_client = raft_client.unwrap();
        let semaphore_client = semaphore_client.unwrap();

        let config = Config {
            cluster_config: config.cluster_config,
            cluster_client,
            raft_config: config.raft_config,
            raft_client,
            semaphore_config: config.semaphore_config,
            semaphore_client,
        };

        cfg_sender.send(config.clone());
        return config;
    })
    .await
    .unwrap();

    select! {
        _ = cluster_task => {},
        _ = raft_task => {},
        _ = semaphore_task => {}
    }
}
