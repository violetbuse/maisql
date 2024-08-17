use crate::{
    cluster::{ClusterClient, ClusterConfig},
    locks::{LocksClient, LocksConfig},
    raft::{RaftClient, RaftConfig},
    sqlite::{db_registry::DbRegistryConfig, SqliteClient, SqliteConfig},
};

#[derive(Debug, Clone)]
pub struct Config {
    pub cluster: ClusterConfig,
    pub raft: RaftConfig,
    pub locks: LocksConfig,
    pub db_registry: DbRegistryConfig,
    pub sqlite: SqliteConfig,
}

impl Config {
    pub fn cluster_client(&self) -> ClusterClient {
        self.clone().into()
    }
    pub fn raft_client(&self) -> RaftClient {
        self.clone().into()
    }
    pub fn locks_client(&self) -> LocksClient {
        self.clone().into()
    }
    pub fn sqlite_client(&self) -> SqliteClient {
        self.clone().into()
    }
    pub fn db_registry_client(&self) -> DbRegistryConfig {
        self.clone().into()
    }
}
