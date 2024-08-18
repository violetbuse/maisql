use crate::{
    cluster::{ClusterClient, ClusterConfig},
    kv::{KvClient, KvConfig},
    locks::{LocksClient, LocksConfig},
    object_storage::{ObjectStorageClient, ObjectStorageConfig},
    raft::{RaftClient, RaftConfig},
    sqlite::{
        db_registry::{DbRegistryClient, DbRegistryConfig},
        SqliteClient, SqliteConfig,
    },
};

#[derive(Debug, Clone)]
pub struct Config {
    pub cluster: ClusterConfig,
    pub raft: RaftConfig,
    pub locks: LocksConfig,
    pub db_registry: DbRegistryConfig,
    pub sqlite: SqliteConfig,
    pub object_storage: ObjectStorageConfig,
    pub kv: KvConfig,
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
    pub fn db_registry_client(&self) -> DbRegistryClient {
        self.clone().into()
    }
    pub fn object_storage_client(&self) -> ObjectStorageClient {
        self.clone().into()
    }
    pub fn kv_client(&self) -> KvClient {
        self.clone().into()
    }
}
