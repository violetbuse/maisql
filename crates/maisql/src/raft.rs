use async_trait::async_trait;
use std::{
    collections::HashMap,
    fmt::Display,
    hash::Hash,
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};

pub struct Config<I: NodeId> {
    pub cluster_heartbeat: Duration,
    pub seed_nodes: Vec<I>,
    pub local_node_id: I,
    pub delete_min_wait: Duration,
}

pub trait NodeId:
    ToString + Send + Sync + Clone + PartialEq + Eq + Hash + Serialize + for<'a> Deserialize<'a>
{
    fn socket_addr(&self) -> SocketAddr;
}

impl NodeId for SocketAddr {
    fn socket_addr(&self) -> SocketAddr {
        self.to_owned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamedAddr {
    name: String,
    addr: SocketAddr,
}

impl NodeId for NamedAddr {
    fn socket_addr(&self) -> SocketAddr {
        self.addr.to_owned()
    }
}

impl Display for NamedAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.name, self.addr)
    }
}

impl NamedAddr {
    pub fn new(name: String, addr: SocketAddr) -> Self {
        Self { name, addr }
    }
}

#[async_trait]
pub trait State: Send + Sync + Clone + Copy {
    async fn config(&self) -> Config<impl NodeId>;
    async fn nodes(&self) -> HashMap<impl NodeId, Node>;
    async fn node_ids(&self) -> Vec<impl NodeId>;
    async fn node(&self, node_id: impl NodeId) -> Option<Node>;
    async fn local_node(&self) -> Node;
    /// Change the state of a node. To delete the node return None.
    /// This function returns the previous state of the node if it existed.
    /// The callback function receives the id, as well as an option representing
    /// the value of the node, if it was there already.
    ///
    /// For basic insert/update/delete operations, you can use the `insert`,
    /// `update`, `upsert`, and `remove` functions.
    ///
    /// ```rust
    /// // update the node
    /// state.map_node(some_id, |node_id, node| {
    ///     new_state = get_new_state();
    ///     Some((node_id, new_state))
    /// }).await;
    ///
    /// // rename the node
    /// state.map_node(some_id, |node_id, node| {
    ///     new_id = get_new_id();
    ///     Some((new_id, node))
    /// }).await;
    ///
    /// // delete the node
    /// state.map_node(some_id, |_, _| None).await;
    /// ```
    async fn map_node<I, F>(&self, node_id: impl NodeId, fxn: F) -> Option<Node>
    where
        I: NodeId,
        F: Fn(I, Option<Node>) -> Option<(I, Node)>;
    /// Insert a node with value **only** if there isn't one already.
    /// Use `upsert` to insert and update on conflict.
    async fn insert(&self, node_id: impl NodeId, value: Node) {
        self.map_node(node_id.clone(), |_, old_value| match old_value {
            None => Some((node_id.clone(), value.clone())),
            Some(old_value) => Some((node_id.clone(), old_value)),
        })
        .await;
    }
    /// Update a node's value **only** if the node already exists.
    /// use `upsert` to also insert the value if it doesn't
    /// yet exist.
    async fn update(&self, node_id: impl NodeId, value: Node) {
        self.map_node(node_id.clone(), |_, old_value| match old_value {
            None => None,
            Some(_) => Some((node_id.clone(), value.clone())),
        })
        .await;
    }
    async fn upsert(&self, node_id: impl NodeId, value: Node) {
        self.map_node(node_id.clone(), |_, _| {
            Some((node_id.clone(), value.clone()))
        })
        .await;
    }
    async fn remove<I>(&self, node_id: I) -> Option<Node>
    where
        I: NodeId,
    {
        self.map_node(node_id, |_: I, _| None).await
    }

    async fn map_local_node<F>(&self, fxn: F)
    where
        F: Fn(Node) -> Node;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    cluster: ClusterData,
    raft: RaftData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterData {
    last_seen: SystemTime,
    metadata: HashMap<String, String>,
    marked_for_deletion: bool,
    version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftData {}
