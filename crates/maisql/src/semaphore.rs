use std::{
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime},
};

use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    time::{self, Instant},
};

use crate::{
    config::Config,
    transport::{NodeId, Transport, TransportData},
};

#[derive(Debug, Clone)]
pub struct SemaphoreConfig {
    /// How long it takes to revoke a lock.
    pub lock_timeout: Duration,
}

#[derive(Debug, Clone)]
struct State {
    entries: Vec<Entry>,
    committed_at: u64,
    last_snapshot: Option<Snapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    id: u64,
    value: EntryType,
    replicated_by: HashSet<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum EntryType {
    NoOpEntry,
    GetLock {
        resource: String,
        node: NodeId,
        expires: SystemTime,
    },
    RenewLock {
        resource: String,
        new_expiration: SystemTime,
    },
    DropLock {
        resource: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    locks: HashMap<String, Lock>,
    snapshot_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lock {
    holder: NodeId,
    expires_at: SystemTime,
}

pub async fn run_semaphore(
    transport: &impl Transport,
    config: &mut broadcast::Receiver<Config>,
    client_sender: oneshot::Sender<SemaphoreClient>,
) -> anyhow::Result<()> {
    let (client, mut client_rx) = SemaphoreClient::new();
    client_sender.send(client).unwrap();

    let config = config.recv().await.unwrap();

    let mut state = State {
        entries: Vec::new(),
        committed_at: 0,
        last_snapshot: None,
    };

    let mut heartbeat_interval = time::interval(config.semaphore_config.lock_timeout);

    loop {
        let _ = tokio::select! {
            instant = heartbeat_interval.tick() => handle_heartbeat(instant, transport, config.to_owned(), &mut state).await,
            Some((bytes, from)) = transport.recv() => handle_transport_message(bytes, from, transport, config.to_owned(), &mut state).await,
            Some(req) = client_rx.recv() => handle_client_req(req, transport, config.to_owned(), &mut state).await,
        };
    }
}

impl State {
    pub fn locks(&self) -> HashMap<String, Lock> {
        let mut snapshotted = self
            .last_snapshot
            .clone()
            .map(|snap| snap.locks)
            .unwrap_or(HashMap::new());

        let committed: Vec<_> = self
            .entries
            .iter()
            .take_while(|entry| entry.id <= self.committed_at)
            .cloned()
            .collect();

        for entry in committed {
            match entry.value {
                EntryType::NoOpEntry => {}
                EntryType::GetLock {
                    resource,
                    node,
                    expires,
                } => {
                    snapshotted.insert(
                        resource,
                        Lock {
                            holder: node,
                            expires_at: expires,
                        },
                    );
                }
                EntryType::RenewLock {
                    resource,
                    new_expiration,
                } => {
                    snapshotted
                        .entry(resource)
                        .and_modify(|lock| lock.expires_at = new_expiration);
                }
                EntryType::DropLock { resource } => {
                    snapshotted.remove(&resource);
                }
            }
        }

        return snapshotted;
    }
    pub fn run_compaction(&mut self) {
        let locks = self.locks();
        let mut uncommited_entries: Vec<Entry> = Vec::new();
        let mut next_entry_id = 1;

        let not_committed: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| entry.id > self.committed_at)
            .cloned()
            .collect();

        for entry in not_committed {
            let new_entry = Entry {
                id: next_entry_id,
                ..entry
            };
            uncommited_entries.push(new_entry);
            next_entry_id += 1;
        }

        let new_snapshot = Snapshot {
            locks,
            snapshot_index: self
                .last_snapshot
                .as_ref()
                .map(|snap| snap.snapshot_index)
                .unwrap_or(0),
        };

        self.last_snapshot = Some(new_snapshot);
        self.entries = uncommited_entries;
        self.committed_at = 0;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SemaphoreMessage {
    AquireLock {
        resource: String,
    },
    RenewLock {
        resource: String,
    },
    DropLock {
        resource: String,
    },
    AppendEntries {
        entries: Vec<Entry>,
    },
    EntryAppended {
        entries: Vec<u64>,
    },
    CommitEntries {
        until: u64,
    },
    ReplicateLeaderState {
        entries: Vec<Entry>,
        committed_until: u64,
        base_snapshot: Snapshot,
    },
    BroadcastLatestState {
        newest_commited_entry: u64,
        newest_snapshot_version: u64,
    },
    RequestLatestState,
    NotifyLatestState {
        committed_entries: Vec<Entry>,
        newest_snapshot: Option<Snapshot>,
    },
}

impl TransportData for SemaphoreMessage {}

async fn handle_transport_message(
    bytes: &[u8],
    from: NodeId,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let message = SemaphoreMessage::parse(bytes, "semaphore_message")?;

    let locks = state.locks();
    let local_id = config.cluster_config.own_node_id.to_owned();

    match config.raft_client.leader().await? {
        None => {
            handle_transport_message_as_observer(message, from, locks, transport, config, state)
                .await
        }
        Some(leader_id) if leader_id == local_id => {
            handle_transport_message_as_leader(message, from, locks, transport, config, state).await
        }
        Some(_) => {
            handle_transport_message_as_voter(message, from, locks, transport, config, state).await
        }
    }
}

async fn handle_transport_message_as_leader(
    message: SemaphoreMessage,
    from: NodeId,
    locks: HashMap<String, Lock>,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let next_entry_id = state.entries.last().map(|entry| entry.id).unwrap_or(1);

    match message {
        SemaphoreMessage::AquireLock { resource } => {
            let existing_lock = locks.get(&resource);

            if existing_lock.is_none() {
                let expiration = SystemTime::now() + config.semaphore_config.lock_timeout;
                let mut replicated_by = HashSet::new();

                replicated_by.insert(config.cluster_config.own_node_id);

                let new_entry = Entry {
                    id: next_entry_id,
                    value: EntryType::GetLock {
                        resource,
                        node: from,
                        expires: expiration,
                    },
                    replicated_by,
                };

                state.entries.push(new_entry);
            }
        }
        SemaphoreMessage::RenewLock { resource } => {
            if let Some(existing_lock) = locks.get(&resource) {
                if existing_lock.holder == from {
                    let new_expiration = SystemTime::now() + config.semaphore_config.lock_timeout;
                    let mut replicated_by = HashSet::new();

                    replicated_by.insert(config.cluster_config.own_node_id);

                    let new_entry = Entry {
                        id: next_entry_id,
                        value: EntryType::RenewLock {
                            resource,
                            new_expiration,
                        },
                        replicated_by,
                    };

                    state.entries.push(new_entry);
                }
            }
        }
        SemaphoreMessage::DropLock { resource } => {
            if let Some(existing_lock) = locks.get(&resource) {
                if existing_lock.holder == from {
                    let mut replicated_by = HashSet::new();
                    replicated_by.insert(config.cluster_config.own_node_id);

                    let new_entry = Entry {
                        id: next_entry_id,
                        value: EntryType::DropLock { resource },
                        replicated_by,
                    };

                    state.entries.push(new_entry);
                }
            }
        }
        SemaphoreMessage::AppendEntries { .. } => {}
        SemaphoreMessage::EntryAppended { entries } => {
            for entry in state.entries.iter_mut() {
                if entries.contains(&entry.id) {
                    entry.replicated_by.insert(from.clone());
                }
            }
        }
        SemaphoreMessage::CommitEntries { .. } => {}
        SemaphoreMessage::ReplicateLeaderState { .. } => {}
        SemaphoreMessage::BroadcastLatestState { .. } => {}
        SemaphoreMessage::RequestLatestState => {
            let committed: Vec<_> = state
                .entries
                .iter()
                .take_while(|entry| entry.id <= state.committed_at)
                .cloned()
                .collect();

            let notification = SemaphoreMessage::NotifyLatestState {
                committed_entries: committed,
                newest_snapshot: state.last_snapshot.clone(),
            };

            transport
                .send_reliable(from, &notification.serialize_for_send())
                .await;
        }
        SemaphoreMessage::NotifyLatestState { .. } => {}
    }

    Ok(())
}

async fn handle_transport_message_as_voter(
    message: SemaphoreMessage,
    from: NodeId,
    locks: HashMap<String, Lock>,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
}

async fn handle_transport_message_as_observer(
    message: SemaphoreMessage,
    from: NodeId,
    locks: HashMap<String, Lock>,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match message {
        SemaphoreMessage::BroadcastLatestState {
            newest_commited_entry,
            newest_snapshot_version,
        } => {
            let entries_out_of_date = state.committed_at != newest_commited_entry;
            let snapshot_out_of_date = state
                .last_snapshot
                .map(|snap| snap.snapshot_index != newest_snapshot_version)
                .unwrap_or(true);

            if entries_out_of_date || snapshot_out_of_date {
                let raft_nodes = config
                    .cluster_client
                    .list_raft_nodes()
                    .await?
                    .iter()
                    .choose_multiple(&mut rand::thread_rng(), 5);
                let msg = SemaphoreMessage::RequestLatestState.serialize_for_send();
            }
        }
    }

    Ok(())
}

async fn handle_heartbeat(
    instant: Instant,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
}

#[derive(Debug)]
pub enum SemaphoreClientRequest {
    GetLockForResource {
        resource: String,
        response: oneshot::Sender<Option<Lock>>,
    },
    ListLocks {
        response: oneshot::Sender<HashMap<String, Lock>>,
    },
}

#[derive(Debug, Clone)]
pub struct SemaphoreClient {
    sender: mpsc::Sender<SemaphoreClientRequest>,
}

impl SemaphoreClient {
    pub fn new() -> (Self, mpsc::Receiver<SemaphoreClientRequest>) {
        let (tx, rx) = mpsc::channel(128);
        return (Self { sender: tx }, rx);
    }
    pub async fn get_lock(&self, resource: String) -> anyhow::Result<Option<Lock>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(SemaphoreClientRequest::GetLockForResource {
                resource,
                response: tx,
            })
            .await;

        let lock = rx.await?;
        return Ok(lock);
    }
    pub async fn list_locks(&self) -> anyhow::Result<HashMap<String, Lock>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(SemaphoreClientRequest::ListLocks { response: tx })
            .await;

        let locks = rx.await?;
        return Ok(locks);
    }
}

async fn handle_client_req(
    req: SemaphoreClientRequest,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match req {
        SemaphoreClientRequest::GetLockForResource { resource, response } => {
            let lock = state.locks().get(&resource).map(|lock| lock.clone());
            response.send(lock);

            Ok(())
        }
        SemaphoreClientRequest::ListLocks { response } => {
            let locks = state.locks();
            response.send(locks);

            Ok(())
        }
    }
}
