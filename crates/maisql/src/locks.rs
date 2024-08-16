use std::{
    cmp::Ordering,
    collections::HashMap,
    mem,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use sha2::{self, Digest, Sha256};
use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Instant},
};

use crate::{
    cluster::ClusterClient,
    config::Config,
    raft::RaftClient,
    transport::{NodeId, Transport, TransportData},
};

#[derive(Debug, Clone)]
pub struct LocksConfig {
    pub client: mpsc::Sender<LocksClientRequest>,
    lock_default_timeout: Duration,
    /// This mainly controls how often replicas try to sync with the leader.
    lock_service_heartbeat: Duration,
    /// The percentage of *raft* nodes which must have replicated an entry before it can be committed.
    /// min: 0.5, max: 1.0
    entry_commit_minimum_replication_ratio: f64,
    /// The interval between which to compact logs.
    compaction_interval: Duration,
}

#[derive(Debug, Clone)]
struct State {
    follower_digests: HashMap<NodeId, SnapshotDigest>,
    current_snapshot: Snapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotDigest {
    pub snapshot_id: u128,
    snapshot_checksum: [u8; 32],
    entry_id: u128,
    entry_checksum: [u8; 32],
}

impl SnapshotDigest {
    fn replicated(&self, leader: &Snapshot) -> u128 {
        let leader_checksum = leader.generate_entries_checksum(Some(self.entry_id));
        if leader_checksum != self.entry_checksum {
            return 0;
        } else {
            return self.entry_id;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SnapshotDelta {
    FullSnapshot(Snapshot),
    AppendEntries(NextEntry),
    DeleteEntries(u128),
    EmptyDelta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Snapshot {
    id: u128,
    locks: HashMap<String, Lock>,
    entries: Option<NextEntry>,
}

impl Snapshot {
    pub fn last_id(&self) -> u128 {
        match &self.entries {
            None => self.id,
            Some(entry) => entry.last_id(),
        }
    }
    pub fn push(&mut self, value: EntryType) {
        match &mut self.entries {
            curr @ None => {
                *curr = Some(NextEntry::Proposed(Box::new(Entry {
                    id: self.id + 1,
                    value,
                    next: None,
                })));
            }
            Some(entry) => {
                entry.push(value);
            }
        }
    }
    pub fn committed_state(&self) -> HashMap<String, Lock> {
        let mut curr_state = self.locks.clone();

        match &self.entries {
            None => curr_state,
            Some(entry) => {
                entry.committed_state(&mut curr_state);
                curr_state
            }
        }
    }
    pub fn proposed_state(&self) -> HashMap<String, Lock> {
        let mut curr_state = self.locks.clone();

        match &self.entries {
            None => curr_state,
            Some(entry) => {
                entry.proposed_state(&mut curr_state);
                curr_state
            }
        }
    }
    pub fn commit_to(&mut self, committed: u128) {
        match &mut self.entries {
            None => {}
            Some(entry) => entry.commit_to(committed),
        }
    }
    pub fn entries_from(&self, entry_id: u128) -> Option<NextEntry> {
        self.entries
            .as_ref()
            .map(|entry| entry.entries_from(entry_id))
            .flatten()
    }
    pub fn entries_to(&self, entry_id: u128) -> Option<NextEntry> {
        self.entries
            .as_ref()
            .map(|entry| entry.entries_to(entry_id))
    }
    pub fn compact(&self) -> Self {
        let mut locks = self.locks.clone();
        let id = self.id.clone();

        match &self.entries {
            None => Snapshot {
                id,
                locks,
                entries: None,
            },
            Some(next_entry) if next_entry.is_committed() => next_entry.compact(&mut locks),
            Some(next_entry) => Snapshot {
                id,
                locks,
                entries: Some(next_entry.clone()),
            },
        }
    }
    pub fn generate_snapshot_checksum(&self) -> [u8; 32] {
        let mut locks_vec: Vec<_> = self
            .locks
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        locks_vec.sort_by(|(k1, _), (k2, _)| -> Ordering { k1.cmp(&k2) });

        let serialized = rmp_serde::to_vec(&locks_vec).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(serialized);

        hasher.finalize().into()
    }
    pub fn generate_entries_checksum(&self, until: Option<u128>) -> [u8; 32] {
        let entries = match until {
            None => self.entries.clone(),
            Some(until) => self.entries_to(until),
        };

        let serialized = rmp_serde::to_vec(&entries).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(serialized);

        hasher.finalize().into()
    }
    pub fn generate_digest(&self) -> SnapshotDigest {
        let last = self.last_id();
        SnapshotDigest {
            snapshot_id: self.id,
            snapshot_checksum: self.generate_snapshot_checksum(),
            entry_id: last,
            entry_checksum: self.generate_entries_checksum(None),
        }
    }
    pub fn generate_delta(&self, digest: SnapshotDigest) -> SnapshotDelta {
        if digest.snapshot_id < self.id {
            SnapshotDelta::FullSnapshot(self.clone())
        } else if digest.snapshot_id == self.id
            && digest.snapshot_checksum != self.generate_snapshot_checksum()
        {
            SnapshotDelta::FullSnapshot(self.clone())
        } else if digest.entry_checksum != self.generate_entries_checksum(Some(digest.entry_id)) {
            SnapshotDelta::FullSnapshot(self.clone())
        } else if digest.entry_id < self.last_id() {
            self.entries_from(digest.entry_id + 1)
                .map(SnapshotDelta::AppendEntries)
                .unwrap_or(SnapshotDelta::EmptyDelta)
        } else if digest.entry_id > self.last_id() {
            SnapshotDelta::DeleteEntries(self.last_id() + 1)
        } else {
            SnapshotDelta::EmptyDelta
        }
    }
    pub fn apply_delta(&mut self, delta: SnapshotDelta) {
        match delta {
            SnapshotDelta::FullSnapshot(snap) => {
                *self = snap;
            }
            SnapshotDelta::AppendEntries(entries) => {
                self.push_entries(entries);
            }
            SnapshotDelta::DeleteEntries(from) => {
                self.remove_entries(from);
            }
            SnapshotDelta::EmptyDelta => {}
        }
    }
    pub fn push_entries(&mut self, entries: NextEntry) {
        self.entries
            .as_mut()
            .map(|entry| entry.push_entries(entries));
    }
    pub fn remove_entries(&mut self, from: u128) {
        self.entries
            .as_mut()
            .map(|entries| entries.remove_entries(from));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    id: u128,
    value: EntryType,
    next: Option<NextEntry>,
}

impl Entry {
    pub fn last_id(&self) -> u128 {
        match &self.next {
            None => self.id,
            Some(entry) => entry.last_id(),
        }
    }
    pub fn push(&mut self, value: EntryType) {
        match &mut self.next {
            curr @ None => {
                *curr = Some(NextEntry::Proposed(Box::new(Entry {
                    id: self.id + 1,
                    value,
                    next: None,
                })));
            }
            Some(next_entry) => next_entry.push(value),
        }
    }
    pub fn committed_state(&self, prev: &mut HashMap<String, Lock>) {
        self.map_state(prev);

        match &self.next {
            Some(entry) => entry.committed_state(prev),
            None => {}
        }
    }
    pub fn proposed_state(&self, prev: &mut HashMap<String, Lock>) {
        self.map_state(prev);

        match &self.next {
            Some(entry) => entry.proposed_state(prev),
            None => {}
        }
    }
    pub fn map_state(&self, prev: &mut HashMap<String, Lock>) {
        match &self.value {
            EntryType::NoOp => {}
            EntryType::AquireLock {
                resource,
                holder,
                expires,
                data,
            } => {
                prev.entry(resource.clone()).or_insert(Lock {
                    holder: holder.clone(),
                    expires: expires.clone(),
                    data: data.clone(),
                });
            }
            EntryType::RenewLock {
                resource,
                new_expiration,
            } => {
                prev.entry(resource.clone())
                    .and_modify(|lock| lock.expires = new_expiration.clone());
            }
            EntryType::ModifyLockData { resource, new_data } => {
                prev.entry(resource.clone())
                    .and_modify(|lock| lock.data = new_data.clone());
            }
            EntryType::ReleaseLock { resource } => {
                prev.remove(resource);
            }
        };
    }
    pub fn commit_to(&mut self, committed: u128) {
        if self.id >= committed {
            return;
        } else {
            match &mut self.next {
                None => {}
                Some(next_entry) => next_entry.commit_to(committed),
            }
        }
    }
    pub fn entries_from(&self, entry_id: u128) -> Option<NextEntry> {
        self.next
            .as_ref()
            .map(|next_entry| next_entry.entries_from(entry_id))
            .flatten()
    }
    pub fn entries_to(&self, entry_id: u128) -> Entry {
        if entry_id > self.id {
            Entry {
                id: u128::max_value(),
                value: EntryType::NoOp,
                next: None,
            }
        } else if entry_id == self.id {
            Entry {
                id: self.id,
                value: self.value.to_owned(),
                next: None,
            }
        } else {
            let next_entry = self.next.as_ref().map(|next| next.entries_to(entry_id));
            Entry {
                id: self.id,
                value: self.value.to_owned(),
                next: next_entry,
            }
        }
    }
    pub fn compact(&self, prev_locks: &mut HashMap<String, Lock>) -> Snapshot {
        self.map_state(prev_locks);

        match &self.next {
            None => Snapshot {
                id: self.id.to_owned(),
                locks: prev_locks.clone(),
                entries: None,
            },
            Some(next_entry) if next_entry.is_committed() => next_entry.compact(prev_locks),
            Some(next_entry) => Snapshot {
                id: self.id.to_owned(),
                locks: prev_locks.clone(),
                entries: Some(next_entry.clone()),
            },
        }
    }
    pub fn push_entries(&mut self, start: NextEntry) {
        if self.id + 1 == start.inner().id {
            self.next = Some(start);
        } else {
            self.next.as_mut().map(|next| next.push_entries(start));
        }
    }
    pub fn remove_entries(&mut self, from: u128) {
        if self.id == from - 1 {
            self.next = None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum NextEntry {
    Committed(Box<Entry>),
    Proposed(Box<Entry>),
}

impl NextEntry {
    pub fn inner(&self) -> &Entry {
        match &self {
            Self::Committed(entry) | Self::Proposed(entry) => entry,
        }
    }
    pub fn inner_mut(&mut self) -> &mut Entry {
        match self {
            Self::Committed(entry) | Self::Proposed(entry) => entry,
        }
    }
    pub fn is_committed(&self) -> bool {
        match self {
            Self::Committed(..) => true,
            Self::Proposed(..) => false,
        }
    }
    pub fn as_committed(&self) -> Option<&Box<Entry>> {
        match self {
            Self::Committed(entry) => Some(entry),
            Self::Proposed(..) => None,
        }
    }
    pub fn as_committed_mut(&mut self) -> Option<&mut Box<Entry>> {
        match self {
            Self::Committed(entry) => Some(entry),
            Self::Proposed(..) => None,
        }
    }
    pub fn is_proposed(&self) -> bool {
        match self {
            Self::Proposed(..) => true,
            Self::Committed(..) => false,
        }
    }
    pub fn as_proposed(&self) -> Option<&Box<Entry>> {
        match self {
            Self::Proposed(entry) => Some(entry),
            Self::Committed(..) => None,
        }
    }
    pub fn as_proposed_mut(&mut self) -> Option<&mut Box<Entry>> {
        match self {
            Self::Proposed(entry) => Some(entry),
            Self::Committed(..) => None,
        }
    }
    pub fn last_id(&self) -> u128 {
        self.inner().last_id()
    }
    pub fn push(&mut self, value: EntryType) {
        self.inner_mut().push(value);
    }
    pub fn committed_state(&self, prev: &mut HashMap<String, Lock>) {
        match self {
            Self::Committed(entry) => entry.committed_state(prev),
            Self::Proposed(_) => {}
        }
    }
    pub fn proposed_state(&self, prev: &mut HashMap<String, Lock>) {
        match self {
            Self::Committed(entry) | Self::Proposed(entry) => entry.proposed_state(prev),
        }
    }
    pub fn commit_to(&mut self, committed: u128) {
        match self {
            Self::Committed(entry) => entry.commit_to(committed),
            curr @ Self::Proposed(..) => {
                let entry = curr.as_proposed_mut().unwrap();
                let dummy_entry = Box::new(Entry {
                    id: u128::max_value(),
                    value: EntryType::NoOp,
                    next: None,
                });

                let internal = mem::replace(entry, dummy_entry);

                *curr = Self::Committed(internal);
            }
        }
    }
    pub fn entries_from(&self, entry_id: u128) -> Option<NextEntry> {
        if self.inner().id == entry_id {
            return Some(self.clone());
        } else {
            return self.inner().entries_from(entry_id);
        }
    }
    pub fn entries_to(&self, entry_id: u128) -> NextEntry {
        match self {
            Self::Committed(entry) => Self::Committed(Box::new(entry.entries_to(entry_id))),
            Self::Proposed(entry) => Self::Proposed(Box::new(entry.entries_to(entry_id))),
        }
    }
    pub fn compact(&self, prev_locks: &mut HashMap<String, Lock>) -> Snapshot {
        match self {
            Self::Committed(entry) | Self::Proposed(entry) => entry.compact(prev_locks),
        }
    }
    pub fn push_entries(&mut self, start: Self) {
        self.inner_mut().push_entries(start);
    }
    pub fn remove_entries(&mut self, from: u128) {
        self.inner_mut().remove_entries(from);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum EntryType {
    NoOp,
    AquireLock {
        resource: String,
        holder: NodeId,
        expires: SystemTime,
        data: String,
    },
    RenewLock {
        resource: String,
        new_expiration: SystemTime,
    },
    ModifyLockData {
        resource: String,
        new_data: String,
    },
    ReleaseLock {
        resource: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lock {
    pub holder: NodeId,
    pub expires: SystemTime,
    pub data: String,
}

pub async fn run_locks(
    transport: &impl Transport,
    config: Config,
    client_rx: &mut mpsc::Receiver<LocksClientRequest>,
) -> anyhow::Result<()> {
    let mut state = State {
        current_snapshot: Snapshot {
            id: 0,
            locks: HashMap::new(),
            entries: None,
        },
        follower_digests: HashMap::new(),
    };
    let mut heartbeat = time::interval(config.locks.lock_service_heartbeat);
    let mut compaction = time::interval(config.locks.compaction_interval);

    loop {
        let _ = tokio::select! {
            Some((bytes, from)) = transport.recv() => handle_transport_data(bytes, from, transport, config.to_owned(), &mut state).await,
            Some(req) = client_rx.recv() => handle_client_request(req, transport, config.to_owned(), &mut state).await,
            instant = heartbeat.tick() => handle_heartbeat(instant, transport, config.to_owned(), &mut state).await,
            _ = compaction.tick() => handle_compaction(transport, config.to_owned(), &mut state).await,
        };
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "lock_msg_type")]
enum LocksMessage {
    AquireLock { resource: String, data: String },
    RenewLock { resource: String },
    EditLock { resource: String, new_data: String },
    ReleaseLock { resource: String },
    Digest(SnapshotDigest),
    Delta(SnapshotDelta),
}

impl TransportData for LocksMessage {}

async fn handle_transport_data(
    bytes: &[u8],
    from: NodeId,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let message = LocksMessage::parse(bytes, "locks_message")?;
    let is_leader = config
        .raft_client()
        .self_is_leader(config.to_owned())
        .await
        .unwrap_or(false);

    match is_leader {
        true => handle_message_as_leader(message, from, transport, config, state).await,
        false => handle_message_as_replica(message, from, transport, config, state).await,
    }
}

async fn handle_message_as_leader(
    message: LocksMessage,
    from: NodeId,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let current_state = state.current_snapshot.proposed_state();

    let new_entry: Option<EntryType> = match message {
        LocksMessage::AquireLock { resource, data } => {
            if let None = current_state.get(&resource) {
                let expires = SystemTime::now() + config.locks.lock_default_timeout;
                Some(EntryType::AquireLock {
                    resource,
                    holder: from,
                    expires,
                    data,
                })
            } else {
                None
            }
        }
        LocksMessage::RenewLock { resource } => {
            if let Some(lock) = current_state.get(&resource) {
                if lock.holder == from {
                    let new_expiration = SystemTime::now() + config.locks.lock_default_timeout;
                    Some(EntryType::RenewLock {
                        resource,
                        new_expiration,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        }
        LocksMessage::EditLock { resource, new_data } => {
            if let Some(lock) = current_state.get(&resource) {
                if lock.data != new_data && lock.holder == from {
                    Some(EntryType::ModifyLockData { resource, new_data })
                } else {
                    None
                }
            } else {
                None
            }
        }
        LocksMessage::ReleaseLock { resource } => {
            if let Some(lock) = current_state.get(&resource) {
                if lock.holder == from {
                    Some(EntryType::ReleaseLock { resource })
                } else {
                    None
                }
            } else {
                None
            }
        }
        LocksMessage::Digest(digest) => {
            state.follower_digests.insert(from.clone(), digest.clone());
            let delta = state.current_snapshot.generate_delta(digest);
            let message = LocksMessage::Delta(delta);
            let _ = transport
                .send_unreliable(from, &message.serialize_for_send())
                .await;

            let _ = handle_commit_calculations(transport, config, state).await;

            None
        }
        LocksMessage::Delta(..) => None,
    };

    if let Some(new_entry) = new_entry {
        state.current_snapshot.push(new_entry);
    }

    Ok(())
}

async fn handle_message_as_replica(
    message: LocksMessage,
    from: NodeId,
    _transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let leader = config.raft_client().leader().await?;

    match message {
        LocksMessage::Delta(delta) => {
            if Some(from) == leader {
                state.current_snapshot.apply_delta(delta);
            }
        }
        _ => {}
    };

    Ok(())
}

async fn handle_heartbeat(
    _instant: Instant,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let leader = config.raft_client().leader().await?;

    if leader == Some(config.cluster.own_node_id) {
        let current_state = state.current_snapshot.proposed_state();
        let expired_locks = current_state
            .iter()
            .filter_map(|(resource, lock)| {
                let is_expired = lock.expires.gt(&SystemTime::now());

                match is_expired {
                    false => None,
                    true => Some(resource),
                }
            })
            .collect::<Vec<&String>>();

        for resource in expired_locks {
            state.current_snapshot.push(EntryType::ReleaseLock {
                resource: resource.to_owned(),
            });
        }
    } else {
        if let Some(leader) = leader {
            let digest = state.current_snapshot.generate_digest();
            let message = LocksMessage::Digest(digest);
            transport
                .send_unreliable(leader, &message.serialize_for_send())
                .await?;
        }
    }

    Ok(())
}

async fn handle_commit_calculations(
    _transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let raft_nodes = config.cluster_client().list_raft_nodes().await?;

    let checkpoints = state
        .follower_digests
        .iter()
        .filter(|(node, _digest)| raft_nodes.contains(node))
        .map(|(node, digest)| (digest.replicated(&state.current_snapshot), node))
        .collect::<Vec<_>>();

    let mut checkpoint_counts: HashMap<u128, u32> = HashMap::new();

    for (replicated, _node) in checkpoints {
        checkpoint_counts
            .entry(replicated)
            .and_modify(|count| {
                *count += 1;
            })
            .or_insert(1);
    }

    let min_replications_to_commit = (raft_nodes.len() as f64
        * config.locks.entry_commit_minimum_replication_ratio)
        .ceil() as u32;
    let mut committable_entries: Vec<_> = checkpoint_counts
        .iter()
        .filter_map(
            |(entry_id, replications)| match *replications >= min_replications_to_commit {
                true => Some(entry_id),
                false => None,
            },
        )
        .collect();

    committable_entries.sort();

    let last_committable = committable_entries.last().to_owned();

    last_committable.map(|index| {
        state.current_snapshot.commit_to(**index);
    });

    Ok(())
}

async fn handle_compaction(
    _transport: &impl Transport,
    _config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    state.current_snapshot.compact();

    Ok(())
}

#[derive(Debug)]
pub enum LocksClientRequest {
    GetLock {
        resource: String,
        response: oneshot::Sender<Option<Lock>>,
    },
    AquireLock {
        resource: String,
        data: String,
    },
    RenewLock {
        resource: String,
    },
    UpdateLockData {
        resource: String,
        data: String,
    },
    ReleaseLock {
        resource: String,
    },
}

async fn handle_client_request(
    req: LocksClientRequest,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let leader = config.raft_client().leader().await.ok().flatten();

    match req {
        LocksClientRequest::GetLock { resource, response } => {
            let locks = state.current_snapshot.committed_state();
            let lock = locks.get(&resource).map(|lock| lock.to_owned());

            let _ = response.send(lock);
            Ok(())
        }
        LocksClientRequest::AquireLock { resource, data } => {
            if let Some(leader_id) = leader {
                let msg = LocksMessage::AquireLock { resource, data };
                let data = msg.serialize_for_send();
                transport.send_reliable(leader_id, &data).await?;
            }

            Ok(())
        }
        LocksClientRequest::RenewLock { resource } => {
            if let Some(leader_id) = leader {
                let msg = LocksMessage::RenewLock { resource };
                let data = msg.serialize_for_send();
                transport.send_reliable(leader_id, &data).await?;
            }

            Ok(())
        }
        LocksClientRequest::UpdateLockData { resource, data } => {
            if let Some(leader_id) = leader {
                let msg = LocksMessage::EditLock {
                    resource,
                    new_data: data,
                };
                let data = msg.serialize_for_send();
                transport.send_reliable(leader_id, &data).await?;
            }

            Ok(())
        }
        LocksClientRequest::ReleaseLock { resource } => {
            if let Some(leader_id) = leader {
                let msg = LocksMessage::ReleaseLock { resource };
                let data = msg.serialize_for_send();
                transport.send_reliable(leader_id, &data).await?;
            }

            Ok(())
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocksClient {
    sender: mpsc::Sender<LocksClientRequest>,
    _cluster_client: ClusterClient,
    _raft_client: RaftClient,
    config: Config,
}

impl LocksClient {
    async fn lock_info(&self, resource: String) -> anyhow::Result<Option<Lock>> {
        let (tx, rx) = oneshot::channel();
        let req = LocksClientRequest::GetLock {
            resource,
            response: tx,
        };

        self.sender.send(req).await?;
        let result = rx.await?;

        Ok(result)
    }
    async fn aquire_lock(&self, resource: String, data: String) {
        let req = LocksClientRequest::AquireLock { resource, data };
        let _ = self.sender.send(req).await;
    }
    async fn renew_lock(&self, resource: String) {
        let req = LocksClientRequest::RenewLock { resource };
        let _ = self.sender.send(req).await;
    }
    async fn _modify_lock(&self, resource: String, data: String) {
        let req = LocksClientRequest::UpdateLockData { resource, data };
        let _ = self.sender.send(req).await;
    }
    async fn release_lock(&self, resource: String) {
        let req = LocksClientRequest::ReleaseLock { resource };
        let _ = self.sender.send(req).await;
    }
    pub fn lock(&self, resource: String, lock_renew_interval: Duration) -> LockHandle {
        let (tx, mut rx) = mpsc::channel::<LockHandleRequest>(32);

        let int_resource = resource.clone();
        let int_lock_renew_interval = lock_renew_interval.clone();
        let int_config = self.config.clone();

        tokio::spawn(async move {
            let resource = int_resource;

            let mut try_to_lock = false;
            let mut renew_interval = time::interval(int_lock_renew_interval);
            let config = int_config;

            let lock_client: LocksClient = config.clone().into();
            let _raft_client: RaftClient = config.clone().into();

            loop {
                let _ = tokio::select! {
                    Some(req) = rx.recv() => {
                        match req {
                            LockHandleRequest::Lock => {
                                try_to_lock = true;
                            }
                            LockHandleRequest::Unlock => {
                                try_to_lock = false;
                            }
                            LockHandleRequest::IsLocked(res) => {
                                let lock = lock_client.lock_info(resource.clone()).await.ok().flatten();
                                let is_locked_locally = lock.clone().map(|lock| lock.holder == config.cluster.own_node_id).unwrap_or(false);
                                let lock_has_not_expired = lock.clone().map(|lock| lock.expires > SystemTime::now()).unwrap_or(false);

                                let _ = res.send(is_locked_locally && lock_has_not_expired);
                            }
                        }
                    },
                    _ = renew_interval.tick() => {
                        let lock = lock_client.lock_info(resource.clone()).await.ok().flatten();
                        let is_locked_locally = lock.map(|lock| lock.holder == config.cluster.own_node_id).unwrap_or(false);

                        match try_to_lock {
                            true => {
                                if is_locked_locally {
                                    lock_client.aquire_lock(resource.clone(), "".to_string()).await;
                                } else {
                                    lock_client.renew_lock(resource.clone()).await;
                                }
                            }
                            false => {
                                if is_locked_locally {
                                    lock_client.release_lock(resource.clone()).await;
                                }
                            }
                        };
                    }
                };
            }
        });

        LockHandle {
            sender: tx,
            _resource: resource.clone(),
        }
    }
}

impl From<Config> for LocksClient {
    fn from(value: Config) -> Self {
        Self {
            sender: value.clone().locks.client,
            _cluster_client: value.clone().into(),
            _raft_client: value.clone().into(),
            config: value.clone(),
        }
    }
}

enum LockHandleRequest {
    Lock,
    Unlock,
    IsLocked(oneshot::Sender<bool>),
}

pub struct LockHandle {
    sender: mpsc::Sender<LockHandleRequest>,
    _resource: String,
}

impl LockHandle {
    pub async fn lock(&self) {
        self.sender.send(LockHandleRequest::Lock).await;
    }
    pub async fn is_locked(&self) -> anyhow::Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(LockHandleRequest::IsLocked(tx));
        let result = rx.await?;

        Ok(result)
    }
    pub async fn unlock(&self) {
        self.sender.send(LockHandleRequest::Unlock).await;
    }
}
