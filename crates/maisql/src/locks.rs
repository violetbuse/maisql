use std::{
    collections::HashMap,
    mem,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    config::Config,
    transport::{NodeId, Transport},
};

#[derive(Debug, Clone)]
pub struct LocksConfig {
    lock_default_timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotDigest {
    snapshot_id: u128,
    entry_id: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotDelta {
    FullSnapshot(Snapshot),
    Entries(Entry),
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
    pub fn entries_from(&self, entry_id: u128) -> Option<Entry> {
        self.entries
            .as_ref()
            .map(|entry| entry.entries_from(entry_id))
            .flatten()
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
    pub fn generate_digest(&self) -> SnapshotDigest {
        let last = self.last_id();
        SnapshotDigest {
            snapshot_id: self.id,
            entry_id: last,
        }
    }
    pub fn generate_delta(&self, digest: SnapshotDigest) -> SnapshotDelta {
        if self.id > digest.snapshot_id {
            SnapshotDelta::FullSnapshot(self.clone())
        } else {
            match self.last_id() > digest.entry_id {
                true => match self.entries_from(digest.entry_id + 1) {
                    Some(entry) => SnapshotDelta::Entries(entry),
                    None => SnapshotDelta::EmptyDelta,
                },
                false => SnapshotDelta::EmptyDelta,
            }
        }
    }
    pub fn apply_delta(&mut self, delta: SnapshotDelta) {
        match delta {
            SnapshotDelta::FullSnapshot(snap) => {
                *self = snap;
            }
            SnapshotDelta::Entries()
        }
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
    pub fn entries_from(&self, entry_id: u128) -> Option<Entry> {
        if entry_id < self.id {
            None
        } else if entry_id == self.id {
            Some(self.clone())
        } else {
            self.next
                .as_ref()
                .map(|next_entry| next_entry.entries_from(entry_id))
                .flatten()
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum NextEntry {
    Committed(Box<Entry>),
    Proposed(Box<Entry>),
}

impl NextEntry {
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
        match self {
            Self::Committed(entry) | Self::Proposed(entry) => entry.last_id(),
        }
    }
    pub fn push(&mut self, value: EntryType) {
        match self {
            Self::Committed(entry) | Self::Proposed(entry) => entry.push(value),
        }
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
                drop(entry);

                *curr = Self::Committed(internal);
            }
        }
    }
    pub fn entries_from(&self, entry_id: u128) -> Option<Entry> {
        match self {
            Self::Committed(entry) | Self::Proposed(entry) => entry.entries_from(entry_id),
        }
    }
    pub fn compact(&self, prev_locks: &mut HashMap<String, Lock>) -> Snapshot {
        match self {
            Self::Committed(entry) | Self::Proposed(entry) => entry.compact(prev_locks),
        }
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
struct Lock {
    holder: NodeId,
    expires: SystemTime,
    data: String,
}

pub async fn run_locks(
    transport: &impl Transport,
    config: &mut broadcast::Receiver<Config>,
    client_sender: oneshot::Sender<LocksClient>,
) -> anyhow::Result<()> {
}

#[derive(Debug, Clone)]
pub enum LocksClientRequest {}

pub struct LocksClient {
    sender: mpsc::Sender<LocksClientRequest>,
}

impl LocksClient {
    pub fn new() -> (Self, mpsc::Receiver<LocksClientRequest>) {
        let (tx, rx) = mpsc::channel(128);
        return (Self { sender: tx }, rx);
    }
}
