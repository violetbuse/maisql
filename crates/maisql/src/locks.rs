use std::{
    cmp::Ordering,
    collections::HashMap,
    mem,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use sha2::{self, Digest, Sha256};
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
    snapshot_checksum: [u8; 32],
    entry_id: u128,
    entry_checksum: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotDelta {
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
        self.entries.as_mut().map(|entries| entries.remove_entries(from));
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
        self.next.as_ref().map(|next_entry| next_entry.entries_from(entry_id)).flatten()
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
pub  fn remove_entries(&mut self, from: u128) {
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
                drop(entry);

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
            Self::Proposed(entry) => Self::Proposed(Box::new(entry.entries_to(entry_id)))
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
    todo!()
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
