use std::time::{Duration, SystemTime};

use futures::future;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    time::{self, Instant},
};

use crate::{
    config::Config,
    transport::{RequestHandle, Transport, TransportData},
};

#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Does this node participate in raft
    pub raft_voter: bool,
    /// Must be the lowest interval
    pub tick_interval: Duration,
    /// Recommended to be anywhere between 1/5 and 1/2 of the min election timeout.
    /// Lower is better.
    pub heartbeat_interval: Duration,
    pub min_election_timeout: Duration,
    pub max_election_timeout: Duration,
    /// How many terms need to pass before one is cleaned up
    pub term_cleanup: u64,
    /// How long to wait before removing heartbeat entries from memory
    pub leader_heartbeat_cleanup: Duration,
    /// Value between 0 and 1
    pub min_vote_ratio_to_lead: f64,
}

struct State {}

pub async fn run_raft(
    transport: &impl Transport,
    config: &mut broadcast::Receiver<Config>,
    client_sender: oneshot::Sender<RaftClient>,
) -> anyhow::Result<()> {
    let (client, mut client_rx) = RaftClient::new();
    client_sender.send(client).unwrap();

    let config = config.recv().await.unwrap();

    let mut rng = rand::thread_rng();

    let state = State {};

    let election_timeout = rng.gen_range(
        config.raft_config.min_election_timeout..config.raft_config.max_election_timeout,
    );

    let mut election_timeout_interval = time::interval(election_timeout);
    let mut heartbeat_interval = time::interval(config.raft_config.heartbeat_interval);
    let mut tick_interval = time::interval(config.raft_config.tick_interval);

    loop {
        let _ = tokio::select! {
            _instant = election_timeout_interval.tick() => handle_election_timeout(election_timeout, transport, config.to_owned(), &mut state).await,
            _instant = tick_interval.tick() => handle_tick(transport, config.to_owned(), &mut state).await,
            instant = heartbeat_interval.tick() => handle_own_heartbeat(instant, transport, config.to_owned(), &mut state).await,
            Some(req) = client_rx.recv() => handle_raft_client_request(req, transport, config.to_owned(), &mut state).await,
            Some((bytes, from)) = transport.recv() => handle_transport_message(bytes, from, transport, config.to_owned(), &mut state).await,
        };
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "raft_msg")]
pub enum RaftMessage {
    LeaderHeartbeat { term: u64 },
    MemberHeartbeat,
    AnnounceCandidacy { term: u64 },
    CommitVote { term: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "resource_lock_type")]
pub enum Resource {}

impl TransportData for RaftMessage {}

impl State {
    pub fn register_raft_leader_heartbeat(&mut self, leader: NodeId, term: u64) {
        self.raft_terms
            .entry(term)
            .and_modify(|term_data| {
                match term_data.leader.clone() {
                    None => {
                        term_data.leader = Some(leader.clone());
                        term_data.leader_heartbeats.push(SystemTime::now());
                    }
                    Some(curr_leader) if curr_leader == leader => {
                        term_data.leader_heartbeats.push(SystemTime::now());
                    }
                    Some(_) => {}
                };
            })
            .or_insert_with(|| {
                let mut term = TermData::default();
                term.leader = Some(leader);
                term.leader_heartbeats.push(SystemTime::now());
                return term;
            });
    }
    pub fn handle_candidature_announcement(&mut self, candidate: NodeId, term: u64) -> bool {
        let term_entry = self
            .raft_terms
            .entry(term)
            .and_modify(|term_data| match term_data.voted_for.clone() {
                None => term_data.voted_for = Some(candidate.clone()),
                Some(_) => {}
            })
            .or_insert_with(|| {
                let mut term = TermData::default();
                term.voted_for = Some(candidate.clone());
                return term;
            });

        return term_entry.voted_for == Some(candidate.clone());
    }
    pub fn handle_vote_commitment(&mut self, term: u64, from: NodeId) {
        self.raft_terms.entry(term).and_modify(|term_data| {
            term_data.votes_received.insert(from);
        });
    }
    pub fn current_raft_term(&self) -> u64 {
        let mut current_term = 0;
        for (id, _) in self.raft_terms.clone() {
            if current_term < id {
                current_term = id;
            }
        }

        return current_term;
    }
    pub fn current_raft_leader(&self) -> Option<NodeId> {
        let current_term = self.current_raft_term();
        self.raft_terms
            .get(&current_term)
            .map(|term_data| term_data.leader.clone())
            .flatten()
    }
}

async fn handle_transport_message(
    bytes: &[u8],
    from: NodeId,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let message = RaftMessage::parse(bytes, "raft_message")?;

    match message {
        RaftMessage::LeaderHeartbeat { term } => {
            global.map_state_mut(|state| state.register_raft_leader_heartbeat(from.clone(), term))
        }
        RaftMessage::MemberHeartbeat => {
            global.map_state_mut(|state| {
                state.nodes.entry(from.clone()).and_modify(|node_state| {
                    node_state.raft.member = true;
                });
            });
        }
        RaftMessage::AnnounceCandidacy { term } => {
            let voted_for_candidate = global.map_state_mut(|state| {
                state.handle_candidature_announcement(from.clone(), term.clone())
            });
            if voted_for_candidate {
                let message = RaftMessage::CommitVote { term };
                let data = TransportData::serialize(&message);
                transport.send_reliable(from, &data).await;
            }
        }
        RaftMessage::CommitVote { term } => {
            global.map_state_mut(|state| {
                state.handle_vote_commitment(term.clone(), from.clone());
            });
        }
    };

    Ok(())
}

async fn handle_tick(
    _transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let current_term = global.map_state(|state| state.current_raft_term());

    // do some cleanup first
    global.map_state_mut(|state| {
        state
            .raft_terms
            .retain(|k, _v| *k > current_term - global.config.raft_config.term_cleanup);

        let heartbeat_cleanup_cutoff =
            SystemTime::now().checked_sub(global.config.raft_config.leader_heartbeat_cleanup);

        for (_, term) in state.raft_terms.iter_mut() {
            term.leader_heartbeats.retain(|heartbeat| {
                heartbeat_cleanup_cutoff
                    .clone()
                    .map(|cutoff| heartbeat.gt(&cutoff))
                    .unwrap_or(true)
            });
        }
    });

    // confirm leadership
    global.map_state_mut(|state| {
        let node_count = state.nodes.keys().count();

        state
            .raft_terms
            .entry(current_term)
            .and_modify(|term_data| {
                let vote_count = term_data.votes_received.len();
                let ratio = vote_count as f64 / node_count as f64;

                if ratio > global.config.raft_config.min_vote_ratio_to_lead {
                    term_data.leader = Some(state.local_node.0.to_owned());
                }
            });
    });

    Ok(())
}

async fn handle_own_heartbeat(
    _instant: Instant,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let current_term = global.map_state(|state| state.current_raft_term());
    let current_leader = global.map_state(|state| state.current_raft_leader());

    let is_current_leader = current_leader
        .map(|leader| leader == global.config.local_node_id)
        .unwrap_or(false);

    let heartbeat_message = match is_current_leader {
        false => RaftMessage::MemberHeartbeat,
        true => RaftMessage::LeaderHeartbeat { term: current_term },
    };

    let heartbeat_data = TransportData::serialize(&heartbeat_message);
    let nodes: Vec<_> = global.map_state(|state| state.nodes.keys().cloned().collect());

    let _ = future::join_all(
        nodes
            .iter()
            .map(|node_id| transport.send_reliable(node_id.clone(), &heartbeat_data)),
    );
    Ok(())
}

async fn handle_election_timeout(
    election_timeout: Duration,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let current_term = global.map_state(|state| state.current_raft_term());

    let received_leader_heartbeat = global.map_state(|state| -> bool {
        let is_current_leader = state.current_raft_leader() == Some(state.local_node.0.clone());

        if is_current_leader {
            return true;
        }

        let current_term = state.current_raft_term();
        let term = state.raft_terms.get(&current_term);

        let heartbeats_in_interval = term
            .map(|term| {
                term.leader_heartbeats
                    .iter()
                    .filter(|hb| {
                        SystemTime::now()
                            .duration_since(**hb)
                            .map(|diff| diff.gt(&election_timeout))
                            .unwrap_or(false)
                    })
                    .count()
                    > 0
            })
            .unwrap_or(false);

        return heartbeats_in_interval;
    });

    if !received_leader_heartbeat {
        let nodes = global.map_state_mut(|state| {
            state
                .raft_terms
                .insert(current_term + 1, TermData::default());

            return state
                .nodes
                .iter()
                .map(|(node_id, _)| node_id.clone())
                .collect::<Vec<_>>();
        });

        let candidacy = RaftMessage::AnnounceCandidacy {
            term: current_term + 1,
        };
        let data = TransportData::serialize(&candidacy);

        let mut messages: Vec<_> = Vec::new();

        for node in nodes {
            let msg = transport.send_reliable(node, &data);
            messages.push(msg);
        }

        future::join_all(messages);
    }

    Ok(())
}

#[derive(Debug)]
pub enum RaftClientRequest {}

#[derive(Debug, Clone)]
pub struct RaftClient {
    sender: mpsc::Sender<RaftClientRequest>,
}

impl RaftClient {
    pub fn new() -> (Self, mpsc::Receiver<RaftClientRequest>) {
        let (tx, rx) = mpsc::channel(128);
        return (Self { sender: tx }, rx);
    }
}

async fn handle_raft_client_request(
    req: RaftClientRequest,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
}
