use std::{collections::HashSet, mem, time::Duration};

use anyhow::anyhow;
use futures::future::join_all;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Instant},
};

use crate::{
    cluster::ClusterClient,
    config::Config,
    transport::{NodeId, Transport, TransportData},
};

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub client: mpsc::Sender<RaftClientRequest>,
    /// Does this node participate in raft
    pub raft_voter: bool,
    pub heartbeat_interval: Duration,
    pub min_election_timeout: Duration,
    pub max_election_timeout: Duration,
}

enum State {
    Voter { term: Term },
    Observer { leader: Option<NodeId>, term: u64 },
}

#[derive(Debug, Clone, Default)]
struct Term {
    id: u64,
    voted: bool,
    votes_received: HashSet<NodeId>,
    leader: Option<NodeId>,
    last_leader_heartbeat: Option<Instant>,
    previous_term: Option<Box<Term>>,
}

impl Term {
    pub fn new_term(&mut self) {
        let mut next_term = Term::default();
        next_term.id = self.id + 1;

        let old_term = mem::replace(self, next_term);
        self.previous_term = Some(Box::new(old_term));
    }
    pub fn push_term(&mut self, new_term_id: u64) {
        let mut new_term = Term::default();
        new_term.id = new_term_id;

        let old_term = mem::replace(self, new_term);
        self.previous_term = Some(Box::new(old_term));
    }
    pub fn add_vote(&mut self, voted: NodeId) {
        self.votes_received.insert(voted);
    }
    pub fn majority(&self, node_count: usize) -> bool {
        (self.votes_received.len() as f64 / node_count as f64) > 0.5
    }
    pub fn register_leader_heartbeat(&mut self) {
        self.last_leader_heartbeat = Some(Instant::now());
    }
}

pub async fn run_raft(
    transport: &impl Transport,
    config: Config,
    client_rx: &mut mpsc::Receiver<RaftClientRequest>,
) -> anyhow::Result<()> {
    let mut rng = rand::thread_rng();
    let election_timeout =
        rng.gen_range(config.raft.min_election_timeout..config.raft.max_election_timeout);

    let mut election_timeout_interval = time::interval(election_timeout);
    let mut heartbeat_interval = time::interval(config.raft.heartbeat_interval);

    let mut state = match config.raft.raft_voter {
        true => State::Voter {
            term: Term::default(),
        },
        false => State::Observer {
            leader: None,
            term: 0,
        },
    };

    if config.raft.raft_voter {
        loop {
            let _ = tokio::select! {
                _ = election_timeout_interval.tick() => handle_election_timeout(election_timeout, transport, config.to_owned(), &mut state).await,
                _ = heartbeat_interval.tick() => handle_own_heartbeat(transport, config.to_owned(), &mut state).await,
                Some((bytes, from)) = transport.recv() => handle_transport_message(bytes, from, transport, config.to_owned(), &mut state).await,
                Some(req) = client_rx.recv() => handle_raft_client_request(req, transport, config.to_owned(), &mut state).await,
            };
        }
    } else {
        loop {
            let _ = tokio::select! {
                Some((bytes, from)) = transport.recv() => handle_transport_message(bytes, from, transport, config.to_owned(), &mut state).await,
                Some(req) = client_rx.recv() => handle_raft_client_request(req, transport, config.to_owned(), &mut state).await
            };
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "raft_msg")]
pub enum RaftMessage {
    LeaderHeartbeat { term: u64 },
    AnnounceLeadership { term: u64 },
    AnnounceCandidate { term: u64 },
    CommitVote { term: u64 },
}

impl TransportData for RaftMessage {}

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
            handle_leader_heartbeat(term, from, transport, config, state).await
        }
        RaftMessage::AnnounceLeadership { term } => {
            handle_announce_leadership(term, from, transport, config, state).await
        }
        RaftMessage::AnnounceCandidate { term } => {
            handle_announce_candidate(term, from, transport, config, state).await
        }
        RaftMessage::CommitVote { term } => {
            handle_commit_vote(term, from, transport, config, state).await
        }
    }
}

async fn handle_leader_heartbeat(
    req_term: u64,
    from: NodeId,
    _transport: &impl Transport,
    _config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match state {
        State::Voter { term } => {
            if req_term == term.id {
                term.register_leader_heartbeat();
            } else if req_term > term.id {
                term.push_term(req_term);
                term.register_leader_heartbeat();
            }
        }
        State::Observer { leader, term } => {
            if req_term >= *term {
                *term = req_term;
                *leader = Some(from);
            }
        }
    }

    Ok(())
}

async fn handle_announce_leadership(
    req_term: u64,
    from: NodeId,
    _transport: &impl Transport,
    _config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match state {
        State::Voter { term } => {
            if req_term > term.id {
                term.push_term(req_term);
                term.leader = Some(from);
                term.register_leader_heartbeat();
            } else if req_term == term.id {
                term.leader = Some(from);
                term.register_leader_heartbeat();
            }
        }
        State::Observer { leader, term } => {
            if req_term >= *term {
                *term = req_term;
                *leader = Some(from);
            }
        }
    }

    Ok(())
}

async fn handle_announce_candidate(
    req_term: u64,
    from: NodeId,
    transport: &impl Transport,
    _config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match state {
        State::Voter { term } => {
            if req_term > term.id {
                term.push_term(req_term);
            }

            if !term.voted {
                term.voted = true;
                let commit_msg = RaftMessage::CommitVote { term: req_term };

                let _ = transport
                    .send_reliable(from, &commit_msg.serialize_for_send())
                    .await;
            }
        }
        State::Observer { leader, term } => {
            if req_term > *term {
                *leader = None;
                *term = req_term;
            }
        }
    }

    Ok(())
}

async fn handle_commit_vote(
    req_term: u64,
    from: NodeId,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match state {
        State::Voter { term } => {
            if req_term == term.id {
                term.add_vote(from);

                let raft_nodes = config.cluster_client().list_raft_nodes().await?;
                let promote = term.majority(raft_nodes.len());

                if promote {
                    term.leader = Some(config.cluster.own_node_id);
                    let msg = RaftMessage::AnnounceLeadership { term: req_term };
                    let data = msg.serialize_for_send();

                    join_all(
                        raft_nodes
                            .iter()
                            .map(|node| transport.send_reliable(node.to_owned(), &data)),
                    )
                    .await;
                }
            }
        }
        State::Observer { .. } => {}
    }

    Ok(())
}

async fn handle_own_heartbeat(
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    if let State::Voter { term: state_term } = state {
        let local_is_leader = state_term.leader == Some(config.clone().cluster.own_node_id);

        if local_is_leader {
            let raft_nodes = config.clone().cluster_client().list_raft_nodes().await?;
            let message = RaftMessage::LeaderHeartbeat {
                term: state_term.id,
            };
            let serialized = message.serialize_for_send();

            join_all(
                raft_nodes
                    .iter()
                    .map(|node| transport.send_reliable(node.to_owned(), &serialized)),
            )
            .await;
        }
    }

    Ok(())
}

async fn handle_election_timeout(
    election_timeout: Duration,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    if let State::Voter { term } = state {
        let elapsed_since_hb = term
            .last_leader_heartbeat
            .map(|hb| Instant::now().duration_since(hb))
            .unwrap_or(
                election_timeout
                    .checked_add(Duration::from_secs(5))
                    .unwrap(),
            );

        let leader_exists_but_died =
            term.leader.is_some() && elapsed_since_hb.gt(&election_timeout);

        let should_become_candidate = leader_exists_but_died && term.voted == false;

        if should_become_candidate {
            term.new_term();

            term.voted = true;
            term.votes_received
                .insert(config.cluster.own_node_id.clone());

            let raft_nodes = config.cluster_client().list_raft_nodes().await?;
            let raft_node_count = raft_nodes.len();

            if raft_node_count == 1 {
                term.leader = Some(config.cluster.own_node_id.clone());
            } else {
                let announcement = RaftMessage::AnnounceCandidate { term: term.id };
                let serialized = announcement.serialize_for_send();

                let _ = join_all(
                    raft_nodes
                        .iter()
                        .map(|node| transport.send_reliable(node.to_owned(), &serialized)),
                )
                .await;
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
pub enum RaftClientRequest {
    QueryLeader {
        response: oneshot::Sender<Option<NodeId>>,
    },
}

async fn handle_raft_client_request(
    req: RaftClientRequest,
    _transport: &impl Transport,
    _config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match req {
        RaftClientRequest::QueryLeader { response } => {
            let leader = match state {
                State::Voter { term } => term.leader.to_owned(),
                State::Observer { leader, .. } => leader.to_owned(),
            };

            return response
                .send(leader)
                .map_err(|_| anyhow!("could not send raft leader response."));
        }
    }
}

#[derive(Debug, Clone)]
pub struct RaftClient {
    sender: mpsc::Sender<RaftClientRequest>,
    _cluster: ClusterClient,
    _config: Config,
}

impl RaftClient {
    pub async fn leader(&self) -> anyhow::Result<Option<NodeId>> {
        let (tx, rx) = oneshot::channel();
        let req = RaftClientRequest::QueryLeader { response: tx };
        self.sender.send(req).await?;

        let response = rx.await?;
        Ok(response)
    }
    pub async fn self_is_leader(&self, config: Config) -> anyhow::Result<bool> {
        let leader = self.leader().await?;
        let leader_is_self = leader
            .map(|leader| leader == config.cluster.own_node_id)
            .unwrap_or(false);

        Ok(leader_is_self)
    }
}

impl From<Config> for RaftClient {
    fn from(value: Config) -> Self {
        Self {
            sender: value.clone().raft.client,
            _cluster: value.clone().into(),
            _config: value.clone(),
        }
    }
}
