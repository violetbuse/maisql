use std::{collections::HashSet, time::Duration};

use futures::future::join_all;
use rand::Rng;
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
pub struct RaftConfig {
    /// Does this node participate in raft
    pub raft_voter: bool,
    pub heartbeat_interval: Duration,
    pub min_election_timeout: Duration,
    pub max_election_timeout: Duration,
}

// struct State {
//     term: Term,
// }

enum State {
    Voter {
        term: Term,
    },
    Observer {
        leader: Option<NodeId>,
        current_term: u64,
    },
}

struct Term {
    id: u64,
    leader: Option<NodeId>,
    voted: bool,
    votes_received: HashSet<NodeId>,
    last_leader_heartbeat: Option<Instant>,
}

pub async fn run_raft(
    transport: &impl Transport,
    config: &mut broadcast::Receiver<Config>,
    client_sender: oneshot::Sender<RaftClient>,
) -> anyhow::Result<()> {
    let (client, mut client_rx) = RaftClient::new();
    client_sender.send(client);

    let config = config.recv().await.unwrap();
    let mut rng = rand::thread_rng();
    let election_timeout = rng.gen_range(
        config.raft_config.min_election_timeout..config.raft_config.max_election_timeout,
    );

    let mut election_timeout_interval = time::interval(election_timeout);
    let mut heartbeat_interval = time::interval(config.raft_config.heartbeat_interval);

    let mut state = match config.raft_config.raft_voter {
        true => State::Voter {
            term: Term {
                id: 0,
                leader: None,
                voted: false,
                votes_received: HashSet::new(),
                last_leader_heartbeat: None,
            },
        },
        false => State::Observer {
            leader: None,
            current_term: 0,
        },
    };

    if config.raft_config.raft_voter {
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
    term: u64,
    from: NodeId,
    _transport: &impl Transport,
    _config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match state {
        State::Voter { term: state_term } => {
            if term == state_term.id {
                state_term.last_leader_heartbeat = Some(Instant::now());
            } else if term > state_term.id {
                *state_term = Term {
                    id: term,
                    leader: Some(from),
                    voted: false,
                    votes_received: HashSet::new(),
                    last_leader_heartbeat: Some(Instant::now()),
                };
            }
        }
        State::Observer {
            leader,
            current_term,
        } => {
            if term >= *current_term {
                *current_term = term;
                *leader = Some(from);
            }
        }
    }

    Ok(())
}

async fn handle_announce_leadership(
    term: u64,
    from: NodeId,
    _transport: &impl Transport,
    _config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match state {
        State::Voter { term: state_term } => {
            if term >= state_term.id {
                state_term.last_leader_heartbeat = Some(Instant::now());
                state_term.leader = Some(from);
            }
        }
        State::Observer {
            leader,
            current_term,
        } => {
            if term >= *current_term {
                *current_term = term;
                *leader = Some(from)
            }
        }
    }

    Ok(())
}

async fn handle_announce_candidate(
    term: u64,
    from: NodeId,
    transport: &impl Transport,
    _config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match state {
        State::Voter { term: state_term } => {
            if term > state_term.id {
                *state_term = Term {
                    id: term,
                    leader: None,
                    voted: false,
                    votes_received: HashSet::new(),
                    last_leader_heartbeat: None,
                };
            }

            if !state_term.voted {
                state_term.voted = true;
                let commit_msg = RaftMessage::CommitVote { term };

                transport
                    .send_reliable(from, &commit_msg.serialize_for_send())
                    .await;
            }
        }
        State::Observer {
            leader,
            current_term,
        } => {
            if term > *current_term {
                *leader = None;
                *current_term = term;
            }
        }
    }

    Ok(())
}

async fn handle_commit_vote(
    term: u64,
    from: NodeId,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match state {
        State::Voter { term: state_term } => {
            if term == state_term.id {
                state_term.votes_received.insert(from);
                let total_votes_received = state_term.votes_received.len();
                let raft_nodes = config.cluster_client.list_raft_nodes().await?;
                let raft_node_count = raft_nodes.len();

                let vote_percentage = total_votes_received as f64 / raft_node_count as f64;

                if vote_percentage > 0.5 {
                    state_term.leader = Some(config.cluster_config.own_node_id);
                    let message = RaftMessage::AnnounceLeadership { term };
                    let serialized = message.serialize_for_send();
                    join_all(raft_nodes.iter().map(|node| {
                        transport.send_reliable(node.to_owned(), &serialized.as_slice())
                    }));
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
        let local_is_leader = state_term.leader == Some(config.cluster_config.own_node_id);

        if local_is_leader {
            let raft_nodes = config.cluster_client.list_raft_nodes().await?;
            let message = RaftMessage::LeaderHeartbeat {
                term: state_term.id,
            };
            let serialized = message.serialize_for_send();

            join_all(
                raft_nodes
                    .iter()
                    .map(|node| transport.send_reliable(node.to_owned(), &serialized)),
            );
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
    if let State::Voter { term: state_term } = state {
        let elapsed_since_hb = state_term
            .last_leader_heartbeat
            .map(|hb| Instant::now().duration_since(hb))
            .unwrap_or(
                election_timeout
                    .checked_add(Duration::from_secs(5))
                    .unwrap(),
            );

        let leader_exists_but_died =
            state_term.leader.is_some() && elapsed_since_hb.gt(&election_timeout);

        let should_become_candidate = leader_exists_but_died && state_term.voted == false;

        if should_become_candidate {
            *state_term = Term {
                id: state_term.id + 1,
                leader: None,
                voted: false,
                votes_received: HashSet::new(),
                last_leader_heartbeat: None,
            };

            state_term.voted = true;
            state_term
                .votes_received
                .insert(config.cluster_config.own_node_id.clone());

            let raft_nodes = config.cluster_client.list_raft_nodes().await?;
            let raft_node_count = raft_nodes.len();

            if raft_node_count == 1 {
                state_term.leader = Some(config.cluster_config.own_node_id.clone());
            } else {
                let announcement = RaftMessage::AnnounceCandidate {
                    term: state_term.id,
                };
                let serialized = announcement.serialize_for_send();

                join_all(
                    raft_nodes
                        .iter()
                        .map(|node| transport.send_reliable(node.to_owned(), &serialized)),
                );
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

#[derive(Debug, Clone)]
pub struct RaftClient {
    sender: mpsc::Sender<RaftClientRequest>,
}

impl RaftClient {
    pub fn new() -> (Self, mpsc::Receiver<RaftClientRequest>) {
        let (tx, rx) = mpsc::channel(128);
        return (Self { sender: tx }, rx);
    }
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
            .map(|leader| leader == config.cluster_config.own_node_id)
            .unwrap_or(false);

        Ok(leader_is_self)
    }
}

async fn handle_raft_client_request(
    req: RaftClientRequest,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    match req {
        RaftClientRequest::QueryLeader { response } => {
            let leader = match state {
                State::Voter { term } => term.leader.to_owned(),
                State::Observer { leader, .. } => leader.to_owned(),
            };

            response.send(leader);

            Ok(())
        }
    }
}
