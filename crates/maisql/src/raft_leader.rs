use std::{collections::HashSet, time::Duration};

use anyhow::anyhow;
use futures::future::join_all;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    time::{self, Instant},
};

use crate::{
    config::Config,
    transport::{NodeId, RequestHandle, ResponseHandle, Transport, TransportData},
};

#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Does this node participate in raft
    pub raft_voter: bool,
    pub heartbeat_interval: Duration,
    pub min_election_timeout: Duration,
    pub max_election_timeout: Duration,
}

struct State {
    term: Term,
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

    let mut state = State {
        term: Term {
            id: 0,
            leader: None,
            voted: false,
            votes_received: HashSet::new(),
            last_leader_heartbeat: None,
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
    if term == state.term.id {
        state.term.last_leader_heartbeat = Some(Instant::now());
    } else if term > state.term.id {
        state.term = Term {
            id: term,
            leader: Some(from),
            voted: false,
            votes_received: HashSet::new(),
            last_leader_heartbeat: Some(Instant::now()),
        };
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
    if term >= state.term.id {
        state.term.last_leader_heartbeat = Some(Instant::now());
        state.term.leader = Some(from);
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
    if term > state.term.id {
        state.term = Term {
            id: term,
            leader: None,
            voted: false,
            votes_received: HashSet::new(),
            last_leader_heartbeat: None,
        };
    }

    if !state.term.voted {
        state.term.voted = true;
        let commit_msg = RaftMessage::CommitVote { term };

        transport
            .send_reliable(from, &commit_msg.serialize_for_send())
            .await;
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
    if term == state.term.id {
        state.term.votes_received.insert(from);
        let total_votes_received = state.term.votes_received.len();
        let raft_nodes = config.cluster_client.list_raft_nodes().await?;
        let raft_node_count = raft_nodes.len();

        let vote_percentage = total_votes_received as f64 / raft_node_count as f64;

        if vote_percentage > 0.5 {
            state.term.leader = Some(config.cluster_config.own_node_id);
            let message = RaftMessage::AnnounceLeadership { term };
            let serialized = message.serialize_for_send();
            join_all(
                raft_nodes
                    .iter()
                    .map(|node| transport.send_reliable(node.to_owned(), &serialized.as_slice())),
            );
        }
    }

    Ok(())
}

async fn handle_own_heartbeat(
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let local_is_leader = state.term.leader == Some(config.cluster_config.own_node_id);

    if local_is_leader {
        let raft_nodes = config.cluster_client.list_raft_nodes().await?;
        let message = RaftMessage::LeaderHeartbeat {
            term: state.term.id,
        };
        let serialized = message.serialize_for_send();

        join_all(
            raft_nodes
                .iter()
                .map(|node| transport.send_reliable(node.to_owned(), &serialized)),
        );
    }

    Ok(())
}

async fn handle_election_timeout(
    election_timeout: Duration,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let elapsed_since_hb = state
        .term
        .last_leader_heartbeat
        .map(|hb| Instant::now().duration_since(hb))
        .unwrap_or(
            election_timeout
                .checked_add(Duration::from_secs(5))
                .unwrap(),
        );

    let leader_exists_but_died =
        state.term.leader.is_some() && elapsed_since_hb.gt(&election_timeout);

    let should_become_candidate = leader_exists_but_died && state.term.voted == false;

    if should_become_candidate {
        state.term = Term {
            id: state.term.id + 1,
            leader: None,
            voted: false,
            votes_received: HashSet::new(),
            last_leader_heartbeat: None,
        };

        state.term.voted = true;
        state
            .term
            .votes_received
            .insert(config.cluster_config.own_node_id.clone());

        let raft_nodes = config.cluster_client.list_raft_nodes().await?;
        let raft_node_count = raft_nodes.len();

        if raft_node_count == 1 {
            state.term.leader = Some(config.cluster_config.own_node_id.clone());
        } else {
            let announcement = RaftMessage::AnnounceCandidate {
                term: state.term.id,
            };
            let serialized = announcement.serialize_for_send();

            join_all(
                raft_nodes
                    .iter()
                    .map(|node| transport.send_reliable(node.to_owned(), &serialized)),
            );
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "raft_network_req")]
pub enum RaftNetworkRequest {
    LeaderQuery,
}

impl TransportData for RaftNetworkRequest {}

async fn handle_raft_network_req(
    req: &impl RequestHandle,
    _transport: &impl Transport,
    _config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let (data, _) = req.data();
    let data = RaftNetworkRequest::parse(&data, "raft_network_request")?;

    let res = req.accept()?;

    match data {
        RaftNetworkRequest::LeaderQuery => {
            let leader = state.term.leader.to_owned();
            let serialized = rmp_serde::to_vec(&leader)?;

            res.send_response(&serialized);
        }
    }

    Ok(())
}

#[derive(Debug)]
pub enum RaftClientRequest {
    QueryLeader {
        response: oneshot::Sender<Result<Option<NodeId>, ()>>,
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
        response.map_err(|_| anyhow!("Could not determine raft cluster leader"))
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
            handle_leader_query(response, transport, config, state).await
        }
    }
}

async fn handle_leader_query(
    channel: oneshot::Sender<Result<Option<NodeId>, ()>>,
    transport: &impl Transport,
    config: Config,
    state: &mut State,
) -> anyhow::Result<()> {
    let leader: Result<Option<NodeId>, ()> = match config.raft_config.raft_voter {
        true => Ok(state.term.leader.to_owned()),
        false => {
            let node = config
                .cluster_client
                .list_raft_nodes()
                .await
                .ok()
                .map(|vec| vec.first().map(|node| node.to_owned()))
                .flatten()
                .map(|node| node.clone());

            match node {
                None => Err(()),
                Some(node) => {
                    let req = RaftNetworkRequest::LeaderQuery.serialize_for_send();
                    let res = transport
                        .send_request(node, &req, Duration::from_millis(1500))
                        .await;

                    res.ok()
                        .map(|bytes| rmp_serde::from_slice::<Option<NodeId>>(&bytes).ok())
                        .flatten()
                        .ok_or(())
                }
            }
        }
    };

    channel.send(leader).unwrap();
    Ok(())
}
