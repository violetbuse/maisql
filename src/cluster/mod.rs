pub mod client;
use anyhow::Result;
use etcd_client::Client as EtcdClient;
use futures_util::stream::StreamExt;
use futures_util::FutureExt;
use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;

#[derive(Clone)]
pub struct Cluster {
    command_tx: mpsc::Sender<ClusterCommand>,
}

#[derive()]
pub struct ClusterState {
    command_rx: mpsc::Receiver<ClusterCommand>,
    connections: Vec<(
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        std::net::SocketAddr,
    )>,
    etcd_client: Arc<EtcdClient>,
}

#[derive(Debug)]
pub enum ClusterCommand {
    Ping(oneshot::Sender<Result<()>>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterRequest {
    #[serde(rename = "request_id")]
    id: u64,
    from: SocketAddr,
    data: ClusterRequestData,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "content")]
pub enum ClusterRequestData {
    Ping,
    UpdateAddress(SocketAddr),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterResponse {
    #[serde(rename = "response_id")]
    id: u64,
    from: SocketAddr,
    data: ClusterResponseData,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "content")]
pub enum ClusterResponseData {
    Pong,
    AddressUpdated,
}

impl Cluster {
    pub fn new(socket_addr: SocketAddr, etcd_client: Arc<EtcdClient>) -> Self {
        let (command_tx, command_rx) = mpsc::channel(32);

        let state = ClusterState {
            command_rx,
            connections: Vec::new(),
            etcd_client,
        };

        tokio::spawn(async move {
            if let Err(e) = Self::run(state, socket_addr).await {
                eprintln!("Cluster run encountered an error: {}", e);
                std::process::exit(1);
            }
        });

        Self { command_tx }
    }

    async fn handle_ping_request(
        mut ws_sender: futures_util::stream::SplitSink<&mut WebSocketStream<TcpStream>, Message>,
        request: &ClusterRequest,
        addr: &SocketAddr,
    ) -> Result<()> {
        let response = ClusterResponse {
            id: request.id,
            from: *addr,
            data: ClusterResponseData::Pong,
        };
        let response_text = serde_json::to_string(&response)?;
        ws_sender.send(Message::Text(response_text)).await?;
        Ok(())
    }

    async fn handle_update_address_request(
        mut ws_sender: futures_util::stream::SplitSink<&mut WebSocketStream<TcpStream>, Message>,
        request: &ClusterRequest,
        addr: &SocketAddr,
    ) -> Result<()> {
        let response = ClusterResponse {
            id: request.id,
            from: *addr,
            data: ClusterResponseData::AddressUpdated,
        };
        let response_text = serde_json::to_string(&response)?;
        ws_sender.send(Message::Text(response_text)).await?;
        Ok(())
    }

    async fn handle_request(
        mut ws_sender: futures_util::stream::SplitSink<&mut WebSocketStream<TcpStream>, Message>,
        request: ClusterRequest,
        addr: &SocketAddr,
    ) -> Result<()> {
        match request.data {
            ClusterRequestData::Ping => {
                Self::handle_ping_request(ws_sender, &request, addr).await?;
            }
            ClusterRequestData::UpdateAddress(_) => {
                Self::handle_update_address_request(ws_sender, &request, addr).await?;
            }
        }
        Ok(())
    }

    async fn run(state: ClusterState, socket_addr: SocketAddr) -> Result<()> {
        let mut state = state;

        let listener = TcpListener::bind(socket_addr).await?;
        println!("Cluster listening on {}", listener.local_addr()?);

        loop {
            select! {
                Some(command) = state.command_rx.recv() => {
                    match command {
                        ClusterCommand::Ping(response) => {
                            response.send(Ok(())).unwrap();
                        }
                    }
                },
                Ok((stream, addr)) = listener.accept() => {
                    if let Err(e) = Self::handle_connection(&mut state, stream, addr).await {
                        eprintln!("Error handling connection: {}", e);
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    // Process WebSocket messages
                    let mut to_remove = Vec::new();
                    for (i, (ws_stream, addr)) in state.connections.iter_mut().enumerate() {
                        let (ws_sender, mut ws_receiver) = ws_stream.split();

                        match ws_receiver.next().now_or_never() {
                            Some(Some(Ok(msg))) => {
                                if msg.is_text() {
                                    if let Ok(request) =
                                        serde_json::from_str::<ClusterRequest>(msg.to_text()?)
                                    {
                                        if let Err(e) = Self::handle_request(ws_sender, request, addr).await {
                                            eprintln!("Error handling request: {}", e);
                                        }
                                    }
                                }
                            }
                            Some(Some(Err(_))) | Some(None) => {
                                // Connection closed or error occurred, mark for removal
                                to_remove.push(i);
                            }
                            None => {} // No message ready yet
                        }
                    }

                    // Remove closed connections
                    for &i in to_remove.iter().rev() {
                        state.connections.remove(i);
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_connection(
        state: &mut ClusterState,
        stream: tokio::net::TcpStream,
        addr: std::net::SocketAddr,
    ) -> Result<()> {
        let ws_stream = accept_async(stream).await?;
        state.connections.push((ws_stream, addr));
        Ok(())
    }
}
