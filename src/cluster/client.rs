use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

use super::{ClusterRequest, ClusterRequestData, ClusterResponse, ClusterResponseData};

pub struct Client {
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    local_addr: SocketAddr,
    next_request_id: u64,
}

impl Client {
    pub async fn connect(server_addr: SocketAddr) -> Result<Self> {
        let url = format!("ws://{}", server_addr);
        let (ws_stream, _) = connect_async(url).await?;

        let local_addr = server_addr; // Use server address as local for now

        Ok(Self {
            ws_stream,
            local_addr,
            next_request_id: 1,
        })
    }

    pub async fn ping(&mut self) -> Result<()> {
        let request = ClusterRequest {
            id: self.next_request_id,
            from: self.local_addr,
            data: ClusterRequestData::Ping,
        };

        self.next_request_id += 1;

        let request_text = serde_json::to_string(&request)?;
        self.ws_stream.send(Message::Text(request_text)).await?;

        // Wait for response
        while let Some(msg) = self.ws_stream.next().await {
            match msg? {
                Message::Text(text) => {
                    if let Ok(response) = serde_json::from_str::<ClusterResponse>(&text) {
                        if response.id == request.id {
                            match response.data {
                                ClusterResponseData::Pong => return Ok(()),
                                ClusterResponseData::AddressUpdated => continue,
                            }
                        }
                    }
                }
                Message::Close(_) => {
                    return Err(anyhow::anyhow!("Connection closed"));
                }
                _ => continue,
            }
        }

        Err(anyhow::anyhow!("No response received"))
    }

    pub async fn update_address(&mut self, new_addr: SocketAddr) -> Result<()> {
        let request = ClusterRequest {
            id: self.next_request_id,
            from: self.local_addr,
            data: ClusterRequestData::UpdateAddress(new_addr),
        };

        self.next_request_id += 1;

        let request_text = serde_json::to_string(&request)?;
        self.ws_stream.send(Message::Text(request_text)).await?;

        // Wait for response
        while let Some(msg) = self.ws_stream.next().await {
            match msg? {
                Message::Text(text) => {
                    if let Ok(response) = serde_json::from_str::<ClusterResponse>(&text) {
                        if response.id == request.id {
                            match response.data {
                                ClusterResponseData::AddressUpdated => {
                                    self.local_addr = new_addr;
                                    return Ok(());
                                }
                                _ => continue,
                            }
                        }
                    }
                }
                Message::Close(_) => {
                    return Err(anyhow::anyhow!("Connection closed"));
                }
                _ => continue,
            }
        }

        Err(anyhow::anyhow!("No response received"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    async fn setup_test_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws_stream = accept_async(stream).await.unwrap();

            while let Some(msg) = ws_stream.next().await {
                match msg.unwrap() {
                    Message::Text(text) => {
                        if let Ok(request) = serde_json::from_str::<ClusterRequest>(&text) {
                            let response = ClusterResponse {
                                id: request.id,
                                from: request.from,
                                data: ClusterResponseData::Pong,
                            };
                            let response_text = serde_json::to_string(&response).unwrap();
                            ws_stream.send(Message::Text(response_text)).await.unwrap();
                        }
                    }
                    _ => break,
                }
            }
        });

        addr
    }

    #[tokio::test]
    async fn test_ping() {
        let server_addr = setup_test_server().await;
        let mut client = Client::connect(server_addr).await.unwrap();
        assert!(client.ping().await.is_ok());
    }
}
