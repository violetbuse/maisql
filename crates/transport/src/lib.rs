use std::net::SocketAddr;

use async_trait::async_trait;

#[async_trait]
pub trait Transport: Send + Sync + Clone + Copy {
    async fn recv(&self) -> Option<(&[u8], SocketAddr)>;
    async fn send(&self, addr: SocketAddr, value: &Vec<u8>) -> anyhow::Result<()>;
    fn addr(&self) -> SocketAddr;
}
