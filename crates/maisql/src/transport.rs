use crate::state::NodeId;
use std::time::Duration;

use async_trait::async_trait;

#[async_trait]
pub trait ResponseHandle: Send + Sync {
    async fn send_response(self, value: &Vec<u8>) -> anyhow::Result<()>;
}

pub trait RequestHandle: Send + Sync + Clone + Copy {
    fn data(&self) -> (&[u8], NodeId);
    fn accept(&self) -> anyhow::Result<&impl ResponseHandle>;
}

#[async_trait]
pub trait Transport: Send + Sync + Clone + Copy {
    async fn recv(&self) -> Option<(&[u8], NodeId)>;
    async fn recv_request(&self) -> Option<&impl RequestHandle>;
    async fn send_unreliable(&self, addr: NodeId, value: &Vec<u8>) -> anyhow::Result<()>;
    async fn send_reliable(&self, addr: NodeId, value: &Vec<u8>) -> anyhow::Result<()>;
    async fn send_request(
        &self,
        addr: NodeId,
        value: &Vec<u8>,
        response_timeout: Duration,
    ) -> anyhow::Result<&[u8]>;
    fn addr(&self) -> NodeId;
}
