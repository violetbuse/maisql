use crate::state::NodeId;
use std::{env, time::Duration};

use anyhow::anyhow;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait ResponseHandle: Send + Sync {
    async fn send_response(self, value: &[u8]) -> anyhow::Result<()>;
}

pub trait RequestHandle: Send + Sync + Clone + Copy {
    fn data(&self) -> (&[u8], NodeId);
    /// Returns `Ok(response_handle)` if no other function has accepted yet.
    fn accept(&self) -> anyhow::Result<&impl ResponseHandle>;
}

#[async_trait]
pub trait Transport: Send + Sync + Clone + Copy {
    async fn recv(&self) -> Option<(&[u8], NodeId)>;
    async fn recv_request(&self) -> Option<&impl RequestHandle>;
    async fn send_unreliable(&self, addr: NodeId, value: &[u8]) -> anyhow::Result<()>;
    async fn send_reliable(&self, addr: NodeId, value: &[u8]) -> anyhow::Result<()>;
    async fn send_request(
        &self,
        addr: NodeId,
        value: &[u8],
        response_timeout: Duration,
    ) -> anyhow::Result<&[u8]>;
    fn addr(&self) -> NodeId;
}

pub trait TransportData: Serialize + for<'a> Deserialize<'a> {
    /// Serialize based on defaults:
    ///
    /// if $DEBUG = true or 0 use json, otherwise use $MAISQL_SERIALIZE_FMT
    /// where valid values are "json" and "msgpack". if no valid values, fallback
    /// to msgpack.
    fn serialize(&self) -> Vec<u8> {
        let debug = env::var("DEBUG")
            .map(|debug_val| debug_val.as_str() == "true" || debug_val.as_str() == "1")
            .unwrap_or(false);

        let serialize_format = env::var("MAISQL_SERIALIZE_FMT").unwrap_or("".to_string());

        match (debug, serialize_format.as_str()) {
            (true, _) => self.serialize_json(),
            (false, "msgpack") => self.serialize_msgpack(),
            (false, "json") => self.serialize_json(),
            (false, _) => self.serialize_msgpack(),
        }
    }
    fn serialize_json(&self) -> Vec<u8> {
        return serde_json::to_vec_pretty(self).unwrap();
    }
    fn serialize_msgpack(&self) -> Vec<u8> {
        return rmp_serde::to_vec_named(self).unwrap();
    }
    fn parse(data: &[u8], struct_name: &str) -> anyhow::Result<Self> {
        if let Ok(data) = rmp_serde::from_slice(data) {
            return Ok(data);
        } else if let Ok(data) = serde_json::from_slice(data) {
            return Ok(data);
        } else {
            return Err(anyhow!("Could not deserialize {} from slice", struct_name));
        }
    }
}
