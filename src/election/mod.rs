use std::net::SocketAddr;

pub trait ElectionClient {
    pub fn is_leader(&self) -> bool;
    pub fn get_leader(&self) -> SocketAddr;
    pub fn start_election(&self) -> Result<()>;
    pub fn stop_election(&self) -> Result<()>;
}
