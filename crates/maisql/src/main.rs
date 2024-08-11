use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

mod cluster;
mod config;
pub mod manager;
mod raft_leader;
mod semaphore;
pub mod transport;

fn main() {
    println!("Hello, world!");
}
