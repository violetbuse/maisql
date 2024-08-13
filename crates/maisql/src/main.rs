use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

mod cluster;
mod config;
mod locks;
pub mod manager;
mod raft;
mod semaphore;
pub mod transport;

fn main() {
    println!("Hello, world!");
}
