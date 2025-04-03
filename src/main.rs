mod cluster;
mod db;
use anyhow::Result;
use cluster::Cluster;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:8080";
    let _cluster = Cluster::new(addr.parse().unwrap());

    _ = signal::ctrl_c().await;

    Ok(())
}
