mod cluster;
mod db;
mod election;
use anyhow::Result;
use clap::Parser;
use cluster::Cluster;
use etcd_client::Client as EtcdClient;
use std::sync::Arc;
use tokio::signal;

/// A simple SQL database server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The address to bind the server to
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    addr: String,

    /// The etcd server address
    #[arg(short, long, default_value = "http://127.0.0.1:2379")]
    etcd_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let etcd_client = Arc::new(EtcdClient::connect([&args.etcd_addr], None).await?);
    let _cluster = Cluster::new(args.addr.parse().unwrap(), etcd_client);

    _ = signal::ctrl_c().await;

    Ok(())
}
