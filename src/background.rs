use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, Receiver, Sender};

#[derive(Debug, Deserialize)]
pub struct Command {
    pub action: String,
    pub data: String,
}

#[derive(Debug, Serialize)]
pub struct CommandResponse {
    pub status: String,
    pub message: String,
}

pub fn spawn_background_process() -> Sender<Command> {
    let (command_tx, command_rx) = mpsc::channel::<Command>(32);
    
    // Spawn the background process
    tokio::spawn(run_background_process(command_rx));
    
    command_tx
}

async fn run_background_process(mut command_rx: Receiver<Command>) {
    println!("Background process started");
    while let Some(command) = command_rx.recv().await {
        println!("Received command: {:?}", command);
        // Handle commands here
        match command.action.as_str() {
            "ping" => println!("Pong! Data: {}", command.data),
            _ => println!("Unknown command: {}", command.action),
        }
    }
} 