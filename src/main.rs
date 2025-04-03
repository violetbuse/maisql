mod background;

use axum::{
    routing::{get, post},
    Router,
    response::IntoResponse,
    extract::{WebSocketUpgrade, State},
    Json,
};
use tower_http::cors::CorsLayer;
use tokio::sync::mpsc::Sender;
use std::sync::Arc;

use background::{Command, CommandResponse};

// Shared state
#[derive(Clone)]
struct AppState {
    command_tx: Arc<Sender<Command>>,
}

// Create a function that returns our app router
pub fn create_app(command_tx: Sender<Command>) -> Router {
    let state = AppState {
        command_tx: Arc::new(command_tx),
    };

    Router::new()
        .route("/", get(hello_handler))
        .route("/ws", get(ws_handler))
        .route("/command", post(command_handler))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

#[tokio::main]
async fn main() {
    let command_tx = background::spawn_background_process();
    let app = create_app(command_tx);

    // Run the server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Server running on http://127.0.0.1:3000");
    
    axum::serve(listener, app).await.unwrap();
}

// Basic HTTP handler
async fn hello_handler() -> &'static str {
    "Hello, World!"
}

// Command handler
async fn command_handler(
    State(state): State<AppState>,
    Json(command): Json<Command>,
) -> impl IntoResponse {
    match state.command_tx.send(command).await {
        Ok(_) => Json(CommandResponse {
            status: "success".to_string(),
            message: "Command sent successfully".to_string(),
        }),
        Err(_) => Json(CommandResponse {
            status: "error".to_string(),
            message: "Failed to send command".to_string(),
        }),
    }
}

// WebSocket handler
async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

// Handle WebSocket connection
async fn handle_socket(mut socket: axum::extract::ws::WebSocket) {
    while let Some(msg) = socket.recv().await {
        if let Ok(msg) = msg {
            match msg {
                axum::extract::ws::Message::Text(text) => {
                    // Echo the message back
                    if socket
                        .send(axum::extract::ws::Message::Text(format!("You said: {}", text)))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                axum::extract::ws::Message::Close(_) => break,
                _ => {}
            }
        } else {
            break;
        }
    }
}
