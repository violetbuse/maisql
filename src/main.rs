use axum::{
    routing::{get, post},
    Router,
    response::IntoResponse,
    extract::WebSocketUpgrade,
};
use tower_http::cors::CorsLayer;

#[tokio::main]
async fn main() {
    // Create a new router
    let app = Router::new()
        .route("/", get(hello_handler))
        .route("/ws", get(ws_handler))
        .layer(CorsLayer::permissive());

    // Run the server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Server running on http://127.0.0.1:3000");
    
    axum::serve(listener, app).await.unwrap();
}

// Basic HTTP handler
async fn hello_handler() -> &'static str {
    "Hello, World!"
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
