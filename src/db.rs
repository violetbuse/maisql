use rusqlite::{Connection, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Block {
    pub id: String,
    pub previous_id: Option<String>,
    pub command: String,
    pub created_at: i64,
}

#[derive(Clone)]
pub struct DatabaseManager {
    connections: Arc<Mutex<HashMap<String, Connection>>>,
}

impl DatabaseManager {
    pub fn new() -> Self {
        DatabaseManager {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn add_database(&self, name: &str, path: PathBuf) -> Result<()> {
        let conn = Connection::open(path)?;
        
        // Initialize blockchain table for new connection
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blockchain (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT UNIQUE NOT NULL,
                previous_id TEXT,
                command TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY (previous_id) REFERENCES blockchain(id)
            )",
            [],
        )?;

        let mut connections = self.connections.lock().await;
        connections.insert(name.to_string(), conn);
        Ok(())
    }

    pub async fn apply_blocks(&self, db_name: &str, blocks: &[Block]) -> Result<()> {
        let mut connections = self.connections.lock().await;
        if let Some(conn) = connections.get_mut(db_name) {
            let tx = conn.transaction()?;
            
            for block in blocks {
                // First execute the SQL command from the block
                tx.execute_batch(&block.command)?;
                
                // Then record the block in the blockchain table
                tx.execute(
                    "INSERT INTO blockchain (id, previous_id, command, created_at) 
                     VALUES (?1, ?2, ?3, ?4)",
                    (
                        &block.id,
                        &block.previous_id,
                        &block.command,
                        block.created_at,
                    ),
                )?;
            }
            
            tx.commit()?;
        }
        Ok(())
    }

    // Helper method to initialize a new database with schema
    pub async fn initialize_database(&self, name: &str) -> Result<()> {
        let mut connections = self.connections.lock().await;
        if let Some(conn) = connections.get_mut(name) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    quantity INTEGER NOT NULL
                )",
                [],
            )?;
        }
        Ok(())
    }
} 