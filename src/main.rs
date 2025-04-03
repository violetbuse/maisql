mod db;
use db::{Block, DatabaseManager};
use std::path::PathBuf;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let db_manager = DatabaseManager::new();
    
    // Initialize the main database
    db_manager.add_database("main", PathBuf::from("database.db")).await?;
    
    let blocks = vec![
        Block {
            id: "block1".to_string(),
            previous_id: None,
            command: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)".to_string(),
            created_at: chrono::Utc::now().timestamp(),
        },
        Block {
            id: "block2".to_string(),
            previous_id: Some("block1".to_string()),
            command: "INSERT INTO users (name) VALUES ('Alice'), ('Bob')".to_string(),
            created_at: chrono::Utc::now().timestamp(),
        },
    ];
    
    db_manager.apply_blocks("main", &blocks).await?;
    println!("Blocks applied successfully!");
    
    // Add additional databases
    db_manager.add_database("inventory", PathBuf::from("inventory.db")).await?;
    db_manager.initialize_database("inventory").await?;

    db_manager.add_database("backup", PathBuf::from("backup.db")).await?;
    db_manager.initialize_database("backup").await?;
    
    Ok(())
}
