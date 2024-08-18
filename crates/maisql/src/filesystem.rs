use async_trait::async_trait;

#[async_trait]
pub trait Storage: Send + Sync + Clone + Copy {
    async fn read(&self, filename: String, offset: u32, len: u32) -> anyhow::Result<&[u8]>;
    async fn write(&self, filename: String, offset: u32, data: Vec<u8>) -> anyhow::Result<()>;
    async fn sync(&self, filename: String) -> anyhow::Result<()>;
    async fn delete(&self, filename: String) -> anyhow::Result<()>;
}
