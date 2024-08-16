use anyhow::anyhow;

pub enum DatabaseMode {
    Journal,
    Wal,
}

pub struct DatabaseHeader {
    page_size: u16,
    read_mode: DatabaseMode,
    write_mode: DatabaseMode,
    page_count: u32,
    file_change_counter: u32,
}

impl TryFrom<&[u8]> for DatabaseHeader {
    type Error = anyhow::Error;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {}
}
