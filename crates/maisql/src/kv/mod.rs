use byteorder::{ReadBytesExt, WriteBytesExt};
use core::str;
use std::io::{Cursor, Read, Write};

use anyhow::anyhow;
use byteorder::BigEndian;
use tokio::sync::mpsc;

use crate::{config::Config, filesystem::Storage, transport::Transport};

#[derive(Debug, Clone)]
pub struct KvConfig {
    sender: mpsc::Sender<KvClientReq>,
}

struct State {}

struct Header {
    root_page: u32,
    free_pages: [u32; 20],
}

impl Header {
    pub async fn read_header(filename: String, storage: &impl Storage) -> anyhow::Result<Self> {
        let header_bytes = storage.read(filename, 0, 100).await?;
        let mut cursor = Cursor::new(header_bytes);

        let mut magic_bytes = [0; 4];
        cursor.read_exact(&mut magic_bytes)?;
        let magic_string = str::from_utf8(&magic_bytes[..])?;

        if magic_string != "kv/1" {
            return Err(anyhow!(
                "Unsupported kv file header magic string: {}",
                magic_string
            ));
        }

        let root_page_pointer = cursor.read_u32::<BigEndian>()?;
        let mut free_page_list = [0; 20];
        cursor.read_u32_into::<BigEndian>(&mut free_page_list);

        Ok(Header {
            root_page: root_page_pointer,
            free_pages: free_page_list,
        })
    }
    pub async fn write_header(
        self,
        filename: String,
        storage: &impl Storage,
    ) -> anyhow::Result<()> {
        let mut header_bytes: [u8; 100] = [0; 100];
        let mut cursor = Cursor::new(&mut header_bytes[..]);

        cursor.write_all("kv/1".as_bytes())?;
        cursor.write_u32::<BigEndian>(self.root_page)?;

        for free_page in self.free_pages {
            cursor.write_u32::<BigEndian>(free_page)?;
        }

        storage.write(filename, 0, header_bytes.to_vec());

        Ok(())
    }
}

enum Page {
    RootPage {},
    BranchPage { parent_page: u32 },
    LeafPage { parent_page: u32 },
}

impl Page {
    pub async fn read_page(
        filename: String,
        page: u32,
        storage: &impl Storage,
    ) -> anyhow::Result<Self> {
        let page_size: u32 = 2 ^ 12;
        let offset = 100 + page * page_size;
        let bytes = storage.read(filename, offset, page_size).await?;
        let mut cursor = Cursor::new(bytes);

        let page_type = cursor.read_u8()?;
        let parent_cursor = cursor.read_u32::<BigEndian>();

        let lt_child_pointer = cursor.read_u32::<BigEndian>();

        let mut entries: Vec<(String, u32, String)> = Vec::new();

        for _ in 0..40 {
            let mut key_bytes = [0; 32];
            cursor.read_exact(&mut key_bytes)?;

            if key_bytes.iter().all(|byte| *byte == 0x00) {
                break;
            }

            let key_null_terminated: Vec<u8> = key_bytes
                .into_iter()
                .take_while(|byte| *byte == 0x00)
                .collect::<Vec<_>>();

            let key = str::from_utf8(key_null_terminated.as_slice())?;

            let child_pointer = cursor.read_u32::<BigEndian>()?;

            let mut value_bytes = [0; 66];
            cursor.read_exact(&mut value_bytes)?;

            if value_bytes.iter().all(|byte| *byte == 0x00) {
                break;
            }

            let value_null_terminated: Vec<u8> = value_bytes
                .into_iter()
                .take_while(|byte| *byte == 0x00)
                .collect::<Vec<_>>();

            let value = str::from_utf8(value_null_terminated.as_slice())?;

            entries.push((key.to_owned(), child_pointer, value.to_owned()))
        }

        let page = match page_type {
            0 => Page::RootPage {},
            1 => Page::BranchPage {},
            2 => Page::LeafPage {},
            invalid => return Err(anyhow!("invalid kv page type {invalid}")),
        };

        Ok(page)
    }
}

pub async fn run_kv(
    transport: &impl Transport,
    filesystem: &impl Storage,
    config: Config,
    client_rx: mpsc::Receiver<KvClientReq>,
) -> anyhow::Result<()> {
}

pub enum KvClientReq {}

#[derive(Debug, Clone)]
pub struct KvClient {
    sender: mpsc::Sender<KvClientReq>,
    config: Config,
}

impl From<Config> for KvClient {
    fn from(value: Config) -> Self {
        Self {
            sender: value.clone().kv.sender,
            config: value,
        }
    }
}
