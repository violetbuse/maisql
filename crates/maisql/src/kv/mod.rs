use byteorder::{ReadBytesExt, WriteBytesExt};
use core::str;
use futures::stream::{iter, SelectNextSome};
use std::{
    array,
    collections::HashMap,
    hash::Hash,
    io::{Cursor, Read, Write},
    iter,
};

use anyhow::anyhow;
use byteorder::BigEndian;
use tokio::sync::mpsc;

use crate::{
    config::Config,
    filesystem::{self, Storage},
    transport::Transport,
};

#[derive(Debug, Clone)]
pub struct KvConfig {
    sender: mpsc::Sender<KvClientReq>,
}

struct State {}

struct KvInstance {
    filename: String,
}

impl KvInstance {
    pub async fn create_new(filename: String, filesystem: &impl Storage) -> anyhow::Result<Self> {
        let free_pages_data: Vec<u8> = vec![0; 20 * 4096];
        let free_pages: [u32; 20] = array::from_fn(|i| i as u32 + 1);

        let header = Header {
            root_page: 0,
            free_pages,
        };

        let root_page = Page::RootPage { entries: None };

        header.write_header(filename.clone(), filesystem).await?;
        root_page
            .write_page(filename.clone(), filesystem, 0)
            .await?;

        filesystem
            .write(filename.clone(), 100 + 4096, free_pages_data)
            .await?;

        Ok(Self { filename })
    }
}

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
    RootPage {
        entries: Option<(u32, Vec<(String, u32, String)>)>,
    },
    BranchPage {
        parent_page: u32,
        first_child_pointer: u32,
        entries: Vec<(String, u32, String)>,
    },
    LeafPage {
        parent_page: u32,
        first_child_pointer: u32,
        entries: Vec<(String, String)>,
    },
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
        let parent_page = cursor.read_u32::<BigEndian>()?;

        let first_child_pointer = cursor.read_u32::<BigEndian>()?;

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
            0 => Page::RootPage {
                entries: Some((first_child_pointer, entries)),
            },
            1 => Page::RootPage { entries: None },
            2 => Page::BranchPage {
                parent_page,
                first_child_pointer,
                entries,
            },
            3 => Page::LeafPage {
                parent_page,
                first_child_pointer,
                entries: entries
                    .into_iter()
                    .map(|(key, _child, value)| (key, value))
                    .collect(),
            },
            invalid => return Err(anyhow!("invalid kv page type {invalid}")),
        };

        Ok(page)
    }
    pub async fn write_page(
        self,
        filename: String,
        storage: &impl Storage,
        page_id: u32,
    ) -> anyhow::Result<()> {
        let mut data: [u8; 4096] = [0; 4096];
        let mut cursor = Cursor::new(&mut data[..]);

        let page_type: u8 = match &self {
            Page::RootPage { entries: None } => 0,
            Page::RootPage { entries: Some(..) } => 1,
            Page::BranchPage { .. } => 2,
            Page::LeafPage { .. } => 3,
        };

        cursor.write_u8(page_type)?;

        match &self {
            Page::BranchPage { parent_page, .. } | Page::LeafPage { parent_page, .. } => {
                cursor.write_u32::<BigEndian>(parent_page.to_owned())?;
            }
            Page::RootPage { .. } => {}
        };

        let lt_pointer: u32 = match &self {
            Page::RootPage {
                entries: Some((lt_pointer, _)),
            }
            | Page::BranchPage {
                first_child_pointer: lt_pointer,
                ..
            }
            | Page::LeafPage {
                first_child_pointer: lt_pointer,
                ..
            } => lt_pointer.to_owned(),
            _ => 0,
        };

        cursor.write_u32::<BigEndian>(lt_pointer)?;

        let entries: Vec<(String, u32, String)> = match &self {
            Page::RootPage { entries: None } => Vec::new(),
            Page::RootPage {
                entries: Some((_, entries)),
            }
            | Page::BranchPage { entries, .. } => entries.to_owned(),
            Page::LeafPage { entries, .. } => entries
                .iter()
                .cloned()
                .map(|(key, value)| (key, 0, value))
                .collect(),
        };

        for (key, pointer, value) in entries {
            let key_bytes: [u8; 32] = key
                .bytes()
                .chain(iter::repeat(0))
                .take(32)
                .collect::<Vec<_>>()
                .try_into()
                .map_err(|_| anyhow!("could not truncate key into 32 bytes"))?;

            let value_bytes: [u8; 32] = value
                .bytes()
                .chain(iter::repeat(0))
                .take(66)
                .collect::<Vec<_>>()
                .try_into()
                .map_err(|_| anyhow!("could not truncate value into 0 bytes"))?;

            cursor.write(&key_bytes[..])?;
            cursor.write_u32::<BigEndian>(pointer)?;
            cursor.write(&value_bytes[..])?;
        }

        let offset: u32 = 100 + page_id * 4096;

        storage.write(filename, offset, data.to_vec()).await?;

        Ok(())
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
