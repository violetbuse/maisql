use std::{collections::HashMap, io::Cursor};

use anyhow::anyhow;
use byteorder::{BigEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    modified_pages: HashMap<u32, Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionSet {
    salts: String,
    transactions: Vec<Transaction>,
}

pub fn parse_wal(bytes: &[u8]) -> anyhow::Result<TransactionSet> {
    let mut cursor = Cursor::new(bytes);

    let header: WalHeader = {
        let magic_nr = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
        let endianness = match magic_nr {
            0x377f0682 => Endianness::LittleEndian,
            0x377f0683 => Endianness::BigEndian,
            _ => return Err(anyhow!("Invalid wal-header magic nr.")),
        };

        let file_format_version = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
        if file_format_version != 3007000 {
            return Err(anyhow!(
                "Invalid wal-header file version. Only 3007000 accepted."
            ));
        }

        let page_size = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
        let checkpoint_sequence_no = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;

        let salt_1 = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
        let salt_2 = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
        let checksum_1 = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
        let checksum_2 = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;

        let checksummed_header_bytes = &bytes[0..24];
        let (valid_s1, valid_s2) = checksum(endianness, checksummed_header_bytes, 0, 0);

        if checksum_1 != valid_s1 || checksum_2 != valid_s2 {
            return Err(anyhow!("Invalid wal-header checksum"));
        }

        WalHeader {
            endianness,
            page_size,
            checkpoint_sequence_no,
            salt_1,
            salt_2,
            checksum_1,
            checksum_2,
        }
    };

    let wal_frames: Vec<(u32, WalFrameHeader, Page)> = {
        let mut current_frame = 0;
        let mut prev_frames: Vec<(u32, WalFrameHeader, Page)> = Vec::new();

        loop {
            if cursor.position() + 1 == bytes.len() {
                break;
            }

            let offset = 32 + current_frame * (24 + header.page_size);

            let page_no = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;

            let commit_record_data = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
            let is_commit_rec = match commit_record_data {
                0 => None,
                new_size => Some(new_size),
            };

            let salt_1 = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
            let salt_2 = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;

            let checksum_1 = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
            let checksum_2 = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;

            let (prev_check_1, prev_check_2) = match prev_frames {
                [.., (_, header, _)] => (header.checksum_1, header.checksum_2),
                [] => (header.checksum_1, header.checksum_2),
            };

            let checksummed_header_bytes = &bytes[offset..(offset + 8)];
            let page_bytes = &bytes[(offset + 24)..(offset + 24 + header.page_size)];

            let (s1, s2) = checksum(
                header.endianness,
                checksummed_header_bytes,
                prev_check_1,
                prev_check_2,
            );
            let (valid_s1, valid_s2) = checksum(header.endianness, bytes, s1, s2);

            if valid_s1 != checksum_1 || valid_s2 != checksum_2 {
                break;
            }

            if salt_1 != header.salt_1 || salt_2 != header.salt_2 {
                break;
            }

            let frame_header = WalFrameHeader {
                page_no,
                is_commit_rec,
                salt_1,
                salt_2,
                checksum_1,
                checksum_2,
            };

            prev_frames.push((current_frame, frame_header, page_bytes.to_owned()));
            current_frame += 1;
        }

        prev_frames
    };

    let mut transactions = vec![Transaction {
        modified_pages: HashMap::new(),
    }];

    while let Some((frame_id, frame_header, page)) = wal_frames.iter().next() {
        transactions
            .last_mut()
            .unwrap()
            .modified_pages
            .insert(frame_header.page_no, page);

        if frame_header.is_commit_rec.is_some() {
            transactions.push(Transaction {
                modified_pages: HashMap::new(),
            });
        }
    }

    let salts = format!("{:#x}/{:#x}", header.salt_1, header.salt_2);

    return Ok(TransactionSet {
        salts,
        transactions,
    });
}

fn checksum(endianness: Endianness, bytes: &[u8], s0: u32, s1: u32) -> (u32, u32) {
    let mut s0 = s0;
    let mut s1 = s1;

    let cursor = Cursor::new(bytes);

    for i in (0..(bytes.len() - 1)).step_by(2) {
        s0 += bytes[i] + s1;
        s1 += bytes[i + 1] + s0;
    }

    (s0, s1)
}

enum Endianness {
    BigEndian,
    LittleEndian,
}

struct WalHeader {
    endianness: Endianness,
    page_size: u32,
    checkpoint_sequence_no: u32,
    salt_1: u32,
    salt_2: u32,
    checksum_1: u32,
    checksum_2: u32,
}

struct WalFrameHeader {
    page_no: u32,
    /// If this is a commit record, Some(u32) otherwise None
    is_commit_rec: Option<u32>,
    salt_1: u32,
    salt_2: u32,
    checksum_1: u32,
    checksum_2: u32,
}

type Page = Vec<u8>;
