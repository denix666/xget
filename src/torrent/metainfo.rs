use anyhow::{Context, Result, bail};
use std::collections::BTreeMap;
use std::path::PathBuf;

use super::bencode::{self, Value};

pub struct Torrent {
    pub announce: String,
    pub announce_list: Vec<Vec<String>>,
    pub info_hash: [u8; 20],
    pub name: String,
    pub piece_length: u64,
    pub pieces: Vec<[u8; 20]>,
    pub files: Vec<FileInfo>,
    pub total_size: u64,
}

pub struct FileInfo {
    pub path: PathBuf,
    pub length: u64,
}

pub fn parse(data: &[u8]) -> Result<Torrent> {
    let value = bencode::decode(data)?;
    let dict = value.as_dict().context("torrent file is not a dictionary")?;

    let announce = dict
        .get("announce")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let announce_list = parse_announce_list(dict);

    let info_raw = bencode::find_dict_value_raw(data, "info")?;
    let info_hash = sha1_hash(info_raw);

    let info = dict
        .get("info")
        .and_then(|v| v.as_dict())
        .context("missing 'info' dictionary")?;

    let name = info
        .get("name")
        .and_then(|v| v.as_str())
        .context("missing 'name' in info")?
        .to_string();

    let piece_length = info
        .get("piece length")
        .and_then(|v| v.as_int())
        .context("missing 'piece length'")? as u64;

    let pieces_raw = info
        .get("pieces")
        .and_then(|v| v.as_bytes())
        .context("missing 'pieces'")?;

    if pieces_raw.len() % 20 != 0 {
        bail!(
            "'pieces' length {} is not a multiple of 20",
            pieces_raw.len()
        );
    }

    let pieces: Vec<[u8; 20]> = pieces_raw
        .as_chunks::<20>()
        .0
        .to_vec();

    let (files, total_size) = if let Some(length) = info.get("length").and_then(|v| v.as_int()) {
        let fi = FileInfo {
            path: PathBuf::from(&name),
            length: length as u64,
        };
        (vec![fi], length as u64)
    } else if let Some(file_list) = info.get("files").and_then(|v| v.as_list()) {
        let mut files = Vec::new();
        let mut total = 0u64;
        for f in file_list {
            let fd = f.as_dict().context("file entry is not a dictionary")?;
            let length = fd
                .get("length")
                .and_then(|v| v.as_int())
                .context("missing file length")? as u64;
            let path_parts: Vec<&str> = fd
                .get("path")
                .and_then(|v| v.as_list())
                .context("missing file path")?
                .iter()
                .filter_map(|p| p.as_str())
                .collect();
            let mut path = PathBuf::new();
            for part in &path_parts {
                path.push(part);
            }
            files.push(FileInfo { path, length });
            total += length;
        }
        (files, total)
    } else {
        bail!("torrent has neither 'length' nor 'files' in info");
    };

    Ok(Torrent {
        announce,
        announce_list,
        info_hash,
        name,
        piece_length,
        pieces,
        files,
        total_size,
    })
}

fn parse_announce_list(dict: &BTreeMap<String, Value>) -> Vec<Vec<String>> {
    dict.get("announce-list")
        .and_then(|v| v.as_list())
        .map(|tiers| {
            tiers
                .iter()
                .filter_map(|tier| {
                    tier.as_list().map(|urls| {
                        urls.iter()
                            .filter_map(|u| u.as_str().map(String::from))
                            .collect()
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn sha1_hash(data: &[u8]) -> [u8; 20] {
    use sha1::{Digest, Sha1};
    let result = Sha1::digest(data);
    let mut hash = [0u8; 20];
    hash.copy_from_slice(&result);
    hash
}
