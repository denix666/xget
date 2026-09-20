mod bencode;
mod metainfo;
mod peer;
mod tracker;

use anyhow::{Result, bail};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub async fn download(torrent_path: &Path, output_dir: &Path, max_peers: usize) -> Result<()> {
    let data = tokio::fs::read(torrent_path).await?;
    let torrent = metainfo::parse(&data)?;

    println!("  name: {}", torrent.name);
    println!(
        "  size: {} ({} pieces)",
        format_size(torrent.total_size),
        torrent.pieces.len()
    );

    let base = output_dir.join(&torrent.name);
    prepare_files(&torrent, &base).await?;

    let peer_id = generate_peer_id();
    let manager = Arc::new(Mutex::new(PieceManager::new(&torrent)));
    let writer = Arc::new(FileWriter::new(&torrent, &base));

    let num_pieces = torrent.pieces.len();
    let mut verified = 0u64;

    print!("  checking existing data...");
    use std::io::Write;
    std::io::stdout().flush().ok();

    for idx in 0..num_pieces {
        let (piece_len, expected) = {
            let mgr = manager.lock().unwrap();
            (mgr.piece_size(idx as u32) as usize, mgr.piece_hash(idx as u32))
        };
        if let Ok(data) = writer.read_piece(idx as u32, piece_len).await
            && data.len() == piece_len
            && verify_sha1(&data, &expected)
        {
            manager.lock().unwrap().piece_done(idx as u32);
            verified += piece_len as u64;
        }
    }

    if verified > 0 {
        let done = manager.lock().unwrap().completed_count();
        println!(" {}/{} pieces ({}) verified", done, num_pieces, format_size(verified));
    } else {
        println!(" fresh download");
    }

    if manager.lock().unwrap().is_complete() {
        println!("  already complete");
        return Ok(());
    }

    let peers = tracker::get_peers(&torrent, &peer_id).await?;
    println!("  peers: {}", peers.len());

    let pb = crate::progress::create(Some(torrent.total_size));
    pb.inc(verified);

    if peers.is_empty() {
        bail!("no peers found");
    }

    let concurrent = max_peers.max(30).min(peers.len());
    let mut next_peer = 0usize;
    let mut set = tokio::task::JoinSet::new();

    for &addr in peers.iter().take(concurrent) {
        let ih = torrent.info_hash;
        let pid = peer_id;
        let mgr = manager.clone();
        let wr = writer.clone();
        let p = pb.clone();
        set.spawn(async move { peer::run(addr, ih, pid, mgr, wr, p).await });
        next_peer += 1;
    }

    while let Some(_result) = set.join_next().await {
        if manager.lock().unwrap().is_complete() {
            set.abort_all();
            break;
        }
        while set.len() < concurrent && next_peer < peers.len() {
            let addr = peers[next_peer];
            next_peer += 1;
            let ih = torrent.info_hash;
            let pid = peer_id;
            let mgr = manager.clone();
            let wr = writer.clone();
            let p = pb.clone();
            set.spawn(async move { peer::run(addr, ih, pid, mgr, wr, p).await });
        }
    }

    if manager.lock().unwrap().is_complete() {
        pb.finish_with_message("done");
        Ok(())
    } else {
        let done = manager.lock().unwrap().completed_count();
        let total = manager.lock().unwrap().num_pieces();
        pb.abandon_with_message("incomplete");
        bail!("download incomplete: {done}/{total} pieces")
    }
}

// --- PieceManager ---

#[derive(Clone, Copy, PartialEq)]
enum PieceState {
    Needed,
    Downloading,
    Done,
}

pub struct PieceManager {
    states: Vec<PieceState>,
    hashes: Vec<[u8; 20]>,
    piece_length: u64,
    total_size: u64,
}

impl PieceManager {
    fn new(torrent: &metainfo::Torrent) -> Self {
        Self {
            states: vec![PieceState::Needed; torrent.pieces.len()],
            hashes: torrent.pieces.clone(),
            piece_length: torrent.piece_length,
            total_size: torrent.total_size,
        }
    }

    pub fn num_pieces(&self) -> usize {
        self.states.len()
    }

    pub fn next_piece(&mut self, peer_has: &[bool]) -> Option<u32> {
        for (i, state) in self.states.iter_mut().enumerate() {
            if *state == PieceState::Needed && peer_has.get(i).copied().unwrap_or(false) {
                *state = PieceState::Downloading;
                return Some(i as u32);
            }
        }
        None
    }

    pub fn piece_size(&self, index: u32) -> u64 {
        let last = self.states.len() as u32 - 1;
        if index == last {
            let rem = self.total_size % self.piece_length;
            if rem == 0 {
                self.piece_length
            } else {
                rem
            }
        } else {
            self.piece_length
        }
    }

    pub fn piece_hash(&self, index: u32) -> [u8; 20] {
        self.hashes[index as usize]
    }

    pub fn piece_done(&mut self, index: u32) {
        self.states[index as usize] = PieceState::Done;
    }

    pub fn piece_failed(&mut self, index: u32) {
        self.states[index as usize] = PieceState::Needed;
    }

    pub fn is_complete(&self) -> bool {
        self.states.iter().all(|s| *s == PieceState::Done)
    }

    pub fn completed_count(&self) -> usize {
        self.states.iter().filter(|s| **s == PieceState::Done).count()
    }
}

// --- FileWriter ---

pub struct FileWriter {
    files: Vec<(std::path::PathBuf, u64)>,
    piece_length: u64,
}

impl FileWriter {
    fn new(torrent: &metainfo::Torrent, base: &Path) -> Self {
        let files = if torrent.files.len() == 1 {
            vec![(base.to_path_buf(), torrent.files[0].length)]
        } else {
            torrent
                .files
                .iter()
                .map(|f| (base.join(&f.path), f.length))
                .collect()
        };
        Self {
            files,
            piece_length: torrent.piece_length,
        }
    }

    pub async fn read_piece(&self, index: u32, piece_size: usize) -> Result<Vec<u8>> {
        let mut global_pos = index as u64 * self.piece_length;
        let mut result = Vec::with_capacity(piece_size);
        let mut remaining = piece_size;
        let mut file_start = 0u64;

        for (path, file_len) in &self.files {
            let file_end = file_start + file_len;
            if global_pos >= file_end {
                file_start = file_end;
                continue;
            }
            if remaining == 0 {
                break;
            }

            let offset = global_pos - file_start;
            let available = (*file_len - offset) as usize;
            let to_read = remaining.min(available);

            let mut f = tokio::fs::File::open(path).await?;
            f.seek(std::io::SeekFrom::Start(offset)).await?;
            let mut buf = vec![0u8; to_read];
            f.read_exact(&mut buf).await?;
            result.extend_from_slice(&buf);

            remaining -= to_read;
            global_pos += to_read as u64;
            file_start = file_end;
        }

        Ok(result)
    }

    pub async fn write_piece(&self, index: u32, data: &[u8]) -> Result<()> {
        let mut global_pos = index as u64 * self.piece_length;
        let mut remaining = data;
        let mut file_start = 0u64;

        for (path, file_len) in &self.files {
            let file_end = file_start + file_len;
            if global_pos >= file_end {
                file_start = file_end;
                continue;
            }
            if remaining.is_empty() {
                break;
            }

            let offset = global_pos - file_start;
            let available = (*file_len - offset) as usize;
            let to_write = remaining.len().min(available);

            let mut f = tokio::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .await?;
            f.seek(std::io::SeekFrom::Start(offset)).await?;
            f.write_all(&remaining[..to_write]).await?;

            remaining = &remaining[to_write..];
            global_pos += to_write as u64;
            file_start = file_end;
        }

        Ok(())
    }
}

// --- Helpers ---

async fn prepare_files(torrent: &metainfo::Torrent, base: &Path) -> Result<()> {
    if torrent.files.len() == 1 {
        if let Some(parent) = base.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let f = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(base)
            .await?;
        f.set_len(torrent.total_size).await?;
    } else {
        for fi in &torrent.files {
            let path = base.join(&fi.path);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let f = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .await?;
            f.set_len(fi.length).await?;
        }
    }
    Ok(())
}

fn verify_sha1(data: &[u8], expected: &[u8; 20]) -> bool {
    use sha1::{Digest, Sha1};
    let hash = Sha1::digest(data);
    hash.as_slice() == expected
}

fn generate_peer_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(b"-XG0001-");
    rand::fill(&mut id[8..]);
    id
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}
