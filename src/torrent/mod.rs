mod bencode;
mod dht;
pub mod magnet;
mod metainfo;
mod peer;
mod tracker;

use anyhow::{Context, Result, bail};
use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::log::vlog;

pub struct SpeedTracker {
    total_bytes: AtomicU64,
    start: std::time::Instant,
}

impl SpeedTracker {
    fn new() -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            start: std::time::Instant::now(),
        }
    }

    pub fn add_bytes(&self, n: u64) {
        self.total_bytes.fetch_add(n, Ordering::Relaxed);
    }

    pub fn average_speed(&self) -> f64 {
        let elapsed = self.start.elapsed().as_secs_f64();
        if elapsed < 1.0 {
            return 0.0;
        }
        self.total_bytes.load(Ordering::Relaxed) as f64 / elapsed
    }
}

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

    vlog!("checking existing data ({num_pieces} pieces)...");

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
        vlog!("{done}/{num_pieces} pieces ({}) verified", format_size(verified));
    }

    if manager.lock().unwrap().is_complete() {
        println!("  already complete");
        return Ok(());
    }

    let pb = crate::progress::create(Some(torrent.total_size));
    pb.inc(verified);

    let (tx, rx) = tokio::sync::mpsc::channel(200);
    let trackers = tracker::collect_trackers(&torrent);
    let ih = torrent.info_hash;
    let pid = peer_id;
    let ts = torrent.total_size;
    let discover_handle = tokio::spawn(async move {
        tracker::discover_peers(trackers, ih, pid, ts, tx).await;
    });

    run_download_stream(rx, torrent.info_hash, peer_id, &manager, &writer, &pb, max_peers).await;
    discover_handle.abort();

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

async fn run_download_stream(
    mut rx: tokio::sync::mpsc::Receiver<SocketAddr>,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    manager: &Arc<Mutex<PieceManager>>,
    writer: &Arc<FileWriter>,
    pb: &indicatif::ProgressBar,
    max_peers: usize,
) {
    let max_concurrent = max_peers.max(30);
    let speed = Arc::new(SpeedTracker::new());
    let pex_peers: Arc<Mutex<VecDeque<SocketAddr>>> = Arc::new(Mutex::new(VecDeque::new()));
    let mut seen = HashSet::new();
    let mut pending = VecDeque::new();
    let mut set = tokio::task::JoinSet::new();
    let mut channel_open = true;

    let mut ok_count = 0u32;
    let mut connect_fail = 0u32;
    let mut no_unchoke = 0u32;
    let mut no_pieces = 0u32;
    let mut other_err = 0u32;

    loop {
        if manager.lock().unwrap().is_complete() {
            set.abort_all();
            break;
        }

        // Drain PEX-discovered peers
        {
            let mut pex = pex_peers.lock().unwrap();
            for addr in pex.drain(..) {
                if seen.insert(addr) {
                    pending.push_back(addr);
                }
            }
        }

        // Fill active set from pending queue
        while set.len() < max_concurrent {
            if let Some(addr) = pending.pop_front() {
                let mgr = manager.clone();
                let wr = writer.clone();
                let p = pb.clone();
                let sp = speed.clone();
                let pp = pex_peers.clone();
                set.spawn(async move {
                    let r = peer::run(addr, info_hash, peer_id, mgr, wr, p, sp, pp).await;
                    (addr, r)
                });
            } else {
                break;
            }
        }

        // Show status when idle
        if set.is_empty() && pending.is_empty() && channel_open {
            pb.set_message("waiting for peers");
        } else {
            pb.set_message("");
        }

        tokio::select! {
            result = rx.recv(), if channel_open => {
                match result {
                    Some(addr) => {
                        if seen.insert(addr) {
                            pending.push_back(addr);
                        }
                    }
                    None => {
                        channel_open = false;
                        vlog!("peer discovery finished ({} unique peers)", seen.len());
                    }
                }
            }
            result = set.join_next(), if !set.is_empty() => {
                if let Some(join_result) = result {
                    match join_result {
                        Ok((_, Ok(()))) => ok_count += 1,
                        Ok((peer_addr, Err(e))) => {
                            let msg = format!("{e:#}");
                            vlog!("  peer {peer_addr}: {msg}");
                            if msg.contains("connect timeout") || msg.contains("timed out") || msg.contains("refused") {
                                connect_fail += 1;
                            } else if msg.contains("unchoke timeout") || msg.contains("no data received") {
                                no_unchoke += 1;
                            } else if msg.contains("no needed pieces") {
                                no_pieces += 1;
                            } else {
                                other_err += 1;
                            }
                        }
                        Err(_) => other_err += 1,
                    }

                    if manager.lock().unwrap().is_complete() {
                        set.abort_all();
                        break;
                    }
                }
            }
            else => break,
        }
    }

    pb.set_message("");
    let total = ok_count + connect_fail + no_unchoke + no_pieces + other_err;
    if total > 0 {
        vlog!(
            "peer stats: {ok_count} ok, {connect_fail} connect fail, {no_unchoke} no unchoke, {no_pieces} no pieces, {other_err} error"
        );
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

    pub fn has_needed_for(&self, peer_has: &[bool]) -> bool {
        self.states.iter().enumerate().any(|(i, s)| {
            *s == PieceState::Needed && peer_has.get(i).copied().unwrap_or(false)
        })
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

pub async fn download_magnet(
    magnet_uri: &str,
    output_dir: &Path,
    max_peers: usize,
) -> Result<String> {
    let ml = magnet::parse(magnet_uri)?;

    vlog!("info_hash: {}", hex_encode(&ml.info_hash));
    if let Some(ref dn) = ml.display_name {
        println!("  name: {dn}");
    }

    let peer_id = generate_peer_id();

    vlog!("fetching metadata...");

    let peers = tracker::get_peers_for_hash(&ml.info_hash, &ml.trackers, &peer_id, 0).await?;
    vlog!("metadata peers: {}", peers.len());

    if peers.is_empty() {
        bail!("no peers found for magnet link");
    }

    let mut metadata = None;
    let mut tried = 0;
    for &addr in peers.iter().take(30) {
        tried += 1;
        match peer::fetch_metadata(addr, ml.info_hash, peer_id).await {
            Ok(data) => {
                metadata = Some(data);
                break;
            }
            Err(_) if tried < 30 => continue,
            Err(e) => {
                vlog!("metadata from {addr}: {e:#}");
                continue;
            }
        }
    }

    let info_raw = metadata.context("failed to fetch metadata from any peer")?;

    let torrent_data = {
        use std::collections::BTreeMap;
        let info_val = bencode::decode(&info_raw)?;
        let mut root = BTreeMap::new();
        root.insert(
            "announce".to_string(),
            bencode::Value::Bytes(
                ml.trackers.first().unwrap_or(&String::new()).as_bytes().to_vec(),
            ),
        );
        if !ml.trackers.is_empty() {
            let tiers: Vec<bencode::Value> = ml
                .trackers
                .iter()
                .map(|t| {
                    bencode::Value::List(vec![bencode::Value::Bytes(t.as_bytes().to_vec())])
                })
                .collect();
            root.insert("announce-list".to_string(), bencode::Value::List(tiers));
        }
        root.insert("info".to_string(), info_val);
        bencode::encode(&bencode::Value::Dict(root))
    };

    let torrent = metainfo::parse(&torrent_data)?;
    println!("  name: {}", torrent.name);
    println!(
        "  size: {} ({} pieces)",
        format_size(torrent.total_size),
        torrent.pieces.len()
    );

    let base = output_dir.join(&torrent.name);
    prepare_files(&torrent, &base).await?;

    let manager = Arc::new(Mutex::new(PieceManager::new(&torrent)));
    let writer = Arc::new(FileWriter::new(&torrent, &base));
    let num_pieces = torrent.pieces.len();

    let mut verified = 0u64;
    vlog!("checking existing data ({num_pieces} pieces)...");

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
        vlog!("{done}/{num_pieces} pieces ({}) verified", format_size(verified));
    }

    if manager.lock().unwrap().is_complete() {
        println!("  already complete");
        return Ok(torrent.name.clone());
    }

    let pb = crate::progress::create(Some(torrent.total_size));
    pb.inc(verified);

    let (tx, rx) = tokio::sync::mpsc::channel(200);
    let existing = peers;
    let trackers = tracker::collect_trackers(&torrent);
    let ih = torrent.info_hash;
    let pid = peer_id;
    let ts = torrent.total_size;
    let discover_handle = tokio::spawn(async move {
        for addr in existing {
            if tx.send(addr).await.is_err() {
                return;
            }
        }
        tracker::discover_peers(trackers, ih, pid, ts, tx).await;
    });

    run_download_stream(rx, torrent.info_hash, peer_id, &manager, &writer, &pb, max_peers).await;
    discover_handle.abort();

    if manager.lock().unwrap().is_complete() {
        pb.finish_with_message("done");
        Ok(torrent.name.clone())
    } else {
        let done = manager.lock().unwrap().completed_count();
        let total = manager.lock().unwrap().num_pieces();
        pb.abandon_with_message("incomplete");
        bail!("download incomplete: {done}/{total} pieces")
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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
