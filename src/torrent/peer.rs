use anyhow::{Context, Result, bail};
use indicatif::ProgressBar;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::Instant;

use super::bencode::{self, Value};
use super::{FileWriter, PieceManager, SpeedTracker};

const BLOCK_SIZE: u32 = 16384;
const PIPELINE_DEPTH: u32 = 16;
const METADATA_PIECE_SIZE: usize = 16384;
const UT_METADATA_ID: u8 = 1;
const WARMUP_SECS: f64 = 15.0;
const MIN_SPEED_BPS: f64 = 10_000.0;
const SPEED_CHECK_INTERVAL: Duration = Duration::from_secs(10);

struct PieceGuard {
    index: Option<u32>,
    manager: Arc<Mutex<PieceManager>>,
}

impl Drop for PieceGuard {
    fn drop(&mut self) {
        if let Some(idx) = self.index.take()
            && let Ok(mut mgr) = self.manager.lock()
        {
            mgr.piece_failed(idx);
        }
    }
}

struct PieceDownload {
    index: u32,
    piece_size: u32,
    blocks: Vec<Option<Vec<u8>>>,
    next_to_request: u32,
    outstanding: u32,
}

impl PieceDownload {
    fn new(index: u32, piece_size: u32) -> Self {
        let num_blocks = piece_size.div_ceil(BLOCK_SIZE);
        Self {
            index,
            piece_size,
            blocks: vec![None; num_blocks as usize],
            next_to_request: 0,
            outstanding: 0,
        }
    }

    fn pending_requests(&mut self) -> Vec<(u32, u32, u32)> {
        let num_blocks = self.blocks.len() as u32;
        let mut reqs = Vec::new();
        while self.outstanding < PIPELINE_DEPTH && self.next_to_request < num_blocks {
            let begin = self.next_to_request * BLOCK_SIZE;
            let len = BLOCK_SIZE.min(self.piece_size - begin);
            reqs.push((self.index, begin, len));
            self.next_to_request += 1;
            self.outstanding += 1;
        }
        reqs
    }

    fn add_block(&mut self, begin: u32, data: Vec<u8>) -> bool {
        let idx = (begin / BLOCK_SIZE) as usize;
        if idx < self.blocks.len() && self.blocks[idx].is_none() {
            self.blocks[idx] = Some(data);
            self.outstanding = self.outstanding.saturating_sub(1);
        }
        self.is_complete()
    }

    fn is_complete(&self) -> bool {
        self.blocks.iter().all(|b| b.is_some())
    }

    fn assemble(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.piece_size as usize);
        for b in self.blocks.iter().flatten() {
            data.extend_from_slice(b);
        }
        data
    }
}

pub async fn run(
    addr: SocketAddr,
    info_hash: [u8; 20],
    our_peer_id: [u8; 20],
    manager: Arc<Mutex<PieceManager>>,
    writer: Arc<FileWriter>,
    pb: ProgressBar,
    speed: Arc<SpeedTracker>,
) -> Result<()> {
    let mut guard = PieceGuard {
        index: None,
        manager: manager.clone(),
    };

    let stream = tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(addr))
        .await
        .map_err(|_| anyhow::anyhow!("connect timeout"))??;
    stream.set_nodelay(true)?;

    let (rd, wr) = tokio::io::split(stream);
    let mut rd = BufReader::new(rd);
    let mut wr = wr;

    send_handshake(&mut wr, &info_hash, &our_peer_id).await?;
    let hs = tokio::time::timeout(Duration::from_secs(10), recv_handshake(&mut rd))
        .await
        .map_err(|_| anyhow::anyhow!("handshake timeout"))??;

    if hs.info_hash != info_hash {
        bail!("info_hash mismatch");
    }

    let num_pieces = manager.lock().unwrap().num_pieces();
    let mut peer_has = vec![false; num_pieces];
    let mut choked = true;
    let mut current: Option<PieceDownload> = None;
    let mut got_data = false;
    let deadline = Instant::now() + Duration::from_secs(30);

    let mut peer_bytes: u64 = 0;
    let mut peer_start: Option<Instant> = None;
    let mut last_speed_check = Instant::now();

    send_msg(&mut wr, 2, &[]).await?;

    loop {
        if manager.lock().unwrap().is_complete() {
            return Ok(());
        }

        let read_timeout = if got_data {
            Duration::from_secs(60)
        } else {
            deadline.saturating_duration_since(Instant::now())
        };
        if read_timeout.is_zero() {
            bail!("no data received within 30s");
        }

        let msg = tokio::time::timeout(read_timeout, read_msg(&mut rd))
            .await
            .map_err(|_| if got_data {
                anyhow::anyhow!("read timeout")
            } else {
                anyhow::anyhow!("unchoke timeout")
            })??;

        match msg {
            Msg::KeepAlive => {}
            Msg::Choke => choked = true,
            Msg::Unchoke => {
                choked = false;
                try_start(&manager, &peer_has, &mut current, &mut guard);
                if !choked {
                    flush_requests(&mut wr, &mut current).await?;
                }
            }
            Msg::Have(idx) => {
                if (idx as usize) < peer_has.len() {
                    peer_has[idx as usize] = true;
                }
                if current.is_none() && !choked {
                    try_start(&manager, &peer_has, &mut current, &mut guard);
                    flush_requests(&mut wr, &mut current).await?;
                }
            }
            Msg::Bitfield(data) => {
                for (i, has) in peer_has.iter_mut().enumerate().take(num_pieces) {
                    let byte = i / 8;
                    let bit = 7 - (i % 8);
                    if byte < data.len() {
                        *has = (data[byte] >> bit) & 1 == 1;
                    }
                }
                if current.is_none() && !choked {
                    try_start(&manager, &peer_has, &mut current, &mut guard);
                    flush_requests(&mut wr, &mut current).await?;
                }
            }
            Msg::Piece { index, begin, data } => {
                let len = data.len() as u64;
                let mut done = false;
                got_data = true;

                if peer_start.is_none() {
                    peer_start = Some(Instant::now());
                }
                peer_bytes += len;
                speed.add_bytes(len);

                if let Some(piece) = current.as_mut()
                    && piece.index == index
                {
                    done = piece.add_block(begin, data);
                    pb.inc(len);
                }

                if let Some(start) = peer_start {
                    if last_speed_check.elapsed() >= SPEED_CHECK_INTERVAL {
                        last_speed_check = Instant::now();
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed > WARMUP_SECS {
                            let peer_speed = peer_bytes as f64 / elapsed;
                            let avg_speed = speed.average_speed();
                            if avg_speed > 50_000.0 && peer_speed < avg_speed * 0.15 {
                                bail!("slow peer: {:.0} B/s (avg {:.0} B/s)", peer_speed, avg_speed);
                            }
                            if peer_speed < MIN_SPEED_BPS {
                                bail!("peer below minimum speed: {:.0} B/s", peer_speed);
                            }
                        }
                    }
                }

                if done {
                    let piece = current.take().unwrap();
                    guard.index = None;
                    let assembled = piece.assemble();
                    let expected = manager.lock().unwrap().piece_hash(piece.index);

                    if verify_sha1(&assembled, &expected) {
                        writer.write_piece(piece.index, &assembled).await?;
                        manager.lock().unwrap().piece_done(piece.index);
                    } else {
                        manager.lock().unwrap().piece_failed(piece.index);
                    }

                    if manager.lock().unwrap().is_complete() {
                        return Ok(());
                    }

                    try_start(&manager, &peer_has, &mut current, &mut guard);
                    if !choked {
                        flush_requests(&mut wr, &mut current).await?;
                    }
                } else if !choked {
                    flush_requests(&mut wr, &mut current).await?;
                }
            }
            Msg::Unknown => {}
        }
    }
}

fn try_start(
    manager: &Arc<Mutex<PieceManager>>,
    peer_has: &[bool],
    current: &mut Option<PieceDownload>,
    guard: &mut PieceGuard,
) {
    if current.is_some() {
        return;
    }
    let mut mgr = manager.lock().unwrap();
    if let Some(idx) = mgr.next_piece(peer_has) {
        let size = mgr.piece_size(idx);
        *current = Some(PieceDownload::new(idx, size as u32));
        guard.index = Some(idx);
    }
}

async fn flush_requests(
    wr: &mut tokio::io::WriteHalf<TcpStream>,
    current: &mut Option<PieceDownload>,
) -> Result<()> {
    if let Some(piece) = current.as_mut() {
        let reqs = piece.pending_requests();
        if reqs.is_empty() {
            return Ok(());
        }
        // 17 bytes per request: 4 (length) + 1 (id) + 4+4+4 (index, begin, len)
        let mut buf = Vec::with_capacity(reqs.len() * 17);
        for (index, begin, length) in reqs {
            buf.extend_from_slice(&13u32.to_be_bytes());
            buf.push(6);
            buf.extend_from_slice(&index.to_be_bytes());
            buf.extend_from_slice(&begin.to_be_bytes());
            buf.extend_from_slice(&length.to_be_bytes());
        }
        wr.write_all(&buf).await?;
        wr.flush().await?;
    }
    Ok(())
}

fn verify_sha1(data: &[u8], expected: &[u8; 20]) -> bool {
    use sha1::{Digest, Sha1};
    let hash = Sha1::digest(data);
    hash.as_slice() == expected
}

// --- Wire protocol ---

enum Msg {
    KeepAlive,
    Choke,
    Unchoke,
    Have(u32),
    Bitfield(Vec<u8>),
    Piece { index: u32, begin: u32, data: Vec<u8> },
    Unknown,
}

async fn read_msg(rd: &mut (impl AsyncReadExt + Unpin)) -> Result<Msg> {
    let length = rd.read_u32().await?;
    if length == 0 {
        return Ok(Msg::KeepAlive);
    }
    if length > 1 << 24 {
        bail!("message too large: {length}");
    }
    let id = rd.read_u8().await?;
    let payload_len = length as usize - 1;

    match id {
        0 => Ok(Msg::Choke),
        1 => Ok(Msg::Unchoke),
        2 | 3 => Ok(Msg::Unknown),
        4 => {
            let idx = rd.read_u32().await?;
            Ok(Msg::Have(idx))
        }
        5 => {
            let mut data = vec![0u8; payload_len];
            rd.read_exact(&mut data).await?;
            Ok(Msg::Bitfield(data))
        }
        7 => {
            if payload_len < 8 {
                bail!("piece message too short");
            }
            let index = rd.read_u32().await?;
            let begin = rd.read_u32().await?;
            let mut data = vec![0u8; payload_len - 8];
            rd.read_exact(&mut data).await?;
            Ok(Msg::Piece { index, begin, data })
        }
        _ => {
            if payload_len > 0 {
                let mut discard = vec![0u8; payload_len];
                rd.read_exact(&mut discard).await?;
            }
            Ok(Msg::Unknown)
        }
    }
}

async fn send_msg(wr: &mut (impl AsyncWriteExt + Unpin), id: u8, payload: &[u8]) -> Result<()> {
    let length = (1 + payload.len()) as u32;
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.extend_from_slice(&length.to_be_bytes());
    buf.push(id);
    buf.extend_from_slice(payload);
    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

async fn send_handshake(
    wr: &mut (impl AsyncWriteExt + Unpin),
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
) -> Result<()> {
    let mut msg = Vec::with_capacity(68);
    msg.push(19);
    msg.extend_from_slice(b"BitTorrent protocol");
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10; // BEP 10: extension protocol
    msg.extend_from_slice(&reserved);
    msg.extend_from_slice(info_hash);
    msg.extend_from_slice(peer_id);
    wr.write_all(&msg).await?;
    wr.flush().await?;
    Ok(())
}

struct Handshake {
    info_hash: [u8; 20],
    _peer_id: [u8; 20],
    supports_extensions: bool,
}

async fn recv_handshake(rd: &mut (impl AsyncReadExt + Unpin)) -> Result<Handshake> {
    let pstrlen = rd.read_u8().await?;
    if pstrlen != 19 {
        bail!("invalid protocol string length: {pstrlen}");
    }
    let mut pstr = [0u8; 19];
    rd.read_exact(&mut pstr).await?;
    if &pstr != b"BitTorrent protocol" {
        bail!("unknown protocol");
    }
    let mut reserved = [0u8; 8];
    rd.read_exact(&mut reserved).await?;
    let mut info_hash = [0u8; 20];
    rd.read_exact(&mut info_hash).await?;
    let mut peer_id = [0u8; 20];
    rd.read_exact(&mut peer_id).await?;
    Ok(Handshake {
        info_hash,
        _peer_id: peer_id,
        supports_extensions: reserved[5] & 0x10 != 0,
    })
}

// --- BEP 9/10: Metadata exchange ---

pub async fn fetch_metadata(
    addr: SocketAddr,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
) -> Result<Vec<u8>> {
    let stream = tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(addr))
        .await
        .map_err(|_| anyhow::anyhow!("connect timeout"))??;

    let (rd, wr) = tokio::io::split(stream);
    let mut rd = BufReader::new(rd);
    let mut wr = wr;

    send_handshake(&mut wr, &info_hash, &peer_id).await?;
    let hs = tokio::time::timeout(Duration::from_secs(10), recv_handshake(&mut rd))
        .await
        .map_err(|_| anyhow::anyhow!("handshake timeout"))??;

    if hs.info_hash != info_hash {
        bail!("info_hash mismatch");
    }
    if !hs.supports_extensions {
        bail!("peer does not support extension protocol");
    }

    send_ext_handshake(&mut wr).await?;

    let mut remote_ut_metadata: Option<u8> = None;
    let mut metadata_size: Option<usize> = None;
    let mut pieces: Vec<Option<Vec<u8>>> = Vec::new();
    let mut requested_up_to: usize = 0;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            bail!("metadata download timeout");
        }

        let msg = tokio::time::timeout(remaining, read_msg_raw(&mut rd))
            .await
            .map_err(|_| anyhow::anyhow!("metadata read timeout"))??;

        if msg.is_empty() {
            continue;
        }

        let id = msg[0];
        let payload = &msg[1..];

        if id == 20 {
            if payload.is_empty() {
                continue;
            }
            let ext_id = payload[0];
            let ext_payload = &payload[1..];

            if ext_id == 0 {
                let val = bencode::decode(ext_payload)
                    .context("failed to decode extension handshake")?;
                let dict = val.as_dict().context("ext handshake not a dict")?;

                if let Some(m) = dict.get("m").and_then(|v| v.as_dict()) {
                    if let Some(id) = m.get("ut_metadata").and_then(|v| v.as_int()) {
                        remote_ut_metadata = Some(id as u8);
                    }
                }
                if let Some(size) = dict.get("metadata_size").and_then(|v| v.as_int()) {
                    metadata_size = Some(size as usize);
                }

                if let (Some(ut_id), Some(size)) = (remote_ut_metadata, metadata_size) {
                    if size == 0 || size > 10 * 1024 * 1024 {
                        bail!("invalid metadata_size: {size}");
                    }
                    let num_pieces = size.div_ceil(METADATA_PIECE_SIZE);
                    pieces.resize(num_pieces, None);
                    for i in requested_up_to..num_pieces {
                        send_metadata_request(&mut wr, ut_id, i as u32).await?;
                    }
                    requested_up_to = num_pieces;
                }
            } else if remote_ut_metadata.is_some_and(|ut| ext_id == ut) {
                if let Some(total_size) = metadata_size {
                    if let Some((piece_idx, piece_data)) =
                        parse_metadata_data(ext_payload, total_size)
                    {
                        if piece_idx < pieces.len() {
                            pieces[piece_idx] = Some(piece_data);
                        }

                        if pieces.iter().all(|p| p.is_some()) {
                            let mut assembled = Vec::with_capacity(total_size);
                            for p in &pieces {
                                assembled.extend_from_slice(p.as_ref().unwrap());
                            }
                            assembled.truncate(total_size);

                            use sha1::{Digest, Sha1};
                            let hash = Sha1::digest(&assembled);
                            if hash.as_slice() != info_hash {
                                bail!("metadata hash mismatch");
                            }
                            return Ok(assembled);
                        }
                    }
                }
            }
        }
    }
}

async fn send_ext_handshake(wr: &mut (impl AsyncWriteExt + Unpin)) -> Result<()> {
    let mut m = BTreeMap::new();
    m.insert(
        "ut_metadata".to_string(),
        Value::Int(UT_METADATA_ID as i64),
    );

    let mut hs = BTreeMap::new();
    hs.insert("m".to_string(), Value::Dict(m));

    let payload = bencode::encode(&Value::Dict(hs));
    let mut msg = Vec::with_capacity(2 + payload.len());
    msg.push(0); // extension handshake ID
    msg.extend_from_slice(&payload);
    send_msg(wr, 20, &msg).await
}

async fn send_metadata_request(
    wr: &mut (impl AsyncWriteExt + Unpin),
    ut_metadata_id: u8,
    piece: u32,
) -> Result<()> {
    let mut req = BTreeMap::new();
    req.insert("msg_type".to_string(), Value::Int(0));
    req.insert("piece".to_string(), Value::Int(piece as i64));

    let payload = bencode::encode(&Value::Dict(req));
    let mut msg = Vec::with_capacity(1 + payload.len());
    msg.push(ut_metadata_id);
    msg.extend_from_slice(&payload);
    send_msg(wr, 20, &msg).await
}

fn parse_metadata_data(payload: &[u8], total_size: usize) -> Option<(usize, Vec<u8>)> {
    let (val, consumed) = bencode::decode_at(payload, 0).ok()?;
    let dict = val.as_dict()?;
    let msg_type = dict.get("msg_type").and_then(|v| v.as_int())?;
    if msg_type != 1 {
        return None;
    }
    let piece = dict.get("piece").and_then(|v| v.as_int())? as usize;
    let data = payload[consumed..].to_vec();

    let expected_len = if (piece + 1) * METADATA_PIECE_SIZE > total_size {
        total_size - piece * METADATA_PIECE_SIZE
    } else {
        METADATA_PIECE_SIZE
    };
    if data.len() != expected_len {
        return None;
    }
    Some((piece, data))
}

async fn read_msg_raw(rd: &mut (impl AsyncReadExt + Unpin)) -> Result<Vec<u8>> {
    let length = rd.read_u32().await?;
    if length == 0 {
        return Ok(Vec::new());
    }
    if length > 1 << 24 {
        bail!("message too large: {length}");
    }
    let mut data = vec![0u8; length as usize];
    rd.read_exact(&mut data).await?;
    Ok(data)
}
