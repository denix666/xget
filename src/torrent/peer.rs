use anyhow::{Result, bail};
use indicatif::ProgressBar;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use super::{FileWriter, PieceManager};

const BLOCK_SIZE: u32 = 16384;
const PIPELINE_DEPTH: u32 = 5;

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
) -> Result<()> {
    let mut guard = PieceGuard {
        index: None,
        manager: manager.clone(),
    };

    let stream = tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(addr))
        .await
        .map_err(|_| anyhow::anyhow!("connect timeout"))??;

    let (rd, wr) = tokio::io::split(stream);
    let mut rd = BufReader::new(rd);
    let mut wr = wr;

    send_handshake(&mut wr, &info_hash, &our_peer_id).await?;
    let (recv_hash, _) = tokio::time::timeout(Duration::from_secs(10), recv_handshake(&mut rd))
        .await
        .map_err(|_| anyhow::anyhow!("handshake timeout"))??;

    if recv_hash != info_hash {
        bail!("info_hash mismatch");
    }

    let num_pieces = manager.lock().unwrap().num_pieces();
    let mut peer_has = vec![false; num_pieces];
    let mut choked = true;
    let mut current: Option<PieceDownload> = None;
    let mut got_data = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    send_msg(&mut wr, 2, &[]).await?;

    loop {
        if manager.lock().unwrap().is_complete() {
            return Ok(());
        }

        let read_timeout = if got_data {
            Duration::from_secs(120)
        } else {
            deadline.saturating_duration_since(tokio::time::Instant::now())
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

                if let Some(piece) = current.as_mut()
                    && piece.index == index
                {
                    done = piece.add_block(begin, data);
                    pb.inc(len);
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
        for (index, begin, length) in piece.pending_requests() {
            let mut payload = Vec::with_capacity(12);
            payload.extend_from_slice(&index.to_be_bytes());
            payload.extend_from_slice(&begin.to_be_bytes());
            payload.extend_from_slice(&length.to_be_bytes());
            send_msg(wr, 6, &payload).await?;
        }
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
    wr.write_u32(length).await?;
    wr.write_u8(id).await?;
    if !payload.is_empty() {
        wr.write_all(payload).await?;
    }
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
    msg.extend_from_slice(&[0u8; 8]);
    msg.extend_from_slice(info_hash);
    msg.extend_from_slice(peer_id);
    wr.write_all(&msg).await?;
    wr.flush().await?;
    Ok(())
}

async fn recv_handshake(rd: &mut (impl AsyncReadExt + Unpin)) -> Result<([u8; 20], [u8; 20])> {
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
    Ok((info_hash, peer_id))
}
