use anyhow::{Context, Result, bail};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;

use super::bencode;
use super::dht;
use super::metainfo::Torrent;
use crate::log::vlog;

pub fn collect_trackers(torrent: &Torrent) -> Vec<String> {
    let mut trackers = Vec::new();
    if !torrent.announce.is_empty() {
        trackers.push(torrent.announce.clone());
    }
    for tier in &torrent.announce_list {
        for url in tier {
            if !trackers.contains(url) {
                trackers.push(url.clone());
            }
        }
    }
    trackers
}

pub async fn get_peers_for_hash(
    info_hash: &[u8; 20],
    trackers: &[String],
    peer_id: &[u8; 20],
    total_size: u64,
) -> Result<Vec<SocketAddr>> {
    let mut all_peers = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut set = tokio::task::JoinSet::new();

    for tracker_url in trackers {
        let url = tracker_url.clone();
        let ih = *info_hash;
        let pid = *peer_id;
        set.spawn(async move {
            let result = if url.starts_with("udp://") {
                udp_announce(&url, &ih, &pid, total_size).await
            } else if url.starts_with("http://") || url.starts_with("https://") {
                http_announce(&url, &ih, &pid, total_size).await
            } else {
                Ok(Vec::new())
            };
            (url, result)
        });
    }

    let ih = *info_hash;
    set.spawn(async move {
        let result = dht::find_peers(&ih).await;
        ("DHT".to_string(), result)
    });

    while let Some(Ok((source, result))) = set.join_next().await {
        match result {
            Ok(peers) => {
                let count = peers.len();
                for addr in peers {
                    if seen.insert(addr) {
                        all_peers.push(addr);
                    }
                }
                if source == "DHT" && count > 0 {
                    vlog!("  DHT: found {count} peers");
                }
            }
            Err(e) => {
                if source == "DHT" {
                    vlog!("  DHT: {e:#}");
                } else {
                    vlog!("  tracker {source}: {e:#}");
                }
            }
        }
    }

    Ok(all_peers)
}

pub async fn discover_peers(
    trackers: Vec<String>,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    total_size: u64,
    tx: tokio::sync::mpsc::Sender<SocketAddr>,
) {
    let mut set = tokio::task::JoinSet::new();

    for tracker_url in trackers {
        let tx = tx.clone();
        set.spawn(async move {
            let result = if tracker_url.starts_with("udp://") {
                udp_announce(&tracker_url, &info_hash, &peer_id, total_size).await
            } else if tracker_url.starts_with("http://") || tracker_url.starts_with("https://") {
                http_announce(&tracker_url, &info_hash, &peer_id, total_size).await
            } else {
                return;
            };
            match result {
                Ok(peers) => {
                    for addr in peers {
                        let _ = tx.send(addr).await;
                    }
                }
                Err(e) => vlog!("  tracker {tracker_url}: {e:#}"),
            }
        });
    }

    let tx_dht = tx.clone();
    set.spawn(async move {
        let mut round = 0u32;
        loop {
            if round > 0 {
                let delay = match round {
                    1..=5 => 20,
                    6..=10 => 40,
                    _ => 60,
                };
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }
            match dht::find_peers(&info_hash).await {
                Ok(peers) => {
                    let count = peers.len();
                    for addr in peers {
                        if tx_dht.send(addr).await.is_err() {
                            return;
                        }
                    }
                    if count > 0 {
                        vlog!("  DHT round {}: found {count} peers", round + 1);
                    }
                }
                Err(e) => vlog!("  DHT: {e:#}"),
            }
            round += 1;
        }
    });

    drop(tx);
    while set.join_next().await.is_some() {}
}

async fn http_announce(
    announce_url: &str,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    left: u64,
) -> Result<Vec<SocketAddr>> {
    let sep = if announce_url.contains('?') { '&' } else { '?' };
    let url = format!(
        "{announce_url}{sep}info_hash={}&peer_id={}&port=0&uploaded=0&downloaded=0&left={left}&compact=1&event=started",
        urlencode(info_hash),
        urlencode(peer_id),
    );

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(15))
        .local_address("0.0.0.0".parse::<std::net::IpAddr>().unwrap())
        .build()?;

    let body = client.get(&url).send().await?.bytes().await?;
    if body.first() == Some(&b'<') {
        bail!("tracker returned HTML (likely authentication/passkey required)");
    }
    let value = bencode::decode(&body).context("failed to decode tracker response")?;
    let dict = value.as_dict().context("tracker response is not a dict")?;

    if let Some(err) = dict.get("failure reason").and_then(|v| v.as_str()) {
        bail!("tracker: {err}");
    }

    let peers_val = dict.get("peers").context("no 'peers' in tracker response")?;
    parse_peers(peers_val)
}

async fn udp_announce(
    tracker_url: &str,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    left: u64,
) -> Result<Vec<SocketAddr>> {
    let host_port = tracker_url
        .strip_prefix("udp://")
        .context("not a UDP URL")?
        .split('/')
        .next()
        .context("invalid UDP tracker URL")?;

    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket
        .connect(host_port)
        .await
        .with_context(|| format!("cannot reach UDP tracker {host_port}"))?;

    // --- Connect ---
    let txn: u32 = rand::random();
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&0x0417_2710_1980u64.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf.extend_from_slice(&txn.to_be_bytes());
    socket.send(&buf).await?;

    let mut resp = [0u8; 2048];
    let n = tokio::time::timeout(Duration::from_secs(15), socket.recv(&mut resp))
        .await
        .context("UDP connect timeout")??;
    if n < 16 {
        bail!("UDP connect response too short");
    }
    let action = u32::from_be_bytes(resp[0..4].try_into()?);
    let recv_txn = u32::from_be_bytes(resp[4..8].try_into()?);
    if action != 0 || recv_txn != txn {
        bail!("invalid UDP connect response");
    }
    let conn_id = u64::from_be_bytes(resp[8..16].try_into()?);

    // --- Announce ---
    let txn: u32 = rand::random();
    let mut req = Vec::with_capacity(98);
    req.extend_from_slice(&conn_id.to_be_bytes());
    req.extend_from_slice(&1u32.to_be_bytes()); // action
    req.extend_from_slice(&txn.to_be_bytes());
    req.extend_from_slice(info_hash);
    req.extend_from_slice(peer_id);
    req.extend_from_slice(&0u64.to_be_bytes()); // downloaded
    req.extend_from_slice(&left.to_be_bytes());
    req.extend_from_slice(&0u64.to_be_bytes()); // uploaded
    req.extend_from_slice(&2u32.to_be_bytes()); // event=started
    req.extend_from_slice(&0u32.to_be_bytes()); // ip
    req.extend_from_slice(&rand::random::<u32>().to_be_bytes()); // key
    req.extend_from_slice(&(-1i32).to_be_bytes()); // num_want
    req.extend_from_slice(&0u16.to_be_bytes()); // port=0
    socket.send(&req).await?;

    let n = tokio::time::timeout(Duration::from_secs(15), socket.recv(&mut resp))
        .await
        .context("UDP announce timeout")??;
    if n < 20 {
        bail!("UDP announce response too short");
    }
    let action = u32::from_be_bytes(resp[0..4].try_into()?);
    let recv_txn = u32::from_be_bytes(resp[4..8].try_into()?);
    if action != 1 || recv_txn != txn {
        bail!("invalid UDP announce response");
    }

    let mut peers = Vec::new();
    let mut pos = 20;
    while pos + 6 <= n {
        let ip = Ipv4Addr::new(resp[pos], resp[pos + 1], resp[pos + 2], resp[pos + 3]);
        let port = u16::from_be_bytes([resp[pos + 4], resp[pos + 5]]);
        if port > 0 {
            peers.push(SocketAddr::new(IpAddr::V4(ip), port));
        }
        pos += 6;
    }

    Ok(peers)
}

fn parse_peers(value: &bencode::Value) -> Result<Vec<SocketAddr>> {
    match value {
        bencode::Value::Bytes(data) => {
            if data.len() % 6 != 0 {
                bail!("compact peers length is not a multiple of 6");
            }
            Ok(data
                .as_chunks::<6>()
                .0
                .iter()
                .filter_map(|c| {
                    let ip = Ipv4Addr::new(c[0], c[1], c[2], c[3]);
                    let port = u16::from_be_bytes([c[4], c[5]]);
                    (port > 0).then(|| SocketAddr::new(IpAddr::V4(ip), port))
                })
                .collect())
        }
        bencode::Value::List(list) => {
            let mut peers = Vec::new();
            for entry in list {
                if let Some(d) = entry.as_dict() {
                    let ip = d.get("ip").and_then(|v| v.as_str()).unwrap_or("");
                    let port = d.get("port").and_then(|v| v.as_int()).unwrap_or(0) as u16;
                    if let Ok(addr) = format!("{ip}:{port}").parse() {
                        peers.push(addr);
                    }
                }
            }
            Ok(peers)
        }
        _ => bail!("unexpected peers format"),
    }
}

fn urlencode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
