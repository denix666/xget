use anyhow::Result;
use std::collections::{BTreeMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::net::UdpSocket;

use super::bencode::{self, Value};

const BOOTSTRAP_NODES: &[&str] = &[
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "router.utorrent.com:6881",
    "dht.libtorrent.org:25401",
];

const MAX_ITERATIONS: usize = 6;
const QUERY_TIMEOUT: Duration = Duration::from_secs(4);
const BATCH_SIZE: usize = 8;

pub async fn find_peers(info_hash: &[u8; 20]) -> Result<Vec<SocketAddr>> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let our_id = generate_node_id(info_hash);

    let mut peers: HashSet<SocketAddr> = HashSet::new();
    let mut queried: HashSet<SocketAddr> = HashSet::new();
    let mut candidates: Vec<(SocketAddr, [u8; 20])> = Vec::new();
    let mut txn: u16 = 0;

    for addr_str in BOOTSTRAP_NODES {
        match tokio::net::lookup_host(addr_str).await {
            Ok(addrs) => {
                for addr in addrs {
                    if addr.is_ipv4() {
                        candidates.push((addr, [0u8; 20]));
                    }
                }
            }
            Err(_) => continue,
        }
    }

    for _ in 0..MAX_ITERATIONS {
        if candidates.is_empty() || peers.len() >= 200 {
            break;
        }

        candidates.sort_by(|a, b| {
            xor_distance(&a.1, info_hash).cmp(&xor_distance(&b.1, info_hash))
        });

        let batch: Vec<SocketAddr> = candidates
            .drain(..candidates.len().min(BATCH_SIZE))
            .map(|(addr, _)| addr)
            .filter(|addr| queried.insert(*addr))
            .collect();

        if batch.is_empty() {
            continue;
        }

        for &addr in &batch {
            txn = txn.wrapping_add(1);
            let msg = build_get_peers(&our_id, info_hash, txn);
            let _ = socket.send_to(&msg, addr).await;
        }

        let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
        let mut buf = [0u8; 4096];

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) => {
                    let Ok(value) = bencode::decode(&buf[..n]) else {
                        continue;
                    };
                    let Some(dict) = value.as_dict() else {
                        continue;
                    };
                    let Some(r) = dict.get("r").and_then(|v| v.as_dict()) else {
                        continue;
                    };

                    if let Some(values) = r.get("values").and_then(|v| v.as_list()) {
                        for v in values {
                            if let Some(data) = v.as_bytes() {
                                if let Some(addr) = parse_compact_peer(data) {
                                    peers.insert(addr);
                                }
                            }
                        }
                    }

                    if let Some(nodes_data) = r.get("nodes").and_then(|v| v.as_bytes()) {
                        for (id, addr) in parse_compact_nodes(nodes_data) {
                            if !queried.contains(&addr) {
                                candidates.push((addr, id));
                            }
                        }
                    }
                }
                _ => break,
            }
        }
    }

    Ok(peers.into_iter().collect())
}

fn generate_node_id(info_hash: &[u8; 20]) -> [u8; 20] {
    let mut id = [0u8; 20];
    rand::fill(&mut id);
    // Place first 3 bytes close to target for Sybil-resistant DHT implementations
    id[0] = info_hash[0];
    id[1] = info_hash[1];
    id[2] = (info_hash[2] & 0xF8) | (id[2] & 0x07);
    id
}

fn xor_distance(a: &[u8; 20], b: &[u8; 20]) -> [u8; 20] {
    let mut d = [0u8; 20];
    for i in 0..20 {
        d[i] = a[i] ^ b[i];
    }
    d
}

fn build_get_peers(our_id: &[u8; 20], info_hash: &[u8; 20], txn: u16) -> Vec<u8> {
    let mut args = BTreeMap::new();
    args.insert("id".to_string(), Value::Bytes(our_id.to_vec()));
    args.insert("info_hash".to_string(), Value::Bytes(info_hash.to_vec()));

    let mut msg = BTreeMap::new();
    msg.insert("a".to_string(), Value::Dict(args));
    msg.insert("q".to_string(), Value::Bytes(b"get_peers".to_vec()));
    msg.insert("t".to_string(), Value::Bytes(txn.to_be_bytes().to_vec()));
    msg.insert("y".to_string(), Value::Bytes(b"q".to_vec()));

    bencode::encode(&Value::Dict(msg))
}

fn parse_compact_peer(data: &[u8]) -> Option<SocketAddr> {
    if data.len() != 6 {
        return None;
    }
    let ip = Ipv4Addr::new(data[0], data[1], data[2], data[3]);
    let port = u16::from_be_bytes([data[4], data[5]]);
    (port > 0).then(|| SocketAddr::V4(SocketAddrV4::new(ip, port)))
}

fn parse_compact_nodes(data: &[u8]) -> Vec<([u8; 20], SocketAddr)> {
    let mut nodes = Vec::new();
    let mut pos = 0;
    while pos + 26 <= data.len() {
        let mut id = [0u8; 20];
        id.copy_from_slice(&data[pos..pos + 20]);
        let ip = Ipv4Addr::new(data[pos + 20], data[pos + 21], data[pos + 22], data[pos + 23]);
        let port = u16::from_be_bytes([data[pos + 24], data[pos + 25]]);
        if port > 0 {
            nodes.push((id, SocketAddr::V4(SocketAddrV4::new(ip, port))));
        }
        pos += 26;
    }
    nodes
}
