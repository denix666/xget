use anyhow::{Context, Result, bail};

pub struct MagnetLink {
    pub info_hash: [u8; 20],
    pub display_name: Option<String>,
    pub trackers: Vec<String>,
}

pub fn parse(uri: &str) -> Result<MagnetLink> {
    if !uri.starts_with("magnet:?") {
        bail!("not a magnet URI");
    }

    let query = &uri["magnet:?".len()..];
    let mut info_hash = None;
    let mut display_name = None;
    let mut trackers = Vec::new();

    for param in query.split('&') {
        let Some((key, value)) = param.split_once('=') else {
            continue;
        };
        match key {
            "xt" => {
                let hash_str = value
                    .strip_prefix("urn:btih:")
                    .context("xt is not urn:btih:")?;
                info_hash = Some(decode_info_hash(hash_str)?);
            }
            "dn" => {
                display_name = Some(urldecode(value));
            }
            "tr" => {
                trackers.push(urldecode(value));
            }
            _ => {}
        }
    }

    let info_hash = info_hash.context("magnet URI missing xt (info_hash)")?;
    Ok(MagnetLink {
        info_hash,
        display_name,
        trackers,
    })
}

fn decode_info_hash(s: &str) -> Result<[u8; 20]> {
    if s.len() == 40 {
        hex_decode(s)
    } else if s.len() == 32 {
        base32_decode(s)
    } else {
        bail!("info_hash must be 40 hex chars or 32 base32 chars, got {}", s.len());
    }
}

fn hex_decode(s: &str) -> Result<[u8; 20]> {
    let mut hash = [0u8; 20];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk)?;
        hash[i] = u8::from_str_radix(hex, 16).context("invalid hex in info_hash")?;
    }
    Ok(hash)
}

fn base32_decode(s: &str) -> Result<[u8; 20]> {
    let s = s.to_ascii_uppercase();
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut bits = 0u64;
    let mut bit_count = 0u32;
    let mut result = Vec::with_capacity(20);

    for &c in s.as_bytes() {
        let val = alphabet
            .iter()
            .position(|&a| a == c)
            .context("invalid base32 character")? as u64;
        bits = (bits << 5) | val;
        bit_count += 5;
        if bit_count >= 8 {
            bit_count -= 8;
            result.push((bits >> bit_count) as u8);
            bits &= (1 << bit_count) - 1;
        }
    }

    if result.len() != 20 {
        bail!("base32 decoded to {} bytes, expected 20", result.len());
    }
    let mut hash = [0u8; 20];
    hash.copy_from_slice(&result);
    Ok(hash)
}

fn urldecode(s: &str) -> String {
    let mut result = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(
                std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""),
                16,
            ) {
                result.push(b);
                i += 3;
                continue;
            }
        } else if bytes[i] == b'+' {
            result.push(b' ');
            i += 1;
            continue;
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&result).into_owned()
}
