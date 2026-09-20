use anyhow::{Context, Result, bail};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<String, Value>),
}

impl Value {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Dict(d) => Some(d),
            _ => None,
        }
    }
}

pub fn decode(data: &[u8]) -> Result<Value> {
    let (value, _) = decode_value(data, 0)?;
    Ok(value)
}

pub fn decode_at(data: &[u8], pos: usize) -> Result<(Value, usize)> {
    decode_value(data, pos)
}

pub fn find_dict_value_raw<'a>(data: &'a [u8], key: &str) -> Result<&'a [u8]> {
    if data.is_empty() || data[0] != b'd' {
        bail!("not a bencoded dictionary");
    }
    let mut pos = 1;
    while pos < data.len() && data[pos] != b'e' {
        let (key_val, key_end) = decode_value(data, pos)?;
        let k = match &key_val {
            Value::Bytes(b) => std::str::from_utf8(b).unwrap_or(""),
            _ => "",
        };
        let value_start = key_end;
        let (_, value_end) = decode_value(data, value_start)?;
        if k == key {
            return Ok(&data[value_start..value_end]);
        }
        pos = value_end;
    }
    bail!("key '{key}' not found in dict");
}

fn decode_value(data: &[u8], pos: usize) -> Result<(Value, usize)> {
    if pos >= data.len() {
        bail!("unexpected end of bencode data");
    }
    match data[pos] {
        b'i' => decode_int(data, pos),
        b'l' => decode_list(data, pos),
        b'd' => decode_dict(data, pos),
        b'0'..=b'9' => decode_bytes(data, pos),
        c => bail!("unexpected byte 0x{c:02X} at position {pos}"),
    }
}

fn decode_int(data: &[u8], pos: usize) -> Result<(Value, usize)> {
    let end = data[pos + 1..]
        .iter()
        .position(|&b| b == b'e')
        .context("unterminated integer")?;
    let s = std::str::from_utf8(&data[pos + 1..pos + 1 + end])?;
    let i: i64 = s.parse().context("invalid integer")?;
    Ok((Value::Int(i), pos + 1 + end + 1))
}

fn decode_bytes(data: &[u8], pos: usize) -> Result<(Value, usize)> {
    let colon = data[pos..]
        .iter()
        .position(|&b| b == b':')
        .context("missing ':' in byte string")?;
    let len_str = std::str::from_utf8(&data[pos..pos + colon])?;
    let len: usize = len_str.parse().context("invalid byte string length")?;
    let start = pos + colon + 1;
    if start + len > data.len() {
        bail!("byte string extends past end of data");
    }
    Ok((Value::Bytes(data[start..start + len].to_vec()), start + len))
}

fn decode_list(data: &[u8], pos: usize) -> Result<(Value, usize)> {
    let mut items = Vec::new();
    let mut p = pos + 1;
    while p < data.len() && data[p] != b'e' {
        let (val, next) = decode_value(data, p)?;
        items.push(val);
        p = next;
    }
    if p >= data.len() {
        bail!("unterminated list");
    }
    Ok((Value::List(items), p + 1))
}

fn decode_dict(data: &[u8], pos: usize) -> Result<(Value, usize)> {
    let mut map = BTreeMap::new();
    let mut p = pos + 1;
    while p < data.len() && data[p] != b'e' {
        let (key_val, key_end) = decode_bytes(data, p)?;
        let key = match key_val {
            Value::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
            _ => bail!("dict key must be a byte string"),
        };
        let (val, val_end) = decode_value(data, key_end)?;
        map.insert(key, val);
        p = val_end;
    }
    if p >= data.len() {
        bail!("unterminated dict");
    }
    Ok((Value::Dict(map), p + 1))
}

pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(value, &mut out);
    out
}

fn encode_into(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Int(i) => {
            out.push(b'i');
            out.extend_from_slice(i.to_string().as_bytes());
            out.push(b'e');
        }
        Value::Bytes(b) => {
            out.extend_from_slice(b.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(b);
        }
        Value::List(items) => {
            out.push(b'l');
            for item in items {
                encode_into(item, out);
            }
            out.push(b'e');
        }
        Value::Dict(map) => {
            out.push(b'd');
            for (key, val) in map {
                out.extend_from_slice(key.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(key.as_bytes());
                encode_into(val, out);
            }
            out.push(b'e');
        }
    }
}
