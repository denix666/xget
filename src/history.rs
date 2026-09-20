use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
pub struct Entry {
    pub url: String,
    pub output: String,
    pub size: Option<u64>,
    pub timestamp: String,
    pub status: String,
}

fn history_path() -> Result<PathBuf> {
    let dir = dirs::data_local_dir()
        .or_else(|| dirs::home_dir().map(|h| h.join(".local/share")))
        .unwrap_or_else(|| PathBuf::from("."));
    Ok(dir.join("xget").join("history.jsonl"))
}

pub fn load() -> Result<Vec<Entry>> {
    let path = history_path()?;
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(&path)?;
    let entries = content
        .lines()
        .filter(|l| !l.is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    Ok(entries)
}

pub fn save(entry: &Entry) -> Result<()> {
    let path = history_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    writeln!(file, "{}", serde_json::to_string(entry)?)?;
    Ok(())
}

pub fn clear() -> Result<()> {
    let path = history_path()?;
    if path.exists() {
        fs::remove_file(&path)?;
    }
    Ok(())
}

pub fn unique_urls() -> Result<Vec<String>> {
    let entries = load()?;
    let mut seen = std::collections::HashSet::new();
    let mut urls = Vec::new();
    for e in entries.iter().rev() {
        if seen.insert(&e.url) {
            urls.push(e.url.clone());
        }
    }
    Ok(urls)
}

pub fn display(entries: &[Entry]) {
    if entries.is_empty() {
        println!("History is empty.");
        return;
    }
    for e in entries {
        let size_str = match e.size {
            Some(s) => format_size(s),
            None => "---".to_string(),
        };
        let status = if e.status == "completed" {
            "ok"
        } else {
            "FAIL"
        };
        println!(
            "[{}] {:>10}  {:<4}  {}",
            e.timestamp, size_str, status, e.url
        );
        println!(
            "{:>43}-> {}",
            "", e.output
        );
    }
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
