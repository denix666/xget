use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize)]
pub struct PartFile {
    pub url: String,
    pub total_size: u64,
    pub segments: Vec<Segment>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Segment {
    pub start: u64,
    pub end: u64,
    pub downloaded: u64,
}

impl Segment {
    pub fn is_done(&self) -> bool {
        self.downloaded > self.end - self.start
    }

    pub fn resume_offset(&self) -> u64 {
        self.start + self.downloaded
    }
}

pub fn part_path(output: &Path) -> PathBuf {
    let mut p = output.as_os_str().to_owned();
    p.push(".get.part");
    PathBuf::from(p)
}

pub fn load(output: &Path) -> Option<PartFile> {
    let path = part_path(output);
    let data = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

pub fn save(output: &Path, part: &PartFile) -> Result<()> {
    let path = part_path(output);
    let data = serde_json::to_string(part)?;
    std::fs::write(&path, data).context("failed to write .get.part file")?;
    Ok(())
}

pub fn remove(output: &Path) {
    let _ = std::fs::remove_file(part_path(output));
}

pub fn create_segments(total: u64, threads: usize) -> Vec<Segment> {
    let segment_size = total / threads as u64;
    (0..threads)
        .map(|i| {
            let start = i as u64 * segment_size;
            let end = if i == threads - 1 {
                total - 1
            } else {
                (i as u64 + 1) * segment_size - 1
            };
            Segment {
                start,
                end,
                downloaded: 0,
            }
        })
        .collect()
}

pub type SharedPart = Arc<Mutex<PartFile>>;

pub fn mark_progress(shared: &SharedPart, seg_index: usize, bytes: u64, output: &Path) {
    let mut part = shared.lock().unwrap();
    part.segments[seg_index].downloaded += bytes;
    let _ = save(output, &part);
}

pub fn mark_segment_done(shared: &SharedPart, seg_index: usize, output: &Path) {
    let mut part = shared.lock().unwrap();
    let seg = &mut part.segments[seg_index];
    seg.downloaded = seg.end - seg.start + 1;
    let _ = save(output, &part);
}
