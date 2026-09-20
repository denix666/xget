use anyhow::{Context, Result, bail};
use futures::StreamExt;
use indicatif::ProgressBar;
use reqwest::Client;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::progress;
use crate::resume;

const MIN_SEGMENT_SIZE: u64 = 1_048_576;

pub async fn download(client: &Client, url: &str, output: &Path, threads: usize) -> Result<()> {
    let resp = client.head(url).send().await;

    let (content_length, accepts_ranges) = match resp {
        Ok(r) if r.status().is_success() => {
            let len = r
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|&v| v > 0);
            let ranges = r
                .headers()
                .get("accept-ranges")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v == "bytes");
            (len, ranges)
        }
        _ => (None, false),
    };

    if let Some(total) = content_length
        && accepts_ranges && threads > 1 && total >= MIN_SEGMENT_SIZE * 2
    {
        let effective = threads.min((total / MIN_SEGMENT_SIZE) as usize).max(1);
        return segmented_download(client, url, output, effective, threads, total).await;
    }

    let pb = progress::create(content_length);
    let result = simple_download(client, url, output, &pb).await;
    if result.is_ok() {
        pb.finish_with_message("done");
    } else {
        pb.abandon_with_message("failed");
    }
    result
}

async fn simple_download(
    client: &Client,
    url: &str,
    output: &Path,
    pb: &ProgressBar,
) -> Result<()> {
    let resp = client
        .get(url)
        .send()
        .await?
        .error_for_status()
        .context("server returned an error")?;

    if let Some(len) = resp.content_length() {
        pb.set_length(len);
    }

    let mut stream = resp.bytes_stream();
    let mut file = tokio::fs::File::create(output).await?;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        pb.inc(chunk.len() as u64);
    }

    file.flush().await?;
    resume::remove(output);
    Ok(())
}

async fn segmented_download(
    client: &Client,
    url: &str,
    output: &Path,
    effective_threads: usize,
    requested_threads: usize,
    total: u64,
) -> Result<()> {
    let (part, already_downloaded) = match resume::load(output) {
        Some(existing) if existing.url == url && existing.total_size == total => {
            let done: u64 = existing.segments.iter().map(|s| s.downloaded).sum();
            (existing, done)
        }
        _ => {
            let segments = resume::create_segments(total, effective_threads);
            let part = resume::PartFile {
                url: url.to_string(),
                total_size: total,
                segments,
            };
            (part, 0)
        }
    };

    let remaining_segments: Vec<usize> = part
        .segments
        .iter()
        .enumerate()
        .filter(|(_, s)| !s.is_done())
        .map(|(i, _)| i)
        .collect();

    if remaining_segments.is_empty() {
        resume::remove(output);
        println!("  already complete");
        return Ok(());
    }

    let seg_count = part.segments.len();
    if already_downloaded > 0 {
        let done_count = seg_count - remaining_segments.len();
        println!(
            "  resuming: {done_count}/{seg_count} segments done, {} downloaded",
            format_size(already_downloaded)
        );
    } else if effective_threads < requested_threads {
        println!("  segments: {effective_threads} (adjusted from {requested_threads}, file too small)");
    } else {
        println!("  segments: {effective_threads}");
    }

    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(output)
        .await?;
    file.set_len(total).await?;
    drop(file);

    let shared: resume::SharedPart = Arc::new(Mutex::new(part));
    resume::save(output, &shared.lock().unwrap())?;

    let pb = progress::create(Some(total));
    pb.inc(already_downloaded);

    let mut handles = Vec::with_capacity(remaining_segments.len());

    for seg_idx in remaining_segments {
        let seg = shared.lock().unwrap().segments[seg_idx].clone();
        let client = client.clone();
        let url = url.to_string();
        let output = output.to_path_buf();
        let pb = pb.clone();
        let shared = shared.clone();

        handles.push(tokio::spawn(async move {
            download_segment(&client, &url, &output, seg_idx, &seg, &shared, &pb).await
        }));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        handle
            .await?
            .with_context(|| format!("segment {i} failed"))?;
    }

    resume::remove(output);
    pb.finish_with_message("done");
    Ok(())
}

async fn download_segment(
    client: &Client,
    url: &str,
    output: &Path,
    seg_index: usize,
    seg: &resume::Segment,
    shared: &resume::SharedPart,
    pb: &ProgressBar,
) -> Result<()> {
    let from = seg.resume_offset();
    let to = seg.end;

    if from > to {
        return Ok(());
    }

    let resp = client
        .get(url)
        .header("Range", format!("bytes={from}-{to}"))
        .send()
        .await?;

    if resp.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        bail!(
            "expected 206 Partial Content, got {}",
            resp.status()
        );
    }

    let mut stream = resp.bytes_stream();
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(output)
        .await?;

    file.seek(std::io::SeekFrom::Start(from)).await?;

    let mut written = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        let len = chunk.len() as u64;
        written += len;
        pb.inc(len);
        if written % (256 * 1024) < len {
            resume::mark_progress(shared, seg_index, written, output);
            written = 0;
        }
    }

    file.flush().await?;
    resume::mark_segment_done(shared, seg_index, output);
    Ok(())
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
