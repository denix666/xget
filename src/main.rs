mod cli;
mod completions;
mod ftp;
mod history;
mod http;
mod progress;
mod resume;
mod torrent;

use anyhow::{Result, bail};
use clap::Parser;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let args = cli::Args::parse();

    if args.history_urls {
        for url in history::unique_urls()? {
            println!("{url}");
        }
        return Ok(());
    }

    if args.clear_history {
        history::clear()?;
        println!("History cleared.");
        return Ok(());
    }

    if args.history {
        let entries = history::load()?;
        history::display(&entries);
        return Ok(());
    }

    if let Some(ref shell) = args.completions {
        return completions::generate(shell);
    }

    let urls = collect_urls(&args)?;
    let is_batch = urls.len() > 1;

    let client = reqwest::Client::builder()
        .user_agent(format!("xget/{}", env!("CARGO_PKG_VERSION")))
        .build()?;

    for url in &urls {
        let (result, hist_output, hist_size) = if is_local_torrent(url) {
            let output_dir = args.output.clone().unwrap_or_else(default_download_dir);
            println!("{url}");
            let r = torrent::download(std::path::Path::new(url), &output_dir, args.threads).await;
            (r, output_dir.display().to_string(), None)
        } else {
            let output = resolve_output(url, &args, is_batch)?;
            if let Some(parent) = output.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            println!("{url}");
            println!("  -> {}", output.display());

            let r = if url.starts_with("ftp://") {
                ftp::download(url, &output).await
            } else {
                http::download(&client, url, &output, args.threads).await
            };

            let size = if r.is_ok() {
                tokio::fs::metadata(&output).await.ok().map(|m| m.len())
            } else {
                None
            };
            (r, output.display().to_string(), size)
        };

        let status = if result.is_ok() {
            "completed"
        } else {
            "failed"
        };
        let _ = history::save(&history::Entry {
            url: url.clone(),
            output: hist_output,
            size: hist_size,
            timestamp: chrono::Local::now().format("%Y-%m-%d %H:%M").to_string(),
            status: status.to_string(),
        });

        result?;
        println!();
    }

    Ok(())
}

fn collect_urls(args: &cli::Args) -> Result<Vec<String>> {
    let mut urls = Vec::new();

    if let Some(ref input) = args.input {
        let content = std::fs::read_to_string(input)?;
        for line in content.lines() {
            let line = line.trim();
            if !line.is_empty() && !line.starts_with('#') {
                urls.push(line.to_string());
            }
        }
    }

    if let Some(ref url) = args.reget {
        urls.push(url.clone());
    } else if let Some(ref url) = args.url {
        urls.push(url.clone());
    }

    if urls.is_empty() {
        bail!("No URLs specified. Provide a URL or use -i <file>");
    }

    Ok(urls)
}

fn resolve_output(url: &str, args: &cli::Args, is_batch: bool) -> Result<PathBuf> {
    let filename = filename_from_url(url);

    if let Some(ref output) = args.output {
        if is_batch || output.is_dir() || output.to_string_lossy().ends_with('/') {
            return Ok(output.join(&filename));
        }
        return Ok(output.clone());
    }

    Ok(default_download_dir().join(&filename))
}

fn default_download_dir() -> PathBuf {
    dirs::download_dir()
        .or_else(|| dirs::home_dir().map(|h| h.join("Downloads")))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn filename_from_url(url_str: &str) -> String {
    url::Url::parse(url_str)
        .ok()
        .and_then(|u| {
            u.path_segments()?
                .rev()
                .find(|s| !s.is_empty())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "download".to_string())
}

fn is_local_torrent(path: &str) -> bool {
    path.ends_with(".torrent") && std::path::Path::new(path).is_file()
}
