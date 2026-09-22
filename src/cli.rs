use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "xget", about = "Universal downloader", version)]
pub struct Args {
    /// URL to download
    pub url: Option<String>,

    /// Input file with list of URLs (one per line, # for comments)
    #[arg(short, long)]
    pub input: Option<PathBuf>,

    /// Output path (file or directory)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Number of download threads
    #[arg(short, long, default_value = "8")]
    pub threads: usize,

    /// Show download history
    #[arg(long)]
    pub history: bool,

    /// Clear download history
    #[arg(long)]
    pub clear_history: bool,

    /// Re-download a URL from history
    #[arg(long)]
    pub reget: Option<String>,

    /// Generate shell completions (bash, zsh, fish)
    #[arg(long, value_name = "SHELL")]
    pub completions: Option<String>,

    /// Print history URLs for shell completion
    #[arg(long, hide = true)]
    pub history_urls: bool,

    /// Enable verbose logging to ~/.local/share/xget/xget.log
    #[arg(short, long)]
    pub verbose: bool,
}
