use anyhow::{Context, Result, bail};
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::progress;
use crate::resume;

pub async fn download(url_str: &str, output: &Path) -> Result<()> {
    let url = url::Url::parse(url_str).context("invalid FTP URL")?;
    let host = url.host_str().context("no host in FTP URL")?;
    let port = url.port().unwrap_or(21);
    let user = if url.username().is_empty() {
        "anonymous"
    } else {
        url.username()
    };
    let pass = url.password().unwrap_or("anonymous@");
    let remote_path = url.path();

    let stream = TcpStream::connect(format!("{host}:{port}"))
        .await
        .context("failed to connect to FTP server")?;
    let (rd, wr) = tokio::io::split(stream);
    let mut rd = BufReader::new(rd);
    let mut wr = wr;

    read_response(&mut rd, &[220]).await?;

    send(&mut wr, &format!("USER {user}")).await?;
    let code = read_response(&mut rd, &[230, 331]).await?;
    if code == 331 {
        send(&mut wr, &format!("PASS {pass}")).await?;
        read_response(&mut rd, &[230]).await?;
    }

    send(&mut wr, "TYPE I").await?;
    read_response(&mut rd, &[200]).await?;

    send(&mut wr, &format!("SIZE {remote_path}")).await?;
    let size = read_size_response(&mut rd).await;

    let resume_from = match (&size, resume::load(output)) {
        (Some(total), Some(part)) if part.url == url_str && part.total_size == *total => {
            let downloaded = part.segments.first().map(|s| s.downloaded).unwrap_or(0);
            if downloaded > 0 && downloaded < *total {
                println!(
                    "  resuming from {}",
                    format_size(downloaded)
                );
                Some(downloaded)
            } else {
                None
            }
        }
        _ => None,
    };

    if let Some(offset) = resume_from {
        send(&mut wr, &format!("REST {offset}")).await?;
        read_response(&mut rd, &[350]).await?;
    }

    send(&mut wr, "PASV").await?;
    let data_addr = read_pasv_response(&mut rd, host).await?;

    let data_stream = TcpStream::connect(&data_addr)
        .await
        .with_context(|| format!("failed to connect to FTP data address {data_addr}"))?;

    send(&mut wr, &format!("RETR {remote_path}")).await?;
    read_response(&mut rd, &[125, 150]).await?;

    let total = size.unwrap_or(0);
    let offset = resume_from.unwrap_or(0);
    let pb = progress::create(size);
    pb.inc(offset);

    let mut data_reader = data_stream;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(resume_from.is_none())
        .open(output)
        .await?;

    if let Some(off) = resume_from {
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(off)).await?;
    }

    if let Some(t) = size
        && resume_from.is_none()
    {
        let part = resume::PartFile {
            url: url_str.to_string(),
            total_size: t,
            segments: vec![resume::Segment {
                start: 0,
                end: t - 1,
                downloaded: 0,
            }],
        };
        resume::save(output, &part)?;
    }

    let mut buf = vec![0u8; 65536];
    let mut downloaded = offset;

    loop {
        let n = data_reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n]).await?;
        downloaded += n as u64;
        pb.inc(n as u64);

        if total > 0 && downloaded % (256 * 1024) < n as u64 {
            let part = resume::PartFile {
                url: url_str.to_string(),
                total_size: total,
                segments: vec![resume::Segment {
                    start: 0,
                    end: total - 1,
                    downloaded,
                }],
            };
            resume::save(output, &part)?;
        }
    }

    file.flush().await?;

    read_response(&mut rd, &[226]).await?;

    send(&mut wr, "QUIT").await?;
    let _ = read_response(&mut rd, &[221]).await;

    resume::remove(output);
    pb.finish_with_message("done");
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

async fn send(
    wr: &mut tokio::io::WriteHalf<TcpStream>,
    cmd: &str,
) -> Result<()> {
    wr.write_all(format!("{cmd}\r\n").as_bytes()).await?;
    wr.flush().await?;
    Ok(())
}

async fn read_response(
    rd: &mut BufReader<tokio::io::ReadHalf<TcpStream>>,
    expected: &[u16],
) -> Result<u16> {
    loop {
        let mut line = String::new();
        rd.read_line(&mut line).await?;
        if line.len() < 4 {
            bail!("invalid FTP response: {}", line.trim());
        }
        let code: u16 = line[..3]
            .parse()
            .context("invalid FTP response code")?;
        if line.as_bytes()[3] == b' ' {
            if !expected.contains(&code) {
                bail!("FTP error {code}: {}", line[4..].trim());
            }
            return Ok(code);
        }
    }
}

async fn read_size_response(
    rd: &mut BufReader<tokio::io::ReadHalf<TcpStream>>,
) -> Option<u64> {
    let mut line = String::new();
    rd.read_line(&mut line).await.ok()?;
    if let Some(size_str) = line.strip_prefix("213 ") {
        size_str.trim().parse().ok()
    } else {
        None
    }
}

async fn read_pasv_response(
    rd: &mut BufReader<tokio::io::ReadHalf<TcpStream>>,
    control_host: &str,
) -> Result<String> {
    let mut line = String::new();
    loop {
        line.clear();
        rd.read_line(&mut line).await?;
        if line.len() >= 4 && line.as_bytes()[3] == b' ' {
            break;
        }
    }

    if !line.starts_with("227 ") {
        bail!("PASV failed: {}", line.trim());
    }

    let start = line.find('(').context("invalid PASV response")?;
    let end = line.find(')').context("invalid PASV response")?;
    let nums: Vec<u16> = line[start + 1..end]
        .split(',')
        .map(|s| s.trim().parse::<u16>())
        .collect::<Result<Vec<_>, _>>()
        .context("invalid PASV response")?;

    if nums.len() != 6 {
        bail!("invalid PASV response: expected 6 numbers, got {}", nums.len());
    }

    let ip = format!("{}.{}.{}.{}", nums[0], nums[1], nums[2], nums[3]);
    let port = nums[4] * 256 + nums[5];

    let host = if ip == "0.0.0.0"
        || ip.starts_with("127.")
        || ip.starts_with("10.")
        || ip.starts_with("192.168.")
        || ip.starts_with("172.")
    {
        control_host.to_string()
    } else {
        ip
    };

    Ok(format!("{host}:{port}"))
}
