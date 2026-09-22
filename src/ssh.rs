use anyhow::{Context, Result, bail};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;

struct SshHandler;

impl russh::client::Handler for SshHandler {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        _key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

pub struct SshTarget {
    pub user: String,
    pub password: Option<String>,
    pub host: String,
    pub port: u16,
    pub path: String,
}

impl SshTarget {
    pub fn filename(&self) -> &str {
        self.path
            .rsplit('/')
            .find(|s| !s.is_empty())
            .unwrap_or("download")
    }

    pub fn display_url(&self) -> String {
        if let Some(ref _pass) = self.password {
            format!("sftp://{}:***@{}:{}{}", self.user, self.host, self.port, self.path)
        } else {
            format!("sftp://{}@{}:{}{}", self.user, self.host, self.port, self.path)
        }
    }
}

pub fn is_ssh_target(s: &str) -> bool {
    if s.starts_with("sftp://") || s.starts_with("scp://") {
        return true;
    }
    if let Some(at_pos) = s.find('@') {
        let user_part = &s[..at_pos];
        let host_part = &s[at_pos + 1..];
        !user_part.contains('/')
            && !user_part.contains(' ')
            && !host_part.is_empty()
            && (host_part.contains(':') || host_part.contains('/'))
    } else {
        false
    }
}

pub fn parse(s: &str) -> Result<SshTarget> {
    if s.starts_with("sftp://") || s.starts_with("scp://") {
        let url = url::Url::parse(s)?;
        let user = if url.username().is_empty() {
            std::env::var("USER").unwrap_or_else(|_| "root".to_string())
        } else {
            url.username().to_string()
        };
        let password = url.password().map(|p| p.to_string());
        let host = url.host_str().context("missing host")?.to_string();
        let port = url.port().unwrap_or(22);
        let path = url.path().to_string();
        if path.is_empty() || path == "/" {
            bail!("missing remote file path");
        }
        return Ok(SshTarget {
            user,
            password,
            host,
            port,
            path,
        });
    }

    let at_pos = s.find('@').context("not an SSH target")?;
    let user = s[..at_pos].to_string();
    let rest = &s[at_pos + 1..];

    let (host, path) = if let Some(colon_pos) = rest.find(':') {
        (&rest[..colon_pos], rest[colon_pos + 1..].to_string())
    } else if let Some(slash_pos) = rest.find('/') {
        (&rest[..slash_pos], rest[slash_pos..].to_string())
    } else {
        bail!("missing remote file path");
    };

    let path = if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    };

    if host.is_empty() {
        bail!("missing host");
    }

    Ok(SshTarget {
        user,
        password: None,
        host: host.to_string(),
        port: 22,
        path,
    })
}

pub async fn download(target: &SshTarget, output: &Path) -> Result<()> {
    let config = Arc::new(russh::client::Config::default());

    let mut session = tokio::time::timeout(
        Duration::from_secs(15),
        russh::client::connect(config, (&*target.host, target.port), SshHandler),
    )
    .await
    .map_err(|_| anyhow::anyhow!("SSH connect timeout"))?
    .context("SSH connection failed")?;

    if !try_auth(&mut session, &target.user, target.password.as_deref()).await? {
        bail!(
            "SSH authentication failed for {}@{}\n  hint: set up ssh-agent, or use sftp://user:pass@host/path",
            target.user,
            target.host
        );
    }

    let channel = session.channel_open_session().await?;
    channel.request_subsystem(true, "sftp").await?;
    let sftp = russh_sftp::client::SftpSession::new(channel.into_stream()).await?;

    let metadata = sftp
        .metadata(&target.path)
        .await
        .with_context(|| format!("remote file not found: {}", target.path))?;
    let total_size = metadata.size.unwrap_or(0);

    println!("  size: {}", format_size(total_size));

    let mut resume_offset = 0u64;
    if output.exists() {
        let local_size = tokio::fs::metadata(output).await?.len();
        if local_size > 0 && local_size < total_size {
            resume_offset = local_size;
            println!("  resuming from {}", format_size(resume_offset));
        }
    }

    let mut remote_file = sftp.open(&target.path).await?;

    if resume_offset > 0 {
        use tokio::io::AsyncSeekExt;
        remote_file
            .seek(std::io::SeekFrom::Start(resume_offset))
            .await?;
    }

    if let Some(parent) = output.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut local_file = if resume_offset > 0 {
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(output)
            .await?
    } else {
        tokio::fs::File::create(output).await?
    };

    let pb = crate::progress::create(Some(total_size));
    pb.inc(resume_offset);

    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = remote_file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        tokio::io::AsyncWriteExt::write_all(&mut local_file, &buf[..n]).await?;
        pb.inc(n as u64);
    }

    pb.finish_with_message("done");
    remote_file.close().await.ok();
    sftp.close().await.ok();

    Ok(())
}

async fn try_auth(
    session: &mut russh::client::Handle<SshHandler>,
    user: &str,
    password: Option<&str>,
) -> Result<bool> {
    // 1. SSH agent
    if let Ok(mut agent) = russh::keys::agent::client::AgentClient::connect_env().await {
        if let Ok(identities) = agent.request_identities().await {
            for identity in &identities {
                let hash_alg = session.best_supported_rsa_hash().await?.flatten();
                let pubkey = identity.public_key().clone().into_owned();
                if let Ok(result) = session
                    .authenticate_publickey_with(user, pubkey, hash_alg, &mut agent)
                    .await
                {
                    if result.success() {
                        return Ok(true);
                    }
                }
            }
        }
    }

    // 2. Key files
    let home = dirs::home_dir().unwrap_or_default();
    for name in ["id_ed25519", "id_rsa", "id_ecdsa"] {
        let path = home.join(".ssh").join(name);
        if let Ok(key_data) = std::fs::read_to_string(&path) {
            if let Ok(key) = russh::keys::decode_secret_key(&key_data, None) {
                let hash_alg = session.best_supported_rsa_hash().await?.flatten();
                let wrapped = russh::keys::PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg);
                if let Ok(result) = session.authenticate_publickey(user, wrapped).await {
                    if result.success() {
                        return Ok(true);
                    }
                }
            }
        }
    }

    // 3. Password from URL
    if let Some(pass) = password {
        if let Ok(result) = session.authenticate_password(user, pass).await {
            if result.success() {
                return Ok(true);
            }
        }
    }

    Ok(false)
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
