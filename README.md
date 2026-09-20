# xget

## Disclaimer
This program comes with no warranty. You must use this program at your own risk.

## Introduction
A universal CLI downloader written in Rust. Downloads files over HTTP/HTTPS, FTP, and BitTorrent — all from a single command.

## Features
- **HTTP/HTTPS** — segmented parallel downloads using Range requests
- **FTP** — async client with passive mode and NAT traversal
- **BitTorrent** — `.torrent` files and `magnet:` links
- **DHT** — peer discovery via Distributed Hash Table (BEP 5)
- **Metadata exchange** — download torrent metadata from peers (BEP 9/10)
- **Resume** — automatically resumes interrupted downloads for all protocols
- **Download history** — JSONL-based log with shell tab-completion for re-downloading
- **Batch downloads** — read URL lists from a text file
- **Shell completions** — bash, zsh, fish

## Building
Requires Rust 1.85+ (edition 2024).

```bash
cargo build --release
cp target/release/xget ~/.local/bin/
```

## Usage

```bash
# Download a file (saved to ~/Downloads by default)
xget https://example.com/file.tar.gz

# Specify output path
xget https://example.com/file.tar.gz -o /tmp/file.tar.gz

# Use 8 parallel threads
xget https://example.com/large.iso -t 8

# Download via FTP
xget ftp://ftp.example.com/pub/file.tar.gz

# Download a torrent
xget /path/to/file.torrent -o /tmp/downloads/ -t 10

# Download via magnet link
xget "magnet:?xt=urn:btih:..."

# Download multiple URLs from a file
xget -i urls.txt -o /tmp/downloads/

# View download history
xget --history

# Re-download a file from history
xget --reget https://example.com/file.tar.gz
```

### URL list file format

One URL per line. Empty lines and lines starting with `#` are ignored.

```
# Images
https://example.com/photo1.jpg
https://example.com/photo2.jpg

# Documents
https://example.com/report.pdf
```

### BitTorrent

Pass a `.torrent` file path or a `magnet:` link as the argument. The downloader operates in **outbound-only** mode:

- Announces `port=0` to trackers (no incoming connections)
- Never opens a listening socket
- DHT works in passive mode — queries other nodes but does not accept incoming requests
- Does not seed after download completes

This makes it safe to use on networks where torrent servers are not allowed.

**Peer discovery:** HTTP/HTTPS trackers, UDP trackers (BEP 15), DHT (BEP 5).

**Magnet links:** peers are found via trackers (if present in the link) and DHT. Torrent metadata is fetched from peers using the extension protocol (BEP 10) and metadata exchange (BEP 9).

## Options

| Option | Short | Description |
|---|---|---|
| `URL` | | URL to download (positional argument) |
| `--output PATH` | `-o` | Output file or directory |
| `--threads N` | `-t` | Number of download threads (default: 8) |
| `--input FILE` | `-i` | Text file with list of URLs |
| `--history` | | Show download history |
| `--clear-history` | | Clear download history |
| `--reget URL` | | Re-download a URL from history |
| `--completions SHELL` | | Generate shell completion script |
| `--help` | `-h` | Show help |
| `--version` | `-V` | Show version |

### Defaults

- Output directory: `~/Downloads`
- Filename: parsed from URL
- Threads: 8

## Shell Completions

`xget` supports tab-completion for bash, zsh, and fish. Completions provide:

- Flag and option names (`--output`, `--threads`, etc.)
- Thread count suggestions (1, 2, 4, 8, 16)
- File path completion for `--output` and `--input`
- **Dynamic URL completion** for `--reget` — suggests URLs from your download history

### Setup

#### Bash

Generate the script and source it from your `.bashrc`:

```bash
xget --completions bash > ~/.local/share/bash-completion/completions/xget
```

Or, if the directory doesn't exist:

```bash
mkdir -p ~/.local/share/bash-completion/completions
xget --completions bash > ~/.local/share/bash-completion/completions/xget
```

The completion will load automatically in new shells. To activate immediately:

```bash
source ~/.local/share/bash-completion/completions/xget
```

#### Zsh

Generate the script and place it in your `fpath`:

```bash
mkdir -p ~/.zfunc
xget --completions zsh > ~/.zfunc/_xget
```

Make sure `~/.zfunc` is in your `fpath`. Add this to `~/.zshrc` **before** `compinit`:

```zsh
fpath=(~/.zfunc $fpath)
autoload -Uz compinit && compinit
```

Then restart your shell or run:

```bash
source ~/.zshrc
```

#### Fish

Generate the script directly into fish's completions directory:

```bash
xget --completions fish > ~/.config/fish/completions/xget.fish
```

Fish picks it up automatically — no restart needed.

### Using `--reget` completion

Once completions are installed, type `xget --reget ` and press `Tab`. The shell will suggest URLs from your download history:

```
$ xget --reget <Tab>
https://example.com/file.tar.gz    https://example.com/image.iso
ftp://mirror.example.com/data.bin
```

Select the desired URL and press `Enter` to re-download it.

## Resume Support

If a download is interrupted (network failure, Ctrl+C, etc.), simply run the same command again — `xget` will pick up where it left off.

### HTTP/HTTPS

Segmented downloads track progress in a `.get.part` file next to the output file. On resume, completed segments are skipped and partially downloaded segments continue from the last saved position.

```
$ xget https://example.com/large.iso -o /tmp/large.iso
  segments: 8
  [████████████████████░░░░░░░░░░░░░░░░░░░░] 500.0/1000.0 MiB ...
^C

$ xget https://example.com/large.iso -o /tmp/large.iso
  resuming: 5/8 segments done, 625.0 MB downloaded
  [████████████████████████████████████░░░░░] 875.0/1000.0 MiB ...
```

The `.get.part` file is automatically deleted once the download completes.

### FTP

Uses the FTP `REST` (restart) command to resume from the last byte written.

### BitTorrent

On restart, all existing pieces are verified against their SHA1 hashes. Only pieces that are missing or corrupted are re-downloaded.

```
$ xget movie.torrent
  name: Movie
  size: 1.5 GB (6000 pieces)
  checking existing data... 4500/6000 pieces (1.1 GB) verified
  peers: 150
```

## Download History

All downloads (successful and failed) are logged to `~/.local/share/xget/history.jsonl`.

View the history:

```
$ xget --history
[2026-09-20 11:34]     1.0 MB  ok    https://example.com/file.dat
                                           -> /home/user/Downloads/file.dat
[2026-09-20 11:35]        ---  FAIL  https://example.com/broken.zip
                                           -> /home/user/Downloads/broken.zip
```

Clear the history:

```
$ xget --clear-history
History cleared.
```

## License

MIT
