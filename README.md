# dwd-alda

Follow new files on [DWD opendata](https://opendata.dwd.de/) as they appear.

DWD appends a line to `alda.log` for every file it publishes. `dwd-alda` tails that log over HTTP Range requests, so you see new files within ~2 s, without listing directories or guessing file names.

Thanks to Holger Kiehl at DWD for pointing us to `alda.log` and suggesting this way of pulling. Questions about the data itself go to [opendata@dwd.de](mailto:opendata@dwd.de).

## Why

Getting new files from opendata quickly is surprisingly awkward:

- `content.log.bz2` is several MB, compressed so it can't be read partially, and lags a few minutes behind.
- Directory listings are current but large (~1 MB each for radar sweeps), and every product and site has its own directory.
- Guessing file names from the pattern causes lots of 404s, and `LATEST` files don't exist everywhere.

`alda.log` lists every file the moment it is placed. Tailing it costs one small request per second for the whole server, gives exact file names, and nothing has to be guessed or re-listed.

## Install

```sh
cargo install --path .
```

## Usage

```sh
# all single-site radar volume sweeps
dwd-alda -m '^radar/sites/sweep_vol_'

# reflectivity for two sites, downloaded, as JSON lines
dwd-alda -m '^radar/sites/sweep_vol_z/(boo|umd)/' -d data -o json

# radar products except the sites tree, as URLs
dwd-alda -m '^radar/' -x '^radar/sites/' -o url

# everything ICON-D2 published today, then exit
dwd-alda -m '^nwp/icon-d2/' --from-start --once
```

Paths are matched relative to `--base` (default `https://opendata.dwd.de/weather/`). Results go to stdout, logs to stderr.

| Flag | |
|---|---|
| `-m, --match REGEX` | include (repeatable) |
| `-x, --exclude REGEX` | exclude (repeatable) |
| `-o, --output` | `path`, `url`, `tsv` or `json` |
| `-d, --download DIR` | save files, mirroring server paths |
| `--state FILE` | resume after restarts without gaps |
| `--backfill SIZE` / `--from-start` | replay part or all of today's log first |
| `--once` | exit after catching up |
| `--poll` | poll interval, default `1s` |

See `dwd-alda --help` for the rest.

## Library

```rust
use dwd_alda::{Downloader, Filter, Tailer, TailerConfig};

let client = reqwest::Client::new();
let mut tailer = Tailer::connect(client.clone(), TailerConfig::default()).await?;
tailer.set_filter(Filter::new([r"^radar/sites/sweep_vol_z/"], [] as [&str; 0])?);
let dl = Downloader::new(client, tailer.base(), 3);

loop {
    for entry in tailer.poll().await? {
        let (bytes, _) = dl.fetch(&entry.path).await?;
        println!("{} {} bytes", entry.path, bytes.len());
    }
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
```

`examples/in_memory.rs` has a complete version.

## How it works

Each poll requests `bytes=<offset-1>-`. The overlapping byte confirms the file still continues where we left off, so a single request both detects growth and fetches it. The log rotates daily around 00:00 UTC; the rest of the old log is read from `alda.log.1` before switching, so no lines are lost.

opendata runs on two separate servers, and on failover the IP moves to the other one, whose log has the same files in a different order. The header line (`node=`, `inode=`) is checked every 10 s; after a switch the new log is replayed from 15 minutes before the last line seen, and files already emitted are skipped.

## License

MIT
