//! Embedding dwd-alda in a service: new sweeps are fetched into memory and handed
//! to `process`, nothing touches the disk.
//!
//!     cargo run --release --example in_memory

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dwd_alda::{Downloader, Entry, Error, Filter, Start, Tailer, TailerConfig};
use tokio::sync::Semaphore;

async fn process(entry: Entry, data: Bytes) {
    match entry.sweep() {
        Some(s) => println!("{} {} {} sweep {:02}: {} bytes", s.site, s.product, s.moment, s.index, data.len()),
        None => println!("{}: {} bytes", entry.path, data.len()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
    let client = reqwest::Client::builder().pool_max_idle_per_host(16).build()?;
    let mut tailer = Tailer::connect(client.clone(), TailerConfig { start: Start::End, ..Default::default() }).await?;
    tailer.set_filter(Filter::new([r"^radar/sites/sweep_vol_"], [r"/filter_simple/"])?);
    let dl = Arc::new(Downloader::new(client, tailer.base(), 3));
    // Bounds parallel downloads; polling never waits for them.
    let permits = Arc::new(Semaphore::new(8));

    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tick.tick().await;
        let entries = match tailer.poll().await {
            Ok(v) => v,
            Err(e) => {
                log::warn!("poll: {e}");
                continue;
            }
        };
        for entry in entries {
            let (dl, permits) = (dl.clone(), permits.clone());
            tokio::spawn(async move {
                let _permit = permits.acquire_owned().await;
                match dl.fetch(&entry.path).await {
                    Ok((data, _retries)) => process(entry, data).await,
                    Err(e) => log::warn!("{}: {e}", entry.path),
                }
            });
        }
    }
}
