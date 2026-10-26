use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use log::{info, warn};
use serde_json::json;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

use dwd_alda::{DEFAULT_BASE, Downloader, Entry, Error, Filter, Start, State, Tailer, TailerConfig, unix_secs};

/// Follow DWD opendata alda.log and output (and optionally download) newly
/// published files whose path matches the given regexes.
///
/// Paths are relative to --base, e.g.
/// radar/sites/sweep_vol_z/boo/hdf5/filter_simple/ras07-stqual-vol5minng01_sweeph5onem_dbzh_00-2026102200005800-boo-10132-hd5
#[derive(Parser, Debug)]
#[command(version, after_help = EXAMPLES)]
struct Args {
    /// Include paths matching REGEX (repeatable; default: everything).
    #[arg(short = 'm', long = "match", value_name = "REGEX")]
    include: Vec<String>,

    /// Exclude paths matching REGEX (repeatable).
    #[arg(short = 'x', long = "exclude", value_name = "REGEX")]
    exclude: Vec<String>,

    /// Output format on stdout, one line per file.
    #[arg(short, long, value_enum, default_value_t = Format::Path)]
    output: Format,

    /// Download matching files into DIR (server paths mirrored). Lines are
    /// printed once the file is on disk.
    #[arg(short, long, value_name = "DIR")]
    download: Option<PathBuf>,

    /// Parallel downloads.
    #[arg(short, long, default_value_t = 8)]
    jobs: usize,

    /// Download retries per file.
    #[arg(long, default_value_t = 3)]
    retries: u32,

    /// alda.log poll interval.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "1s")]
    poll: Duration,

    /// Directory containing alda.log.
    #[arg(long, default_value = DEFAULT_BASE)]
    base: String,

    /// Persist the alda.log position here and resume from it on restart.
    #[arg(long, value_name = "FILE")]
    state: Option<PathBuf>,

    /// Replay this much of alda.log before following (e.g. 500K, 20M).
    #[arg(long, value_parser = parse_size, conflicts_with = "from_start")]
    backfill: Option<u64>,

    /// Read the whole current alda.log (since ~00:00 UTC) before following.
    #[arg(long)]
    from_start: bool,

    /// Exit after catching up instead of following.
    #[arg(long)]
    once: bool,

    /// Log delay statistics to stderr at this interval (0 disables).
    #[arg(long, value_parser = humantime::parse_duration, default_value = "1m")]
    stats: Duration,
}

const EXAMPLES: &str = "\
Examples:
  # All single-site volume sweeps, print paths
  dwd-alda -m '^radar/sites/sweep_vol_'

  # Unfiltered reflectivity of boo and umd as JSON lines, downloaded
  dwd-alda -m '^radar/sites/sweep_vol_z/(boo|umd)/unfiltered/' -o json -d data

  # Everything from radar except the 'sites' tree, URLs only
  dwd-alda -m '^radar/' -x '^radar/sites/' -o url

  # What was published today so far for ICON-D2 total precipitation, then exit
  dwd-alda -m '^nwp/icon-d2/grib/\\d+/tot_prec/' --from-start --once";

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Format {
    /// Path relative to --base.
    Path,
    /// Full download URL.
    Url,
    /// Tab-separated: alda time, size, path, local file.
    Tsv,
    /// One JSON object per line with sweep metadata and delays.
    Json,
}

fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (num, mul) = match s.char_indices().last() {
        Some((i, 'k' | 'K')) => (&s[..i], 1 << 10),
        Some((i, 'm' | 'M')) => (&s[..i], 1 << 20),
        Some((i, 'g' | 'G')) => (&s[..i], 1 << 30),
        _ => (s, 1),
    };
    num.parse::<u64>().map(|n| n * mul).map_err(|e| format!("{s:?}: {e}"))
}

struct Done {
    entry: Entry,
    local: Option<PathBuf>,
    downloaded: Option<SystemTime>,
}

#[derive(Default)]
struct Stats {
    delays: BTreeMap<&'static str, Vec<f64>>,
    counters: BTreeMap<&'static str, u64>,
}

impl Stats {
    fn observe(&mut self, name: &'static str, from: SystemTime, to: SystemTime) {
        self.delays.entry(name).or_default().push(unix_secs(to) - unix_secs(from));
    }
    fn inc(&mut self, name: &'static str, n: u64) {
        *self.counters.entry(name).or_default() += n;
    }
    fn report(&mut self) {
        for (name, v) in &mut self.delays {
            if v.is_empty() {
                continue;
            }
            v.sort_by(f64::total_cmp);
            let q = |p: f64| v[((p * v.len() as f64) as usize).min(v.len() - 1)];
            info!(
                "delay {name:<9} n={:<6} p50={:6.2}s p90={:6.2}s p99={:6.2}s max={:6.2}s",
                v.len(),
                q(0.5),
                q(0.9),
                q(0.99),
                v[v.len() - 1]
            );
            v.clear();
        }
        if !self.counters.is_empty() {
            let c: Vec<String> = self.counters.iter().map(|(k, v)| format!("{k}={v}")).collect();
            info!("totals {}", c.join(" "));
        }
    }
}

fn format_line(f: Format, base: &str, d: &Done) -> String {
    let e = &d.entry;
    let local = d.local.as_ref().map(|p| p.display().to_string());
    match f {
        Format::Path => e.path.clone(),
        Format::Url => e.url(base),
        Format::Tsv => format!(
            "{}\t{}\t{}\t{}",
            humantime::format_rfc3339_seconds(e.time),
            e.size,
            e.path,
            local.unwrap_or_default()
        ),
        Format::Json => {
            let between =
                |from: SystemTime, to: SystemTime| ((unix_secs(to) - unix_secs(from)) * 1000.0).round() / 1000.0;
            let sweep = e.sweep();
            let mut delay = serde_json::Map::new();
            delay.insert("detect".into(), json!(between(e.time, e.seen)));
            if let Some(s) = &sweep {
                delay.insert("publish".into(), json!(between(s.scan, e.time)));
            }
            if let Some(done) = d.downloaded {
                delay.insert("download".into(), json!(between(e.seen, done)));
                if let Some(s) = &sweep {
                    delay.insert("total".into(), json!(between(s.scan, done)));
                }
            }
            json!({
                "time": humantime::format_rfc3339_seconds(e.time).to_string(),
                "size": e.size,
                "path": e.path,
                "url": e.url(base),
                "matched": e.matched,
                "local": local,
                "sweep": sweep.map(|s| json!({
                    "product": s.product,
                    "site": s.site,
                    "subdir": s.subdir,
                    "moment": s.moment,
                    "index": s.index,
                    "scan": humantime::format_rfc3339_seconds(s.scan).to_string(),
                })),
                "delay_s": delay,
            })
            .to_string()
        }
    }
}

fn load_state(path: &PathBuf) -> Option<State> {
    let b = std::fs::read(path).ok()?;
    serde_json::from_slice(&b).map_err(|e| warn!("ignoring unreadable state {}: {e}", path.display())).ok()
}

fn save_state(path: &PathBuf, s: &State) {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let res = std::fs::write(&tmp, serde_json::to_vec(s).unwrap()).and_then(|()| std::fs::rename(&tmp, path));
    if let Err(e) = res {
        warn!("saving state to {}: {e}", path.display());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .target(env_logger::Target::Stderr)
        .init();
    let args = Args::parse();

    let filter = Filter::new(&args.include, &args.exclude).context("invalid regex")?;
    let client = reqwest::Client::builder()
        .user_agent(concat!("dwd-alda/", env!("CARGO_PKG_VERSION")))
        .pool_max_idle_per_host(args.jobs + 2)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .build()?;

    let start = match (&args.state, args.from_start, args.backfill) {
        (_, true, _) => Start::Beginning,
        (Some(p), _, b) => match load_state(p) {
            Some(s) => Start::Resume(s),
            None => b.map_or(Start::End, Start::Backfill),
        },
        (None, _, Some(b)) => Start::Backfill(b),
        (None, _, None) => Start::End,
    };
    let mut tailer =
        Tailer::connect(client.clone(), TailerConfig { base: args.base.clone(), start, ..Default::default() }).await?;
    tailer.set_filter(filter);
    let base = tailer.base().to_owned();

    // One blocking writer so lines never interleave; a closed pipe (`| head`) just stops output.
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Done>();
    let format = args.output;
    let printer_base = base.clone();
    let printer = tokio::task::spawn_blocking(move || {
        let stdout = io::stdout();
        let mut w = io::BufWriter::new(stdout.lock());
        while let Some(d) = out_rx.blocking_recv() {
            let mut ok = writeln!(w, "{}", format_line(format, &printer_base, &d)).is_ok();
            while ok && let Ok(d) = out_rx.try_recv() {
                ok = writeln!(w, "{}", format_line(format, &printer_base, &d)).is_ok();
            }
            if !ok || w.flush().is_err() {
                return;
            }
        }
    });

    let stats = Arc::new(Mutex::new(Stats::default()));
    let downloader =
        args.download.clone().map(|dir| Arc::new((Downloader::new(client.clone(), &base, args.retries), dir)));
    let permits = Arc::new(Semaphore::new(args.jobs.max(1)));
    let mut tasks = JoinSet::new();

    let mut poll = tokio::time::interval(args.poll);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut report =
        tokio::time::interval(if args.stats.is_zero() { Duration::from_secs(365 * 86400) } else { args.stats });
    report.tick().await;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = &mut ctrl_c => break,
            _ = report.tick() => stats.lock().unwrap().report(),
            _ = poll.tick() => {
                let res = tailer.poll_with(|entry| {
                    let mut st = stats.lock().unwrap();
                    st.inc("matched", 1);
                    st.observe("detect", entry.time, entry.seen);
                    if let Some(s) = entry.sweep() {
                        st.observe("publish", s.scan, entry.time);
                    }
                    drop(st);
                    match &downloader {
                        None => {
                            let _ = out_tx.send(Done { entry, local: None, downloaded: None });
                        }
                        Some(dl) => {
                            let (dl, permits, stats, out_tx) = (dl.clone(), permits.clone(), stats.clone(), out_tx.clone());
                            tasks.spawn(async move {
                                let _permit = permits.acquire_owned().await;
                                match dl.0.fetch_to(&entry.path, &dl.1).await {
                                    Ok((local, retries)) => {
                                        let now = SystemTime::now();
                                        let mut st = stats.lock().unwrap();
                                        st.inc("downloaded", 1);
                                        st.inc("bytes", entry.size);
                                        st.inc("retries", retries.into());
                                        st.observe("download", entry.seen, now);
                                        if let Some(s) = entry.sweep() {
                                            st.observe("total", s.scan, now);
                                        }
                                        drop(st);
                                        let _ = out_tx.send(Done { entry, local: Some(local), downloaded: Some(now) });
                                    }
                                    Err(e) => {
                                        stats.lock().unwrap().inc("failed", 1);
                                        warn!("download {} failed: {e}", entry.path);
                                    }
                                }
                            });
                        }
                    }
                }).await;
                match res {
                    Ok(()) => {
                        if let Some(p) = &args.state {
                            save_state(p, tailer.state());
                        }
                        if args.once {
                            break;
                        }
                    }
                    Err(Error::Inconsistent) => warn!("{}", Error::Inconsistent),
                    Err(e) => {
                        stats.lock().unwrap().inc("poll_errors", 1);
                        warn!("poll: {e}");
                    }
                }
                while tasks.try_join_next().is_some() {}
            }
        }
    }

    while tasks.join_next().await.is_some() {}
    if let Some(p) = &args.state {
        save_state(p, tailer.state());
    }
    drop(out_tx);
    let _ = printer.await;
    if !args.stats.is_zero() {
        stats.lock().unwrap().report();
    }
    Ok(())
}
