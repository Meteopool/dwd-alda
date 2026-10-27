use std::collections::{HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::{info, warn};
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};

use crate::{Entry, Error, Filter};

pub const DEFAULT_BASE: &str = "https://opendata.dwd.de/weather/";
const LOG: &str = "alda.log";
const ROTATED: &str = "alda.log.1";
/// After a node switch, lines from this far before the last one seen are replayed.
const REPLAY_WINDOW: Duration = Duration::from_secs(15 * 60);
/// Emitted files are remembered this long so a node switch does not repeat them.
const SEEN_WINDOW: Duration = Duration::from_secs(20 * 60);
const MAX_REPLAY_BYTES: u64 = 1 << 30;
/// A same-file mismatch lasting this long is treated like a node switch.
const INCONSISTENT_GIVE_UP: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    /// opendata runs on multiple independent servers; the IP moves on failover.
    #[serde(default)]
    pub node: String,
    pub inode: String,
    /// First log line; inodes are reused across rotations, this is not.
    #[serde(default)]
    pub first: String,
    /// Bytes of complete lines consumed.
    pub offset: u64,
    pub last_byte: Option<u8>,
    /// Newest alda.log time seen (unix seconds).
    #[serde(default)]
    pub last_time: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum Start {
    End,
    /// Replay roughly this many bytes before the current end.
    Backfill(u64),
    Beginning,
    /// Falls back to `End` if the log has rotated since.
    Resume(State),
}

#[derive(Debug, Clone)]
pub struct TailerConfig {
    pub base: String,
    pub start: Start,
    pub header_check: Duration,
}

impl Default for TailerConfig {
    fn default() -> Self {
        TailerConfig { base: DEFAULT_BASE.to_owned(), start: Start::End, header_check: Duration::from_secs(10) }
    }
}

/// Follows `alda.log` with HTTP Range requests, including the daily rotation to
/// `alda.log.1` and switches between opendata nodes.
pub struct Tailer {
    client: Client,
    base: String,
    header_check: Duration,
    state: State,
    pending: Vec<u8>,
    last_header: Instant,
    filter: Filter,
    seen: Seen,
    /// While replaying after a node switch: skip lines older than this.
    replay_from: Option<u64>,
    inconsistent_since: Option<Instant>,
}

/// Identity of a log file, from its first lines.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Ident {
    node: String,
    inode: String,
    first: String,
}

impl Ident {
    fn of(s: &State) -> Ident {
        Ident { node: s.node.clone(), inode: s.inode.clone(), first: s.first.clone() }
    }

    /// An empty first line (log just rotated) matches anything.
    fn same_file(&self, other: &Ident) -> bool {
        self.node == other.node
            && self.inode == other.inode
            && (self.first.is_empty() || other.first.is_empty() || self.first == other.first)
    }
}

#[derive(Default)]
struct Seen {
    order: VecDeque<(u64, u64)>, // (alda time, path hash)
    set: HashSet<u64>,
}

impl Seen {
    fn insert(&mut self, time: u64, path: &str) {
        let h = hash(path);
        if self.set.insert(h) {
            self.order.push_back((time, h));
        }
        let cutoff = time.saturating_sub(SEEN_WINDOW.as_secs());
        while self.order.front().is_some_and(|&(t, _)| t < cutoff) {
            let (_, h) = self.order.pop_front().unwrap();
            self.set.remove(&h);
        }
    }

    fn contains(&self, path: &str) -> bool {
        self.set.contains(&hash(path))
    }
}

fn hash(s: &str) -> u64 {
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

enum ReadError {
    Mismatch,
    Other(Error),
}

impl<E: Into<Error>> From<E> for ReadError {
    fn from(e: E) -> Self {
        ReadError::Other(e.into())
    }
}

impl Tailer {
    pub async fn connect(client: Client, cfg: TailerConfig) -> Result<Tailer, Error> {
        let mut base = cfg.base;
        if !base.ends_with('/') {
            base.push('/');
        }
        let mut t = Tailer {
            client,
            base,
            header_check: cfg.header_check,
            state: State {
                node: String::new(),
                inode: String::new(),
                first: String::new(),
                offset: 0,
                last_byte: None,
                last_time: None,
            },
            pending: Vec::new(),
            last_header: Instant::now(),
            filter: Filter::default(),
            seen: Seen::default(),
            replay_from: None,
            inconsistent_since: None,
        };
        let id = t.header(LOG).await?;
        let fresh = |offset| State {
            node: id.node.clone(),
            inode: id.inode.clone(),
            first: id.first.clone(),
            offset,
            last_byte: None,
            last_time: None,
        };
        t.state = match &cfg.start {
            Start::Resume(s) if Ident::of(s).same_file(&id) => State { first: id.first.clone(), ..s.clone() },
            // Same day on the other node: replay from shortly before where we stopped (may repeat lines).
            Start::Resume(s) if s.node != id.node && s.last_time.is_some() => {
                warn!(
                    "saved state is from node {}, now on {}; replaying from before the last line seen",
                    s.node, id.node
                );
                let size = t.size(LOG).await?;
                let from = s.last_time.unwrap().saturating_sub(REPLAY_WINDOW.as_secs());
                State { last_time: s.last_time, ..fresh(t.offset_before(LOG, size, from).await?) }
            }
            Start::Resume(s) => {
                warn!("saved state is for another alda.log (inode {}); starting at end", s.inode);
                fresh(t.size(LOG).await?)
            }
            Start::Beginning => fresh(0),
            Start::End => fresh(t.size(LOG).await?),
            Start::Backfill(n) => fresh(t.size(LOG).await?.saturating_sub(*n)),
        };
        info!("following {}{LOG} node={} inode={} offset={}", t.base, t.state.node, t.state.inode, t.state.offset);
        Ok(t)
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    pub fn set_filter(&mut self, filter: Filter) {
        self.filter = filter;
    }

    pub async fn poll(&mut self) -> Result<Vec<Entry>, Error> {
        let mut v = Vec::new();
        self.poll_with(|e| v.push(e)).await?;
        Ok(v)
    }

    /// Streams entries to `sink` so large backfills are never collected in memory.
    pub async fn poll_with<F: FnMut(Entry)>(&mut self, mut sink: F) -> Result<(), Error> {
        if self.last_header.elapsed() >= self.header_check {
            let id = self.header(LOG).await?;
            if !Ident::of(&self.state).same_file(&id) {
                return self.reopen(id, &mut sink).await;
            }
            if self.state.first.is_empty() {
                self.state.first = id.first;
            }
        }
        match self.read(LOG, &mut sink).await {
            Ok(()) => {
                self.inconsistent_since = None;
                Ok(())
            }
            Err(ReadError::Other(e)) => Err(e),
            Err(ReadError::Mismatch) => {
                let id = self.header(LOG).await?;
                if !Ident::of(&self.state).same_file(&id) {
                    return self.reopen(id, &mut sink).await;
                }
                // Same file but different content: most likely a stale cached copy.
                let since = *self.inconsistent_since.get_or_insert_with(Instant::now);
                if since.elapsed() < INCONSISTENT_GIVE_UP {
                    return Err(Error::Inconsistent);
                }
                warn!("{LOG} inconsistent for {}s, repositioning", INCONSISTENT_GIVE_UP.as_secs());
                self.switch_node(id, &mut sink).await
            }
        }
    }

    async fn reopen<F: FnMut(Entry)>(&mut self, id: Ident, sink: &mut F) -> Result<(), Error> {
        if id.node != self.state.node { self.switch_node(id, sink).await } else { self.rotate(id, sink).await }
    }

    // Drain the old log from alda.log.1 (verified by identity) before switching, so no lines are lost.
    async fn rotate<F: FnMut(Entry)>(&mut self, new: Ident, sink: &mut F) -> Result<(), Error> {
        let old = Ident::of(&self.state);
        match self.header(ROTATED).await {
            Ok(i) if i.same_file(&old) => match self.read(ROTATED, sink).await {
                Ok(()) => {}
                Err(ReadError::Mismatch) => {
                    warn!("{ROTATED} does not continue where {LOG} left off; entries may be lost")
                }
                Err(ReadError::Other(e)) => warn!("draining {ROTATED} failed, entries may be lost: {e}"),
            },
            Ok(i) => warn!("{ROTATED} has inode {}, expected {}; entries may be lost", i.inode, old.inode),
            Err(e) => warn!("reading {ROTATED} failed, entries may be lost: {e}"),
        }
        info!("{LOG} rotated: inode {} -> {}", old.inode, new.inode);
        self.reposition(new, 0);
        self.finish_read(sink).await
    }

    // The nodes receive files in different orders, so the byte offset means nothing on
    // the other one. Replay its log from shortly before the last line seen; `seen`
    // drops what was already emitted.
    async fn switch_node<F: FnMut(Entry)>(&mut self, new: Ident, sink: &mut F) -> Result<(), Error> {
        let last = self.state.last_time.unwrap_or_else(unix_now);
        let from = last.saturating_sub(REPLAY_WINDOW.as_secs());
        let size = self.size(LOG).await?;
        let offset = self.offset_before(LOG, size, from).await?;
        warn!(
            "opendata node switched {} -> {}; replaying {} bytes of the new node's {LOG}",
            self.state.node,
            new.node,
            size - offset
        );
        self.reposition(new, offset);
        self.replay_from = Some(from);
        let r = self.finish_read(sink).await;
        self.replay_from = None;
        r
    }

    fn reposition(&mut self, id: Ident, offset: u64) {
        let last_time = self.state.last_time;
        self.state = State { node: id.node, inode: id.inode, first: id.first, offset, last_byte: None, last_time };
        self.pending.clear();
        self.last_header = Instant::now();
        self.inconsistent_since = None;
    }

    async fn finish_read<F: FnMut(Entry)>(&mut self, sink: &mut F) -> Result<(), Error> {
        match self.read(LOG, sink).await {
            Ok(()) => Ok(()),
            Err(ReadError::Mismatch) => Err(Error::Inconsistent),
            Err(ReadError::Other(e)) => Err(e),
        }
    }

    async fn header(&mut self, name: &str) -> Result<Ident, Error> {
        let url = format!("{}{name}", self.base);
        let resp = self.client.get(&url).header(header::RANGE, "bytes=0-1023").send().await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(Error::Status { url, status: status.as_u16() });
        }
        let body = resp.bytes().await?;
        let mut lines = body.split_inclusive(|&b| b == b'\n');
        let line = String::from_utf8_lossy(lines.next().unwrap_or_default());
        let field = |k: &str| {
            line.strip_prefix("# V")
                .and_then(|r| r.split_whitespace().find_map(|w| w.strip_prefix(k)))
                .map(str::to_owned)
        };
        let (Some(inode), Some(node)) = (field("inode="), field("node=")) else {
            return Err(Error::Header { name: name.to_owned(), line: line.trim_end().to_owned() });
        };
        // Only a complete line counts; a log that just rotated may have none yet.
        let first = lines
            .find(|l| !l.starts_with(b"#"))
            .filter(|l| l.ends_with(b"\n"))
            .map(|l| String::from_utf8_lossy(l).trim_end().to_owned())
            .unwrap_or_default();
        if name == LOG {
            self.last_header = Instant::now();
        }
        Ok(Ident { node, inode, first })
    }

    async fn size(&self, name: &str) -> Result<u64, Error> {
        let url = format!("{}{name}", self.base);
        let resp = self.client.get(&url).header(header::RANGE, "bytes=0-0").send().await?;
        if !resp.status().is_success() {
            return Err(Error::Status { url, status: resp.status().as_u16() });
        }
        Ok(total_size(&resp).unwrap_or(0))
    }

    /// An offset whose lines are no newer than `time`, found by stepping back from the end
    /// (lines are roughly in time order).
    async fn offset_before(&self, name: &str, size: u64, time: u64) -> Result<u64, Error> {
        let mut back = 8 << 20;
        loop {
            let off = size.saturating_sub(back);
            if off == 0 || back >= MAX_REPLAY_BYTES {
                return Ok(off);
            }
            if self.time_at(name, off).await?.is_some_and(|t| t <= time) {
                return Ok(off);
            }
            back *= 2;
        }
    }

    async fn time_at(&self, name: &str, off: u64) -> Result<Option<u64>, Error> {
        let url = format!("{}{name}", self.base);
        let resp = self.client.get(&url).header(header::RANGE, format!("bytes={off}-{}", off + 4095)).send().await?;
        let body = resp.bytes().await?;
        let mut lines = body.split(|&b| b == b'\n').skip(1);
        Ok(lines.next().and_then(line_time))
    }

    // Requests from offset-1: the overlapping byte must equal the last byte read,
    // which detects a replaced file without an extra request.
    async fn read<F: FnMut(Entry)>(&mut self, name: &str, sink: &mut F) -> Result<(), ReadError> {
        let start = self.state.offset + self.pending.len() as u64;
        let from = start.saturating_sub(1);
        let url = format!("{}{name}", self.base);
        let mut resp = self.client.get(&url).header(header::RANGE, format!("bytes={from}-")).send().await?;
        let mut skip = match resp.status() {
            StatusCode::PARTIAL_CONTENT => 0,
            StatusCode::OK => from,
            StatusCode::RANGE_NOT_SATISFIABLE => return Err(ReadError::Mismatch),
            s => return Err(Error::Status { url, status: s.as_u16() }.into()),
        };
        if total_size(&resp).is_some_and(|t| t < start) {
            return Err(ReadError::Mismatch);
        }

        let mut verify = start > 0;
        let mut align = false;
        let mut pos = from;
        while let Some(chunk) = resp.chunk().await? {
            let mut c: &[u8] = &chunk;
            if skip > 0 {
                let n = (skip as usize).min(c.len());
                c = &c[n..];
                skip -= n as u64;
                if c.is_empty() {
                    continue;
                }
            }
            if verify {
                verify = false;
                let prev = self.pending.last().copied().or(self.state.last_byte);
                match prev {
                    Some(p) if c[0] != p => return Err(ReadError::Mismatch),
                    Some(_) => {}
                    None if c[0] == b'\n' => {}
                    // Unknown previous byte (fresh start mid-file): drop the partial first line.
                    None => align = true,
                }
                if !align {
                    c = &c[1..];
                    pos += 1;
                }
            }
            if align {
                match memchr::memchr(b'\n', c) {
                    None => {
                        pos += c.len() as u64;
                        continue;
                    }
                    Some(i) => {
                        align = false;
                        pos += i as u64 + 1;
                        self.state.offset = pos;
                        self.state.last_byte = Some(b'\n');
                        c = &c[i + 1..];
                    }
                }
            }
            pos += c.len() as u64;
            self.consume(c, sink);
        }
        if verify {
            return Err(ReadError::Mismatch);
        }
        Ok(())
    }

    fn consume<F: FnMut(Entry)>(&mut self, body: &[u8], sink: &mut F) {
        let Some(end) = memchr::memrchr(b'\n', body) else {
            self.pending.extend_from_slice(body);
            return;
        };
        let consumed = (self.pending.len() + end + 1) as u64;
        let mut lines = body[..end].split(|&b| b == b'\n');
        let mut first = std::mem::take(&mut self.pending);
        first.extend_from_slice(lines.next().unwrap_or_default());

        let now = SystemTime::now();
        let (filter, seen, last_time, replay_from) =
            (&self.filter, &mut self.seen, &mut self.state.last_time, self.replay_from);
        // Filter on the raw path before allocating an Entry; most lines are discarded.
        let mut emit = |line: &[u8]| {
            let Ok(line) = std::str::from_utf8(line) else { return };
            let line = line.trim_end_matches('\r');
            let Some(t) = line_time(line.as_bytes()) else { return };
            *last_time = Some(last_time.map_or(t, |l| l.max(t)));
            // "size|path": a file republished with the same size within SEEN_WINDOW is a
            // duplicate from a node switch, not an update.
            let key = line.split_once('|').map_or("", |(_, k)| k);
            let path = key.split_once('|').map_or("", |(_, p)| p);
            if replay_from.is_some_and(|from| t < from) || seen.contains(key) {
                return;
            }
            if let Some(matched) = filter.check(path)
                && let Some(mut e) = Entry::parse_line(line, now)
            {
                seen.insert(t, key);
                e.matched = matched;
                sink(e);
            }
        };
        emit(&first);
        lines.for_each(&mut emit);

        self.state.offset += consumed;
        self.state.last_byte = Some(b'\n');
        self.pending = body[end + 1..].to_vec();
    }
}

fn line_time(line: &[u8]) -> Option<u64> {
    let end = memchr::memchr(b'|', line)?;
    std::str::from_utf8(&line[..end]).ok()?.parse().ok()
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

fn total_size(resp: &reqwest::Response) -> Option<u64> {
    if let Some(cr) = resp.headers().get(header::CONTENT_RANGE) {
        return cr.to_str().ok()?.rsplit_once('/')?.1.parse().ok();
    }
    (resp.status() == StatusCode::OK).then(|| resp.content_length()).flatten()
}
