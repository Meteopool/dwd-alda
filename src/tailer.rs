use std::time::{Duration, Instant, SystemTime};

use log::{info, warn};
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};

use crate::{Entry, Error, Filter};

pub const DEFAULT_BASE: &str = "https://opendata.dwd.de/weather/";
const LOG: &str = "alda.log";
const ROTATED: &str = "alda.log.1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub inode: String,
    /// Bytes of complete lines consumed.
    pub offset: u64,
    pub last_byte: Option<u8>,
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
        TailerConfig { base: DEFAULT_BASE.to_owned(), start: Start::End, header_check: Duration::from_secs(30) }
    }
}

/// Follows `alda.log` with HTTP Range requests, including the daily rotation to `alda.log.1`.
pub struct Tailer {
    client: Client,
    base: String,
    header_check: Duration,
    state: State,
    pending: Vec<u8>,
    last_header: Instant,
    filter: Filter,
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
            state: State { inode: String::new(), offset: 0, last_byte: None },
            pending: Vec::new(),
            last_header: Instant::now(),
            filter: Filter::default(),
        };
        let inode = t.header(LOG).await?;
        let resume = match &cfg.start {
            Start::Resume(s) if s.inode == inode => Some(s.clone()),
            Start::Resume(s) => {
                warn!("saved state is for inode {} but alda.log is {inode}; starting at end", s.inode);
                None
            }
            _ => None,
        };
        t.state = match (resume, &cfg.start) {
            (Some(s), _) => s,
            (None, Start::Beginning) => State { inode, offset: 0, last_byte: None },
            (None, start) => {
                let back = if let Start::Backfill(n) = start { *n } else { 0 };
                let size = t.size(LOG).await?;
                State { inode, offset: size.saturating_sub(back), last_byte: None }
            }
        };
        info!("following {}{LOG} inode={} offset={}", t.base, t.state.inode, t.state.offset);
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
            let inode = self.header(LOG).await?;
            if inode != self.state.inode {
                return self.rotate(inode, &mut sink).await;
            }
        }
        match self.read(LOG, &mut sink).await {
            Ok(()) => Ok(()),
            Err(ReadError::Other(e)) => Err(e),
            Err(ReadError::Mismatch) => {
                let inode = self.header(LOG).await?;
                // Same inode means a stale/cached response, not a rotation: keep the position.
                if inode == self.state.inode {
                    return Err(Error::Inconsistent);
                }
                self.rotate(inode, &mut sink).await
            }
        }
    }

    // Drain the old log from alda.log.1 (verified by inode) before switching, so no lines are lost.
    async fn rotate<F: FnMut(Entry)>(&mut self, new_inode: String, sink: &mut F) -> Result<(), Error> {
        let old = self.state.inode.clone();
        match self.header(ROTATED).await {
            Ok(i) if i == old => match self.read(ROTATED, sink).await {
                Ok(()) => {}
                Err(ReadError::Mismatch) => {
                    warn!("{ROTATED} does not continue where {LOG} left off; entries may be lost")
                }
                Err(ReadError::Other(e)) => warn!("draining {ROTATED} failed, entries may be lost: {e}"),
            },
            Ok(i) => warn!("{ROTATED} has inode {i}, expected {old}; entries may be lost"),
            Err(e) => warn!("reading {ROTATED} failed, entries may be lost: {e}"),
        }
        info!("{LOG} rotated: inode {old} -> {new_inode}");
        self.state = State { inode: new_inode, offset: 0, last_byte: None };
        self.pending.clear();
        self.last_header = Instant::now();
        match self.read(LOG, sink).await {
            Ok(()) => Ok(()),
            Err(ReadError::Mismatch) => Err(Error::Inconsistent),
            Err(ReadError::Other(e)) => Err(e),
        }
    }

    async fn header(&mut self, name: &str) -> Result<String, Error> {
        let url = format!("{}{name}", self.base);
        let resp = self.client.get(&url).header(header::RANGE, "bytes=0-255").send().await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(Error::Status { url, status: status.as_u16() });
        }
        let body = resp.bytes().await?;
        let line = body.split(|&b| b == b'\n').next().unwrap_or_default();
        let line = String::from_utf8_lossy(line);
        let inode = line
            .strip_prefix("# V")
            .and_then(|r| r.split_whitespace().find_map(|w| w.strip_prefix("inode=")))
            .ok_or_else(|| Error::Header { name: name.to_owned(), line: line.clone().into_owned() })?;
        if name == LOG {
            self.last_header = Instant::now();
        }
        Ok(inode.to_owned())
    }

    async fn size(&self, name: &str) -> Result<u64, Error> {
        let url = format!("{}{name}", self.base);
        let resp = self.client.get(&url).header(header::RANGE, "bytes=0-0").send().await?;
        if !resp.status().is_success() {
            return Err(Error::Status { url, status: resp.status().as_u16() });
        }
        Ok(total_size(&resp).unwrap_or(0))
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

        let seen = SystemTime::now();
        let filter = &self.filter;
        // Filter on the raw path before allocating an Entry; most lines are discarded.
        let mut emit = |line: &[u8]| {
            let Ok(line) = std::str::from_utf8(line) else { return };
            let line = line.trim_end_matches('\r');
            let path = line.splitn(3, '|').nth(2).unwrap_or("");
            if let Some(matched) = filter.check(path)
                && let Some(mut e) = Entry::parse_line(line, seen)
            {
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

fn total_size(resp: &reqwest::Response) -> Option<u64> {
    if let Some(cr) = resp.headers().get(header::CONTENT_RANGE) {
        return cr.to_str().ok()?.rsplit_once('/')?.1.parse().ok();
    }
    (resp.status() == StatusCode::OK).then(|| resp.content_length()).flatten()
}
