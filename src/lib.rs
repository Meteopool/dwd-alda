//! Follow DWD's `alda.log` on opendata.dwd.de and react to newly published files.

mod download;
mod entry;
mod filter;
mod tailer;

pub use download::Downloader;
pub use entry::{Entry, Sweep, unix_secs};
pub use filter::Filter;
pub use tailer::{DEFAULT_BASE, Start, State, Tailer, TailerConfig};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Regex(#[from] regex::Error),
    #[error("{url}: HTTP {status}")]
    Status { url: String, status: u16 },
    #[error("{name}: unexpected header line {line:?}")]
    Header { name: String, line: String },
    /// Content changed under the same inode (e.g. stale cache); position is kept, poll again.
    #[error("alda.log content inconsistent with saved position, will retry")]
    Inconsistent,
    #[error("refusing unsafe path {0:?}")]
    UnsafePath(String),
}
