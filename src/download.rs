use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use reqwest::Client;
use tokio::io::AsyncWriteExt;

use crate::Error;

#[derive(Debug, Clone)]
pub struct Downloader {
    client: Client,
    base: String,
    retries: u32,
}

impl Downloader {
    pub fn new(client: Client, base: impl Into<String>, retries: u32) -> Self {
        Downloader { client, base: base.into(), retries }
    }

    /// Downloads into memory. Returns the body and the number of retries needed.
    pub async fn fetch(&self, path: &str) -> Result<(Bytes, u32), Error> {
        self.retry(|| async {
            let resp = self.get(path).await?;
            Ok(resp.bytes().await?)
        })
        .await
    }

    /// Downloads to `dir`, mirroring the server path.
    pub async fn fetch_to(&self, path: &str, dir: &Path) -> Result<(PathBuf, u32), Error> {
        let dst = Self::local_path(dir, path)?;
        let (_, retries) = self.retry(|| self.try_save(path, &dst)).await?;
        Ok((dst, retries))
    }

    pub fn local_path(dir: &Path, path: &str) -> Result<PathBuf, Error> {
        if !Path::new(path).components().all(|c| matches!(c, Component::Normal(_))) {
            return Err(Error::UnsafePath(path.to_owned()));
        }
        // ':' occurs in nwp/v1 paths and is invalid on Windows.
        Ok(path.replace(':', "_").split('/').fold(dir.to_path_buf(), |p, c| p.join(c)))
    }

    async fn retry<T, F, Fut>(&self, mut f: F) -> Result<(T, u32), Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        let mut attempt = 0;
        loop {
            match f().await {
                Ok(v) => return Ok((v, attempt)),
                Err(e) if attempt >= self.retries => return Err(e),
                Err(_) => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_secs(attempt.into())).await;
                }
            }
        }
    }

    async fn get(&self, path: &str) -> Result<reqwest::Response, Error> {
        let url = format!("{}{path}", self.base);
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(Error::Status { url, status: resp.status().as_u16() });
        }
        Ok(resp)
    }

    // Write to .part and rename, so readers never see a partial file.
    async fn try_save(&self, path: &str, dst: &Path) -> Result<(), Error> {
        let mut resp = self.get(path).await?;
        if let Some(dir) = dst.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        let mut part = dst.as_os_str().to_owned();
        part.push(".part");
        let part = PathBuf::from(part);
        let result = async {
            let mut f = tokio::fs::File::create(&part).await?;
            while let Some(chunk) = resp.chunk().await? {
                f.write_all(&chunk).await?;
            }
            f.flush().await?;
            drop(f);
            tokio::fs::rename(&part, dst).await?;
            Ok::<_, Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&part).await;
        }
        result
    }
}
