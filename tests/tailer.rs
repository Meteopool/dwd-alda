//! Runs the tailer against a minimal in-process HTTP server with Range support.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dwd_alda::{Error, Filter, Start, Tailer, TailerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

type Files = Arc<Mutex<HashMap<String, Vec<u8>>>>;

async fn serve(files: Files) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let files = files.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = sock.read(&mut tmp).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                let req = String::from_utf8_lossy(&buf).to_string();
                let path = req.split_whitespace().nth(1).unwrap().trim_start_matches('/').to_owned();
                let range =
                    req.lines().find_map(|l| l.to_ascii_lowercase().strip_prefix("range: bytes=").map(str::to_owned));
                let body = files.lock().unwrap().get(&path).cloned();
                let resp = match body {
                    None => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
                    Some(b) => {
                        let total = b.len();
                        let (from, to) = match range {
                            None => (0, total.saturating_sub(1)),
                            Some(r) => {
                                let (a, z) = r.trim().split_once('-').unwrap();
                                let a: usize = a.parse().unwrap();
                                let z = if z.is_empty() {
                                    total.saturating_sub(1)
                                } else {
                                    z.parse::<usize>().unwrap().min(total - 1)
                                };
                                (a, z)
                            }
                        };
                        if from >= total {
                            format!("HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{total}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").into_bytes()
                        } else {
                            let part = &b[from..=to];
                            let mut r = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {from}-{to}/{total}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                part.len()
                            )
                            .into_bytes();
                            r.extend_from_slice(part);
                            r
                        }
                    }
                };
                let _ = sock.write_all(&resp).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    format!("http://{addr}/")
}

fn hdr(inode: &str) -> String {
    format!("# V1.0 inode={inode} node=x\n# Format: <unix time>|<size in bytes>|<file name with path>\n")
}

fn line(i: u32) -> String {
    format!("{}|{i}|radar/sites/sweep_vol_z/boo/f{i:04}\n", 1_792_627_200 + i)
}

fn set(files: &Files, name: &str, body: &str) {
    files.lock().unwrap().insert(name.to_owned(), body.as_bytes().to_vec());
}

fn names(v: &[dwd_alda::Entry]) -> Vec<String> {
    v.iter().map(|e| e.path.rsplit('/').next().unwrap().to_owned()).collect()
}

async fn tailer(base: String, start: Start) -> Tailer {
    Tailer::connect(reqwest::Client::new(), TailerConfig { base, start, ..Default::default() }).await.unwrap()
}

#[tokio::test]
async fn append_split_rotate() {
    let files: Files = Default::default();
    let base = serve(files.clone()).await;
    let mut old = hdr("A") + &line(0) + &line(1);
    set(&files, "alda.log", &old);
    let mut t = tailer(base, Start::End).await;
    let mut got = Vec::new();

    let l3 = line(3);
    old += &(line(2) + &l3[..10]);
    set(&files, "alda.log", &old);
    got.extend(t.poll().await.unwrap());
    old += &l3[10..];
    set(&files, "alda.log", &old);
    got.extend(t.poll().await.unwrap());
    got.extend(t.poll().await.unwrap());
    // Line 4 lands in the old log just before it becomes alda.log.1.
    old += &line(4);
    set(&files, "alda.log.1", &old);
    set(&files, "alda.log", &(hdr("B") + &line(5) + &line(6)));
    got.extend(t.poll().await.unwrap());
    set(&files, "alda.log", &(hdr("B") + &line(5) + &line(6) + &line(7)));
    got.extend(t.poll().await.unwrap());

    assert_eq!(names(&got), ["f0002", "f0003", "f0004", "f0005", "f0006", "f0007"]);
    assert_eq!(t.state().inode, "B");
    assert_eq!(t.state().offset as usize, (hdr("B") + &line(5) + &line(6) + &line(7)).len());
}

#[tokio::test]
async fn backfill_starts_mid_line() {
    let files: Files = Default::default();
    let base = serve(files.clone()).await;
    set(&files, "alda.log", &(hdr("A") + &line(0) + &line(1) + &line(2)));
    let mut t = tailer(base, Start::Backfill(line(2).len() as u64 + 5)).await;
    assert_eq!(names(&t.poll().await.unwrap()), ["f0002"]);
}

#[tokio::test]
async fn from_start_and_filter() {
    let files: Files = Default::default();
    let base = serve(files.clone()).await;
    set(&files, "alda.log", &(hdr("A") + &line(0) + "1792627201|5|nwp/icon/x\n" + &line(2)));
    let mut t = tailer(base, Start::Beginning).await;
    t.set_filter(Filter::new([r"^radar/"], [r"f0002$"]).unwrap());
    assert_eq!(names(&t.poll().await.unwrap()), ["f0000"]);
}

#[tokio::test]
async fn stale_response_does_not_reset() {
    let files: Files = Default::default();
    let base = serve(files.clone()).await;
    let full = hdr("A") + &line(0) + &line(1);
    set(&files, "alda.log", &full);
    let mut t = tailer(base, Start::End).await;
    // A cache serves an older, shorter copy of the same file.
    set(&files, "alda.log", &(hdr("A") + &line(0)));
    assert!(matches!(t.poll().await, Err(Error::Inconsistent)));
    assert_eq!(t.state().offset as usize, full.len());
    set(&files, "alda.log", &(full.clone() + &line(2)));
    assert_eq!(names(&t.poll().await.unwrap()), ["f0002"]);
}

#[tokio::test]
async fn resume_from_state() {
    let files: Files = Default::default();
    let base = serve(files.clone()).await;
    let body = hdr("A") + &line(0);
    set(&files, "alda.log", &body);
    let state = tailer(base.clone(), Start::End).await.state().clone();
    set(&files, "alda.log", &(body + &line(1)));
    let mut t = tailer(base, Start::Resume(state)).await;
    assert_eq!(names(&t.poll().await.unwrap()), ["f0001"]);
}
