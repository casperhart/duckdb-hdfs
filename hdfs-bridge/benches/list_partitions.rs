//! Recursive-listing benchmark: the sequential walk (hdfs-native's lazy
//! one-RPC-at-a-time recursive iterator, which the bridge exposed as
//! `hdfs_bridge_list_status` before the streaming API replaced it) vs the
//! streaming parallel walk (`hdfs_bridge_list_stream_*`), on a Hive-style
//! partition tree (`year=YYYY/month=MM/day=DD`, one empty marker file per day).
//!
//! Network latency is simulated by a local TCP proxy in front of the NameNode
//! that delays every chunk by RTT/2 in each direction. Chunks are delayed on a
//! schedule (not by blocking the pipe), so concurrent RPCs multiplexed on the
//! one NameNode connection are *not* serialized by the proxy — each just
//! arrives RTT/2 later, like a real network. Listings only ever talk to the
//! NameNode, so proxying that one port covers the whole workload.
//!
//! Requires the docker HDFS from test/docker:
//!
//!     test/scripts/hdfs_up.sh
//!     cargo bench --bench list_partitions     # HDFS_BENCH_RTT_MS to tweak
//!     test/scripts/hdfs_down.sh
//!
//! Exits cleanly with a SKIP message when no NameNode is listening.

use std::ffi::{CStr, CString};
use std::net::SocketAddr;
use std::ptr;
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt as _};
use hdfs_native::{Client, ClientBuilder, WriteOptions};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use hdfs_bridge::{
    hdfs_bridge_connect, hdfs_bridge_free_dir_entries, hdfs_bridge_list_stream_free,
    hdfs_bridge_list_stream_next, hdfs_bridge_list_stream_open, BridgeClient, Status,
};

const NAMENODE: &str = "127.0.0.1:9000";
const ROOT: &str = "/bench_partitions";
const YEARS: std::ops::RangeInclusive<u32> = 2023..=2025;
const MONTHS: std::ops::RangeInclusive<u32> = 1..=12;
const DAYS: std::ops::RangeInclusive<u32> = 1..=28;

fn main() {
    if std::net::TcpStream::connect(NAMENODE).is_err() {
        eprintln!("SKIP: no NameNode at {NAMENODE}; start one with test/scripts/hdfs_up.sh");
        return;
    }
    let rtt_ms: u64 = std::env::var("HDFS_BENCH_RTT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);

    let rt = Runtime::new().unwrap();
    rt.block_on(setup_tree());
    let proxy = rt.block_on(start_proxy(Duration::from_micros(rtt_ms * 500)));
    let n_days = YEARS.count() * MONTHS.count() * DAYS.count();
    println!(
        "tree: {} day partitions (+1 marker file each) under {ROOT}; \
         simulated RTT +{rtt_ms}ms via proxy {proxy}",
        n_days
    );

    // The bridge client dials the proxy, exactly as the extension would dial a
    // remote NameNode.
    let url = CString::new(format!("hdfs://{proxy}")).unwrap();
    let user = CString::new("hadoop").unwrap();
    let root = CString::new(ROOT).unwrap();
    let mut st = ok_status();
    let client = unsafe { hdfs_bridge_connect(url.as_ptr(), ptr::null(), user.as_ptr(), &mut st) };
    check(&st, "connect");
    assert!(!client.is_null());

    println!();
    println!(
        "{:<26} {:>9} {:>9} {:>9} {:>10} {:>8}",
        "variant", "min", "mean", "max", "first-rows", "entries"
    );

    // Baseline: hdfs-native's own recursive listing, dialed through the same
    // proxy so both variants see the same latency. This is the sequential
    // iterator the bridge wrapped before the streaming API replaced it.
    let seq_client = ClientBuilder::new()
        .with_url(format!("hdfs://{proxy}"))
        .with_user("hadoop")
        .with_io_runtime(rt.handle().clone())
        .build()
        .unwrap();
    let seq = bench("sequential (baseline)", 3, || {
        let (total, n) = run_sequential(&rt, &seq_client);
        (total, None, n)
    });

    for par in [1, 4, 16, 64] {
        bench_vs(&format!("stream par={par}"), 5, seq, || unsafe {
            let (total, first, n) = run_stream(client, &root, par);
            (total, Some(first), n)
        });
    }

    println!();
    println!("(first-rows = time until the first batch is available to the query;");
    println!(" the sequential path has no rows until the entire walk finishes)");
}

// --- measurement -------------------------------------------------------------

type Sample = (Duration, Option<Duration>, usize);

fn bench(name: &str, iters: usize, mut f: impl FnMut() -> Sample) -> Duration {
    f(); // warmup
    let samples: Vec<Sample> = (0..iters).map(|_| f()).collect();
    report(name, &samples, None);
    mean(&samples)
}

fn bench_vs(name: &str, iters: usize, baseline: Duration, mut f: impl FnMut() -> Sample) {
    f(); // warmup
    let samples: Vec<Sample> = (0..iters).map(|_| f()).collect();
    report(name, &samples, Some(baseline));
}

fn mean(samples: &[Sample]) -> Duration {
    samples.iter().map(|s| s.0).sum::<Duration>() / samples.len() as u32
}

fn report(name: &str, samples: &[Sample], baseline: Option<Duration>) {
    let min = samples.iter().map(|s| s.0).min().unwrap();
    let max = samples.iter().map(|s| s.0).max().unwrap();
    let avg = mean(samples);
    let entries = samples[0].2;
    assert!(
        samples.iter().all(|s| s.2 == entries),
        "entry count varied across runs"
    );
    let first = match samples.iter().filter_map(|s| s.1).min() {
        Some(f) => format!("{:>9.1?}", f),
        None => format!("{:>9}", "-"),
    };
    let speedup = match baseline {
        Some(b) => format!("  ({:.1}x)", b.as_secs_f64() / avg.as_secs_f64()),
        None => String::new(),
    };
    println!(
        "{:<26} {:>9.1?} {:>9.1?} {:>9.1?} {:>10} {:>8}{}",
        name, min, avg, max, first, entries, speedup
    );
}

// --- the two listing paths under test ----------------------------------------

/// hdfs-native's plain recursive listing: one `getListing` RPC at a time, the
/// full result materialized before any entry is available.
fn run_sequential(rt: &Runtime, client: &Client) -> (Duration, usize) {
    let start = Instant::now();
    let statuses = rt.block_on(client.list_status(ROOT, true)).unwrap();
    (start.elapsed(), statuses.len())
}

unsafe fn run_stream(
    client: *mut BridgeClient,
    root: &CString,
    par: i32,
) -> (Duration, Duration, usize) {
    let start = Instant::now();
    let stream = hdfs_bridge_list_stream_open(client, root.as_ptr(), true, par);
    let mut total = 0usize;
    let mut first = None;
    loop {
        let mut count = 0i32;
        let mut st = ok_status();
        let batch = hdfs_bridge_list_stream_next(stream, 2048, &mut count, &mut st);
        check(&st, "list_stream_next");
        if batch.is_null() {
            break;
        }
        first.get_or_insert_with(|| start.elapsed());
        total += count as usize;
        hdfs_bridge_free_dir_entries(batch, count);
    }
    let elapsed = start.elapsed();
    hdfs_bridge_list_stream_free(stream);
    (elapsed, first.unwrap_or(elapsed), total)
}

fn ok_status() -> Status {
    Status {
        code: 0,
        msg: ptr::null_mut(),
    }
}

fn check(st: &Status, ctx: &str) {
    if st.code != 0 {
        let msg = if st.msg.is_null() {
            "<no message>".to_string()
        } else {
            unsafe { CStr::from_ptr(st.msg) }
                .to_string_lossy()
                .into_owned()
        };
        panic!("{ctx} failed: [{}] {msg}", st.code);
    }
}

// --- fixture tree -------------------------------------------------------------

/// Create the partition tree (idempotent) over a direct, un-proxied connection.
async fn setup_tree() {
    let client = ClientBuilder::new()
        .with_url(format!("hdfs://{NAMENODE}"))
        .with_user("hadoop")
        .build()
        .unwrap();
    let probe = format!(
        "{ROOT}/year={}/month={:02}/day={:02}/part-0000.parquet",
        YEARS.end(),
        MONTHS.end(),
        DAYS.end()
    );
    if client.get_file_info(&probe).await.is_ok() {
        println!("partition tree already present, skipping setup");
        return;
    }
    println!("creating partition tree (one-time setup)...");
    let client = &client;
    let mut tasks = FuturesUnordered::new();
    for year in YEARS {
        for month in MONTHS {
            for day in DAYS {
                let dir = format!("{ROOT}/year={year}/month={month:02}/day={day:02}");
                tasks.push(async move {
                    client.mkdirs(&dir, 0o755, true).await.unwrap();
                    // Zero-length marker file: create + complete touch only the
                    // NameNode, so no DataNode connectivity is needed.
                    let mut writer = client
                        .create(
                            &format!("{dir}/part-0000.parquet"),
                            WriteOptions::default().overwrite(true),
                        )
                        .await
                        .unwrap();
                    writer.close().await.unwrap();
                });
                if tasks.len() >= 32 {
                    tasks.next().await;
                }
            }
        }
    }
    while tasks.next().await.is_some() {}
}

// --- latency proxy -------------------------------------------------------------

/// Accept connections and forward them to the NameNode, delaying every chunk by
/// `one_way` in each direction. Delivery is scheduled rather than blocking, so
/// back-to-back chunks are not queued behind each other's delay.
async fn start_proxy(one_way: Duration) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(NAMENODE).await else {
                    return;
                };
                inbound.set_nodelay(true).ok();
                outbound.set_nodelay(true).ok();
                let (client_r, client_w) = inbound.into_split();
                let (server_r, server_w) = outbound.into_split();
                tokio::join!(
                    pump(client_r, server_w, one_way),
                    pump(server_r, client_w, one_way)
                );
            });
        }
    });
    addr
}

async fn pump(
    mut read: tokio::net::tcp::OwnedReadHalf,
    mut write: tokio::net::tcp::OwnedWriteHalf,
    delay: Duration,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
    let reader = tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match read.read(&mut buf).await {
                Ok(0) | Err(_) => break, // EOF/error: dropping tx drains the pump
                Ok(n) => {
                    if tx
                        .send((Instant::now() + delay, buf[..n].to_vec()))
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });
    while let Some((due, chunk)) = rx.recv().await {
        tokio::time::sleep_until(due.into()).await;
        if write.write_all(&chunk).await.is_err() {
            break;
        }
    }
    let _ = write.shutdown().await;
    reader.abort();
}
