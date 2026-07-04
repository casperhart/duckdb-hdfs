//! Safe, blocking wrappers around the async `hdfs-native` client.
//!
//! Each handle pairs an async `hdfs-native` object with the Tokio runtime that
//! drives it and exposes blocking methods, so the FFI layer (`lib.rs`) never
//! touches async code or the underlying objects directly.
//!
//! Field order matters in these structs: `inner` is declared before `rt` so
//! the hdfs-native object (whose `Drop` may use the runtime, e.g. a writer
//! releasing its file lease) is dropped while the runtime is still alive.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt as _};
use hdfs_native::client::FileStatus;
use hdfs_native::file::{FileReader, FileWriter};
use hdfs_native::{Client, ClientBuilder, HdfsError, WriteOptions};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

// The C++ filesystem issues concurrent positional reads against a single
// `FileReader` (DuckDB's parquet reader reads ranges from multiple threads on
// one handle). That is only sound if `FileReader` is `Sync`. Assert it at
// compile time so an upstream change can't silently break the bridge.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<FileReader>();
    assert_send_sync::<Client>();
    assert_send_sync::<BridgeClient>();
    assert_send_sync::<BridgeReader>();
    // Writers and list streams move between threads but are only used from one
    // at a time.
    assert_send::<BridgeWriter>();
    assert_send::<BridgeListStream>();
};

/// An HDFS client plus the Tokio runtime all its operations run on.
pub struct BridgeClient {
    inner: Client,
    rt: Arc<Runtime>,
}

impl BridgeClient {
    /// Connect to HDFS. `url` (e.g. `hdfs://namenode:8020`), `config_dir`, and
    /// `user` are all optional.
    pub fn connect(
        url: Option<String>,
        config_dir: Option<String>,
        user: Option<String>,
    ) -> Result<Self, HdfsError> {
        let rt = Runtime::new().map_err(|e| {
            HdfsError::IOError(std::io::Error::new(
                e.kind(),
                format!("failed to start IO runtime: {e}"),
            ))
        })?;
        let rt = Arc::new(rt);
        // The client spawns its IO tasks on our runtime; a plain `build()`
        // would lazily create a second, hidden runtime.
        let mut builder = ClientBuilder::new().with_io_runtime(rt.handle().clone());
        if let Some(url) = url {
            builder = builder.with_url(url);
        }
        if let Some(dir) = config_dir {
            builder = builder.with_config_dir(dir);
        }
        if let Some(user) = user {
            builder = builder.with_user(user);
        }
        Ok(Self {
            inner: builder.build()?,
            rt,
        })
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.rt.block_on(future)
    }

    /// Metadata for a single path.
    pub fn get_file_info(&self, path: &str) -> Result<FileStatus, HdfsError> {
        self.block_on(self.inner.get_file_info(path))
    }

    /// Open `path` for reading.
    pub fn open(&self, path: &str) -> Result<BridgeReader, HdfsError> {
        let inner = self.block_on(self.inner.read(path))?;
        Ok(BridgeReader {
            inner,
            rt: Arc::clone(&self.rt),
        })
    }

    /// Create a file for writing. `overwrite` controls whether an existing
    /// file is replaced; missing parent directories are always created.
    pub fn create(&self, path: &str, overwrite: bool) -> Result<BridgeWriter, HdfsError> {
        let opts = WriteOptions::default()
            .overwrite(overwrite)
            .create_parent(true);
        let inner = self.block_on(self.inner.create(path, opts))?;
        Ok(BridgeWriter {
            inner,
            rt: Arc::clone(&self.rt),
        })
    }

    /// Children of directory `path`, the whole subtree when `recursive`. For
    /// large or recursive listings prefer [`BridgeClient::list_stream`], which
    /// doesn't materialize the result and can parallelize the walk.
    pub fn list_status(&self, path: &str, recursive: bool) -> Result<Vec<FileStatus>, HdfsError> {
        self.block_on(self.inner.list_status(path, recursive))
    }

    /// Start a streaming listing of `path`. When `recursive` is true the whole
    /// subtree is walked, with `max_parallelism` (clamped to at least 1)
    /// bounding the number of concurrent listing RPCs. Never fails; errors
    /// (including not-found) surface on the first `next_batch` call.
    pub fn list_stream(
        &self,
        path: String,
        recursive: bool,
        max_parallelism: usize,
    ) -> BridgeListStream {
        let (tx, rx) = mpsc::channel(LIST_STREAM_BUFFER);
        let task = self.rt.spawn(stream_walk(
            self.inner.clone(),
            path.clone(),
            recursive,
            max_parallelism.max(1),
            tx,
        ));
        BridgeListStream {
            rx,
            task: Some(task),
            rt: Arc::clone(&self.rt),
            path,
            pending_error: None,
        }
    }

    /// Entries matching a glob `pattern`.
    pub fn glob_status(&self, pattern: &str) -> Result<Vec<FileStatus>, HdfsError> {
        self.block_on(self.inner.glob_status(pattern))
    }

    /// Create directory `path` (and any missing parents) with mode 0o755.
    pub fn mkdirs(&self, path: &str) -> Result<(), HdfsError> {
        self.block_on(self.inner.mkdirs(path, 0o755, true))
    }

    /// Delete `path`. `recursive` must be true to remove a non-empty
    /// directory. `Ok(false)` means the server deleted nothing.
    pub fn delete(&self, path: &str, recursive: bool) -> Result<bool, HdfsError> {
        self.block_on(self.inner.delete(path, recursive))
    }

    /// Rename `src` to `dst`.
    pub fn rename(&self, src: &str, dst: &str, overwrite: bool) -> Result<(), HdfsError> {
        self.block_on(self.inner.rename(src, dst, overwrite))
    }
}

/// An open file reader; see [`BridgeClient`] for the runtime coupling.
pub struct BridgeReader {
    inner: FileReader,
    rt: Arc<Runtime>,
}

impl BridgeReader {
    /// Total file length (cached on the reader, no RPC).
    pub fn file_length(&self) -> usize {
        self.inner.file_length()
    }

    /// Read exactly `buf.len()` bytes starting at `offset`. The caller must
    /// ensure the range lies within the file. Thread-safe: takes `&self`.
    pub fn read_range_buf(&self, buf: &mut [u8], offset: usize) -> Result<(), HdfsError> {
        self.rt.block_on(self.inner.read_range_buf(buf, offset))
    }
}

/// An open file writer; see [`BridgeClient`] for the runtime coupling.
pub struct BridgeWriter {
    inner: FileWriter,
    rt: Arc<Runtime>,
}

impl BridgeWriter {
    /// Append `data` to the file. `write_bytes` loops until the whole buffer
    /// is written (or errors), so a success is always a full write.
    pub fn write(&mut self, data: &[u8]) -> Result<(), HdfsError> {
        self.rt
            .block_on(self.inner.write_bytes(Bytes::copy_from_slice(data)))
            .map(|_| ())
    }

    /// Flush and close the file, consuming the writer.
    pub fn close(self) -> Result<(), HdfsError> {
        let BridgeWriter { mut inner, rt } = self;
        rt.block_on(inner.close())
    }
}

// --- streaming listing -------------------------------------------------------

/// Entries buffered between the walker task and the consumer before
/// backpressure pauses the walk.
const LIST_STREAM_BUFFER: usize = 8192;

/// A streaming (optionally recursive, optionally parallel) directory listing.
/// A background task on the client's runtime walks the tree and feeds entries
/// through a bounded channel; [`BridgeListStream::next_batch`] drains it in
/// batches. Entries arrive in completion order, not DFS order.
pub struct BridgeListStream {
    rx: mpsc::Receiver<Result<FileStatus, HdfsError>>,
    /// Taken (awaited) once the channel closes, to distinguish a completed
    /// walk from a panicked one.
    task: Option<tokio::task::JoinHandle<()>>,
    rt: Arc<Runtime>,
    /// The listed path, for error messages.
    path: String,
    /// An error received mid-batch; surfaced on the following `next_batch`.
    pending_error: Option<HdfsError>,
}

impl Drop for BridgeListStream {
    fn drop(&mut self) {
        // Cancel an unfinished walk promptly instead of letting it fill the
        // channel and stall until the runtime dies.
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl BridgeListStream {
    /// The listed path, for error messages.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Fetch the next batch of entries (at most `max_entries`, clamped to at
    /// least 1), blocking until one is available. An empty batch means the
    /// listing is exhausted. Not thread-safe; drive each stream from one
    /// thread at a time.
    pub fn next_batch(&mut self, max_entries: usize) -> Result<Vec<FileStatus>, HdfsError> {
        if let Some(e) = self.pending_error.take() {
            return Err(e);
        }
        let first = match self.rt.block_on(self.rx.recv()) {
            Some(Ok(entry)) => entry,
            Some(Err(e)) => return Err(e),
            None => {
                // Channel closed: the walk finished — or its task died without
                // reporting, which must not masquerade as a clean end of
                // stream.
                if let Some(task) = self.task.take() {
                    if let Err(e) = self.rt.block_on(task) {
                        return Err(HdfsError::IOError(std::io::Error::other(format!(
                            "walker task died: {e}"
                        ))));
                    }
                }
                return Ok(Vec::new());
            }
        };
        let mut batch = vec![first];
        while batch.len() < max_entries.max(1) {
            match self.rx.try_recv() {
                Ok(Ok(entry)) => batch.push(entry),
                Ok(Err(e)) => {
                    // Hand out what we have; surface the error on the next call.
                    self.pending_error = Some(e);
                    break;
                }
                Err(_) => break, // channel empty or closed: the batch is done
            }
        }
        Ok(batch)
    }
}

/// Walk `root`, sending each discovered entry over `tx`. Directories are
/// listed from a flat work queue with up to `max_parallelism` listing RPCs in
/// flight; subdirectories found by any listing are appended to the queue. (A
/// scheduler loop rather than recursion, so poll depth stays constant no
/// matter how deep the tree is.) Sending blocks once the channel is full,
/// pausing the walk until the consumer catches up. Exits on the first error
/// (forwarded to the consumer) or when the consumer drops the stream.
async fn stream_walk(
    client: Client,
    root: String,
    recursive: bool,
    max_parallelism: usize,
    tx: mpsc::Sender<Result<FileStatus, HdfsError>>,
) {
    let client = &client;
    let mut pending = VecDeque::from([root]);
    let mut in_flight = FuturesUnordered::new();
    loop {
        while in_flight.len() < max_parallelism {
            match pending.pop_front() {
                Some(path) => in_flight.push(async move { client.list_status(&path, false).await }),
                None => break,
            }
        }
        match in_flight.next().await {
            Some(Ok(children)) => {
                for child in children {
                    if recursive && child.isdir {
                        pending.push_back(child.path.clone());
                    }
                    if tx.send(Ok(child)).await.is_err() {
                        return; // consumer dropped the stream
                    }
                }
            }
            Some(Err(e)) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
            None => return, // queue drained and nothing in flight: walk complete
        }
    }
}
