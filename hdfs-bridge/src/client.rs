//! Safe, blocking wrappers around the async `hdfs-native` client.
//!
//! Each handle pairs an async `hdfs-native` object with the Tokio runtime that
//! drives it and exposes blocking methods, so the FFI layer (`lib.rs`) never
//! touches async code or the underlying objects directly.
//!
//! All handles share one process-wide runtime (see [`shared_runtime`]):
//! clients are created per authority and again on every reconnect, so a
//! runtime per client (each with a worker thread per CPU core) would multiply
//! OS threads for no benefit. Handles still hold an `Arc` to the runtime so
//! an object whose `Drop` needs it (e.g. a writer releasing its file lease)
//! can never outlive it, whatever the drop order.

use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt as _};
use hdfs_native::client::FileStatus;
use hdfs_native::file::{FileReader, FileWriter};
use hdfs_native::{Client, ClientBuilder, HdfsError, WriteOptions};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::glob::{self, GlobPlan, Pos};

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

/// The process-wide Tokio runtime that drives all HDFS IO. Created on first
/// use and kept alive for the life of the process. A failure to start it is
/// returned rather than cached, so a later connect can try again.
fn shared_runtime() -> Result<Arc<Runtime>, HdfsError> {
    static RUNTIME: Mutex<Option<Arc<Runtime>>> = Mutex::new(None);
    // Recover a poisoned lock instead of panicking across the FFI boundary; a
    // poisoning panic can only have happened before the slot was written.
    let mut slot = RUNTIME.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(rt) = slot.as_ref() {
        return Ok(Arc::clone(rt));
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("hdfs-bridge-io")
        .build()
        .map_err(|e| {
            HdfsError::IOError(std::io::Error::new(
                e.kind(),
                format!("failed to start IO runtime: {e}"),
            ))
        })?;
    let rt = Arc::new(rt);
    *slot = Some(Arc::clone(&rt));
    Ok(rt)
}

/// An HDFS client plus the shared Tokio runtime its operations run on.
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
        let rt = shared_runtime()?;
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

    /// Start a streaming listing of `path`. When `recursive` is true the whole
    /// subtree is walked, with `max_parallelism` (clamped to at least 1)
    /// bounding the number of concurrent listing RPCs. Never fails; errors
    /// (including not-found) surface on the first `next_batch` call — except
    /// for subdirectories pruned per `options`. The path itself is taken as
    /// explicitly named: errors on it always surface, and it is never
    /// hidden-filtered.
    pub fn list_stream(
        &self,
        path: String,
        recursive: bool,
        max_parallelism: usize,
        options: WalkOptions,
    ) -> BridgeListStream {
        let (tx, rx) = mpsc::channel(LIST_STREAM_BUFFER);
        let task = self.rt.spawn(stream_walk(
            self.inner.clone(),
            path.clone(),
            recursive,
            max_parallelism.max(1),
            options,
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

    /// Start a streaming glob of `pattern` (see the [`crate::glob`] module for
    /// the supported syntax and the hidden-entry rules), returning matched
    /// entries — files and directories — themselves. `max_parallelism`
    /// (clamped to at least 1) bounds the number of concurrent listing RPCs.
    /// Fails only on an invalid pattern; a pattern matching nothing yields an
    /// empty stream. Permission errors below the pattern's literal root are
    /// pruned per `options`; on the root itself they always surface.
    pub fn glob_stream(
        &self,
        pattern: String,
        max_parallelism: usize,
        options: WalkOptions,
    ) -> Result<BridgeListStream, HdfsError> {
        let plan = GlobPlan::parse(&pattern, options.include_hidden)?;
        let (tx, rx) = mpsc::channel(LIST_STREAM_BUFFER);
        let task = self.rt.spawn(glob_walk(
            self.inner.clone(),
            plan,
            max_parallelism.max(1),
            options,
            tx,
        ));
        Ok(BridgeListStream {
            rx,
            task: Some(task),
            rt: Arc::clone(&self.rt),
            path: pattern,
            pending_error: None,
        })
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

/// Per-walk behavior toggles shared by listings and globs.
#[derive(Clone, Copy)]
pub struct WalkOptions {
    /// Prune subtrees whose listing fails with a permission error instead of
    /// failing the walk. Only applies below the walk root: an error on the
    /// path/pattern-root the caller named always surfaces, so a misconfigured
    /// principal can't masquerade as an empty result. All other error classes
    /// (connection, SASL, IO) always fail the walk.
    pub skip_permission_errors: bool,
    /// Return entries whose name starts with `.` or `_` (Hadoop's hidden
    /// convention). When false, hidden entries are neither returned nor
    /// descended into, except where a glob component names them explicitly
    /// (see [`crate::glob`]).
    pub include_hidden: bool,
}

/// A streaming (optionally recursive, optionally parallel) directory listing.
/// A background task on the client's runtime walks the tree and feeds entries
/// through a bounded channel; [`BridgeListStream::next_batch`] drains it in
/// batches. Entries arrive in completion order (the walk schedules
/// depth-first, but parallel RPCs finish out of order), so no path order is
/// guaranteed.
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
/// listed from a flat LIFO work stack with up to `max_parallelism` listing
/// RPCs in flight; subdirectories found by any listing are pushed onto the
/// stack. (A scheduler loop rather than recursion, so poll depth stays
/// constant no matter how deep the tree is.) LIFO makes the walk
/// depth-first-ish: the freshest discovery is listed next, keeping the gap
/// between listing a parent and listing its children small — which both
/// shrinks the window for a concurrent delete (Spark removing `_temporary`)
/// to strand queued children and bounds the stack by depth x fan-out rather
/// than the tree's widest level. Sending blocks once the channel is full,
/// pausing the walk until the consumer catches up. Exits on the first
/// non-pruned error (forwarded to the consumer) or when the consumer drops
/// the stream. Below the root, a subdirectory that vanished since its parent
/// was listed is pruned rather than an error, as is — with
/// `skip_permission_errors` — one the caller may not list.
async fn stream_walk(
    client: Client,
    root: String,
    recursive: bool,
    max_parallelism: usize,
    options: WalkOptions,
    tx: mpsc::Sender<Result<FileStatus, HdfsError>>,
) {
    let client = &client;
    let mut pending = vec![(root, true)];
    let mut in_flight = FuturesUnordered::new();
    loop {
        while in_flight.len() < max_parallelism {
            match pending.pop() {
                Some((path, is_root)) => in_flight.push(async move {
                    let listing = client.list_status(&path, false).await;
                    (path, is_root, listing)
                }),
                None => break,
            }
        }
        match in_flight.next().await {
            Some((path, _, Ok(children))) => {
                for child in children {
                    // Hidden filtering exempts `child.path == path` (listing a
                    // file returns the file itself): the caller named that
                    // path explicitly.
                    if !options.include_hidden && child.path != path {
                        let name = child
                            .path
                            .rsplit_once('/')
                            .map(|(_, n)| n)
                            .unwrap_or(child.path.as_str());
                        if glob::is_hidden(name) {
                            continue;
                        }
                    }
                    if recursive && child.isdir {
                        pending.push((child.path.clone(), false));
                    }
                    if tx.send(Ok(child)).await.is_err() {
                        return; // consumer dropped the stream
                    }
                }
            }
            Some((_, is_root, Err(e))) => {
                let prune = !is_root
                    && (matches!(e, HdfsError::FileNotFound(_))
                        || (options.skip_permission_errors && crate::is_permission(&e)));
                if !prune {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }
            None => return, // queue drained and nothing in flight: walk complete
        }
    }
}

/// Walk the tree from `plan`'s literal root, sending every entry matching the
/// glob over `tx`. Same scheduling as [`stream_walk`] (flat LIFO work stack,
/// up to `max_parallelism` listing RPCs in flight, backpressure via the
/// channel),
/// but each queued directory carries its NFA states and children are filtered
/// through [`GlobPlan::step`], which prunes descent to directories that can
/// still match (and applies the hidden-entry rules). Missing paths are pruned
/// silently (a glob matching nothing is not an error), and so — with
/// `skip_permission_errors`, below the pattern's literal root — are
/// directories the caller may not list; any other error is forwarded and ends
/// the walk.
async fn glob_walk(
    client: Client,
    plan: GlobPlan,
    max_parallelism: usize,
    options: WalkOptions,
    tx: mpsc::Sender<Result<FileStatus, HdfsError>>,
) {
    let client = &client;
    // An all-literal pattern (or brace alternative) matches the root itself.
    if plan.emit_root() {
        match client.get_file_info(plan.root()).await {
            Ok(status) => {
                if tx.send(Ok(status)).await.is_err() {
                    return;
                }
            }
            Err(HdfsError::FileNotFound(_)) => {}
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        }
    }
    let mut pending: Vec<(String, Vec<Pos>, bool)> = Vec::new();
    if !plan.initial().is_empty() {
        pending.push((plan.root().to_string(), plan.initial().to_vec(), true));
    }
    let mut in_flight = FuturesUnordered::new();
    loop {
        while in_flight.len() < max_parallelism {
            match pending.pop() {
                Some((path, states, is_root)) => in_flight.push(async move {
                    let listing = client.list_status(&path, false).await;
                    (path, states, is_root, listing)
                }),
                None => break,
            }
        }
        match in_flight.next().await {
            Some((path, states, _, Ok(children))) => {
                for child in children {
                    // Listing a file returns the file itself; it has no
                    // children to match.
                    if child.path == path {
                        continue;
                    }
                    let name = child
                        .path
                        .rsplit_once('/')
                        .map(|(_, n)| n)
                        .unwrap_or(child.path.as_str());
                    let step = plan.step(&states, name, child.isdir);
                    if child.isdir && !step.next.is_empty() {
                        pending.push((child.path.clone(), step.next, false));
                    }
                    if step.emit && tx.send(Ok(child)).await.is_err() {
                        return; // consumer dropped the stream
                    }
                }
            }
            // The root (or, in a race, a directory found earlier) is gone:
            // that's zero matches down this branch, not an error.
            Some((_, _, _, Err(HdfsError::FileNotFound(_)))) => {}
            // A forbidden directory below the root: zero matches down this
            // branch when the caller opted in; the root stays strict.
            Some((_, _, false, Err(ref e))) if options.skip_permission_errors && crate::is_permission(e) => {}
            Some((_, _, _, Err(e))) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
            None => return, // queue drained and nothing in flight: walk complete
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn runtime_is_process_wide() {
        let a = shared_runtime().unwrap();
        let b = shared_runtime().unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }
}
