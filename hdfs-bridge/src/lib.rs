//! C FFI bridge exposing a blocking subset of the `hdfs-native` client to the
//! DuckDB HDFS extension (C++).
//!
//! The bridge drives the *async* `hdfs-native` client on a Tokio runtime it
//! owns, blocking at the FFI boundary (rather than using `hdfs_native::sync`,
//! which hides its runtime). Owning the runtime lets the bridge run custom
//! concurrent operations — notably the parallel streaming listing behind
//! [`hdfs_bridge_list_stream_open`].
//!
//! ## Conventions
//!
//! * Every fallible function takes a trailing `status: *mut Status`. On success
//!   it is left untouched (the caller initializes it to `{HDFS_OK, null}`). On
//!   failure the function writes a category code and a heap-allocated,
//!   NUL-terminated message; the caller must free the message with
//!   [`hdfs_bridge_free_string`]. The category lets the C++ side react to the
//!   *kind* of failure (not-found vs unreachable cluster vs bad argument)
//!   without parsing message strings.
//! * Opaque handles (`Client`, `FileReader`, `FileWriter`) are returned as raw
//!   `Box` pointers and must be released with their matching `free`/`close`
//!   function.
//! * All returned C strings / arrays are owned by the caller and must be freed
//!   with the matching `hdfs_bridge_free_*` function.

use std::collections::VecDeque;
use std::ffi::{CStr, CString};
use std::future::Future;
use std::os::raw::c_char;
use std::ptr;
use std::slice;
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

// Opaque handle types behind the FFI pointers. Each pairs an async
// `hdfs-native` object with the runtime that drives it; readers/writers hold
// their own `Arc` so they stay usable even if the client is freed first.
//
// Field order matters: `inner` is declared before `rt` so the hdfs-native
// object (whose `Drop` may use the runtime, e.g. a writer releasing its file
// lease) is dropped while the runtime is still alive.

/// An HDFS client plus the Tokio runtime all its operations run on.
pub struct BridgeClient {
    inner: Client,
    rt: Arc<Runtime>,
}

impl BridgeClient {
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.rt.block_on(future)
    }
}

/// An open file reader; see [`BridgeClient`] for the runtime coupling.
pub struct BridgeReader {
    inner: FileReader,
    rt: Arc<Runtime>,
}

/// An open file writer; see [`BridgeClient`] for the runtime coupling.
pub struct BridgeWriter {
    inner: FileWriter,
    rt: Arc<Runtime>,
}

// Error categories shared with the C++ side. Keep in sync with
// `hdfs_error_code_t` in `hdfs_bridge.h`.
#[allow(dead_code)] // success leaves status untouched; kept for parity with the header
const HDFS_OK: i32 = 0;
const HDFS_ERR_IO: i32 = 1;
const HDFS_ERR_NOT_FOUND: i32 = 2;
const HDFS_ERR_PERMISSION: i32 = 3;
const HDFS_ERR_ALREADY_EXISTS: i32 = 4;
const HDFS_ERR_CONNECTION: i32 = 5;
const HDFS_ERR_INVALID_ARGUMENT: i32 = 6;

/// FFI result struct, mirrored by `hdfs_status_t` in `hdfs_bridge.h`.
#[repr(C)]
pub struct Status {
    pub code: i32,
    pub msg: *mut c_char,
}

/// Information about a single file or directory, mirrored in `hdfs_bridge.h`.
#[repr(C)]
pub struct FileInfo {
    pub length: i64,
    pub is_dir: bool,
    pub mtime: u64,
}

/// One entry in a directory listing or glob result, mirrored in
/// `hdfs_bridge.h`. `path`, `owner` and `group` are owned C strings.
/// `replication` and `block_size` use `-1` to mean "not applicable" (HDFS
/// leaves them unset for directories).
#[repr(C)]
pub struct DirEntry {
    pub path: *mut c_char,
    pub is_dir: bool,
    pub length: i64,
    pub mtime: u64,
    pub atime: u64,
    pub owner: *mut c_char,
    pub group: *mut c_char,
    pub permission: u16,
    pub replication: i32,
    pub block_size: i64,
}

// --- error helpers ---------------------------------------------------------

/// Map an `HdfsError` to one of the FFI error categories. The RPC and IO arms
/// dig into the underlying Hadoop exception class / IO error kind so the C++
/// side can, for example, distinguish "access denied" or "namenode in standby"
/// (retryable) from a generic failure.
fn classify(err: &HdfsError) -> i32 {
    match err {
        HdfsError::FileNotFound(_) => HDFS_ERR_NOT_FOUND,
        HdfsError::AlreadyExists(_) => HDFS_ERR_ALREADY_EXISTS,
        HdfsError::InvalidPath(_) | HdfsError::InvalidArgument(_) | HdfsError::UrlParseError(_) => {
            HDFS_ERR_INVALID_ARGUMENT
        }
        HdfsError::RPCError(class, _) | HdfsError::FatalRPCError(class, _) => classify_rpc(class),
        HdfsError::SASLError(_)
        | HdfsError::GSSAPIError(..)
        | HdfsError::NoSASLMechanism
        | HdfsError::DataTransferError(_)
        | HdfsError::BlocksNotFound(_) => HDFS_ERR_CONNECTION,
        HdfsError::IOError(io) => classify_io(io),
        _ => HDFS_ERR_IO,
    }
}

/// Classify a Hadoop server-side exception by its Java class name.
fn classify_rpc(class: &str) -> i32 {
    if class.contains("AccessControlException") || class.contains("SecurityException") {
        HDFS_ERR_PERMISSION
    } else if class.contains("FileNotFoundException") {
        HDFS_ERR_NOT_FOUND
    } else if class.contains("FileAlreadyExistsException") || class.contains("AlreadyBeingCreated")
    {
        HDFS_ERR_ALREADY_EXISTS
    } else if class.contains("StandbyException") || class.contains("RetriableException") {
        // Namenode failover / retryable: the cached client should reconnect.
        HDFS_ERR_CONNECTION
    } else {
        HDFS_ERR_IO
    }
}

/// Classify a transport-level `std::io::Error` from the Rust side.
fn classify_io(io: &std::io::Error) -> i32 {
    use std::io::ErrorKind;
    match io.kind() {
        ErrorKind::NotFound => HDFS_ERR_NOT_FOUND,
        ErrorKind::PermissionDenied => HDFS_ERR_PERMISSION,
        ErrorKind::AlreadyExists => HDFS_ERR_ALREADY_EXISTS,
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::BrokenPipe
        | ErrorKind::TimedOut
        | ErrorKind::UnexpectedEof => HDFS_ERR_CONNECTION,
        _ => HDFS_ERR_IO,
    }
}

/// Write `code` and `msg` into `*status`, if `status` is non-null.
unsafe fn set_status(status: *mut Status, code: i32, msg: impl std::fmt::Display) {
    if status.is_null() {
        return;
    }
    // Replace interior NULs so CString::new never fails.
    let cleaned: String = msg.to_string().replace('\0', " ");
    let cmsg = match CString::new(cleaned) {
        Ok(c) => c.into_raw(),
        Err(_) => ptr::null_mut(),
    };
    unsafe {
        (*status).code = code;
        (*status).msg = cmsg;
    }
}

/// Write a classified `HdfsError` with a context prefix into `*status`.
unsafe fn set_error(status: *mut Status, context: impl std::fmt::Display, err: &HdfsError) {
    unsafe { set_status(status, classify(err), format_args!("{context}: {err}")) }
}

/// Convert a C string pointer into an owned `String`, returning `None` for null
/// or empty input.
unsafe fn opt_str(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let s = unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// Free a C string previously returned by the bridge (status messages, etc.).
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

// --- client ----------------------------------------------------------------

/// Connect to HDFS. `url` (e.g. `hdfs://namenode:8020`), `config_dir`, and
/// `user` are all optional (pass null or empty to omit).
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_connect(
    url: *const c_char,
    config_dir: *const c_char,
    user: *const c_char,
    status: *mut Status,
) -> *mut BridgeClient {
    let rt = match Runtime::new() {
        Ok(rt) => Arc::new(rt),
        Err(e) => {
            unsafe {
                set_status(
                    status,
                    classify_io(&e),
                    format_args!("failed to start IO runtime: {e}"),
                )
            };
            return ptr::null_mut();
        }
    };
    // The client spawns its IO tasks on our runtime; a plain `build()` would
    // lazily create a second, hidden runtime.
    let mut builder = ClientBuilder::new().with_io_runtime(rt.handle().clone());
    if let Some(url) = unsafe { opt_str(url) } {
        builder = builder.with_url(url);
    }
    if let Some(dir) = unsafe { opt_str(config_dir) } {
        builder = builder.with_config_dir(dir);
    }
    if let Some(user) = unsafe { opt_str(user) } {
        builder = builder.with_user(user);
    }

    match builder.build() {
        Ok(client) => Box::into_raw(Box::new(BridgeClient { inner: client, rt })),
        Err(e) => {
            unsafe { set_error(status, "failed to connect to HDFS", &e) };
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_free_client(client: *mut BridgeClient) {
    if !client.is_null() {
        unsafe { drop(Box::from_raw(client)) };
    }
}

// --- stat ------------------------------------------------------------------

/// Fill `out` with metadata for `path`. Returns 0 on success, -1 on error
/// (including not-found; check `status->code`).
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_get_file_info(
    client: *mut BridgeClient,
    path: *const c_char,
    out: *mut FileInfo,
    status: *mut Status,
) -> i32 {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.block_on(client.inner.get_file_info(&path)) {
        Ok(info) => {
            unsafe {
                (*out).length = info.length as i64;
                (*out).is_dir = info.isdir;
                (*out).mtime = info.modification_time;
            }
            0
        }
        Err(e) => {
            unsafe { set_error(status, format_args!("stat '{path}' failed"), &e) };
            -1
        }
    }
}

// --- reader ----------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_open(
    client: *mut BridgeClient,
    path: *const c_char,
    status: *mut Status,
) -> *mut BridgeReader {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.block_on(client.inner.read(&path)) {
        Ok(reader) => Box::into_raw(Box::new(BridgeReader {
            inner: reader,
            rt: Arc::clone(&client.rt),
        })),
        Err(e) => {
            unsafe { set_error(status, format_args!("open '{path}' for reading failed"), &e) };
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_close_reader(reader: *mut BridgeReader) {
    if !reader.is_null() {
        unsafe { drop(Box::from_raw(reader)) };
    }
}

/// Total file length (cached on the reader, no RPC).
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_file_size(reader: *mut BridgeReader) -> i64 {
    let reader = unsafe { &*reader };
    reader.inner.file_length() as i64
}

/// Read exactly `len` bytes into `buf` starting at `offset`. The caller must
/// ensure `offset + len <= file_size`. Returns the number of bytes read
/// (`len`) on success, or -1 on error. Thread-safe: takes `&self`.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_read_range(
    reader: *mut BridgeReader,
    buf: *mut u8,
    len: i64,
    offset: u64,
    status: *mut Status,
) -> i64 {
    if len < 0 {
        unsafe { set_status(status, HDFS_ERR_INVALID_ARGUMENT, "negative read length") };
        return -1;
    }
    let reader = unsafe { &*reader };
    let slice = unsafe { slice::from_raw_parts_mut(buf, len as usize) };
    match reader
        .rt
        .block_on(reader.inner.read_range_buf(slice, offset as usize))
    {
        Ok(()) => len,
        Err(e) => {
            unsafe { set_error(status, format_args!("read at offset {offset} failed"), &e) };
            -1
        }
    }
}

// --- writer ----------------------------------------------------------------

/// Create a file for writing. `overwrite` controls whether an existing file is
/// replaced; missing parent directories are always created.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_create(
    client: *mut BridgeClient,
    path: *const c_char,
    overwrite: bool,
    status: *mut Status,
) -> *mut BridgeWriter {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    let opts = WriteOptions::default()
        .overwrite(overwrite)
        .create_parent(true);
    match client.block_on(client.inner.create(&path, opts)) {
        Ok(writer) => Box::into_raw(Box::new(BridgeWriter {
            inner: writer,
            rt: Arc::clone(&client.rt),
        })),
        Err(e) => {
            unsafe {
                set_error(
                    status,
                    format_args!("create '{path}' for writing failed"),
                    &e,
                )
            };
            ptr::null_mut()
        }
    }
}

/// Append `len` bytes from `buf` to the file. Returns bytes written, or -1.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_write(
    writer: *mut BridgeWriter,
    buf: *const u8,
    len: i64,
    status: *mut Status,
) -> i64 {
    if len < 0 {
        unsafe { set_status(status, HDFS_ERR_INVALID_ARGUMENT, "negative write length") };
        return -1;
    }
    let writer = unsafe { &mut *writer };
    let slice = unsafe { slice::from_raw_parts(buf, len as usize) };
    // write_bytes loops until the whole buffer is written (or errors), so a
    // success is always a full write of `len` bytes.
    match writer
        .rt
        .block_on(writer.inner.write_bytes(Bytes::copy_from_slice(slice)))
    {
        Ok(_) => len,
        Err(e) => {
            unsafe { set_error(status, "write failed", &e) };
            -1
        }
    }
}

/// Flush and close the writer, consuming the handle. Returns 0 or -1. The
/// handle must not be used afterwards regardless of the result.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_close_writer(
    writer: *mut BridgeWriter,
    status: *mut Status,
) -> i32 {
    if writer.is_null() {
        return 0;
    }
    let writer = unsafe { Box::from_raw(writer) };
    let BridgeWriter { mut inner, rt } = *writer;
    match rt.block_on(inner.close()) {
        Ok(()) => 0,
        Err(e) => {
            unsafe { set_error(status, "close writer failed", &e) };
            -1
        }
    }
}

// --- directory operations --------------------------------------------------

/// Build an owned C string, falling back to an empty string on the (impossible
/// for HDFS) interior-NUL case rather than panicking across the FFI boundary.
fn to_c_string(s: String) -> *mut c_char {
    CString::new(s)
        .unwrap_or_else(|_| CString::new("").unwrap())
        .into_raw()
}

/// Convert a single `FileStatus` into an owned `DirEntry`. Callers must free the
/// entry's strings (`path`/`owner`/`group`) via `hdfs_bridge_free_dir_entries`.
fn status_to_entry(status: hdfs_native::client::FileStatus) -> DirEntry {
    DirEntry {
        path: to_c_string(status.path),
        is_dir: status.isdir,
        length: status.length as i64,
        mtime: status.modification_time,
        atime: status.access_time,
        owner: to_c_string(status.owner),
        group: to_c_string(status.group),
        permission: status.permission,
        // Replication and block size apply only to files; `-1` signals "not
        // applicable" so the C++ side surfaces SQL NULL. The NameNode reports 0
        // (not absent) for directories, so gate on `isdir` rather than the
        // Option being None.
        replication: if status.isdir {
            -1
        } else {
            status.replication.map(|r| r as i32).unwrap_or(-1)
        },
        block_size: if status.isdir {
            -1
        } else {
            status.blocksize.map(|b| b as i64).unwrap_or(-1)
        },
    }
}

/// Build a heap array of `DirEntry` from file statuses and hand ownership to
/// the caller. Returns null and sets `*out_count = 0` for an empty list.
fn statuses_to_entries(
    statuses: Vec<hdfs_native::client::FileStatus>,
    out_count: *mut i32,
) -> *mut DirEntry {
    let count = statuses.len();
    unsafe { *out_count = count as i32 };
    if count == 0 {
        return ptr::null_mut();
    }
    let entries: Vec<DirEntry> = statuses.into_iter().map(status_to_entry).collect();
    let boxed = entries.into_boxed_slice();
    Box::into_raw(boxed) as *mut DirEntry
}

/// Glob `pattern`, returning matching entries. `out_count` receives the count.
/// A null return with `status->code == HDFS_OK` means no matches.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_glob(
    client: *mut BridgeClient,
    pattern: *const c_char,
    out_count: *mut i32,
    status: *mut Status,
) -> *mut DirEntry {
    let client = unsafe { &*client };
    let pattern = unsafe { CStr::from_ptr(pattern) }.to_string_lossy();
    match client.block_on(client.inner.glob_status(&pattern)) {
        Ok(statuses) => statuses_to_entries(statuses, out_count),
        Err(e) => {
            unsafe {
                *out_count = 0;
                set_error(status, format_args!("glob '{pattern}' failed"), &e);
            }
            ptr::null_mut()
        }
    }
}

/// List the children of directory `path`. When `recursive` is true the whole
/// subtree is walked. A null return with `status->code == HDFS_OK` means an
/// empty directory. For large or recursive listings prefer the streaming API
/// ([`hdfs_bridge_list_stream_open`]), which doesn't materialize the result
/// and can parallelize the walk.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_list_status(
    client: *mut BridgeClient,
    path: *const c_char,
    recursive: bool,
    out_count: *mut i32,
    status: *mut Status,
) -> *mut DirEntry {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.block_on(client.inner.list_status(&path, recursive)) {
        Ok(statuses) => statuses_to_entries(statuses, out_count),
        Err(e) => {
            unsafe {
                *out_count = 0;
                set_error(status, format_args!("list '{path}' failed"), &e);
            }
            ptr::null_mut()
        }
    }
}

// --- streaming listing -------------------------------------------------------

/// Entries buffered between the walker task and the consumer before
/// backpressure pauses the walk.
const LIST_STREAM_BUFFER: usize = 8192;

/// A streaming (optionally recursive, optionally parallel) directory listing.
/// A background task on the client's runtime walks the tree and feeds entries
/// through a bounded channel; `hdfs_bridge_list_stream_next` drains it in
/// batches. Entries arrive in completion order, not DFS order.
pub struct BridgeListStream {
    rx: mpsc::Receiver<Result<FileStatus, HdfsError>>,
    /// Taken (awaited) once the channel closes, to distinguish a completed
    /// walk from a panicked one.
    task: Option<tokio::task::JoinHandle<()>>,
    rt: Arc<Runtime>,
    /// The listed path, for error messages.
    path: String,
    /// An error received mid-batch; surfaced on the following `next` call.
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

/// Start a streaming listing of `path`. When `recursive` is true the whole
/// subtree is walked, with `max_parallelism` bounding the number of concurrent
/// listing RPCs (values <= 1 list one directory at a time). Opening never
/// fails; errors (including not-found) surface on the first `next` call.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_list_stream_open(
    client: *mut BridgeClient,
    path: *const c_char,
    recursive: bool,
    max_parallelism: i32,
) -> *mut BridgeListStream {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }
        .to_string_lossy()
        .into_owned();
    let (tx, rx) = mpsc::channel(LIST_STREAM_BUFFER);
    let task = client.rt.spawn(stream_walk(
        client.inner.clone(),
        path.clone(),
        recursive,
        max_parallelism.max(1) as usize,
        tx,
    ));
    Box::into_raw(Box::new(BridgeListStream {
        rx,
        task: Some(task),
        rt: Arc::clone(&client.rt),
        path,
        pending_error: None,
    }))
}

/// Fetch the next batch of entries (at most `max_entries`), blocking until at
/// least one is available. Returns null with `*out_count == 0` and an OK
/// status when the listing is exhausted, or null with a non-OK status on
/// error. Streams are not thread-safe; drive each from one thread at a time.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_list_stream_next(
    stream: *mut BridgeListStream,
    max_entries: i32,
    out_count: *mut i32,
    status: *mut Status,
) -> *mut DirEntry {
    let stream = unsafe { &mut *stream };
    unsafe { *out_count = 0 };
    if let Some(e) = stream.pending_error.take() {
        let path = &stream.path;
        unsafe { set_error(status, format_args!("list '{path}' failed"), &e) };
        return ptr::null_mut();
    }
    let first = match stream.rt.block_on(stream.rx.recv()) {
        Some(Ok(entry)) => entry,
        Some(Err(e)) => {
            let path = &stream.path;
            unsafe { set_error(status, format_args!("list '{path}' failed"), &e) };
            return ptr::null_mut();
        }
        None => {
            // Channel closed: the walk finished — or its task died without
            // reporting, which must not masquerade as a clean end of stream.
            if let Some(task) = stream.task.take() {
                if let Err(e) = stream.rt.block_on(task) {
                    let path = &stream.path;
                    unsafe {
                        set_status(
                            status,
                            HDFS_ERR_IO,
                            format_args!("list '{path}' failed: walker task died: {e}"),
                        )
                    };
                }
            }
            return ptr::null_mut();
        }
    };
    let mut batch = vec![first];
    while batch.len() < max_entries.max(1) as usize {
        match stream.rx.try_recv() {
            Ok(Ok(entry)) => batch.push(entry),
            Ok(Err(e)) => {
                // Hand out what we have; surface the error on the next call.
                stream.pending_error = Some(e);
                break;
            }
            Err(_) => break, // channel empty or closed: the batch is done
        }
    }
    statuses_to_entries(batch, out_count)
}

#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_list_stream_free(stream: *mut BridgeListStream) {
    if !stream.is_null() {
        unsafe { drop(Box::from_raw(stream)) };
    }
}

/// Free an array of `DirEntry` returned by glob/list, including each path.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_free_dir_entries(entries: *mut DirEntry, count: i32) {
    if entries.is_null() || count <= 0 {
        return;
    }
    let slice = unsafe { Box::from_raw(slice::from_raw_parts_mut(entries, count as usize)) };
    for entry in slice.iter() {
        for s in [entry.path, entry.owner, entry.group] {
            if !s.is_null() {
                unsafe { drop(CString::from_raw(s)) };
            }
        }
    }
}

/// Stat a single `path`, returning a one-element `DirEntry` array (freed with
/// `hdfs_bridge_free_dir_entries(ptr, 1)`). Returns null on error (including
/// not-found; check `status->code`). This is the rich counterpart to
/// `hdfs_bridge_get_file_info`, which stays lean for the internal hot path.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_stat(
    client: *mut BridgeClient,
    path: *const c_char,
    status: *mut Status,
) -> *mut DirEntry {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.block_on(client.inner.get_file_info(&path)) {
        Ok(info) => {
            let boxed = vec![status_to_entry(info)].into_boxed_slice();
            Box::into_raw(boxed) as *mut DirEntry
        }
        Err(e) => {
            unsafe { set_error(status, format_args!("stat '{path}' failed"), &e) };
            ptr::null_mut()
        }
    }
}

// --- mutations -------------------------------------------------------------

/// Create directory `path` (and any missing parents) with mode 0o755.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_mkdirs(
    client: *mut BridgeClient,
    path: *const c_char,
    status: *mut Status,
) -> i32 {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.block_on(client.inner.mkdirs(&path, 0o755, true)) {
        Ok(()) => 0,
        Err(e) => {
            unsafe { set_error(status, format_args!("mkdirs '{path}' failed"), &e) };
            -1
        }
    }
}

/// Delete `path`. `recursive` must be true to remove a non-empty directory.
/// A server response of "false" (nothing deleted) is reported as not-found.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_delete(
    client: *mut BridgeClient,
    path: *const c_char,
    recursive: bool,
    status: *mut Status,
) -> i32 {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.block_on(client.inner.delete(&path, recursive)) {
        Ok(true) => 0,
        Ok(false) => {
            unsafe {
                set_status(
                    status,
                    HDFS_ERR_NOT_FOUND,
                    format_args!("delete '{path}': path not found"),
                )
            };
            -1
        }
        Err(e) => {
            unsafe { set_error(status, format_args!("delete '{path}' failed"), &e) };
            -1
        }
    }
}

/// Rename `src` to `dst`.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_rename(
    client: *mut BridgeClient,
    src: *const c_char,
    dst: *const c_char,
    overwrite: bool,
    status: *mut Status,
) -> i32 {
    let client = unsafe { &*client };
    let src = unsafe { CStr::from_ptr(src) }.to_string_lossy();
    let dst = unsafe { CStr::from_ptr(dst) }.to_string_lossy();
    match client.block_on(client.inner.rename(&src, &dst, overwrite)) {
        Ok(()) => 0,
        Err(e) => {
            unsafe { set_error(status, format_args!("rename '{src}' -> '{dst}' failed"), &e) };
            -1
        }
    }
}
