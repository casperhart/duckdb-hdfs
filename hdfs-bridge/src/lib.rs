//! C FFI bridge exposing a synchronous subset of the `hdfs-native` client to
//! the DuckDB HDFS extension (C++).
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

use std::ffi::{CStr, CString};
use std::io::Write as _;
use std::os::raw::c_char;
use std::ptr;
use std::slice;

use hdfs_native::client::WriteOptions;
use hdfs_native::sync::{Client, ClientBuilder, FileReader, FileWriter};
use hdfs_native::HdfsError;

// The C++ filesystem issues concurrent positional reads against a single
// `FileReader` (DuckDB's parquet reader reads ranges from multiple threads on
// one handle). That is only sound if `FileReader` is `Sync`. Assert it at
// compile time so an upstream change can't silently break the bridge.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<FileReader>();
    assert_send_sync::<Client>();
};

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
/// `hdfs_bridge.h`. `path` is an owned C string.
#[repr(C)]
pub struct DirEntry {
    pub path: *mut c_char,
    pub is_dir: bool,
    pub length: i64,
    pub mtime: u64,
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
        HdfsError::InvalidPath(_)
        | HdfsError::InvalidArgument(_)
        | HdfsError::UrlParseError(_) => HDFS_ERR_INVALID_ARGUMENT,
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
    } else if class.contains("FileAlreadyExistsException") || class.contains("AlreadyBeingCreated") {
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
    let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
    if s.is_empty() { None } else { Some(s) }
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
) -> *mut Client {
    let mut builder = ClientBuilder::new();
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
        Ok(client) => Box::into_raw(Box::new(client)),
        Err(e) => {
            unsafe { set_error(status, "failed to connect to HDFS", &e) };
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_free_client(client: *mut Client) {
    if !client.is_null() {
        unsafe { drop(Box::from_raw(client)) };
    }
}

// --- stat ------------------------------------------------------------------

/// Fill `out` with metadata for `path`. Returns 0 on success, -1 on error
/// (including not-found; check `status->code`).
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_get_file_info(
    client: *mut Client,
    path: *const c_char,
    out: *mut FileInfo,
    status: *mut Status,
) -> i32 {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.get_file_info(&path) {
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
    client: *mut Client,
    path: *const c_char,
    status: *mut Status,
) -> *mut FileReader {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.read(&path) {
        Ok(reader) => Box::into_raw(Box::new(reader)),
        Err(e) => {
            unsafe { set_error(status, format_args!("open '{path}' for reading failed"), &e) };
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_close_reader(reader: *mut FileReader) {
    if !reader.is_null() {
        unsafe { drop(Box::from_raw(reader)) };
    }
}

/// Total file length (cached on the reader, no RPC).
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_file_size(reader: *mut FileReader) -> i64 {
    let reader = unsafe { &*reader };
    reader.file_length() as i64
}

/// Read exactly `len` bytes into `buf` starting at `offset`. The caller must
/// ensure `offset + len <= file_size`. Returns the number of bytes read
/// (`len`) on success, or -1 on error. Thread-safe: takes `&self`.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_read_range(
    reader: *mut FileReader,
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
    match reader.read_range_buf(slice, offset as usize) {
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
    client: *mut Client,
    path: *const c_char,
    overwrite: bool,
    status: *mut Status,
) -> *mut FileWriter {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    let opts = WriteOptions::default().overwrite(overwrite).create_parent(true);
    match client.create(&path, opts) {
        Ok(writer) => Box::into_raw(Box::new(writer)),
        Err(e) => {
            unsafe { set_error(status, format_args!("create '{path}' for writing failed"), &e) };
            ptr::null_mut()
        }
    }
}

/// Append `len` bytes from `buf` to the file. Returns bytes written, or -1.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_write(
    writer: *mut FileWriter,
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
    match writer.write_all(slice) {
        Ok(()) => len,
        Err(e) => {
            // write_all comes from std::io::Write, so this is a std::io::Error.
            unsafe { set_status(status, classify_io(&e), format_args!("write failed: {e}")) };
            -1
        }
    }
}

/// Flush and close the writer, consuming the handle. Returns 0 or -1. The
/// handle must not be used afterwards regardless of the result.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_close_writer(
    writer: *mut FileWriter,
    status: *mut Status,
) -> i32 {
    if writer.is_null() {
        return 0;
    }
    let mut writer = unsafe { Box::from_raw(writer) };
    match writer.close() {
        Ok(()) => 0,
        Err(e) => {
            unsafe { set_error(status, "close writer failed", &e) };
            -1
        }
    }
}

// --- directory operations --------------------------------------------------

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
    let mut entries: Vec<DirEntry> = Vec::with_capacity(count);
    for status in statuses {
        // path comes back scheme-less ("/a/b"); interior NULs are impossible
        // in HDFS paths, but guard anyway.
        let c_path = CString::new(status.path).unwrap_or_else(|_| CString::new("").unwrap());
        entries.push(DirEntry {
            path: c_path.into_raw(),
            is_dir: status.isdir,
            length: status.length as i64,
            mtime: status.modification_time,
        });
    }
    let boxed = entries.into_boxed_slice();
    Box::into_raw(boxed) as *mut DirEntry
}

/// Glob `pattern`, returning matching entries. `out_count` receives the count.
/// A null return with `status->code == HDFS_OK` means no matches.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_glob(
    client: *mut Client,
    pattern: *const c_char,
    out_count: *mut i32,
    status: *mut Status,
) -> *mut DirEntry {
    let client = unsafe { &*client };
    let pattern = unsafe { CStr::from_ptr(pattern) }.to_string_lossy();
    match client.glob_status(&pattern) {
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

/// List the immediate children of directory `path` (non-recursive). A null
/// return with `status->code == HDFS_OK` means an empty directory.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_list_status(
    client: *mut Client,
    path: *const c_char,
    out_count: *mut i32,
    status: *mut Status,
) -> *mut DirEntry {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.list_status(&path, false) {
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

/// Free an array of `DirEntry` returned by glob/list, including each path.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_free_dir_entries(entries: *mut DirEntry, count: i32) {
    if entries.is_null() || count <= 0 {
        return;
    }
    let slice = unsafe { Box::from_raw(slice::from_raw_parts_mut(entries, count as usize)) };
    for entry in slice.iter() {
        if !entry.path.is_null() {
            unsafe { drop(CString::from_raw(entry.path)) };
        }
    }
}

// --- mutations -------------------------------------------------------------

/// Create directory `path` (and any missing parents) with mode 0o755.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_mkdirs(
    client: *mut Client,
    path: *const c_char,
    status: *mut Status,
) -> i32 {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.mkdirs(&path, 0o755, true) {
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
    client: *mut Client,
    path: *const c_char,
    recursive: bool,
    status: *mut Status,
) -> i32 {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.delete(&path, recursive) {
        Ok(true) => 0,
        Ok(false) => {
            unsafe { set_status(status, HDFS_ERR_NOT_FOUND, format_args!("delete '{path}': path not found")) };
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
    client: *mut Client,
    src: *const c_char,
    dst: *const c_char,
    overwrite: bool,
    status: *mut Status,
) -> i32 {
    let client = unsafe { &*client };
    let src = unsafe { CStr::from_ptr(src) }.to_string_lossy();
    let dst = unsafe { CStr::from_ptr(dst) }.to_string_lossy();
    match client.rename(&src, &dst, overwrite) {
        Ok(()) => 0,
        Err(e) => {
            unsafe { set_error(status, format_args!("rename '{src}' -> '{dst}' failed"), &e) };
            -1
        }
    }
}
