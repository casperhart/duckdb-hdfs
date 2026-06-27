//! C FFI bridge exposing a synchronous subset of the `hdfs-native` client to
//! the DuckDB HDFS extension (C++).
//!
//! ## Conventions
//!
//! * Every fallible function takes a trailing `out_err: *mut *mut c_char`. On
//!   success it is left untouched (the caller initializes it to null). On
//!   failure the function writes a heap-allocated, NUL-terminated error message
//!   to `*out_err` and returns a sentinel (null pointer, or `-1`). The caller
//!   must free that string with [`hdfs_bridge_free_string`].
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

// The C++ filesystem issues concurrent positional reads against a single
// `FileReader` (DuckDB's parquet reader reads ranges from multiple threads on
// one handle). That is only sound if `FileReader` is `Sync`. Assert it at
// compile time so an upstream change can't silently break the bridge.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<FileReader>();
    assert_send_sync::<Client>();
};

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

/// Write `msg` into `*out_err` as an owned C string, if `out_err` is non-null.
unsafe fn set_error(out_err: *mut *mut c_char, msg: impl std::fmt::Display) {
    if out_err.is_null() {
        return;
    }
    let s = msg.to_string();
    // Replace interior NULs so CString::new never fails.
    let cleaned: String = s.replace('\0', " ");
    match CString::new(cleaned) {
        Ok(c) => unsafe { *out_err = c.into_raw() },
        Err(_) => unsafe { *out_err = ptr::null_mut() },
    }
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

/// Free a C string previously returned by the bridge (error messages, etc.).
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
    out_err: *mut *mut c_char,
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
            unsafe { set_error(out_err, format_args!("failed to connect to HDFS: {e}")) };
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
/// (including not-found).
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_get_file_info(
    client: *mut Client,
    path: *const c_char,
    out: *mut FileInfo,
    out_err: *mut *mut c_char,
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
            unsafe { set_error(out_err, format_args!("stat '{path}' failed: {e}")) };
            -1
        }
    }
}

/// Return true if `path` exists. Never reports an error: missing paths and
/// connection issues alike map to false.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_exists(client: *mut Client, path: *const c_char) -> bool {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    client.get_file_info(&path).is_ok()
}

// --- reader ----------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_open(
    client: *mut Client,
    path: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut FileReader {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.read(&path) {
        Ok(reader) => Box::into_raw(Box::new(reader)),
        Err(e) => {
            unsafe { set_error(out_err, format_args!("open '{path}' for reading failed: {e}")) };
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
    out_err: *mut *mut c_char,
) -> i64 {
    if len < 0 {
        unsafe { set_error(out_err, "negative read length") };
        return -1;
    }
    let reader = unsafe { &*reader };
    let slice = unsafe { slice::from_raw_parts_mut(buf, len as usize) };
    match reader.read_range_buf(slice, offset as usize) {
        Ok(()) => len,
        Err(e) => {
            unsafe { set_error(out_err, format_args!("read at offset {offset} failed: {e}")) };
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
    out_err: *mut *mut c_char,
) -> *mut FileWriter {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    let opts = WriteOptions::default().overwrite(overwrite).create_parent(true);
    match client.create(&path, opts) {
        Ok(writer) => Box::into_raw(Box::new(writer)),
        Err(e) => {
            unsafe { set_error(out_err, format_args!("create '{path}' for writing failed: {e}")) };
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
    out_err: *mut *mut c_char,
) -> i64 {
    if len < 0 {
        unsafe { set_error(out_err, "negative write length") };
        return -1;
    }
    let writer = unsafe { &mut *writer };
    let slice = unsafe { slice::from_raw_parts(buf, len as usize) };
    match writer.write_all(slice) {
        Ok(()) => len,
        Err(e) => {
            unsafe { set_error(out_err, format_args!("write failed: {e}")) };
            -1
        }
    }
}

/// Flush and close the writer, consuming the handle. Returns 0 or -1. The
/// handle must not be used afterwards regardless of the result.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_close_writer(
    writer: *mut FileWriter,
    out_err: *mut *mut c_char,
) -> i32 {
    if writer.is_null() {
        return 0;
    }
    let mut writer = unsafe { Box::from_raw(writer) };
    match writer.close() {
        Ok(()) => 0,
        Err(e) => {
            unsafe { set_error(out_err, format_args!("close writer failed: {e}")) };
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
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_glob(
    client: *mut Client,
    pattern: *const c_char,
    out_count: *mut i32,
    out_err: *mut *mut c_char,
) -> *mut DirEntry {
    let client = unsafe { &*client };
    let pattern = unsafe { CStr::from_ptr(pattern) }.to_string_lossy();
    match client.glob_status(&pattern) {
        Ok(statuses) => statuses_to_entries(statuses, out_count),
        Err(e) => {
            unsafe {
                *out_count = 0;
                set_error(out_err, format_args!("glob '{pattern}' failed: {e}"));
            }
            ptr::null_mut()
        }
    }
}

/// List the immediate children of directory `path` (non-recursive).
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_list_status(
    client: *mut Client,
    path: *const c_char,
    out_count: *mut i32,
    out_err: *mut *mut c_char,
) -> *mut DirEntry {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.list_status(&path, false) {
        Ok(statuses) => statuses_to_entries(statuses, out_count),
        Err(e) => {
            unsafe {
                *out_count = 0;
                set_error(out_err, format_args!("list '{path}' failed: {e}"));
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
    out_err: *mut *mut c_char,
) -> i32 {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.mkdirs(&path, 0o755, true) {
        Ok(()) => 0,
        Err(e) => {
            unsafe { set_error(out_err, format_args!("mkdirs '{path}' failed: {e}")) };
            -1
        }
    }
}

/// Delete `path`. `recursive` must be true to remove a non-empty directory.
#[no_mangle]
pub unsafe extern "C" fn hdfs_bridge_delete(
    client: *mut Client,
    path: *const c_char,
    recursive: bool,
    out_err: *mut *mut c_char,
) -> i32 {
    let client = unsafe { &*client };
    let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    match client.delete(&path, recursive) {
        Ok(true) => 0,
        Ok(false) => {
            unsafe { set_error(out_err, format_args!("delete '{path}' returned false")) };
            -1
        }
        Err(e) => {
            unsafe { set_error(out_err, format_args!("delete '{path}' failed: {e}")) };
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
    out_err: *mut *mut c_char,
) -> i32 {
    let client = unsafe { &*client };
    let src = unsafe { CStr::from_ptr(src) }.to_string_lossy();
    let dst = unsafe { CStr::from_ptr(dst) }.to_string_lossy();
    match client.rename(&src, &dst, overwrite) {
        Ok(()) => 0,
        Err(e) => {
            unsafe { set_error(out_err, format_args!("rename '{src}' -> '{dst}' failed: {e}")) };
            -1
        }
    }
}
