#pragma once

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handles owned by the Rust side.
typedef struct hdfs_client hdfs_client_t;
typedef struct hdfs_reader hdfs_reader_t;
typedef struct hdfs_writer hdfs_writer_t;
typedef struct hdfs_list_stream hdfs_list_stream_t;

// Error categories carried across the FFI boundary. The bridge classifies the
// underlying `hdfs_native::HdfsError` into one of these so the C++ side can act
// on the *kind* of failure (e.g. treat "not found" differently from "the
// cluster is unreachable") instead of pattern-matching on message strings.
// Mirrored by `HdfsErrorCode` in lib.rs; keep the two in sync.
typedef enum {
	HDFS_OK = 0,
	HDFS_ERR_IO = 1,               // generic I/O / RPC failure
	HDFS_ERR_NOT_FOUND = 2,        // path does not exist
	HDFS_ERR_PERMISSION = 3,       // access denied
	HDFS_ERR_ALREADY_EXISTS = 4,   // path already exists
	HDFS_ERR_CONNECTION = 5,       // connect/RPC/SASL failure; the client may be stale
	HDFS_ERR_INVALID_ARGUMENT = 6, // bad path / argument
} hdfs_error_code_t;

// Result of a fallible call. On success `code == HDFS_OK` and `msg == NULL`. On
// failure `code` is the category and `msg` is an owned, NUL-terminated message
// that the caller must free with hdfs_bridge_free_string. Callers must
// initialize the struct to `{HDFS_OK, NULL}` before each call; the bridge only
// writes it on failure.
typedef struct {
	int32_t code; // hdfs_error_code_t
	char *msg;
} hdfs_status_t;

// Metadata for a single path. Mirrors `FileInfo` in lib.rs.
typedef struct {
	int64_t length;
	bool is_dir;
	uint64_t mtime;
} hdfs_file_info_t;

// One entry in a listing/glob result. `path`, `owner` and `group` are owned C
// strings (`path` is scheme-less, e.g. "/a/b"). `replication` and `block_size`
// are `-1` when not applicable (HDFS leaves them unset for directories).
// Mirrors `DirEntry` in lib.rs.
typedef struct {
	char *path;
	bool is_dir;
	int64_t length;
	uint64_t mtime;
	uint64_t atime;
	char *owner;
	char *group;
	uint16_t permission;
	int32_t replication;
	int64_t block_size;
} hdfs_dir_entry_t;

// Free a heap string owned by the caller (an `hdfs_status_t::msg`, etc.).
void hdfs_bridge_free_string(char *s);

// Client management.
hdfs_client_t *hdfs_bridge_connect(const char *url, const char *config_dir, const char *user, hdfs_status_t *status);
void hdfs_bridge_free_client(hdfs_client_t *client);

// Metadata. Returns 0 on success, -1 on failure (see `status` for the category).
int32_t hdfs_bridge_get_file_info(hdfs_client_t *client, const char *path, hdfs_file_info_t *out,
                                  hdfs_status_t *status);

// Rich single-path stat: returns a one-element array (free with
// hdfs_bridge_free_dir_entries(ptr, 1)), or NULL on failure (see `status`).
hdfs_dir_entry_t *hdfs_bridge_stat(hdfs_client_t *client, const char *path, hdfs_status_t *status);

// Reading.
hdfs_reader_t *hdfs_bridge_open(hdfs_client_t *client, const char *path, hdfs_status_t *status);
void hdfs_bridge_close_reader(hdfs_reader_t *reader);
int64_t hdfs_bridge_file_size(hdfs_reader_t *reader);
// Reads exactly `len` bytes at `offset`; caller must ensure offset+len <= size.
// Thread-safe across concurrent calls on the same reader.
int64_t hdfs_bridge_read_range(hdfs_reader_t *reader, uint8_t *buf, int64_t len, uint64_t offset,
                               hdfs_status_t *status);

// Writing (append-only).
hdfs_writer_t *hdfs_bridge_create(hdfs_client_t *client, const char *path, bool overwrite, hdfs_status_t *status);
int64_t hdfs_bridge_write(hdfs_writer_t *writer, const uint8_t *buf, int64_t len, hdfs_status_t *status);
int32_t hdfs_bridge_close_writer(hdfs_writer_t *writer, hdfs_status_t *status);

// Directory operations. Returned arrays are freed with hdfs_bridge_free_dir_entries.
// A NULL return with `status->code == HDFS_OK` means an empty result (not an error).
hdfs_dir_entry_t *hdfs_bridge_list_status(hdfs_client_t *client, const char *path, bool recursive, int32_t *out_count,
                                          hdfs_status_t *status);
void hdfs_bridge_free_dir_entries(hdfs_dir_entry_t *entries, int32_t count);

// Streaming listing: a background walk feeds entries through a bounded buffer,
// so results flow before the (possibly huge) tree is fully listed. When
// `recursive`, up to `max_parallelism` listing RPCs run concurrently (<= 1
// lists one directory at a time) and entries arrive in completion order, not
// DFS order. Opening never fails; errors (including not-found) surface on the
// first _next call. Streams are not thread-safe: drive each from one thread at
// a time, and free with hdfs_bridge_list_stream_free (which also cancels an
// unfinished walk).
hdfs_list_stream_t *hdfs_bridge_list_stream_open(hdfs_client_t *client, const char *path, bool recursive,
                                                 int32_t max_parallelism);
// Streaming glob: like a listing stream, but `pattern` may contain wildcards
// (`*`, `?`, `[...]` classes, `\` escapes, `{a,b}` alternation, and `**` as a
// whole component matching zero or more levels). Matched entries — files and
// directories — are returned themselves; matched directories are not listed.
// Returns NULL with a non-OK status for an invalid pattern (unclosed brace
// group, multiple `**`). A pattern matching nothing yields an empty stream;
// other errors surface on the first _next call.
hdfs_list_stream_t *hdfs_bridge_glob_stream_open(hdfs_client_t *client, const char *pattern, int32_t max_parallelism,
                                                 hdfs_status_t *status);
// Blocks until at least one entry is available; returns a batch of at most
// `max_entries` (freed with hdfs_bridge_free_dir_entries). NULL with an OK
// status means the listing is exhausted; NULL with a non-OK status is an error.
hdfs_dir_entry_t *hdfs_bridge_list_stream_next(hdfs_list_stream_t *stream, int32_t max_entries, int32_t *out_count,
                                               hdfs_status_t *status);
void hdfs_bridge_list_stream_free(hdfs_list_stream_t *stream);

// Mutations.
int32_t hdfs_bridge_mkdirs(hdfs_client_t *client, const char *path, hdfs_status_t *status);
int32_t hdfs_bridge_delete(hdfs_client_t *client, const char *path, bool recursive, hdfs_status_t *status);
int32_t hdfs_bridge_rename(hdfs_client_t *client, const char *src, const char *dst, bool overwrite,
                           hdfs_status_t *status);

#ifdef __cplusplus
}
#endif
