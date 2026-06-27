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

// Metadata for a single path. Mirrors `FileInfo` in lib.rs.
typedef struct {
	int64_t length;
	bool is_dir;
	uint64_t mtime;
} hdfs_file_info_t;

// One entry in a listing/glob result. `path` is an owned C string (scheme-less,
// e.g. "/a/b"). Mirrors `DirEntry` in lib.rs.
typedef struct {
	char *path;
	bool is_dir;
	int64_t length;
	uint64_t mtime;
} hdfs_dir_entry_t;

// Error convention: every function taking `char **out_err` leaves it untouched
// on success and writes an owned message on failure. Free it with
// hdfs_bridge_free_string. Callers must initialize *out_err to NULL.
void hdfs_bridge_free_string(char *s);

// Client management.
hdfs_client_t *hdfs_bridge_connect(const char *url, const char *config_dir, const char *user,
                                   char **out_err);
void hdfs_bridge_free_client(hdfs_client_t *client);

// Metadata.
int32_t hdfs_bridge_get_file_info(hdfs_client_t *client, const char *path, hdfs_file_info_t *out,
                                  char **out_err);
bool hdfs_bridge_exists(hdfs_client_t *client, const char *path);

// Reading.
hdfs_reader_t *hdfs_bridge_open(hdfs_client_t *client, const char *path, char **out_err);
void hdfs_bridge_close_reader(hdfs_reader_t *reader);
int64_t hdfs_bridge_file_size(hdfs_reader_t *reader);
// Reads exactly `len` bytes at `offset`; caller must ensure offset+len <= size.
// Thread-safe across concurrent calls on the same reader.
int64_t hdfs_bridge_read_range(hdfs_reader_t *reader, uint8_t *buf, int64_t len, uint64_t offset,
                               char **out_err);

// Writing (append-only).
hdfs_writer_t *hdfs_bridge_create(hdfs_client_t *client, const char *path, bool overwrite,
                                  char **out_err);
int64_t hdfs_bridge_write(hdfs_writer_t *writer, const uint8_t *buf, int64_t len, char **out_err);
int32_t hdfs_bridge_close_writer(hdfs_writer_t *writer, char **out_err);

// Directory operations. Returned arrays are freed with hdfs_bridge_free_dir_entries.
hdfs_dir_entry_t *hdfs_bridge_glob(hdfs_client_t *client, const char *pattern, int32_t *out_count,
                                   char **out_err);
hdfs_dir_entry_t *hdfs_bridge_list_status(hdfs_client_t *client, const char *path,
                                          int32_t *out_count, char **out_err);
void hdfs_bridge_free_dir_entries(hdfs_dir_entry_t *entries, int32_t count);

// Mutations.
int32_t hdfs_bridge_mkdirs(hdfs_client_t *client, const char *path, char **out_err);
int32_t hdfs_bridge_delete(hdfs_client_t *client, const char *path, bool recursive, char **out_err);
int32_t hdfs_bridge_rename(hdfs_client_t *client, const char *src, const char *dst, bool overwrite,
                           char **out_err);

#ifdef __cplusplus
}
#endif
