#pragma once

#include "duckdb/common/file_system.hpp"
#include "hdfs_bridge.h"

#include <memory>
#include <mutex>
#include <unordered_map>

namespace duckdb {

// Implementation details defined in hdfs_filesystem.cpp.
struct BridgeStatus;  // RAII wrapper around hdfs_status_t.
class HdfsConnection; // A reconnectable, lazily-established client for one authority.
class HdfsFileSystem;

// Default for the hdfs_list_parallelism setting (registered in
// hdfs_extension.cpp); also the fallback when no setting context is available.
static constexpr uint64_t DEFAULT_HDFS_LIST_PARALLELISM = 16;

// A fully-resolved metadata row for one HDFS path, backing the hdfs_ls /
// hdfs_glob / hdfs_stat table functions. `url` carries the authority back
// (e.g. "hdfs://host:port/a/b"); `name` is the basename. `replication` and
// `block_size` are -1 when not applicable (HDFS leaves them unset for
// directories); `mtime`/`atime` are epoch milliseconds.
struct HdfsEntry {
	string url;
	string name;
	bool is_dir = false;
	int64_t size = 0;
	string owner;
	string group;
	uint16_t permission = 0;
	int32_t replication = -1;
	int64_t block_size = -1;
	uint64_t mtime = 0;
	uint64_t atime = 0;
};

struct HdfsFileHandle : public FileHandle {
	HdfsFileHandle(FileSystem &fs, string path, FileOpenFlags flags, hdfs_reader_t *reader, hdfs_writer_t *writer)
	    : FileHandle(fs, std::move(path), flags), reader(reader), writer(writer) {
	}

	~HdfsFileHandle() override {
		// Close() can throw (a writer flush may fail). A destructor must not
		// propagate, so swallow here; explicit Close() surfaces the error.
		try {
			HdfsFileHandle::Close();
		} catch (...) { // NOLINT: intentional - destructors must not throw
		}
	}

	void Close() override;

	hdfs_reader_t *reader = nullptr;
	hdfs_writer_t *writer = nullptr;
	// Cursor for the streaming Read/Write/Seek interface.
	idx_t position = 0;
	// Cached length for readers; bytes written so far for writers.
	idx_t length = 0;
};

// A streaming listing or glob backing hdfs_ls / hdfs_glob: a background walk
// in the bridge produces entries while Next() hands them out in batches, so
// rows flow before a large tree is fully walked. Walks touching more than one
// directory fan out up to `max_parallelism` concurrent listing RPCs and
// deliver entries in completion order (no global ordering guarantee). Not
// thread-safe: drive from one thread at a time. Created via
// HdfsFileSystem::OpenListStream() / OpenGlobStream().
class HdfsListStream {
public:
	~HdfsListStream();
	HdfsListStream(const HdfsListStream &) = delete;
	HdfsListStream &operator=(const HdfsListStream &) = delete;

	// Replace `out` with the next batch (at most max_entries), blocking until
	// at least one entry arrives. Returns false when the listing is exhausted
	// (`out` left empty). Throws on listing errors; a connection-level failure
	// is transparently retried once, but only while no entries have been
	// handed out yet (afterwards a restart would duplicate them).
	bool Next(vector<HdfsEntry> &out, idx_t max_entries);

private:
	friend class HdfsFileSystem;
	HdfsListStream() = default;
	// (Re)establish the client and open the bridge stream.
	void Open();

	string url;       // original URL, for error messages
	string authority; // "hdfs://host:port", or "" for the default FS
	string hdfs_path; // scheme-less path (a pattern when `glob`)
	bool glob = false;
	int32_t max_parallelism = 1;
	HdfsConnection *conn = nullptr; // owned by the filesystem, which outlives us
	std::shared_ptr<hdfs_client_t> client;
	hdfs_list_stream_t *handle = nullptr;
	bool emitted_any = false;
	bool retried = false;
};

class HdfsFileSystem : public FileSystem {
public:
	HdfsFileSystem();
	~HdfsFileSystem() override;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;
	void Reset(FileHandle &handle) override;

	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	// Extended listing: each entry carries type/size/last-modified in
	// OpenFileInfo::extended_info, so DuckDB doesn't re-stat per file. The base
	// FileSystem::ListFiles(name, is_dir) overload routes through this.
	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override;
	bool SupportsListFilesExtended() const override {
		return true;
	}
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;
	// HDFS has no mid-stream flush in the sync API; data is durable on Close().
	void FileSync(FileHandle &handle) override {
	}
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	// Rich-metadata accessors backing the SQL metadata functions. Unlike Glob()
	// (which drops directories and exposes only type/size/mtime), these carry the
	// full FileStatus and keep directory entries.
	//
	// OpenListStream: streaming listing of `url`'s immediate children; a file
	//   path yields that single entry. The path is taken literally.
	// OpenGlobStream: streaming glob of a wildcard `url`; matched entries are
	//   returned themselves, directories included. Throws on an invalid
	//   pattern; a pattern matching nothing yields an empty stream.
	// Stat: metadata for a single `url` (file or directory).
	//
	// `max_parallelism` bounds the concurrent listing RPCs of walks that touch
	// more than one directory (the hdfs_list_parallelism setting).
	unique_ptr<HdfsListStream> OpenListStream(const string &url, int32_t max_parallelism = 1);
	unique_ptr<HdfsListStream> OpenGlobStream(const string &url, int32_t max_parallelism = 1);
	HdfsEntry Stat(const string &url);
	// True if `url` exists (file or directory); a single stat RPC.
	bool Exists(const string &url);

	bool CanHandleFile(const string &fpath) override;
	std::string GetName() const override {
		return "HdfsFileSystem";
	}
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}

private:
	// Look up (or create) the connection for the given authority
	// (e.g. "hdfs://namenode:8020", or "" for the configured default FS).
	// Connections are created once and reused; the returned reference is stable.
	HdfsConnection &GetConnection(const string &authority);

	// Run `op(client, &status)` against a live client for `authority`. `op`
	// returns true on success. If the first attempt fails with a connection-
	// level error (stale client after a NameNode failover, a dropped socket, an
	// expired ticket), the connection is invalidated and `op` is retried once on
	// a freshly established client. The final outcome is left in `status` (OK on
	// success, otherwise the last failure with its category and message). A
	// failure to (re)connect propagates as an IOException.
	template <class FN>
	void Execute(const string &authority, BridgeStatus &status, FN &&op);

	// Guards only insertion/lookup into `connections`; entries are never erased,
	// so the per-connection lock (inside HdfsConnection) covers (re)connection.
	std::mutex connections_mutex;
	std::unordered_map<string, unique_ptr<HdfsConnection>> connections;
};

} // namespace duckdb
