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

struct HdfsFileHandle : public FileHandle {
	HdfsFileHandle(FileSystem &fs, string path, FileOpenFlags flags, hdfs_reader_t *reader,
	               hdfs_writer_t *writer)
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
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void MoveFile(const string &source, const string &target,
	              optional_ptr<FileOpener> opener = nullptr) override;
	// HDFS has no mid-stream flush in the sync API; data is durable on Close().
	void FileSync(FileHandle &handle) override {
	}
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

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
