#pragma once

#include "duckdb/common/file_system.hpp"
#include "hdfs_bridge.h"

#include <mutex>
#include <unordered_map>

namespace duckdb {

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
	// Look up (or create and cache) a client for the given authority
	// (e.g. "hdfs://namenode:8020", or "" for the configured default FS).
	// Connection config is resolved by hdfs-native from the Hadoop environment.
	hdfs_client_t *GetClient(const string &authority);

	std::mutex client_mutex;
	std::unordered_map<string, hdfs_client_t *> client_cache;
};

} // namespace duckdb
