#include "hdfs_filesystem.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <memory>

namespace duckdb {

namespace {

// Split a path into its authority ("hdfs://host:port", or "" for the default
// FS) and the scheme-less HDFS path ("/a/b"). Non-hdfs inputs are treated as a
// bare path against the default FS.
void ParseHdfsPath(const string &path, string &authority, string &hdfs_path) {
	const string scheme = "hdfs://";
	if (path.rfind(scheme, 0) != 0) {
		authority = "";
		hdfs_path = path;
		return;
	}
	size_t authority_start = scheme.size();
	size_t slash_pos = path.find('/', authority_start);
	if (slash_pos == string::npos) {
		// "hdfs://host:port" with no trailing path.
		authority = path;
		hdfs_path = "/";
	} else if (slash_pos == authority_start) {
		// "hdfs:///a/b" -> default FS.
		authority = "";
		hdfs_path = path.substr(slash_pos);
	} else {
		authority = path.substr(0, slash_pos);
		hdfs_path = path.substr(slash_pos);
	}
}

// Build DuckDB's extended metadata from a bridge directory entry. The bridge
// already returns type/size/mtime for every glob and listing entry, so handing
// them to DuckDB here lets glob expansion, the external file cache, and the
// glob()/read_blob/read_text functions avoid a second per-file stat RPC.
shared_ptr<ExtendedOpenFileInfo> MakeExtendedInfo(const hdfs_dir_entry_t &entry) {
	auto ext = make_shared_ptr<ExtendedOpenFileInfo>();
	auto &options = ext->options;
	options.emplace("type", Value(entry.is_dir ? "directory" : "file"));
	options.emplace("file_size", Value::BIGINT(entry.length));
	// HDFS modification times are epoch milliseconds.
	options.emplace("last_modified", Value::TIMESTAMP(Timestamp::FromEpochMs(static_cast<int64_t>(entry.mtime))));
	return ext;
}

// Reattach an authority to a scheme-less path returned by the bridge.
string MakeUrl(const string &authority, const string &hdfs_path) {
	string prefix = authority.empty() ? "hdfs://" : authority;
	if (hdfs_path.empty()) {
		return prefix + "/";
	}
	if (hdfs_path[0] != '/') {
		return prefix + "/" + hdfs_path;
	}
	return prefix + hdfs_path;
}

// Map a bridge directory entry (scheme-less full path) to a rich HdfsEntry,
// reattaching `authority` to build the URL and splitting off the basename.
HdfsEntry MakeHdfsEntry(const string &authority, const hdfs_dir_entry_t &entry) {
	string full = entry.path ? string(entry.path) : string();
	string name = full;
	size_t slash = full.find_last_of('/');
	if (slash != string::npos) {
		name = full.substr(slash + 1);
	}
	HdfsEntry out;
	out.url = MakeUrl(authority, full);
	out.name = std::move(name);
	out.is_dir = entry.is_dir;
	out.size = entry.length;
	out.owner = entry.owner ? string(entry.owner) : string();
	out.group = entry.group ? string(entry.group) : string();
	out.permission = entry.permission;
	out.replication = entry.replication;
	out.block_size = entry.block_size;
	out.mtime = entry.mtime;
	out.atime = entry.atime;
	return out;
}

} // namespace

// RAII owner for an `hdfs_status_t` produced by the Rust bridge. Pass `&status`
// where a `hdfs_status_t *` is expected; the message is freed on scope exit.
// Reset() makes it reusable across the retry attempts in Execute().
struct BridgeStatus {
	hdfs_status_t status {HDFS_OK, nullptr};

	BridgeStatus() = default;
	BridgeStatus(const BridgeStatus &) = delete;
	BridgeStatus &operator=(const BridgeStatus &) = delete;
	~BridgeStatus() {
		Reset();
	}

	void Reset() {
		if (status.msg) {
			hdfs_bridge_free_string(status.msg);
			status.msg = nullptr;
		}
		status.code = HDFS_OK;
	}
	hdfs_status_t *operator&() {
		return &status;
	}
	int Code() const {
		return status.code;
	}
	bool Ok() const {
		return status.code == HDFS_OK;
	}
	bool IsNotFound() const {
		return status.code == HDFS_ERR_NOT_FOUND;
	}
	bool IsConnection() const {
		return status.code == HDFS_ERR_CONNECTION;
	}
	string Message() const {
		return status.msg ? string(status.msg) : string("unknown error");
	}
};

// A reconnectable client for a single authority. The client is established
// lazily and shared via shared_ptr: an in-flight operation keeps its client
// alive even if another thread invalidates the connection concurrently (e.g.
// after a NameNode failover), so reconnection is free of use-after-free.
class HdfsConnection {
public:
	explicit HdfsConnection(string authority) : authority(std::move(authority)) {
	}

	// Current client, established on first use. The returned shared_ptr keeps it
	// alive for the duration of the caller's operation.
	std::shared_ptr<hdfs_client_t> Get() {
		std::lock_guard<std::mutex> lock(mutex);
		if (!client) {
			client = Connect();
		}
		return client;
	}

	// Drop `stale` if it is still the current client, forcing the next Get() to
	// reconnect. No-op if another thread already reconnected.
	void Invalidate(const std::shared_ptr<hdfs_client_t> &stale) {
		std::lock_guard<std::mutex> lock(mutex);
		if (client == stale) {
			client.reset();
		}
	}

private:
	// Establish a new client. Caller holds `mutex`.
	std::shared_ptr<hdfs_client_t> Connect() {
		// Pass only the authority; hdfs-native resolves the config dir
		// (HADOOP_CONF_DIR / HADOOP_HOME) and user (HADOOP_USER_NAME / keytab /
		// current account) from the Hadoop environment.
		const char *url = authority.empty() ? nullptr : authority.c_str();
		BridgeStatus status;
		auto *raw = hdfs_bridge_connect(url, /*config_dir=*/nullptr, /*user=*/nullptr, &status);
		if (!raw) {
			throw IOException("Failed to connect to HDFS (%s): %s",
			                  authority.empty() ? "default fs" : authority.c_str(), status.Message());
		}
		return std::shared_ptr<hdfs_client_t>(raw, [](hdfs_client_t *c) { hdfs_bridge_free_client(c); });
	}

	const string authority;
	std::mutex mutex;
	std::shared_ptr<hdfs_client_t> client;
};

void HdfsFileHandle::Close() {
	if (reader) {
		hdfs_bridge_close_reader(reader);
		reader = nullptr;
	}
	if (writer) {
		// Take ownership locally so a second Close() (e.g. from the destructor)
		// is a no-op even if the flush throws.
		auto *w = writer;
		writer = nullptr;
		BridgeStatus status;
		if (hdfs_bridge_close_writer(w, &status) < 0) {
			throw IOException("Failed to close HDFS file '%s': %s", path, status.Message());
		}
	}
}

HdfsFileSystem::HdfsFileSystem() = default;

// Defined here (not defaulted in the header) because HdfsConnection is an
// incomplete type there; the unique_ptr deleter needs the full definition.
HdfsFileSystem::~HdfsFileSystem() = default;

HdfsConnection &HdfsFileSystem::GetConnection(const string &authority) {
	std::lock_guard<std::mutex> lock(connections_mutex);
	auto it = connections.find(authority);
	if (it != connections.end()) {
		return *it->second;
	}
	auto conn = make_uniq<HdfsConnection>(authority);
	auto &ref = *conn;
	connections[authority] = std::move(conn);
	return ref;
}

template <class FN>
void HdfsFileSystem::Execute(const string &authority, BridgeStatus &status, FN &&op) {
	HdfsConnection &conn = GetConnection(authority);
	for (int attempt = 0;; attempt++) {
		status.Reset();
		std::shared_ptr<hdfs_client_t> client = conn.Get(); // throws on connect failure
		if (op(client.get(), &status)) {
			return; // success; status == OK
		}
		if (attempt == 0 && status.IsConnection()) {
			// Stale client (failover / dropped socket / expired ticket): drop it
			// and retry once on a freshly established client.
			conn.Invalidate(client);
			continue;
		}
		return; // non-retryable failure; details left in status
	}
}

unique_ptr<FileHandle> HdfsFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(path, authority, hdfs_path);

	if (flags.OpenForWriting()) {
		bool overwrite = flags.OverwriteExistingFile();
		hdfs_writer_t *writer = nullptr;
		BridgeStatus status;
		Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
			writer = hdfs_bridge_create(client, hdfs_path.c_str(), overwrite, st);
			return writer != nullptr;
		});
		if (!writer) {
			throw IOException("Failed to open HDFS file for writing '%s': %s", path, status.Message());
		}
		return make_uniq<HdfsFileHandle>(*this, path, flags, nullptr, writer);
	}

	hdfs_reader_t *reader = nullptr;
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		reader = hdfs_bridge_open(client, hdfs_path.c_str(), st);
		return reader != nullptr;
	});
	if (!reader) {
		// Only a genuine "not found" is reported as a missing file; connection
		// or permission failures still surface as errors.
		if (status.IsNotFound() && flags.ReturnNullIfNotExists()) {
			return nullptr;
		}
		throw IOException("Failed to open HDFS file for reading '%s': %s", path, status.Message());
	}
	auto handle = make_uniq<HdfsFileHandle>(*this, path, flags, reader, nullptr);
	int64_t size = hdfs_bridge_file_size(reader);
	handle->length = size < 0 ? 0 : static_cast<idx_t>(size);
	return std::move(handle);
}

void HdfsFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &h = handle.Cast<HdfsFileHandle>();
	if (!h.reader) {
		throw IOException("File not opened for reading: " + handle.path);
	}
	if (nr_bytes == 0) {
		return;
	}
	// Bounds check written to avoid unsigned overflow on `location + nr_bytes`.
	if (location > h.length || static_cast<idx_t>(nr_bytes) > h.length - location) {
		throw IOException("Cannot read %lld bytes at offset %llu from HDFS file '%s' of size %llu", nr_bytes, location,
		                  handle.path, h.length);
	}
	BridgeStatus status;
	int64_t res = hdfs_bridge_read_range(h.reader, reinterpret_cast<uint8_t *>(buffer), nr_bytes, location, &status);
	if (res < 0) {
		throw IOException("Failed to read from HDFS file '%s': %s", handle.path, status.Message());
	}
}

int64_t HdfsFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &h = handle.Cast<HdfsFileHandle>();
	if (h.position >= h.length) {
		return 0;
	}
	int64_t to_read = nr_bytes;
	if (h.position + static_cast<idx_t>(to_read) > h.length) {
		to_read = static_cast<int64_t>(h.length - h.position);
	}
	if (to_read <= 0) {
		return 0;
	}
	Read(handle, buffer, to_read, h.position);
	h.position += static_cast<idx_t>(to_read);
	return to_read;
}

void HdfsFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("Random/positional writes are not supported on HDFS (append-only)");
}

int64_t HdfsFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &h = handle.Cast<HdfsFileHandle>();
	if (!h.writer) {
		throw IOException("File not opened for writing: " + handle.path);
	}
	BridgeStatus status;
	int64_t res = hdfs_bridge_write(h.writer, reinterpret_cast<const uint8_t *>(buffer), nr_bytes, &status);
	if (res < 0) {
		throw IOException("Failed to write to HDFS file '%s': %s", handle.path, status.Message());
	}
	h.position += static_cast<idx_t>(res);
	h.length += static_cast<idx_t>(res);
	return res;
}

void HdfsFileSystem::Seek(FileHandle &handle, idx_t location) {
	handle.Cast<HdfsFileHandle>().position = location;
}

idx_t HdfsFileSystem::SeekPosition(FileHandle &handle) {
	return handle.Cast<HdfsFileHandle>().position;
}

void HdfsFileSystem::Reset(FileHandle &handle) {
	handle.Cast<HdfsFileHandle>().position = 0;
}

int64_t HdfsFileSystem::GetFileSize(FileHandle &handle) {
	return static_cast<int64_t>(handle.Cast<HdfsFileHandle>().length);
}

timestamp_t HdfsFileSystem::GetLastModifiedTime(FileHandle &handle) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(handle.path, authority, hdfs_path);
	hdfs_file_info_t info;
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		return hdfs_bridge_get_file_info(client, hdfs_path.c_str(), &info, st) == 0;
	});
	if (!status.Ok()) {
		throw IOException("Failed to stat HDFS file '%s': %s", handle.path, status.Message());
	}
	// HDFS modification times are epoch milliseconds.
	return Timestamp::FromEpochMs(static_cast<int64_t>(info.mtime));
}

bool HdfsFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(filename, authority, hdfs_path);
	hdfs_file_info_t info;
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		return hdfs_bridge_get_file_info(client, hdfs_path.c_str(), &info, st) == 0;
	});
	if (status.Ok()) {
		return !info.is_dir;
	}
	if (status.IsNotFound()) {
		return false;
	}
	// A connection/permission error is not the same as "does not exist".
	throw IOException("Failed to check HDFS file '%s': %s", filename, status.Message());
}

bool HdfsFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(directory, authority, hdfs_path);
	hdfs_file_info_t info;
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		return hdfs_bridge_get_file_info(client, hdfs_path.c_str(), &info, st) == 0;
	});
	if (status.Ok()) {
		return info.is_dir;
	}
	if (status.IsNotFound()) {
		return false;
	}
	throw IOException("Failed to check HDFS directory '%s': %s", directory, status.Message());
}

void HdfsFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(directory, authority, hdfs_path);
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		return hdfs_bridge_mkdirs(client, hdfs_path.c_str(), st) == 0;
	});
	if (!status.Ok()) {
		throw IOException("Failed to create HDFS directory '%s': %s", directory, status.Message());
	}
}

void HdfsFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(directory, authority, hdfs_path);
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		return hdfs_bridge_delete(client, hdfs_path.c_str(), /*recursive=*/true, st) == 0;
	});
	if (!status.Ok()) {
		throw IOException("Failed to remove HDFS directory '%s': %s", directory, status.Message());
	}
}

void HdfsFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(filename, authority, hdfs_path);
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		return hdfs_bridge_delete(client, hdfs_path.c_str(), /*recursive=*/false, st) == 0;
	});
	if (!status.Ok()) {
		throw IOException("Failed to remove HDFS file '%s': %s", filename, status.Message());
	}
}

void HdfsFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	string src_authority;
	string src_path;
	ParseHdfsPath(source, src_authority, src_path);
	string dst_authority;
	string dst_path;
	ParseHdfsPath(target, dst_authority, dst_path);
	// HDFS rename is a single-NameNode operation; a cross-authority move would
	// previously have silently renamed within the source cluster.
	if (src_authority != dst_authority) {
		throw NotImplementedException("Cannot move HDFS files across different NameNodes ('%s' -> '%s')", source,
		                              target);
	}
	BridgeStatus status;
	Execute(src_authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		return hdfs_bridge_rename(client, src_path.c_str(), dst_path.c_str(), /*overwrite=*/true, st) == 0;
	});
	if (!status.Ok()) {
		throw IOException("Failed to move HDFS file '%s' -> '%s': %s", source, target, status.Message());
	}
}

bool HdfsFileSystem::ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
                                       optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(directory, authority, hdfs_path);

	int32_t count = 0;
	hdfs_dir_entry_t *entries = nullptr;
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		entries = hdfs_bridge_list_status(client, hdfs_path.c_str(), /*recursive=*/false, &count, st);
		// A null result with an OK status is an empty directory, not a failure;
		// the status code is the sole success signal.
		return st->code == HDFS_OK;
	});
	if (!status.Ok()) {
		throw IOException("Failed to list HDFS directory '%s': %s", directory, status.Message());
	}
	for (int32_t i = 0; i < count; i++) {
		// Listing reports bare child names, not full paths.
		string full = entries[i].path ? string(entries[i].path) : string();
		string name = full;
		size_t slash = full.find_last_of('/');
		if (slash != string::npos) {
			name = full.substr(slash + 1);
		}
		OpenFileInfo info(name);
		info.extended_info = MakeExtendedInfo(entries[i]);
		callback(info);
	}
	if (entries) {
		hdfs_bridge_free_dir_entries(entries, count);
	}
	return count > 0;
}

vector<OpenFileInfo> HdfsFileSystem::Glob(const string &path, FileOpener *opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(path, authority, hdfs_path);

	int32_t count = 0;
	hdfs_dir_entry_t *entries = nullptr;
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		entries = hdfs_bridge_glob(client, hdfs_path.c_str(), &count, st);
		// Null + OK means "no matches"; only a non-OK status is an error.
		return st->code == HDFS_OK;
	});
	if (!status.Ok()) {
		throw IOException("Failed to glob HDFS path '%s': %s", path, status.Message());
	}
	vector<OpenFileInfo> result;
	result.reserve(count);
	for (int32_t i = 0; i < count; i++) {
		// glob_status matches directories too (e.g. "dir/*" yields subdirs). We
		// don't implement the extended-glob path, so DuckDB wraps these results
		// without filtering and would try to open a directory as a data file.
		// Drop directories here so only files reach the multi-file readers.
		if (entries[i].is_dir) {
			continue;
		}
		string child = entries[i].path ? string(entries[i].path) : string();
		OpenFileInfo info(MakeUrl(authority, child));
		// Carry the metadata the glob already returned so DuckDB can read
		// size/mtime without re-statting each file.
		info.extended_info = MakeExtendedInfo(entries[i]);
		result.push_back(std::move(info));
	}
	if (entries) {
		hdfs_bridge_free_dir_entries(entries, count);
	}
	return result;
}

HdfsListStream::~HdfsListStream() {
	if (handle) {
		hdfs_bridge_list_stream_free(handle);
	}
}

void HdfsListStream::Open() {
	client = conn->Get(); // throws on connect failure
	handle = hdfs_bridge_list_stream_open(client.get(), hdfs_path.c_str(), recursive, max_parallelism);
}

bool HdfsListStream::Next(vector<HdfsEntry> &out, idx_t max_entries) {
	out.clear();
	for (;;) {
		int32_t count = 0;
		BridgeStatus status;
		hdfs_dir_entry_t *entries =
		    hdfs_bridge_list_stream_next(handle, static_cast<int32_t>(max_entries), &count, &status);
		if (status.Ok()) {
			out.reserve(count);
			for (int32_t i = 0; i < count; i++) {
				out.push_back(MakeHdfsEntry(authority, entries[i]));
			}
			if (entries) {
				hdfs_bridge_free_dir_entries(entries, count);
			}
			emitted_any |= count > 0;
			return count > 0; // 0 with an OK status means the walk completed
		}
		// Mirror Execute()'s retry-once semantics for a stale connection
		// (NameNode failover, dropped socket, expired ticket). Only transparent
		// while nothing has been handed out yet: the walk restarts from the
		// root, so retrying later would duplicate entries.
		if (status.IsConnection() && !emitted_any && !retried) {
			retried = true;
			hdfs_bridge_list_stream_free(handle);
			handle = nullptr;
			conn->Invalidate(client);
			Open();
			continue;
		}
		// hdfs_ls treats its argument literally; a wildcard here won't expand.
		// Point the user at hdfs_glob rather than a bare "not found".
		if (status.IsNotFound() && hdfs_path.find_first_of("*?[{") != string::npos) {
			throw IOException("hdfs_ls path '%s' looks like a glob pattern but is matched literally; "
			                  "use hdfs_glob() for wildcard patterns",
			                  url);
		}
		throw IOException("Failed to list HDFS path '%s': %s", url, status.Message());
	}
}

unique_ptr<HdfsListStream> HdfsFileSystem::OpenListStream(const string &url, bool recursive, int32_t max_parallelism) {
	// Not make_uniq: the constructor is private to keep construction here.
	auto stream = unique_ptr<HdfsListStream>(new HdfsListStream());
	stream->url = url;
	ParseHdfsPath(url, stream->authority, stream->hdfs_path);
	stream->recursive = recursive;
	stream->max_parallelism = max_parallelism;
	stream->conn = &GetConnection(stream->authority);
	stream->Open();
	return stream;
}

vector<HdfsEntry> HdfsFileSystem::GlobStatus(const string &pattern) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(pattern, authority, hdfs_path);

	int32_t count = 0;
	hdfs_dir_entry_t *entries = nullptr;
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		entries = hdfs_bridge_glob(client, hdfs_path.c_str(), &count, st);
		// Null + OK means "no matches"; only a non-OK status is an error.
		return st->code == HDFS_OK;
	});
	if (!status.Ok()) {
		throw IOException("Failed to glob HDFS path '%s': %s", pattern, status.Message());
	}
	vector<HdfsEntry> result;
	result.reserve(count);
	// Unlike Glob(), keep directory matches: the metadata functions surface them
	// as rows rather than feeding a multi-file reader.
	for (int32_t i = 0; i < count; i++) {
		result.push_back(MakeHdfsEntry(authority, entries[i]));
	}
	if (entries) {
		hdfs_bridge_free_dir_entries(entries, count);
	}
	return result;
}

HdfsEntry HdfsFileSystem::Stat(const string &url) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(url, authority, hdfs_path);

	hdfs_dir_entry_t *entry = nullptr;
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		entry = hdfs_bridge_stat(client, hdfs_path.c_str(), st);
		return st->code == HDFS_OK;
	});
	if (!status.Ok()) {
		throw IOException("Failed to stat HDFS path '%s': %s", url, status.Message());
	}
	HdfsEntry result = MakeHdfsEntry(authority, *entry);
	hdfs_bridge_free_dir_entries(entry, 1);
	return result;
}

bool HdfsFileSystem::Exists(const string &url) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(url, authority, hdfs_path);
	hdfs_file_info_t info;
	BridgeStatus status;
	Execute(authority, status, [&](hdfs_client_t *client, hdfs_status_t *st) {
		return hdfs_bridge_get_file_info(client, hdfs_path.c_str(), &info, st) == 0;
	});
	if (status.Ok()) {
		return true;
	}
	if (status.IsNotFound()) {
		return false;
	}
	// A connection/permission error is not the same as "does not exist".
	throw IOException("Failed to check HDFS path '%s': %s", url, status.Message());
}

bool HdfsFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("hdfs://", 0) == 0;
}

} // namespace duckdb
