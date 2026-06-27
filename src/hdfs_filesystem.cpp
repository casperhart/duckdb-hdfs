#include "hdfs_filesystem.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

namespace {

// RAII owner for an error string produced by the Rust bridge. Pass `&err` where
// a `char **out_err` is expected; the message is freed on scope exit.
struct BridgeError {
	char *msg = nullptr;
	BridgeError() = default;
	BridgeError(const BridgeError &) = delete;
	BridgeError &operator=(const BridgeError &) = delete;
	~BridgeError() {
		if (msg) {
			hdfs_bridge_free_string(msg);
		}
	}
	char **operator&() {
		return &msg;
	}
	string Get() const {
		return msg ? string(msg) : string("unknown error");
	}
};

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

} // namespace

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
		BridgeError err;
		if (hdfs_bridge_close_writer(w, &err) < 0) {
			throw IOException("Failed to close HDFS file '%s': %s", path, err.Get());
		}
	}
}

HdfsFileSystem::HdfsFileSystem() = default;

HdfsFileSystem::~HdfsFileSystem() {
	std::lock_guard<std::mutex> lock(client_mutex);
	for (auto &pair : client_cache) {
		hdfs_bridge_free_client(pair.second);
	}
}

hdfs_client_t *HdfsFileSystem::GetClient(const string &authority) {
	std::lock_guard<std::mutex> lock(client_mutex);
	auto it = client_cache.find(authority);
	if (it != client_cache.end()) {
		return it->second;
	}

	// Pass only the authority; hdfs-native resolves the config dir
	// (HADOOP_CONF_DIR / HADOOP_HOME) and user (HADOOP_USER_NAME / keytab /
	// current account) from the Hadoop environment.
	const char *url_cstr = authority.empty() ? nullptr : authority.c_str();

	BridgeError err;
	auto *client = hdfs_bridge_connect(url_cstr, /*config_dir=*/nullptr, /*user=*/nullptr, &err);
	if (!client) {
		throw IOException("Failed to connect to HDFS (%s): %s",
		                  authority.empty() ? "default fs" : authority.c_str(), err.Get());
	}
	client_cache[authority] = client;
	return client;
}

unique_ptr<FileHandle> HdfsFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(path, authority, hdfs_path);
	auto *client = GetClient(authority);

	if (flags.OpenForWriting()) {
		bool overwrite = flags.OverwriteExistingFile();
		BridgeError err;
		auto *writer = hdfs_bridge_create(client, hdfs_path.c_str(), overwrite, &err);
		if (!writer) {
			throw IOException("Failed to open HDFS file for writing '%s': %s", path, err.Get());
		}
		return make_uniq<HdfsFileHandle>(*this, path, flags, nullptr, writer);
	}

	BridgeError err;
	auto *reader = hdfs_bridge_open(client, hdfs_path.c_str(), &err);
	if (!reader) {
		if (flags.ReturnNullIfNotExists()) {
			return nullptr;
		}
		throw IOException("Failed to open HDFS file for reading '%s': %s", path, err.Get());
	}
	auto handle = make_uniq<HdfsFileHandle>(*this, path, flags, reader, nullptr);
	int64_t size = hdfs_bridge_file_size(reader);
	handle->length = size < 0 ? 0 : (idx_t)size;
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
	if (location + (idx_t)nr_bytes > h.length) {
		throw IOException("Cannot read %lld bytes at offset %llu from HDFS file '%s' of size %llu",
		                  nr_bytes, location, handle.path, h.length);
	}
	BridgeError err;
	int64_t res = hdfs_bridge_read_range(h.reader, (uint8_t *)buffer, nr_bytes, location, &err);
	if (res < 0) {
		throw IOException("Failed to read from HDFS file '%s': %s", handle.path, err.Get());
	}
}

int64_t HdfsFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &h = handle.Cast<HdfsFileHandle>();
	if (h.position >= h.length) {
		return 0;
	}
	int64_t to_read = nr_bytes;
	if (h.position + (idx_t)to_read > h.length) {
		to_read = (int64_t)(h.length - h.position);
	}
	if (to_read <= 0) {
		return 0;
	}
	Read(handle, buffer, to_read, h.position);
	h.position += (idx_t)to_read;
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
	BridgeError err;
	int64_t res = hdfs_bridge_write(h.writer, (const uint8_t *)buffer, nr_bytes, &err);
	if (res < 0) {
		throw IOException("Failed to write to HDFS file '%s': %s", handle.path, err.Get());
	}
	h.position += (idx_t)res;
	h.length += (idx_t)res;
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
	return (int64_t)handle.Cast<HdfsFileHandle>().length;
}

timestamp_t HdfsFileSystem::GetLastModifiedTime(FileHandle &handle) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(handle.path, authority, hdfs_path);
	auto *client = GetClient(authority);
	hdfs_file_info_t info;
	BridgeError err;
	if (hdfs_bridge_get_file_info(client, hdfs_path.c_str(), &info, &err) < 0) {
		throw IOException("Failed to stat HDFS file '%s': %s", handle.path, err.Get());
	}
	// HDFS modification times are epoch milliseconds.
	return Timestamp::FromEpochMs((int64_t)info.mtime);
}

bool HdfsFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(filename, authority, hdfs_path);
	auto *client = GetClient(authority);
	hdfs_file_info_t info;
	BridgeError err;
	if (hdfs_bridge_get_file_info(client, hdfs_path.c_str(), &info, &err) < 0) {
		return false;
	}
	return !info.is_dir;
}

bool HdfsFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(directory, authority, hdfs_path);
	auto *client = GetClient(authority);
	hdfs_file_info_t info;
	BridgeError err;
	if (hdfs_bridge_get_file_info(client, hdfs_path.c_str(), &info, &err) < 0) {
		return false;
	}
	return info.is_dir;
}

void HdfsFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(directory, authority, hdfs_path);
	auto *client = GetClient(authority);
	BridgeError err;
	if (hdfs_bridge_mkdirs(client, hdfs_path.c_str(), &err) < 0) {
		throw IOException("Failed to create HDFS directory '%s': %s", directory, err.Get());
	}
}

void HdfsFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(directory, authority, hdfs_path);
	auto *client = GetClient(authority);
	BridgeError err;
	if (hdfs_bridge_delete(client, hdfs_path.c_str(), /*recursive=*/true, &err) < 0) {
		throw IOException("Failed to remove HDFS directory '%s': %s", directory, err.Get());
	}
}

void HdfsFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(filename, authority, hdfs_path);
	auto *client = GetClient(authority);
	BridgeError err;
	if (hdfs_bridge_delete(client, hdfs_path.c_str(), /*recursive=*/false, &err) < 0) {
		throw IOException("Failed to remove HDFS file '%s': %s", filename, err.Get());
	}
}

void HdfsFileSystem::MoveFile(const string &source, const string &target,
                              optional_ptr<FileOpener> opener) {
	string src_authority;
	string src_path;
	ParseHdfsPath(source, src_authority, src_path);
	string dst_authority;
	string dst_path;
	ParseHdfsPath(target, dst_authority, dst_path);
	auto *client = GetClient(src_authority);
	BridgeError err;
	if (hdfs_bridge_rename(client, src_path.c_str(), dst_path.c_str(), /*overwrite=*/true, &err) < 0) {
		throw IOException("Failed to move HDFS file '%s' -> '%s': %s", source, target, err.Get());
	}
}

bool HdfsFileSystem::ListFiles(const string &directory,
                               const std::function<void(const string &, bool)> &callback,
                               FileOpener *opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(directory, authority, hdfs_path);
	auto *client = GetClient(authority);

	int32_t count = 0;
	BridgeError err;
	auto *entries = hdfs_bridge_list_status(client, hdfs_path.c_str(), &count, &err);
	if (!entries) {
		if (err.msg) {
			throw IOException("Failed to list HDFS directory '%s': %s", directory, err.Get());
		}
		return false; // empty directory
	}
	for (int32_t i = 0; i < count; i++) {
		// ListFiles reports bare child names, not full paths.
		string full = entries[i].path ? string(entries[i].path) : string();
		string name = full;
		size_t slash = full.find_last_of('/');
		if (slash != string::npos) {
			name = full.substr(slash + 1);
		}
		callback(name, entries[i].is_dir);
	}
	hdfs_bridge_free_dir_entries(entries, count);
	return count > 0;
}

vector<OpenFileInfo> HdfsFileSystem::Glob(const string &path, FileOpener *opener) {
	string authority;
	string hdfs_path;
	ParseHdfsPath(path, authority, hdfs_path);
	auto *client = GetClient(authority);

	int32_t count = 0;
	BridgeError err;
	auto *entries = hdfs_bridge_glob(client, hdfs_path.c_str(), &count, &err);
	vector<OpenFileInfo> result;
	if (!entries) {
		if (err.msg) {
			throw IOException("Failed to glob HDFS path '%s': %s", path, err.Get());
		}
		return result; // no matches
	}
	result.reserve(count);
	for (int32_t i = 0; i < count; i++) {
		string child = entries[i].path ? string(entries[i].path) : string();
		result.emplace_back(MakeUrl(authority, child));
	}
	hdfs_bridge_free_dir_entries(entries, count);
	return result;
}

bool HdfsFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("hdfs://", 0) == 0;
}

} // namespace duckdb
