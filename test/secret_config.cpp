#include "duckdb.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "hdfs_filesystem.hpp"

#include <functional>
#include <iostream>
#include <map>
#include <stdexcept>

// 仅替换桥接边界；Secret、SQL 函数和文件系统路由使用实际实现。
struct hdfs_client_t {
	int id;
	std::string user;
};
struct hdfs_reader_t {};
struct hdfs_writer_t {};
struct hdfs_list_stream_t {
	hdfs_client_t *client;
};
namespace {
struct ConnectCall {
	bool explicit_config;
	std::string url, user, principal, keytab;
	std::map<std::string, std::string> conf;
};
std::vector<ConnectCall> connects;
std::vector<int> operations;
int next_error = HDFS_OK;
bool fail_connect = false;
int assertions = 0;
void Check(bool condition, const char *message) {
	++assertions;
	if (!condition)
		throw std::runtime_error(message);
}
int Operation(hdfs_client_t *client, hdfs_status_t *status) {
	operations.push_back(client->id);
	status->code = next_error;
	next_error = HDFS_OK;
	return status->code == HDFS_OK ? 0 : -1;
}
std::string Text(const char *value) {
	return value ? value : "";
}
} // namespace
extern "C" {
hdfs_client_t *__wrap_hdfs_bridge_connect(const char *url, const char *, const char *user, hdfs_status_t *status) {
	connects.push_back({false, Text(url), Text(user), "", "", {}});
	return new hdfs_client_t {int(connects.size()), Text(user)};
}
hdfs_client_t *__wrap_hdfs_bridge_connect_config(const char *url, const char *, const char *user, const char *principal,
                                                 const char *keytab, const char *const *keys, const char *const *values,
                                                 uintptr_t count, hdfs_status_t *status) {
	ConnectCall call {true, Text(url), Text(user), Text(principal), Text(keytab), {}};
	for (uintptr_t i = 0; i < count; ++i)
		call.conf.emplace(keys[i], values[i]);
	connects.push_back(call);
	if (fail_connect) {
		status->code = HDFS_ERR_PERMISSION;
		return nullptr;
	}
	return new hdfs_client_t {int(connects.size()), Text(user)};
}
void __wrap_hdfs_bridge_free_client(hdfs_client_t *client) {
	delete client;
}
hdfs_reader_t *__wrap_hdfs_bridge_open(hdfs_client_t *client, const char *, hdfs_status_t *status) {
	return Operation(client, status) == 0 ? new hdfs_reader_t : nullptr;
}
void __wrap_hdfs_bridge_close_reader(hdfs_reader_t *reader) {
	delete reader;
}
int64_t __wrap_hdfs_bridge_file_size(hdfs_reader_t *) {
	return 0;
}
hdfs_writer_t *__wrap_hdfs_bridge_create(hdfs_client_t *client, const char *, bool, hdfs_status_t *status) {
	return Operation(client, status) == 0 ? new hdfs_writer_t : nullptr;
}
int32_t __wrap_hdfs_bridge_close_writer(hdfs_writer_t *writer, hdfs_status_t *) {
	delete writer;
	return 0;
}
int32_t __wrap_hdfs_bridge_get_file_info(hdfs_client_t *client, const char *, hdfs_file_info_t *info,
                                         hdfs_status_t *status) {
	*info = {0, false, 123};
	return Operation(client, status);
}
int32_t __wrap_hdfs_bridge_mkdirs(hdfs_client_t *client, const char *, hdfs_status_t *status) {
	return Operation(client, status);
}
int32_t __wrap_hdfs_bridge_delete(hdfs_client_t *client, const char *, bool, hdfs_status_t *status) {
	return Operation(client, status);
}
int32_t __wrap_hdfs_bridge_rename(hdfs_client_t *client, const char *, const char *, bool, hdfs_status_t *status) {
	return Operation(client, status);
}
hdfs_list_stream_t *__wrap_hdfs_bridge_list_stream_open(hdfs_client_t *client, const char *, bool, int32_t, bool,
                                                        bool) {
	return new hdfs_list_stream_t {client};
}
hdfs_list_stream_t *__wrap_hdfs_bridge_glob_stream_open(hdfs_client_t *client, const char *, int32_t, bool, bool,
                                                        hdfs_status_t *) {
	return new hdfs_list_stream_t {client};
}
hdfs_dir_entry_t *__wrap_hdfs_bridge_list_stream_next(hdfs_list_stream_t *stream, int32_t, int32_t *count,
                                                      hdfs_status_t *status) {
	*count = 0;
	Operation(stream->client, status);
	return nullptr;
}
void __wrap_hdfs_bridge_list_stream_free(hdfs_list_stream_t *stream) {
	delete stream;
}
hdfs_dir_entry_t *__wrap_hdfs_bridge_stat(hdfs_client_t *client, const char *, hdfs_status_t *status) {
	Operation(client, status);
	if (status->code != HDFS_OK)
		return nullptr;
	auto *entry = new hdfs_dir_entry_t {};
	entry->path = const_cast<char *>("/file");
	entry->owner = const_cast<char *>("owner");
	entry->group = const_cast<char *>("group");
	return entry;
}
void __wrap_hdfs_bridge_free_dir_entries(hdfs_dir_entry_t *entries, int32_t) {
	delete entries;
}
}

using namespace duckdb;
namespace {
struct Fixture {
	DuckDB db {nullptr};
	Connection sql {db};
	ClientContextFileOpener opener {*sql.context};
	HdfsFileSystem fs;
	Fixture() {
		Query("LOAD hdfs");
		Query("BEGIN TRANSACTION");
	}
	void Query(const string &query) {
		auto result = sql.Query(query);
		if (result->HasError())
			throw std::runtime_error(result->GetError());
	}
	void Secret(const string &user, const string &extra = "") {
		Query("CREATE OR REPLACE TEMPORARY SECRET auth (TYPE hdfs, FS_NAME "
		      "'hdfs://cluster', USER '" +
		      user + "'" + extra + ")");
	}
};
template <class FN>
void ExpectError(FN fn) {
	bool failed = false;
	try {
		fn();
	} catch (const duckdb::Exception &) {
		failed = true;
	}
	Check(failed, "operation must reject");
}
} // namespace
int main() {
	int failures = 0, cases = 0;
	auto run = [&](const char *name, const std::function<void()> &test) {
		++cases;
		connects.clear();
		operations.clear();
		next_error = HDFS_OK;
		fail_connect = false;
		try {
			test();
			std::cout << "PASS: " << name << '\n';
		} catch (const std::exception &e) {
			++failures;
			std::cerr << "FAIL: " << name << ": " << e.what() << '\n';
		}
	};
	run("unmatched and absent opener retain environment mode", [] {
		Fixture f;
		f.fs.FileExists("hdfs://cluster/file", &f.opener);
		f.fs.FileExists("hdfs://cluster/another");
		f.fs.FileExists("hdfs:///default");
		Check(connects.size() == 2, "environment clients should share by authority");
		Check(!connects[0].explicit_config && connects[1].url.empty(), "defaultFS/environment mode lost");
	});
	run("secret configuration reaches bridge and replaces new operations only", [] {
		Fixture f;
		f.Secret("alice", ", PRINCIPAL 'alice@REALM', KEYTAB '/tmp/alice.keytab'");
		auto old = f.fs.OpenFile("hdfs://cluster/file", FileFlags::FILE_FLAGS_READ, &f.opener);
		Check(connects.size() == 1 && connects[0].user == "alice", "secret user not forwarded");
		Check(connects[0].principal == "alice@REALM" && connects[0].keytab == "/tmp/alice.keytab",
		      "credentials not forwarded");
		Check(connects[0].conf.at("hadoop.security.authentication") == "kerberos", "kerberos not selected");
		f.Secret("bob");
		f.fs.FileExists("hdfs://cluster/file", &f.opener);
		Check(connects.size() == 2 && connects.back().user == "bob", "replacement reused old client");
		f.fs.GetLastModifiedTime(*old);
		Check(operations.back() == 1, "old handle used replacement identity");
		next_error = HDFS_ERR_CONNECTION;
		f.fs.GetLastModifiedTime(*old);
		Check(connects.size() == 3 && connects.back().user == "alice", "retry changed handle identity");
	});
	run("all filesystem operations forward the same secret", [] {
		Fixture f;
		f.Secret("alice");
		f.fs.OpenFile("hdfs://cluster/file", FileFlags::FILE_FLAGS_WRITE, &f.opener);
		f.fs.FileExists("hdfs://cluster/file", &f.opener);
		f.fs.DirectoryExists("hdfs://cluster/dir", &f.opener);
		f.fs.CreateDirectory("hdfs://cluster/dir", &f.opener);
		f.fs.RemoveDirectory("hdfs://cluster/dir", &f.opener);
		f.fs.RemoveFile("hdfs://cluster/file", &f.opener);
		f.fs.MoveFile("hdfs://cluster/a", "hdfs://cluster/b", &f.opener);
		f.fs.ListFilesExtended("hdfs://cluster/dir", [](OpenFileInfo &) {}, &f.opener);
		f.fs.Glob("hdfs://cluster/*", &f.opener);
		Check(connects.size() == 1 && connects[0].user == "alice", "operation escaped secret client");
		Check(operations.size() == 9, "missing expected filesystem operation");
	});
	run("metadata SQL functions use query secrets", [] {
		Fixture f;
		f.Secret("alice");
		f.Query("SELECT * FROM hdfs_stat('hdfs://cluster/file')");
		f.Query("SELECT * FROM hdfs_ls('hdfs://cluster/dir')");
		f.Query("SELECT * FROM hdfs_ls('hdfs://cluster/*')");
		f.Query("SELECT hdfs_exists('hdfs://cluster/file')");
		Check(connects.size() == 1 && connects[0].user == "alice", "metadata SQL omitted opener");
		Check(operations.size() == 4, "metadata SQL did not reach bridge");
	});
	run("scopes isolate identities and rename rejects before side effects", [] {
		Fixture f;
		f.Secret("root");
		f.Query("CREATE TEMPORARY SECRET scoped (TYPE hdfs, FS_NAME 'hdfs://cluster', "
		        "SCOPE 'hdfs://cluster/finance', USER 'finance')");
		f.fs.FileExists("hdfs://cluster/finance", &f.opener);
		f.fs.FileExists("hdfs://cluster/finance2", &f.opener);
		Check(connects.size() == 2 && connects[0].user == "finance" && connects[1].user == "root",
		      "scope boundary mismatch");
		auto count = operations.size();
		ExpectError([&] { f.fs.MoveFile("hdfs://cluster/finance/a", "hdfs://cluster/b", &f.opener); });
		ExpectError([&] { f.fs.MoveFile("hdfs://cluster/a", "hdfs://other/b", &f.opener); });
		Check(operations.size() == count, "rejected rename touched storage");
	});
	run("same values share independent of secret name and CONF order", [] {
		Fixture f;
		f.Secret("alice", ", CONF MAP {'a':'1','b':'2'}");
		f.fs.FileExists("hdfs://cluster/a", &f.opener);
		f.Query("CREATE TEMPORARY SECRET scoped (TYPE hdfs, FS_NAME "
		        "'hdfs://cluster', SCOPE 'hdfs://cluster/sub', USER 'alice', CONF "
		        "MAP {'b':'2','a':'1'})");
		f.fs.FileExists("hdfs://cluster/sub/a", &f.opener);
		Check(connects.size() == 1, "equal configurations did not share");
		f.Secret("alice", ", CONF MAP {'a':'different','b':'2'}");
		f.fs.FileExists("hdfs://cluster/a", &f.opener);
		Check(connects.size() == 2, "different CONF reused old client");
	});
	run("matched authentication and permission failures never fall back", [] {
		Fixture f;
		f.Secret("alice");
		fail_connect = true;
		ExpectError([&] { f.fs.FileExists("hdfs://cluster/a", &f.opener); });
		Check(connects.size() == 1 && connects[0].explicit_config && operations.empty(), "connect failure fell back");
		fail_connect = false;
		next_error = HDFS_ERR_PERMISSION;
		ExpectError([&] { f.fs.FileExists("hdfs://cluster/a", &f.opener); });
		Check(connects.size() == 2 && connects.back().explicit_config && operations.size() == 1,
		      "permission failure retried/fell back");
	});
	run("principal keytab and source mode each isolate clients", [] {
		Fixture f;
		f.fs.FileExists("hdfs://cluster/file", &f.opener);
		f.Query("CREATE TEMPORARY SECRET auth (TYPE hdfs, FS_NAME 'hdfs://cluster')");
		f.fs.FileExists("hdfs://cluster/file", &f.opener);
		Check(connects.size() == 2 && !connects[0].explicit_config && connects[1].explicit_config,
		      "environment and secret clients mixed");
		for (auto options :
		     {", PRINCIPAL 'alice@REALM', KEYTAB '/tmp/a.keytab'", ", PRINCIPAL 'bob@REALM', KEYTAB '/tmp/a.keytab'",
		      ", PRINCIPAL 'bob@REALM', KEYTAB '/tmp/b.keytab'"}) {
			f.Secret("same", options);
			f.fs.FileExists("hdfs://cluster/file", &f.opener);
		}
		Check(connects.size() == 5, "principal/keytab changes reused a client");
	});
	run("streams and handles survive opener destruction and retry with opening "
	    "config",
	    [] {
		    Fixture f;
		    unique_ptr<HdfsListStream> stream;
		    unique_ptr<FileHandle> handle;
		    {
			    Connection temporary(f.db);
			    auto result = temporary.Query("CREATE TEMPORARY SECRET auth (TYPE hdfs, "
			                                  "FS_NAME 'hdfs://cluster', USER 'alice')");
			    Check(!result->HasError(), "temporary secret creation failed");
			    temporary.Query("BEGIN TRANSACTION");
			    ClientContextFileOpener opener(*temporary.context);
			    stream = f.fs.OpenListStream("hdfs://cluster/dir", 1, {}, &opener);
			    handle = f.fs.OpenFile("hdfs://cluster/file", FileFlags::FILE_FLAGS_READ, &opener);
		    }
		    f.Secret("bob");
		    vector<HdfsEntry> entries;
		    next_error = HDFS_ERR_CONNECTION;
		    Check(!stream->Next(entries, 10), "empty stream should finish");
		    Check(connects.size() == 2 && connects[0].user == "alice" && connects[1].user == "alice",
		          "stream retry resolved replacement secret");
		    f.fs.GetLastModifiedTime(*handle);
		    Check(connects.size() == 2 && operations.back() == 2, "handle lost bound config after context destruction");
		    f.fs.FileExists("hdfs://cluster/file", &f.opener);
		    Check(connects.size() == 3 && connects.back().user == "bob", "new operation missed replacement secret");
	    });
	run("bare paths and metadata SQL retain the default filesystem", [] {
		Fixture f;
		f.fs.FileExists("/absolute/file", &f.opener);
		f.fs.FileExists("relative/file", &f.opener);
		f.fs.FileExists("hdfs:///another/file", &f.opener);
		Check(connects.size() == 1 && connects[0].url.empty() && !connects[0].explicit_config,
		      "bare paths did not share the environment defaultFS client");
		f.Query("SELECT hdfs_exists('/absolute/file')");
		f.Query("SELECT * FROM hdfs_stat('relative/file')");
		f.Query("SELECT * FROM hdfs_ls('/absolute')");
		f.Query("SELECT * FROM hdfs_ls('/absolute/*')");
		Check(connects.size() == 2 && connects.back().url.empty() && !connects.back().explicit_config,
		      "metadata SQL treated a bare path as a connection URL");
		Check(operations.size() == 7, "bare path operations did not reach the bridge");
	});
	std::cout << cases << " cases, " << assertions << " assertions, " << failures << " failures\n";
	return failures ? 1 : 0;
}
