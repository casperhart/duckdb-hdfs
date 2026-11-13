#include "duckdb/common/exception.hpp"
#include "hdfs_filesystem.hpp"

#include <cstring>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>

// 仅替换远端桥接；被测试的打开、写入和游标逻辑均使用真实文件系统实现。
struct hdfs_client_t {};
struct hdfs_reader_t {};
struct hdfs_writer_t {};

namespace {
struct BridgeCalls {
	int connects = 0;
	int creates = 0;
	int writes = 0;
	bool fail_write = false;
	std::string bytes;
} calls;
int assertions = 0;

void Check(bool value, const char *message) {
	++assertions;
	if (!value) {
		throw std::runtime_error(message);
	}
}

template <class EXCEPTION, class FN>
void ExpectError(FN operation, const char *message) {
	bool rejected = false;
	try {
		operation();
	} catch (const EXCEPTION &) {
		rejected = true;
	}
	Check(rejected, message);
}
} // namespace

extern "C" {
hdfs_client_t *__wrap_hdfs_bridge_connect(const char *, const char *, const char *, hdfs_status_t *) {
	++calls.connects;
	return new hdfs_client_t;
}
void __wrap_hdfs_bridge_free_client(hdfs_client_t *client) {
	delete client;
}
hdfs_writer_t *__wrap_hdfs_bridge_create(hdfs_client_t *, const char *, bool, hdfs_status_t *) {
	++calls.creates;
	return new hdfs_writer_t;
}
int32_t __wrap_hdfs_bridge_close_writer(hdfs_writer_t *writer, hdfs_status_t *) {
	delete writer;
	return 0;
}
int64_t __wrap_hdfs_bridge_write(hdfs_writer_t *, const uint8_t *buffer, int64_t length, hdfs_status_t *status) {
	++calls.writes;
	if (calls.fail_write) {
		status->code = HDFS_ERR_IO;
		return -1;
	}
	calls.bytes.append(reinterpret_cast<const char *>(buffer), static_cast<size_t>(length));
	return length;
}
hdfs_reader_t *__wrap_hdfs_bridge_open(hdfs_client_t *, const char *, hdfs_status_t *) {
	return new hdfs_reader_t;
}
void __wrap_hdfs_bridge_close_reader(hdfs_reader_t *reader) {
	delete reader;
}
int64_t __wrap_hdfs_bridge_file_size(hdfs_reader_t *) {
	return 6;
}
int64_t __wrap_hdfs_bridge_read_range(hdfs_reader_t *, uint8_t *buffer, int64_t length, uint64_t offset,
                                      hdfs_status_t *) {
	std::memcpy(buffer, &"abcdef"[offset], static_cast<size_t>(length));
	return length;
}
}

using namespace duckdb;

int main() {
	int failures = 0;
	int tests = 0;
	auto run = [&](const char *name, const std::function<void()> &test) {
		++tests;
		calls = BridgeCalls {};
		try {
			test();
			std::cout << "PASS: " << name << '\n';
		} catch (const std::exception &error) {
			++failures;
			std::cerr << "FAIL: " << name << ": " << error.what() << '\n';
		}
	};
	const auto read_write = FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE;
	const FileOpenFlags modes[] = {read_write, read_write | FileFlags::FILE_FLAGS_FILE_CREATE,
	                               read_write | FileFlags::FILE_FLAGS_FILE_CREATE_NEW};
	for (auto flags : modes) {
		run("read/write open rejects before connecting or creating", [&] {
			HdfsFileSystem fs;
			ExpectError<NotImplementedException>([&] { fs.OpenFile("hdfs://test/file", flags); },
			                                     "read/write open must reject");
			Check(calls.connects == 0 && calls.creates == 0, "rejected open touched remote storage");
		});
	}
	run("positioned writes append in order at the current position", [&] {
		HdfsFileSystem fs;
		auto handle = fs.OpenFile("hdfs://test/file", FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		char first[] = "abc", second[] = "de";
		fs.Write(*handle, first, 3, 0);
		fs.Write(*handle, second, 2, 3);
		Check(calls.bytes == "abcde" && calls.writes == 2, "sequential write bytes differ");
		Check(fs.SeekPosition(*handle) == 5 && fs.GetFileSize(*handle) == 5, "write cursor/length differ");
	});
	run("random writes reject without changing content or cursor", [&] {
		HdfsFileSystem fs;
		auto handle = fs.OpenFile("hdfs://test/file", FileFlags::FILE_FLAGS_WRITE);
		char data[] = "abc";
		fs.Write(*handle, data, 3);
		for (idx_t offset : {idx_t(0), idx_t(4)}) {
			ExpectError<NotImplementedException>([&] { fs.Write(*handle, data, 1, offset); },
			                                     "random write must reject");
			Check(calls.bytes == "abc" && calls.writes == 1, "rejected write changed remote content");
			Check(fs.SeekPosition(*handle) == 3 && fs.GetFileSize(*handle) == 3, "rejected write changed cursor");
		}
	});
	run("writer seek permits only the current position", [&] {
		HdfsFileSystem fs;
		auto handle = fs.OpenFile("hdfs://test/file", FileFlags::FILE_FLAGS_WRITE);
		char data[] = "abc";
		fs.Write(*handle, data, 3);
		fs.Seek(*handle, 3);
		for (idx_t offset : {idx_t(0), idx_t(4)}) {
			ExpectError<NotImplementedException>([&] { fs.Seek(*handle, offset); }, "writer seek must reject");
			Check(fs.SeekPosition(*handle) == 3, "rejected seek changed cursor");
		}
		fs.Write(*handle, data, 1);
		Check(calls.bytes == "abca" && fs.SeekPosition(*handle) == 4, "write after rejected seek differs");
	});
	run("writer reset cannot rewind written data", [&] {
		HdfsFileSystem fs;
		auto handle = fs.OpenFile("hdfs://test/file", FileFlags::FILE_FLAGS_WRITE);
		fs.Reset(*handle);
		char data[] = "abc";
		fs.Write(*handle, data, 3);
		ExpectError<NotImplementedException>([&] { fs.Reset(*handle); }, "writer reset must reject after writing");
		Check(fs.SeekPosition(*handle) == 3 && fs.GetFileSize(*handle) == 3, "rejected reset changed cursor");
	});
	run("reader seek and reset retain positional reading", [&] {
		HdfsFileSystem fs;
		auto handle = fs.OpenFile("hdfs://test/file", FileFlags::FILE_FLAGS_READ);
		char data[3] = {};
		fs.Seek(*handle, 2);
		Check(fs.Read(*handle, data, 2) == 2 && std::string(data, 2) == "cd", "reader seek returned wrong data");
		fs.Reset(*handle);
		Check(fs.Read(*handle, data, 2) == 2 && std::string(data, 2) == "ab", "reader reset returned wrong data");
		Check(calls.creates == 0 && calls.writes == 0, "read-only handle changed storage");
	});
	run("positioned write propagates bridge errors without advancing", [&] {
		HdfsFileSystem fs;
		auto handle = fs.OpenFile("hdfs://test/file", FileFlags::FILE_FLAGS_WRITE);
		char data[] = "abc";
		calls.fail_write = true;
		ExpectError<IOException>([&] { fs.Write(*handle, data, 3, 0); }, "write error must propagate");
		Check(calls.writes == 1 && calls.bytes.empty(), "failed write changed content");
		Check(fs.SeekPosition(*handle) == 0 && fs.GetFileSize(*handle) == 0, "failed write changed cursor");
	});
	std::cout << tests << " cases, " << assertions << " assertions, " << failures << " failures\n";
	return failures == 0 ? 0 : 1;
}
