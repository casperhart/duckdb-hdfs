#define DUCKDB_EXTENSION_MAIN

#include "hdfs_extension.hpp"

#include "duckdb.hpp"

#include "duckdb/main/config.hpp"
#include "hdfs_filesystem.hpp"
#include "hdfs_functions.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register the hdfs:// virtual filesystem. Connection config (Hadoop config
	// dir, effective user, Kerberos) is resolved by hdfs-native from the
	// standard Hadoop environment (HADOOP_CONF_DIR / HADOOP_HOME, HADOOP_USER_NAME
	// / keytab / current account), so connection details need no settings here.
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("hdfs_list_parallelism",
	                          "Maximum number of concurrent HDFS directory-listing RPCs used by recursive "
	                          "listings (hdfs_ls(..., recursive := true)); 1 disables parallelism",
	                          LogicalType::UBIGINT, Value::UBIGINT(16));

	auto &fs = db.GetFileSystem();
	auto hdfs_fs = make_uniq<HdfsFileSystem>();
	// Borrowed by the metadata SQL functions below; the VirtualFileSystem owns it
	// for the database's lifetime, so the raw pointer stays valid.
	auto *hdfs_ptr = hdfs_fs.get();
	fs.RegisterSubSystem(std::move(hdfs_fs));

	// Register the HDFS metadata functions (hdfs_ls / hdfs_glob / hdfs_stat /
	// hdfs_exists).
	RegisterHdfsFunctions(loader, hdfs_ptr);
}

void HdfsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string HdfsExtension::Name() {
	return "hdfs";
}

std::string HdfsExtension::Version() const {
#ifdef EXT_VERSION_HDFS
	return EXT_VERSION_HDFS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hdfs, loader) {
	duckdb::LoadInternal(loader);
}
}
