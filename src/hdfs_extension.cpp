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
	                          "Maximum number of concurrent HDFS directory-listing RPCs used by listings and "
	                          "globs that walk more than one directory (e.g. hdfs_ls('/path/**')); 1 disables "
	                          "parallelism",
	                          LogicalType::UBIGINT, Value::UBIGINT(DEFAULT_HDFS_LIST_PARALLELISM));
	config.AddExtensionOption("hdfs_skip_permission_errors",
	                          "Prune subtrees whose listing fails with a permission error during HDFS globs and "
	                          "recursive listings instead of failing the query; an error on the listed path or "
	                          "glob root itself still fails",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("hdfs_include_hidden",
	                          "Return hidden HDFS entries (names starting with '_' or '.', e.g. _temporary, "
	                          "_SUCCESS) from listings, globs and scans; by default they are excluded unless a "
	                          "glob component names them explicitly (e.g. '_*')",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	auto &fs = db.GetFileSystem();
	auto hdfs_fs = make_uniq<HdfsFileSystem>();
	// Borrowed by the metadata SQL functions below; the VirtualFileSystem owns it
	// for the database's lifetime, so the raw pointer stays valid.
	auto *hdfs_ptr = hdfs_fs.get();
	fs.RegisterSubSystem(std::move(hdfs_fs));

	// Register the HDFS metadata functions (hdfs_ls / hdfs_stat / hdfs_exists).
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
