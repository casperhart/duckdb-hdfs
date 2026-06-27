#define DUCKDB_EXTENSION_MAIN

#include "hdfs_extension.hpp"

#include "duckdb.hpp"

#include "hdfs_filesystem.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register the hdfs:// virtual filesystem. Connection config (Hadoop config
	// dir, effective user, Kerberos) is resolved by hdfs-native from the
	// standard Hadoop environment (HADOOP_CONF_DIR / HADOOP_HOME, HADOOP_USER_NAME
	// / keytab / current account), so there are no extension settings to register.
	auto &fs = loader.GetDatabaseInstance().GetFileSystem();
	fs.RegisterSubSystem(make_uniq<HdfsFileSystem>());
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
