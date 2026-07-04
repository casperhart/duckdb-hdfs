#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class HdfsFileSystem;

// Register the HDFS metadata SQL functions (hdfs_ls, hdfs_stat,
// hdfs_exists). `hdfs` is the filesystem instance owned by the database's
// VirtualFileSystem; it outlives the functions, which borrow it for its
// connection pooling / reconnect logic.
void RegisterHdfsFunctions(ExtensionLoader &loader, HdfsFileSystem *hdfs);

} // namespace duckdb
