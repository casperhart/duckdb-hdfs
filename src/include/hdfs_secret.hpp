#pragma once

#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <map>

namespace duckdb {

/** 一次操作解析出的自有配置；不保留查询上下文或 Secret 引用。 */
struct HdfsConfig {
	bool from_secret = false;
	string fs_name;
	string user;
	string principal;
	string keytab;
	std::map<string, string> conf;

	/** 按配置值隔离客户端，字段使用长度前缀避免拼接歧义。 */
	string CacheKey() const;
};

/** 注册 HDFS 的 config Secret 类型及创建参数。 */
void RegisterHdfsSecret(ExtensionLoader &loader);
/** 未匹配 Secret 时保留环境模式；匹配后的错误直接向上传递。 */
HdfsConfig ResolveHdfsConfig(const string &url, optional_ptr<FileOpener> opener);

} // namespace duckdb
