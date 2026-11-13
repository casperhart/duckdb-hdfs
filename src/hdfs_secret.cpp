#include "hdfs_secret.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {
namespace {

void CheckString(const string &value, const string &name) {
	if (value.find(char(0)) != string::npos) {
		throw InvalidInputException("HDFS %s cannot contain NUL", name);
	}
}

string DirectoryScope(string scope) {
	if (scope.empty() || scope.back() != '/') {
		scope += '/';
	}
	return scope;
}

HdfsConfig ReadConfig(const case_insensitive_tree_t<Value> &values) {
	HdfsConfig config;
	config.from_secret = true;
	for (auto &kv : values) {
		if (kv.second.IsNull()) {
			throw InvalidInputException("HDFS secret option %s cannot be NULL", kv.first);
		}
		if (kv.second.type().id() == LogicalTypeId::VARCHAR) {
			CheckString(kv.second.GetValue<string>(), kv.first);
		}
	}
	auto get = [&](const string &name, bool nonempty) {
		auto it = values.find(name);
		if (it == values.end()) {
			return string();
		}
		auto value = it->second.GetValue<string>();
		if (nonempty && value.empty()) {
			throw InvalidInputException("HDFS %s cannot be empty", name);
		}
		return value;
	};
	config.fs_name = get("fs_name", true);
	if (config.fs_name.empty()) {
		throw InvalidInputException("HDFS secret requires fs_name");
	}
	if (config.fs_name.compare(0, 7, "hdfs://") != 0 || config.fs_name.size() == 7 ||
	    config.fs_name.find_first_of("/?#", 7) != string::npos) {
		throw InvalidInputException("HDFS fs_name must be hdfs://<authority> without a path");
	}
	config.user = get("user", false);
	config.principal = get("principal", true);
	config.keytab = get("keytab", true);
	if (!config.keytab.empty()) {
		if (config.principal.empty()) {
			throw InvalidInputException("HDFS keytab requires principal");
		}
		LocalFileSystem fs;
		if (!fs.IsPathAbsolute(config.keytab)) {
			throw InvalidInputException("HDFS keytab requires an absolute path");
		}
	}
	auto conf = values.find("conf");
	if (conf != values.end()) {
		for (auto &entry : MapValue::GetChildren(conf->second)) {
			auto &kv = StructValue::GetChildren(entry);
			if (kv[0].IsNull() || kv[1].IsNull()) {
				throw InvalidInputException("HDFS conf keys and values cannot be NULL");
			}
			auto key = kv[0].GetValue<string>();
			auto value = kv[1].GetValue<string>();
			CheckString(key, "conf key");
			CheckString(value, "conf value");
			config.conf.emplace(std::move(key), std::move(value));
		}
	}
	if (!config.principal.empty()) {
		auto auth = config.conf.emplace("hadoop.security.authentication", "kerberos");
		if (auth.first->second != "kerberos") {
			throw InvalidInputException("HDFS principal requires kerberos authentication");
		}
	}
	return config;
}

unique_ptr<BaseSecret> CreateHdfsSecret(ClientContext &, CreateSecretInput &input) {
	case_insensitive_tree_t<Value> values;
	for (auto &kv : input.options) {
		values.emplace(kv.first, kv.second);
	}
	auto config = ReadConfig(values);
	auto scopes = input.scope;
	if (scopes.empty()) {
		scopes.push_back(config.fs_name + "/");
	}
	for (auto &scope : scopes) {
		CheckString(scope, "scope");
		scope = DirectoryScope(scope);
		if (scope.compare(0, config.fs_name.size() + 1, config.fs_name + "/") != 0 ||
		    scope.find_first_of("?#") != string::npos) {
			throw InvalidInputException("HDFS secret scope must belong to fs_name");
		}
	}
	auto secret = make_uniq<KeyValueSecret>(scopes, input.type, input.provider, input.name);
	secret->secret_map = std::move(values);
	// 只保存 keytab 路径；展示时隐藏路径及可能包含敏感参数的 CONF。
	secret->redact_keys = {"keytab", "conf"};
	return std::move(secret);
}

} // namespace

void RegisterHdfsSecret(ExtensionLoader &loader) {
	SecretType type;
	type.name = "hdfs";
	type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	type.default_provider = "config";
	type.extension = "hdfs";
	loader.RegisterSecretType(type);
	CreateSecretFunction function;
	function.secret_type = "hdfs";
	function.provider = "config";
	function.function = CreateHdfsSecret;
	for (auto name : {"fs_name", "user", "principal", "keytab"}) {
		function.named_parameters[name] = LogicalType::VARCHAR;
	}
	function.named_parameters["conf"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	loader.RegisterFunction(function);
}

HdfsConfig ResolveHdfsConfig(const string &url, optional_ptr<FileOpener> opener) {
	CheckString(url, "path");
	HdfsConfig config;
	// 与文件系统的路径解析保持一致：裸路径和 hdfs:/// 均使用默认文件系统。
	if (url.compare(0, 7, "hdfs://") == 0) {
		auto slash = url.find('/', 7);
		config.fs_name = slash == string::npos ? url : url.substr(0, slash);
		if (config.fs_name == "hdfs://") {
			config.fs_name.clear();
		}
	}
	auto manager = FileOpener::TryGetSecretManager(opener);
	auto transaction = FileOpener::TryGetCatalogTransaction(opener);
	if (!manager || !transaction) {
		return config;
	}
	// 目录自身也补 /，使 scope 不会把 finance2 误认为 finance 的子目录。
	auto match = manager->LookupSecret(*transaction, DirectoryScope(url), "hdfs");
	if (!match.HasMatch()) {
		return config;
	}
	const auto &secret = dynamic_cast<const KeyValueSecret &>(match.GetSecret());
	auto resolved = ReadConfig(secret.secret_map);
	if (resolved.fs_name != config.fs_name) {
		throw InvalidInputException("HDFS path does not belong to secret fs_name");
	}
	return resolved;
}

string HdfsConfig::CacheKey() const {
	string key = from_secret ? "secret:" : "environment:";
	auto append = [&](const string &value) {
		key += std::to_string(value.size()) + ":" + value;
	};
	for (auto *value : {&fs_name, &user, &principal, &keytab}) {
		append(*value);
	}
	for (auto &kv : conf) {
		append(kv.first);
		append(kv.second);
	}
	return key;
}

} // namespace duckdb
