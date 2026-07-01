#include "hdfs_functions.hpp"

#include "hdfs_filesystem.hpp"

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

// Which HDFS metadata call a hdfs_ls / hdfs_glob / hdfs_stat table function
// invocation maps to. All three share one output schema and execution path;
// only the backing RPC differs.
enum class HdfsMetaOp { LIST, GLOB, STAT };

// Carries the borrowed filesystem and the operation into a table function's
// bind, via TableFunction::function_info.
struct HdfsTableFunctionInfo : public TableFunctionInfo {
	HdfsTableFunctionInfo(HdfsFileSystem *hdfs, HdfsMetaOp op) : hdfs(hdfs), op(op) {
	}
	HdfsFileSystem *hdfs;
	HdfsMetaOp op;
};

// Same, for the hdfs_exists scalar function.
struct HdfsScalarFunctionInfo : public ScalarFunctionInfo {
	explicit HdfsScalarFunctionInfo(HdfsFileSystem *hdfs) : hdfs(hdfs) {
	}
	HdfsFileSystem *hdfs;
};

// Column layout shared by hdfs_ls / hdfs_glob / hdfs_stat.
enum ColumnIndex : idx_t {
	COL_PATH = 0,
	COL_NAME,
	COL_TYPE,
	COL_SIZE,
	COL_OWNER,
	COL_GROUP,
	COL_PERMISSIONS,
	COL_MODE,
	COL_REPLICATION,
	COL_BLOCK_SIZE,
	COL_LAST_MODIFIED,
	COL_LAST_ACCESSED,
	COL_COUNT
};

void DefineMetadataSchema(vector<LogicalType> &types, vector<string> &names) {
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,   LogicalType::BIGINT,
	         LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,   LogicalType::USMALLINT,
	         LogicalType::INTEGER, LogicalType::BIGINT,  LogicalType::TIMESTAMP, LogicalType::TIMESTAMP};
	names = {"path",        "name", "type",        "size",       "owner",         "group",
	         "permissions", "mode", "replication", "block_size", "last_modified", "last_accessed"};
}

// Format the 9 permission bits as "rwxr-xr-x", matching `hdfs dfs -ls`.
string PermissionString(uint16_t mode) {
	static const char *rwx[] = {"---", "--x", "-w-", "-wx", "r--", "r-x", "rw-", "rwx"};
	string out;
	out += rwx[(mode >> 6) & 0x7];
	out += rwx[(mode >> 3) & 0x7];
	out += rwx[mode & 0x7];
	return out;
}

struct HdfsMetaBindData : public TableFunctionData {
	HdfsFileSystem *hdfs = nullptr;
	HdfsMetaOp op = HdfsMetaOp::LIST;
	string path;
	bool recursive = false;
};

unique_ptr<FunctionData> HdfsMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto &info = input.info->Cast<HdfsTableFunctionInfo>();
	auto result = make_uniq<HdfsMetaBindData>();
	result->hdfs = info.hdfs;
	result->op = info.op;
	result->path = input.inputs[0].GetValue<string>();

	auto recursive = input.named_parameters.find("recursive");
	if (recursive != input.named_parameters.end()) {
		result->recursive = BooleanValue::Get(recursive->second);
	}

	DefineMetadataSchema(return_types, names);
	return std::move(result);
}

// Runs the RPC once and holds the resulting rows for streaming out.
struct HdfsMetaGlobalState : public GlobalTableFunctionState {
	vector<HdfsEntry> entries;
	idx_t offset = 0;
};

unique_ptr<GlobalTableFunctionState> HdfsMetaInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<HdfsMetaBindData>();
	auto state = make_uniq<HdfsMetaGlobalState>();
	switch (bind_data.op) {
	case HdfsMetaOp::LIST:
		state->entries = bind_data.hdfs->ListStatus(bind_data.path, bind_data.recursive);
		break;
	case HdfsMetaOp::GLOB:
		state->entries = bind_data.hdfs->GlobStatus(bind_data.path);
		break;
	case HdfsMetaOp::STAT:
		state->entries.push_back(bind_data.hdfs->Stat(bind_data.path));
		break;
	}
	return std::move(state);
}

void HdfsMetaExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<HdfsMetaGlobalState>();
	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.entries.size() - state.offset);

	auto path_data = FlatVector::GetData<string_t>(output.data[COL_PATH]);
	auto name_data = FlatVector::GetData<string_t>(output.data[COL_NAME]);
	auto type_data = FlatVector::GetData<string_t>(output.data[COL_TYPE]);
	auto size_data = FlatVector::GetData<int64_t>(output.data[COL_SIZE]);
	auto owner_data = FlatVector::GetData<string_t>(output.data[COL_OWNER]);
	auto group_data = FlatVector::GetData<string_t>(output.data[COL_GROUP]);
	auto perm_data = FlatVector::GetData<string_t>(output.data[COL_PERMISSIONS]);
	auto mode_data = FlatVector::GetData<uint16_t>(output.data[COL_MODE]);
	auto repl_data = FlatVector::GetData<int32_t>(output.data[COL_REPLICATION]);
	auto block_data = FlatVector::GetData<int64_t>(output.data[COL_BLOCK_SIZE]);
	auto mtime_data = FlatVector::GetData<timestamp_t>(output.data[COL_LAST_MODIFIED]);
	auto atime_data = FlatVector::GetData<timestamp_t>(output.data[COL_LAST_ACCESSED]);

	for (idx_t i = 0; i < count; i++) {
		auto &entry = state.entries[state.offset + i];
		path_data[i] = StringVector::AddString(output.data[COL_PATH], entry.url);
		name_data[i] = StringVector::AddString(output.data[COL_NAME], entry.name);
		type_data[i] = StringVector::AddString(output.data[COL_TYPE], entry.is_dir ? "directory" : "file");
		size_data[i] = entry.size;
		owner_data[i] = StringVector::AddString(output.data[COL_OWNER], entry.owner);
		group_data[i] = StringVector::AddString(output.data[COL_GROUP], entry.group);
		perm_data[i] = StringVector::AddString(output.data[COL_PERMISSIONS], PermissionString(entry.permission));
		mode_data[i] = entry.permission;
		// HDFS reports replication / block size only for files; surface NULL for
		// directories (the bridge signals this with -1).
		if (entry.replication < 0) {
			FlatVector::SetNull(output.data[COL_REPLICATION], i, true);
		} else {
			repl_data[i] = entry.replication;
		}
		if (entry.block_size < 0) {
			FlatVector::SetNull(output.data[COL_BLOCK_SIZE], i, true);
		} else {
			block_data[i] = entry.block_size;
		}
		// HDFS timestamps are epoch milliseconds.
		mtime_data[i] = Timestamp::FromEpochMs(static_cast<int64_t>(entry.mtime));
		atime_data[i] = Timestamp::FromEpochMs(static_cast<int64_t>(entry.atime));
	}
	state.offset += count;
	output.SetCardinality(count);
}

TableFunction MakeMetaFunction(const string &name) {
	// The operation and filesystem are carried on function_info (set at
	// registration) and read in bind; the schema/execution are shared.
	return TableFunction(name, {LogicalType::VARCHAR}, HdfsMetaExecute, HdfsMetaBind, HdfsMetaInit);
}

// hdfs_exists(path) -> BOOLEAN
void HdfsExistsExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto *hdfs = func_expr.function.function_info->Cast<HdfsScalarFunctionInfo>().hdfs;
	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(),
	                                       [&](string_t path) { return hdfs->Exists(path.GetString()); });
}

} // namespace

void RegisterHdfsFunctions(ExtensionLoader &loader, HdfsFileSystem *hdfs) {
	// hdfs_ls(path, recursive := false)
	auto ls = MakeMetaFunction("hdfs_ls");
	ls.named_parameters["recursive"] = LogicalType::BOOLEAN;
	ls.function_info = make_shared_ptr<HdfsTableFunctionInfo>(hdfs, HdfsMetaOp::LIST);
	loader.RegisterFunction(ls);

	// hdfs_glob(pattern)
	auto glob = MakeMetaFunction("hdfs_glob");
	glob.function_info = make_shared_ptr<HdfsTableFunctionInfo>(hdfs, HdfsMetaOp::GLOB);
	loader.RegisterFunction(glob);

	// hdfs_stat(path)
	auto stat = MakeMetaFunction("hdfs_stat");
	stat.function_info = make_shared_ptr<HdfsTableFunctionInfo>(hdfs, HdfsMetaOp::STAT);
	loader.RegisterFunction(stat);

	// hdfs_exists(path) -> BOOLEAN
	ScalarFunction exists("hdfs_exists", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, HdfsExistsExecute);
	exists.function_info = make_shared_ptr<HdfsScalarFunctionInfo>(hdfs);
	loader.RegisterFunction(exists);
}

} // namespace duckdb
