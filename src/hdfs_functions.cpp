#include "hdfs_functions.hpp"

#include "hdfs_filesystem.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

// Which HDFS metadata call a hdfs_ls / hdfs_stat table function invocation
// maps to. Both share one output schema and execution path; only the backing
// RPC differs.
enum class HdfsMetaOp { LIST, STAT };

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

// Column layout shared by hdfs_ls / hdfs_stat.
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
	// Per-call overrides of the hdfs_skip_permission_errors /
	// hdfs_include_hidden settings; NULL (unset) falls back to the setting at
	// execution time.
	Value skip_permission_errors;
	Value include_hidden;
};

unique_ptr<FunctionData> HdfsMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto &info = input.info->Cast<HdfsTableFunctionInfo>();
	auto result = make_uniq<HdfsMetaBindData>();
	result->hdfs = info.hdfs;
	result->op = info.op;
	result->path = input.inputs[0].GetValue<string>();
	// Only hdfs_ls registers these; for hdfs_stat the map is empty.
	for (auto &kv : input.named_parameters) {
		if (kv.first == "skip_permission_errors") {
			result->skip_permission_errors = kv.second;
		} else if (kv.first == "include_hidden") {
			result->include_hidden = kv.second;
		}
	}

	DefineMetadataSchema(return_types, names);
	return std::move(result);
}

// LIST streams batches from a background walk as they arrive; STAT runs its
// RPC once in Init and holds the resulting row.
struct HdfsMetaGlobalState : public GlobalTableFunctionState {
	unique_ptr<HdfsListStream> stream;
	vector<HdfsEntry> entries;
	idx_t offset = 0;
};

// hdfs_list_parallelism (registered at extension load) bounds the number of
// concurrent listing RPCs of a walk that touches more than one directory.
// Capped at INT32_MAX for the FFI signature.
int32_t ListParallelism(ClientContext &context) {
	Value setting;
	if (context.TryGetCurrentSetting("hdfs_list_parallelism", setting)) {
		return static_cast<int32_t>(
		    MinValue<uint64_t>(setting.GetValue<uint64_t>(), NumericLimits<int32_t>::Maximum()));
	}
	return static_cast<int32_t>(DEFAULT_HDFS_LIST_PARALLELISM);
}

// Resolve one walk flag: the per-call named parameter wins; unset (NULL)
// falls back to the session setting, which shares its default with Glob().
bool WalkFlag(ClientContext &context, const Value &override_value, const char *setting_name) {
	if (!override_value.IsNull()) {
		return BooleanValue::Get(override_value);
	}
	Value setting;
	if (context.TryGetCurrentSetting(setting_name, setting)) {
		return setting.GetValue<bool>();
	}
	return false;
}

unique_ptr<GlobalTableFunctionState> HdfsMetaInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<HdfsMetaBindData>();
	auto state = make_uniq<HdfsMetaGlobalState>();
	switch (bind_data.op) {
	case HdfsMetaOp::LIST: {
		HdfsWalkOptions options;
		options.skip_permission_errors =
		    WalkFlag(context, bind_data.skip_permission_errors, "hdfs_skip_permission_errors");
		options.include_hidden = WalkFlag(context, bind_data.include_hidden, "hdfs_include_hidden");
		// A path with glob characters returns the matched entries themselves
		// (files and directories; `**` walks the subtree); a literal path
		// lists the directory's children. Escapes don't suppress detection:
		// like DuckDB's globber, any of these characters selects glob mode,
		// and `\*` etc. are then matched literally by the pattern itself.
		if (bind_data.path.find_first_of("*?[{") != string::npos) {
			state->stream = bind_data.hdfs->OpenGlobStream(bind_data.path, ListParallelism(context), options);
		} else {
			state->stream = bind_data.hdfs->OpenListStream(bind_data.path, ListParallelism(context), options);
		}
		break;
	}
	case HdfsMetaOp::STAT:
		state->entries.push_back(bind_data.hdfs->Stat(bind_data.path));
		break;
	}
	return std::move(state);
}

void HdfsMetaExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<HdfsMetaGlobalState>();
	// LIST pulls the next batch from the background walk (blocking until rows
	// arrive or the walk ends); STAT emits a slice of the materialized rows.
	vector<HdfsEntry> batch;
	const HdfsEntry *rows;
	idx_t count;
	if (state.stream) {
		state.stream->Next(batch, STANDARD_VECTOR_SIZE);
		rows = batch.data();
		count = batch.size();
	} else {
		count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.entries.size() - state.offset);
		rows = state.entries.data() + state.offset;
		state.offset += count;
	}

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
		const auto &entry = rows[i];
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
	// hdfs_ls(path_or_pattern, skip_permission_errors := ..., include_hidden := ...)
	auto ls = MakeMetaFunction("hdfs_ls");
	ls.function_info = make_shared_ptr<HdfsTableFunctionInfo>(hdfs, HdfsMetaOp::LIST);
	// Per-call overrides of the same-named hdfs_* settings (both default false).
	ls.named_parameters["skip_permission_errors"] = LogicalType::BOOLEAN;
	ls.named_parameters["include_hidden"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(ls);

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
