# DuckDB HDFS extension

`hdfs` adds a native HDFS filesystem to DuckDB: read and write `hdfs://` URLs
directly — no JVM, no `libhdfs`. It is backed by the pure-Rust
[`hdfs-native`](https://github.com/Kimahriman/hdfs-native) client through a
thin C FFI bridge (`hdfs-bridge/`).

```sql
-- Read
SELECT * FROM 'hdfs://namenode:8020/data/events/*.parquet';
SELECT * FROM read_csv('hdfs://namenode:8020/data/users.csv');

-- Write
COPY (SELECT * FROM big_table) TO 'hdfs://namenode:8020/out/table.parquet' (FORMAT parquet);
```

## What works

- Reading any file format DuckDB supports (Parquet, CSV, JSON, …) over `hdfs://`.
- Writing via `COPY ... TO`, including `PARTITION_BY` (HDFS is append-only;
  random writes are not supported).
- Globbing, directory listing, file metadata, and directory/file management
  (create/remove directories, remove files, rename).
- Concurrent positional reads (DuckDB's parallel Parquet reader).
- HDFS encryption zones (Transparent Data Encryption): data is
  decrypted/encrypted client-side, unwrapping keys via the KMS over TLS.
  Requires the Hadoop key provider to be configured
  (`hadoop.security.key.provider.path` / `dfs.encryption.key.provider.uri`).

## Listing and metadata

Three read-only SQL functions expose HDFS metadata. Unlike DuckDB's built-in
`glob()`, they include directories and return full `FileStatus` metadata.

```sql
-- List a directory's children.
SELECT name, type, size, owner, permissions
FROM hdfs_ls('hdfs://namenode:8020/data');

-- Recurse with '**', or glob — matched entries come back as rows.
SELECT * FROM hdfs_ls('hdfs://namenode:8020/data/**');
SELECT * FROM hdfs_ls('hdfs://namenode:8020/data/**/*.parquet');

-- Metadata for a single path, as one row.
SELECT * FROM hdfs_stat('hdfs://namenode:8020/data');

-- Existence check (scalar boolean).
SELECT hdfs_exists('hdfs://namenode:8020/data/events');
```

`hdfs_ls` and `hdfs_stat` return the same columns: `path`, `name`, `type`
(`'file'`/`'directory'`), `size`, `owner`, `group`, `permissions`
(`rwxr-xr-x`), `mode`, `replication`, `block_size`, `last_modified`,
`last_accessed`. Row order is not guaranteed — add `ORDER BY path` if it
matters.

Behavior notes:

- **Globs** use DuckDB's syntax (`*`, `?`, `[abc]`, `**` for any depth) plus
  Hadoop-style `{a,b}` alternation, and also work in `read_parquet` /
  `read_csv` / `glob()`. A path with wildcards makes `hdfs_ls` return the
  matched entries themselves (like `ls -d`; no matches → empty result); a
  plain directory path lists its children.
- **Hidden entries** (names starting with `_` or `.`, e.g. `_SUCCESS`) are
  skipped unless the path names them explicitly (`'/data/_*'`) or you set
  `include_hidden := true` / `SET hdfs_include_hidden = true`.
- **Permission errors** fail the query; `skip_permission_errors := true` (or
  the `SET` variant) prunes unreadable subtrees instead. The root path itself
  still fails.
- **Performance**: globs expand client-side, one listing RPC per candidate
  directory (up to `hdfs_list_parallelism` in parallel), so anchor patterns
  deep — `/data/2024/*` starts listing at `/data/2024`, while `**` walks the
  whole subtree.

## URLs

Paths are `hdfs://host:port/path`; `host:port` selects the NameNode. The
scheme-only form `hdfs:///path` uses `fs.defaultFS` from your Hadoop config.

## Configuration

### Extension settings

| Setting | Default | Description |
|---|---|---|
| `hdfs_list_parallelism` | `16` | Max concurrent directory-listing RPCs for recursive listings and globs. Raise cautiously on shared clusters — it multiplies NameNode load. |
| `hdfs_include_hidden` | `false` | Include hidden (`_`/`.`) entries in listings, globs and scans. Per-call override: `include_hidden := …`. |
| `hdfs_skip_permission_errors` | `false` | Skip unreadable subtrees instead of failing the query. Per-call override: `skip_permission_errors := …`. |

```sql
SET hdfs_list_parallelism = 64;
```

### Connection configuration

`hdfs-native` resolves connection config from the standard Hadoop
environment, exactly as the Hadoop CLI tools do:

- **Cluster config** (`core-site.xml` / `hdfs-site.xml`): `HADOOP_CONF_DIR`,
  or failing that `HADOOP_HOME/etc/hadoop`.
- **Effective user**: `HADOOP_USER_NAME` (or `HADOOP_PROXY_USER`), else the
  current OS account; on a Kerberos-secured cluster the identity comes from
  the ticket/keytab.

```sh
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_USER_NAME=analytics   # only on non-secure clusters
```

In an embedded context (e.g. Python), set these before the first HDFS access —
the client is created lazily.

## Building

Requires a Rust toolchain (for the bridge) plus the usual DuckDB extension
build prerequisites; dependencies are managed with vcpkg.

```sh
make            # builds hdfs-bridge (cargo) + the extension + duckdb
```

Artifacts:

- `build/release/duckdb` — shell with the extension statically linked.
- `build/release/extension/hdfs/hdfs.duckdb_extension` — loadable extension.
- `build/release/test/unittest` — DuckDB test runner.

## Testing

Tests that don't need a cluster run with:

```sh
make test
```

The HDFS integration tests (`test/sql/hdfs.test`) run against a real
single-node HDFS in Docker, gated behind `require-env HDFS_TEST_RUNNING`. To
run them end-to-end (requires Docker):

```sh
make                              # ensure duckdb + extension are built
test/scripts/run_hdfs_tests.sh    # builds image, starts HDFS, runs tests, tears down
```

Or manage the cluster manually:

```sh
test/scripts/hdfs_up.sh           # build image, start HDFS, seed fixtures under /test
HDFS_TEST_RUNNING=1 HADOOP_CONF_DIR="$(pwd)/test/hdfs-conf" HADOOP_USER_NAME=hadoop \
    build/release/test/unittest test/sql/hdfs.test
test/scripts/hdfs_down.sh
```

See `test/docker/` for the cluster definition. The one detail that makes HDFS
reachable from the host is advertising the DataNode as `localhost` and setting
`dfs.client.use.datanode.hostname=true` on the client
(`test/hdfs-conf/hdfs-site.xml`).

## Layout

- `src/` — DuckDB extension (C++): `hdfs_extension.cpp` registers the filesystem
  and settings; `hdfs_filesystem.cpp` implements the `FileSystem`;
  `hdfs_functions.cpp` implements the metadata SQL functions.
- `hdfs-bridge/` — Rust staticlib wrapping `hdfs-native` behind a C ABI; its
  header (`hdfs-bridge/include/hdfs_bridge.h`) is generated from the Rust
  definitions by cbindgen on `cargo build` (do not edit).
- `test/` — SQL tests, Docker cluster, and helper scripts.
