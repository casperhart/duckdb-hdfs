# DuckDB HDFS extension

`hdfs` adds a native HDFS filesystem to DuckDB, letting you read and write files
on Hadoop HDFS directly via `hdfs://` URLs — no JVM, no `libhdfs`. It is backed
by the pure-Rust [`hdfs-native`](https://github.com/Kimahriman/hdfs-native)
client through a thin C FFI bridge (`hdfs-bridge/`).

```sql
-- Read
SELECT * FROM 'hdfs://namenode:8020/data/events/*.parquet';
SELECT * FROM read_csv('hdfs://namenode:8020/data/users.csv');

-- Write
COPY (SELECT * FROM big_table) TO 'hdfs://namenode:8020/out/table.parquet' (FORMAT parquet);
```

## What works

- Reading any file format DuckDB supports (Parquet, CSV, JSON, …) over `hdfs://`.
- Writing via `COPY ... TO 'hdfs://...'` (HDFS is append-only; random writes are
  not supported).
- Globbing (`hdfs://host:port/dir/*.parquet`), directory listing, `FileExists`,
  size/last-modified metadata.
- Directory and file management: create/remove directories, remove files, rename
  (move).
- Concurrent positional reads (DuckDB's parallel Parquet reader) on a single
  handle.

## URL / authority handling

Paths are `hdfs://<host>:<port>/<path>`. The `host:port` authority selects the
NameNode. A scheme-only default-FS form (`hdfs:///path`) uses the NameNode from
your Hadoop config (`fs.defaultFS`).

## Configuration

There are no extension-specific settings — connection config is resolved by
`hdfs-native` from the standard Hadoop environment, exactly as the Hadoop CLI
tools do:

- **Cluster config** (`core-site.xml` / `hdfs-site.xml`): `HADOOP_CONF_DIR`, or
  failing that `HADOOP_HOME/etc/hadoop`.
- **Effective user**: `HADOOP_USER_NAME` (or `HADOOP_PROXY_USER`), else the
  current OS account; on a Kerberos-secured cluster the identity comes from the
  ticket/keytab.

```sh
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_USER_NAME=analytics   # only on non-secure clusters
```

In an embedded context (e.g. Python), set these in the environment before the
first HDFS access — the client is created lazily on first use.

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

Unit/SQL tests that don't need a cluster run with:

```sh
make test
```

The HDFS integration tests (`test/sql/hdfs.test`) run against a real single-node
HDFS in Docker. They are gated behind `require-env HDFS_TEST_RUNNING`, so they
are skipped by `make test` unless a cluster is up.

To run them end-to-end (requires Docker):

```sh
make                              # ensure duckdb + extension are built
test/scripts/run_hdfs_tests.sh    # builds image, starts HDFS, runs tests, tears down
```

Or manage the cluster manually:

```sh
test/scripts/hdfs_up.sh           # build image, start HDFS, seed fixtures under /test
HDFS_TEST_RUNNING=1 HADOOP_CONF_DIR="$(pwd)/test/hdfs-conf" \
    build/release/test/unittest test/sql/hdfs.test
test/scripts/hdfs_down.sh
```

See `test/docker/` for the cluster definition. The one detail that makes HDFS
reachable from the host (outside Docker's network) is advertising the DataNode
as `localhost` and setting `dfs.client.use.datanode.hostname=true` on the client
(`test/hdfs-conf/hdfs-site.xml`).

## Layout

- `src/` — DuckDB extension (C++): `hdfs_extension.cpp` registers the filesystem
  and settings; `hdfs_filesystem.cpp` implements the `FileSystem`.
- `src/include/hdfs_bridge.h` — C ABI exposed by the Rust bridge.
- `hdfs-bridge/` — Rust staticlib wrapping `hdfs-native` behind that C ABI.
- `test/` — SQL tests, Docker cluster, and helper scripts.
