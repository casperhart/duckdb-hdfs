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
- Globbing (`hdfs://host:port/dir/*.parquet`, `.../dir/**/*.parquet`),
  directory listing, `FileExists`, size/last-modified metadata.
- Directory and file management: create/remove directories, remove files, rename
  (move).
- Concurrent positional reads (DuckDB's parallel Parquet reader) on a single
  handle.
- Reading and writing data in HDFS encryption zones (Transparent Data
  Encryption): the KMS is contacted over TLS to unwrap the key, and data is
  decrypted/encrypted client-side. Requires the Hadoop key provider to be
  configured (`hadoop.security.key.provider.path` / `dfs.encryption.key.provider.uri`).

## Querying HDFS metadata

Besides reading and writing file *contents*, the extension exposes HDFS
*metadata* as SQL. These return full `FileStatus` metadata — type, size, owner,
group, permissions, replication, block size, and modification/access times — and,
unlike DuckDB's built-in `glob()`, they include directories.

```sql
-- List a directory's immediate children (directories included).
SELECT name, type, size, owner, permissions
FROM hdfs_ls('hdfs://namenode:8020/data');

-- Walk the whole subtree ('**' matches zero or more path levels).
SELECT * FROM hdfs_ls('hdfs://namenode:8020/data/**');

-- Glob patterns return the matched entries themselves (directories kept).
SELECT * FROM hdfs_ls('hdfs://namenode:8020/data/year=*/month=*/*.parquet');
SELECT * FROM hdfs_ls('hdfs://namenode:8020/data/**/*.parquet');
SELECT * FROM hdfs_ls('hdfs://namenode:8020/logs/2026-0{1,2,3}/*.gz');

-- Filter with SQL rather than a glob when one directory is enough (one RPC).
SELECT * FROM hdfs_ls('hdfs://namenode:8020/data')
WHERE starts_with(name, 'temp_');

-- Metadata for one path (file or directory), as a single row.
SELECT * FROM hdfs_stat('hdfs://namenode:8020/data');

-- Existence check (scalar boolean).
SELECT hdfs_exists('hdfs://namenode:8020/data/events');
```

Columns for `hdfs_ls` / `hdfs_glob` / `hdfs_stat` (identical, so results
compose/`UNION`): `path` (full `hdfs://` URL), `name` (basename), `type`
(`'file'`/`'directory'`), `size`, `owner`, `group`, `permissions` (symbolic
`rwxr-xr-x`), `mode` (raw permission bits), `replication`, `block_size` (both
`NULL` for directories), `last_modified`, `last_accessed`.

**Glob syntax and semantics.** Patterns follow DuckDB's globber — `*` (within
one path level), `?`, `[abc]` / `[a-b]` / `[!abc]` character classes, `\`
escapes, and `**` as a whole component matching zero or more levels (at most
one per pattern) — extended with Hadoop-style `{a,b}` alternation, which may
span `/` (`/data/{2024/12,2025/01}/*`). A path containing any of `* ? [ {`
switches `hdfs_ls` to glob semantics: matched entries come back *as rows
themselves* (like `ls -d`, or `hdfs_glob`), files and directories both, and a
pattern matching nothing returns an empty result rather than an error. A path
without wildcards keeps plain `ls` semantics: a directory lists its children.
Unlike the shell, `*` also matches dot-prefixed names, and rows arrive in
completion order — use `ORDER BY path` when order matters. `hdfs_glob` is the
same walk with pattern semantics always on (a literal path returns its own
entry instead of listing children). The same globber backs `read_parquet` /
`read_csv` / `glob()` over `hdfs://`, so `**` works there too.

**Keeping it fast.** HDFS has no server-side glob RPC, so patterns expand
client-side by walking the tree with one `getListing` per directory that can
still match — cost scales with the pattern's fan-out, and up to
`hdfs_list_parallelism` listings run concurrently. Nothing above the pattern's
literal prefix is listed (`/data/2024/*` starts at `/data/2024`), so anchor
patterns as deep as you can; `**` necessarily walks the whole subtree below
its anchor. For single-directory filtering, `hdfs_ls(dir) WHERE …` (one RPC)
beats a glob. Requesting the full metadata columns adds **no** extra RPCs —
HDFS already returns them in the same listing call.

These functions are read-only (metadata queries); the extension does not expose
SQL functions that mutate the filesystem.

## URL / authority handling

Paths are `hdfs://<host>:<port>/<path>`. The `host:port` authority selects the
NameNode. A scheme-only default-FS form (`hdfs:///path`) uses the NameNode from
your Hadoop config (`fs.defaultFS`).

## Configuration

### Extension settings

| Setting | Default | Description |
|---|---|---|
| `hdfs_list_parallelism` | `16` | Maximum number of concurrent directory-listing RPCs used by listings and globs that walk more than one directory (`hdfs_ls('/path/**')`, `read_parquet('.../**/*.parquet')`, …). Walks stream: rows are produced while the tree walk is still in flight, and multi-directory results arrive in completion order (no ordering guarantee — use `ORDER BY` if order matters). The walk fans out per directory, so flat directories see no speedup. Set to `1` to list one directory at a time; raise it cautiously on shared clusters, since it multiplies NameNode request load. |

```sql
SET hdfs_list_parallelism = 64;
```

### Connection configuration

Connection config is resolved by `hdfs-native` from the standard Hadoop
environment, exactly as the Hadoop CLI tools do:

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
