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

Files can be opened for reading or writing, but not both. Positioned writes
are accepted only at the writer's current position and continue sequentially;
random overwrites are rejected. Seeking a writer to another position, including
resetting it after data has been written, is unsupported. Reader seek and reset
remain supported.

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

Without a matching HDFS Secret, the extension retains the environment-based
configuration supported by `hdfs-native`, including `hdfs:///path` with
`fs.defaultFS`. Embedded calls without a `FileOpener` use the same mode.
Set process configuration before the first HDFS access:

```sh
export HADOOP_CONF_DIR=/etc/hadoop/conf
export KRB5_CONFIG=/etc/krb5.conf
# For a non-secure cluster only:
export HADOOP_USER_NAME=analytics
```

`HADOOP_CONF_DIR` (or `HADOOP_HOME/etc/hadoop`) supplies `core-site.xml` and
`hdfs-site.xml`. Existing Kerberos credential-cache/keytab environment support
continues to come from `hdfs-native`.

### SQL Secrets and explicit Kerberos credentials

The extension supports DuckDB's standard temporary and persistent Secrets.
A temporary Secret can select credentials for a cluster or directory:

```sql
LOAD hdfs;
CREATE TEMPORARY SECRET analytics_hdfs (
    TYPE hdfs,
    FS_NAME 'hdfs://namenode:8020',
    SCOPE 'hdfs://namenode:8020/analytics',
    PRINCIPAL 'analytics@EXAMPLE.COM',
    KEYTAB '/run/secrets/analytics.keytab',
    CONF MAP {'dfs.client.use.datanode.hostname': 'true'}
);
SELECT * FROM read_parquet('hdfs://namenode:8020/analytics/*.parquet');
```

- `FS_NAME` is required and contains only `hdfs://authority`, without a path,
  query or fragment. `SCOPE` defaults to that cluster's root. Directory scopes
  end in `/`, so `/analytics` does not match `/analytics2`; the most specific
  matching Secret is selected.
- `USER` optionally sets the effective user. On secure clusters, acting as a
  different effective user still requires the server's proxy-user authorization.
- `PRINCIPAL` selects an explicit Kerberos identity; without `KEYTAB`, the native
  client looks for existing credentials for that principal. `KEYTAB` requires
  a principal and an absolute local path readable by the DuckDB process. Mount
  that file into every process/container that accesses HDFS; the Secret stores
  its path, not its contents.
- `CONF` overrides keys from Hadoop XML configuration. A principal implicitly
  sets `hadoop.security.authentication=kerberos`; a conflicting explicit value
  is rejected. This selects client authentication, without changing the native
  client's server-negotiation policy.
- `KRB5_CONFIG` remains a process-startup setting. There is no per-Secret
  `KRB5_CONF` option, and query execution does not modify the environment.
- NULL values, embedded NUL bytes, empty required credential values, relative
  keytab paths and scopes outside `FS_NAME` are rejected. KEYTAB and CONF are
  redacted in the normal Secret display. Persistent Secrets use DuckDB's
  existing local storage and its security model.

A matched Secret's errors propagate: the extension never retries authentication
with an environment identity after an explicit configuration fails. Missing
Secret matches retain the environment mode; `SCOPE` chooses credentials and is
not an authorization boundary. HDFS permissions remain authoritative.

Clients are reused by configuration values (including the environment/Secret
mode), not by Secret names. Different users, principals, keytab paths or CONF
values get separate clients. Replacing a Secret changes new operations; open
handles and directory streams retain their original configuration, including
on reconnect. Overwriting a keytab or XML file at the same path does not evict
existing clients: recreate the database instance to force configuration reload.

This extension does not publish a shared read-cache identity or add a reader
pool. Supplying Hadoop configuration files is not treated as proof that a
connection is safe to share through a persistent data cache.

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

On Linux, the file-operation boundary tests exercise the real filesystem with
the remote bridge calls replaced, so no HDFS service is required. With
`BUILD_UNITTESTS=ON`, build and run them in an external build directory:

```sh
cmake --build /absolute/path/to/build --target hdfs_open_mode_check hdfs_secret_check
```

The normal Linux `test_release_internal`, `test_debug_internal` and
`test_reldebug_internal` targets run both C++ checks before the existing SQL
runner, and propagate failures. These linker-wrapper tests are Linux-only;
the no-service Secret SQL tests also run on other platforms.

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

### Optional Kerberos integration

`test/sql/hdfs_kerberos.test` is gated by `HDFS_KERBEROS_TEST_RUNNING` and
requires an already-running secure cluster. It does not start services or
create principals. Supply `HDFS_KERBEROS_FS_NAME`, `HDFS_KERBEROS_PRINCIPAL_A/B`,
`HDFS_KERBEROS_KEYTAB_A/B`, and full-URL `HDFS_KERBEROS_ROOT_A/B`. The two roots
must permit their respective identity to read/write and deny the other identity
traversal/read access. Set a unique `HDFS_KERBEROS_RUN_ID`, process `KRB5_CONFIG`
and the Hadoop configuration directory. `HDFS_KERBEROS_ENV_FILE` must be an
existing file readable through the original environment credentials.

The test covers concurrent reads using two identities, COPY, listing, stat,
permission rejection after Secret replacement, bad keytab/principal, and the
old environment path. It leaves two uniquely named Parquet files under those
roots for inspection; the cluster operator can remove those exact files later.
The no-service C++ suite separately checks credential routing for delete and
rename. A passing no-service suite does not validate real Kerberos negotiation
or HDFS permissions.

## Layout

- `src/` — DuckDB extension (C++): `hdfs_extension.cpp` registers the filesystem
  and settings; `hdfs_filesystem.cpp` implements the `FileSystem`;
  `hdfs_functions.cpp` implements the metadata SQL functions.
- `hdfs-bridge/` — Rust staticlib wrapping `hdfs-native` behind a C ABI; its
  header (`hdfs-bridge/include/hdfs_bridge.h`) is generated from the Rust
  definitions by cbindgen on `cargo build` (do not edit).
- `test/` — SQL tests, Docker cluster, and helper scripts.
