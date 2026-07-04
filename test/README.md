# Tests

[SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) and the Docker
cluster used to exercise the `hdfs` extension end-to-end.

## Layout

- `sql/hdfs.test` — integration tests run against a real HDFS. Gated behind
  `require-env HDFS_TEST_RUNNING`, so `make test` skips them unless a cluster is
  up.
- `docker/` — a single-node HDFS (NameNode + DataNode) image + compose file.
- `hdfs-conf/` — host-side Hadoop client config (`core-site.xml` /
  `hdfs-site.xml`) for talking to that cluster. Pointed at via `HADOOP_CONF_DIR`.
- `scripts/` — `hdfs_up.sh`, `hdfs_down.sh`, and `run_hdfs_tests.sh`.

## Running

Without a cluster (unit/SQL tests only):

```bash
make test
```

Full integration run (requires Docker; needs `make` first so the binaries
exist):

```bash
test/scripts/run_hdfs_tests.sh
```

This builds the HDFS image, starts the cluster, seeds fixtures under `/test`,
runs `test/sql/hdfs.test` with `HDFS_TEST_RUNNING=1` and `HADOOP_CONF_DIR`
pointing at `test/hdfs-conf`, then tears everything down.

To keep the cluster up between runs, use `hdfs_up.sh` / `hdfs_down.sh` directly
(see the project README).
