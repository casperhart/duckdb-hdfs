#!/usr/bin/env bash
# Start the single-node HDFS test cluster and seed it with fixtures.
#
# Usage: test/scripts/hdfs_up.sh
#
# After this succeeds, run the integration tests with:
#   HDFS_TEST_RUNNING=1 HADOOP_CONF_DIR="$(pwd)/test/hdfs-conf" \
#       build/release/test/unittest test/sql/hdfs.test
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_DIR="${REPO_ROOT}/test/docker"
CONTAINER="duckdb-hdfs-test"
DUCKDB_BIN="${REPO_ROOT}/build/release/duckdb"

cd "${COMPOSE_DIR}"

echo "==> Building and starting HDFS container (clean slate)..."
# Always recreate so we get a freshly formatted, freshly started cluster rather
# than reusing a stale (possibly half-dead) container.
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --build --force-recreate

echo "==> Waiting for HDFS to leave safe mode and register a live DataNode..."
# Generous: the image is amd64 and may run emulated on arm64 hosts.
deadline=$(( $(date +%s) + 300 ))
# NB: capture the report into a variable rather than piping into `grep -q`.
# Under `set -o pipefail`, `grep -q` closes the pipe on first match and the
# still-writing `hdfs dfsadmin` dies with SIGPIPE (141), which would mark the
# whole pipeline as failed and loop forever despite the match.
while true; do
    report="$(docker exec "${CONTAINER}" hdfs dfsadmin -report 2>/dev/null || true)"
    if printf '%s' "${report}" | grep -q "Live datanodes (1)"; then
        break
    fi
    if [ "$(date +%s)" -ge "${deadline}" ]; then
        echo "ERROR: HDFS did not become ready in time. Recent logs:" >&2
        docker compose logs --tail=50 hdfs >&2 || true
        exit 1
    fi
    sleep 3
done
# Leave safe mode explicitly in case it lingers on a fresh format.
docker exec "${CONTAINER}" hdfs dfsadmin -safemode leave >/dev/null 2>&1 || true

echo "==> Seeding fixtures under /test ..."
docker exec "${CONTAINER}" hdfs dfs -mkdir -p /test /test/nested

# CSV fixture, created directly in the container.
docker exec "${CONTAINER}" bash -lc \
    "printf 'id,name\n1,alice\n2,bob\n3,carol\n' > /tmp/data.csv && hdfs dfs -put -f /tmp/data.csv /test/data.csv"

# Parquet fixture, generated on the host with the built duckdb, then uploaded.
if [ -x "${DUCKDB_BIN}" ]; then
    "${DUCKDB_BIN}" -c \
        "COPY (SELECT range AS id, 'v' || range AS name FROM range(5)) TO '/tmp/hdfs_fixture.parquet' (FORMAT parquet);"
    docker cp /tmp/hdfs_fixture.parquet "${CONTAINER}:/tmp/fixture.parquet"
    docker exec "${CONTAINER}" hdfs dfs -put -f /tmp/fixture.parquet /test/data.parquet
    docker exec "${CONTAINER}" hdfs dfs -put -f /tmp/fixture.parquet /test/nested/more.parquet
else
    echo "WARNING: ${DUCKDB_BIN} not found; skipping parquet fixture. Run 'make' first." >&2
fi

echo "==> HDFS is ready. Fixtures:"
docker exec "${CONTAINER}" hdfs dfs -ls -R /test
