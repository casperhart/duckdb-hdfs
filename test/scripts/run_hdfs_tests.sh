#!/usr/bin/env bash
# One-shot: bring HDFS up, run the integration SQL tests against it, tear down.
#
# Usage: test/scripts/run_hdfs_tests.sh
#
# Requires a prior `make` so build/release/test/unittest exists.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UNITTEST="${REPO_ROOT}/build/release/test/unittest"

if [ ! -x "${UNITTEST}" ]; then
    echo "ERROR: ${UNITTEST} not found. Run 'make' first." >&2
    exit 1
fi

cleanup() {
    "${REPO_ROOT}/test/scripts/hdfs_down.sh" || true
}
trap cleanup EXIT

"${REPO_ROOT}/test/scripts/hdfs_up.sh"

echo "==> Running HDFS integration tests..."
# unittest matches its registered test names by repo-relative path, so run from
# the repo root and pass the relative path.
cd "${REPO_ROOT}"
# HADOOP_CONF_DIR provides client config; HADOOP_USER_NAME makes us the owner of
# the hadoop-owned /test dir so the write tests are permitted. Both are the
# standard Hadoop env vars that hdfs-native reads natively.
HDFS_TEST_RUNNING=1 \
HADOOP_CONF_DIR="${REPO_ROOT}/test/hdfs-conf" \
HADOOP_USER_NAME=hadoop \
    "${UNITTEST}" test/sql/hdfs.test

echo "==> Running HDFS permission tests (as a non-superuser)..."
# A separate run as a plain user: the superuser above bypasses HDFS permission
# checks, so it can never hit the AccessControlExceptions these tests need.
HDFS_TEST_RUNNING=1 \
HADOOP_CONF_DIR="${REPO_ROOT}/test/hdfs-conf" \
HADOOP_USER_NAME=guest \
    "${UNITTEST}" test/sql/hdfs_permissions.test
