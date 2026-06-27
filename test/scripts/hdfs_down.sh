#!/usr/bin/env bash
# Stop and remove the single-node HDFS test cluster.
#
# Usage: test/scripts/hdfs_down.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}/test/docker"

echo "==> Stopping HDFS container..."
docker compose down -v
