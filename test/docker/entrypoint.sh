#!/usr/bin/env bash
# Format (once) and run a single-node HDFS: NameNode + DataNode in one container.
set -euo pipefail

export HADOOP_CONF_DIR=/etc/hadoop-conf

NAME_DIR=/tmp/hdfs/name
mkdir -p /tmp/hdfs/name /tmp/hdfs/data

if [ ! -d "${NAME_DIR}/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force -nonInteractive
fi

echo "Starting NameNode..."
hdfs namenode &
NN_PID=$!

echo "Starting DataNode..."
hdfs datanode &
DN_PID=$!

# Forward termination to the daemons so `docker compose down` is clean.
trap 'kill "${NN_PID}" "${DN_PID}" 2>/dev/null || true' TERM INT

# Stay in the foreground until one of the daemons exits. (The image's bash
# predates `wait -n`, so poll liveness instead.)
while kill -0 "${NN_PID}" 2>/dev/null && kill -0 "${DN_PID}" 2>/dev/null; do
    sleep 5
done

echo "A HDFS daemon exited; shutting down."
