# Single-node HDFS for integration testing the DuckDB hdfs extension.
FROM apache/hadoop:3.4.1

USER root

ENV HADOOP_CONF_DIR=/etc/hadoop-conf

COPY core-site.xml hdfs-site.xml /etc/hadoop-conf/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

# The base image runs as the unprivileged "hadoop" user. The data/name dirs are
# created by the entrypoint (as this user) so the DataNode owns them and can set
# its required 0700 permission.
USER hadoop

ENTRYPOINT ["/entrypoint.sh"]
