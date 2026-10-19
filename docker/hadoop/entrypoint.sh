#!/usr/bin/env bash
# Starts one Hadoop role per container, chosen by the first argument.
set -e
ROLE="${1:-edge}"

case "$ROLE" in
  namenode)
    if [ ! -d /data/hdfs/namenode/current ]; then
      echo "Formatting NameNode (first start)..."
      hdfs namenode -format -clusterid radar -force -nonInteractive
    fi
    exec hdfs namenode
    ;;
  datanode)
    exec hdfs datanode
    ;;
  resourcemanager)
    exec yarn resourcemanager
    ;;
  nodemanager)
    exec yarn nodemanager
    ;;
  historyserver)
    exec mapred historyserver
    ;;
  edge)
    echo "Edge node ready. Use: docker compose exec edge bash"
    exec sleep infinity
    ;;
  *)
    exec "$@"
    ;;
esac
