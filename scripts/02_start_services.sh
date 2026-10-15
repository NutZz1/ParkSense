#!/usr/bin/env bash
# Starts whatever is down: HDFS, HBase (with its ZooKeeper), HBase Thrift (9090) and REST (8085). Verifies each.
cd "$(dirname "$0")/.." && source scripts/env.sh
up() { jps | grep -qw "$1"; }
up NameNode || start-dfs.sh
for i in $(seq 1 30); do hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF && break; sleep 2; done
up HMaster || start-hbase.sh
for i in $(seq 1 30); do echo status | hbase shell -n 2>/dev/null | grep -q "1 active master" && break; sleep 3; done
(echo > /dev/tcp/127.0.0.1/9090) 2>/dev/null || hbase-daemon.sh start thrift
curl -s -m 3 localhost:8085/version/cluster >/dev/null || hbase-daemon.sh start rest -p 8085
for i in $(seq 1 30); do curl -s -m 3 localhost:8085/version/cluster >/dev/null && break; sleep 2; done
echo "jps: $(jps | grep -v Jps | awk '{print $2}' | sort | tr '\n' ' ')"
echo "HDFS: $(hdfs dfsadmin -safemode get 2>/dev/null)"
echo "HBase: $(echo status | hbase shell -n 2>/dev/null | grep 'active master')"
echo "Thrift 9090: $((echo > /dev/tcp/127.0.0.1/9090) 2>/dev/null && echo up || echo DOWN)"
echo "REST 8085: $(curl -s -m 3 -H 'Accept: application/json' localhost:8085/version/cluster || echo DOWN)"
