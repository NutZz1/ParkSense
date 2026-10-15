#!/usr/bin/env bash
# Spark History Server over outputs/spark-events (DAG screenshots) -> http://localhost:18080
cd "$(dirname "$0")/.." && source scripts/env.sh
mkdir -p outputs/spark-events
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://$PARKSENSE_HOME/outputs/spark-events"
$SPARK_HOME/sbin/stop-history-server.sh >/dev/null 2>&1
$SPARK_HOME/sbin/start-history-server.sh && echo "Spark History Server: http://localhost:18080"
