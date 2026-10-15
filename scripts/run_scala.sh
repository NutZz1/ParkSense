#!/usr/bin/env bash
# Run a Scala job. Usage: scripts/run_scala.sh <RddPipeline|SparkConcepts|StreamingJob> [master] [--stream]
# HBase client jars (shaded) + hbase-site.xml are added for jobs that write to HBase.
set -euo pipefail
cd "$(dirname "$0")/.." && source scripts/env.sh
CLASS=$1; MASTER=${2:-local[*]}
JAR=spark-scala/target/scala-2.12/parksense_2.12-0.1.jar
[ -f "$JAR" ] || scripts/build_scala.sh
HBJARS=$(hbase mapredcp 2>/dev/null | tr ':' '\n' | grep -E "hbase-shaded-mapreduce|opentelemetry|htrace|audience-annotations" | paste -sd,)
LOGF=outputs/logs/${CLASS}_console.txt; [ "${3:-}" = "--stream" ] && LOGF=/dev/null
spark-submit --class parksense.$CLASS --master "$MASTER" --driver-memory $([ "$CLASS" = StreamingJob ] && echo 768m || echo 3500m) \
  --jars "$HBJARS" --files $HBASE_HOME/conf/hbase-site.xml --driver-class-path $HBASE_HOME/conf \
  --conf spark.ui.port=$([ "$CLASS" = StreamingJob ] && echo 4050 || echo 4040) \
  --driver-java-options "-Dlog4j.configurationFile=file://$PARKSENSE_HOME/config/log4j2.properties" \
  "$JAR" config/config.yaml 2>&1 | tee $LOGF
