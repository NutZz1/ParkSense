#!/usr/bin/env bash
# RDD demo for screenshots (read-only, ~1 min): scripts/demo_rdd.sh [YYYY-MM-DD | all]   (default 2026-09-23)
# Prints 1 RDD creation, 2 transformations, 3 actions, 4 key/value operations; code: spark-scala/demo/rdd_demo.scala
# "all" reads the full 30 days and needs ~3 GB free RAM (stop the live layer first: scripts/stop_live.sh).
set -euo pipefail
cd "$(dirname "$0")/.." && source scripts/env.sh
export DEMO_DAY=${1:-2026-09-23}
MEM=$([ "$DEMO_DAY" = all ] && echo 3g || echo 1g)
# spark.locality.wait=0: in local mode a partly cached RDD can otherwise leave tasks waiting for locality
spark-shell --master "local[4]" --driver-memory $MEM --conf spark.ui.showConsoleProgress=false \
  --conf spark.locality.wait=0 -i spark-scala/demo/rdd_demo.scala < /dev/null 2>&1 \
  | grep --line-buffered -v -E "^(Setting default log level|To adjust logging level|Spark context|Spark session|.*WARN (NativeCodeLoader|Utils))" | grep --line-buffered -v -E "^\s+$|^Terminated$"
