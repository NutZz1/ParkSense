#!/usr/bin/env bash
# End-to-end batch rerun: HDFS upload -> Scala RDD -> Spark concepts -> Spark SQL analytics -> features -> EDA ->
# train -> evaluate -> perf -> publish metrics -> figures -> report. Stops on the first error.
# Usage: scripts/run_batch.sh [--skip-perf] [--skip-horizons] [--from "6 train"]   (--from resumes at that step)
set -euo pipefail
cd "$(dirname "$0")/.." && source scripts/env.sh
SKIP_PERF=""; EVAL_ARGS=""; FROM=""
while [ $# -gt 0 ]; do case $1 in --skip-perf) SKIP_PERF=1 ;; --skip-horizons) EVAL_ARGS="--skip-horizons" ;; --from) FROM=$2; shift ;; esac; shift; done
LOG=outputs/logs/run_batch.log; mkdir -p outputs/logs
T0=$(date +%s); declare -a DONE=()
step() { local n=$1; shift
  if [ -n "$FROM" ] && [ "$n" != "$FROM" ] && [ "$n" != "services" ]; then echo "skip [$n] (resuming at $FROM)"; return; fi
  [ "$n" = "$FROM" ] && FROM=""
  local t=$(date +%s); echo -e "\n>>> [$n] $*" | tee -a $LOG
  if ! "$@" >> $LOG 2>&1; then echo "FAILED at: $n (see $LOG)"; tail -30 $LOG; exit 1; fi
  DONE+=("$(printf '%-30s %6ss' "$n" $(( $(date +%s) - t )))"); }
step "services" scripts/02_start_services.sh
step "hdfs upload" scripts/03_hdfs_layout.sh --archive
step "build jar" scripts/build_scala.sh
[ -z "$FROM" ] && { mv -f outputs/metrics/timings.csv outputs/logs/timings_previous.csv 2>/dev/null || true; }
step "2 RddPipeline" scripts/run_scala.sh RddPipeline
step "3 SparkConcepts" scripts/run_scala.sh SparkConcepts "local[4,3]"
step "4 analytics (Spark SQL)" python pyspark_jobs/analytics.py
step "6 features" python pyspark_jobs/features.py
step "5 EDA" python pyspark_jobs/eda.py
step "6 train" python pyspark_jobs/train.py
step "6 evaluate" python pyspark_jobs/evaluate.py $EVAL_ARGS
[ -z "$SKIP_PERF" ] && step "7 perf" python pyspark_jobs/perf_system.py
step "9 publish metrics" python pyspark_jobs/publish_metrics.py
step "10 figures" python viz/make_figures.py
step "10 report" python viz/fill_report.py
echo -e "\n=== run_batch summary ==="; for d in "${DONE[@]}"; do echo "  $d"; done
echo "  total $(( $(date +%s) - T0 ))s · log $LOG"
python -c "import pandas as pd; print(pd.read_csv('outputs/metrics/model_comparison.csv')[['model','accuracy','precision','recall','f1']].round(4).to_string(index=False))"
