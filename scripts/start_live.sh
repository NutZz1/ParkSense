#!/usr/bin/env bash
# Live layer with nohup + pid files: poller, StreamingJob, score_live loop, verify_live loop.
# Usage: scripts/start_live.sh [poller|stream|score|verify ...]   (default: all available)
set -uo pipefail
cd "$(dirname "$0")/.." && source scripts/env.sh
LOG=$PARKSENSE_HOME/outputs/logs; mkdir -p $LOG data/stream/incoming
JAR=$(ls spark-scala/target/scala-2.12/parksense_2.12-*.jar 2>/dev/null | head -1)
start() {
  local name=$1; shift
  if [ -f $LOG/$name.pid ] && kill -0 "$(cat $LOG/$name.pid)" 2>/dev/null; then echo "$name already running (pid $(cat $LOG/$name.pid))"; return; fi
  nohup "$@" >> $LOG/$name.out 2>&1 &
  echo $! > $LOG/$name.pid; echo "started $name (pid $!)"
}
for c in ${*:-poller stream score verify}; do case $c in
  poller) start poller python ingest/live_poller.py ;;
  stream) if [ -n "$JAR" ]; then start stream scripts/run_scala.sh StreamingJob "local[2]" --stream; else echo "skip stream: build the jar first"; fi ;;
  score)  if [ -d models/best_model ]; then start score python pyspark_jobs/score_live.py --loop ${SCORE_SECONDS:-300}; else echo "skip score: no models/best_model"; fi ;;
  verify) if [ -d models/best_model ]; then start verify python pyspark_jobs/verify_live.py --loop ${VERIFY_SECONDS:-300}; else echo "skip verify: no models/best_model"; fi ;;
esac; done
