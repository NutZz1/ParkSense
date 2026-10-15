#!/usr/bin/env bash
# Stops components started by start_live.sh (and the UI server). Usage: scripts/stop_live.sh [names...]
cd "$(dirname "$0")/.."
for name in ${*:-ui verify score stream poller}; do
  f=outputs/logs/$name.pid
  if [ -f $f ] && kill -0 "$(cat $f)" 2>/dev/null; then
    # kill the whole tree: spark-submit/PySpark JVMs are grandchildren and survive a parent-only kill
    tree() { local c; for c in $(pgrep -P "$1"); do tree "$c"; echo "$c"; done; }
    pids="$(tree "$(cat $f)") $(cat $f)"
    kill $pids 2>/dev/null && echo "stopped $name ($(echo $pids | wc -w) processes)"
  else echo "$name not running"; fi
  rm -f $f
done
