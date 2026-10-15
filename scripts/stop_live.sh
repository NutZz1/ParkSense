#!/usr/bin/env bash
# Stops components started by start_live.sh (and the UI server). Usage: scripts/stop_live.sh [names...]
cd "$(dirname "$0")/.."
for name in ${*:-ui verify score stream poller}; do
  f=outputs/logs/$name.pid
  if [ -f $f ] && kill -0 "$(cat $f)" 2>/dev/null; then pkill -TERM -P "$(cat $f)" 2>/dev/null; kill "$(cat $f)" && echo "stopped $name"; else echo "$name not running"; fi
  rm -f $f
done
