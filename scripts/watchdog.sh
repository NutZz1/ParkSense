#!/usr/bin/env bash
# Watchdog: every 60 s, restart whatever has failed — HDFS, HBase (master, region server, Thrift, REST) and the
# live components (poller, stream, score, verify). Actions are logged to outputs/logs/watchdog.log.
# Start:  scripts/watchdog.sh start      Stop: scripts/watchdog.sh stop      Status: scripts/watchdog.sh status
cd "$(dirname "$0")/.." && source scripts/env.sh
LOG=$PARKSENSE_HOME/outputs/logs/watchdog.log
PID=$PARKSENSE_HOME/outputs/logs/watchdog.pid
COMPONENTS=${WATCHDOG_COMPONENTS:-"poller stream score verify"}
MIN_FREE_MB=${WATCHDOG_MIN_FREE_MB:-800}   # do not (re)start heavy Spark jobs when memory is this low

log() { echo "$(date -u +%FT%TZ) $*" >> $LOG; }
up() { jps 2>/dev/null | grep -qw "$1"; }
port() { (echo > /dev/tcp/127.0.0.1/$1) 2>/dev/null; }
alive() { [ -f outputs/logs/$1.pid ] && kill -0 "$(cat outputs/logs/$1.pid)" 2>/dev/null; }
free_mb() { awk '/MemAvailable/ {print int($2/1024)}' /proc/meminfo; }

check_once() {
  up NameNode && up DataNode || { log "HDFS down -> start-dfs.sh"; start-dfs.sh >> $LOG 2>&1; sleep 10; }
  up HMaster || { log "HMaster down -> start-hbase.sh"; start-hbase.sh >> $LOG 2>&1; sleep 30; }
  up HRegionServer || { log "RegionServer down -> start regionserver"; hbase-daemon.sh start regionserver >> $LOG 2>&1; sleep 30; }
  port 9090 || { log "Thrift down -> start thrift"; hbase-daemon.sh start thrift >> $LOG 2>&1; sleep 5; }
  port 8085 || { log "REST down -> start rest"; hbase-daemon.sh start rest -p 8085 --infoport 8086 >> $LOG 2>&1; sleep 5; }
  for c in $COMPONENTS; do
    alive $c && continue
    if [ "$c" != poller ] && [ "$(free_mb)" -lt "$MIN_FREE_MB" ]; then
      log "$c down but only $(free_mb) MB free -> waiting"; continue
    fi
    log "$c down -> restart"; scripts/start_live.sh $c >> $LOG 2>&1
  done
}

case "${1:-run}" in
  start)
    if [ -f $PID ] && kill -0 "$(cat $PID)" 2>/dev/null; then echo "watchdog already running (pid $(cat $PID))"; exit 0; fi
    setsid nohup "$0" run > /dev/null 2>&1 < /dev/null &
    echo $! > $PID; echo "watchdog started (pid $!) -> $LOG" ;;
  stop)
    [ -f $PID ] && kill "$(cat $PID)" 2>/dev/null && echo "watchdog stopped"; rm -f $PID ;;
  status)
    if [ -f $PID ] && kill -0 "$(cat $PID)" 2>/dev/null; then echo "watchdog running (pid $(cat $PID))"; else echo "watchdog not running"; fi
    tail -5 $LOG 2>/dev/null ;;
  run)
    log "watchdog loop started (components: $COMPONENTS)"
    while true; do check_once; sleep 60; done ;;
esac
