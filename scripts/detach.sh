#!/usr/bin/env bash
# Run a command fully detached (new session via setsid, no controlling terminal) so it survives the
# launching shell/session ending. Usage: scripts/detach.sh <logfile> <command> [args...]
LOGF=$1; shift
cd "$(dirname "$0")/.."
setsid nohup "$@" > "$LOGF" 2>&1 < /dev/null &
echo "detached pid $! -> $LOGF"
