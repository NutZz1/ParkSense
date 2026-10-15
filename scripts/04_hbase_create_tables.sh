#!/usr/bin/env bash
cd "$(dirname "$0")/.." && source scripts/env.sh
hbase shell -n hbase/create_tables.hbase 2>&1 | grep -vE "SLF4J|^\s*$"
