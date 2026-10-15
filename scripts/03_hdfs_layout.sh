#!/usr/bin/env bash
# Creates the HDFS layout and uploads metadata + flat files (re-runnable: only new day/files are copied).
cd "$(dirname "$0")/.." && source scripts/env.sh
R=/parksense
hdfs dfs -mkdir -p $R/raw/flat $R/raw/meta $R/processed $R/stream/archive
hdfs dfs -put -f data/raw/carpark_info.csv $R/raw/meta/
for d in data/raw/flat/dt=*; do
  day=$(basename $d)
  hdfs dfs -mkdir -p $R/raw/flat/$day
  have=$(hdfs dfs -ls $R/raw/flat/$day 2>/dev/null | awk 'NR>1{n=split($NF,a,"/"); print a[n]}' | sort)
  new=$(comm -13 <(echo "$have") <(ls $d | sort))
  [ -n "$new" ] && (cd $d && hdfs dfs -put $new $R/raw/flat/$day/)
done
[ "${1:-}" = "--archive" ] && for d in data/stream/archive/dt=*; do hdfs dfs -mkdir -p $R/stream/archive/$(basename $d); hdfs dfs -put -f $d/* $R/stream/archive/$(basename $d)/; done
echo "HDFS: $(hdfs dfs -count $R/raw/flat | awk '{print $2" files, "$3" bytes"}') under $R/raw/flat"
hdfs dfs -ls $R/raw/flat | tail -3
