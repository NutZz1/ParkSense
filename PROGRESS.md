# PROGRESS — ParkSense SG

Every number here comes from `outputs/` or `data/raw/*_meta.json`. Sections are updated as phases complete.

## Phase 0 — Environment (DONE 2026-10-15)
See `outputs/logs/env.txt` and `outputs/logs/compat.md` (`scripts/00_check_env.sh`).
* Claude Code runs as `sreepu` and executes everything as `hduser` through a sudoers rule; sources are staged in
  `/home/sreepu/parksense-src` and rsynced to `/home/hduser/parksense-sg`.
* Kept the installed stack (no upgrades): Hadoop 3.3.1 + HBase 2.5.15 daemons on Java 8; Spark 3.5.9 (Scala 2.12.18) on
  Java 11 from `~/tools` (installed in Review 3 prep); sbt 1.10.7; Node 20; Python 3.12 venv `~/parksense-venv`.
* `scripts/02_start_services.sh` started HDFS, HBase (managed ZooKeeper), HBase Thrift :9090 and REST :8085.

## Phase 1 — Data acquisition (RUNNING)
* Metadata: 2,273 carparks, SVY21 → WGS84 with pyproj, 0 outside Singapore's bounding box.
* Live API: ~2,021 carparks per snapshot (measured). `{"items": []}` is returned for exactly 00:00:00 → the backfill
  retries at +1/+2 min.
* Backfill: 30 days × 15 min = 2,880 snapshots, newest first, 3 workers under a shared 4 req/10 s limiter (the poller
  keeps 1 req/10 s), resumable. Final numbers in `data/raw/backfill_meta.json`.
* Live poller every 60 s → `data/stream/incoming` + `archive/` + HBase `carpark_live`/`carpark_history`.
  Fix: the Thrift server drops idle sockets (~60 s, "Broken pipe"), so `hbase_io.py` opens a connection per operation.
* HBase: namespace `parksense`, tables carpark_live (s/p/m), carpark_history (s, TTL 14 d), predictions_log (p/a), metrics (v).
