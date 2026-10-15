# PROGRESS — ParkSense SG

Every number here comes from `outputs/` or `data/raw/*_meta.json`. Sections are updated as phases complete.

## Phase 0 — Environment (DONE 2026-10-15)
See `outputs/logs/env.txt` and `outputs/logs/compat.md` (`scripts/00_check_env.sh`).
* Claude Code runs as `sreepu` and executes everything as `hduser` through a sudoers rule; sources are staged in
  `/home/sreepu/parksense-src` and rsynced to `/home/hduser/parksense-sg`.
* Kept the installed stack (no upgrades): Hadoop 3.3.1 + HBase 2.5.15 daemons on Java 8; Spark 3.5.9 (Scala 2.12.18) on
  Java 11 from `~/tools` (installed in Review 3 prep); sbt 1.10.7; Node 20; Python 3.12 venv `~/parksense-venv`.
* `scripts/02_start_services.sh` started HDFS, HBase (managed ZooKeeper), HBase Thrift :9090 and REST :8085.

## Phase 1 — Data acquisition (DONE)
* Metadata: 2,273 carparks, SVY21 → WGS84 with pyproj, 0 outside Singapore's bounding box.
* Live API: ~2,021 carparks per snapshot (measured). `{"items": []}` is returned for exactly 00:00:00 → the backfill
  retries at +1/+2 min.
* Backfill: 30 days × 15 min = 2,880 snapshots, newest first, 3 workers under a shared 4 req/10 s limiter (the poller
  keeps 1 req/10 s), resumable. Final numbers in `data/raw/backfill_meta.json`.
* Live poller every 60 s → `data/stream/incoming` + `archive/` + HBase `carpark_live`/`carpark_history`.
  Fix: the Thrift server drops idle sockets (~60 s, "Broken pipe"), so `hbase_io.py` opens a connection per operation.
* HBase: namespace `parksense`, tables carpark_live (s/p/m), carpark_history (s, TTL 14 d), predictions_log (p/a), metrics (v).
* Final backfill: **2,880 / 2,880 snapshots** (2026-09-15 12:45 → 2026-10-15 12:30 SGT), 5,818,338 flat lines,
  carparks per snapshot min 2,010 / median 2,020 / max 2,047, **0 HTTP 429s**, 0 timestamp mismatches; first pass
  7,374 s (30 midnight snapshots then filled by a second pass at 00:01).

## Phase 2 — Scala RDD pipeline (DONE)
`scripts/run_scala.sh RddPipeline` (console: `outputs/logs/RddPipeline_console.txt`, numbers: `rdd_summary.csv`).
6,652,105 input lines (backfill + live archive via `union`) → flatMap → 7,991,116 lot records → 7,851,051 clean
(car lots 6,472,654); stale 126,479; total ≤ 0 13,586; clipped 3,293; unmatched metadata 19,300; 2,000 carparks;
24 always-zero + 172 never-changing carparks excluded from ML; textFile 2,880 partitions → coalesce(128);
shuffle join 9,179 ms vs broadcast join 1,831 ms; busiest moment 2026-09-23 04:00 SGT (50.5% citywide); runtime 206 s.
Memory fixes needed for the full data on a 3.5 GB driver: local[8], 128 partitions, release `lots`/`cleaned` caches early,
`DISK_ONLY` for the wide joined records.

### Rubric item → file:line (`scripts/rubric_lines.sh`)

| Rubric item | File:line |
|---|---|
| SparkContext/Session | `RddPipeline.scala:39` |
| create RDD (textFile) | `RddPipeline.scala:49` |
| number of partitions | `RddPipeline.scala:51` |
| transformation union (backfill + live archive) | `RddPipeline.scala:56` |
| create RDD (metadata) | `RddPipeline.scala:62` |
| accumulator | `RddPipeline.scala:67` |
| transformation map (parse) | `RddPipeline.scala:78` |
| transformation filter (valid) | `RddPipeline.scala:80` |
| transformation flatMap (explode lot types) | `RddPipeline.scala:85` |
| persist (re-used for stale stats and cleaning) | `RddPipeline.scala:87` |
| transformation filter (total, stale) | `RddPipeline.scala:89` |
| transformation map (clean + derive) | `RddPipeline.scala:96` |
| key-value pair RDD (keyBy) | `RddPipeline.scala:102` |
| action count | `RddPipeline.scala:106` |
| action first | `RddPipeline.scala:110` |
| action take | `RddPipeline.scala:111` |
| action countByValue (lot-type distribution) | `RddPipeline.scala:113` |
| transformation distinct | `RddPipeline.scala:115` |
| action collect (small) | `RddPipeline.scala:116` |
| key-value join (shuffle) | `RddPipeline.scala:133` |
| broadcast variable | `RddPipeline.scala:135` |
| action countByKey | `RddPipeline.scala:148` |
| transformation reduceByKey (citywide per snapshot) | `RddPipeline.scala:155` |
| transformation mapValues | `RddPipeline.scala:156` |
| action reduce (busiest moment) | `RddPipeline.scala:157` |
| transformation aggregateByKey (mean via sum, count) | `RddPipeline.scala:166` |
| transformation combineByKey (min/max/mean/count) | `RddPipeline.scala:172` |
| transformation sortBy + action take | `RddPipeline.scala:193` |
| action saveAsTextFile | `RddPipeline.scala:218` |
| repartition (wide) | `SparkConcepts.scala:53` |
| coalesce (narrow) | `SparkConcepts.scala:54` |
| records per partition (mapPartitionsWithIndex) | `SparkConcepts.scala:56` |
| partitionBy HashPartitioner | `SparkConcepts.scala:65` |
| reduceByKey with the same partitioner (no new shuffle) | `SparkConcepts.scala:70` |
| lineage toDebugString | `SparkConcepts.scala:109` |
| cache()/persist(level) | `SparkConcepts.scala:157` |
| unpersist | `SparkConcepts.scala:165` |
| reduceByKey (map-side combine) | `SparkConcepts.scala:182` |
| groupByKey (comparison only) | `SparkConcepts.scala:186` |
| broadcast variable | `SparkConcepts.scala:191` |
| fault tolerance (simulated task failure) | `SparkConcepts.scala:210` |
| checkpoint (lineage truncation) | `SparkConcepts.scala:228` |
| accumulator | `SparkConcepts.scala:237` |

## Phase 3 — Spark concepts (DONE)
`scripts/run_scala.sh SparkConcepts "local[4,3]"`: partitions 2,880 → coalesce 128 → repartition 16 → coalesce 4, skew
1.048; same-partitioner reduceByKey 1 stage / 0 B shuffle; aggregation 1/2/4/8/16/32 partitions = 4.8/2.9/1.8/2.0/2.6/4.0 s;
cache: 2nd action 6.9 s → 0.22 s; reduceByKey 7.3 MB vs groupByKey 50.6 MB shuffle; broadcast join 0 B vs 51.5 MB;
fault tolerance: partition 2 attempt 0 FAILED → attempt 1 SUCCESS, identical result; checkpoint 17 → 2 lineage lines.
Screenshot checklist: `docs/dag_explanation.md` (History Server: `scripts/start_history_server.sh`).

## Phase 4 — Spark SQL analytics (DONE)
8 queries → `outputs/analytics/q1…q8` + `query_times.csv` (3.2–11.7 s each); Parquet row counts equal the Scala output
(`consistency_check.json`); results pushed to HBase `analytics:q1…q8`.

## Phase 5 — EDA & data quality (DONE)
`docs/data_quality_report.md`; positive rate of `full_30` 5.47% → `full_ratio` 0.10 kept; state changes within 15/30/60 min
in 1.01% / 1.39% / 1.93% of rows; median staleness 2.35 min; no public holidays in the window.

## Phase 6 — Features & ML (DONE)
5,086,867 carpark-slot rows (1,780 carparks), 30 numeric + 2 categorical features. Split by days: train 22 (3,556,401 rows,
5.52% pos), val 4 (661,556), test 5 (807,637, 5.25%). Validation: RF {60 trees, depth 10} F1 0.837 (threshold 0.89) beat
GBT 0.832. **Test**: persistence F1 0.873 · historical 0.743 · **RF 0.860 (PR-AUC 0.943)** · GBT 0.853. The model does not
beat persistence on F1; on the 10,757 transition rows persistence is 0% correct and RF 31.8% (40.0% of "becomes full").
Horizon sensitivity (15/60 min): TBD (`python pyspark_jobs/evaluate.py` without `--skip-horizons`).

## Phase 7 — Performance (DONE)
`perf_system.csv` / `perf_interpretation.txt`: 1 → 16 cores 3.0×; data 25 → 100% = 3.8× rows, 4.2× time; shuffle
partitions 16 best (57.9 s) vs 200 (188.5 s); cached features 4.3× faster in a 3-model loop; HBase poller writes
~20,800 rows/s; API lag median 51 s; poll → micro-batch 33 s; RDD micro-batch 0.26 s; live scoring ~13 s.

## Phase 8 — Live layer (RUNNING)
poller (60 s) · StreamingJob (Scala, RDD per micro-batch, KPIs → HBase via BufferedMutator in foreachPartition) ·
score_live (every 5 min, same `build_features`) · verify_live (every 5 min). `predictions_log` was truncated before the
final model went live so the live evaluation only contains final-model predictions.

## Phase 9 — UI (ON HOLD by design: waiting for the user's UI design approval)
`publish_metrics.py` done (metrics, insights and per-carpark profiles in HBase `parksense:metrics`).

## Phase 10 — Figures & docs (DONE, live parts refresh as data arrives)
13 figures (`fig13` needs live_eval) · `report_draft.md` (generated by `viz/fill_report.py`) · `slides_outline.md` ·
`demo_script.md` · `viva_notes.md` (30 Q&A) · `architecture.md` · `fallbacks.md` · `scripts/run_batch.sh` (with `--from`).

## Environment incidents (for the record)
* WSL restarted several times (memory pressure / session end) → raised WSL to 10 GB RAM + 8 GB swap (`.wslconfig`).
* HBase RegionServer aborted twice (ZooKeeper session expired under heavy Spark load) → restarted; on a real cluster
  raise `zookeeper.session.timeout` and run HBase on separate nodes.
* HBase REST: the info UI also defaults to 8085 → `--infoport 8086`.

## Rubric coverage
| Rubric item | Where |
|---|---|
| Real-world dataset, problem, attributes | data.gov.sg HDB feeds; `docs/analysis_question.md`, `docs/data_dictionary.md` |
| Create RDD | `RddPipeline.scala:49` (textFile), `:62` (metadata) |
| ≥ 3 transformations | map, filter, **flatMap**, keyBy, **reduceByKey**, aggregateByKey, combineByKey, mapValues, join, distinct, sortBy, union |
| ≥ 2 actions | count, first, take, collect, **reduce**, countByKey, countByValue, saveAsTextFile |
| Key-value RDD operations | carpark × hour × dow aggregates, type × hour, joins with metadata, broadcast join |
| Number of partitions | `RddPipeline.scala:51`, SparkConcepts §1 |
| Lineage & DAG | SparkConcepts §2, `outputs/lineage/occupancy_lineage.txt`, `docs/dag_explanation.md`, History Server |
| cache()/persist() | SparkConcepts §3, `concepts_cache.csv`, `fig10` |
| Fault tolerance | SparkConcepts §5, `concepts_fault_tolerance.txt` |
| Prepare features / split / ≥ 1 model | `features.py`, `train.py` (time split by days), RF + GBT + 2 baselines |
| Accuracy, precision, recall, F1 | `outputs/metrics/model_comparison.csv` (+ ROC/PR-AUC, transitions) |
| ≥ 3 visualisations | 13 figures in `outputs/figures/` |
| Real-time dataset | live poller + StreamingJob + score_live + verify_live |
| Preprocessing & EDA | RddPipeline cleaning + `eda.py` + `docs/data_quality_report.md` |
| Performance analysis | model + transition metrics + `perf_system.csv` + live evaluation |
| NoSQL backend + full stack | HBase (4 tables, Thrift + REST); UI on hold for design approval |
