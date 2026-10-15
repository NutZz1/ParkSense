# Demo script (10–15 min) — Dataset → Scala/Spark → ML → Evaluation → Visualization → Results

Run as `hduser` from `/home/hduser/parksense-sg` after `source scripts/env.sh`.

## T − 30 min
```bash
scripts/02_start_services.sh            # HDFS, HBase, Thrift, REST
scripts/start_live.sh                   # poller, StreamingJob, score_live, verify_live
scripts/start_history_server.sh         # http://localhost:18080
python pyspark_jobs/verify_live.py && python viz/make_figures.py && python viz/fill_report.py
```
Open: the History Server, `outputs/figures/`, a terminal, the HBase web UI http://localhost:16010.

## 1. Dataset (1.5 min)
```bash
cat data/raw/backfill_meta.json
hdfs dfs -ls /parksense/raw/flat | tail -3
head -3 data/raw/flat/dt=*/1200.txt | head -3      # flat line: snapshot, carpark, update time, C:total:avail|...
```
Real, official data: data.gov.sg (HDB), 15-minute history for 30 days plus a 1-minute live feed.

## 2. Scala + Spark RDD (4 min) — the 10 marks
```bash
scripts/run_scala.sh RddPipeline
```
Point at: `getNumPartitions` after textFile (one per small file) → coalesce; the labelled transformations (map, filter,
**flatMap** of lot types, reduceByKey, aggregateByKey, combineByKey, join vs broadcast join, sortBy, distinct, union);
the actions (count, first, take, reduce = busiest moment, countByKey, countByValue, saveAsTextFile); the accumulators
summary. The rubric → file:line table is in PROGRESS.md.

```bash
scripts/run_scala.sh SparkConcepts "local[4,3]"
```
Point at: partitionBy + same-partitioner reduceByKey = 0 B shuffle; lineage `toDebugString`; cache table;
reduceByKey vs groupByKey shuffle bytes; **attempt 0 FAILED → attempt 1 SUCCESS, identical result**; checkpoint
truncating the lineage. Then the History Server: DAG of the "2 lineage" job, the Tasks table of "5 fault tolerance".

## 3. Spark SQL + EDA (1.5 min)
`outputs/analytics/q1…q8`, `fig02_hour_weekday_heatmap.png`, `fig03_top20_full.png`, `fig06_data_quality.png`.

## 4. PySpark ML + evaluation (3 min)
Features (self-joins on the 15-min slot, train-only target encoding) → time split by whole days → RF vs GBT with class
weights → threshold on validation. `fig07_model_comparison.png`, `fig08_confusion_best.png`,
`outputs/metrics/transition_metrics.csv` and `fig14_transitions_horizons.png` — **persistence is strong because states
rarely flip; on the transitions persistence is always wrong and the model is not.**

## 5. Live + HBase (2.5 min)
```bash
tail -3 outputs/logs/poller.log
tail -3 outputs/metrics/stream_batches.csv
echo "get 'parksense:carpark_live', 'HLM'" | hbase shell -n
echo "scan 'parksense:carpark_history', {ROWPREFIXFILTER => 'HLM#', LIMIT => 5}" | hbase shell -n
curl -s -H 'Accept: application/json' localhost:8085/parksense:metrics/live:eval | head -c 300
cat outputs/metrics/live_eval_summary.json
```
Explain the row keys (reversed timestamp → newest first) and the column families (different update rates).
`fig13_live_eval.png`.

## 6. Results + conclusion (1 min)
Slides 11–12: chronic carparks and risky hours, model vs persistence, live verification, limitations.

## Offline fallback
| Problem | Fallback |
|---|---|
| No internet | every batch job runs from HDFS/local data; the poller just logs failures |
| HBase down | show `outputs/metrics/*.csv` and the HBase screenshots; restart with `scripts/02_start_services.sh` |
| A Spark job fails on the day | show `outputs/logs/RddPipeline_console.txt`, `SparkConcepts_console.txt` (saved runs) |
| Laptop problem | screenshots in `docs/screenshots/` (take them the day before) and the PPT |
