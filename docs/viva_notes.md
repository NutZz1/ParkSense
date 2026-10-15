# Viva notes — likely examiner questions

Numbers are from the full 30-day run (`outputs/metrics/`, `outputs/analytics/`). File names are given so you can show
the evidence.

## Spark / Scala RDD

**1. Why the RDD API rather than DataFrames?**
The rubric asks for RDD skills, and RDDs make every decision explicit: the key, the partitioner, where to combine, what
to cache. DataFrames would be shorter and faster (Catalyst optimiser, Tungsten memory layout); we use one only at the
end of `RddPipeline` to write Parquet, and Spark SQL for the eight analytics queries.

**2. How many partitions, and why coalesce?**
`textFile` over HDFS created **2,880 partitions** — one per 15-minute snapshot file (the *small-files problem*). We
`coalesce(128)` before the heavy work: coalesce is **narrow** (merges neighbouring partitions, no shuffle), unlike
`repartition`, which is a full shuffle. After coalescing, partition sizes are even (max/min 1.048).

**3. Where is flatMap genuinely needed?**
Each flat line packs all lot types of a carpark (`C:105:102|Y:20:20|H:1:0`). `flatMap(explodeLots)` turns 6,652,105
lines into 7,991,116 lot records (C 6.47M car lots after cleaning; Y and H the rest).

**4. Narrow vs wide dependencies; stages?**
Narrow (map, filter, flatMap, mapValues, coalesce): one parent partition per child, pipelined inside a single task.
Wide (join, reduceByKey, groupByKey, repartition): a child needs data from many parents → shuffle → new stage.
`outputs/lineage/occupancy_lineage.txt` marks each ShuffledRDD/CoGroupedRDD; `docs/dag_explanation.md` lists the
stages of the join + reduceByKey job (two ShuffleMapStages for the two join inputs, one for the post-join
reduceByKey, then the ResultStage).

**5. Why does reduceByKey beat groupByKey?** (`concepts_shuffle.csv`)
reduceByKey combines on the map side, so only partial sums cross the network: **7.3 MB / 255,970 records** vs
**50.6 MB / 5,662,821 records** for groupByKey (2.1 s vs 4.6 s), identical results. groupByKey also risks running out
of memory on a hot key.

**6. Shuffle join vs broadcast join?**
The metadata (2,273 carparks) is tiny, so we broadcast it and join inside `map` — no shuffle of the big side:
RddPipeline 9.2 s (shuffle `leftOuterJoin`) vs 1.8 s (broadcast); SparkConcepts 51.5 MB shuffled vs 0 B.

**7. partitionBy and "no extra shuffle"?**
After `partitionBy(new HashPartitioner(8))`, a `reduceByKey` with the same partitioner ran in **1 stage with 0 B of
shuffle**, because equal keys are already co-located. `map` drops the partitioner (keys may change); `mapValues` keeps it.

**8. How many partitions is right?** (`concepts_partitions.csv`)
Aggregation time: 1 partition 4.8 s → 4 partitions 1.8 s → 32 partitions 4.0 s. Too few under-uses the cores; too
many adds scheduling and shuffle-file overhead. Rule of thumb: 2–4 partitions per core, partitions of ~100–200 MB.

**9. cache vs persist and the storage levels?** (`concepts_cache.csv`)
`cache()` = `persist(MEMORY_ONLY)` (deserialised objects). Second action: **6.9 s uncached → 0.22 s cached**;
MEMORY_AND_DISK 0.17 s; MEMORY_ONLY_SER 1.07 s but 269 MB instead of 747 MB (serialised bytes are smaller, but cost CPU
to read). The first action is slower when caching because it also stores the blocks. We used `MEMORY_AND_DISK_SER`
and `DISK_ONLY` in the pipeline after a 3.5 GB driver ran out of heap with three large RDDs cached at once — release
caches (`unpersist`) as soon as they are no longer needed.

**10. How does lineage give fault tolerance?** (`concepts_fault_tolerance.txt`)
An RDD records how it was built, not the data. With `--master local[4,3]` (up to 3 attempts per task), a map throws
on partition 2, attempt 0; Spark re-runs the task, recomputes that partition from its lineage, and the result is
identical to the failure-free run. Only the lost partition is recomputed, not the whole job.

**11. Driver vs executor failure?**
Executor/task failure: tasks are rescheduled and partitions recomputed from lineage (or re-read from surviving shuffle
files). Driver failure ends the application — the driver holds the DAG and scheduler. For streaming, the checkpoint
directory (source offsets) lets a restarted driver resume; we saw this when `StreamingJob` restarted and continued from
its checkpoint after WSL restarts.

**12. When should you checkpoint?**
When lineage gets long (iterative jobs, stateful streaming) so recomputation would be expensive. `checkpoint()` writes
the RDD to reliable storage and truncates the lineage: 17 lines → 2 (`ReliableCheckpointRDD`). cache does not replace
it, because cached blocks die with their executor.

**13. Accumulators — any pitfall?**
They count side information (malformed 0, stale 126,479, clipped 3,293, total ≤ 0 13,586, unmatched metadata 19,300).
Updates inside *transformations* are re-applied if a partition is recomputed, so we read them after actions on persisted
RDDs and never use them for results.

## Data

**14. Why Singapore, and why HDB carparks?**
Indian cities do not publish open, live, machine-readable parking data. data.gov.sg (HDB) is official, free, live (every
minute) and historical (date_time parameter). The method transfers to any city with such a feed.

**15. How was the history collected without breaking rate limits?**
2,880 snapshots (30 days × 96), newest first, 3 worker threads sharing a limiter of 4 requests / 10 s (the live poller
keeps the 5th slot; the limit is 6), exponential backoff on 429/5xx, resumable (existing files skipped): **0 HTTP 429s**,
5.8M flat lines. Every response's `items[0].timestamp` was checked against the request (0 mismatches). The API returns
`{"items": []}` for exactly 00:00:00, so midnight is fetched at 00:01.

**16. Stale data?**
Staleness = snapshot time − sensor update time; median 2.35 min. 2.05% of readings are older than 30 min and are dropped
(126,479 records). 24 always-zero and 172 never-changing carparks are excluded from ML as unreliable sensors.

**17. Time zones?**
The API's `date_time` and `update_datetime` are SGT without an offset; everything is converted to epoch ms with +08:00,
Spark's session time zone is Asia/Singapore, and calendar features use SGT.

## Machine learning

**18. Why a time-based split?**
Consecutive 15-minute slots are almost identical, so a random split puts near-duplicates of test rows in training — it
measures interpolation and leaks the future. We train on the first 22 days, validate on the next 4, test on the last 5,
with a 30-minute embargo so no training label falls in a later period.

**19. Leakage-safe target encoding?**
`te_chd` = smoothed share of "full in 30 min" per carpark × hour × weekday, computed **only from the training days**
(m-estimate shrinkage towards the carpark × hour rate, then the global rate) and joined onto validation/test/live. It is
the top feature (RF importance 0.225).

**20. Why is persistence so strong, and what are transition metrics?**
Only **1.39%** of rows change state within 30 minutes, so "it will be full if it is full now" is right 98.7% of the
time (test F1 0.873). The Random Forest has F1 0.860 overall — **it does not beat persistence on F1**, which we report
honestly — but its PR-AUC is far higher (0.943 vs 0.778). On the 10,757 test rows where the state *does* change,
persistence is 0% correct by definition; the model is right on **31.8%** (40.0% of "becomes full" events). That is the
value of a prediction over a live count.

**21. Class imbalance?**
5.47% positives. Inverse-frequency `weightCol` on the training split, PR-AUC as the main ranking metric, and the
threshold tuned on validation (max F1 → 0.89 for RF).

**22. Why RF and not GBT?**
Both were tuned on validation; RF won (F1 0.837 vs 0.832) and stayed better on test (0.860 vs 0.853). RF is also faster
to refit and less sensitive to the noisy sensors.

**23. Train/serve consistency?**
`score_live.py` calls the same `build_features()` as training, maps minute-level live readings to the same 15-minute
slots, loads the training-period target-encoding tables and the saved threshold.

## HBase / architecture

**24. Why HBase and not Hive?**
The app needs point lookups — one carpark's latest state, its last 24 h — which HBase serves by row key in
milliseconds and with a high write rate (the poller writes ~4,000 rows per minute — live state + history — at a median of ~20,800 rows/s).
The big scans run once in Spark, and their results are stored in HBase, so no SQL warehouse is needed.

**25. Explain the row keys.**
`carpark_history` key = `carpark_no#<Long.MAX − epoch_ms, 19 digits>`: the prefix keeps one carpark together, the
reversed timestamp sorts newest first, so "last 24 h" is a short forward scan. Writes spread over ~2,000 prefixes (no
hot spot). A `timestamp#carpark` key would send every minute's writes to one region.

**26. Why separate column families?**
Families are stored and configured separately. In `carpark_live`, `s` (state) is rewritten every minute by the poller,
`p` (prediction) every 5 minutes by the scorer, `m` (metadata) once — so frequent writes never rewrite static data.
`predictions_log` keeps `p` (at prediction time) and `a` (actuals, written 30 minutes later). Few families (1–3), short
names (every name is stored with every cell), TTL 14 days on history.

**27. Why are HBase connections opened inside foreachPartition?**
Connections are not serialisable and are expensive; one per partition (with a `BufferedMutator` batching puts) on the
executor is the standard pattern. In Python, the Thrift server dropped idle sockets after ~60 s, so `hbase_io.py` opens
a connection per operation.

**28. What broke during the build, and what did you learn?** (good honesty answer)
Out-of-memory with three large cached RDDs (fixed by releasing caches and DISK_ONLY persistence); HBase RegionServer
ZooKeeper-session expiry under heavy Spark load (restart; would raise `zookeeper.session.timeout` on a real cluster);
the HBase REST info port clashing with its own service port; the API's empty midnight snapshot.

**29. Scaling to a real cluster?**
Run on YARN with several executors (`--num-executors`, `--executor-memory`), HDFS replication 3, more HBase region
servers with pre-split regions by carpark prefix, and Kafka instead of the file stream.

**30. Limitations?**
One month of history (no holidays in the window), HDB carparks only, sensor staleness, live verification over hours,
not weeks.
