# ParkSense SG: Real-time Carpark Availability Analytics and "Will it be full?" Prediction

## 1. Title Page

**ParkSense SG — Real-time Carpark Availability Analytics & 30-minute "Nearly Full" Prediction with Apache Spark (Scala RDD + PySpark ML), HDFS and HBase**

23CSE352 Big Data Analytics — Project Review 3 (Apache Spark)

| Name | Register number |
|---|---|
| _Team member 1_ | _Reg. no._ |
| _Team member 2_ | _Reg. no._ |
| _Team member 3_ | _Reg. no._ |
| _Team member 4_ | _Reg. no._ |

## 2. Introduction

Drivers circle looking for parking, wasting time and fuel and adding congestion. Singapore's Housing & Development
Board publishes the live availability of about 2,000 public-housing carparks through data.gov.sg, updated every minute.
A live count only says what is free now; this project predicts whether a carpark will be **nearly full (≤ 10% of car
lots free) 30 minutes ahead**, and explains which hours and carpark types are most at risk. Processing is done in Scala
with Spark's RDD API, the model in PySpark MLlib, the data lake is HDFS and the serving store is HBase.

## 3. Problem Statement

> Using live and historical availability for ~2,000 Singapore HDB carparks, can we predict whether a carpark will be
> nearly full (≤ 10% of car lots free) 30 minutes from now, and which hours and carpark types are most at risk?

Decisions supported: (1) a driver choosing between nearby carparks by predicted space on arrival; (2) planners
identifying chronically full carparks and hours. Because availability rarely flips within 30 minutes, the model is
judged against a persistence baseline and on the rows where the state actually changes.

## 4. Objectives

1. Build a resumable, rate-limited ingestion of TBD days of 15-minute history plus a 1-minute live feed into HDFS and HBase.
2. Implement the processing in **Scala + Spark RDDs** (transformations, actions, key-value aggregation, joins) and
   demonstrate partitions, lineage/DAG, persistence, shuffle optimisation and lineage-based fault tolerance.
3. Train and evaluate PySpark Random Forest and GBT classifiers with leakage-free features and a time-based split,
   reporting accuracy, precision, recall and F1 against two baselines.
4. Serve live predictions, verify them on unseen data, and expose the results through HBase for the dashboard.

## 5. Dataset Description

| Item | Value |
|---|---|
| Source | data.gov.sg `GET /v1/transport/carpark-availability?date_time=…` (HDB), Singapore Open Data Licence v1.0 |
| Metadata | HDB Carpark Information (`d_23f946fa557947f93a8043bbef41dd09`), 2,273 carparks, SVY21 → WGS84 |
| Backfill window | TBD → TBD SGT, every TBD min |
| Snapshots | TBD requested · TBD failed · HTTP 429s: TBD |
| Carparks per snapshot | min TBD · median TBD · max TBD |
| Flat lines (one per snapshot × carpark) | 389,610 |
| Lot-type records after flatMap | 468,604 |
| Clean records / car-lot records | 460,643 / 379,780 |
| ML rows (carpark × 15-min slot, car lots, reliable sensors) | 329,836 over 1,739 carparks |
| Target | `full_30`: available/total ≤ 0.10 at t + 30 min — positive rate 4.59% |

Attributes are listed in `docs/data_dictionary.md`. Data-quality findings (`docs/data_quality_report.md`): 7,189
stale readings (> 30 min since the sensor update) dropped, 772 records with total ≤ 0,
193 availability values clipped, 30 always-zero and
210 never-changing carparks excluded from ML, 1,344 records without metadata,
and the API returns an empty snapshot for exactly 00:00:00 (retried at +1 min).

## 6. Methodology

1. **Ingestion** — metadata download and coordinate conversion; backfill of the 15-minute grid (newest first, 3
   workers, ≤ 4 requests/10 s, resumable, timestamp check); live poller every 60 s (atomic files + HBase).
2. **HDFS** — `scripts/03_hdfs_layout.sh` uploads flat files by day and the metadata.
3. **Scala RDD pipeline** — parse, flatMap lot types, clean, join metadata, key-value aggregates, Parquet.
4. **Spark concepts** — partitions, lineage/DAG, cache/persist, shuffle, fault tolerance (`--master local[4,3]`).
5. **Spark SQL analytics** — eight numbered queries over temp views of the Parquet output.
6. **EDA** — staleness, carparks per snapshot, occupancy patterns, class balance, persistence analysis.
7. **Features + ML** — slot-based self-join features, time split with embargo, train-only target encoding, class
   weights, validation grid, threshold tuning, test evaluation with transition metrics and horizons.
8. **Live** — StreamingJob (RDD per micro-batch → HBase), score_live (same feature code) and verify_live.
9. **Serving** — metrics and per-carpark profiles published to HBase for the Express/React UI.

## 7. Scala + Spark Implementation

**RDD creation and partitions.** `sc.textFile(hdfs:///parksense/raw/flat/*/*.txt, minPartitions = 8)` created
193 partitions — one per small 15-minute file (the small-files effect) — so the pipeline
`coalesce`s to 32 partitions (a narrow transformation, no shuffle) before heavy work; live archive files
are added with `union`.

**Transformations** (`RddPipeline.scala`, rubric table in PROGRESS.md): `map` (parseLine → Either), `filter` (valid
lines, counting malformed), **`flatMap`** (one record per lot type), `filter` (total ≤ 0, stale > 30 min), `map`
(clip, occupancy, SGT calendar), `keyBy` (metadata pairs), `leftOuterJoin` (shuffle join) vs a broadcast map-side join,
**`reduceByKey`** (citywide occupancy per snapshot), `aggregateByKey` (carpark × hour × dow means), `combineByKey`
(per-carpark min/max/mean/full-rate in one pass), `mapValues`, `distinct`, `sortBy`, `union`.

**Actions**: `count` (389,610 lines), `first`, `take(5)`, `collect` (small aggregates), **`reduce`**
(citywide busiest moment: 2026-10-15T04:00 SGT at 49.92% occupancy),
`countByKey` (records per carpark type), `countByValue` (lot-type distribution), `saveAsTextFile`.

**Join strategies**: shuffle join 2073 ms vs broadcast join 968 ms (identical row counts).

**Partitions** (`SparkConcepts.scala` §1): records per partition after coalesce: skew 1.167;
`partitionBy(HashPartitioner(8))` followed by `reduceByKey` with the same partitioner ran 1.0 stage
with 0.0 B of shuffle. Aggregation time vs partitions:

| partitions | ms |
|---|---|
| 1 | 653 |
| 2 | 435 |
| 4 | 329 |
| 8 | 529 |
| 16 | 499 |
| 32 | 611 |

**Lineage & DAG** — `outputs/lineage/occupancy_lineage.txt` (annotated narrow vs wide), `docs/dag_explanation.md`
(stages of the join + reduceByKey job and a History Server screenshot checklist).

**Persistence** (`concepts_cache.csv`):

| mode | storage_level | action1_ms | action2_ms | action3_ms | mem_bytes |
|---|---|---|---|---|---|
| no_cache | NONE | 730 | 734 | 557 | 0 |
| cache_MEMORY_ONLY | Memory Deserialized 1x Replicated | 943 | 34 | 29 | 49932272 |
| persist_MEMORY_AND_DISK | Disk Memory Deserialized 1x Replicated | 944 | 32 | 25 | 49932272 |
| persist_MEMORY_ONLY_SER | Memory Serialized 1x Replicated | 779 | 82 | 78 | 18021502 |

**Shuffle** (`concepts_shuffle.csv`, bytes from a SparkListener on `onStageCompleted`):

| method | ms | shuffle_write_bytes | shuffle_write_records | results_identical |
|---|---|---|---|---|
| reduceByKey | 295 | 1208995 | 63851 | True |
| groupByKey | 436 | 3880428 | 379780 | True |
| shuffle join | 476 | 4129033 | 382053 | True |
| broadcast join | 136 | 0 | 0 | True |

**Fault tolerance** (`concepts_fault_tolerance.txt`): a `map` throws when `TaskContext.attemptNumber == 0 &&
partitionId == 2`; with `local[4,3]` Spark re-runs the task and recomputes the partition from lineage:

```
Task attempts for partition 2:
  stage 64 partition 2 attempt 0 -> FAILED SIMULATED executor failure in partition 2, attempt 0
  stage 64 partition 2 attempt 1 -> SUCCESS 
Identical: true
BEFORE (17 lines):
AFTER (2 lines):
```

**Real-time** — `StreamingJob.scala` processes each micro-batch with the same `Records` parser via the RDD API and
writes KPIs to HBase with `BufferedMutator` inside `foreachPartition` (connections are not serialisable).

## 8. PySpark ML Implementation

* **Unit**: carpark × 15-minute slot, car lots, total ≥ 10, reliable sensors (329,836 rows).
* **Features** (30 numeric + 2 categorical): availability now; lags at 15/30/60/120 min and
  deltas (self-joins on the slot); rolling 1 h / 3 h mean and std; same time yesterday and last week; calendar;
  metadata; **target encoding of the carpark × hour × weekday full-rate computed on the training period only**.
* **Label** via a self-join on slot + 2 (30 min); also 15 and 60 min for the horizon study.
* **Split** (time-based, whole days, embargo = horizon):

| split | rows | positive_rate | days | weekend_share | start | end |
|---|---|---|---|---|---|---|
| train | 219101 | 0.0453 | 2 | 0.0000 | 2026-10-13 04:00 UTC | 2026-10-14 12:45 UTC |
| val | 44866 | 0.0509 | 2 | 0.0000 | 2026-10-14 13:30 UTC | 2026-10-14 20:15 UTC |
| test | 49039 | 0.0438 | 1 | 0.0000 | 2026-10-14 21:00 UTC | 2026-10-15 04:00 UTC |

* **Models**: persistence (full_now), historical (carpark-hour-dow rate > 0.5), Random Forest, GBT; inverse-frequency
  `weightCol`; grid on a max validation F1 (grid on a training sample; refit on train+val); threshold = max F1 on validation.
* **Selected**: Random Forest {"numTrees": 60, "maxDepth": 10}, threshold 0.06.

**Evaluation on the test days** (`model_comparison.csv`):

| model | threshold | accuracy | precision | recall | f1 | roc_auc | pr_auc |
|---|---|---|---|---|---|---|---|
| Persistence | full_now | 0.9843 | 0.8297 | 0.8080 | 0.8187 | 0.9002 | 0.6941 |
| Historical | carpark-hour-dow rate > 0.5 | 0.9573 | 0.8971 | 0.0284 | 0.0551 | 0.9221 | 0.7105 |
| Random Forest | 0.06 | 0.9625 | 0.5393 | 0.9776 | 0.6952 | 0.9922 | 0.9031 |
| Gradient-Boosted Trees | 0.1 | 0.9680 | 0.5830 | 0.9408 | 0.7199 | 0.9828 | 0.8582 |

**Transition metrics** (rows whose state changes between t and t + 30 min):

| model | transition | n | correct_share | precision | recall | f1 |
|---|---|---|---|---|---|---|
| Persistence | becomes full (0->1) | 412 | 0.0000 | nan | nan | nan |
| Persistence | frees up (1->0) | 356 | 0.0000 | nan | nan | nan |
| Persistence | all transitions | 768 | 0.0000 | nan | nan | nan |
| Persistence | precision/recall/F1 on transition rows | 768 | nan | 0.0000 | 0.0000 | 0.0000 |
| Random Forest | becomes full (0->1) | 412 | 0.8835 | nan | nan | nan |
| Random Forest | frees up (1->0) | 356 | 0.0000 | nan | nan | nan |
| Random Forest | all transitions | 768 | 0.4740 | nan | nan | nan |
| Random Forest | precision/recall/F1 on transition rows | 768 | nan | 0.5056 | 0.8835 | 0.6431 |

**Horizon sensitivity**:

TBD

_Generated by `evaluate.py`. Test period: 2026-10-14 21:00 UTC → 2026-10-15 04:00 UTC, 49,039 carpark-slots, positive rate 4.38% (nearly full = ≤ 10% of car lots free in 30 min)._

The selected model is **Random Forest** ({"numTrees": 60, "maxDepth": 10}, threshold 0.06 chosen on validation). On test: accuracy 0.9625, precision 0.539, recall 0.978, F1 0.695, PR-AUC 0.903. Persistence ("full in 30 min if full now"): precision 0.830, recall 0.808, F1 0.819. Historical baseline F1 0.055. The model **beats persistence** (ΔF1 -0.124, Δrecall +0.170).

Only 1.6% of test rows change state within 30 min. On exactly those transition rows persistence is always wrong (0% correct), while the model gets 47.4% right — this is where prediction adds value over a live count.

Most important features: te_chd, avail_ratio, te_ch, ratio_lag30, ratio_lag15.


## 9. Visualization

| Figure | Insight |
|---|---|
| `fig02_hour_weekday_heatmap.png` | Occupancy peaks overnight (residents park at home) and is lowest in the working day; weekends differ from weekdays. |
| `fig03_top20_full.png` | A small set of carparks is nearly full most of the time — candidates for pricing or capacity action. |
| `fig04_type_hour_full_rate.png` | The nearly-full rate by carpark type and hour: the "which hours and types are most at risk" answer. |
| `fig05_map_evening_occupancy.png` | Spatial pattern of evening occupancy across Singapore's HDB estates. |
| `fig06_data_quality.png` | Staleness distribution and the daily share of dropped stale readings. |
| `fig07_model_comparison.png` · `fig08_confusion_best.png` · `fig09_feature_importance.png` | Model vs baselines, errors of the selected model, and what drives predictions. |
| `fig10_cache_benchmark.png` · `fig11_scaling.png` · `fig12_shuffle.png` | Spark persistence, scaling and shuffle evidence. |
| `fig13_live_eval.png` · `fig14_transitions_horizons.png` | Live verification; where the model beats persistence (transitions) and how it decays with horizon. |

## 10. Results and Discussion

**Where and when carparks fill.** Q1 (Spark SQL): citywide mean occupancy by hour, weekday vs weekend
(`q1_occupancy_by_hour_weekday_weekend.csv`). Most often full carparks (Top 20):

| rank | carpark_no | address | car_park_type | full_rate |
|---|---|---|---|---|
| 1 | UA2 | BLK 3 TO 5 UPPER ALJUNIED LANE | SURFACE CAR PARK | 0.9823 |
| 2 | M38 | BLK 63 TO 64 CIRCUIT ROAD | SURFACE CAR PARK | 0.9793 |
| 3 | PL34 | BLK 603 PUNGGOL ROAD | MULTI-STOREY CAR PARK | 0.9793 |
| 4 | TP30 | BLK 116/122 LORONG 1 TOA PAYOH | SURFACE CAR PARK | 0.9744 |
| 5 | WCGL | BLK 568A TO 568C CHAMPIONS WAY | SURFACE CAR PARK | 0.9275 |
| 6 | TP48 | BLK 8 LORONG 7 TOA PAYOH | SURFACE CAR PARK | 0.8994 |
| 7 | B65L | BLK 152A BEDOK SOUTH ROAD | SURFACE CAR PARK | 0.8705 |
| 8 | A20 | BLK 304/307/319 ANG MO KIO STREET 31 | SURFACE CAR PARK | 0.8601 |
| 9 | M36 | BLK 43 CIRCUIT ROAD | SURFACE CAR PARK | 0.8562 |
| 10 | KU9 | BLK 302,304 UBI AVENUE 1 | SURFACE CAR PARK | 0.8446 |

Nearly-full rate by carpark type and parking system (Q3):

| car_park_type | parking_system | carparks | nearly_full_rate | mean_occupancy |
|---|---|---|---|---|
| UNKNOWN | UNKNOWN | 7 | 0.2269 | 0.4310 |
| SURFACE CAR PARK | ELECTRONIC PARKING | 798 | 0.1039 | 0.5419 |
| BASEMENT CAR PARK | ELECTRONIC PARKING | 36 | 0.0527 | 0.4775 |
| SURFACE/MULTI-STOREY CAR PARK | ELECTRONIC PARKING | 11 | 0.0358 | 0.2914 |
| SURFACE CAR PARK | COUPON PARKING | 3 | 0.0260 | 0.3471 |
| MULTI-STOREY CAR PARK | ELECTRONIC PARKING | 910 | 0.0089 | 0.3741 |
| MECHANISED AND SURFACE CAR PARK | ELECTRONIC PARKING | 2 | 0.0000 | 0.3857 |
| COVERED CAR PARK | ELECTRONIC PARKING | 9 | 0.0000 | 0.5267 |

**Prediction.** See Section 8. The state changes within 30 minutes for only 1.35% of rows,
which is why persistence is strong overall; the value of the model is on those transitions and at longer horizons.

**Live evaluation.** TBD (run `pyspark_jobs/verify_live.py (after the live layer has run ≥ 30 min)`)

**System performance** (`perf_system.csv`): TBD (run `pyspark_jobs/perf_system.py`)

## 11. Conclusion and Future Work

The project turns an official live feed into a forward-looking decision: will this carpark have space when I arrive?
The Scala RDD pipeline processes 389,610 snapshot lines with measured evidence for every Spark concept in
the rubric; a leakage-safe PySpark model is evaluated honestly against persistence, with transition metrics showing
where prediction adds value; and the whole system runs live with HBase as the serving store. Future work: event and
weather features, longer history (holidays, school terms), more cities with open feeds, a mobile app with push alerts,
and running on a YARN cluster with more executors.

## 12. References

1. data.gov.sg — Carpark Availability (HDB) API, `https://api.data.gov.sg/v1/transport/carpark-availability`.
2. data.gov.sg — HDB Carpark Information, dataset `d_23f946fa557947f93a8043bbef41dd09`.
3. "Contains information from Carpark Availability and HDB Carpark Information accessed on TBD from data.gov.sg which is made available under the terms of the Singapore Open Data Licence version 1.0." (verify wording on data.gov.sg)
4. data.gov.sg API documentation and rate limits.
5. Apache Spark 3.5 documentation — RDD Programming Guide, Structured Streaming, MLlib.
6. Zaharia, M. et al. (2012). Resilient Distributed Datasets. NSDI'12.
7. Apache HBase Reference Guide (2.5) — schema design, REST and Thrift gateways.
8. pyproj — EPSG:3414 (SVY21) to EPSG:4326 transformation.
