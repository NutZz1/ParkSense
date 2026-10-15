"""Writes docs/report_draft.md (official 12-section structure) with every number read from outputs/ and data/raw.
Missing inputs become 'TBD (run <script>)'. Usage: python viz/fill_report.py
"""
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
M, A, MD = ROOT / "outputs/metrics", ROOT / "outputs/analytics", ROOT / "models"


def csv(name, base=M):
    f = base / name
    return pd.read_csv(f) if f.exists() else None


def js(p):
    return json.loads(Path(p).read_text()) if Path(p).exists() else None


def tbd(script):
    return f"TBD (run `{script}`)"


def table(df, cols=None, fmt=None):
    if df is None:
        return "TBD"
    d = df[cols] if cols else df
    fmt = fmt or {}
    head = "| " + " | ".join(d.columns) + " |\n|" + "---|" * len(d.columns) + "\n"
    rows = []
    for r in d.itertuples(index=False):
        rows.append("| " + " | ".join(fmt.get(c, lambda v: f"{v:.4f}" if isinstance(v, float) else str(v))(v)
                                      for c, v in zip(d.columns, r)) + " |")
    return head + "\n".join(rows)


rdd = csv("rdd_summary.csv")
S = rdd.set_index("metric")["value"].to_dict() if rdd is not None else {}
bf = js(ROOT / "data/raw/backfill_meta.json") or {}
eda = js(M / "eda_summary.json") or {}
fs = js(M / "features_summary.json") or {}
best = js(MD / "best_model_meta.json") or {}
mc, tm, hz = csv("model_comparison.csv"), csv("transition_metrics.csv"), csv("horizon_sensitivity.csv")
cache, shuf, parts = csv("concepts_cache.csv"), csv("concepts_shuffle.csv"), csv("concepts_partitions.csv")
pops, perf = csv("concepts_partition_ops.csv"), csv("perf_system.csv")
live = js(M / "live_eval_summary.json")
top20, q1, q3 = csv("rdd_top20_full.csv"), csv("q1_occupancy_by_hour_weekday_weekend.csv", A), csv("q3_full_rate_by_type_and_system.csv", A)
ft = (M / "concepts_fault_tolerance.txt").read_text() if (M / "concepts_fault_tolerance.txt").exists() else ""
summary_md = (M / "model_summary.md").read_text().split("\n", 2)[-1] if (M / "model_summary.md").exists() else tbd("pyspark_jobs/evaluate.py")
perf_txt = (M / "perf_interpretation.txt").read_text().strip() if (M / "perf_interpretation.txt").exists() else tbd("pyspark_jobs/perf_system.py")
fc = fs.get("counts", {})
cb = eda.get("class_balance_30", {})
n = lambda v: f"{int(float(v)):,}" if v not in (None, "") else "TBD"  # noqa: E731
pct = lambda v: f"{100 * float(v):.2f}%" if v is not None else "TBD"  # noqa: E731
po = pops.set_index("operation")["value"].to_dict() if pops is not None else {}
splits = best.get("split", {}).get("splits", [])

R = f"""# ParkSense SG: Real-time Carpark Availability Analytics and "Will it be full?" Prediction

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

1. Build a resumable, rate-limited ingestion of {bf.get('days', 'TBD')} days of 15-minute history plus a 1-minute live feed into HDFS and HBase.
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
| Backfill window | {bf.get('grid_start_sgt', 'TBD')} → {bf.get('grid_end_sgt', 'TBD')} SGT, every {bf.get('interval_min', 'TBD')} min |
| Snapshots | {bf.get('requested', 'TBD')} requested · {bf.get('failed', 'TBD')} failed · HTTP 429s: {bf.get('http_429', 'TBD')} |
| Carparks per snapshot | min {bf.get('carparks_per_snapshot', {}).get('min', 'TBD')} · median {bf.get('carparks_per_snapshot', {}).get('median', 'TBD')} · max {bf.get('carparks_per_snapshot', {}).get('max', 'TBD')} |
| Flat lines (one per snapshot × carpark) | {n(S.get('input_lines'))} |
| Lot-type records after flatMap | {n(S.get('lot_records_exploded'))} |
| Clean records / car-lot records | {n(S.get('clean_records'))} / {n(S.get('car_lot_records'))} |
| ML rows (carpark × 15-min slot, car lots, reliable sensors) | {n(fc.get('feature_rows'))} over {n(fc.get('carparks'))} carparks |
| Target | `full_30`: available/total ≤ 0.10 at t + 30 min — positive rate {pct(cb.get('positive_rate'))} |

Attributes are listed in `docs/data_dictionary.md`. Data-quality findings (`docs/data_quality_report.md`): {n(S.get('stale_dropped'))}
stale readings (> 30 min since the sensor update) dropped, {n(S.get('non_positive_total'))} records with total ≤ 0,
{n(S.get('clipped_available'))} availability values clipped, {n(S.get('always_zero_carparks'))} always-zero and
{n(S.get('never_changing_carparks'))} never-changing carparks excluded from ML, {n(S.get('unmatched_meta_records'))} records without metadata,
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
{n(S.get('input_partitions_textFile'))} partitions — one per small 15-minute file (the small-files effect) — so the pipeline
`coalesce`s to {n(S.get('partitions_after_coalesce'))} partitions (a narrow transformation, no shuffle) before heavy work; live archive files
are added with `union`.

**Transformations** (`RddPipeline.scala`, rubric table in PROGRESS.md): `map` (parseLine → Either), `filter` (valid
lines, counting malformed), **`flatMap`** (one record per lot type), `filter` (total ≤ 0, stale > 30 min), `map`
(clip, occupancy, SGT calendar), `keyBy` (metadata pairs), `leftOuterJoin` (shuffle join) vs a broadcast map-side join,
**`reduceByKey`** (citywide occupancy per snapshot), `aggregateByKey` (carpark × hour × dow means), `combineByKey`
(per-carpark min/max/mean/full-rate in one pass), `mapValues`, `distinct`, `sortBy`, `union`.

**Actions**: `count` ({n(S.get('input_lines'))} lines), `first`, `take(5)`, `collect` (small aggregates), **`reduce`**
(citywide busiest moment: {S.get('busiest_moment_sgt', 'TBD')} SGT at {pct(S.get('busiest_citywide_occupancy'))} occupancy),
`countByKey` (records per carpark type), `countByValue` (lot-type distribution), `saveAsTextFile`.

**Join strategies**: shuffle join {S.get('shuffle_join_ms', 'TBD')} ms vs broadcast join {S.get('broadcast_join_ms', 'TBD')} ms (identical row counts).

**Partitions** (`SparkConcepts.scala` §1): records per partition after coalesce: skew {po.get('coalesce32 skew max/min', 'TBD')};
`partitionBy(HashPartitioner(8))` followed by `reduceByKey` with the same partitioner ran {po.get('reduceByKey(samePartitioner) stages', 'TBD')} stage
with {po.get('reduceByKey(samePartitioner) shuffle_write_bytes', 'TBD')} B of shuffle. Aggregation time vs partitions:

{table(parts)}

**Lineage & DAG** — `outputs/lineage/occupancy_lineage.txt` (annotated narrow vs wide), `docs/dag_explanation.md`
(stages of the join + reduceByKey job and a History Server screenshot checklist).

**Persistence** (`concepts_cache.csv`):

{table(cache, ["mode", "storage_level", "action1_ms", "action2_ms", "action3_ms", "mem_bytes"])}

**Shuffle** (`concepts_shuffle.csv`, bytes from a SparkListener on `onStageCompleted`):

{table(shuf, ["method", "ms", "shuffle_write_bytes", "shuffle_write_records", "results_identical"])}

**Fault tolerance** (`concepts_fault_tolerance.txt`): a `map` throws when `TaskContext.attemptNumber == 0 &&
partitionId == 2`; with `local[4,3]` Spark re-runs the task and recomputes the partition from lineage:

```
{chr(10).join(l for l in ft.splitlines() if 'attempt' in l or 'Identical' in l or 'lines)' in l)}
```

**Real-time** — `StreamingJob.scala` processes each micro-batch with the same `Records` parser via the RDD API and
writes KPIs to HBase with `BufferedMutator` inside `foreachPartition` (connections are not serialisable).

## 8. PySpark ML Implementation

* **Unit**: carpark × 15-minute slot, car lots, total ≥ 10, reliable sensors ({n(fc.get('slot_rows'))} rows).
* **Features** ({len(fs.get('numeric_features', []))} numeric + {len(fs.get('categorical_features', []))} categorical): availability now; lags at 15/30/60/120 min and
  deltas (self-joins on the slot); rolling 1 h / 3 h mean and std; same time yesterday and last week; calendar;
  metadata; **target encoding of the carpark × hour × weekday full-rate computed on the training period only**.
* **Label** via a self-join on slot + 2 (30 min); also 15 and 60 min for the horizon study.
* **Split** (time-based, whole days, embargo = horizon):

{table(pd.DataFrame(splits)) if splits else tbd('pyspark_jobs/train.py')}

* **Models**: persistence (full_now), historical (carpark-hour-dow rate > 0.5), Random Forest, GBT; inverse-frequency
  `weightCol`; grid on a {best.get('selection', '')}; threshold = max F1 on validation.
* **Selected**: {best.get('algorithm', 'TBD')} {json.dumps(best.get('params', {}))}, threshold {best.get('threshold', 'TBD')}.

**Evaluation on the test days** (`model_comparison.csv`):

{table(mc, ["model", "threshold", "accuracy", "precision", "recall", "f1", "roc_auc", "pr_auc"]) if mc is not None else tbd('pyspark_jobs/evaluate.py')}

**Transition metrics** (rows whose state changes between t and t + 30 min):

{table(tm) if tm is not None else 'TBD'}

**Horizon sensitivity**:

{table(hz, ["horizon_min", "state_change_rate", "f1", "persistence_f1", "recall", "persistence_recall"]) if hz is not None else 'TBD'}

{summary_md}

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

{table(top20.head(10), ["rank", "carpark_no", "address", "car_park_type", "full_rate"]) if top20 is not None else 'TBD'}

Nearly-full rate by carpark type and parking system (Q3):

{table(q3.head(8)) if q3 is not None else 'TBD'}

**Prediction.** See Section 8. The state changes within 30 minutes for only {pct(cb.get('state_change_rate_30min'))} of rows,
which is why persistence is strong overall; the value of the model is on those transitions and at longer horizons.

**Live evaluation.** {f"{live['verified']:,} live predictions verified against the readings that arrived 30 minutes later: accuracy {live['model']['accuracy']:.4f}, precision {live['model']['precision']:.3f}, recall {live['model']['recall']:.3f}, F1 {live['model']['f1']:.3f} (persistence F1 {live['persistence']['f1']:.3f}); transitions in the live window: {live['transitions']['n']}." if live else tbd('pyspark_jobs/verify_live.py (after the live layer has run ≥ 30 min)')}

**System performance** (`perf_system.csv`): {perf_txt}

## 11. Conclusion and Future Work

The project turns an official live feed into a forward-looking decision: will this carpark have space when I arrive?
The Scala RDD pipeline processes {n(S.get('input_lines'))} snapshot lines with measured evidence for every Spark concept in
the rubric; a leakage-safe PySpark model is evaluated honestly against persistence, with transition metrics showing
where prediction adds value; and the whole system runs live with HBase as the serving store. Future work: event and
weather features, longer history (holidays, school terms), more cities with open feeds, a mobile app with push alerts,
and running on a YARN cluster with more executors.

## 12. References

1. data.gov.sg — Carpark Availability (HDB) API, `https://api.data.gov.sg/v1/transport/carpark-availability`.
2. data.gov.sg — HDB Carpark Information, dataset `d_23f946fa557947f93a8043bbef41dd09`.
3. "Contains information from Carpark Availability and HDB Carpark Information accessed on {bf.get('finished_sgt', 'TBD')[:10]} from data.gov.sg which is made available under the terms of the Singapore Open Data Licence version 1.0." (verify wording on data.gov.sg)
4. data.gov.sg API documentation and rate limits.
5. Apache Spark 3.5 documentation — RDD Programming Guide, Structured Streaming, MLlib.
6. Zaharia, M. et al. (2012). Resilient Distributed Datasets. NSDI'12.
7. Apache HBase Reference Guide (2.5) — schema design, REST and Thrift gateways.
8. pyproj — EPSG:3414 (SVY21) to EPSG:4326 transformation.
"""
(ROOT / "docs" / "report_draft.md").write_text(R)
print(f"wrote docs/report_draft.md ({len(R.split())} words; live eval {'yes' if live else 'TBD'})")
