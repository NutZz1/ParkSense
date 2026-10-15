# Slides outline — official 12 slides

Figures are in `outputs/figures/`; numbers come from `outputs/metrics/` and `outputs/analytics/` (full 30-day run).

### 1. Title
* **ParkSense SG — Real-time Carpark Availability Analytics & "Will it be full in 30 minutes?"**
* Apache Spark (Scala RDD + PySpark ML) · HDFS · HBase · 23CSE352 Big Data Analytics, Review 3
* Team names / register numbers
* *Speaker:* "A live count tells you what is free now; we predict what will be free when you arrive."

### 2. Problem statement
* Circling for parking wastes time and fuel and adds congestion; availability changes minute by minute
* A live number answers "now", not "when I arrive in 30 minutes"
* Question: will carpark X be **nearly full (≤ 10% of car lots free) in 30 min**, and which hours/types are most at risk?
* Decisions: driver picks a carpark with space on arrival; planners find chronically full carparks
* Why Singapore: official, free, live + historical feed (no open Indian equivalent)

### 3. Dataset
* data.gov.sg Carpark Availability (HDB): ~2,020 carparks per snapshot, updated every minute; history via `date_time`
* 30 days × 96 snapshots = **2,880 snapshots, 5.8M flat lines** backfilled with **0 HTTP 429s**, plus the live stream
* HDB Carpark Information: 2,273 carparks, SVY21 → WGS84 (all inside Singapore)
* Attributes: total/available lots per lot type (C/Y/H), update time, carpark type, parking system, free/night parking
* Target: `full_30` (5.47% positive); data quality: 2.05% stale readings, 24 always-zero + 172 frozen sensors excluded
* Visual: `fig06_data_quality.png`

### 4. Workflow
* Visual: `fig01_architecture.png` — ingestion → HDFS → Scala RDD → Parquet → Spark SQL / PySpark ML → HBase; speed
  layer: poller (60 s) → StreamingJob (RDD per micro-batch) → live scoring → live verification

### 5. Scala + Spark implementation
* `sc.textFile(hdfs://…/raw/flat/*/*.txt)` → 2,880 partitions (one per small file) → `coalesce(128)`
* Transformations: map(parse) · filter · **flatMap** (lot types: 6.65M lines → 7.99M records) · filter(stale, total) ·
  map(clean) · keyBy · **leftOuterJoin** vs broadcast join · **reduceByKey** · aggregateByKey · combineByKey ·
  mapValues · distinct · sortBy · union (backfill + live archive)
* Actions: count · first · take · collect · **reduce** (busiest moment: 23 Sep 04:00 SGT, 50.5% of all car lots occupied) ·
  countByKey · countByValue · saveAsTextFile
* Accumulators: stale 126,479 · clipped 3,293 · total ≤ 0 13,586 · unmatched metadata 19,300
* Code snippets + console screenshot; rubric → file:line table in PROGRESS.md

### 6. Spark concepts
* Partitions: 1 → 4.8 s, 4 → 1.8 s, 32 → 4.0 s; `partitionBy` + same-partitioner reduceByKey = **0 B shuffle**
* Lineage/DAG: `occupancy_lineage.txt` + History Server DAG screenshot (narrow vs wide, stage boundaries)
* Persistence: 2nd action **6.9 s → 0.22 s** with cache(); MEMORY_ONLY_SER 269 MB vs 747 MB (`fig10`)
* Shuffle: reduceByKey **7.3 MB** vs groupByKey **50.6 MB**; broadcast join 0 B vs shuffle join 51.5 MB (`fig12`)
* Fault tolerance: partition 2 attempt 0 FAILED → attempt 1 SUCCESS, identical result; checkpoint 17 → 2 lineage lines

### 7. Data processing results
* `fig02_hour_weekday_heatmap.png`: weekday occupancy peaks at **04:00 (54.2%)** and is lowest at **15:00 (37.2%)** —
  residents park at home overnight; weekend afternoons stay fuller (47.0% at 15:00)
* `fig03_top20_full.png`: JB1 (Jalan Batu) nearly full 97.1% of the time, PL34 (Punggol Rd) 95.8%, UA2 95.5%
* Spark SQL Q3: surface carparks (electronic) nearly full 11.9% of readings vs basement 6.0%
* Q4: carparks *without* free or night parking are nearly full 22.6% of the evening vs 2.6% with both

### 8. PySpark ML
* Unit: carpark × 15-min slot (5.09M rows, 1,780 carparks); labels via self-join at slot + 2
* Features: availability now, lags 15/30/60/120 min, deltas, rolling 1 h/3 h, same time yesterday/last week, calendar,
  metadata, **train-only target encoding** (carpark × hour × weekday)
* Split by whole days: train 22 days (3.56M rows), validation 4, test 5; 30-min embargo
* Models: persistence, historical, **Random Forest** (selected), GBT; class weights; threshold 0.89 tuned on validation

### 9. Model evaluation
* Table (test): accuracy / precision / recall / F1 / PR-AUC
  - Persistence 0.987 / 0.873 / 0.874 / **0.873** / 0.778
  - Historical 0.973 / 0.754 / 0.733 / 0.743 / 0.783
  - **Random Forest** 0.985 / 0.855 / 0.866 / 0.860 / **0.943**
  - GBT 0.985 / 0.866 / 0.840 / 0.853 / 0.932
* Honest result: the model does **not** beat persistence on F1 — only 1.39% of rows change state in 30 min
* Transitions (10,757 test rows that change): persistence **0%** correct, model **31.8%** (40.0% of "becomes full")
* Visuals: `fig07_model_comparison.png`, `fig08_confusion_best.png`, `fig14_transitions_horizons.png`

### 10. Visualisation (≥ 3 charts)
* `fig02` heatmap · `fig04_type_hour_full_rate.png` · `fig05_map_evening_occupancy.png` · `fig09_feature_importance.png`
* System performance (`fig11_scaling.png`): 1 → 16 cores 3.0× faster; shuffle partitions 16 = 57.9 s vs default 200 =
  188.5 s; caching the features 4.3× faster across a 3-model loop

### 11. Results & discussion
* Chronic carparks and risky hours → pricing/signage targets (Top 20, Q3/Q4)
* 30-minute alert: strongest on the moments that matter — the transitions a live count cannot anticipate
* Live verification on unseen data: `fig13_live_eval.png` and `outputs/metrics/live_eval_summary.json`
* Speed layer: poll → micro-batch processed median 33 s; RDD micro-batch 0.26 s; live scoring of ~1,770 carparks ~13–25 s

### 12. Conclusion & future work
* Official live data → Scala RDD pipeline with measured Spark-concept evidence → leakage-safe ML evaluated honestly →
  live system on HDFS + HBase
* Limitations: one month, HDB carparks only, no holidays in the window, stale sensors
* Future: event/weather features, longer history, more cities, push alerts / mobile app, YARN cluster + Kafka
