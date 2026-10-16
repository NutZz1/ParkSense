# ParkSense SG

Real-time carpark availability analytics and a **"will it be full in 30 minutes?"** prediction for ~2,000 Singapore
HDB carparks. Apache Spark (Scala RDD + PySpark ML) · HDFS · HBase · (Express + React UI). Course: 23CSE352 Big Data Analytics — Project Review 3.

Data: data.gov.sg Carpark Availability and HDB Carpark Information, Singapore Open Data Licence v1.0.

## Layout

| Path | What |
|---|---|
| `config/config.yaml` | every endpoint, path, threshold, interval and port |
| `ingest/` | `download_metadata.py`, `backfill_history.py`, `flatten.py`, `live_poller.py`, `hbase_io.py`, `common.py` |
| `spark-scala/` | `Records`, `RddPipeline` (core RDD processing), `SparkConcepts`, `StreamingJob`, `HBaseWriter`, `util/` |
| `hbase/create_tables.hbase` | namespace + 4 tables (row keys and families explained in `docs/architecture.md`) |
| `pyspark_jobs/` | `analytics.py` (8 Spark SQL queries), `eda.py`, `features.py`, `train.py`, `evaluate.py`, `perf_system.py`, `score_live.py`, `verify_live.py`, `publish_metrics.py` |
| `viz/` | `make_figures.py` (figures), `fill_report.py` (report from outputs) |
| `docs/` | analysis question, data dictionary, data-quality report, architecture, DAG explanation, fallbacks, report draft, slides, demo, viva |
| `outputs/` | metrics, analytics, figures, lineage, logs, Spark event logs — every number in the docs comes from here |

## Setup (once, as hduser)

```bash
cd /home/hduser/parksense-sg
scripts/01_setup_python.sh          # venv ~/parksense-venv
source scripts/env.sh               # Java 11 + Spark 3.5.9 (Scala 2.12.18), Hadoop, HBase, Node, venv
scripts/02_start_services.sh        # HDFS, HBase, Thrift :9090, REST :8085 (info UI :8086)
scripts/00_check_env.sh             # outputs/logs/env.txt + compat.md
scripts/04_hbase_create_tables.sh   # namespace parksense + 4 tables
cp -n .env.example .env             # optional DATAGOV_API_KEY
```

## Data

```bash
python ingest/download_metadata.py
nohup python ingest/backfill_history.py --days 30 > outputs/logs/backfill.out 2>&1 &   # ~2 h, resumable
scripts/start_live.sh poller        # 60 s poller -> stream files + HBase
scripts/03_hdfs_layout.sh           # upload flat files + metadata (re-runnable; --archive adds live files)
```

## Batch (Phases 2–7, 10)

```bash
scripts/run_batch.sh                # services -> HDFS -> RddPipeline -> SparkConcepts -> analytics -> features -> EDA
                                    #   -> train -> evaluate -> perf -> publish -> figures -> report
scripts/run_batch.sh --skip-perf    # faster
scripts/run_scala.sh RddPipeline    # individual Scala jobs
scripts/run_scala.sh SparkConcepts "local[4,3]"
scripts/start_history_server.sh     # http://localhost:18080 for DAG screenshots
```

## Live layer

```bash
scripts/start_live.sh               # poller + StreamingJob + score_live (5 min) + verify_live (5 min)
scripts/stop_live.sh
scripts/watchdog.sh start      # auto-restarts HDFS/HBase/Thrift/REST and the live jobs if they fail (log: outputs/logs/watchdog.log)
python pyspark_jobs/verify_live.py && python viz/make_figures.py && python viz/fill_report.py   # refresh live results
```

## UI

```bash
scripts/build_ui.sh              # once (npm install + vite build)
scripts/start_live.sh ui         # http://localhost:5000  (deep link: http://localhost:5000/#carpark/AM20)
```

The **Assistant** tab answers questions from the live HBase data. It needs `ANTHROPIC_API_KEY` in `.env` (without it, it
gives template answers). Design: `docs/chatbot.md`.

`PROGRESS.md` is the phase log; `docs/demo_script.md` is the review demo.
