# Fallbacks

| Failure | Fallback |
|---|---|
| HDFS down | set `paths.use_hdfs: false` in `config/config.yaml`; Scala and PySpark read `file://…/data/raw/flat` and write `data/processed` |
| HBase down | the poller, scorer and StreamingJob log the error and keep running (files + CSVs still written); restart with `scripts/02_start_services.sh` |
| HBase REST won't start ("Address already in use") | the REST info UI also defaults to 8085: start with `hbase-daemon.sh start rest -p 8085 --infoport 8086` |
| Spark → HBase classpath errors (NoSuchMethodError, Jackson/Guava) | pass only the shaded `hbase-shaded-mapreduce` + its small deps from `hbase mapredcp` (as `run_scala.sh` does); if it still fails, the Python HappyBase writers (`ingest/hbase_io.py`) cover every table |
| HBase unusable on review day | MongoDB 8.0 is installed in `~/tools/mongodb`: start `mongod --dbpath ~/mongo-data`, and write the same documents (one collection per HBase table, `_id` = row key) — not implemented by default because HBase is the primary design |
| API 429 / network | the backfill is resumable (re-run skips saved files); the poller retries with backoff; the demo runs from saved data |
| Empty snapshot at exactly 00:00:00 | the API returns `{"items": []}`; the backfill retries at +1 and +2 minutes |
