"""Live scoring: P(nearly full in 30 min) for every carpark at the latest 15-min slot.

History = processed Parquet (last 8 days, so the t-7d features exist) + the live archive lines parsed with the
same rules as the Scala pipeline; then the SAME build_features() as training, the train-period target
encoding, models/best_model and its threshold. Writes HBase carpark_live:p + predictions_log and
outputs/metrics/live_predictions.csv.

Usage: python pyspark_jobs/score_live.py [--loop 300]
"""
import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from features import add_target_encoding, base_readings, build_features
from spark_session import CFG, ROOT, get_logger, get_spark, lake, path

ap = argparse.ArgumentParser()
ap.add_argument("--loop", type=int, default=0, help="seconds between runs (0 = once)")
args = ap.parse_args()
log = get_logger("score_live")
sys.path.insert(0, str(ROOT / "ingest"))
spark = get_spark("ParkSense-ScoreLive", master="local[4]", shuffle_partitions=8,
                  extra={"spark.ui.port": "4060", "spark.eventLog.enabled": "false", "spark.driver.memory": "1500m"})
MD = path("models")
meta = json.load(open(MD / "best_model_meta.json"))
model = PipelineModel.load(f"file://{MD / 'best_model'}")
te_chd = spark.read.parquet(f"file://{meta['te_tables']['chd']}").cache()
te_ch = spark.read.parquet(f"file://{meta['te_tables']['ch']}").cache()
h, thr, T = meta["horizon_min"], meta["threshold"], CFG["thresholds"]
info = spark.read.option("header", True).option("inferSchema", True).csv(f"file://{path('metadata_csv')}") \
    .select(F.col("car_park_no").alias("carpark_no"), "car_park_type", "type_of_parking_system", "free_parking",
            "night_parking", "car_park_basement", F.col("car_park_decks").cast("int").alias("car_park_decks"))
excluded = spark.read.parquet(lake("processed/excluded_carparks")).cache()
out_csv = path("metrics") / "live_predictions.csv"


def live_archive(since_ms):
    """Live poll lines -> occupancy-like rows (same rules as RddPipeline: C lots, total>0, stale<=30, clip)."""
    glob = f"file://{path('stream_archive')}/*/*.txt"
    try:
        raw = spark.read.text(glob)
    except Exception:  # noqa: BLE001 - no archive yet
        return None
    p = F.split("value", "\t")
    df = raw.select(p[0].alias("snap"), p[1].alias("carpark_no"), p[2].alias("upd"), F.explode(F.split(p[3], r"\|")).alias("lot"))
    lp = F.split("lot", ":")
    df = (df.select("carpark_no", lp[0].alias("lot_type"), lp[1].cast("int").alias("total_lots"), lp[2].cast("int").alias("avail"),
                    (F.unix_timestamp(F.to_timestamp("snap")) * 1000).alias("snapshot_ms"),
                    ((F.unix_timestamp(F.to_timestamp("snap")) - F.unix_timestamp(F.to_timestamp("upd"))) / 60).alias("stale_min"))
          .filter((F.col("total_lots") > 0) & (F.col("stale_min") <= T["stale_max_min"]) & (F.col("snapshot_ms") >= since_ms))
          .withColumn("lots_available", F.least(F.greatest("avail", F.lit(0)), "total_lots"))
          .withColumn("avail_ratio", F.col("lots_available") / F.col("total_lots"))
          .withColumn("occupancy", 1 - F.col("avail_ratio")))
    return df.join(info, "carpark_no", "left")


def score_once():
    t0 = time.time()
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=8, hours=1)
    since_ms = int(since.timestamp() * 1000)
    cols = ["carpark_no", "lot_type", "snapshot_ms", "avail_ratio", "occupancy", "total_lots", "lots_available", "car_park_type",
            "type_of_parking_system", "free_parking", "night_parking", "car_park_basement", "car_park_decks"]
    hist = spark.read.parquet(lake("processed/occupancy")).filter(F.col("dt") >= since.strftime("%Y-%m-%d")) \
        .filter(F.col("snapshot_ms") >= since_ms).select(*cols)
    live = live_archive(since_ms)
    occ = hist.unionByName(live.select(*cols)) if live is not None else hist
    feats = build_features(spark, base_readings(spark, occ, excluded), with_label=False)
    last_slot = feats.agg(F.max("slot")).first()[0]
    cur = add_target_encoding(feats.filter(F.col("slot") == last_slot), te_chd, te_ch, meta["te_tables"]["global"])
    sc = model.transform(cur).withColumn("p", vector_to_array("probability")[1]) \
        .select("carpark_no", "slot", "snapshot_ms", "avail_ratio", "p").toPandas()
    made = datetime.now(timezone.utc)
    t_slot = datetime.fromtimestamp(last_slot * 900, timezone.utc)
    # target = each carpark's own reading time + horizon (the slot is only the nearest 15-min grid point)
    tgt = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc) + timedelta(minutes=h)  # noqa: E731
    rows = [{"carpark": r.carpark_no, "prob": round(float(r.p), 4), "pred": bool(r.p >= thr), "made_at": made.isoformat(),
             "t_slot": t_slot.isoformat(), "target_at": tgt(r.snapshot_ms).isoformat(), "target_ms": int(r.snapshot_ms) + h * 60000,
             "threshold": thr, "current_avail_ratio": round(float(r.avail_ratio), 4),
             "reading_age_s": round((made.timestamp() * 1000 - r.snapshot_ms) / 1000, 1)} for r in sc.itertuples()]
    try:
        import hbase_io
        hbase_io.put_predictions(rows, meta["model_version"])
    except Exception as e:  # noqa: BLE001
        log.warning("HBase write failed: %s", e)
    df = pd.DataFrame(rows).assign(model_version=meta["model_version"], latency_s=round(time.time() - t0, 1))
    if out_csv.exists():
        df = pd.concat([pd.read_csv(out_csv), df], ignore_index=True).drop_duplicates(["carpark", "target_ms"], keep="last")
    df.to_csv(out_csv, index=False)
    log.info("scored %d carparks for slot %s (targets = reading time + %d min): %d predicted nearly full; %.1fs", len(rows),
             t_slot.isoformat(), h, sum(r["pred"] for r in rows), time.time() - t0)


failures = 0
while True:
    try:
        score_once()
        failures = 0
    except Exception as e:  # noqa: BLE001 - keep the loop alive
        failures += 1
        log.exception("scoring failed (%d in a row): %s", failures, e)
        if failures >= 3:  # e.g. the Spark JVM died: exit so the watchdog restarts a fresh process
            log.error("exiting after %d consecutive failures", failures)
            sys.exit(1)
    if not args.loop:
        break
    time.sleep(args.loop)
spark.stop()
