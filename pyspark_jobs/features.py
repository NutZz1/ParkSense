"""Phase 6 — feature engineering. `build_features()` is the SAME code for training and live scoring.

Every reading is mapped to its 15-minute slot (slot = round(epoch_s / 900)). Backfill data is already on the
grid; live data (one poll per minute) keeps the latest reading per slot. All lags, seasonal lookbacks and
labels are built with **timestamp self-joins on the slot** (never row offsets), so gaps cannot misalign them,
and every feature uses information at or before t only.

Usage: python pyspark_jobs/features.py  ->  data/processed/features.parquet (lake) + outputs/metrics/features_summary.json
"""
import json
import time

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark_session import CFG, HOLIDAYS, get_logger, get_spark, lake, path

SLOT = 900  # seconds
LAGS = {"15": 1, "30": 2, "60": 4, "120": 8}
SEASONAL = {"1d": 96, "7d": 672}
T = CFG["thresholds"]

NUMERIC = (["avail_ratio", "occupancy", "full_now", "total_lots"]
           + [f"ratio_lag{k}" for k in LAGS] + ["delta_15", "delta_60", "roll_mean_1h", "roll_std_1h", "roll_mean_3h", "roll_std_3h"]
           + [f"ratio_{k}" for k in SEASONAL] + [f"has_{k}" for k in SEASONAL]
           + ["hour_sin", "hour_cos", "minute_of_day", "dow", "is_weekend", "is_holiday",
              "free_parking_flag", "night_parking_flag", "basement_flag", "car_park_decks", "te_chd", "te_ch"])
CATEGORICAL = ["car_park_type", "type_of_parking_system"]


def base_readings(spark, occupancy: DataFrame, excluded: DataFrame = None) -> DataFrame:
    """Car lots, total >= min_total_lots, reliable sensors only, one row per (carpark, slot)."""
    df = occupancy.filter((F.col("lot_type") == T["lot_type_ml"]) & (F.col("total_lots") >= T["min_total_lots"]))
    if excluded is not None:
        df = df.join(F.broadcast(excluded), "carpark_no", "left_anti")
    df = df.withColumn("slot", F.round(F.col("snapshot_ms") / 1000 / SLOT).cast("long"))
    w = Window.partitionBy("carpark_no", "slot").orderBy(F.desc("snapshot_ms"))
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return df.select("carpark_no", "slot", "snapshot_ms", "avail_ratio", "occupancy", "total_lots", "lots_available",
                     "car_park_type", "type_of_parking_system", "free_parking", "night_parking", "car_park_basement",
                     "car_park_decks")


def build_features(spark, readings: DataFrame, horizons=(30,), with_label=True, te_chd: DataFrame = None,
                   te_ch: DataFrame = None, te_global: float = None, counts=None) -> DataFrame:
    """readings = base_readings(...). te_* = target-encoding tables computed on the TRAIN period only
    (None while building the training table: they are joined later by train.py via add_target_encoding)."""
    counts = counts if counts is not None else {}
    r = readings.cache()
    counts["slot_rows"] = r.count()
    thin = r.select("carpark_no", "slot", "avail_ratio")

    # ---- time features from the slot (grid) time in SGT
    ts = F.to_timestamp(F.from_unixtime(F.col("slot") * SLOT))  # rendered in session tz = Asia/Singapore
    df = (r.withColumn("ts", ts)
          .withColumn("hour", F.hour("ts"))
          .withColumn("minute_of_day", (F.hour("ts") * 60 + F.minute("ts")).cast("double"))
          .withColumn("dow", ((F.dayofweek("ts") + 5) % 7 + 1).cast("double"))  # 1 = Monday
          .withColumn("is_weekend", (F.col("dow") >= 6).cast("double"))
          .withColumn("is_holiday", F.date_format("ts", "yyyy-MM-dd").isin(HOLIDAYS).cast("double"))
          .withColumn("dt", F.date_format("ts", "yyyy-MM-dd"))
          .withColumn("hour_sin", F.sin(2 * 3.141592653589793 * F.col("minute_of_day") / 1440))
          .withColumn("hour_cos", F.cos(2 * 3.141592653589793 * F.col("minute_of_day") / 1440))
          .withColumn("full_now", (F.col("avail_ratio") <= T["full_ratio"]).cast("double"))
          .withColumn("free_parking_flag", (F.coalesce(F.col("free_parking"), F.lit("NO")) != "NO").cast("double"))
          .withColumn("night_parking_flag", F.coalesce((F.col("night_parking") == "YES").cast("double"), F.lit(0.0)))
          .withColumn("basement_flag", F.coalesce((F.col("car_park_basement") == "Y").cast("double"), F.lit(0.0)))
          .withColumn("car_park_decks", F.coalesce(F.col("car_park_decks").cast("double"), F.lit(0.0)))
          .withColumn("total_lots", F.col("total_lots").cast("double"))
          .fillna({"car_park_type": "UNKNOWN", "type_of_parking_system": "UNKNOWN"}))

    # ---- lags (t - 15/30/60/120 min) via self-join on slot; missing lag -> current value
    for k, s in LAGS.items():
        lag = thin.select("carpark_no", (F.col("slot") + s).alias("slot"), F.col("avail_ratio").alias(f"ratio_lag{k}"))
        df = df.join(lag, ["carpark_no", "slot"], "left").withColumn(f"ratio_lag{k}", F.coalesce(f"ratio_lag{k}", "avail_ratio"))
    # ---- same time yesterday / last week; missing -> -1 with a has_* flag (trees split on the sentinel)
    for k, s in SEASONAL.items():
        sea = thin.select("carpark_no", (F.col("slot") + s).alias("slot"), F.col("avail_ratio").alias(f"ratio_{k}"))
        df = df.join(sea, ["carpark_no", "slot"], "left") \
               .withColumn(f"has_{k}", F.col(f"ratio_{k}").isNotNull().cast("double")) \
               .withColumn(f"ratio_{k}", F.coalesce(f"ratio_{k}", F.lit(-1.0)))
    df = df.withColumn("delta_15", F.col("avail_ratio") - F.col("ratio_lag15")) \
           .withColumn("delta_60", F.col("avail_ratio") - F.col("ratio_lag60"))
    # ---- rolling mean/std over the past 1 h / 3 h (rangeBetween on slots, current included)
    for name, n in (("1h", 3), ("3h", 11)):
        w = Window.partitionBy("carpark_no").orderBy("slot").rangeBetween(-n, 0)
        df = df.withColumn(f"roll_mean_{name}", F.avg("avail_ratio").over(w)) \
               .withColumn(f"roll_std_{name}", F.coalesce(F.stddev_samp("avail_ratio").over(w), F.lit(0.0)))

    # ---- labels: nearly full at t + h (self-join on slot + h/15)
    if with_label:
        for h in horizons:
            fut = thin.select("carpark_no", (F.col("slot") - h // 15).alias("slot"), F.col("avail_ratio").alias(f"future_ratio_{h}"))
            df = df.join(fut, ["carpark_no", "slot"], "left") \
                   .withColumn(f"label_{h}", (F.col(f"future_ratio_{h}") <= T["full_ratio"]).cast("double"))
    if te_chd is not None:
        df = add_target_encoding(df, te_chd, te_ch, te_global)
    return df


def target_encoding(train: DataFrame, label_col: str, m: float = 5.0):
    """Leakage-safe target encoding from the TRAIN period only: smoothed full-rate per (carpark, hour, dow),
    shrunk towards the per-(carpark, hour) rate, which is shrunk towards the global rate (m-estimate)."""
    g = train.agg(F.avg(label_col)).first()[0]
    ch = train.groupBy("carpark_no", "hour").agg(F.sum(label_col).alias("s"), F.count("*").alias("n")) \
        .select("carpark_no", "hour", ((F.col("s") + m * g) / (F.col("n") + m)).alias("te_ch"))
    chd = train.groupBy("carpark_no", "hour", "dow").agg(F.sum(label_col).alias("s"), F.count("*").alias("n")) \
        .join(ch, ["carpark_no", "hour"]) \
        .select("carpark_no", "hour", "dow", ((F.col("s") + m * F.col("te_ch")) / (F.col("n") + m)).alias("te_chd"))
    return chd, ch, float(g)


def add_target_encoding(df, te_chd, te_ch, g):
    return df.join(F.broadcast(te_ch), ["carpark_no", "hour"], "left") \
             .join(F.broadcast(te_chd), ["carpark_no", "hour", "dow"], "left") \
             .withColumn("te_ch", F.coalesce("te_ch", F.lit(g))) \
             .withColumn("te_chd", F.coalesce("te_chd", "te_ch"))


if __name__ == "__main__":
    log = get_logger("features")
    t0 = time.time()
    spark = get_spark("ParkSense-Features", shuffle_partitions=32)
    horizons = sorted(set(T["horizons_sensitivity"] + [T["horizon_min"]]))
    occ = spark.read.parquet(lake("processed/occupancy"))
    excl = spark.read.parquet(lake("processed/excluded_carparks"))
    counts = {"occupancy_rows": occ.count(), "car_rows": occ.filter(F.col("lot_type") == "C").count(),
              "excluded_carparks": excl.count()}
    base = base_readings(spark, occ, excl)
    feats = build_features(spark, base, horizons=horizons, counts=counts)
    out = lake("processed/features")
    feats.write.mode("overwrite").partitionBy("dt").parquet(out)
    f = spark.read.parquet(out)
    counts["feature_rows"] = f.count()
    counts["carparks"] = f.select("carpark_no").distinct().count()
    for h in horizons:
        lab = f.filter(F.col(f"label_{h}").isNotNull())
        counts[f"rows_with_label_{h}"] = lab.count()
        counts[f"positive_rate_{h}"] = round(lab.agg(F.avg(f"label_{h}")).first()[0], 4)
    counts["dropped_no_future_30"] = counts["feature_rows"] - counts["rows_with_label_30"]
    summary = {"horizons_min": horizons, "full_ratio": T["full_ratio"], "numeric_features": NUMERIC,
               "categorical_features": CATEGORICAL, "filters": {"lot_type": T["lot_type_ml"], "min_total_lots": T["min_total_lots"],
                                                                 "excluded": "always-zero / never-changing carparks (RddPipeline)"},
               "counts": counts, "elapsed_s": round(time.time() - t0, 1)}
    with open(path("metrics") / "features_summary.json", "w") as fh:
        json.dump(summary, fh, indent=2)
    print("\n=== features summary ===")
    for k, v in counts.items():
        print(f"  {k:<26} {v}")
    print(f"  {len(NUMERIC)} numeric + {len(CATEGORICAL)} categorical features; elapsed {time.time() - t0:.0f}s -> {out}")
    spark.stop()
