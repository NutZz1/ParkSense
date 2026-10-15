"""Push model / analytics / Spark metrics to HBase parksense:metrics as JSON (for the UI; design-independent).

Rows: model:comparison, model:confusion, model:feature_importance, model:transitions, model:horizons, model:best,
model:slices, spark:rdd_summary, spark:cache, spark:partitions, spark:shuffle, spark:perf, insights:top20,
insights:type_hour, insights:daily, profile:<carpark> (mean occupancy by hour, weekday/weekend).
Usage: python pyspark_jobs/publish_metrics.py
"""
import json
import sys

import pandas as pd

from spark_session import ROOT, get_logger, get_spark, lake, path

sys.path.insert(0, str(ROOT / "ingest"))
import hbase_io  # noqa: E402

log = get_logger("publish_metrics")
M, MD = path("metrics"), path("models")
rec = lambda f: json.loads(pd.read_csv(f).to_json(orient="records"))  # noqa: E731
n = 0


def put(name, obj):
    global n
    hbase_io.put_metric(name, obj)
    n += 1


for name, f in (("model:comparison", "model_comparison.csv"), ("model:feature_importance", "feature_importance.csv"),
                ("model:transitions", "transition_metrics.csv"), ("model:horizons", "horizon_sensitivity.csv"),
                ("model:slice_type", "slice_by_type.csv"), ("model:slice_hour", "slice_by_hour.csv"),
                ("spark:rdd_summary", "rdd_summary.csv"), ("spark:cache", "concepts_cache.csv"),
                ("spark:partitions", "concepts_partitions.csv"), ("spark:shuffle", "concepts_shuffle.csv"),
                ("spark:perf", "perf_system.csv"), ("insights:top20", "rdd_top20_full.csv"),
                ("insights:type_hour", "rdd_type_hour.csv"), ("insights:daily", "rdd_daily_full_rate.csv")):
    if (M / f).exists():
        put(name, rec(M / f))
conf = {p.stem.replace("confusion_", ""): json.loads(pd.read_csv(p, index_col=0).to_json()) for p in M.glob("confusion_*.csv")}
if conf:
    put("model:confusion", conf)
if (MD / "best_model_meta.json").exists():
    m = json.load(open(MD / "best_model_meta.json"))
    put("model:best", {k: m[k] for k in ("algorithm", "params", "threshold", "model_version", "horizon_min", "full_ratio", "label", "split")})

# per-carpark "best time to arrive" profiles from the Scala aggregate (carpark x hour x dow)
spark = get_spark("ParkSense-Publish", master="local[4]")
prof = spark.read.parquet(lake("processed/agg_carpark_hour_dow")).toPandas()
spark.stop()
prof["day_type"] = prof.dow.map(lambda d: "weekend" if d >= 6 else "weekday")
for cp, g in prof.groupby("carpark_no"):
    p = g.groupby(["day_type", "hour"]).apply(lambda x: (x.mean_occupancy * x.records).sum() / x.records.sum(), include_groups=False)
    put(f"profile:{cp}", {dt: {int(h): round(float(v), 4) for (d, h), v in p.items() if d == dt} for dt in ("weekday", "weekend")})
print(f"published {n} metric rows to parksense:metrics")
