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

# catalogue of the Scala RDD key-value analytics (RddPipeline.scala): question, RDD operations, result rows
RDD_CATALOGUE = [
    ("rdd_summary.csv", "Pipeline summary: lines read, records after flatMap, accumulator counts, partitions, runtime",
     "textFile → map(parse) → filter → flatMap(explodeLots) → filter(stale, total) → map(clean) + accumulators", None),
    ("rdd_top20_full.csv", "Top 20 carparks most often nearly full (always-zero / frozen sensors excluded)",
     "map((carpark, record)) → combineByKey(min, max, mean, full-rate) → filter → sortBy(-full_rate) → take(20)", None),
    ("rdd_type_hour.csv", "Mean occupancy and nearly-full rate by carpark type × hour × weekend",
     "map(((type, hour, weekend), (occ, 1, full))) → reduceByKey → mapValues(mean, rate) → collect", None),
    ("rdd_daily_full_rate.csv", "Daily citywide nearly-full rate",
     "map((dt, (full, 1, occ))) → reduceByKey → mapValues → sortByKey → collect", None),
    ("rdd_carpark_stats.csv", "Per-carpark min/max available, mean occupancy, full rate, zero share (one pass)",
     "combineByKey(createCombiner, mergeValue, mergeCombiners) → mapValues → collect", "full_rate"),
    ("rdd_stale_rate_carpark.csv", "Carparks with the most stale readings (> 30 min since the sensor update)",
     "filter(lot C) → map((carpark, (stale, 1))) → reduceByKey → mapValues(rate) → sortBy(-rate)", "stale_rate"),
    ("rdd_excluded_carparks.csv", "Carparks excluded from ML as unreliable sensors",
     "combineByKey results → filter(zero_share > 0.95 or min == max)", None),
]
def bands(series, edges, labels):
    """Pie data: how many carparks fall in each band of a rate."""
    b = pd.cut(series, edges, labels=labels, include_lowest=True).value_counts().reindex(labels).fillna(0)
    return [{"name": k, "value": int(v)} for k, v in b.items()]


cat = []
for f, question, ops, sort_col in RDD_CATALOGUE:
    if (M / f).exists():
        df = pd.read_csv(M / f)
        if sort_col:
            df = df.sort_values(sort_col, ascending=False)
        entry = {"name": f.replace(".csv", ""), "question": question, "rdd_ops": ops, "source": f"outputs/metrics/{f}",
                 "row_count": len(df), "rows": json.loads(df.head(500 if len(df) <= 500 else 30).to_json(orient="records"))}
        # chart data computed over ALL rows (the table shows only the top rows of large results)
        if f == "rdd_carpark_stats.csv":
            entry["pie"] = bands(df.full_rate, [0, 0.01, 0.05, 0.2, 0.5, 1], ["< 1% of time", "1–5%", "5–20%", "20–50%", "> 50%"])
            entry["pie_title"] = "Carparks by how often they are nearly full"
        elif f == "rdd_stale_rate_carpark.csv":
            entry["pie"] = bands(df.stale_rate, [0, 0.001, 0.01, 0.05, 0.2, 1], ["< 0.1%", "0.1–1%", "1–5%", "5–20%", "> 20%"])
            entry["pie_title"] = "Carparks by share of stale readings"
        elif f == "rdd_excluded_carparks.csv":
            entry["pie"] = [{"name": k.replace("_", " "), "value": int(v)} for k, v in df.reason.value_counts().items()]
            entry["pie_title"] = "Why carparks were excluded from ML"
        cat.append(entry)
put("analytics:rdd", cat)

# per-carpark "best time to arrive" profiles from the Scala aggregate (carpark x hour x dow)
spark = get_spark("ParkSense-Publish", master="local[4]")
prof = spark.read.parquet(lake("processed/agg_carpark_hour_dow")).toPandas()
spark.stop()
prof["day_type"] = prof.dow.map(lambda d: "weekend" if d >= 6 else "weekday")
for cp, g in prof.groupby("carpark_no"):
    p = g.groupby(["day_type", "hour"]).apply(lambda x: (x.mean_occupancy * x.records).sum() / x.records.sum(), include_groups=False)
    put(f"profile:{cp}", {dt: {int(h): round(float(v), 4) for (d, h), v in p.items() if d == dt} for dt in ("weekday", "weekend")})
print(f"published {n} metric rows to parksense:metrics")
