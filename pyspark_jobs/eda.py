"""Phase 5 — EDA & data quality (PySpark aggregation, matplotlib figures).

-> outputs/metrics/eda_*.csv, outputs/figures/eda_*.png, docs/data_quality_report.md (numbers from this run only)
Usage: python pyspark_jobs/eda.py
"""
import json
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark_session import CFG, HOLIDAYS, ROOT, get_logger, get_spark, lake, path  # noqa: E402

log = get_logger("eda")
M, FIG = path("metrics"), path("figures")
T = CFG["thresholds"]
t0 = time.time()
spark = get_spark("ParkSense-EDA", shuffle_partitions=32)
R = {}


def save(df, name):
    df.to_csv(M / f"eda_{name}.csv", index=False)


def fig(name):
    plt.tight_layout()
    plt.savefig(FIG / f"eda_{name}.png", dpi=150)
    plt.close()


occ = spark.read.parquet(lake("processed/occupancy"))
car = occ.filter(F.col("lot_type") == "C").cache()
excl = spark.read.parquet(lake("processed/excluded_carparks"))
feats = spark.read.parquet(lake("processed/features"))
scala = pd.read_csv(M / "rdd_summary.csv").set_index("metric")["value"]
bmeta = json.loads((ROOT / "data" / "raw" / "backfill_meta.json").read_text()) if (ROOT / "data/raw/backfill_meta.json").exists() else {}

# 1. carparks per snapshot over time
cps = car.groupBy("snapshot_ms").agg(F.countDistinct("carpark_no").alias("carparks")).toPandas().sort_values("snapshot_ms")
cps["ts"] = pd.to_datetime(cps.snapshot_ms, unit="ms").dt.tz_localize("UTC").dt.tz_convert("Asia/Singapore")
save(cps[["ts", "carparks"]], "carparks_per_snapshot")
R["carparks_per_snapshot"] = {"min": int(cps.carparks.min()), "median": float(cps.carparks.median()), "max": int(cps.carparks.max()),
                              "snapshots": len(cps)}
plt.figure(figsize=(12, 3.4))
plt.plot(cps.ts, cps.carparks, lw=0.8)
plt.title("Carparks with a clean car-lot reading per snapshot")
plt.ylabel("carparks")
fig("carparks_per_snapshot")

# 2. staleness distribution (raw lines, before the stale filter)
raw = spark.read.text(lake("raw/flat/*/*.txt"))
p = F.split("value", "\t")
st = raw.select(((F.unix_timestamp(F.to_timestamp(p[0])) - F.unix_timestamp(F.to_timestamp(p[2]))) / 60).alias("m"),
                F.substring(p[0], 1, 10).alias("dt"))
bins = F.when(F.col("m") < 0, "<0 (clock skew)").when(F.col("m") <= 5, "0-5 min").when(F.col("m") <= 15, "5-15 min") \
    .when(F.col("m") <= 30, "15-30 min").when(F.col("m") <= 60, "30-60 min").when(F.col("m") <= 360, "1-6 h") \
    .when(F.col("m") <= 1440, "6-24 h").otherwise("> 1 day")
sd = st.groupBy(bins.alias("staleness")).count().toPandas()
order = ["<0 (clock skew)", "0-5 min", "5-15 min", "15-30 min", "30-60 min", "1-6 h", "6-24 h", "> 1 day"]
sd = sd.set_index("staleness").reindex(order).fillna(0).reset_index()
sd["share"] = sd["count"] / sd["count"].sum()
save(sd, "staleness_distribution")
stale_day = st.groupBy("dt").agg(F.avg((F.col("m") > T["stale_max_min"]).cast("int")).alias("stale_rate")).toPandas().sort_values("dt")
save(stale_day, "stale_rate_per_day")
R["stale_share"] = float(sd[sd.staleness.isin(["30-60 min", "1-6 h", "6-24 h", "> 1 day"])]["share"].sum())
R["staleness_median_min"] = float(st.approxQuantile("m", [0.5], 0.001)[0])
fig_, ax = plt.subplots(1, 2, figsize=(13, 3.8))
ax[0].bar(sd.staleness, 100 * sd.share, color="#4c72b0")
ax[0].set(title="Staleness = snapshot time − sensor update time", ylabel="% of carpark readings")
ax[0].tick_params(axis="x", rotation=35)
ax[1].plot(pd.to_datetime(stale_day.dt), 100 * stale_day.stale_rate, "o-")
ax[1].set(title=f"Share of readings older than {T['stale_max_min']} min, per day", ylabel="%")
fig("staleness")

# 3. lot-type mix, excluded carparks, clipping
lt = occ.groupBy("lot_type").agg(F.count("*").alias("records"), F.countDistinct("carpark_no").alias("carparks")).toPandas()
save(lt, "lot_type_mix")
ex = pd.read_csv(M / "rdd_excluded_carparks.csv")
R["excluded"] = ex.reason.value_counts().to_dict()
R["clipped"] = int(scala["clipped_available"])

# 4. occupancy distribution + hour x weekday heatmaps (reliable carparks)
good = car.join(F.broadcast(excl), "carpark_no", "left_anti")
hist = good.select((F.floor(F.col("occupancy") * 20) / 20).alias("b")).groupBy("b").count().toPandas().sort_values("b")
save(hist, "occupancy_histogram")
hm = good.groupBy("dow", "hour").agg(F.avg("occupancy").alias("occ"), F.avg((F.col("avail_ratio") <= T["full_ratio"]).cast("int")).alias("full")).toPandas()
save(hm, "heatmap_dow_hour")
fig_, ax = plt.subplots(1, 2, figsize=(15, 4.2))
ax[0].bar(hist.b + 0.025, hist["count"], width=0.045)
ax[0].set(title="Car-lot occupancy distribution (all readings)", xlabel="occupancy (1 − available/total)", ylabel="readings")
piv = hm.pivot(index="dow", columns="hour", values="occ")
piv.index = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][: len(piv)]
sns.heatmap(piv, cmap="YlOrRd", ax=ax[1], cbar_kws={"label": "mean occupancy"})
ax[1].set(title="Mean occupancy by weekday × hour (SGT)", xlabel="hour", ylabel="")
fig("occupancy")
tp = good.groupBy(F.coalesce("car_park_type", F.lit("UNKNOWN")).alias("type"), "hour").agg(F.avg("occupancy").alias("occ")).toPandas()
save(tp, "type_hour")
plt.figure(figsize=(11, 4.2))
for t_, g in tp.groupby("type"):
    g = g.sort_values("hour")
    plt.plot(g.hour, g.occ, marker=".", label=t_)
plt.legend(fontsize=7)
plt.title("Mean occupancy by hour and carpark type")
plt.xlabel("hour (SGT)")
plt.ylabel("mean occupancy")
fig("type_hour")

# evening (18-22 h) mean occupancy per carpark with coordinates (map figure)
ev = good.filter(F.col("hour").between(18, 22)).groupBy("carpark_no").agg(
    F.avg("occupancy").alias("evening_occupancy"), F.first("lat").alias("lat"), F.first("lon").alias("lon"),
    F.first("car_park_type").alias("car_park_type")).toPandas()
save(ev.dropna(subset=["lat"]).round(5), "evening_occupancy_carpark")

# 5. weekday vs weekend, holidays
wk = good.groupBy("is_weekend", "hour").agg(F.avg("occupancy").alias("occ")).toPandas()
save(wk, "weekday_weekend")
days = sorted(str(r[0]) for r in occ.select("dt").distinct().collect())  # partition discovery types dt as date
hol_in = [d for d in HOLIDAYS if days and days[0] <= d <= days[-1]]
R["days"] = {"first": days[0], "last": days[-1], "n": len(days)}
R["holidays_in_range"] = hol_in

# 6. metadata effects
me = good.groupBy("free_parking", "night_parking", "type_of_parking_system").agg(
    F.countDistinct("carpark_no").alias("carparks"), F.avg("occupancy").alias("mean_occ"),
    F.avg((F.col("avail_ratio") <= T["full_ratio"]).cast("int")).alias("full_rate")).toPandas()
save(me.round(4), "metadata_effects")

# 7. class balance + 8. persistence (how often the state changes within the horizon)
lab = feats.filter(F.col("label_30").isNotNull())
cb = lab.agg(F.avg("label_30").alias("pos"), F.count("*").alias("n"),
             F.avg((F.col("label_30") != F.col("full_now")).cast("int")).alias("change")).first()
R["class_balance_30"] = {"positive_rate": round(cb["pos"], 4), "rows": cb["n"], "state_change_rate_30min": round(cb["change"], 4)}
for col, name in (("car_park_type", "class_balance_by_type"), ("hour", "class_balance_by_hour")):
    d = lab.groupBy(col).agg(F.avg("label_30").alias("positive_rate"), F.avg((F.col("label_30") != F.col("full_now")).cast("int")).alias("change_rate"),
                             F.count("*").alias("rows")).toPandas().sort_values(col)
    save(d.round(4), name)
for hz in T["horizons_sensitivity"]:
    c = feats.filter(F.col(f"label_{hz}").isNotNull()).agg(F.avg((F.col(f"label_{hz}") != F.col("full_now")).cast("int"))).first()[0]
    R.setdefault("state_change_rate_by_horizon", {})[f"{hz}min"] = round(c, 4)
pos = R["class_balance_30"]["positive_rate"]
R["full_ratio_decision"] = (f"keep full_ratio = {T['full_ratio']} (positives {pos:.2%} >= 3%)" if pos >= 0.03 else
                            f"positives {pos:.2%} < 3%: propose full_ratio 0.15 — needs user confirmation")
(M / "eda_summary.json").write_text(json.dumps(R, indent=2, default=str))

# ---- data-quality report
bm = bmeta.get("carparks_per_snapshot", {})
md = f"""# Data quality report

_Generated by `pyspark_jobs/eda.py` on {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}; every number comes from `outputs/metrics/` or `data/raw/backfill_meta.json`._

## Sources and volume

| Item | Value |
|---|---|
| Availability snapshots (backfill grid, 15 min) | {bmeta.get('requested', 'TBD')} requested, {bmeta.get('saved_now', 0) + bmeta.get('skipped_existing', 0)} saved, {bmeta.get('failed', 'TBD')} failed |
| Carparks per snapshot (API) | min {bm.get('min', 'TBD')}, median {bm.get('median', 'TBD')}, max {bm.get('max', 'TBD')} |
| HTTP 429 during backfill | {bmeta.get('http_429', 'TBD')} |
| Days covered | {R['days']['first']} → {R['days']['last']} ({R['days']['n']} days) |
| Flat lines read by Scala | {int(scala['input_lines']):,} |
| Lot records after flatMap | {int(scala['lot_records_exploded']):,} |
| Clean records (Parquet) | {int(scala['clean_records']):,} (car lots {int(scala['car_lot_records']):,}) |
| Metadata (HDB Carpark Information) | 2,273 carparks; {int(scala['unmatched_meta_records']):,} availability records have no metadata row |

## Cleaning rules and their effect (Scala accumulators)

| Rule | Records |
|---|---|
| Malformed lines | {int(scala['malformed_lines']):,} |
| Unparseable lot items | {int(scala['bad_lot_items']):,} |
| total_lots ≤ 0 | {int(scala['non_positive_total']):,} |
| Stale (> {T['stale_max_min']} min since the sensor update) | {int(scala['stale_dropped']):,} |
| lots_available clipped to [0, total] | {int(scala['clipped_available']):,} |
| Always-zero carparks (0 available in > {T['always_zero_share']:.0%} of snapshots) | {R['excluded'].get('always_zero', 0)} carparks (excluded from ML) |
| Never-changing carparks (same value all period) | {R['excluded'].get('never_changing', 0)} carparks (excluded from ML) |
| Distinct sensor updates (dedupe on carpark, lot type, update_ts) | {int(scala['sensor_updates_distinct']):,} of {int(scala['clean_records']):,} snapshot rows |

**Snapshot grid vs sensor view.** An unchanged sensor repeats across snapshots, so (carpark, lot type, update_ts) has
duplicates. The ML uses the snapshot grid (what a driver would see at each time); deduplicated sensor updates are
reported for sensor-level statistics only.

## Staleness

Median staleness {R['staleness_median_min']:.1f} min; {R['stale_share']:.2%} of readings are older than {T['stale_max_min']} min and are dropped
(`eda_staleness_distribution.csv`, `eda_staleness.png`). Very old update times (days to years) come from carparks
whose sensors stopped reporting; they would otherwise look like "constant availability".

## Class balance and persistence

* Target `full_30` (≤ {T['full_ratio']:.0%} of car lots free 30 min later): positive rate **{pos:.2%}** over {R['class_balance_30']['rows']:,} rows → {R['full_ratio_decision']}.
* Only **{R['class_balance_30']['state_change_rate_30min']:.2%}** of rows change state (full ↔ not full) within 30 min
  (15 min: {R['state_change_rate_by_horizon'].get('15min', 'TBD'):.2%}, 60 min: {R['state_change_rate_by_horizon'].get('60min', 'TBD'):.2%}).
  This is why the persistence baseline is strong and why transition metrics are reported.
* Holidays in range: {', '.join(hol_in) if hol_in else 'none (holiday effect cannot be measured with this window)'}.
"""
(ROOT / "docs").mkdir(exist_ok=True)
(ROOT / "docs" / "data_quality_report.md").write_text(md)
print("\n=== eda summary ===")
print(json.dumps(R, indent=1, default=str)[:2500])
print(f"elapsed {time.time() - t0:.0f}s")
spark.stop()
