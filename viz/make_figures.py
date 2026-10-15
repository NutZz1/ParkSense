"""Phase 10 — 13 report/slide figures (300 dpi) from outputs/metrics and outputs/analytics only.

Usage: python viz/make_figures.py
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
M, A, FIG, MD = ROOT / "outputs/metrics", ROOT / "outputs/analytics", ROOT / "outputs/figures", ROOT / "models"
FIG.mkdir(parents=True, exist_ok=True)
plt.rcParams.update({"font.size": 10, "axes.titleweight": "bold"})
made, skipped = [], []
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def have(*fs):
    ok = all((M / f).exists() for f in fs)
    return ok


def save(name):
    plt.tight_layout()
    plt.savefig(FIG / name, dpi=300)
    plt.close()
    made.append(name)


# 01 architecture --------------------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(14, 7))
ax.set_xlim(0, 14)
ax.set_ylim(0, 7)
ax.axis("off")


def box(x, y, w, h, t, c):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.05", fc=c, ec="#333"))
    ax.text(x + w / 2, y + h / 2, t, ha="center", va="center", fontsize=8.3)


def arr(a, b):
    ax.annotate("", xy=b, xytext=a, arrowprops=dict(arrowstyle="->", lw=1.2, color="#333"))


for y, t, c in ((6.6, "INGESTION", "#1f4e79"), (4.6, "BATCH LAYER", "#1f4e79"), (2.6, "SPEED LAYER", "#7f3f00"), (0.9, "SERVING LAYER", "#245b24")):
    ax.text(0.1, y, t, fontweight="bold", color=c)
box(0.1, 5.4, 2.4, 1.0, "data.gov.sg API\ncarpark availability\n(history + live, 1 min)", "#dbe9f6")
box(2.9, 5.4, 2.6, 1.0, "backfill_history.py\nlive_poller.py · flatten.py\ndownload_metadata.py", "#dbe9f6")
box(5.9, 5.4, 2.2, 1.0, "HDFS\n/parksense/raw/flat\n/raw/meta", "#c6dcf0")
box(5.9, 3.4, 2.2, 1.6, "Scala Spark RDD\nRddPipeline.scala\nSparkConcepts.scala\n→ Parquet (by dt)", "#c6dcf0")
box(8.5, 3.4, 2.4, 1.6, "PySpark\nanalytics (Spark SQL)\nEDA · features\ntrain · evaluate", "#c6dcf0")
box(11.3, 3.4, 2.5, 1.6, "best model\n(RF / GBT)\n+ metrics", "#dbe9f6")
box(2.9, 1.9, 2.6, 1.1, "stream/incoming/*.txt\n(one file per poll)", "#fbe3cf")
box(5.9, 1.9, 2.2, 1.1, "StreamingJob.scala\nforeachBatch → RDD", "#f7cfae")
box(8.5, 1.9, 2.4, 1.1, "score_live.py\nP(full in 30 min)", "#f7cfae")
box(11.3, 1.9, 2.5, 1.1, "verify_live.py\nlive precision/recall", "#fbe3cf")
box(2.9, 0.1, 5.2, 1.2, "HBase: carpark_live · carpark_history · predictions_log · metrics\n(Thrift :9090 · REST :8085)", "#d7ecd2")
box(8.5, 0.1, 2.4, 1.2, "Express API\n(HBase REST)", "#d7ecd2")
box(11.3, 0.1, 2.5, 1.2, "React UI\n(on hold: design)", "#d7ecd2")
for a, b in (((2.5, 5.9), (2.9, 5.9)), ((5.5, 5.9), (5.9, 5.9)), ((7.0, 5.4), (7.0, 5.0)), ((8.1, 4.2), (8.5, 4.2)), ((10.9, 4.2), (11.3, 4.2)),
             ((4.2, 5.4), (4.2, 3.0)), ((5.5, 2.45), (5.9, 2.45)), ((8.1, 2.45), (8.5, 2.45)), ((10.9, 2.45), (11.3, 2.45)),
             ((12.5, 3.4), (10.2, 3.0)), ((2.95, 5.4), (2.75, 1.3)), ((7.0, 1.9), (6.6, 1.3)), ((9.2, 1.9), (7.6, 1.3)),
             ((8.1, 0.7), (8.5, 0.7)), ((10.9, 0.7), (11.3, 0.7))):
    arr(a, b)
ax.set_title("ParkSense SG — lambda architecture (Spark + HDFS + HBase)", fontsize=13)
save("fig01_architecture.png")

# 02 hour x weekday heatmap ---------------------------------------------------------------------------
if have("eda_heatmap_dow_hour.csv"):
    hm = pd.read_csv(M / "eda_heatmap_dow_hour.csv")
    piv = hm.pivot(index="dow", columns="hour", values="occ")
    piv.index = [DAYS[int(d) - 1] for d in piv.index]
    plt.figure(figsize=(13, 4))
    sns.heatmap(piv * 100, cmap="YlOrRd", annot=True, fmt=".0f", annot_kws={"size": 6.5}, cbar_kws={"label": "mean occupancy (%)"})
    plt.title("Mean car-lot occupancy (%) by weekday and hour (SGT) — HDB carparks")
    plt.xlabel("hour of day")
    plt.ylabel("")
    save("fig02_hour_weekday_heatmap.png")

# 03 top-20 most often full ---------------------------------------------------------------------------
if have("rdd_top20_full.csv"):
    t = pd.read_csv(M / "rdd_top20_full.csv").iloc[::-1]
    plt.figure(figsize=(10, 6.5))
    plt.barh([f"{c} · {str(a)[:34] if pd.notna(a) else '(no metadata)'}" for c, a in zip(t.carpark_no, t.address)], 100 * t.full_rate, color="#c44e52")
    plt.xlabel("% of snapshots nearly full (≤ 10% of car lots free)")
    plt.title("Top 20 most-often-full carparks (always-zero / frozen sensors excluded)")
    save("fig03_top20_full.png")

# 04 nearly-full rate by type x hour ------------------------------------------------------------------
if have("rdd_type_hour.csv"):
    th = pd.read_csv(M / "rdd_type_hour.csv")
    th = th[th.is_weekend.astype(str).str.lower() == "false"]
    big = th.groupby("car_park_type").records.sum().sort_values(ascending=False).head(6).index
    plt.figure(figsize=(11, 4.5))
    for t_ in big:
        g = th[th.car_park_type == t_].sort_values("hour")
        plt.plot(g.hour, 100 * g.full_rate, marker=".", label=t_)
    plt.legend(fontsize=7)
    plt.xticks(range(0, 24, 2))
    plt.xlabel("hour (SGT), weekdays")
    plt.ylabel("% of readings nearly full")
    plt.title("Nearly-full rate by carpark type and hour")
    save("fig04_type_hour_full_rate.png")

# 05 map of evening occupancy -------------------------------------------------------------------------
if have("eda_evening_occupancy_carpark.csv"):
    e = pd.read_csv(M / "eda_evening_occupancy_carpark.csv")
    plt.figure(figsize=(11, 6.5))
    sc = plt.scatter(e.lon, e.lat, c=100 * e.evening_occupancy, cmap="RdYlGn_r", s=9, vmin=0, vmax=100)
    plt.colorbar(sc, label="mean occupancy 18:00–22:00 (%)")
    plt.gca().set_aspect("equal")
    plt.xlabel("longitude")
    plt.ylabel("latitude")
    plt.title(f"{len(e):,} HDB carparks coloured by mean evening occupancy")
    save("fig05_map_evening_occupancy.png")

# 06 data quality -------------------------------------------------------------------------------------
if have("eda_staleness_distribution.csv", "eda_stale_rate_per_day.csv"):
    sd, sdy = pd.read_csv(M / "eda_staleness_distribution.csv"), pd.read_csv(M / "eda_stale_rate_per_day.csv")
    fig, ax = plt.subplots(1, 2, figsize=(13, 4))
    ax[0].bar(sd.staleness, 100 * sd.share, color="#4c72b0")
    ax[0].set(title="Staleness of readings (snapshot − sensor update)", ylabel="% of readings")
    ax[0].tick_params(axis="x", rotation=35)
    ax[1].plot(pd.to_datetime(sdy.dt), 100 * sdy.stale_rate, "o-", ms=3)
    ax[1].set(title="Readings older than 30 min (dropped), per day", ylabel="%")
    ax[1].tick_params(axis="x", rotation=35)
    save("fig06_data_quality.png")

# 07 model comparison / 08 confusion / 09 importance --------------------------------------------------
best = json.load(open(MD / "best_model_meta.json")) if (MD / "best_model_meta.json").exists() else None
if have("model_comparison.csv"):
    mc = pd.read_csv(M / "model_comparison.csv")
    x = np.arange(len(mc))
    plt.figure(figsize=(11, 4.6))
    for i, (k, c) in enumerate((("precision", "#66c2a5"), ("recall", "#fc8d62"), ("f1", "#8da0cb"))):
        b = plt.bar(x + (i - 1) * 0.26, mc[k], 0.26, label=k, color=c)
        for r, v in zip(b, mc[k]):
            plt.text(r.get_x() + r.get_width() / 2, v + 0.01, f"{v:.3f}", ha="center", fontsize=7)
    plt.xticks(x, mc.model)
    plt.ylim(0, 1.12)
    plt.ylabel("test score (positive = nearly full in 30 min)")
    plt.title("Baselines vs models on the held-out test days")
    plt.legend(ncol=3, loc="upper center", frameon=False)
    save("fig07_model_comparison.png")
if best and have(f"confusion_{best['algorithm'].lower().replace(' ', '_').replace('-', '_')}.csv"):
    cm = pd.read_csv(M / f"confusion_{best['algorithm'].lower().replace(' ', '_').replace('-', '_')}.csv", index_col=0)
    pct = cm.values / cm.values.sum() * 100
    lab = np.array([[f"{v:,}\n({p:.2f}%)" for v, p in zip(r, pr)] for r, pr in zip(cm.values, pct)])
    plt.figure(figsize=(5.8, 4.6))
    sns.heatmap(cm, annot=lab, fmt="", cmap="Blues", cbar=False, xticklabels=["pred not full", "pred full"], yticklabels=["actual not full", "actual full"])
    plt.title(f"Confusion matrix — {best['algorithm']} (threshold {best['threshold']:.2f})")
    save("fig08_confusion_best.png")
if best and have("feature_importance.csv"):
    fi = pd.read_csv(M / "feature_importance.csv")
    fi = fi[fi.model == best["algorithm"]].head(15).iloc[::-1]
    plt.figure(figsize=(8, 5.5))
    plt.barh(fi.feature, fi.importance, color="#4c72b0")
    plt.xlabel("importance")
    plt.title(f"Top 15 features — {best['algorithm']}")
    save("fig09_feature_importance.png")

# 10 cache / 11 scaling / 12 shuffle --------------------------------------------------------------------
if have("concepts_cache.csv"):
    c = pd.read_csv(M / "concepts_cache.csv")
    x = np.arange(len(c))
    plt.figure(figsize=(10, 4.3))
    for i, (k, lb) in enumerate((("action1_ms", "1st action"), ("action2_ms", "2nd action"), ("action3_ms", "3rd action"))):
        b = plt.bar(x + (i - 1) * 0.27, c[k], 0.27, label=lb)
        for r, v in zip(b, c[k]):
            plt.text(r.get_x() + r.get_width() / 2, v, str(v), ha="center", va="bottom", fontsize=7)
    plt.xticks(x, [f"{m}\n{mb / 1e6:.0f} MB" if mb else m for m, mb in zip(c["mode"], c.mem_bytes)], fontsize=8)
    plt.ylabel("ms")
    plt.title("RDD persistence: no cache vs cache() vs MEMORY_AND_DISK vs MEMORY_ONLY_SER")
    plt.legend()
    save("fig10_cache_benchmark.png")
if have("concepts_partitions.csv"):
    p = pd.read_csv(M / "concepts_partitions.csv")
    ps = pd.read_csv(M / "perf_system.csv") if have("perf_system.csv") else pd.DataFrame(columns=["experiment"])
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.2))
    ax[0].plot(p.partitions, p.ms, "o-")
    ax[0].set(xscale="log", title="Scala RDD aggregation vs partitions", xlabel="partitions", ylabel="ms")
    ax[0].set_xticks(p.partitions)
    ax[0].set_xticklabels(p.partitions)
    par, sca = ps[ps.experiment == "parallelism"], ps[ps.experiment == "scalability"]
    if len(par):
        ax[1].bar(par.setting, par.total_s, color="#55a868")
        ax[1].set(title="PySpark features + RF vs cores", ylabel="seconds")
    if len(sca):
        ax[2].plot(sca.rows, sca.total_s, "o-", color="#c44e52")
        for r in sca.itertuples():
            ax[2].annotate(r.setting.split(" ")[0], (r.rows, r.total_s), textcoords="offset points", xytext=(4, 4), fontsize=8)
        ax[2].set(title="PySpark features + RF vs data size", xlabel="rows", ylabel="seconds")
    if not len(par):
        skipped.append("fig11 panels 2-3 (run perf_system.py)")
    save("fig11_scaling.png")
if have("concepts_shuffle.csv"):
    s = pd.read_csv(M / "concepts_shuffle.csv")
    fig, ax = plt.subplots(1, 2, figsize=(12, 4))
    ax[0].bar(s.method, s.shuffle_write_bytes / 1e6, color=["#4c72b0", "#dd8452", "#937860", "#55a868"])
    ax[0].set(title="Shuffle write (MB)", ylabel="MB")
    ax[1].bar(s.method, s.ms, color=["#4c72b0", "#dd8452", "#937860", "#55a868"])
    ax[1].set(title="Time (ms)", ylabel="ms")
    for a_ in ax:
        a_.tick_params(axis="x", rotation=15)
    fig.suptitle("reduceByKey vs groupByKey, shuffle join vs broadcast join", fontweight="bold")
    plt.tight_layout(rect=(0, 0, 1, 0.92))
    plt.savefig(FIG / "fig12_shuffle.png", dpi=300)
    plt.close()
    made.append("fig12_shuffle.png")

# 13 live: predicted probability vs actual for one carpark ---------------------------------------------
if have("live_eval.csv"):
    ev = pd.read_csv(M / "live_eval.csv")
    ev["target_at"] = pd.to_datetime(ev.target_at).dt.tz_convert("Asia/Singapore")
    ch = ev.groupby("carpark").apply(lambda g: (g.actual_full.nunique() > 1, len(g)), include_groups=False)
    pick = sorted(ch.items(), key=lambda kv: (kv[1][0], kv[1][1]), reverse=True)[0][0]
    g = ev[ev.carpark == pick].sort_values("target_at")
    s = json.load(open(M / "live_eval_summary.json"))
    fig, ax = plt.subplots(1, 2, figsize=(14, 4.3), gridspec_kw={"width_ratios": [2.2, 1]})
    ax[0].plot(g.target_at, g.prob, "o-", ms=3, label="predicted P(full) 30 min earlier")
    ax[0].plot(g.target_at, 1 - g.actual_ratio, "s-", ms=3, alpha=0.7, label="actual occupancy at target time")
    ax[0].axhline(0.9, color="red", ls="--", lw=1, label="nearly full (occupancy ≥ 90%)")
    ax[0].set(title=f"Live: carpark {pick}", ylim=(-0.02, 1.05))
    ax[0].legend(fontsize=7)
    ks = ["accuracy", "precision", "recall", "f1"]
    x = np.arange(4)
    ax[1].bar(x - 0.2, [s["model"][k] for k in ks], 0.4, label="model")
    ax[1].bar(x + 0.2, [s["persistence"][k] for k in ks], 0.4, label="persistence")
    ax[1].set_xticks(x)
    ax[1].set_xticklabels(ks)
    ax[1].set(ylim=(0, 1.05), title=f"Live evaluation (n = {s['verified']:,})")
    ax[1].legend()
    save("fig13_live_eval.png")
else:
    skipped.append("fig13_live_eval.png (no live_eval.csv yet)")

# 14 transitions + horizons (extra) ------------------------------------------------------------------
if have("transition_metrics.csv"):
    tm = pd.read_csv(M / "transition_metrics.csv")
    tm = tm[tm.transition.isin(["becomes full (0->1)", "frees up (1->0)", "all transitions"])]
    piv = tm.pivot(index="transition", columns="model", values="correct_share")
    hz = pd.read_csv(M / "horizon_sensitivity.csv") if have("horizon_sensitivity.csv") else None
    fig, ax = plt.subplots(1, 2 if hz is not None else 1, figsize=(13, 4.2))
    ax = np.atleast_1d(ax)
    (piv * 100).plot(kind="bar", ax=ax[0], rot=0)
    ax[0].set(title="Rows where the state changes within 30 min: % predicted correctly", ylabel="%", xlabel="")
    if hz is not None:
        ax[1].plot(hz.horizon_min, hz.f1, "o-", label="model F1")
        ax[1].plot(hz.horizon_min, hz.persistence_f1, "s--", label="persistence F1")
        ax[1].set(title="F1 vs horizon", xlabel="minutes ahead", xticks=hz.horizon_min)
        ax[1].legend()
    save("fig14_transitions_horizons.png")

print("=== make_figures ===")
for m in made:
    print("  made   ", m)
for s in skipped:
    print("  skipped", s)
