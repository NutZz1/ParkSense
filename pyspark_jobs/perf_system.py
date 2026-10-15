"""Phase 7 — system performance -> outputs/metrics/perf_system.csv (+ perf_interpretation.txt)

Benchmark job (identical in every run): base_readings + build_features + RandomForest(30 trees, depth 10)
fit on the first 70% of the timeline + scoring the rest. Each configuration runs in a fresh subprocess
(its own SparkSession/master). Scalability uses 25/50/100% of the carparks (each time series stays intact);
the cores and shuffle-partition experiments use 25% of the carparks to keep the sweep short.
Merged in: Scala timings, Spark SQL query times, HBase write throughput, stream latency, API lag.

Usage: python pyspark_jobs/perf_system.py [--interpret-only]
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent


def worker(a):
    from pyspark.sql import functions as F

    from features import base_readings, build_features
    from ml_utils import make_pipeline
    from spark_session import get_spark, lake

    spark = get_spark(f"ParkSense-Perf-{a.tag}", master=a.master, shuffle_partitions=a.shuffle)
    occ = spark.read.parquet(lake("processed/occupancy"))
    if a.fraction < 1:
        cps = sorted(r[0] for r in occ.select("carpark_no").distinct().collect())
        occ = occ.filter(F.col("carpark_no").isin(cps[: max(1, round(len(cps) * a.fraction))]))
    t0 = time.time()
    f = build_features(spark, base_readings(spark, occ, spark.read.parquet(lake("processed/excluded_carparks"))), horizons=(30,))
    f = f.filter(F.col("label_30").isNotNull()).withColumn("label", F.col("label_30")).withColumn("weight", F.lit(1.0)) \
        .withColumn("te_chd", F.lit(0.0)).withColumn("te_ch", F.lit(0.0))
    if a.cache:
        f = f.cache()
    n = f.count()
    tf = time.time() - t0
    b = f.approxQuantile("slot", [0.7], 0.0)[0]
    tr, te = f.filter(F.col("slot") < b), f.filter(F.col("slot") >= b)
    t1 = time.time()
    reps = 3 if a.loop else 1
    for i in range(reps):  # the caching experiment re-reads the features in a 3-model loop
        make_pipeline("rf", {"numTrees": 20 if a.loop else 30, "maxDepth": 8 + i if a.loop else 10}).fit(tr).transform(te).count()
    print("RESULT " + json.dumps({"rows": n, "features_s": round(tf, 2), "train_score_s": round(time.time() - t1, 2),
                                  "total_s": round(tf + time.time() - t1, 2), "parallelism": spark.sparkContext.defaultParallelism}))
    spark.stop()


def run(tag, master="local[*]", shuffle=32, fraction=1.0, cache=False, loop=False):
    cmd = [sys.executable, str(HERE / "perf_system.py"), "--worker", "--tag", tag, "--master", master, "--shuffle", str(shuffle),
           "--fraction", str(fraction)] + (["--cache"] if cache else []) + (["--loop"] if loop else [])
    out = subprocess.run(cmd, capture_output=True, text=True, env=os.environ.copy())
    line = [x for x in out.stdout.splitlines() if x.startswith("RESULT ")]
    if not line:
        print(out.stdout[-1500:], out.stderr[-2500:])
        raise RuntimeError(f"benchmark {tag} failed")
    r = json.loads(line[0][7:])
    print(f"  {tag:<24} rows={r['rows']:>9,} features={r['features_s']:>7}s train+score={r['train_score_s']:>7}s total={r['total_s']:>7}s", flush=True)
    return r


def other_rows(M, CFG):
    rows = []
    t = M / "timings.csv"
    if t.exists():
        for r in pd.read_csv(t).itertuples():
            rows.append({"experiment": f"scala_{r.job}", "setting": r.label, "total_s": round(r.ms / 1000, 3), "notes": "Timer.scala"})
    q = Path(CFG["project"]["root"]) / CFG["paths"]["analytics"] / "query_times.csv"
    if q.exists():
        for r in pd.read_csv(q).itertuples():
            rows.append({"experiment": "spark_sql", "setting": r.query, "rows": r.rows, "total_s": r.seconds, "notes": "analytics.py"})
    ps = M / "poller_stats.csv"
    if ps.exists():
        p = pd.read_csv(ps)
        p = p[p.hbase_rows > 0]
        if len(p):
            rows.append({"experiment": "hbase", "setting": "poller write throughput (median rows/s, HappyBase batch)",
                         "total_s": float(p.hbase_rows_per_s.median()), "rows": int(p.hbase_rows.sum()), "notes": f"{len(p)} polls"})
            rows.append({"experiment": "api", "setting": "API lag: response time - snapshot timestamp (median s)",
                         "total_s": float(p.api_lag_s.median()), "rows": len(p), "notes": "poller_stats.csv"})
    sb = M / "stream_batches.csv"
    if sb.exists():
        s = pd.read_csv(sb)
        s = s[s.files <= 2]  # exclude the start-up backlog batches
        if len(s):
            rows.append({"experiment": "streaming", "setting": "poll file written -> micro-batch processed (median s)",
                         "total_s": float(s.file_to_processed_s.median()), "rows": int(s.lot_records.sum()), "notes": f"{len(s)} steady-state batches"})
            rows.append({"experiment": "streaming", "setting": "micro-batch RDD processing time (median s)",
                         "total_s": round(float(s.batch_ms.median()) / 1000, 2), "rows": len(s), "notes": "StreamingJob.scala"})
    lp = M / "live_predictions.csv"
    if lp.exists():
        p = pd.read_csv(lp)
        rows.append({"experiment": "live", "setting": "score_live run time (median s)", "total_s": float(p.latency_s.median()),
                     "rows": len(p), "notes": "features + model for all carparks"})
    return rows


def interpret(M):
    p = pd.read_csv(M / "perf_system.csv")
    g = lambda e: p[p.experiment == e]  # noqa: E731
    out = []
    sc = g("scalability")
    if len(sc) >= 2:
        a, b = sc.iloc[0], sc.iloc[-1]
        ratio_t, ratio_d = b.total_s / a.total_s, b.rows / a.rows
        out.append(f"Scalability: {a.setting} → {b.setting} is {ratio_d:.1f}x the rows and {ratio_t:.1f}x the time "
                   + ("(sub-linear: fixed overheads dominate)." if ratio_t < 0.9 * ratio_d else "(roughly linear)." if ratio_t <= 1.1 * ratio_d else "(super-linear)."))
    par = g("parallelism")
    if len(par) >= 2 and (par.setting == "local[1]").any():
        one = par[par.setting == "local[1]"].total_s.iloc[0]
        best = par.loc[par.total_s.idxmin()]
        out.append(f"Cores: {best.setting} fastest ({best.total_s:.1f} s vs {one:.1f} s on 1 core, {one / best.total_s:.2f}x speed-up).")
    sp = g("shuffle_partitions")
    if len(sp) >= 2:
        best = sp.loc[sp.total_s.idxmin()]
        d = sp[sp.setting == "200"].total_s
        out.append(f"Shuffle partitions: {best.setting} fastest ({best.total_s:.1f} s)" + (f"; the default 200 took {d.iloc[0]:.1f} s." if len(d) else "."))
    ca = g("caching")
    if len(ca) == 2:
        nc, c = ca[ca.setting == "not cached"].train_score_s.iloc[0], ca[ca.setting == "cached"].train_score_s.iloc[0]
        out.append(f"Caching the feature DataFrame changed the 3-model loop from {nc:.1f} s to {c:.1f} s ({nc / c:.1f}x).")
    for r in p[p.experiment.isin(["hbase", "api", "streaming", "live"])].itertuples():
        out.append(f"{r.setting}: {r.total_s}.")
    txt = " ".join(out)
    (M / "perf_interpretation.txt").write_text(txt + "\n")
    print(txt)


def main():
    from spark_session import CFG, path
    M = path("metrics")
    rows = []

    def add(exp, setting, r, notes):
        rows.append({"experiment": exp, "setting": setting, "rows": r["rows"], "features_s": r["features_s"],
                     "train_score_s": r["train_score_s"], "total_s": r["total_s"], "notes": notes})

    print("1. data-size scaling")
    for fr in (0.25, 0.5, 1.0):
        add("scalability", f"{int(fr * 100)}% of carparks", run(f"scale_{fr}", fraction=fr), "local[*]")
    print("2. cores (25% of carparks)")
    for m in ("local[1]", "local[2]", "local[4]", "local[*]"):
        add("parallelism", m, run(f"cores_{m}", master=m, fraction=0.25), "25% of carparks")
    print("3. shuffle partitions (25% of carparks)")
    for s in (8, 16, 32, 64, 200):
        add("shuffle_partitions", str(s), run(f"shuffle_{s}", shuffle=s, fraction=0.25), "25% of carparks, local[*]")
    print("4. caching (25% of carparks, 3-model loop)")
    for c in (False, True):
        add("caching", "cached" if c else "not cached", run(f"cache_{c}", cache=c, loop=True, fraction=0.25), "3 x RF fit+score")
    rows += other_rows(M, CFG)
    pd.DataFrame(rows).to_csv(M / "perf_system.csv", index=False)
    print(f"wrote {M / 'perf_system.csv'} ({len(rows)} rows)")
    interpret(M)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--worker", action="store_true")
    ap.add_argument("--interpret-only", action="store_true", help="refresh merged live/stream rows + interpretation")
    ap.add_argument("--tag", default="run")
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--shuffle", type=int, default=32)
    ap.add_argument("--fraction", type=float, default=1.0)
    ap.add_argument("--cache", action="store_true")
    ap.add_argument("--loop", action="store_true")
    a = ap.parse_args()
    sys.path.insert(0, str(HERE))
    if a.worker:
        worker(a)
    elif a.interpret_only:
        from spark_session import CFG, path
        M = path("metrics")
        base = pd.read_csv(M / "perf_system.csv")
        base = base[base.experiment.isin(["scalability", "parallelism", "shuffle_partitions", "caching"])]
        pd.concat([base, pd.DataFrame(other_rows(M, CFG))], ignore_index=True).to_csv(M / "perf_system.csv", index=False)
        interpret(M)
    else:
        main()
