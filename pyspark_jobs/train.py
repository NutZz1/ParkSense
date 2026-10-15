"""Phase 6 — train Random Forest (primary) and GBT (challenger) with a time-based split.

* split 70/15/15 of the labelled timeline (whole days when >= 10 days), horizon embargo
* leakage-safe target encoding (carpark x hour x dow full rate) computed on TRAIN only
* class imbalance: inverse-frequency weightCol (GBTClassifier supports weightCol in Spark 3.5)
* small manual grid on the time-based validation set; threshold = max F1 on validation
* winner (by validation F1) refit on train + validation -> models/best_model + best_model_meta.json

Usage: python pyspark_jobs/train.py [--horizon 30] [--tune-sample 0.3]
"""
import argparse
import json
import shutil
import time
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F

from features import CATEGORICAL, NUMERIC, add_target_encoding, target_encoding
from ml_utils import (GRIDS, NAMES, SEED, add_weights, apply_split, choose_threshold, make_pipeline, metrics,
                      probabilities, save_json, split_boundaries)
from spark_session import CFG, get_logger, get_spark, lake, path

ap = argparse.ArgumentParser()
ap.add_argument("--horizon", type=int, default=CFG["thresholds"]["horizon_min"])
ap.add_argument("--tune-sample", type=float, default=0.3, help="share of the training rows used for the grid search")
args = ap.parse_args()
h, label = args.horizon, f"label_{args.horizon}"
log = get_logger("train")
t0 = time.time()
spark = get_spark("ParkSense-Train", shuffle_partitions=32)
M, MD = path("metrics"), path("models")

feats = spark.read.parquet(lake("processed/features"))
b1, b2, split_meta = split_boundaries(feats, label)
train0, val0, test0 = apply_split(feats, label, b1, b2, h)
te_chd, te_ch, g = target_encoding(train0, "label")
te_chd, te_ch = te_chd.cache(), te_ch.cache()
train, val, test = (add_target_encoding(d, te_chd, te_ch, g) for d in (train0, val0, test0))
train, val, test, weights = add_weights(train, val, test)
train, val = train.cache(), val.cache()
utc = lambda slot: datetime.fromtimestamp(slot * 900, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")  # noqa: E731

split_rows = []
for name, d in (("train", train), ("val", val), ("test", test)):
    r = d.agg(F.count("*").alias("n"), F.avg("label").alias("pos"), F.min("slot").alias("a"), F.max("slot").alias("b"),
              F.countDistinct("dt").alias("days"), F.avg("is_weekend").alias("wk")).first()
    split_rows.append({"split": name, "rows": r["n"], "positive_rate": round(r["pos"], 4), "days": r["days"],
                       "weekend_share": round(r["wk"], 3),
                       "start": utc(r["a"]), "end": utc(r["b"])})
pd.DataFrame(split_rows).to_csv(M / f"split_summary_{h}.csv", index=False)
split_info = {"horizon_min": h, "boundary_slots": [b1, b2], **split_meta, "splits": split_rows, "class_weights": weights,
              "target_encoding_global_rate": g}
save_json(split_info, f"split_boundaries_{h}.json")
for r in split_rows:
    log.info("%-5s rows=%d pos=%.4f days=%s %s .. %s", r["split"], r["rows"], r["positive_rate"], r["days"], r["start"], r["end"])

tune = train.sample(fraction=args.tune_sample, seed=SEED) if args.tune_sample < 1 else train
grid_rows, best = [], {}
pv_val = None
for algo, grid in GRIDS.items():
    for params in grid:
        ts = time.time()
        model = make_pipeline(algo, params).fit(tune)
        fit_s = time.time() - ts
        pv = probabilities(model, val)
        m05 = metrics(pv.label, pv.p >= 0.5, pv.p)
        th, curve = choose_threshold(pv.label.values, pv.p.values)
        grid_rows.append({"algo": algo, "params": json.dumps(params), "fit_seconds": round(fit_s, 1), "tune_rows_share": args.tune_sample,
                          "val_f1_at_0.5": m05["f1"], "val_pr_auc": m05["pr_auc"], "val_roc_auc": m05["roc_auc"],
                          "val_best_f1": th["f1"], "val_best_threshold": th["threshold"], "val_precision": th["precision"],
                          "val_recall": th["recall"]})
        log.info("%s %s fit %.0fs: val PR-AUC %.3f, best F1 %.3f @ %.2f (P %.3f R %.3f)", algo, params, fit_s,
                 m05["pr_auc"], th["f1"], th["threshold"], th["precision"], th["recall"])
        if algo not in best or th["f1"] > best[algo]["th"]["f1"]:
            best[algo] = {"params": params, "th": th, "curve": curve}
pd.DataFrame(grid_rows).round(4).to_csv(M / f"train_grid_{h}.csv", index=False)
for algo, b in best.items():
    pd.DataFrame(b["curve"]).round(4).to_csv(M / f"threshold_curve_{algo}_{h}.csv", index=False)

# refit every algorithm's best configuration on train + val (full rows), save; winner = best validation F1
trainval = train.unionByName(val)
meta_all = {}
for algo, b in best.items():
    ts = time.time()
    model = make_pipeline(algo, b["params"]).fit(trainval)
    out = MD / f"{algo}_{h}"
    model.write().overwrite().save(f"file://{out}")
    meta_all[algo] = {"algorithm": NAMES[algo], "params": b["params"], "threshold": b["th"]["threshold"],
                      "val_at_threshold": {k: b["th"][k] for k in ("precision", "recall", "f1")},
                      "model_path": str(out), "refit_seconds": round(time.time() - ts, 1)}
    log.info("refit %s on train+val in %.0fs -> %s", algo, time.time() - ts, out)
with open(MD / f"models_meta_{h}.json", "w") as f:
    json.dump(meta_all, f, indent=2)

# target-encoding tables for live scoring (computed on TRAIN only)
te_chd.write.mode("overwrite").parquet(f"file://{MD / f'te_chd_{h}'}")
te_ch.write.mode("overwrite").parquet(f"file://{MD / f'te_ch_{h}'}")
winner = max(best, key=lambda a: best[a]["th"]["f1"])
if h == CFG["thresholds"]["horizon_min"]:
    bd = MD / "best_model"
    if bd.exists():
        shutil.rmtree(bd)
    shutil.copytree(MD / f"{winner}_{h}", bd)
    meta = {**meta_all[winner], "algo": winner, "model_path": str(bd), "horizon_min": h,
            "full_ratio": CFG["thresholds"]["full_ratio"], "label": f"avail_ratio(t+{h}min) <= {CFG['thresholds']['full_ratio']}",
            "numeric_features": NUMERIC, "categorical_features": CATEGORICAL, "split": split_info,
            "te_tables": {"chd": str(MD / f"te_chd_{h}"), "ch": str(MD / f"te_ch_{h}"), "global": g},
            "selection": "max validation F1 (grid on a training sample; refit on train+val)",
            "trained_at_utc": datetime.now(timezone.utc).isoformat(),
            "model_version": f"{winner}-h{h}-{datetime.now(timezone.utc):%Y%m%d%H%M}"}
    with open(MD / "best_model_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

print("\n=== train summary ===")
for r in split_rows:
    print(f"  {r['split']:<5} rows={r['rows']:>8} positive={r['positive_rate']:.4f} days={r['days']}")
for a, b in best.items():
    print(f"  {a:<4} {b['params']} val F1 {b['th']['f1']:.3f} @ {b['th']['threshold']:.2f} (P {b['th']['precision']:.3f}, R {b['th']['recall']:.3f})")
print(f"  winner: {winner} -> models/best_model; elapsed {time.time() - t0:.0f}s")
spark.stop()
