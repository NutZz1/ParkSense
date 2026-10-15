"""Phase 6 — evaluate both models and both baselines on the TEST period.

outputs/metrics/: model_comparison.csv · confusion_<model>.csv · feature_importance.csv · slice_by_type.csv ·
slice_by_hour.csv · slice_by_daytype.csv · transition_metrics.csv · horizon_sensitivity.csv · model_summary.md
Usage: python pyspark_jobs/evaluate.py [--skip-horizons]
"""
import argparse
import json
import time

import numpy as np
import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from features import add_target_encoding, target_encoding
from ml_utils import NAMES, SEED, add_weights, apply_split, choose_threshold, make_pipeline, metrics, probabilities, split_boundaries
from spark_session import CFG, get_logger, get_spark, lake, path

ap = argparse.ArgumentParser()
ap.add_argument("--skip-horizons", action="store_true")
ap.add_argument("--tune-sample", type=float, default=0.3)
args = ap.parse_args()
log = get_logger("evaluate")
t0 = time.time()
M, MD = path("metrics"), path("models")
h = CFG["thresholds"]["horizon_min"]
spark = get_spark("ParkSense-Evaluate", shuffle_partitions=32)
feats = spark.read.parquet(lake("processed/features"))
meta_all = json.load(open(MD / f"models_meta_{h}.json"))
best = json.load(open(MD / "best_model_meta.json"))


def splits(hz):
    lab = f"label_{hz}"
    b1, b2, _ = split_boundaries(feats, lab)
    tr, va, te = apply_split(feats, lab, b1, b2, hz)
    chd, ch, g = target_encoding(tr, "label")
    return tuple(add_target_encoding(d, chd, ch, g) for d in (tr, va, te))


train, val, test = splits(h)
test = test.cache()
keep = ("carpark_no", "slot", "hour", "dow", "is_weekend", "car_park_type", "label", "full_now", "te_chd", "dt")
tp = test.select(*keep).toPandas()
results, preds = [], {}


def record(name, kind, y, pred, score, thr):
    m = metrics(y, pred, score)
    results.append({"model": name, "type": kind, "threshold": thr, **m})
    pd.DataFrame([[m["tn"], m["fp"]], [m["fn"], m["tp"]]], index=["actual_not_full", "actual_full"],
                 columns=["pred_not_full", "pred_full"]).to_csv(M / f"confusion_{name.lower().replace(' ', '_').replace('-', '_')}.csv")
    log.info("%-34s acc %.4f P %.3f R %.3f F1 %.3f PR-AUC %.3f", name, m["accuracy"], m["precision"], m["recall"], m["f1"], m["pr_auc"])
    return m


y = tp.label.values
preds["Persistence"] = tp.full_now.values.astype(int)
pers = record("Persistence", "baseline", y, preds["Persistence"], tp.full_now.values, "full_now")
preds["Historical"] = (tp.te_chd.values > 0.5).astype(int)
record("Historical", "baseline", y, preds["Historical"], tp.te_chd.values, "carpark-hour-dow rate > 0.5")
scored = {}
for algo, m in meta_all.items():
    model = PipelineModel.load(f"file://{m['model_path']}")
    pv = probabilities(model, test, keep)
    pv = tp[["carpark_no", "slot"]].merge(pv, on=["carpark_no", "slot"])
    preds[NAMES[algo]] = (pv.p.values >= m["threshold"]).astype(int)
    scored[algo] = pv
    record(NAMES[algo], "model", pv.label.values, preds[NAMES[algo]], pv.p.values, m["threshold"])

comp = pd.DataFrame(results)
for k in ("f1", "recall"):
    comp[f"beats_persistence_{k}"] = comp[k] > pers[k]
comp.round(4).to_csv(M / "model_comparison.csv", index=False)

# ---- transition metrics: rows where the state changes between t and t+30 (where persistence is always wrong)
bp = scored[best["algo"]].copy()
bp["pred"] = (bp.p >= best["threshold"]).astype(int)
bp["persist"] = bp.full_now.astype(int)
trans = bp[bp.label != bp.full_now]
tr_rows = []
for name, col in (("Persistence", "persist"), (best["algorithm"], "pred")):
    for kind, sub in (("becomes full (0->1)", trans[trans.label == 1]), ("frees up (1->0)", trans[trans.label == 0]), ("all transitions", trans)):
        tr_rows.append({"model": name, "transition": kind, "n": len(sub),
                        "correct_share": float((sub[col] == sub.label).mean()) if len(sub) else None})
    tm = metrics(trans.label.values, trans[col].values)
    tr_rows.append({"model": name, "transition": "precision/recall/F1 on transition rows", "n": len(trans),
                    "precision": tm["precision"], "recall": tm["recall"], "f1": tm["f1"]})
pd.DataFrame(tr_rows).round(4).to_csv(M / "transition_metrics.csv", index=False)
change_rate = len(trans) / len(bp)

# ---- slices for the best model (with persistence alongside)
def slice_by(col, fname):
    rows = []
    for k, g in bp.groupby(col):
        mm, pm = metrics(g.label.values, g.pred.values), metrics(g.label.values, g.persist.values)
        rows.append({col: k, "n": mm["n"], "positive_rate": g.label.mean(), "precision": mm["precision"], "recall": mm["recall"],
                     "f1": mm["f1"], "persistence_f1": pm["f1"]})
    pd.DataFrame(rows).round(4).to_csv(M / fname, index=False)


slice_by("car_park_type", "slice_by_type.csv")
slice_by("hour", "slice_by_hour.csv")
bp["day_type"] = np.where(bp.is_weekend == 1, "weekend", "weekday")
slice_by("day_type", "slice_by_daytype.csv")
bp.sample(min(len(bp), 200000), random_state=SEED).to_csv(M / "test_predictions_best_sample.csv", index=False)

# ---- feature importance (RF and GBT)
imp = []
for algo, m in meta_all.items():
    mdl = PipelineModel.load(f"file://{m['model_path']}")
    attrs = mdl.transform(test.limit(5)).schema["features"].metadata.get("ml_attr", {}).get("attrs", {})
    names = {a["idx"]: a["name"] for g in attrs.values() for a in g}
    for i, v in enumerate(mdl.stages[-1].featureImportances.toArray()):
        imp.append({"model": NAMES[algo], "feature": names.get(i, f"f{i}"), "importance": float(v)})
imp = pd.DataFrame(imp).sort_values(["model", "importance"], ascending=[True, False])
imp.round(6).to_csv(M / "feature_importance.csv", index=False)

# ---- horizon sensitivity: best config retrained for 15 / 30 / 60 min (grid sample, threshold on validation)
hz_rows = []
if not args.skip_horizons:
    for hz in CFG["thresholds"]["horizons_sensitivity"]:
        tr, va, te = splits(hz)
        tr, va, te, _ = add_weights(tr, va, te)
        if hz == h:
            mdl, thr = PipelineModel.load(f"file://{best['model_path']}"), best["threshold"]
        else:
            mdl = make_pipeline(best["algo"], best["params"]).fit(tr.sample(fraction=args.tune_sample, seed=SEED))
            pv = probabilities(mdl, va, keep)
            thr = choose_threshold(pv.label.values, pv.p.values)[0]["threshold"]
            mdl = make_pipeline(best["algo"], best["params"]).fit(tr.unionByName(va))
        pt = probabilities(mdl, te, keep)
        mm = metrics(pt.label.values, pt.p.values >= thr, pt.p.values)
        pm = metrics(pt.label.values, pt.full_now.values)
        ch = float((pt.label != pt.full_now).mean())
        hz_rows.append({"horizon_min": hz, "model": best["algorithm"], "threshold": thr, "state_change_rate": ch,
                        **{k: mm[k] for k in ("accuracy", "precision", "recall", "f1", "pr_auc")},
                        **{f"persistence_{k}": pm[k] for k in ("accuracy", "precision", "recall", "f1")}})
        log.info("horizon %d min: F1 %.3f vs persistence %.3f", hz, mm["f1"], pm["f1"])
    pd.DataFrame(hz_rows).round(4).to_csv(M / "horizon_sensitivity.csv", index=False)

# ---- interpretation (numbers only from this run)
c = comp.set_index("model")
b, p_ = c.loc[best["algorithm"]], c.loc["Persistence"]
tdf = pd.DataFrame(tr_rows)
tb = tdf[(tdf.model == best["algorithm"]) & (tdf.transition == "all transitions")].correct_share.iloc[0]
top = imp[imp.model == best["algorithm"]].head(5).feature.tolist()
beats = bool(b.f1 > p_.f1 or b.recall > p_.recall)
txt = [
    "# Model summary", "",
    f"_Generated by `evaluate.py`. Test period: {best['split']['splits'][2]['start']} → {best['split']['splits'][2]['end']}, "
    f"{int(b.n):,} carpark-slots, positive rate {y.mean():.2%} (nearly full = ≤ {CFG['thresholds']['full_ratio']:.0%} of car lots free in {h} min)._", "",
    f"The selected model is **{best['algorithm']}** ({json.dumps(best['params'])}, threshold {best['threshold']:.2f} chosen on validation). "
    f"On test: accuracy {b.accuracy:.4f}, precision {b.precision:.3f}, recall {b.recall:.3f}, F1 {b.f1:.3f}, PR-AUC {b.pr_auc:.3f}. "
    f"Persistence (\"full in 30 min if full now\"): precision {p_.precision:.3f}, recall {p_.recall:.3f}, F1 {p_.f1:.3f}. "
    f"Historical baseline F1 {c.loc['Historical'].f1:.3f}. "
    + (f"The model **beats persistence on {' and '.join(k for k, d in (('F1', b.f1 - p_.f1), ('recall', b.recall - p_.recall)) if d > 0)}**"
       + ("" if b.f1 > p_.f1 else " but not on F1") + f" (ΔF1 {b.f1 - p_.f1:+.3f}, Δrecall {b.recall - p_.recall:+.3f}, Δprecision {b.precision - p_.precision:+.3f})."
       if beats else f"The model **does not beat persistence overall** (ΔF1 {b.f1 - p_.f1:+.3f}, Δrecall {b.recall - p_.recall:+.3f}): "
       "the state rarely changes within 30 minutes, so persistence is almost always right."),
    "",
    f"Only {change_rate:.1%} of test rows change state within {h} min. On exactly those transition rows persistence is always wrong (0% correct), "
    f"while the model gets {tb:.1%} right — this is where prediction adds value over a live count.",
    "", f"Most important features: {', '.join(top)}.",
]
if hz_rows:
    txt += ["", "Horizon sensitivity (F1 model vs persistence; state-change rate): " + "; ".join(
        f"{r['horizon_min']} min: {r['f1']:.3f} vs {r['persistence_f1']:.3f} ({r['state_change_rate']:.1%})" for r in hz_rows) + "."]
(M / "model_summary.md").write_text("\n".join(txt) + "\n")

print("\n=== evaluate summary (test) ===")
print(comp[["model", "threshold", "accuracy", "precision", "recall", "f1", "pr_auc"]].round(4).to_string(index=False))
print(pd.DataFrame(tr_rows).round(3).to_string(index=False))
if hz_rows:
    print(pd.DataFrame(hz_rows)[["horizon_min", "f1", "persistence_f1", "state_change_rate"]].round(3).to_string(index=False))
print(f"elapsed {time.time() - t0:.0f}s")
spark.stop()
