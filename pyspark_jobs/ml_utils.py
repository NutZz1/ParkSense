"""Shared ML helpers: time-based split, pipelines, numpy metrics, threshold selection."""
import json

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from features import CATEGORICAL, NUMERIC
from spark_session import CFG, path

SEED = CFG["project"]["random_seed"]
NAMES = {"rf": "Random Forest", "gbt": "Gradient-Boosted Trees"}
GRIDS = {"rf": [{"numTrees": 60, "maxDepth": d} for d in (10, 14)],
         "gbt": [{"maxIter": i, "maxDepth": 6} for i in (40, 80)]}


def split_boundaries(df, label_col):
    """Time split on the labelled timeline: first 70% train, next 15% validation, last 15% test.
    With >= 10 days the cut points are snapped to midnight SGT so whole days stay together."""
    d = df.filter(F.col(label_col).isNotNull())
    days = sorted(str(r[0]) for r in d.select("dt").distinct().collect())  # dt is a date (partition discovery)
    if len(days) >= 10:
        n = len(days)
        tr, va = round(0.70 * n), round(0.85 * n)
        b1 = d.filter(F.col("dt") == days[tr]).agg(F.min("slot")).first()[0]
        b2 = d.filter(F.col("dt") == days[va]).agg(F.min("slot")).first()[0]
        return int(b1), int(b2), {"mode": "days", "train_days": days[:tr], "val_days": days[tr:va], "test_days": days[va:]}
    b1, b2 = d.approxQuantile("slot", [0.70, 0.85], 0.0)
    return int(b1), int(b2), {"mode": "timeline-quantiles (fewer than 10 days available)"}


def apply_split(df, label_col, b1, b2, horizon_min):
    emb = horizon_min // 15  # embargo: no training label may fall inside validation/test
    d = df.filter(F.col(label_col).isNotNull()).withColumn("label", F.col(label_col))
    return (d.filter(F.col("slot") + emb < b1), d.filter((F.col("slot") >= b1) & (F.col("slot") + emb < b2)),
            d.filter(F.col("slot") >= b2))


def add_weights(train, *others):
    n, pos = train.count(), train.agg(F.sum("label")).first()[0]
    w_pos, w_neg = n / (2.0 * pos), n / (2.0 * (n - pos))
    f = lambda d: d.withColumn("weight", F.when(F.col("label") == 1, w_pos).otherwise(w_neg))  # noqa: E731
    return (f(train),) + tuple(f(o) for o in others) + ({"w_pos": w_pos, "w_neg": w_neg, "train_rows": n, "train_pos": pos},)


def make_pipeline(algo, params, numeric=None):
    idx = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATEGORICAL]
    ohe = OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL], outputCols=[f"{c}_ohe" for c in CATEGORICAL],
                        handleInvalid="keep")
    asm = VectorAssembler(inputCols=(numeric or NUMERIC) + [f"{c}_ohe" for c in CATEGORICAL], outputCol="features",
                          handleInvalid="keep")
    if algo == "rf":
        clf = RandomForestClassifier(labelCol="label", weightCol="weight", seed=SEED, subsamplingRate=0.8,
                                     featureSubsetStrategy="sqrt", maxBins=64, **params)
    elif algo == "gbt":
        clf = GBTClassifier(labelCol="label", weightCol="weight", seed=SEED, stepSize=0.1, subsamplingRate=0.8,
                            maxBins=64, **params)
    else:
        raise ValueError(algo)
    return Pipeline(stages=idx + [ohe, asm, clf])


def probabilities(model, df, keep=("carpark_no", "slot", "hour", "dow", "is_weekend", "car_park_type", "label", "full_now", "dt")):
    out = model.transform(df).withColumn("p", vector_to_array("probability")[1])
    return out.select(*[c for c in keep if c in out.columns], "p").toPandas()


# ------------------------------------------------------------------------------------------------ metrics
def confusion(y, pred):
    y, pred = np.asarray(y).astype(int), np.asarray(pred).astype(int)
    return (int(((y == 1) & (pred == 1)).sum()), int(((y == 0) & (pred == 1)).sum()),
            int(((y == 0) & (pred == 0)).sum()), int(((y == 1) & (pred == 0)).sum()))


def roc_auc(y, s):
    y, s = np.asarray(y).astype(int), np.asarray(s, dtype=float)
    pos, neg = y.sum(), len(y) - y.sum()
    if pos == 0 or neg == 0:
        return float("nan")
    order = np.argsort(s, kind="mergesort")
    ss = s[order]
    ranks = np.empty(len(s))
    i = 0
    while i < len(s):  # average ranks over ties
        j = i
        while j + 1 < len(s) and ss[j + 1] == ss[i]:
            j += 1
        ranks[order[i:j + 1]] = (i + j) / 2.0 + 1
        i = j + 1
    return float((ranks[y == 1].sum() - pos * (pos + 1) / 2) / (pos * neg))


def pr_auc(y, s):
    y, s = np.asarray(y).astype(int), np.asarray(s, dtype=float)
    if y.sum() == 0:
        return float("nan")
    ys = y[np.argsort(-s, kind="mergesort")]
    tp = np.cumsum(ys)
    return float(((tp / np.arange(1, len(y) + 1)) * ys).sum() / y.sum())


def metrics(y, pred, score=None):
    tp, fp, tn, fn = confusion(y, pred)
    n = tp + fp + tn + fn
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    p0 = tn / (tn + fn) if tn + fn else 0.0
    r0 = tn / (tn + fp) if tn + fp else 0.0
    f0 = 2 * p0 * r0 / (p0 + r0) if p0 + r0 else 0.0
    m = {"n": n, "accuracy": (tp + tn) / n if n else 0.0, "precision": prec, "recall": rec, "f1": f1,
         "macro_f1": (f1 + f0) / 2, "tp": tp, "fp": fp, "tn": tn, "fn": fn}
    if score is not None:
        m["roc_auc"], m["pr_auc"] = roc_auc(y, score), pr_auc(y, score)
    return m


def threshold_curve(y, p):
    rows = []
    for t in np.round(np.arange(0.01, 0.991, 0.01), 2):
        m = metrics(y, p >= t)
        rows.append({"threshold": float(t), "precision": m["precision"], "recall": m["recall"], "f1": m["f1"]})
    return rows


def choose_threshold(y, p):
    """Max-F1 threshold on validation (the precision/recall trade-off is saved alongside)."""
    curve = threshold_curve(y, p)
    best = max(curve, key=lambda r: r["f1"])
    return best, curve


def save_json(obj, name):
    with open(path("metrics") / name, "w") as f:
        json.dump(obj, f, indent=2, default=str)
