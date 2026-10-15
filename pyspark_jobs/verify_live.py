"""Verify live predictions once their target time has passed (genuinely unseen data).

predictions_log rows with target_at <= now - 2 min and no `a` columns -> actual reading nearest the target from
carpark_history (±2 min) -> a:actual_* columns. Running accuracy/precision/recall/F1 (and persistence on the
same rows) -> HBase metrics `live:eval` + outputs/metrics/live_eval.csv / live_eval_summary.json.

Usage: python pyspark_jobs/verify_live.py [--loop 300]
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from spark_session import CFG, ROOT, get_logger, path

sys.path.insert(0, str(ROOT / "ingest"))
sys.path.insert(0, str(ROOT / "pyspark_jobs"))
import hbase_io  # noqa: E402
from ml_utils import metrics  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--loop", type=int, default=0)
args = ap.parse_args()
log = get_logger("verify_live")
M = path("metrics")
full = CFG["thresholds"]["full_ratio"]


def verify_once():
    now_ms = int(time.time() * 1000)
    due, fresh = [], 0
    for key, v in hbase_io.scan_predictions_log(only_unverified=True):
        cp, target_ms = key.split("#")
        if int(target_ms) > now_ms - 120000:
            continue
        r = hbase_io.get_reading_near(cp, int(target_ms))
        if r is None:
            continue
        ratio = r["available"] / r["total"] if r["total"] else 0.0
        due.append({"key": key, "actual_available": r["available"], "actual_ratio": round(ratio, 4), "actual_full": ratio <= full})
    if due:
        fresh = hbase_io.put_actuals(due)
    rows = []
    for key, v in hbase_io.scan_predictions_log(only_unverified=False):
        if "a:verified_at" not in v:
            continue
        cp, target_ms = key.split("#")
        rows.append({"carpark": cp, "target_at": datetime.fromtimestamp(int(target_ms) / 1000, timezone.utc).isoformat(),
                     "prob": float(v["p:prob"]), "pred": int(v["p:pred"]), "current_avail_ratio": float(v.get("p:current_avail_ratio") or "nan"),
                     "actual_ratio": float(v["a:actual_ratio"]), "actual_full": int(v["a:actual_full"]), "made_at": v["p:made_at"],
                     "verified_at": v["a:verified_at"]})
    if not rows:
        log.info("nothing verifiable yet")
        return
    ev = pd.DataFrame(rows)
    ev["persist"] = (ev.current_avail_ratio <= full).astype(int)
    ev.to_csv(M / "live_eval.csv", index=False)
    m, pm = metrics(ev.actual_full.values, ev.pred.values), metrics(ev.actual_full.values, ev.persist.values)
    tr = ev[ev.actual_full != ev.persist]
    summary = {"verified": len(ev), "new_this_run": fresh, "first_target": ev.target_at.min(), "last_target": ev.target_at.max(),
               "positive_rate": float(ev.actual_full.mean()), "model": {k: m[k] for k in ("accuracy", "precision", "recall", "f1", "tp", "fp", "tn", "fn")},
               "persistence": {k: pm[k] for k in ("accuracy", "precision", "recall", "f1")},
               "transitions": {"n": len(tr), "model_correct_share": float((tr.pred == tr.actual_full).mean()) if len(tr) else None},
               "updated_utc": datetime.now(timezone.utc).isoformat()}
    (M / "live_eval_summary.json").write_text(json.dumps(summary, indent=2))
    try:
        hbase_io.put_metric("live:eval", summary)
    except Exception as e:  # noqa: BLE001
        log.warning("HBase metric write failed: %s", e)
    log.info("verified %d (+%d): acc %.4f P %.3f R %.3f F1 %.3f | persistence F1 %.3f | transitions %d", len(ev), fresh,
             m["accuracy"], m["precision"], m["recall"], m["f1"], pm["f1"], len(tr))


while True:
    try:
        verify_once()
    except Exception as e:  # noqa: BLE001
        log.exception("verify failed: %s", e)
    if not args.loop:
        break
    time.sleep(args.loop)
