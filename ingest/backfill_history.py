"""Backfill historical snapshots on a 15-min SGT grid (newest first, resumable, rate-limited).

Usage: python ingest/backfill_history.py [--days 30] [--interval 15] [--end 2026-10-15T12:00]
Saves data/raw/snapshots/YYYY-MM-DD/HHMM.json.gz and data/raw/flat/dt=YYYY-MM-DD/HHMM.txt,
then data/raw/backfill_meta.json.
"""
import argparse
import gzip
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from common import CFG, RateLimitedSession, SGT, fmt_sgt, get_logger, now_sgt, p, write_atomic
from flatten import snapshot_json_to_lines, snapshot_ts

ap = argparse.ArgumentParser()
ap.add_argument("--days", type=int, default=CFG["backfill"]["days"])
ap.add_argument("--interval", type=int, default=CFG["backfill"]["interval_min"])
ap.add_argument("--end", help="SGT grid end, default = now floored to the interval")
ap.add_argument("--rate", type=int, default=4, help="max requests per 10 s (leave room for the live poller)")
ap.add_argument("--workers", type=int, default=3)
args = ap.parse_args()

log = get_logger("backfill")
sess = RateLimitedSession(log, max_per_10s=args.rate)
snap_dir, flat_dir = p("snapshots"), p("flat")
tol = timedelta(minutes=CFG["api"]["timestamp_tolerance_min"])

end = datetime.fromisoformat(args.end).replace(tzinfo=SGT) if args.end else now_sgt()
end = end.replace(second=0, minute=end.minute - end.minute % args.interval)
n = args.days * 24 * 60 // args.interval
grid = [end - timedelta(minutes=args.interval * i) for i in range(n)]  # newest first
log.info("backfill %d snapshots: %s .. %s", n, fmt_sgt(grid[-1]), fmt_sgt(grid[0]))

t0 = time.time()
saved = skipped = failed = mismatched = 0
counts, flat_lines, bad_total = [], 0, 0


def fetch(ts):
    """Returns ('skip'|'saved'|'failed', n_lines, n_bad, mismatch)."""
    day, hhmm = ts.strftime("%Y-%m-%d"), ts.strftime("%H%M")
    jf = snap_dir / day / f"{hhmm}.json.gz"
    ff = flat_dir / f"dt={day}" / f"{hhmm}.txt"
    if ff.exists() and jf.exists():
        with open(ff) as fh:
            return "skip", sum(1 for _ in fh), 0, False
    try:
        js = sess.get_json(CFG["api"]["availability_url"], {"date_time": fmt_sgt(ts)})
        for shift in (1, 2):  # the API returns {"items": []} for exactly 00:00:00; try a minute later
            if js.get("items"):
                break
            js = sess.get_json(CFG["api"]["availability_url"], {"date_time": fmt_sgt(ts + timedelta(minutes=shift))})
        got = snapshot_ts(js)
        mism = abs(got - ts) > tol
        if mism:
            log.warning("timestamp mismatch: requested %s got %s", fmt_sgt(ts), got.isoformat())
        lines, bad = snapshot_json_to_lines(js, snapshot_override=ts)
        jf.parent.mkdir(parents=True, exist_ok=True)
        ff.parent.mkdir(parents=True, exist_ok=True)
        tmp = jf.with_name(jf.name + ".tmp")
        with gzip.open(tmp, "wt", encoding="utf-8") as g:
            json.dump(js, g)
        tmp.replace(jf)
        write_atomic(ff, "\n".join(lines) + "\n")
        return "saved", len(lines), bad, mism
    except Exception as e:  # noqa: BLE001 - keep going, resumable
        log.error("snapshot %s failed: %s", fmt_sgt(ts), e)
        return "failed", 0, 0, False


with ThreadPoolExecutor(max_workers=args.workers) as ex:
    for k, (status, c, bad, mism) in enumerate(ex.map(fetch, grid), 1):  # map keeps newest-first order
        if status == "skip":
            skipped += 1
        elif status == "saved":
            saved += 1
        else:
            failed += 1
        if status != "failed":
            counts.append(c)
            flat_lines += c
        bad_total += bad
        mismatched += int(mism)
        if k % 20 == 0 or k == n:
            el = time.time() - t0
            eta = el / k * (n - k)
            log.info("progress %d/%d (saved %d, skipped %d, failed %d) elapsed %.0fs ETA %.0f min",
                     k, n, saved, skipped, failed, el, eta / 60)

meta = {"requested": n, "saved_now": saved, "skipped_existing": skipped, "failed": failed,
        "timestamp_mismatches": mismatched, "interval_min": args.interval, "days": args.days,
        "grid_start_sgt": fmt_sgt(grid[-1]), "grid_end_sgt": fmt_sgt(grid[0]),
        "carparks_per_snapshot": {"min": min(counts) if counts else None,
                                  "median": statistics.median(counts) if counts else None,
                                  "max": max(counts) if counts else None},
        "total_flat_lines": flat_lines, "bad_carpark_records": bad_total, "http_429": sess.n429,
        "wall_seconds": round(time.time() - t0, 1), "finished_sgt": fmt_sgt(now_sgt())}
write_atomic(p("raw") / "backfill_meta.json", json.dumps(meta, indent=2))
print("\n=== backfill summary ===")
for k_, v in meta.items():
    print(f"  {k_:<24} {v}")
