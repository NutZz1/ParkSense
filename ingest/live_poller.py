"""Live poller: every 60 s fetch the current snapshot (always with date_time = now SGT, because a request
without it once returned a 13 h old cached response), write flat lines atomically for StreamingJob,
archive them, and update HBase carpark_live (s) + carpark_history.

Usage: python ingest/live_poller.py [--interval 60] [--once]
"""
import argparse
import csv
import json
import time

from common import CFG, RateLimitedSession, fmt_sgt, get_logger, now_sgt, p, parse_sgt, write_atomic
from flatten import snapshot_json_to_lines, snapshot_ts

ap = argparse.ArgumentParser()
ap.add_argument("--interval", type=int, default=CFG["live"]["poll_seconds"])
ap.add_argument("--once", action="store_true")
args = ap.parse_args()

log = get_logger("poller")
sess = RateLimitedSession(log, max_per_10s=1)  # the backfill may use the other request slots
incoming, archive = p("stream_incoming"), p("stream_archive")
state_f = p("stream_state")
stats_f = p("metrics") / "poller_stats.csv"
stale_max = CFG["thresholds"]["stale_max_min"]

try:
    import hbase_io
    hb = hbase_io.ping()
    log.info("HBase: %s", hb)
    if isinstance(hb, str):
        hbase_io = None
except Exception as e:  # noqa: BLE001
    log.warning("HBase disabled: %s", e)
    hbase_io = None

state = json.loads(state_f.read_text()) if state_f.exists() else {}
if hbase_io and not state.get("meta_written"):
    try:
        with open(p("metadata_csv"), encoding="utf-8") as fh:
            n = hbase_io.put_meta_batch(list(csv.DictReader(fh)))
        state["meta_written"] = True
        log.info("wrote static metadata for %d carparks to carpark_live:m", n)
    except Exception as e:  # noqa: BLE001
        log.warning("metadata write failed: %s", e)


def to_rows(lines):
    live, hist, stale = [], [], 0
    for ln in lines:
        snap, cp, upd, lots = ln.split("\t")
        s, u = parse_sgt(snap), parse_sgt(upd)
        st = (s - u).total_seconds() / 60
        if st > stale_max:
            stale += 1
        for item in lots.split("|"):
            lt, tot, av = item.split(":")
            if lt != "C" or int(tot) <= 0:
                continue
            tot, av = int(tot), min(max(int(av), 0), int(tot))
            occ = round(1 - av / tot, 4)
            live.append({"carpark": cp, "total": tot, "available": av, "occupancy": occ, "update_ts": upd, "snapshot_ts": snap})
            hist.append({"carpark": cp, "epoch_ms": int(s.timestamp() * 1000), "available": av, "total": tot, "occupancy": occ})
    return live, hist, stale


def poll_once():
    t_req = now_sgt()
    js = sess.get_json(CFG["api"]["availability_url"], {"date_time": fmt_sgt(t_req)})
    t_resp = now_sgt()
    ts = snapshot_ts(js)
    if state.get("last_timestamp") == ts.isoformat():
        log.info("unchanged snapshot %s, skipped", ts.isoformat())
        return
    lines, bad = snapshot_json_to_lines(js)
    epoch = int(time.time())
    body = "\n".join(lines) + "\n"
    write_atomic(incoming / f"{epoch}.txt", body)   # .tmp then rename: Spark never sees partial files
    day_dir = archive / f"dt={ts:%Y-%m-%d}"
    day_dir.mkdir(parents=True, exist_ok=True)
    write_atomic(day_dir / f"{ts:%H%M}.txt", body)
    live, hist, stale = to_rows(lines)
    hb_rows, hb_s = 0, 0.0
    if hbase_io:
        try:
            t0 = time.time()
            hb_rows = hbase_io.put_live_batch(live) + hbase_io.put_history_batch(hist)
            hb_s = time.time() - t0
        except Exception as e:  # noqa: BLE001 - never crash on HBase
            log.warning("HBase write failed: %s", e)
    lag = (t_resp - ts).total_seconds()
    new = not stats_f.exists()
    with open(stats_f, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["poll_sgt", "snapshot_ts", "carparks", "bad", "api_lag_s", "stale_share", "hbase_rows",
                        "hbase_seconds", "hbase_rows_per_s"])
        w.writerow([t_resp.isoformat(), ts.isoformat(), len(lines), bad, round(lag, 1), round(stale / max(len(lines), 1), 4),
                    hb_rows, round(hb_s, 2), round(hb_rows / hb_s, 0) if hb_s else ""])
    state["last_timestamp"] = ts.isoformat()
    state["last_poll_sgt"] = t_resp.isoformat()
    write_atomic(state_f, json.dumps(state))
    log.info("snapshot %s: %d carparks, lag %.0fs, stale %.1f%%, HBase %d rows in %.1fs",
             ts.isoformat(), len(lines), lag, 100 * stale / max(len(lines), 1), hb_rows, hb_s)


log.info("poller started (interval %ss)", args.interval)
while True:
    t0 = time.time()
    try:
        poll_once()
    except Exception as e:  # noqa: BLE001
        log.error("poll failed: %s", e)
    if args.once:
        break
    time.sleep(max(1, args.interval - (time.time() - t0)))
