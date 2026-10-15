"""HappyBase helpers over the HBase Thrift server (port 9090).

Tables (namespace parksense): carpark_live, carpark_history, predictions_log, metrics — see hbase/create_tables.hbase.
Every function fails soft (logs and returns) so the live layer keeps running if HBase is down.
"""
import json
import time
from contextlib import contextmanager
from functools import wraps
from datetime import datetime, timezone

import happybase
from thriftpy2.thrift import TException

from common import CFG, SGT

NS = CFG["hbase"]["namespace"]
LONG_MAX = 9223372036854775807


@contextmanager
def conn():
    """A fresh Thrift connection per operation: the Thrift server closes idle sockets after ~60 s,
    so pooled connections go stale between polls (seen as 'Broken pipe')."""
    h = CFG["hbase"]
    c = happybase.Connection(host=h["thrift_host"], port=h["thrift_port"], timeout=20000)
    try:
        yield c
    finally:
        c.close()


def retry(fn):
    """Retry an HBase operation once on a transport error."""
    @wraps(fn)
    def w(*a, **k):
        try:
            return fn(*a, **k)
        except (OSError, TException):
            time.sleep(1)
            return fn(*a, **k)
    return w


def t(name):
    return f"{NS}:{name}"


def b(v):
    return b"" if v is None else str(v).encode()


def rev_ts(epoch_ms):
    """Long.MAX_VALUE - epoch_ms, zero-padded to 19 digits: newest rows sort first."""
    return f"{LONG_MAX - int(epoch_ms):019d}"


def history_key(carpark, epoch_ms):
    return f"{carpark}#{rev_ts(epoch_ms)}".encode()


@retry
def put_live_batch(rows):
    """rows: dicts with carpark, total, available, occupancy, update_ts, snapshot_ts -> carpark_live:s."""
    with conn() as c:
        with c.table(t("carpark_live")).batch(batch_size=1000) as bt:
            for r in rows:
                bt.put(r["carpark"].encode(), {b"s:total": b(r["total"]), b"s:available": b(r["available"]),
                                               b"s:occupancy": b(r["occupancy"]), b"s:update_ts": b(r["update_ts"]),
                                               b"s:snapshot_ts": b(r["snapshot_ts"])})
    return len(rows)


@retry
def put_history_batch(rows):
    """rows: dicts with carpark, epoch_ms (snapshot), available, total, occupancy -> carpark_history:s."""
    with conn() as c:
        with c.table(t("carpark_history")).batch(batch_size=2000) as bt:
            for r in rows:
                bt.put(history_key(r["carpark"], r["epoch_ms"]),
                       {b"s:available": b(r["available"]), b"s:total": b(r["total"]), b"s:occupancy": b(r["occupancy"])})
    return len(rows)


@retry
def put_meta_batch(rows):
    """Static metadata -> carpark_live:m (written once)."""
    with conn() as c:
        with c.table(t("carpark_live")).batch(batch_size=1000) as bt:
            for r in rows:
                bt.put(r["car_park_no"].encode(), {b"m:address": b(r["address"]), b"m:lat": b(r["lat"]), b"m:lon": b(r["lon"]),
                                                   b"m:type": b(r["car_park_type"]), b"m:system": b(r["type_of_parking_system"]),
                                                   b"m:free_parking": b(r["free_parking"]), b"m:night_parking": b(r["night_parking"])})
    return len(rows)


@retry
def put_predictions(rows, model_ver):
    """rows: carpark, prob, pred, made_at, target_at (SGT ISO), target_ms, threshold.
    Writes carpark_live:p and predictions_log:p (row key carpark#target_epoch_ms)."""
    with conn() as c:
        live, plog = c.table(t("carpark_live")), c.table(t("predictions_log"))
        with live.batch(batch_size=1000) as bl, plog.batch(batch_size=1000) as bp:
            for r in rows:
                bl.put(r["carpark"].encode(), {b"p:p_full_30": b(r["prob"]), b"p:pred_full_30": b(int(r["pred"])),
                                               b"p:made_at": b(r["made_at"]), b"p:target_at": b(r["target_at"]),
                                               b"p:model_ver": b(model_ver)})
                bp.put(f"{r['carpark']}#{r['target_ms']}".encode(),
                       {b"p:prob": b(r["prob"]), b"p:pred": b(int(r["pred"])), b"p:made_at": b(r["made_at"]),
                        b"p:threshold": b(r["threshold"]), b"p:current_avail_ratio": b(r.get("current_avail_ratio"))})
    return len(rows)


@retry
def put_metric(name, obj):
    with conn() as c:
        c.table(t("metrics")).put(name.encode(), {b"v:json": json.dumps(obj, default=str).encode(),
                                                  b"v:updated_at": datetime.now(SGT).isoformat().encode()})


@retry
def get_metric(name):
    with conn() as c:
        row = c.table(t("metrics")).row(name.encode())
    return json.loads(row[b"v:json"]) if row else None


@retry
def get_history(carpark, hours=24, now_ms=None):
    """Newest-first prefix scan of carpark_history, stopping after `hours`."""
    now_ms = now_ms or int(time.time() * 1000)
    stop_ms = now_ms - hours * 3600 * 1000
    out = []
    with conn() as c:
        for k, v in c.table(t("carpark_history")).scan(row_prefix=f"{carpark}#".encode()):
            ms = LONG_MAX - int(k.decode().split("#")[1])
            if ms < stop_ms:
                break
            out.append({"epoch_ms": ms, "available": int(v[b"s:available"]), "total": int(v[b"s:total"]),
                        "occupancy": float(v[b"s:occupancy"])})
    return out


def scan_predictions_log(only_unverified=True):
    with conn() as c:
        for k, v in c.table(t("predictions_log")).scan():
            if only_unverified and b"a:verified_at" in v:
                continue
            yield k.decode(), {kk.decode(): vv.decode() for kk, vv in v.items()}


@retry
def put_actuals(rows):
    """rows: key, actual_available, actual_ratio, actual_full -> predictions_log:a."""
    now = datetime.now(SGT).isoformat()
    with conn() as c:
        with c.table(t("predictions_log")).batch(batch_size=1000) as bt:
            for r in rows:
                bt.put(r["key"].encode(), {b"a:actual_available": b(r["actual_available"]),
                                           b"a:actual_ratio": b(r["actual_ratio"]),
                                           b"a:actual_full": b(int(r["actual_full"])), b"a:verified_at": now.encode()})
    return len(rows)


def ping():
    try:
        with conn() as c:
            return sorted(x.decode() for x in c.tables() if x.decode().startswith(NS + ":"))
    except Exception as e:  # noqa: BLE001
        return f"HBase unavailable: {e}"


def utc_iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()
