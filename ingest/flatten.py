"""Snapshot JSON -> flat line format shared by backfill and the live poller (read by Scala).

One tab-separated line per (snapshot, carpark):
    snapshot_ts_sgt \\t carpark_number \\t update_datetime_sgt \\t C:105:102|Y:20:20|H:1:0
(lot_type:total_lots:lots_available joined by '|'; numbers arrive as strings and are cast.)
"""
from common import parse_sgt


def snapshot_ts(js):
    items = js.get("items") or []
    if not items or "timestamp" not in items[0]:
        raise ValueError("snapshot without items[0].timestamp")
    return parse_sgt(items[0]["timestamp"])


def snapshot_json_to_lines(js, snapshot_override=None):
    """Returns (lines, n_bad). snapshot_override (SGT datetime) is the grid time for backfill rows."""
    items = js.get("items") or []
    if not items or "carpark_data" not in items[0]:
        raise ValueError("snapshot without items[0].carpark_data")
    ts = snapshot_override or snapshot_ts(js)
    ts_s = ts.strftime("%Y-%m-%dT%H:%M:%S")
    lines, bad = [], 0
    for cp in items[0]["carpark_data"]:
        try:
            lots = "|".join(f"{li['lot_type'].strip()}:{int(li['total_lots'])}:{int(li['lots_available'])}"
                            for li in cp["carpark_info"])
            upd = parse_sgt(cp["update_datetime"]).strftime("%Y-%m-%dT%H:%M:%S")
            num = cp["carpark_number"].strip()
            if not num or not lots:
                raise ValueError("empty")
            lines.append(f"{ts_s}\t{num}\t{upd}\t{lots}")
        except (KeyError, ValueError, TypeError, AttributeError):
            bad += 1
    return lines, bad
