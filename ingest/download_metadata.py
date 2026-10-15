"""HDB Carpark Information (data.gov.sg datastore API) -> data/raw/carpark_info.csv with WGS84 lat/lon.

SVY21 (EPSG:3414) x_coord/y_coord are converted with pyproj; every point must fall inside
Singapore's bounding box (≈1.15–1.48 N, 103.6–104.1 E).
"""
import csv

from pyproj import Transformer

from common import CFG, RateLimitedSession, get_logger, p

log = get_logger("download_metadata")
sess = RateLimitedSession(log)
COLS = ["car_park_no", "address", "x_coord", "y_coord", "car_park_type", "type_of_parking_system",
        "short_term_parking", "free_parking", "night_parking", "car_park_decks", "gantry_height", "car_park_basement"]

rows, offset = [], 0
while True:
    js = sess.get_json(CFG["api"]["metadata_url"], {"resource_id": CFG["api"]["metadata_resource_id"],
                                                    "limit": 1000, "offset": offset})
    if not js.get("success"):
        raise SystemExit(f"datastore_search failed: {js}. Download the CSV manually from data.gov.sg into data/raw/.")
    recs = js["result"]["records"]
    rows += recs
    total = js["result"].get("total")
    log.info("fetched %d (total %s)", len(rows), total)
    if not recs or (total is not None and len(rows) >= total):
        break
    offset += len(recs)

tf = Transformer.from_crs("EPSG:3414", "EPSG:4326", always_xy=True)
out, bad_bbox = [], []
for r in rows:
    lon, lat = tf.transform(float(r["x_coord"]), float(r["y_coord"]))
    rec = {c: r.get(c, "") for c in COLS}
    rec["lat"], rec["lon"] = round(lat, 6), round(lon, 6)
    if not (1.15 <= lat <= 1.48 and 103.6 <= lon <= 104.1):
        bad_bbox.append(r["car_park_no"])
    out.append(rec)

f = p("metadata_csv")
with open(f, "w", newline="", encoding="utf-8") as fh:
    w = csv.DictWriter(fh, fieldnames=COLS + ["lat", "lon"])
    w.writeheader()
    w.writerows(out)

print("\n=== download_metadata summary ===")
print(f"  carparks: {len(out)}  (unique ids {len({r['car_park_no'] for r in out})})")
print(f"  outside Singapore bbox: {len(bad_bbox)} {bad_bbox[:10]}")
print(f"  lat range {min(r['lat'] for r in out)}..{max(r['lat'] for r in out)}, lon range {min(r['lon'] for r in out)}..{max(r['lon'] for r in out)}")
print(f"  wrote {f}")
