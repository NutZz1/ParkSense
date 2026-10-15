# Data dictionary

## Raw availability (data.gov.sg `transport/carpark-availability`, HDB)
| Field | Type | Notes |
|---|---|---|
| items[0].timestamp | ISO time, +08:00 | snapshot time; must be within ±10 min of the requested `date_time` |
| carpark_number | string | join key to HDB Carpark Information `car_park_no` |
| update_datetime | SGT, no offset | last sensor update; staleness = snapshot − update |
| carpark_info[].lot_type | C / Y / H | car / motorcycle / heavy vehicle (other codes possible) |
| carpark_info[].total_lots | int (string in JSON) | cast; ≤ 0 dropped |
| carpark_info[].lots_available | int (string in JSON) | cast; clipped to [0, total] |

## Flat line (written by `ingest/flatten.py`, read by Scala)
`snapshot_ts_sgt \t carpark_number \t update_datetime_sgt \t C:105:102|Y:20:20|H:1:0` — one line per (snapshot, carpark);
lot items `lot_type:total:available` are packed in the last field, so the Scala job uses `flatMap` to expand them.

## HDB Carpark Information (dataset d_23f946fa557947f93a8043bbef41dd09)
`car_park_no, address, x_coord, y_coord (SVY21 / EPSG:3414), car_park_type, type_of_parking_system, short_term_parking,
free_parking, night_parking, car_park_decks, gantry_height, car_park_basement` + derived `lat, lon` (WGS84, pyproj).

## Clean occupancy (Scala → Parquet `/parksense/processed/occupancy`, partitioned by `dt`)
| Field | Type | Notes |
|---|---|---|
| carpark_no, lot_type | string | ML uses C only |
| snapshot_ts / snapshot_ms | timestamp / long | 15-min grid time (backfill) or poll time (live) |
| update_ts, staleness_min | timestamp, int | readings with staleness > 30 min are dropped |
| total_lots, lots_available | int | available clipped to [0, total] |
| occupancy, avail_ratio | double | 1 − available/total, available/total |
| hour, minute_of_day, dow (1=Mon), is_weekend, is_holiday | | SGT calendar |
| car_park_type … address, lat, lon, has_meta | | broadcast join with metadata (null if unmatched) |
| dt | string/date | partition column YYYY-MM-DD |

## Features (`pyspark_jobs/features.py`, one row per carpark × 15-min slot)
| Group | Columns |
|---|---|
| Now | avail_ratio, occupancy, full_now, total_lots |
| Lags (self-join on slot) | ratio_lag15, ratio_lag30, ratio_lag60, ratio_lag120; delta_15, delta_60 |
| Rolling (rangeBetween on slots) | roll_mean_1h, roll_std_1h, roll_mean_3h, roll_std_3h |
| Seasonal | ratio_1d, ratio_7d (−1 if missing) + has_1d, has_7d |
| Calendar | hour_sin, hour_cos, minute_of_day, dow, is_weekend, is_holiday |
| Metadata | car_park_type, type_of_parking_system (StringIndexer + OneHotEncoder), free_parking_flag, night_parking_flag, basement_flag, car_park_decks |
| Target encoding (train period only) | te_chd = smoothed full-rate per carpark × hour × dow; te_ch = per carpark × hour |
| Labels | label_15, label_30, label_60 (+ future_ratio_*) |

## HBase (namespace `parksense`)
See `architecture.md` for row keys and column families.
