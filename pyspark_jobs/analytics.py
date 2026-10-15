"""Phase 4 — eight numbered Spark SQL queries over the Scala output (temp views on Parquet/CSV, no Hive).

Each result -> outputs/analytics/qN_<name>.csv with its runtime (outputs/analytics/query_times.csv), and
-> HBase parksense:metrics as analytics:qN (if HBase is up).
Usage: python pyspark_jobs/analytics.py
"""
import json
import time

import pandas as pd

from spark_session import CFG, ROOT, get_logger, get_spark, lake, path

log = get_logger("analytics")
A = path("analytics")
full = CFG["thresholds"]["full_ratio"]
spark = get_spark("ParkSense-Analytics")
spark.read.parquet(lake("processed/occupancy")).createOrReplaceTempView("occupancy")
spark.read.option("header", True).option("inferSchema", True).csv(lake("raw/meta/carpark_info.csv")).createOrReplaceTempView("carpark_info")
spark.read.parquet(lake("processed/excluded_carparks")).createOrReplaceTempView("excluded")
# car lots of reliable sensors only (always-zero / never-changing carparks excluded, as in the ML)
spark.sql("""CREATE OR REPLACE TEMP VIEW car AS
             SELECT o.* FROM occupancy o LEFT ANTI JOIN excluded e ON o.carpark_no = e.carpark_no
             WHERE o.lot_type = 'C'""")

Q = {
    "q1_occupancy_by_hour_weekday_weekend": """
        SELECT hour, CASE WHEN is_weekend THEN 'weekend' ELSE 'weekday' END AS day_type,
               ROUND(AVG(occupancy), 4) AS mean_occupancy,
               ROUND(AVG(CASE WHEN avail_ratio <= {f} THEN 1 ELSE 0 END), 4) AS nearly_full_rate, COUNT(*) AS records
        FROM car GROUP BY hour, is_weekend ORDER BY day_type, hour""",
    "q2_top20_most_often_full": """
        SELECT carpark_no, FIRST(address) AS address, FIRST(car_park_type) AS car_park_type,
               ROUND(AVG(CASE WHEN avail_ratio <= {f} THEN 1 ELSE 0 END), 4) AS nearly_full_rate,
               ROUND(AVG(occupancy), 4) AS mean_occupancy, MAX(total_lots) AS total_lots, COUNT(*) AS records
        FROM car GROUP BY carpark_no ORDER BY nearly_full_rate DESC, mean_occupancy DESC LIMIT 20""",
    "q3_full_rate_by_type_and_system": """
        SELECT COALESCE(car_park_type, 'UNKNOWN') AS car_park_type, COALESCE(type_of_parking_system, 'UNKNOWN') AS parking_system,
               COUNT(DISTINCT carpark_no) AS carparks,
               ROUND(AVG(CASE WHEN avail_ratio <= {f} THEN 1 ELSE 0 END), 4) AS nearly_full_rate,
               ROUND(AVG(occupancy), 4) AS mean_occupancy
        FROM car GROUP BY 1, 2 ORDER BY nearly_full_rate DESC""",
    "q4_policy_effect_evening": """
        SELECT free_parking, night_parking, COUNT(DISTINCT carpark_no) AS carparks,
               ROUND(AVG(occupancy), 4) AS evening_mean_occupancy,
               ROUND(AVG(CASE WHEN avail_ratio <= {f} THEN 1 ELSE 0 END), 4) AS evening_nearly_full_rate
        FROM car WHERE hour BETWEEN 18 AND 23 AND free_parking IS NOT NULL
        GROUP BY free_parking, night_parking ORDER BY evening_mean_occupancy DESC""",
    "q5_busiest_day_per_type": """
        WITH d AS (SELECT COALESCE(car_park_type,'UNKNOWN') AS car_park_type, dow, AVG(occupancy) AS mean_occupancy
                   FROM car GROUP BY 1, 2),
             r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY car_park_type ORDER BY mean_occupancy DESC) AS rk FROM d)
        SELECT car_park_type, dow AS busiest_dow, ROUND(mean_occupancy, 4) AS mean_occupancy FROM r WHERE rk = 1
        ORDER BY mean_occupancy DESC""",
    "q6_daily_citywide_full_rate": """
        SELECT dt, ROUND(AVG(CASE WHEN avail_ratio <= {f} THEN 1 ELSE 0 END), 4) AS nearly_full_rate,
               ROUND(AVG(occupancy), 4) AS mean_occupancy, COUNT(DISTINCT snapshot_ms) AS snapshots, COUNT(*) AS records
        FROM car GROUP BY dt ORDER BY dt""",
    "q7_evening_vs_morning_swing": """
        WITH p AS (SELECT carpark_no, FIRST(address) AS address, FIRST(car_park_type) AS car_park_type,
                          AVG(CASE WHEN hour BETWEEN 19 AND 22 THEN occupancy END) AS evening,
                          AVG(CASE WHEN hour BETWEEN 9 AND 12 THEN occupancy END) AS morning
                   FROM car WHERE NOT is_weekend GROUP BY carpark_no)
        SELECT carpark_no, address, car_park_type, ROUND(evening, 4) AS evening_occ, ROUND(morning, 4) AS morning_occ,
               ROUND(evening - morning, 4) AS swing,
               CASE WHEN evening - morning > 0 THEN 'residential (fills at night)' ELSE 'commercial (fills by day)' END AS behaviour
        FROM p WHERE evening IS NOT NULL AND morning IS NOT NULL ORDER BY ABS(evening - morning) DESC LIMIT 30""",
    "q8_data_quality_per_day": None,  # built below: raw lines vs cleaned rows
}

times = []
results = {}
for name, sql in Q.items():
    t0 = time.time()
    if name == "q8_data_quality_per_day":
        raw = spark.read.text(lake("raw/flat/*/*.txt"))
        from pyspark.sql import functions as F
        r = (raw.select(F.substring(F.split("value", "\t")[0], 1, 10).alias("dt"),
                        F.split("value", "\t")[2].alias("upd"), F.split("value", "\t")[0].alias("snap"))
             .withColumn("stale", ((F.unix_timestamp(F.to_timestamp("snap")) - F.unix_timestamp(F.to_timestamp("upd"))) / 60
                                   > CFG["thresholds"]["stale_max_min"]).cast("int"))
             .groupBy("dt").agg(F.count("*").alias("raw_lines"), F.avg("stale").alias("stale_line_rate")))
        r.createOrReplaceTempView("raw_daily")
        df = spark.sql("""SELECT r.dt, r.raw_lines, ROUND(r.stale_line_rate, 4) AS stale_line_rate,
                                 c.clean_rows, c.clean_car_rows
                          FROM raw_daily r LEFT JOIN (SELECT dt, COUNT(*) AS clean_rows,
                                                             SUM(CASE WHEN lot_type='C' THEN 1 ELSE 0 END) AS clean_car_rows
                                                      FROM occupancy GROUP BY dt) c ON r.dt = c.dt ORDER BY r.dt""").toPandas()
    else:
        df = spark.sql(sql.format(f=full)).toPandas()
    sec = round(time.time() - t0, 2)
    df.to_csv(A / f"{name}.csv", index=False)
    times.append({"query": name, "rows": len(df), "seconds": sec})
    results[name] = df
    log.info("%s: %d rows in %.2fs", name, len(df), sec)
pd.DataFrame(times).to_csv(A / "query_times.csv", index=False)

# consistency with the Scala output
scala = pd.read_csv(path("metrics") / "rdd_summary.csv").set_index("metric")["value"]
total_rows = spark.sql("SELECT COUNT(*) AS n, SUM(CASE WHEN lot_type='C' THEN 1 ELSE 0 END) AS c FROM occupancy").first()
check = {"parquet_rows": total_rows["n"], "scala_clean_records": int(scala["clean_records"]),
         "parquet_car_rows": total_rows["c"], "scala_car_lot_records": int(scala["car_lot_records"])}
check["match"] = check["parquet_rows"] == check["scala_clean_records"] and check["parquet_car_rows"] == check["scala_car_lot_records"]
with open(A / "consistency_check.json", "w") as f:
    json.dump(check, f, indent=2, default=int)

pushed = 0
try:
    import hbase_io
    for name, df in results.items():
        hbase_io.put_metric(f"analytics:{name.split('_')[0]}", {"name": name, "rows": json.loads(df.to_json(orient="records"))})
        pushed += 1
except Exception as e:  # noqa: BLE001
    log.warning("HBase push skipped: %s", e)

print("\n=== analytics summary ===")
print(pd.DataFrame(times).to_string(index=False))
print(f"consistency with Scala output: {check}")
print(f"pushed {pushed} results to HBase parksense:metrics")
spark.stop()
