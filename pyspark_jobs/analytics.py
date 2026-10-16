"""Phase 4 — eight numbered Spark SQL queries over the Scala output (temp views on Parquet/CSV, no Hive).

Each result -> outputs/analytics/qN_<name>.csv with its runtime (outputs/analytics/query_times.csv), and
-> HBase parksense:metrics as analytics:qN (if HBase is up).
Usage: python pyspark_jobs/analytics.py
"""
import json
import time

import pandas as pd

from analytics_queries import Q, Q8_SQL, QUESTIONS, VIEW_SQL
from spark_session import CFG, ROOT, get_logger, get_spark, lake, path

log = get_logger("analytics")
A = path("analytics")
full = CFG["thresholds"]["full_ratio"]
spark = get_spark("ParkSense-Analytics")
spark.read.parquet(lake("processed/occupancy")).createOrReplaceTempView("occupancy")
spark.read.option("header", True).option("inferSchema", True).csv(lake("raw/meta/carpark_info.csv")).createOrReplaceTempView("carpark_info")
spark.read.parquet(lake("processed/excluded_carparks")).createOrReplaceTempView("excluded")
# car lots of reliable sensors only (always-zero / never-changing carparks excluded, as in the ML)
spark.sql(VIEW_SQL)


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
        t = next(x for x in times if x["query"] == name)
        hbase_io.put_metric(f"analytics:{name.split('_')[0]}", {
            "name": name, "question": QUESTIONS[name], "sql": (Q8_SQL if Q[name] is None else Q[name].format(f=full)).strip(),
            "seconds": t["seconds"], "row_count": t["rows"], "rows": json.loads(df.to_json(orient="records"))})
        pushed += 1
except Exception as e:  # noqa: BLE001
    log.warning("HBase push skipped: %s", e)

print("\n=== analytics summary ===")
print(pd.DataFrame(times).to_string(index=False))
print(f"consistency with Scala output: {check}")
print(f"pushed {pushed} results to HBase parksense:metrics")
spark.stop()
