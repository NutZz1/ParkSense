"""The eight numbered Spark SQL analytics queries (shared by analytics.py and the UI catalogue).

Views: `occupancy` (Scala output, all lot types), `car` (car lots of reliable sensors), `carpark_info`, `excluded`.
`{f}` is replaced by thresholds.full_ratio. Q8 is built with the DataFrame API over the raw flat lines (see analytics.py).
"""

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
    "q8_data_quality_per_day": None,  # built in analytics.py: raw lines vs cleaned rows (DataFrame API)
}

VIEW_SQL = """CREATE OR REPLACE TEMP VIEW car AS
SELECT o.* FROM occupancy o LEFT ANTI JOIN excluded e ON o.carpark_no = e.carpark_no
WHERE o.lot_type = 'C'"""

Q8_SQL = """-- DataFrame API over the raw flat lines (spark.read.text), then joined in SQL:
SELECT r.dt, r.raw_lines, ROUND(r.stale_line_rate, 4) AS stale_line_rate, c.clean_rows, c.clean_car_rows
FROM raw_daily r LEFT JOIN (SELECT dt, COUNT(*) AS clean_rows,
                                   SUM(CASE WHEN lot_type='C' THEN 1 ELSE 0 END) AS clean_car_rows
                            FROM occupancy GROUP BY dt) c ON r.dt = c.dt ORDER BY r.dt
-- raw_daily = per day: COUNT(*) raw lines, AVG(staleness > 30 min)"""

QUESTIONS = {
    "q1_occupancy_by_hour_weekday_weekend": "When are carparks fullest? Occupancy by hour, weekday vs weekend",
    "q2_top20_most_often_full": "Which carparks are chronically full? Top 20 by share of readings with <= 10% free",
    "q3_full_rate_by_type_and_system": "Does the carpark design matter? Nearly-full rate by carpark type and parking system",
    "q4_policy_effect_evening": "Do free / night-parking policies change evening demand?",
    "q5_busiest_day_per_type": "Busiest day of the week for each carpark type",
    "q6_daily_citywide_full_rate": "Daily trend of the citywide nearly-full rate",
    "q7_evening_vs_morning_swing": "Residential vs commercial behaviour: largest evening vs morning swing",
    "q8_data_quality_per_day": "Data quality: raw lines vs cleaned rows and stale-reading rate per day",
}
