"""Shared SparkSession builder + config/path helpers for every PySpark job."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ingest"))
from common import CFG, get_logger  # noqa: E402


def path(key):
    p = ROOT / CFG["paths"][key]
    (p.parent if p.suffix else p).mkdir(parents=True, exist_ok=True)
    return p


def lake(rel):
    """hdfs:///parksense/<rel> when HDFS is enabled, else file://<root>/data/<rel> (same rule as Config.scala)."""
    if CFG["paths"].get("use_hdfs"):
        return f"{CFG['paths']['hdfs_root']}/{rel}"
    return f"file://{ROOT / 'data' / rel}"


def get_spark(app, master=None, shuffle_partitions=None, extra=None):
    from pyspark.sql import SparkSession
    s = CFG["spark"]
    b = (SparkSession.builder.appName(app).master(master or os.environ.get("PARKSENSE_MASTER", s["master"]))
         .config("spark.driver.memory", s["driver_memory"])
         .config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", f"file://{path('spark_events')}")
         .config("spark.sql.session.timeZone", CFG["time"]["tz"])
         .config("spark.sql.shuffle.partitions", str(shuffle_partitions or s["shuffle_partitions"]))
         .config("spark.ui.showConsoleProgress", "false"))
    for k, v in (extra or {}).items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


HOLIDAYS = [str(d) for d in CFG["holidays"]]
__all__ = ["CFG", "ROOT", "HOLIDAYS", "get_logger", "get_spark", "lake", "path"]
