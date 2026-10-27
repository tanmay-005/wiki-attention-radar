"""One place to build the SparkSession, so every job uses the same settings."""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app: str, shuffle_partitions: int = 16) -> SparkSession:
    spark = (SparkSession.builder.appName(app)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
