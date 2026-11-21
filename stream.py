"""Spark Structured Streaming from Kafka into a Delta Lake lakehouse.

  bronze/flight_events     every Kafka message as it arrived, plus partition and offset
  silver/flights_current   one row per flight, latest state, kept with MERGE
  gold/origin_hourly       per airport and hour: departures, mean departure delay,
                           cancellations (event-time windows with a 2-hour watermark)

Each layer is its own streaming query with its own checkpoint, so a crash or restart
picks up exactly where it stopped. By default every query runs with trigger
availableNow: it processes whatever is in Kafka, then stops. --watch keeps it running.

    python stream.py              # process what is there and stop
    python stream.py --watch      # keep running, micro-batch every 10 s
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

ROOT = Path(__file__).resolve().parents[0]
LAKE = os.environ.get("LAKE_DIR", str(ROOT / "data" / "lakehouse"))
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "flight-events")
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
WATERMARK = "2 hours"

SCHEMA = T.StructType([
    T.StructField("op", T.StringType()), T.StructField("event_time", T.StringType()),
    T.StructField("flight_id", T.LongType()), T.StructField("carrier", T.StringType()),
    T.StructField("flight", T.LongType()), T.StructField("tailnum", T.StringType()),
    T.StructField("origin", T.StringType()), T.StructField("dest", T.StringType()),
    T.StructField("sched_dep_utc", T.StringType()), T.StructField("status", T.StringType()),
    T.StructField("dep_delay", T.DoubleType()), T.StructField("arr_delay", T.DoubleType()),
    T.StructField("air_time", T.DoubleType()),
])


def step():
    """Lifecycle position, used to break ties between changes with the same timestamp."""
    return F.when(F.col("status") == "scheduled", 0).when(F.col("status") == "arrived", 2).otherwise(1)


def p(*parts: str) -> str:
    return "/".join([LAKE, *parts])


def spark_session() -> SparkSession:
    b = (SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[4]")).appName("flights-lakehouse")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.enabled", "false"))
    spark = configure_spark_with_delta_pip(b, extra_packages=[KAFKA_PKG]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parsed(df: DataFrame) -> DataFrame:
    return (df.withColumn("e", F.from_json("value", SCHEMA)).select("e.*", "kafka_partition", "kafka_offset")
            .withColumn("event_time", F.to_timestamp("event_time"))
            .withColumn("sched_dep_utc", F.to_timestamp("sched_dep_utc")))


def bronze(spark: SparkSession, trigger: dict):
    src = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC).option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", int(os.environ.get("MAX_OFFSETS_PER_BATCH", "200000"))).load())
    out = src.select(F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value"),
                     F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
                     F.col("timestamp").alias("kafka_time"), F.current_timestamp().alias("ingested_at"))
    return (out.writeStream.format("delta").option("checkpointLocation", p("_checkpoints", "bronze"))
            .trigger(**trigger).start(p("bronze", "flight_events")))


def _merge_latest(batch: DataFrame, batch_id: int) -> None:
    w = Window.partitionBy("flight_id").orderBy(F.col("event_time").desc(), F.col("step").desc())
    latest = (parsed(batch).withColumn("step", step()).withColumn("rn", F.row_number().over(w))
              .where("rn = 1").drop("rn", "op", "kafka_partition", "kafka_offset")
              .withColumnRenamed("event_time", "updated_at"))
    spark = batch.sparkSession
    path = p("silver", "flights_current")
    if not DeltaTable.isDeltaTable(spark, path):
        latest.limit(0).write.format("delta").save(path)
    # only move a flight forward: an older change that shows up late never overwrites a newer one
    (DeltaTable.forPath(spark, path).alias("t").merge(latest.alias("s"), "t.flight_id = s.flight_id")
     .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at or "
                                     "(s.updated_at = t.updated_at and s.step >= t.step)")
     .whenNotMatchedInsertAll().execute())


def silver(spark: SparkSession, trigger: dict):
    src = spark.readStream.format("delta").option("maxFilesPerTrigger", 50).load(p("bronze", "flight_events"))
    return (src.writeStream.foreachBatch(_merge_latest).option("checkpointLocation", p("_checkpoints", "silver"))
            .trigger(**trigger).start())


def gold(spark: SparkSession, trigger: dict):
    ev = parsed(spark.readStream.format("delta").option("maxFilesPerTrigger", 50).load(p("bronze", "flight_events")))
    agg = (ev.withWatermark("event_time", WATERMARK)
           .where(F.col("status").isin("departed", "cancelled"))
           .groupBy(F.window("event_time", "1 hour").alias("w"), "origin")
           .agg(F.sum((F.col("status") == "departed").cast("int")).alias("departures"),
                F.avg(F.when(F.col("status") == "departed", F.col("dep_delay"))).alias("mean_dep_delay"),
                F.sum((F.col("status") == "cancelled").cast("int")).alias("cancellations"))
           .select(F.col("w.start").alias("hour_start"), "origin", "departures", "mean_dep_delay", "cancellations"))
    return (agg.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", p("_checkpoints", "gold")).trigger(**trigger)
            .start(p("gold", "origin_hourly")))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--watch", action="store_true")
    a = ap.parse_args(argv)
    spark = spark_session()
    trigger = {"processingTime": "10 seconds"} if a.watch else {"availableNow": True}
    timings = {}
    if a.watch:
        qs = [bronze(spark, trigger)]
        qs[0].awaitTermination(15)  # let bronze create its table first
        qs += [silver(spark, trigger), gold(spark, trigger)]
        spark.streams.awaitAnyTermination()
        return 0
    for name, fn in [("bronze", bronze), ("silver", silver), ("gold", gold)]:
        t0 = time.perf_counter()
        q = fn(spark, trigger)
        q.awaitTermination()
        timings[name] = {"seconds": round(time.perf_counter() - t0, 1),
                         "batches": len(q.recentProgress),
                         "rows_in": sum(int(x.numInputRows) for x in q.recentProgress)}
    print(json.dumps(timings))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
