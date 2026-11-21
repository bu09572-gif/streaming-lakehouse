"""Check the lakehouse against Kafka and against the source events.

  1. bronze has exactly one row per Kafka message (count matches end offsets, no
     repeated partition/offset)
  2. silver equals the latest state of every flight, worked out directly in pandas
     from the same events
  3. gold equals a pandas group-by of the same events, for every hour the watermark
     has closed

    python validate.py --month 2 --upto 2/2
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, TopicPartition

from flightsdata.events import build_events, load_flights
from producer import BOOTSTRAP, TOPIC
from stream import p, spark_session

OUT = Path(__file__).resolve().parent / "results"
KEY = ["flight_id", "carrier", "flight", "tailnum", "origin", "dest", "status", "dep_delay", "arr_delay", "air_time"]


def kafka_message_count() -> int:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "validate"})
    parts = c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    total = 0
    for i in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC, i), timeout=10)
        total += hi - lo
    c.close()
    return total


def gold_watermark() -> pd.Timestamp:
    """The watermark Spark used in the gold query's last batch, from its checkpoint."""
    d = Path(p("_checkpoints", "gold", "offsets"))
    last = max((f for f in d.iterdir() if f.name.isdigit()), key=lambda f: int(f.name))
    ms = json.loads(last.read_text().splitlines()[1])["batchWatermarkMs"]
    return pd.Timestamp(ms, unit="ms", tz="UTC")


def expected_events(month: int | None, upto: str | None) -> pd.DataFrame:
    ev = build_events(load_flights(month=month))
    if upto:
        k, n = map(int, upto.split("/"))
        ev = ev.iloc[: len(ev) * k // n]
    return ev


def norm(df: pd.DataFrame) -> pd.DataFrame:
    df = df[KEY].copy()
    for c in ["flight_id", "flight"]:
        df[c] = df[c].astype("int64")
    for c in ["dep_delay", "arr_delay", "air_time"]:
        df[c] = pd.to_numeric(df[c]).astype("float64").round(3)
    for c in ["carrier", "tailnum", "origin", "dest", "status"]:
        df[c] = df[c].astype(object).where(df[c].notna(), None)
    return df


def diff(a: pd.DataFrame, b: pd.DataFrame, on: list[str]) -> int:
    m = a.merge(b, how="outer", on=on, indicator=True)
    return int((m._merge != "both").sum())


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", type=int)
    ap.add_argument("--upto")
    ap.add_argument("--debug", action="store_true")
    a = ap.parse_args(argv)
    spark = spark_session()
    ev = expected_events(a.month, a.upto)

    b = spark.read.format("delta").load(p("bronze", "flight_events"))
    bronze_rows = b.count()
    dup_offsets = b.groupBy("kafka_partition", "kafka_offset").count().where("count > 1").count()
    kafka_msgs = kafka_message_count()

    silver = spark.read.format("delta").load(p("silver", "flights_current")).toPandas()
    exp_silver = ev.groupby("flight_id", sort=False).tail(1)  # events are in time order
    silver_diff = diff(norm(silver), norm(exp_silver), KEY)

    gold = spark.read.format("delta").load(p("gold", "origin_hourly")).toPandas()
    gold["hour_start"] = pd.to_datetime(gold.hour_start, utc=True)
    # an hour is final once the watermark passes its end; later hours stay in Spark's state
    watermark = gold_watermark()
    closed_before = watermark - pd.Timedelta("1h")
    g = ev[ev.status.isin(["departed", "cancelled"])].assign(hour_start=lambda d: d["at"].dt.floor("h"))
    exp_gold = (g.groupby(["hour_start", "origin"])
                .apply(lambda d: pd.Series({
                    "departures": int((d.status == "departed").sum()),
                    "mean_dep_delay": d.loc[d.status == "departed", "dep_delay"].mean(),
                    "cancellations": int((d.status == "cancelled").sum())}), include_groups=False)
                .reset_index())
    exp_gold = exp_gold[exp_gold.hour_start <= closed_before]
    got_gold = gold[gold.hour_start <= closed_before]
    cols = ["hour_start", "origin", "departures", "cancellations", "mean_dep_delay"]
    for d in (exp_gold, got_gold):
        d["mean_dep_delay"] = d.mean_dep_delay.astype("float64").round(3)
        d["departures"] = d.departures.astype("int64")
        d["cancellations"] = d.cancellations.astype("int64")
    gold_diff = diff(got_gold[cols], exp_gold[cols], cols)
    if gold_diff and "--debug" in sys.argv:
        m = got_gold[cols].merge(exp_gold[cols], how="outer", on=cols, indicator=True)
        print(m[m._merge != "both"].sort_values(["hour_start", "origin"]).to_string())

    checks = {
        "bronze_rows_minus_kafka_messages": bronze_rows - kafka_msgs,
        "bronze_repeated_offsets": dup_offsets,
        "silver_rows_differing_from_expected": silver_diff,
        "gold_closed_hours_differing_from_expected": gold_diff,
    }
    report = {
        "scope": {"month": a.month, "upto": a.upto},
        "kafka_messages": kafka_msgs, "bronze_rows": bronze_rows,
        "silver_flights": len(silver), "silver_status": silver.status.value_counts().to_dict(),
        "gold_rows": len(gold), "gold_rows_checked": len(got_gold),
        "gold_watermark": str(watermark), "last_event": str(ev["at"].max()),
        "checks": checks, "passed": all(v == 0 for v in checks.values()),
    }
    OUT.mkdir(exist_ok=True)
    (OUT / f"validate_month{a.month or 'all'}_{(a.upto or 'all').replace('/', 'of')}.json").write_text(
        json.dumps(report, indent=2, default=int))
    print(json.dumps(report, indent=2, default=int))
    spark.stop()
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
