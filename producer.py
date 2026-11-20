"""Publish the real flight changes (scheduled, departed, arrived, cancelled) to Kafka.

Each message is one change, keyed by flight_id, so all changes to a flight land on the
same partition in order. Messages go out in event-time order.

    python producer.py --month 2              # all of February
    python producer.py --month 2 --part 1/2   # first half of February's changes (by time)
"""
from __future__ import annotations

import argparse
import json
import os
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from flightsdata.events import build_events, load_flights

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "flight-events")
PARTITIONS = 3


def ensure_topic() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if TOPIC not in admin.list_topics(timeout=10).topics:
        for f in admin.create_topics([NewTopic(TOPIC, PARTITIONS, 1)]).values():
            f.result()


def events_for(month: int | None, part: str | None):
    ev = build_events(load_flights(month=month))
    if part:
        k, n = map(int, part.split("/"))
        lo, hi = len(ev) * (k - 1) // n, len(ev) * k // n
        ev = ev.iloc[lo:hi]
    return ev


def to_message(r: dict) -> bytes:
    def iso(t):
        return t.strftime("%Y-%m-%dT%H:%M:%SZ")

    def num(x):
        return None if x != x else x  # NaN -> null

    return json.dumps({
        "op": r["op"], "event_time": iso(r["at"]), "flight_id": int(r["flight_id"]),
        "carrier": r["carrier"], "flight": int(r["flight"]),
        "tailnum": None if r["tailnum"] is None or r["tailnum"] != r["tailnum"] else r["tailnum"],
        "origin": r["origin"], "dest": r["dest"], "sched_dep_utc": iso(r["sched_dep_utc"]),
        "status": r["status"], "dep_delay": num(r["dep_delay"]), "arr_delay": num(r["arr_delay"]),
        "air_time": num(r["air_time"]),
    }).encode()


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", type=int)
    ap.add_argument("--part")
    a = ap.parse_args(argv)
    ensure_topic()
    ev = events_for(a.month, a.part)
    p = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all",
                  "linger.ms": 20, "compression.type": "lz4"})
    t0 = time.perf_counter()
    for i, r in enumerate(ev.to_dict("records")):
        while True:
            try:
                p.produce(TOPIC, key=str(r["flight_id"]).encode(), value=to_message(r))
                break
            except BufferError:  # local queue full: let it drain
                p.poll(0.5)
        if i % 10_000 == 0:
            p.poll(0)
    left = p.flush(60)
    secs = time.perf_counter() - t0
    print(json.dumps({"topic": TOPIC, "sent": len(ev), "unsent": left, "seconds": round(secs, 1),
                      "per_second": round(len(ev) / secs)}))
    return 1 if left else 0


if __name__ == "__main__":
    raise SystemExit(main())
