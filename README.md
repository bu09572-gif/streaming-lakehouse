# Streaming Flight Lakehouse (Kafka, Spark, Delta Lake)

[![tests](https://github.com/bu09572-gif/streaming-lakehouse/actions/workflows/tests.yml/badge.svg)](https://github.com/bu09572-gif/streaming-lakehouse/actions/workflows/tests.yml)

A streaming pipeline that takes flight status changes from Kafka and builds bronze, silver and gold Delta Lake tables with Spark Structured Streaming. I fed it the full real 2013 NYC flights year, 1,000,898 changes, and checked every layer against an answer worked out independently in pandas.

```
producer.py ──▶ Kafka topic flight-events (3 partitions, key = flight_id)
                    │
stream.py           ▼
  bronze/flight_events     every message as received, with partition and offset    (append)
  silver/flights_current   one row per flight, latest status                       (MERGE)
  gold/origin_hourly       per airport and hour: departures, mean delay,           (event-time windows,
                           cancellations                                            2-hour watermark)
validate.py: compares each layer with Kafka and with the source events
```

The changes are the same ones the [CDC project](https://github.com/bu09572-gif/ai-ml-portfolio/blob/main/projects/data-engineering/flight-status-cdc/README.md) uses (`src/flightsdata/events.py`): each flight is scheduled, then departs and arrives, or is cancelled, at the times recorded in the data.

## Design choices

- **Key by flight.** All changes to one flight go to the same partition, so they stay in order.
- **Each layer is its own query with its own checkpoint.** A crash in silver doesn't make bronze read Kafka again.
- **Silver only moves forward.** The MERGE updates a row only when the incoming change is newer (or the same time and later in the lifecycle). A late or repeated message can't put a landed flight back to "departed".
- **Gold uses event time, not arrival time.** Hourly windows close once the watermark (latest event time seen, minus 2 hours) passes them. Later hours stay in Spark's state until more data arrives.
- **availableNow trigger by default.** Each run processes what is in Kafka and stops, which is how I run it in CI. `--watch` keeps it running.

## Results

Full year, from `results/validate_monthall_2of2.json`. I sent the first half of the changes, ran the stream and validated, then sent the second half and ran it again.

| Check | Result |
|---|---|
| bronze rows minus Kafka messages | 0 (1,000,898 each) |
| Kafka offsets written twice in bronze | 0 |
| silver rows that differ from the expected latest state | 0 of 336,776 |
| gold hours that differ from a pandas group-by | 0 of 21,144 |

Final silver status: 327,346 arrived, 8,255 cancelled, 1,175 departed with no arrival recorded. These match the CDC project exactly.

Speed on a 4-core machine: the producer sends about 42,000 messages a second. Spark took 28 s for bronze, 9 s for silver and 5 s for gold on 500,449 changes.

**Crash test.** For February (73,513 changes) I started the stream on the second half with small batches (3,000 messages) and killed the process with `kill -9` partway through. On restart, bronze picked up only the 12,769 messages it hadn't committed, and all four checks came back 0 (`results/validate_month2_2of2.json`).

## Running it

You need Java 17+, a Kafka broker on `localhost:9092` and the raw CSVs (see [Flight Operations Warehouse](https://github.com/bu09572-gif/ai-ml-portfolio/blob/main/projects/data-engineering/flight-ops-dbt-warehouse/README.md)). Spark downloads the Kafka and Delta packages from Maven the first time it starts.

```bash
pip install pyspark==4.0.1 delta-spark==4.0.0 confluent-kafka pandas numpy
export PYTHONPATH=src

# Kafka 4 in KRaft mode, no ZooKeeper
bin/kafka-storage.sh format --standalone -t $(bin/kafka-storage.sh random-uuid) -c config/server.properties
bin/kafka-server-start.sh config/server.properties &

./run_demo.sh 2      # February: produce, stream, validate, crash, restart, validate (~3 min)
./run_demo.sh        # full year
```

---

Part of my [AI/ML and data engineering portfolio](https://github.com/bu09572-gif/ai-ml-portfolio), where every project uses real public data and free, open-source tools.
