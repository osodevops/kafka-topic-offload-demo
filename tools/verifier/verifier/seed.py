"""Create a topic and seed it with deterministic energy device telemetry.

Per partition, timestamps advance chronologically across the span. Three details make
the data behave like real telemetry under retention and time window restores:

* a late cohort (default 0.5%) whose CreateTime is 1 to LATE_MAX_HOURS behind the
  partition clock, as delayed device uploads are;
* boundary records at exactly UTC midnight minus 1 ms and at midnight;
* nothing newer than SEED_END_TS, so no stray "now" timestamp can pin a segment.
"""
import argparse
import io
import json
import multiprocessing as mp
import os
import random
import struct
import time
from pathlib import Path

import requests

from .common import DAY_MS, HOUR_MS, gate, log, run_dir, watermarks, write_json
from .murmur import java_partition, self_test

SCALES = {"smoke": 1 << 30, "10g": 10 << 30, "100g": 100 << 30}
AVG_RECORD_BYTES = 2000
SEGMENTS_PER_PARTITION = 60
MIN_SEGMENT_BYTES = 1 << 20
DEVICES = 40_000
STATUSES = ["ONLINE", "CURTAILED", "FAULT", "OFFLINE"]
SR_HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

# Registered first so the telemetry schemas never hold IDs 1 and 2. A registry that
# assigns IDs itself, rather than importing them, then gives different IDs, which the
# schema gate has to catch.
DECOY_SCHEMAS = {
    "demo-decoy-meter-read-value": {"type": "record", "name": "MeterRead", "namespace": "demo.decoy",
                                    "fields": [{"name": "nmi", "type": "string"}, {"name": "kwh", "type": "double"}]},
    "demo-decoy-tariff-value": {"type": "record", "name": "Tariff", "namespace": "demo.decoy",
                                "fields": [{"name": "code", "type": "string"}, {"name": "centsPerKwh", "type": "double"}]},
}


def sr_register(sr_url: str, subject: str, schema: dict, timeout_s: int = 180) -> int:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            r = requests.post(f"{sr_url}/subjects/{subject}/versions", headers=SR_HEADERS,
                              data=json.dumps({"schema": json.dumps(schema)}), timeout=30)
            if r.status_code == 200:
                return r.json()["id"]
            raise RuntimeError(f"register {subject}: HTTP {r.status_code} {r.text}")
        except requests.ConnectionError:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)


def ensure_topic(bootstrap: str, topic: str, partitions: int, configs: dict) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=30).topics:
        marks = watermarks(bootstrap, topic, range(partitions))
        if any(hi > 0 for _, hi in marks.values()):
            raise SystemExit(f"topic {topic} already holds data; run `make clean` for a fresh run")
        log(f"topic {topic} exists and is empty")
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1, config=configs)])
    futures[topic].result()
    for _ in range(120):
        md = admin.list_topics(topic=topic, timeout=10).topics.get(topic)
        if md and len(md.partitions) == partitions and all(pm.leader >= 0 for pm in md.partitions.values()):
            log(f"created {topic}: {partitions} partitions, {configs}")
            return
        time.sleep(1)
    raise RuntimeError(f"{topic} partitions have no leader")


def _produce_partition(job: dict) -> dict:
    from confluent_kafka import Producer
    from fastavro import parse_schema, schemaless_writer

    p, n, topic = job["partition"], job["count"], job["topic"]
    start_ts, end_ts = job["start_ts"], job["end_ts"]
    late_rate, late_max_ms = job["late_rate"], job["late_max_hours"] * HOUR_MS
    keys = job["keys"]
    rng = random.Random(f"{job['seed']}:{topic}:{p}")
    writers = {
        1: (parse_schema(job["schemas"]["v1"]), b"\x00" + struct.pack(">I", job["schema_ids"]["v1"])),
        2: (parse_schema(job["schemas"]["v2"]), b"\x00" + struct.pack(">I", job["schema_ids"]["v2"])),
    }
    v2_from = int(n * job["v2_from_fraction"])

    producer = Producer({
        "bootstrap.servers": job["bootstrap"],
        "enable.idempotence": True,
        "acks": "all",
        "partitioner": "murmur2_random",
        "compression.type": "none",
        "linger.ms": 20,
        "batch.size": 900_000,
        "batch.num.messages": 100_000,
        "queue.buffering.max.messages": 1_000_000,
        "queue.buffering.max.kbytes": 2_097_151,
        "message.timeout.ms": 600_000,
    })
    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    step = (end_ts - start_ts) / n
    clock = None          # largest timestamp produced so far; late records never move it
    prev_day = None
    pin_midnight = None
    late = boundary = value_bytes = 0
    min_ts = max_ts = None
    buf = io.BytesIO()
    t0 = time.monotonic()
    report_every = max(n // 10, 1)

    for i in range(n):
        natural = start_ts + int(i * step)
        day = natural // DAY_MS
        is_boundary = False
        if prev_day is not None and day != prev_day:
            ts = day * DAY_MS - 1
            pin_midnight = day * DAY_MS
            is_boundary = True
        elif pin_midnight is not None:
            ts = pin_midnight
            pin_midnight = None
            is_boundary = True
        else:
            ts = natural
        prev_day = day

        if (not is_boundary and clock is not None and clock - start_ts > late_max_ms
                and rng.random() < late_rate):
            ts = clock - rng.randint(HOUR_MS, late_max_ms)
            late += 1
        else:
            clock = ts if clock is None else max(clock, ts)
            boundary += is_boundary

        key = keys[rng.randrange(len(keys))]
        device = key.decode()
        idx = int(device[4:])
        power = rng.uniform(-5000.0, 10000.0)
        voltage = rng.gauss(240.0, 4.0)
        frame = f"{device};{ts};V={voltage:.1f};P={power:.0f};"
        record = {
            "deviceId": device,
            "siteId": f"site-{idx % 2000:04d}",
            "eventTime": ts,
            "seq": i,
            "activePowerW": power,
            "reactivePowerVar": rng.uniform(-2000.0, 2000.0),
            "voltageV": voltage,
            "frequencyHz": rng.gauss(50.0, 0.05),
            "stateOfChargePct": rng.uniform(0.0, 100.0) if idx % 3 == 0 else None,
            "exportLimitW": 5000.0 if idx % 5 == 0 else None,
            "status": STATUSES[0] if rng.random() < 0.9 else STATUSES[rng.randrange(1, 4)],
            "firmwareVersion": f"fw-{1 + idx % 7}.{idx % 13}.{idx % 4}",
            # Part random, part repetitive text, so zstd sees a realistic mix.
            "rawPayload": rng.randbytes(700) + (frame * (1150 // len(frame) + 1))[:1150].encode(),
        }
        version = 2 if i >= v2_from else 1
        if version == 2:
            record["gridZone"] = f"zone-{idx % 12:02d}"
        schema, header = writers[version]
        buf.seek(0)
        buf.truncate()
        schemaless_writer(buf, schema, record)
        value = header + buf.getvalue()
        value_bytes += len(value)
        min_ts = ts if min_ts is None else min(min_ts, ts)
        max_ts = ts if max_ts is None else max(max_ts, ts)

        while True:
            try:
                producer.produce(topic, value=value, key=key, partition=p, timestamp=ts, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.2)
        if i % 5000 == 0:
            producer.poll(0)
            if errors:
                raise RuntimeError(f"partition {p}: delivery failed: {errors[:3]}")
        if (i + 1) % report_every == 0:
            rate = (i + 1) / (time.monotonic() - t0)
            log(f"seed p{p}: {i + 1:,}/{n:,} records ({rate:,.0f} rec/s)")

    remaining = producer.flush(900)
    if remaining or errors:
        raise RuntimeError(f"partition {p}: {remaining} undelivered, errors {errors[:3]}")
    return {
        "partition": p, "count": n, "late": late, "boundary": boundary, "min_ts": min_ts, "max_ts": max_ts,
        "value_bytes": value_bytes, "v2_from_index": v2_from, "seconds": round(time.monotonic() - t0, 1),
    }


def main(_cmd: str, argv) -> int:
    ap = argparse.ArgumentParser(prog="verifier seed")
    ap.add_argument("--topic", default=os.environ.get("TOPIC", "device-telemetry"))
    ap.add_argument("--partitions", type=int, default=int(os.environ.get("PARTITIONS", "6")))
    ap.add_argument("--scale", default=os.environ.get("SCALE", "smoke"), choices=sorted(SCALES))
    ap.add_argument("--records", type=int, help="override the scale's record count")
    ap.add_argument("--span-days", type=int, default=365)
    ap.add_argument("--end-ts", type=int, default=int(os.environ.get("SEED_END_TS", "0")) or None)
    ap.add_argument("--segment-bytes", type=int)
    ap.add_argument("--seed", default=os.environ.get("SEED", "20260915"))
    ap.add_argument("--late-rate", type=float, default=0.005)
    ap.add_argument("--late-max-hours", type=int, default=int(os.environ.get("LATE_MAX_HOURS", "72")))
    ap.add_argument("--v2-from-fraction", type=float, default=0.7)
    ap.add_argument("--schema-dir", default="/config/schemas")
    args = ap.parse_args(argv)

    bootstrap = os.environ["SOURCE_BOOTSTRAP"]
    sr_url = os.environ["SOURCE_SR"]
    parts = args.partitions
    total = args.records or SCALES[args.scale] // AVG_RECORD_BYTES
    counts = [total // parts + (1 if p < total % parts else 0) for p in range(parts)]
    segment_bytes = args.segment_bytes or max(MIN_SEGMENT_BYTES, total * AVG_RECORD_BYTES // parts // SEGMENTS_PER_PARTITION)

    now = int(time.time() * 1000)
    end_ts = args.end_ts or (now // HOUR_MS) * HOUR_MS - HOUR_MS
    if end_ts > now:
        raise SystemExit("SEED_END_TS is in the future; retention would never delete that segment")
    start_ts = end_ts - args.span_days * DAY_MS

    murmur = self_test()
    log(f"murmur2 self test: {murmur}")

    schema_dir = Path(args.schema_dir)
    schemas = {v: json.loads((schema_dir / f"device-telemetry-{v}.avsc").read_text()) for v in ("v1", "v2")}
    decoy_ids = {s: sr_register(sr_url, s, sch) for s, sch in DECOY_SCHEMAS.items()}
    subject = f"{args.topic}-value"
    schema_ids = {v: sr_register(sr_url, subject, schemas[v]) for v in ("v1", "v2")}
    log(f"schema ids: decoys {decoy_ids}, {subject} {schema_ids}")

    topic_configs = {
        "cleanup.policy": "delete",
        "retention.ms": "-1",
        "retention.bytes": "-1",
        "segment.bytes": str(segment_bytes),
        "segment.ms": str(10 * 365 * DAY_MS),
        "message.timestamp.type": "CreateTime",
        "message.timestamp.before.max.ms": str(2**63 - 1),
    }
    ensure_topic(bootstrap, args.topic, parts, topic_configs)

    keys_by_partition = {p: [] for p in range(parts)}
    for i in range(DEVICES):
        key = f"dev-{i:05d}".encode()
        keys_by_partition[java_partition(key, parts)].append(key)

    jobs = [{
        "bootstrap": bootstrap, "topic": args.topic, "partition": p, "count": counts[p],
        "start_ts": start_ts, "end_ts": end_ts, "seed": args.seed, "keys": keys_by_partition[p],
        "late_rate": args.late_rate, "late_max_hours": args.late_max_hours,
        "schemas": schemas, "schema_ids": schema_ids, "v2_from_fraction": args.v2_from_fraction,
    } for p in range(parts)]

    t0 = time.monotonic()
    log(f"seeding {total:,} records into {args.topic} across {parts} partitions, segment.bytes={segment_bytes:,}")
    with mp.get_context("spawn").Pool(parts) as pool:
        results = pool.map(_produce_partition, jobs)
    seconds = time.monotonic() - t0

    marks = watermarks(bootstrap, args.topic, range(parts))
    value_bytes = sum(r["value_bytes"] for r in results)
    summary = {
        "topic": args.topic, "scale": args.scale, "seed": args.seed, "records": total, "partitions": parts,
        "start_ts": start_ts, "seed_end_ts": end_ts, "span_days": args.span_days,
        "late_rate": args.late_rate, "late_max_hours": args.late_max_hours,
        "segment_bytes": segment_bytes, "topic_configs": topic_configs,
        "schema_ids": schema_ids, "decoy_schema_ids": decoy_ids, "subject": subject,
        "keys_per_partition": {p: len(k) for p, k in keys_by_partition.items()},
        "murmur2_self_test": murmur,
        "per_partition": {r["partition"]: r for r in results},
        "watermarks": {p: {"low": lo, "high": hi} for p, (lo, hi) in marks.items()},
        "value_bytes": value_bytes, "seconds": round(seconds, 1),
        "throughput_mb_s": round(value_bytes / seconds / 1e6, 1),
    }
    write_json(run_dir() / "seed" / f"{args.topic}.json", summary)

    ok = gate("seed.murmur2_matches_java_partitioner", all(v["ok"] for v in murmur.values()), json.dumps(murmur))
    ok &= gate("seed.watermarks", all(marks[p] == (0, counts[p]) for p in range(parts)),
              f"{total:,} records, low 0 and high == produced on every partition")
    ok &= gate("seed.no_future_timestamps", all(r["max_ts"] <= end_ts <= now for r in results),
               f"max ts {max(r['max_ts'] for r in results)} <= SEED_END_TS {end_ts}")
    log(f"seeded {value_bytes / 1e9:.2f} GB of values in {seconds:.0f}s ({summary['throughput_mb_s']} MB/s)")
    return 0 if ok else 1
