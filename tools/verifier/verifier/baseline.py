"""Baseline of a source topic, read once before anything is changed.

For every partition it records log start, high watermark and contiguity; for every
UTC day bucket, count, min and max timestamp and a running SHA-256 in offset order;
for every broker segment, count, offsets and min and max timestamp.
"""
import argparse
import multiprocessing as mp
import os
import struct
import time
from datetime import datetime, timezone

from .broker_files import segment_inventory
from .common import DAY_MS, BucketStats, digest_input, gate, log, read_json, read_partition, run_dir, watermarks, write_json


def iso_day(day: int) -> str:
    return datetime.fromtimestamp(day * DAY_MS / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def topic_config(bootstrap: str, topic: str) -> dict:
    from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

    admin = AdminClient({"bootstrap.servers": bootstrap})
    res = ConfigResource(ResourceType.TOPIC, topic)
    entries = admin.describe_configs([res])[res].result()
    return {k: {"value": e.value, "source": e.source} for k, e in sorted(entries.items())}


def _scan(job: dict) -> dict:
    from confluent_kafka import TIMESTAMP_CREATE_TIME

    p, lo, hi = job["partition"], job["low"], job["high"]
    segments = [dict(s, count=0, min_ts=None, max_ts=None, first_offset=None, last_offset=None) for s in job["segments"]]
    bases = [s["base_offset"] for s in segments]
    buckets = {}
    st = {"clock": None, "backwards": 0, "prev": None, "gaps": 0, "non_create": 0, "seg": 0,
          "min_ts": None, "max_ts": None, "bad_magic": 0}
    schema_ids = set()
    t0 = time.monotonic()

    def on_msg(m):
        off = m.offset()
        ts_type, ts = m.timestamp()
        if ts_type != TIMESTAMP_CREATE_TIME:
            st["non_create"] += 1
        key, value = m.key(), m.value()
        if st["prev"] is not None and off != st["prev"] + 1:
            st["gaps"] += 1
        st["prev"] = off
        if st["clock"] is not None and ts < st["clock"]:
            st["backwards"] += 1
        else:
            st["clock"] = ts
        st["min_ts"] = ts if st["min_ts"] is None else min(st["min_ts"], ts)
        st["max_ts"] = ts if st["max_ts"] is None else max(st["max_ts"], ts)

        day = ts // DAY_MS
        b = buckets.get(day)
        if b is None:
            b = buckets[day] = BucketStats()
        b.add(off, ts, digest_input(p, off, ts, key, value, m.headers()))

        if value and len(value) >= 5 and value[0] == 0:
            schema_ids.add(struct.unpack_from(">I", value, 1)[0])
        else:
            st["bad_magic"] += 1

        i = st["seg"]
        while i + 1 < len(bases) and off >= bases[i + 1]:
            i += 1
        st["seg"] = i
        if segments:
            s = segments[i]
            s["count"] += 1
            s["min_ts"] = ts if s["min_ts"] is None else min(s["min_ts"], ts)
            s["max_ts"] = ts if s["max_ts"] is None else max(s["max_ts"], ts)
            if s["first_offset"] is None:
                s["first_offset"] = off
            s["last_offset"] = off

    count = read_partition(job["bootstrap"], job["topic"], p, lo, hi, on_msg)
    log(f"baseline p{p}: {count:,} records in {time.monotonic() - t0:.0f}s")
    return {
        "partition": p, "log_start": lo, "high_watermark": hi, "count": count,
        "offset_gaps": st["gaps"], "backwards_steps": st["backwards"], "non_create_time": st["non_create"],
        "bad_magic": st["bad_magic"], "min_ts": st["min_ts"], "max_ts": st["max_ts"],
        "schema_ids": sorted(schema_ids),
        "buckets": {iso_day(d): b.to_dict() for d, b in sorted(buckets.items())},
        "segments": segments,
    }


def main(_cmd: str, argv) -> int:
    ap = argparse.ArgumentParser(prog="verifier baseline")
    ap.add_argument("--topic", default=os.environ.get("TOPIC", "device-telemetry"))
    ap.add_argument("--partitions", type=int, default=int(os.environ.get("PARTITIONS", "6")))
    ap.add_argument("--data-dir", default="/kafka-data/source", help="source broker log dir, mounted read-only")
    ap.add_argument("--out", help="output name, default baseline/<topic>.json")
    args = ap.parse_args(argv)

    bootstrap = os.environ["SOURCE_BOOTSTRAP"]
    parts = range(args.partitions)
    marks = watermarks(bootstrap, args.topic, parts)
    inventory = segment_inventory(args.data_dir, args.topic, parts)
    jobs = [{"bootstrap": bootstrap, "topic": args.topic, "partition": p, "low": marks[p][0], "high": marks[p][1],
             "segments": inventory[p]} for p in parts]

    t0 = time.monotonic()
    with mp.get_context("spawn").Pool(len(jobs)) as pool:
        results = pool.map(_scan, jobs)
    seconds = time.monotonic() - t0

    after = watermarks(bootstrap, args.topic, parts)
    seed_path = run_dir() / "seed" / f"{args.topic}.json"
    seed = read_json(seed_path) if seed_path.exists() else None
    out = {
        "topic": args.topic,
        "taken_at": int(time.time() * 1000),
        "seconds": round(seconds, 1),
        "topic_config": topic_config(bootstrap, args.topic),
        "schema_ids": sorted({i for r in results for i in r["schema_ids"]}),
        "partitions": {r["partition"]: r for r in results},
    }
    write_json(run_dir() / "baseline" / (args.out or f"{args.topic}.json"), out)

    ok = gate("baseline.stable_watermarks", marks == after, "no writes or deletions while the baseline was read")
    ok &= gate("baseline.count_equals_offsets", all(r["count"] == r["high_watermark"] - r["log_start"] for r in results),
               f"{sum(r['count'] for r in results):,} records read")
    ok &= gate("baseline.contiguous_offsets", all(r["offset_gaps"] == 0 for r in results))
    ok &= gate("baseline.create_time_and_wire_format",
               all(r["non_create_time"] == 0 and r["bad_magic"] == 0 for r in results))
    rolled_match = all(s["timeindex_max_ts"] == s["max_ts"] for r in results for s in r["segments"] if not s["active"])
    ok &= gate("baseline.segment_max_ts_matches_timeindex", rolled_match,
               f"{sum(len(r['segments']) for r in results)} segments; broker time index agrees with record scan")
    ok &= gate("baseline.segment_counts", all(sum(s["count"] for s in r["segments"]) == r["count"] for r in results))
    if seed:
        late = {int(p): v["late"] for p, v in seed["per_partition"].items()}
        ok &= gate("baseline.only_late_cohort_goes_backwards",
                   all(r["backwards_steps"] == late[r["partition"]] for r in results),
                   f"{sum(late.values()):,} late records, every other timestamp non-decreasing")
        ok &= gate("baseline.nothing_after_seed_end", all(r["max_ts"] <= seed["seed_end_ts"] for r in results))
    log(f"baseline complete in {seconds:.0f}s, schema ids {out['schema_ids']}")
    return 0 if ok else 1
