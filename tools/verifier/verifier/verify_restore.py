"""Compare a restored topic with the source baseline.

Restored offsets differ from the source, so every record is keyed by its x-original-offset
header (8 byte little endian i64) and hashed exactly as the baseline hashed the source
record, excluding the headers kafka-backup adds. Per partition and UTC day bucket the
count, min and max timestamp and running SHA-256 must then be identical.

Where a time window restore returns fewer records than the baseline, every missing record
must be accounted for by the backup manifest's segment timestamp bounds (first and last
record, not min and max). That turns "the numbers differ" into a named defect.
"""
import argparse
import io
import os
import struct
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import requests

from .baseline import iso_day
from .common import DAY_MS, BucketStats, digest_input, gate, le_i64, log, read_json, read_partition, run_dir, watermarks, write_json

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _to_ms(value) -> int:
    if isinstance(value, datetime):
        return (value - EPOCH) // __import__("datetime").timedelta(milliseconds=1)
    return int(value)


def _scan(job: dict) -> dict:
    from confluent_kafka import TIMESTAMP_CREATE_TIME

    p, ws, we = job["partition"], job["window_start"], job["window_end"]
    buckets, schema_ids, samples, probe = {}, Counter(), [], {}
    st = {"prev": None, "not_increasing": 0, "missing_header": 0, "ts_header_mismatch": 0, "non_create": 0,
          "outside_window": 0, "source_partition_mismatch": 0, "duplicate_offset_headers": 0, "n": 0}

    def on_msg(m):
        ts_type, ts = m.timestamp()
        if ts_type != TIMESTAMP_CREATE_TIME:
            st["non_create"] += 1
        headers = m.headers() or []
        h = dict(headers)
        if len(h) != len(headers):
            st["duplicate_offset_headers"] += 1
        raw = h.get("x-original-offset")
        if raw is None or len(raw) != 8:
            st["missing_header"] += 1
            return
        orig = le_i64(raw)
        if st["prev"] is not None and orig <= st["prev"]:
            st["not_increasing"] += 1
        st["prev"] = orig
        raw_ts = h.get("x-original-timestamp")
        if raw_ts is None or le_i64(raw_ts) != ts:
            st["ts_header_mismatch"] += 1
        sp = h.get("x-source-partition")
        if sp is not None and len(sp) == 4 and struct.unpack("<i", sp)[0] != p:
            st["source_partition_mismatch"] += 1
        if (ws is not None and ts < ws) or (we is not None and ts > we):
            st["outside_window"] += 1
        value = m.value()
        day = ts // DAY_MS
        b = buckets.get(day)
        if b is None:
            b = buckets[day] = BucketStats()
        b.add(orig, ts, digest_input(p, orig, ts, m.key(), value, headers))
        if value and len(value) >= 5 and value[0] == 0:
            sid = struct.unpack_from(">I", value, 1)[0]
            schema_ids[sid] += 1
            if st["n"] % job["sample_every"] == 0 and len(samples) < job["max_samples"]:
                samples.append((sid, ts, value))
        if m.offset() == job.get("probe_offset"):
            probe.update(target_offset=m.offset(), x_original_offset=orig)
        st["n"] += 1

    lo, hi = job["low"], job["high"]
    count = read_partition(job["bootstrap"], job["topic"], p, lo, hi, on_msg)

    # Decode a sample with schemas fetched from the registry the restore will be read with.
    from fastavro import parse_schema, schemaless_reader

    schemas, decode_errors, event_time_mismatch = {}, [], 0
    for sid, ts, value in samples:
        try:
            if sid not in schemas:
                r = requests.get(f"{job['registry']}/schemas/ids/{sid}", timeout=30)
                r.raise_for_status()
                schemas[sid] = parse_schema(__import__("json").loads(r.json()["schema"]))
            rec = schemaless_reader(io.BytesIO(value[5:]), schemas[sid])
            if _to_ms(rec["eventTime"]) != ts:
                event_time_mismatch += 1
        except Exception as e:  # any failure to decode is the finding
            if len(decode_errors) < 5:
                decode_errors.append(f"id {sid}: {type(e).__name__}: {e}")
            else:
                decode_errors.append("")
    log(f"verify-restore {job['topic']} p{p}: {count:,} records, {len(samples)} decoded samples")
    return dict(st, partition=p, count=count, low=lo, high=hi, probe=probe, schema_ids=dict(schema_ids),
                samples=len(samples), decode_errors=decode_errors, event_time_mismatch=event_time_mismatch,
                buckets={iso_day(d): b.to_dict() for d, b in sorted(buckets.items())})


def main(_cmd: str, argv) -> int:
    import json
    import multiprocessing as mp

    ap = argparse.ArgumentParser(prog="verifier verify-restore")
    ap.add_argument("--source-topic", default=os.environ.get("TOPIC", "device-telemetry"))
    ap.add_argument("--target-topic", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--window-start", type=int)
    ap.add_argument("--window-end", type=int)
    ap.add_argument("--exact-from", help="first UTC day (YYYY-MM-DD) that must match exactly; default all days in the window")
    ap.add_argument("--exact-to", help="last UTC day that must match exactly")
    ap.add_argument("--backup-id")
    ap.add_argument("--group", help="consumer group whose offsets must map through x-original-offset")
    ap.add_argument("--registry", default=os.environ.get("TARGET_SR"))
    ap.add_argument("--samples-per-partition", type=int, default=500)
    args = ap.parse_args(argv)
    rd = run_dir()
    bootstrap = os.environ["TARGET_BOOTSTRAP"]
    backup_id = args.backup_id or args.source_topic

    baseline = read_json(rd / "baseline" / f"{args.source_topic}.json")
    base_parts = {int(p): v for p, v in baseline["partitions"].items()}
    parts = sorted(base_parts)
    marks = watermarks(bootstrap, args.target_topic, parts)

    target_committed = {}
    if args.group:
        from .consumer_sim import group_offsets

        target_committed = group_offsets(bootstrap, args.target_topic).get(args.group, {})

    expected_records = sum(hi - lo for lo, hi in marks.values())
    jobs = [{
        "bootstrap": bootstrap, "topic": args.target_topic, "partition": p, "low": marks[p][0], "high": marks[p][1],
        "window_start": args.window_start, "window_end": args.window_end, "registry": args.registry,
        "sample_every": max(1, (marks[p][1] - marks[p][0]) // args.samples_per_partition),
        "max_samples": args.samples_per_partition, "probe_offset": target_committed.get(p),
    } for p in parts]
    t0 = time.monotonic()
    with mp.get_context("spawn").Pool(len(jobs)) as pool:
        results = {r["partition"]: r for r in pool.map(_scan, jobs)}
    seconds = time.monotonic() - t0

    ws, we = args.window_start, args.window_end
    hazards_path = rd / "backup" / f"time-window-hazards-{backup_id}.json"
    predicted_missing = Counter()
    if hazards_path.exists() and (ws is not None or we is not None):
        for p, off, ts, s_start, s_end in read_json(hazards_path)["hazards"]:
            in_window = (ws is None or ts >= ws) and (we is None or ts <= we)
            excluded = (ws is not None and s_end < ws) or (we is not None and s_start > we)
            if in_window and excluded:
                predicted_missing[(p, iso_day(ts // DAY_MS))] += 1

    def fully_inside(day_iso: str) -> bool:
        start = int(datetime.strptime(day_iso, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        return (ws is None or start >= ws) and (we is None or start + DAY_MS - 1 <= we)

    all_days = sorted({d for p in parts for d in base_parts[p]["buckets"]} | {d for r in results.values() for d in r["buckets"]})
    window_days = [d for d in all_days if fully_inside(d)]
    exact_days = [d for d in window_days if (not args.exact_from or d >= args.exact_from) and (not args.exact_to or d <= args.exact_to)]

    diffs, unexplained, exact_fail = [], [], []
    observed_missing = 0
    for p in parts:
        base_b, rest_b = base_parts[p]["buckets"], results[p]["buckets"]
        for d in window_days:
            b, r = base_b.get(d), rest_b.get(d)
            if b == r:
                continue
            bc, rc = (b or {}).get("count", 0), (r or {}).get("count", 0)
            miss = predicted_missing.get((p, d), 0)
            diffs.append({"partition": p, "day": d, "baseline_count": bc, "restored_count": rc, "predicted_missing": miss})
            observed_missing += bc - rc
            if bc - rc != miss or miss == 0:
                unexplained.append(diffs[-1])
            if d in exact_days:
                exact_fail.append(diffs[-1])
        for d in all_days:
            if not fully_inside(d) and d in rest_b and rest_b[d]["count"] > base_b.get(d, {}).get("count", 0):
                unexplained.append({"partition": p, "day": d, "note": "more records than the source on a partial day"})

    label = args.label
    ok = gate(f"restore.{label}.original_offsets_strictly_increasing",
              all(r["missing_header"] == 0 and r["not_increasing"] == 0 and r["source_partition_mismatch"] == 0 for r in results.values()),
              "every record carries x-original-offset; per partition strictly increasing, no duplicates")
    ok &= gate(f"restore.{label}.create_time_preserved",
               all(r["non_create"] == 0 and r["ts_header_mismatch"] == 0 for r in results.values()))
    ok &= gate(f"restore.{label}.no_records_outside_window", all(r["outside_window"] == 0 for r in results.values()),
               f"window {ws}..{we}")
    restored = sum(r["count"] for r in results.values())
    ok &= gate(f"restore.{label}.buckets_match_baseline", not exact_fail and restored > 0,
               f"{restored:,} records; {len(exact_days)} days x {len(parts)} partitions compared exactly"
               + (f"; {len(exact_fail)} buckets differ, first {exact_fail[:3]}" if exact_fail else ""))
    ok &= gate(f"restore.{label}.every_difference_explained_by_segment_bounds", not unexplained,
               f"{observed_missing} records missing across {len(diffs)} buckets, {sum(predicted_missing.values())} predicted by manifest bounds"
               + (f"; unexplained {unexplained[:3]}" if unexplained else ""))
    samples = sum(r["samples"] for r in results.values())
    decode_errors = [e for r in results.values() for e in r["decode_errors"]]
    ok &= gate(f"restore.{label}.avro_decodes_with_registry",
               samples > 0 and not decode_errors and all(r["event_time_mismatch"] == 0 for r in results.values()),
               f"{samples} sampled records decoded via {args.registry}; eventTime equals CreateTime"
               + (f"; {len(decode_errors)} failures, e.g. {[e for e in decode_errors if e][:2]}" if decode_errors else ""))

    if args.exact_from and args.exact_to and hazards_path.exists():
        a_start = int(datetime.strptime(args.exact_from, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        a_end = int(datetime.strptime(args.exact_to, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000) + DAY_MS - 1
        load_bearing = sum(1 for _, _, ts, s_start, s_end in read_json(hazards_path)["hazards"]
                           if a_start <= ts <= a_end and (s_end < a_start or s_start > a_end))
        print(f"FINDING {load_bearing} records in {args.exact_from}..{args.exact_to} would be missed by an unpadded "
              f"window because their backup segment's first/last timestamps fall outside it", flush=True)

    dup = sum(r["duplicate_offset_headers"] for r in results.values())
    if dup:
        print(f"FINDING {dup:,} restored records in {args.target_topic} carry a header key more than once: the backup "
              f"already wrote x-original-* headers and the header-based restore appended them again (same values)", flush=True)

    group_result = None
    if args.group:
        verify = read_json(rd / "backup" / f"verify-{backup_id}.json")
        source_committed = {int(p): o for p, o in verify["group_snapshot"]["snapshot"].get(args.group, {}).items()}
        checks = {}
        for p in parts:
            src = source_committed.get(p)
            tgt = target_committed.get(p)
            if src is None or tgt is None:
                checks[p] = {"source": src, "target": tgt, "ok": False}
            elif src == base_parts[p]["high_watermark"]:
                checks[p] = {"source": src, "target": tgt, "ok": tgt == marks[p][1]}
            else:
                probe = results[p]["probe"]
                checks[p] = {"source": src, "target": tgt, "record_x_original_offset": probe.get("x_original_offset"),
                             "ok": probe.get("x_original_offset") == src}
        group_result = checks
        ok &= gate(f"restore.{label}.consumer_group_offset_mapped", bool(checks) and all(c["ok"] for c in checks.values()),
                   f"{args.group}: the target record at each committed offset carries the source committed offset")

    schema_ids = sorted({int(i) for r in results.values() for i in r["schema_ids"]})
    write_json(rd / "restore" / f"verify-{label}.json", {
        "target_topic": args.target_topic, "window_start": ws, "window_end": we, "exact_days": [exact_days[:1], exact_days[-1:]],
        "records": restored, "expected_by_watermarks": expected_records, "seconds": round(seconds, 1),
        "schema_ids": schema_ids, "differences": diffs, "unexplained": unexplained,
        "predicted_missing_total": sum(predicted_missing.values()), "group": group_result,
        "partitions": {p: {k: v for k, v in r.items() if k != "buckets"} for p, r in results.items()},
        "passed": bool(ok),
    })
    print(f"SCHEMA_IDS {','.join(map(str, schema_ids))}", flush=True)
    return 0 if ok else 1
