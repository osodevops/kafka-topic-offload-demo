"""Checks on a backup that do not trust kafka-backup.

verify-backup
  * manifest: contiguous segment ranges from the source log start to the high watermark,
    no gaps, no pruned ranges, record counts adding up, topic configs captured;
  * every stored object: SHA-256 and size equal the manifest;
  * every record, decoded with an independent KBAK reader: per UTC day bucket, count,
    min and max timestamp and running SHA-256 equal the source baseline;
  * the consumer group snapshot equals the committed offsets on the source;
  * time window hazards: records outside their segment's manifest timestamp bounds.
  With --from-offsets the archive is a partial one that starts at those offsets: the days it
  covers in full must still match the source exactly, and the day it starts in must be a subset.
s3-requests      MinIO request counters by API, for the transaction cost model.
manifest-summary segment count and sizes for a backup.
archive-coverage the offset and time range an archive holds, read from its manifest.
"""
import argparse
import hashlib
import json
import multiprocessing as mp
import os
import re
import time
from collections import Counter

import requests

from datetime import datetime, timezone

from .baseline import iso_day
from .common import DAY_MS, BucketStats, digest_input, gate, le_i64, log, read_json, run_dir, write_json
from .segments import SegmentError, parse_segment


def iso(ms):
    return "n/a" if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                        config=Config(max_pool_connections=32, retries={"max_attempts": 5, "mode": "standard"}))


def object_key(prefix: str, key: str) -> str:
    prefix = (prefix or "").strip("/")
    if not prefix or key.startswith(prefix + "/"):
        return key
    return f"{prefix}/{key}"


def get_bytes(client, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def load_manifest(bucket: str, prefix: str, backup_id: str):
    raw = get_bytes(s3_client(), bucket, object_key(prefix, f"{backup_id}/manifest.json"))
    return raw, json.loads(raw)


def _check_partition(job: dict) -> dict:
    client = s3_client()
    p = job["partition"]
    buckets = {}
    out = {"partition": p, "segments": 0, "records": 0, "object_bytes": 0, "sha_mismatch": [], "size_mismatch": [],
           "missing_sha": 0, "header_mismatch": [], "parse_errors": [], "offset_header_mismatch": 0,
           "timestamp_header_mismatch": 0, "non_contiguous": 0, "bounds_not_min_max": 0, "hazards": [],
           "max_record_bytes": 0}
    prev = None
    for seg in job["segments"]:
        key = object_key(job["prefix"], seg["key"])
        data = get_bytes(client, job["bucket"], key)
        out["segments"] += 1
        out["object_bytes"] += len(data)
        sha = hashlib.sha256(data).hexdigest()
        if not seg.get("sha256"):
            out["missing_sha"] += 1
        elif sha != seg["sha256"]:
            out["sha_mismatch"].append(key)
        if len(data) != seg.get("compressed_size", len(data)):
            out["size_mismatch"].append(key)
        if not job["content"]:
            continue
        try:
            header, records = parse_segment(data)
            tmin = tmax = None
            n = 0
            for ts, off, rkey, value, headers in records:
                n += 1
                if prev is not None and off != prev + 1:
                    out["non_contiguous"] += 1
                prev = off
                h = dict(headers)
                if "x-original-offset" not in h or le_i64(h["x-original-offset"]) != off:
                    out["offset_header_mismatch"] += 1
                if "x-original-timestamp" not in h or le_i64(h["x-original-timestamp"]) != ts:
                    out["timestamp_header_mismatch"] += 1
                tmin = ts if tmin is None else min(tmin, ts)
                tmax = ts if tmax is None else max(tmax, ts)
                size = len(rkey or b"") + len(value or b"") + sum(len(k) + len(v or b"") for k, v in headers)
                if size > out["max_record_bytes"]:
                    out["max_record_bytes"] = size
                day = ts // DAY_MS
                b = buckets.get(day)
                if b is None:
                    b = buckets[day] = BucketStats()
                b.add(off, ts, digest_input(p, off, ts, rkey, value, headers))
                if ts < seg["start_timestamp"] or ts > seg["end_timestamp"]:
                    out["hazards"].append([p, off, ts, seg["start_timestamp"], seg["end_timestamp"]])
            if (header["record_count"], header["start_offset"], header["end_offset"]) != \
                    (seg["record_count"], seg["start_offset"], seg["end_offset"]) or n != seg["record_count"]:
                out["header_mismatch"].append(key)
            if (tmin, tmax) != (seg["start_timestamp"], seg["end_timestamp"]):
                out["bounds_not_min_max"] += 1
            out["records"] += n
        except (SegmentError, KeyError, ValueError) as e:
            out["parse_errors"].append(f"{key}: {e}")
    out["buckets"] = {iso_day(d): b.to_dict() for d, b in sorted(buckets.items())}
    log(f"verify-backup p{p}: {out['segments']} objects, {out['records']:,} records")
    return out


def committed_offsets(topic: str) -> dict:
    from .consumer_sim import group_offsets

    return group_offsets(os.environ["SOURCE_BOOTSTRAP"], topic)


def minio_request_counts() -> dict:
    # MinIO refreshes these counters about every 10 seconds; read only once the cache has
    # caught up with every request made before this call.
    time.sleep(float(os.environ.get("MINIO_METRICS_SETTLE_S", "12")))
    url = os.environ.get("MINIO_METRICS_URL", "http://minio:9000/minio/v2/metrics/node")
    counts = Counter()
    for line in requests.get(url, timeout=30).text.splitlines():
        if line.startswith("minio_s3_requests_total{"):
            api = re.search(r'api="([^"]+)"', line)
            if api:
                counts[api.group(1)] += float(line.rsplit(" ", 1)[1])
    return dict(counts)


def main(cmd: str, argv) -> int:
    ap = argparse.ArgumentParser(prog=f"verifier {cmd}")
    ap.add_argument("--topic", default=os.environ.get("TOPIC", "device-telemetry"))
    ap.add_argument("--backup-id")
    ap.add_argument("--bucket", default=os.environ.get("S3_BUCKET", "kafka-backups"))
    ap.add_argument("--prefix", default="backups")
    ap.add_argument("--anchor", choices=["write", "check", "none"], default="write")
    ap.add_argument("--sha-only", action="store_true", help="skip decoding; objects and manifest only")
    ap.add_argument("--label", default="")
    ap.add_argument("--seconds", type=float, help="backup wall clock seconds, for throughput")
    ap.add_argument("--diff", nargs=2, metavar=("BEFORE", "AFTER"))
    ap.add_argument("--group", default="telemetry-analytics", help="group that must be in the snapshot; '' for none")
    ap.add_argument("--from-offsets", help="JSON file of {partition: offset}: a partial archive that starts there")
    args = ap.parse_args(argv)
    rd = run_dir()

    if cmd == "s3-requests":
        if args.diff:
            before = read_json(rd / "s3" / f"requests-{args.diff[0]}.json")
            after = read_json(rd / "s3" / f"requests-{args.diff[1]}.json")
            diff = {k: int(after.get(k, 0) - before.get(k, 0)) for k in sorted(set(before) | set(after))}
            diff = {k: v for k, v in diff.items() if v}
            write_json(rd / "s3" / f"requests-{args.label or 'diff'}.json", diff)
            log(f"S3 requests {args.label}: {diff}")
        else:
            write_json(rd / "s3" / f"requests-{args.label}.json", minio_request_counts())
        return 0

    raw, manifest = load_manifest(args.bucket, args.prefix, args.backup_id)
    topic = next((t for t in manifest["topics"] if t["name"] == args.topic), None)
    if topic is None:
        return 0 if gate("backup.topic_in_manifest", False, f"{args.topic} not in {args.backup_id}") else 1

    segs_all = [s for p in topic["partitions"] for s in p["segments"]]
    summary = {
        "backup_id": args.backup_id, "bucket": args.bucket, "segments": len(segs_all),
        "records": sum(s["record_count"] for s in segs_all),
        "uncompressed_bytes": sum(s.get("uncompressed_size", 0) for s in segs_all),
        "compressed_bytes": sum(s.get("compressed_size", 0) for s in segs_all),
    }
    summary["compression_ratio"] = round(summary["uncompressed_bytes"] / max(summary["compressed_bytes"], 1), 3)
    if args.seconds:
        summary["seconds"] = args.seconds
        summary["mb_per_s_uncompressed"] = round(summary["uncompressed_bytes"] / args.seconds / 1e6, 1)
    if cmd == "manifest-summary":
        write_json(rd / "backup" / f"summary-{args.backup_id}.json", summary)
        log(json.dumps(summary))
        return 0

    if cmd == "archive-coverage":
        # What time and offset range this archive holds, read from its manifest. Timestamps are
        # the manifest's segment bounds (first and last record), so treat them as approximate.
        per = {}
        for part in topic["partitions"]:
            segs = sorted(part["segments"], key=lambda s: s["start_offset"])
            per[part["partition_id"]] = {
                "segments": len(segs),
                "first_offset": segs[0]["start_offset"] if segs else None,
                "last_offset": segs[-1]["end_offset"] if segs else None,
                "records": sum(s["record_count"] for s in segs),
                "earliest_timestamp": min((s["start_timestamp"] for s in segs), default=None),
                "latest_timestamp": max((s["end_timestamp"] for s in segs), default=None),
                "compressed_bytes": sum(s["compressed_size"] for s in segs),
                "pruned_ranges": part.get("pruned", []),
                "gaps": part.get("gaps", []),
            }
        earliest = min((v["earliest_timestamp"] for v in per.values() if v["earliest_timestamp"]), default=None)
        latest = max((v["latest_timestamp"] for v in per.values() if v["latest_timestamp"]), default=None)
        out = {"backup_id": args.backup_id, "bucket": args.bucket, "created_at": manifest["created_at"],
               "records": summary["records"], "segments": summary["segments"],
               "compressed_bytes": summary["compressed_bytes"],
               "earliest_timestamp": earliest, "latest_timestamp": latest,
               "pruned_ranges": sum(len(v["pruned_ranges"]) for v in per.values()), "partitions": per}
        write_json(rd / "archive" / f"coverage-{args.backup_id}{args.label}.json", out)
        span = f"{iso(earliest)} to {iso(latest)}" if earliest else "empty"
        log(f"{args.backup_id}: {summary['records']:,} records in {summary['segments']} objects, "
            f"{summary['compressed_bytes'] / 1e9:.2f} GB, covering {span} UTC"
            + (f", {out['pruned_ranges']} pruned range(s)" if out["pruned_ranges"] else ""))
        return 0

    manifest_sha = hashlib.sha256(raw).hexdigest()
    anchor_path = rd / "backup" / f"manifest-anchor-{args.backup_id}.json"
    ok = True
    if args.anchor == "write":
        write_json(anchor_path, {"backup_id": args.backup_id, "sha256": manifest_sha, "bytes": len(raw), "taken_at": int(time.time() * 1000)})
    elif args.anchor == "check":
        anchor = read_json(anchor_path)
        ok &= gate(f"backup.manifest_anchor_intact{args.label}", anchor["sha256"] == manifest_sha,
                   f"{args.bucket}: manifest sha256 {manifest_sha[:16]}... vs anchor {anchor['sha256'][:16]}...")

    baseline = read_json(rd / "baseline" / f"{args.topic}.json")
    base_parts = {int(p): v for p, v in baseline["partitions"].items()}
    # A partial archive starts at given offsets instead of the topic's log start.
    from_offsets = {int(p): int(o) for p, o in read_json(args.from_offsets).items()} if args.from_offsets else {}

    coverage = {}
    for part in topic["partitions"]:
        p = part["partition_id"]
        segs = sorted(part["segments"], key=lambda s: s["start_offset"])
        contiguous = all(b["start_offset"] == a["end_offset"] + 1 for a, b in zip(segs, segs[1:]))
        each_full = all(s["record_count"] == s["end_offset"] - s["start_offset"] + 1 for s in segs)
        bp = base_parts[p]
        coverage[p] = {
            "first_offset": segs[0]["start_offset"] if segs else None,
            "last_offset": segs[-1]["end_offset"] if segs else None,
            "records": sum(s["record_count"] for s in segs),
            "contiguous": contiguous and each_full, "gaps": part.get("gaps", []), "pruned": part.get("pruned", []),
            "expected_first": from_offsets.get(p, bp["log_start"]), "expected_last": bp["high_watermark"] - 1,
        }
    full = all(c["contiguous"] and c["first_offset"] == c["expected_first"] and c["last_offset"] == c["expected_last"]
               and c["records"] == c["expected_last"] - c["expected_first"] + 1 for c in coverage.values())
    ok &= gate(f"backup.manifest_covers_log_start_to_high_watermark{args.label}",
               full and len(coverage) == len(base_parts),
               f"{summary['records']:,} records in {len(segs_all)} segments"
               + (f", starting at the requested offsets {from_offsets}" if from_offsets else ""))
    ok &= gate(f"backup.manifest_no_gaps_or_pruned{args.label}", all(not c["gaps"] and not c["pruned"] for c in coverage.values()))
    configs = topic.get("configurations", {})
    ok &= gate(f"backup.topic_config_captured{args.label}",
               configs.get("retention.ms") == "-1" and topic.get("original_partition_count") == len(base_parts),
               f"retention.ms={configs.get('retention.ms')} partitions={topic.get('original_partition_count')}")

    jobs = [{"partition": part["partition_id"], "bucket": args.bucket, "prefix": args.prefix, "content": not args.sha_only,
             "segments": sorted(part["segments"], key=lambda s: s["start_offset"])} for part in topic["partitions"]]
    t0 = time.monotonic()
    with mp.get_context("spawn").Pool(len(jobs)) as pool:
        results = pool.map(_check_partition, jobs)
    summary["verify_seconds"] = round(time.monotonic() - t0, 1)

    ok &= gate(f"backup.object_sha256_matches_manifest{args.label}",
               all(not r["sha_mismatch"] and not r["size_mismatch"] and r["missing_sha"] == 0 for r in results),
               f"{sum(r['segments'] for r in results)} objects, {sum(r['object_bytes'] for r in results) / 1e9:.2f} GB hashed")

    detail = {"summary": summary, "manifest_sha256": manifest_sha, "coverage": coverage,
              "partitions": {r["partition"]: {k: v for k, v in r.items() if k not in ("buckets", "hazards")} for r in results}}

    if not args.sha_only:
        ok &= gate(f"backup.segments_decode_independently{args.label}",
                   all(not r["parse_errors"] and not r["header_mismatch"] and r["non_contiguous"] == 0 for r in results),
                   "CRC, header, record count and offsets agree for every segment")
        ok &= gate(f"backup.offset_headers_little_endian{args.label}",
                   all(r["offset_header_mismatch"] == 0 and r["timestamp_header_mismatch"] == 0 for r in results),
                   "x-original-offset and x-original-timestamp decode as i64 LE and equal the record")
        # Days the archive covers in full must match the source exactly. With --from-offsets the
        # day the archive starts in is partly covered, so it only has to be a subset.
        mismatched, full_days = [], 0
        for r in results:
            p = r["partition"]
            start = from_offsets.get(p, base_parts[p]["log_start"])
            expected, got = base_parts[p]["buckets"], r["buckets"]
            for day, b in expected.items():
                covered_in_full = b["first_offset"] >= start
                g = got.get(day)
                if covered_in_full:
                    full_days += 1
                    if g != b:
                        mismatched.append({"partition": p, "day": day, "reason": "differs from the source"})
                elif g is not None and g["count"] > b["count"]:
                    mismatched.append({"partition": p, "day": day, "reason": "more records than the source"})
            for day in set(got) - set(expected):
                mismatched.append({"partition": p, "day": day, "reason": "day not present in the source"})
        ok &= gate(f"backup.content_matches_source_baseline{args.label}", not mismatched,
                   f"{full_days} fully covered partition-day buckets: count, min/max ts and SHA-256 identical"
                   + (f"; mismatches {mismatched[:3]}" if mismatched else ""))

        hazards = [h for r in results for h in r["hazards"]]
        write_json(rd / "backup" / f"time-window-hazards-{args.backup_id}.json",
                   {"fields": ["partition", "offset", "timestamp", "segment_start_timestamp", "segment_end_timestamp"],
                    "hazards": hazards})
        not_min_max = sum(r["bounds_not_min_max"] for r in results)
        detail["time_window_hazards"] = len(hazards)
        detail["segments_bounds_not_min_max"] = not_min_max
        print(f"FINDING manifest segment timestamps are first/last record, not min/max: {not_min_max} of "
              f"{len(segs_all)} segments; {len(hazards):,} records lie outside their segment's bounds", flush=True)

        detail["max_record_bytes"] = max(r["max_record_bytes"] for r in results)

        snap_key = object_key(args.prefix, f"{args.backup_id}/consumer-groups-snapshot.json")
        snapshot = json.loads(get_bytes(s3_client(), args.bucket, snap_key))
        snap = {g["group_id"]: {int(p): o for p, o in g["offsets"].get(args.topic, {}).items()} for g in snapshot["groups"]}
        snap = {g: v for g, v in snap.items() if v}
        live = committed_offsets(args.topic)
        detail["group_snapshot"] = {"snapshot": snap, "live": live}
        ok &= gate(f"backup.consumer_group_snapshot_matches_committed{args.label}",
                   (not args.group or args.group in snap) and snap == live, f"groups {sorted(snap)}")

    write_json(rd / "backup" / f"verify-{args.backup_id}{args.label}.json", dict(detail, passed=bool(ok)))
    return 0 if ok else 1
