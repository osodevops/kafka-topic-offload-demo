"""Exact prediction of what lowering retention.ms deletes, and the check afterwards.

Kafka's time retention (UnifiedLog.deleteRetentionMsBreachedSegments):
  * walk segments oldest first;
  * a segment is deletable when now - largestTimestamp > retention.ms and the next
    segment's base offset (or the log end) is at or below the high watermark;
  * stop at the first segment that is not deletable;
  * if every segment qualifies the active segment is rolled and deleted as well.
largestTimestamp is the maximum record timestamp in the segment, so one late or future
record keeps a whole segment. The new log start offset is the base offset of the first
kept segment.
"""
import argparse
import os
import time

from .backup_checks import load_manifest
from .broker_files import segment_inventory
from .common import DAY_MS, gate, log, read_json, run_dir, watermarks, write_json


def predict_partition(segments, high_watermark: int, now_ms: int, retention_ms: int):
    deleted = []
    for i, seg in enumerate(segments):
        nxt = segments[i + 1] if i + 1 < len(segments) else None
        upper = nxt["base_offset"] if nxt else high_watermark
        empty_last = nxt is None and seg["count"] == 0
        breached = seg["max_ts"] is not None and now_ms - seg["max_ts"] > retention_ms
        if high_watermark >= upper and breached and not empty_last:
            deleted.append(seg)
        else:
            break
    return deleted, segments[len(deleted):]


def build_prediction(baseline: dict, now_ms: int, retention_ms: int) -> dict:
    parts = {}
    for p, bp in sorted(baseline["partitions"].items(), key=lambda kv: int(kv[0])):
        deleted, kept = predict_partition(bp["segments"], bp["high_watermark"], now_ms, retention_ms)
        first_kept = kept[0] if kept else None
        parts[int(p)] = {
            "log_start": bp["log_start"],
            "high_watermark": bp["high_watermark"],
            "predicted_log_start": first_kept["base_offset"] if first_kept else bp["high_watermark"],
            "deleted_segments": [s["base_offset"] for s in deleted],
            "deleted_bytes": sum(s["log_bytes"] for s in deleted),
            "deleted_records": sum(s["count"] for s in deleted),
            "deleted_max_ts": max((s["max_ts"] for s in deleted), default=None),
            "kept_segments": [s["base_offset"] for s in kept],
            "kept_bytes": sum(s["log_bytes"] for s in kept),
            "first_kept_min_ts": first_kept["min_ts"] if first_kept else None,
            "first_kept_max_ts": first_kept["max_ts"] if first_kept else None,
            "whole_partition": not kept,
        }
    return {
        "now": now_ms, "retention_ms": retention_ms, "cut_ts": now_ms - retention_ms, "partitions": parts,
        "deleted_bytes": sum(v["deleted_bytes"] for v in parts.values()),
        "deleted_records": sum(v["deleted_records"] for v in parts.values()),
        "kept_bytes": sum(v["kept_bytes"] for v in parts.values()),
    }


def ambiguous_segments(baseline: dict, now_ms: int, retention_ms: int, margin_ms: int):
    """Segments whose deletion depends on exactly when the broker's retention check runs."""
    out = []
    for p, bp in baseline["partitions"].items():
        for s in bp["segments"]:
            if s["max_ts"] is not None and abs((now_ms - s["max_ts"]) - retention_ms) <= margin_ms:
                out.append((int(p), s["base_offset"], s["max_ts"] + retention_ms + margin_ms - now_ms))
    return out


def main(cmd: str, argv) -> int:
    ap = argparse.ArgumentParser(prog=f"verifier {cmd}")
    ap.add_argument("--topic", default=os.environ.get("TOPIC", "device-telemetry"))
    ap.add_argument("--retention-ms", type=int, default=int(os.environ.get("RETENTION_AFTER_PRUNE_MS", str(30 * DAY_MS))))
    ap.add_argument("--margin-ms", type=int, default=15 * 60 * 1000)
    ap.add_argument("--data-dir", default="/kafka-data/source")
    ap.add_argument("--backup-id")
    ap.add_argument("--bucket", default=os.environ.get("S3_BUCKET", "kafka-backups"))
    ap.add_argument("--sealed-bucket", default=os.environ.get("S3_SEALED_BUCKET", "kafka-backups-sealed"))
    ap.add_argument("--prefix", default="backups")
    ap.add_argument("--out", default="prediction.json")
    ap.add_argument("--compare-to", help="an earlier prediction that must delete exactly the same segments")
    ap.add_argument("--timeout-s", type=int, default=900)
    args = ap.parse_args(argv)
    rd = run_dir()
    bootstrap = os.environ["SOURCE_BOOTSTRAP"]
    backup_id = args.backup_id or args.topic
    baseline = read_json(rd / "baseline" / f"{args.topic}.json")
    parts = sorted(int(p) for p in baseline["partitions"])

    if cmd == "prune-predict":
        ok = True
        verify = rd / "backup" / f"verify-{backup_id}.json"
        ok &= gate("prune_gate.backup_verified", verify.exists() and read_json(verify)["passed"], str(verify.name))
        anchor = read_json(rd / "backup" / f"manifest-anchor-{backup_id}.json")
        for bucket in (args.bucket, args.sealed_bucket):
            raw, manifest = load_manifest(bucket, args.prefix, backup_id)
            import hashlib

            ok &= gate(f"prune_gate.manifest_anchor_intact.{bucket}", hashlib.sha256(raw).hexdigest() == anchor["sha256"])
        seal = rd / "seal" / "seal.json"
        ok &= gate("prune_gate.sealed_copy_verified", seal.exists() and read_json(seal).get("passed") is True)

        topic = next(t for t in manifest["topics"] if t["name"] == args.topic)
        marks = watermarks(bootstrap, args.topic, parts)
        covered = True
        for part in topic["partitions"]:
            segs = sorted(part["segments"], key=lambda s: s["start_offset"])
            lo, hi = marks[part["partition_id"]]
            covered &= bool(segs) and segs[0]["start_offset"] <= lo and segs[-1]["end_offset"] >= hi - 1 and not part.get("gaps")
        ok &= gate("prune_gate.backup_covers_live_topic", covered and len(topic["partitions"]) == len(parts),
                   "sealed manifest spans the current log start to high watermark on every partition, no gaps")

        inventory = segment_inventory(args.data_dir, args.topic, parts)
        unchanged = all(
            [(s["base_offset"], s["log_bytes"]) for s in inventory[p]]
            == [(s["base_offset"], s["log_bytes"]) for s in baseline["partitions"][str(p)]["segments"]]
            and marks[p] == (baseline["partitions"][str(p)]["log_start"], baseline["partitions"][str(p)]["high_watermark"])
            for p in parts)
        ok &= gate("prune_gate.topic_unchanged_since_baseline", unchanged, "same segments, sizes and watermarks")

        # A segment inside the +/- margin band clears within 2 x margin, so wait that long at most.
        deadline = time.monotonic() + 2 * args.margin_ms / 1000 + 60
        while True:
            now = int(time.time() * 1000)
            amb = ambiguous_segments(baseline, now, args.retention_ms, args.margin_ms)
            if not amb or time.monotonic() > deadline:
                break
            wait_s = min(max(a[2] for a in amb) / 1000 + 1, deadline - time.monotonic())
            log(f"{len(amb)} segment(s) within {args.margin_ms / 60000:.0f} min of the cut; waiting {wait_s:.0f}s for a clean prediction")
            time.sleep(max(wait_s, 1))
        ok &= gate("prune_gate.prediction_unambiguous", not amb,
                   f"no segment's largest timestamp within {args.margin_ms // 60000} min of the cut" if not amb else
                   f"{len(amb)} segment(s) still within {args.margin_ms // 60000} min of the cut after waiting: "
                   + ", ".join(f"p{p}@{base}" for p, base, _ in amb[:5]))

        seed = read_json(rd / "seed" / f"{args.topic}.json")
        ok &= gate("prune_gate.newest_data_survives", seed["seed_end_ts"] > now - args.retention_ms + args.margin_ms,
                   "SEED_END_TS is inside the new retention, so the topic is not emptied")

        prediction = build_prediction(baseline, now, args.retention_ms)
        ok &= gate("prune_gate.no_partition_emptied", not any(v["whole_partition"] for v in prediction["partitions"].values()))
        if args.compare_to:
            earlier = read_json(rd / "prune" / args.compare_to)
            same = all(earlier["partitions"][str(p)]["deleted_segments"] == prediction["partitions"][p]["deleted_segments"] for p in parts)
            ok &= gate("prune.prediction_unchanged_since_gate", same, f"gate prediction made {(now - earlier['now']) // 1000}s ago")
        write_json(rd / "prune" / args.out, dict(prediction, passed=bool(ok)))
        log(f"predicted: delete {prediction['deleted_records']:,} records, {prediction['deleted_bytes'] / 1e9:.2f} GB; "
            f"keep {prediction['kept_bytes'] / 1e9:.2f} GB")
        return 0 if ok else 1

    if cmd == "prune-check":
        prediction = read_json(rd / "prune" / args.out)
        target = {p: prediction["partitions"][str(p)]["predicted_log_start"] for p in parts}
        deadline = time.monotonic() + args.timeout_s
        while True:
            marks = watermarks(bootstrap, args.topic, parts)
            if all(marks[p][0] >= target[p] for p in parts) or time.monotonic() > deadline:
                break
            time.sleep(5)
        first_seen = int(time.time() * 1000)
        log("log start offsets reached the prediction; waiting two more retention checks for any over-deletion")
        time.sleep(70)
        marks = watermarks(bootstrap, args.topic, parts)
        observed = int(time.time() * 1000)
        inventory = segment_inventory(args.data_dir, args.topic, parts)

        ok = gate("prune.log_start_matches_prediction", all(marks[p][0] == target[p] for p in parts),
                  ", ".join(f"p{p} {marks[p][0]}" for p in parts))
        ok &= gate("prune.remaining_segments_match_prediction",
                   all([s["base_offset"] for s in inventory[p]] == prediction["partitions"][str(p)]["kept_segments"] for p in parts))
        before = sum(s["log_bytes"] for p in parts for s in baseline["partitions"][str(p)]["segments"])
        after = sum(s["log_bytes"] for p in parts for s in inventory[p])
        ok &= gate("prune.bytes_reclaimed_match_prediction", before - after == prediction["deleted_bytes"],
                   f"{(before - after) / 1e9:.3f} GB reclaimed, predicted {prediction['deleted_bytes'] / 1e9:.3f} GB")
        newest_deleted = max((v["deleted_max_ts"] for v in prediction["partitions"].values() if v["deleted_max_ts"]), default=None)
        ok &= gate("prune.only_data_older_than_retention_deleted",
                   newest_deleted is not None and newest_deleted < observed - args.retention_ms,
                   f"newest deleted record is {(observed - newest_deleted) / DAY_MS:.2f} days old, retention {args.retention_ms / DAY_MS:.0f} days")

        _, manifest = load_manifest(args.sealed_bucket, args.prefix, backup_id)
        topic = next(t for t in manifest["topics"] if t["name"] == args.topic)
        covers = all(
            min(s["start_offset"] for s in part["segments"]) <= prediction["partitions"][str(part["partition_id"])]["log_start"]
            and max(s["end_offset"] for s in part["segments"]) >= target[part["partition_id"]] - 1
            for part in topic["partitions"])
        ok &= gate("prune.sealed_backup_covers_every_deleted_offset", covers)

        retained_old = {p: prediction["partitions"][str(p)]["first_kept_min_ts"] for p in parts}
        result = {
            "first_seen_at": first_seen, "observed_at": observed, "watermarks": {p: {"low": lo, "high": hi} for p, (lo, hi) in marks.items()},
            "bytes_before": before, "bytes_after": after, "bytes_reclaimed": before - after,
            "reclaimed_pct": round(100 * (before - after) / before, 2),
            "records_deleted": sum(target[p] - prediction["partitions"][str(p)]["log_start"] for p in parts),
            "oldest_retained_ts": retained_old,
            "note": "retention deletes whole segments: records older than the cut remain until their segment's newest record ages out",
            "passed": bool(ok),
        }
        write_json(rd / "prune" / "check.json", result)
        return 0 if ok else 1

    raise SystemExit(f"unknown command {cmd}")
