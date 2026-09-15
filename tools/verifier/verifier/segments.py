"""Read a kafka-backup KBAK segment straight from object storage, without kafka-backup.

Layout (kafka-backup-core/src/segment/format.rs, v1):
  header  32 bytes: "KBAK", version u8, compression u8 (0 none, 1 zstd, 2 lz4), 2 reserved,
                    record_count u64 LE, start_offset i64 LE, end_offset i64 LE
  payload compressed as a whole; decompressed it is a run of records:
          total_len u32 LE (bytes after this field), timestamp i64 LE, offset i64 LE,
          key_len i32 LE (-1 null) + key, value_len i32 LE (-1 null) + value,
          header_count u16 LE, then per header: key_len u16 LE + key, value_len i32 LE (-1 null) + value
  footer  8 bytes: CRC32 u32 LE over every preceding byte, then "BKAE"
"""
import argparse
import base64
import io
import json
import os
import struct
import zlib

from .common import OFFSET_HEADERS, gate, le_i64, log, run_dir, write_json

HEADER = struct.Struct("<4sBB2sQqq")
FOOTER = struct.Struct("<I4s")


class SegmentError(ValueError):
    pass


def parse_segment(data: bytes):
    if len(data) < HEADER.size + FOOTER.size:
        raise SegmentError("segment shorter than header and footer")
    magic, version, compression, _, record_count, start_offset, end_offset = HEADER.unpack_from(data, 0)
    crc, magic_end = FOOTER.unpack_from(data, len(data) - FOOTER.size)
    if magic != b"KBAK" or magic_end != b"BKAE":
        raise SegmentError("bad magic bytes")
    if version != 1:
        raise SegmentError(f"unsupported segment version {version}")
    if zlib.crc32(data[: len(data) - FOOTER.size]) != crc:
        raise SegmentError("CRC32 mismatch")
    payload = data[HEADER.size: len(data) - FOOTER.size]
    if compression == 1:
        import zstandard

        payload = zstandard.ZstdDecompressor().stream_reader(io.BytesIO(payload)).read()
    elif compression == 2:
        raise SegmentError("lz4 segments are not handled by this decoder")
    elif compression != 0:
        raise SegmentError(f"unknown compression byte {compression}")

    header = {"record_count": record_count, "start_offset": start_offset, "end_offset": end_offset,
              "compression": {0: "none", 1: "zstd"}[compression]}
    return header, _records(payload)


def _records(buf: bytes):
    pos, n = 0, len(buf)
    while pos < n:
        (total_len,) = struct.unpack_from("<I", buf, pos)
        pos += 4
        end = pos + total_len
        ts, offset, key_len = struct.unpack_from("<qqi", buf, pos)
        pos += 20
        key = None
        if key_len >= 0:
            key = buf[pos: pos + key_len]
            pos += key_len
        (value_len,) = struct.unpack_from("<i", buf, pos)
        pos += 4
        value = None
        if value_len >= 0:
            value = buf[pos: pos + value_len]
            pos += value_len
        (header_count,) = struct.unpack_from("<H", buf, pos)
        pos += 2
        headers = []
        for _ in range(header_count):
            (hk_len,) = struct.unpack_from("<H", buf, pos)
            pos += 2
            hk = buf[pos: pos + hk_len].decode("utf-8")
            pos += hk_len
            (hv_len,) = struct.unpack_from("<i", buf, pos)
            pos += 4
            hv = None
            if hv_len >= 0:
                hv = buf[pos: pos + hv_len]
                pos += hv_len
            headers.append((hk, hv))
        if pos != end:
            raise SegmentError(f"record at offset {offset} length mismatch")
        yield ts, offset, key, value, headers


def fetch(bucket: str, key: str) -> bytes:
    import boto3

    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def choose_segment(bucket: str, prefix: str, backup_id: str, topic: str, partition: int, at_ts) -> str:
    from .backup_checks import load_manifest, object_key

    _, manifest = load_manifest(bucket, prefix, backup_id)
    t = next(t for t in manifest["topics"] if t["name"] == topic)
    segs = sorted(next(p for p in t["partitions"] if p["partition_id"] == partition)["segments"], key=lambda s: s["start_offset"])
    if at_ts is not None:
        seg = next((s for s in segs if s["start_timestamp"] <= at_ts <= s["end_timestamp"]), segs[len(segs) // 2])
    else:
        seg = segs[len(segs) // 2]
    return object_key(prefix, seg["key"])


def corrupt(bucket: str, key: str) -> dict:
    """Negative test only: flip one byte in the middle of a stored segment."""
    import hashlib

    import boto3

    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
    data = bytearray(fetch(bucket, key))
    before = hashlib.sha256(data).hexdigest()
    pos = len(data) // 2
    data[pos] ^= 0xFF
    s3.put_object(Bucket=bucket, Key=key, Body=bytes(data))
    return {"key": key, "flipped_byte": pos, "sha256_before": before, "sha256_after": hashlib.sha256(data).hexdigest()}


def main(cmd: str, argv) -> int:
    ap = argparse.ArgumentParser(prog=f"verifier {cmd}")
    ap.add_argument("--bucket", default=os.environ.get("S3_BUCKET", "kafka-backups"))
    ap.add_argument("--key", help="object key of a segment-*.bin.zst; default: chosen from the manifest")
    ap.add_argument("--prefix", default="backups")
    ap.add_argument("--backup-id")
    ap.add_argument("--topic", default=os.environ.get("TOPIC", "device-telemetry"))
    ap.add_argument("--partition", type=int, default=0)
    ap.add_argument("--at-ts", type=int, help="pick the segment whose manifest bounds contain this timestamp")
    ap.add_argument("--schemas", help="schemas export JSON, to decode Avro values")
    ap.add_argument("--limit", type=int, default=5, help="records to write as JSON lines")
    args = ap.parse_args(argv)

    if not args.key:
        args.key = choose_segment(args.bucket, args.prefix, args.backup_id or args.topic, args.topic, args.partition, args.at_ts)
    if cmd == "corrupt-object":
        result = corrupt(args.bucket, args.key)
        write_json(run_dir() / "negative" / "corrupted-object.json", result)
        log(json.dumps(result))
        return 0

    data = fetch(args.bucket, args.key)
    header, records = parse_segment(data)
    parsed = {}
    if args.schemas:
        from fastavro import parse_schema

        with open(args.schemas) as f:
            parsed = {int(i): parse_schema(json.loads(e["schema"])) for i, e in json.load(f).items()}

    out_dir = run_dir() / "archive"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / (os.path.basename(args.key) + ".jsonl")
    count, first, last, decoded = 0, None, None, 0
    with out_path.open("w") as out:
        for ts, offset, key, value, headers in records:
            count += 1
            first = offset if first is None else first
            last = offset
            if count > args.limit:
                continue
            h = dict(headers)
            row = {"offset": offset, "timestamp": ts, "key": key.decode("utf-8", "replace") if key is not None else None,
                   "x-original-offset": le_i64(h["x-original-offset"]) if "x-original-offset" in h else None,
                   "x-original-timestamp": le_i64(h["x-original-timestamp"]) if "x-original-timestamp" in h else None,
                   "user_headers": {k: base64.b64encode(v).decode() if v is not None else None
                                    for k, v in headers if k not in OFFSET_HEADERS}}
            if value is not None and len(value) > 5 and value[0] == 0 and parsed:
                from fastavro import schemaless_reader

                schema_id = struct.unpack_from(">I", value, 1)[0]
                rec = schemaless_reader(io.BytesIO(value[5:]), parsed[schema_id])
                rec["rawPayload"] = f"<{len(rec['rawPayload'])} bytes>"
                row.update(schema_id=schema_id, value=rec)
                decoded += 1
            out.write(json.dumps(row, default=str) + "\n")

    summary = {"key": args.key, "object_bytes": len(data), **header, "records_read": count,
               "first_offset": first, "last_offset": last, "decoded_rows": decoded, "jsonl": str(out_path)}
    write_json(out_dir / (os.path.basename(args.key) + ".summary.json"), summary)
    log(json.dumps(summary))
    ok = gate("archive.segment_readable_without_kafka_backup",
              count == header["record_count"] and first == header["start_offset"] and last == header["end_offset"]
              and (decoded > 0 or not parsed),
              f"CRC ok, {count} records {first}..{last}, {decoded} Avro rows decoded")
    return 0 if ok else 1
