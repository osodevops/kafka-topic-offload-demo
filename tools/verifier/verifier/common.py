"""Shared helpers: evidence files, day buckets, the record digest and partition reads."""
import hashlib
import json
import os
import struct
import sys
import time
import uuid
from pathlib import Path

DAY_MS = 86_400_000
HOUR_MS = 3_600_000

# Headers kafka-backup adds on backup (and optionally on restore). Source records never
# carry them, so they are excluded from the digest on both sides.
OFFSET_HEADERS = frozenset({"x-original-offset", "x-original-timestamp", "x-source-cluster", "x-source-partition"})


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def gate(name: str, ok: bool, detail: str = "") -> bool:
    print(f"GATE {name}: {'PASS' if ok else 'FAIL'}{(' - ' + detail) if detail else ''}", flush=True)
    return ok


def run_dir() -> Path:
    d = Path(os.environ.get("RUN_DIR", "/evidence/current"))
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_json(path, data) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    tmp.replace(path)


def read_json(path):
    return json.loads(Path(path).read_text())


def day_bucket(ts_ms: int) -> int:
    """UTC day number: midnight inclusive to the next midnight exclusive."""
    return ts_ms // DAY_MS


def _lp(b) -> bytes:
    if b is None:
        return struct.pack(">i", -1)
    return struct.pack(">i", len(b)) + b


def digest_input(partition: int, offset: int, ts: int, key, value, headers) -> bytes:
    """Canonical bytes for one record. `offset` is always the SOURCE offset."""
    parts = [struct.pack(">iqq", partition, offset, ts), _lp(key), _lp(value)]
    user = [(k, v) for k, v in (headers or ()) if k not in OFFSET_HEADERS]
    parts.append(struct.pack(">i", len(user)))
    for k, v in user:
        parts.append(_lp(k.encode("utf-8") if isinstance(k, str) else k))
        parts.append(_lp(v))
    return b"".join(parts)


class BucketStats:
    __slots__ = ("count", "min_ts", "max_ts", "first_offset", "last_offset", "_h")

    def __init__(self):
        self.count = 0
        self.min_ts = None
        self.max_ts = None
        self.first_offset = None
        self.last_offset = None
        self._h = hashlib.sha256()

    def add(self, offset: int, ts: int, data: bytes) -> None:
        self.count += 1
        if self.min_ts is None or ts < self.min_ts:
            self.min_ts = ts
        if self.max_ts is None or ts > self.max_ts:
            self.max_ts = ts
        if self.first_offset is None:
            self.first_offset = offset
        self.last_offset = offset
        self._h.update(data)

    def to_dict(self) -> dict:
        return {
            "count": self.count,
            "min_ts": self.min_ts,
            "max_ts": self.max_ts,
            "first_offset": self.first_offset,
            "last_offset": self.last_offset,
            "sha256": self._h.hexdigest(),
        }


def consumer_conf(bootstrap: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": f"verifier-{uuid.uuid4()}",
        "enable.auto.commit": False,
        "enable.partition.eof": False,
        "fetch.max.bytes": 104857600,
        "max.partition.fetch.bytes": 20971520,
        "queued.max.messages.kbytes": 2097151,
        "fetch.wait.max.ms": 100,
    }


def watermarks(bootstrap: str, topic: str, partitions) -> dict:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(consumer_conf(bootstrap))
    try:
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=30) for p in partitions}
    finally:
        c.close()


def read_partition(bootstrap: str, topic: str, partition: int, start: int, end_exclusive: int, on_msg,
                   batch: int = 20000, idle_limit_s: int = 120) -> int:
    """Read [start, end_exclusive) from one partition in offset order and call on_msg per record."""
    from confluent_kafka import Consumer, TopicPartition

    if end_exclusive <= start:
        return 0
    c = Consumer(consumer_conf(bootstrap))
    c.assign([TopicPartition(topic, partition, start)])
    seen = 0
    next_needed = start
    idle_since = time.monotonic()
    try:
        while next_needed < end_exclusive:
            msgs = c.consume(batch, timeout=2.0)
            if not msgs:
                if time.monotonic() - idle_since > idle_limit_s:
                    raise RuntimeError(f"stalled reading {topic}[{partition}] at offset {next_needed} (end {end_exclusive})")
                continue
            idle_since = time.monotonic()
            for m in msgs:
                if m.error():
                    raise RuntimeError(f"{topic}[{partition}]: {m.error()}")
                off = m.offset()
                if off >= end_exclusive:
                    next_needed = end_exclusive
                    break
                on_msg(m)
                seen += 1
                next_needed = off + 1
    finally:
        c.close()
    return seen


def le_i64(b: bytes) -> int:
    return struct.unpack("<q", b)[0]


def header_map(headers) -> dict:
    return {k: v for k, v in (headers or ())}
