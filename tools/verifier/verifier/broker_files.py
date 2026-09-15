"""Read-only view of a broker log directory: segment base offsets, sizes and max timestamps.

Only possible locally. On Confluent Cloud the same prediction has to come from the
backup manifest and the Metrics API instead.
"""
import struct
from pathlib import Path

TIMEINDEX_ENTRY = struct.Struct(">qi")  # timestamp, offset relative to the segment base


def last_timeindex_timestamp(path: Path):
    """The last non-empty time index entry, which for a rolled segment is its largest timestamp.

    Kafka appends maxTimestampSoFar when a segment becomes inactive. The active segment's
    file is preallocated with zeros, so scan back from the end for the last real entry.
    """
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    for i in range(len(data) // TIMEINDEX_ENTRY.size - 1, -1, -1):
        ts, rel = TIMEINDEX_ENTRY.unpack_from(data, i * TIMEINDEX_ENTRY.size)
        if ts != 0 or rel != 0:
            return ts
    return None


def segment_inventory(data_dir: str, topic: str, partitions) -> dict:
    inventory = {}
    for p in partitions:
        d = Path(data_dir) / f"{topic}-{p}"
        if not d.is_dir():
            raise FileNotFoundError(f"partition directory not found: {d}")
        logs = sorted(d.glob("*.log"), key=lambda f: int(f.stem))
        segments = []
        for i, f in enumerate(logs):
            segments.append({
                "base_offset": int(f.stem),
                "log_bytes": f.stat().st_size,
                "timeindex_max_ts": last_timeindex_timestamp(f.with_suffix(".timeindex")),
                "active": i == len(logs) - 1,
            })
        inventory[p] = segments
    return inventory
