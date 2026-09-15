"""Kafka's Java murmur2 partitioner, so seeded keys land where a Java producer would put them."""

_M = 0x5BD1E995
_SEED = 0x9747B28C
_MASK = 0xFFFFFFFF


def murmur2(data: bytes) -> int:
    """Unsigned 32 bit murmur2, bit for bit equal to org.apache.kafka.common.utils.Utils.murmur2."""
    length = len(data)
    h = (_SEED ^ length) & _MASK
    for i in range(length // 4):
        j = i * 4
        k = data[j] | (data[j + 1] << 8) | (data[j + 2] << 16) | (data[j + 3] << 24)
        k = (k * _M) & _MASK
        k ^= k >> 24
        k = (k * _M) & _MASK
        h = (h * _M) & _MASK
        h ^= k
    tail = length & ~3
    extra = length % 4
    if extra == 3:
        h ^= data[tail + 2] << 16
    if extra >= 2:
        h ^= data[tail + 1] << 8
    if extra >= 1:
        h ^= data[tail]
        h = (h * _M) & _MASK
    h ^= h >> 13
    h = (h * _M) & _MASK
    h ^= h >> 15
    return h


def java_partition(key: bytes, num_partitions: int) -> int:
    return (murmur2(key) & 0x7FFFFFFF) % num_partitions


def _signed(v: int) -> int:
    return v - (1 << 32) if v & 0x80000000 else v


# Vectors from Kafka's UtilsTest. A mismatch is reported, not fatal: partition placement
# realism is affected, the backup and restore verification is not.
KNOWN_VECTORS = {b"21": -973932308, b"foobar": -790332482, b"abc": 479470107}


def self_test() -> dict:
    return {k.decode(): {"expected": v, "actual": _signed(murmur2(k)), "ok": _signed(murmur2(k)) == v}
            for k, v in KNOWN_VECTORS.items()}
