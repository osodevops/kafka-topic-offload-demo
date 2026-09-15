"""Consumer impact: who sits below the cut, and what happens when they resume."""
import argparse
import os
import time

from .common import gate, log, read_json, run_dir, watermarks, write_json


def group_offsets(bootstrap: str, topic: str) -> dict:
    """Committed offsets on `topic` for every consumer group the principal can list."""
    from confluent_kafka import ConsumerGroupTopicPartitions
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    listing = admin.list_consumer_groups().result()
    out = {}
    for g in sorted(listing.valid, key=lambda g: g.group_id):
        fut = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(g.group_id)])[g.group_id]
        tps = [tp for tp in fut.result().topic_partitions if tp.topic == topic and tp.offset >= 0]
        if tps:
            out[g.group_id] = {tp.partition: tp.offset for tp in tps}
    return out


def main(cmd: str, argv) -> int:
    ap = argparse.ArgumentParser(prog=f"verifier {cmd}")
    ap.add_argument("--topic", default=os.environ.get("TOPIC", "device-telemetry"))
    ap.add_argument("--partitions", type=int, default=int(os.environ.get("PARTITIONS", "6")))
    ap.add_argument("--group", default="telemetry-analytics")
    ap.add_argument("--expect", choices=["out-of-range", "ok"], default="out-of-range")
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--prediction", help="prune prediction JSON with predicted_log_start per partition")
    ap.add_argument("--ack", default=os.environ.get("ACK_CONSUMER_IMPACT", ""))
    ap.add_argument("--label", default="")
    args = ap.parse_args(argv)
    bootstrap = os.environ["SOURCE_BOOTSTRAP"]
    rd = run_dir()

    if cmd == "consumer-inventory":
        offsets = group_offsets(bootstrap, args.topic)
        prediction = read_json(args.prediction)
        cut = {int(p): v["predicted_log_start"] for p, v in prediction["partitions"].items()}
        behind = {g: {p: o for p, o in parts.items() if o < cut[p]} for g, parts in offsets.items()}
        behind = {g: v for g, v in behind.items() if v}
        acked = {a.strip() for a in args.ack.split(",") if a.strip()}
        unacked = sorted(set(behind) - acked)
        write_json(rd / "prune" / f"consumer-inventory{args.label}.json",
                   {"committed": offsets, "cut": cut, "behind_cut": behind, "acknowledged": sorted(acked), "unacknowledged": unacked})
        log(f"groups with offsets on {args.topic}: {sorted(offsets)}; behind the cut: {sorted(behind)}")
        ok = gate("prune_gate.consumer_impact_acknowledged", not unacked,
                  f"behind cut {sorted(behind)}, acknowledged {sorted(acked)}" + (f", NOT acknowledged {unacked}" if unacked else ""))
        return 0 if ok else 1

    # consumer-sim: resume the group from its committed offsets with no silent reset.
    from confluent_kafka import Consumer, KafkaError, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": bootstrap, "group.id": args.group, "enable.auto.commit": False,
        "auto.offset.reset": "error", "enable.partition.eof": False,
    })
    parts = [TopicPartition(args.topic, p) for p in range(args.partitions)]
    committed = {tp.partition: tp.offset for tp in consumer.committed(parts, timeout=30)}
    marks = watermarks(bootstrap, args.topic, range(args.partitions))
    consumer.assign([TopicPartition(args.topic, p, o) for p, o in committed.items()])
    events, consumed = {}, {p: 0 for p in committed}
    deadline = time.monotonic() + args.seconds
    while time.monotonic() < deadline and len(events) + sum(1 for c in consumed.values() if c) < len(committed):
        m = consumer.poll(1.0)
        if m is None:
            continue
        if m.error():
            if m.error().code() == KafkaError._AUTO_OFFSET_RESET:
                events.setdefault(m.partition(), {"code": m.error().name(), "message": m.error().str()})
            else:
                log(f"consumer error: {m.error()}")
        else:
            consumed[m.partition()] += 1
    consumer.close()

    below = {p for p, o in committed.items() if o < marks[p][0]}
    result = {"group": args.group, "committed": committed, "watermarks": {p: {"low": lo, "high": hi} for p, (lo, hi) in marks.items()},
              "below_log_start": sorted(below), "offset_out_of_range": events, "consumed": consumed}
    write_json(rd / "consumers" / f"{args.group}{args.label}.json", result)
    if args.expect == "out-of-range":
        ok = gate("consumer.offset_out_of_range_surfaced", bool(below) and set(events) == below,
                  f"{len(below)} partitions below log start, {len(events)} raised OffsetOutOfRange with auto.offset.reset=error")
    else:
        ok = gate("consumer.resumes_cleanly", not events and not below, f"consumed {sum(consumed.values())} records")
    return 0 if ok else 1
