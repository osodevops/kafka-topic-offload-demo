#!/usr/bin/env bash
# Phase 10: create the topic, seed a year of telemetry, and put a consumer group 60 days behind.
PHASE=10-seed
source "$(dirname "$0")/lib.sh"
start_phase
start_stats_sampler

step "Seeding $TOPIC at scale $SCALE (seed $SEED)"
verifier seed --scale "$SCALE"
seed_json="$RUN_DIR/seed/$TOPIC.json"
# Assign first: bash 3.2 (macOS) brace-expands "{:,}" inside a double-quoted $(...).
msg=$(json_get "$seed_json" '"{:,} records, {:.2f} GB of values, {} MB/s, segment.bytes {:,}".format(d["records"], d["value_bytes"]/1e9, d["throughput_mb_s"], d["segment_bytes"])')
info "$msg"

step "Consumer group telemetry-analytics, committed 60 days behind the newest record"
at="$(json_get "$seed_json" '__import__("datetime").datetime.fromtimestamp(d["seed_end_ts"]/1000 - 60*86400, __import__("datetime").timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000")')"
kcli source kafka-consumer-groups --bootstrap-server "$(bootstrap source)" --group telemetry-analytics --topic "$TOPIC" \
  --reset-offsets --to-datetime "$at" --execute | tee "$RUN_DIR/seed/telemetry-analytics-reset.txt"
kcli source kafka-consumer-groups --bootstrap-server "$(bootstrap source)" --describe --group telemetry-analytics 2>/dev/null \
  | tee "$RUN_DIR/seed/telemetry-analytics-describe.txt"
behind="$(awk -v t="$TOPIC" '$2 == t && $4 > 0 && $4 < $5' "$RUN_DIR/seed/telemetry-analytics-describe.txt" | wc -l | tr -d ' ')"
gate seed.consumer_group_committed_behind "$behind of $PARTITIONS partitions committed at $at UTC" [ "$behind" -eq "$PARTITIONS" ]
