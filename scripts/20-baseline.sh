#!/usr/bin/env bash
# Phase 20: read the source once and record what "the same data" means before anything changes.
PHASE=20-baseline
source "$(dirname "$0")/lib.sh"
start_phase
start_stats_sampler

step "Baseline digest of $TOPIC: per partition, per UTC day, per broker segment"
verifier baseline
baseline="$RUN_DIR/baseline/$TOPIC.json"

step "Broker reported size"
log_dir_sizes source "$TOPIC" | sort -n | tee "$RUN_DIR/baseline/log-dir-sizes.txt"
reported="$(awk '{s += $2} END {print s}' "$RUN_DIR/baseline/log-dir-sizes.txt")"
files="$(json_get "$baseline" 'sum(s["log_bytes"] for p in d["partitions"].values() for s in p["segments"])')"
gate baseline.broker_size_equals_segment_files "kafka-log-dirs $reported bytes, segment files $files bytes" [ "$reported" = "$files" ]
info "$(json_get "$baseline" '"{} segments, schema ids {}, {} day buckets on partition 0".format(sum(len(p["segments"]) for p in d["partitions"].values()), d["schema_ids"], len(d["partitions"]["0"]["buckets"]))')"
