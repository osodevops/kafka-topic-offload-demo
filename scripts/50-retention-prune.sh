#!/usr/bin/env bash
# Phase 50: lower retention on the live topic and prove Kafka deleted exactly what was predicted.
PHASE=50-retention-prune
source "$(dirname "$0")/lib.sh"
start_phase

step "Re-run the gate immediately before the change"
verifier prune-predict --backup-id "$TOPIC" --out prediction-apply.json --compare-to prediction.json
verifier consumer-inventory --prediction "/evidence/$RUN_ID/prune/prediction-apply.json" --label=-apply

step "Set retention.ms=$RETENTION_AFTER_PRUNE_MS on $TOPIC"
kcli source kafka-configs --bootstrap-server "$(bootstrap source)" --entity-type topics --entity-name "$TOPIC" \
  --alter --add-config "retention.ms=$RETENTION_AFTER_PRUNE_MS"
date -u +%FT%TZ > "$RUN_DIR/prune/applied-at.txt"

step "Wait for the retention check and compare with the prediction"
verifier prune-check --backup-id "$TOPIC" --out prediction-apply.json

log_dir_sizes source "$TOPIC" | sort -n > "$RUN_DIR/prune/log-dir-sizes-after.txt"
before="$(awk '{s += $2} END {print s}' "$RUN_DIR/prune/log-dir-sizes-before.txt")"
after="$(awk '{s += $2} END {print s}' "$RUN_DIR/prune/log-dir-sizes-after.txt")"
predicted="$(json_get "$RUN_DIR/prune/prediction-apply.json" 'd["deleted_bytes"]')"
gate prune.broker_reported_reclaim_matches_prediction \
  "kafka-log-dirs $before -> $after bytes, reclaimed $(( before - after )), predicted $predicted" [ $(( before - after )) -eq "$predicted" ]

step "telemetry-analytics resumes with auto.offset.reset=error"
verifier consumer-sim --group telemetry-analytics --expect out-of-range
