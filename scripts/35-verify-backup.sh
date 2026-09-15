#!/usr/bin/env bash
# Phase 35: prove the backup before anything depends on it. kafka-backup's own validate is
# necessary but not sufficient: it exits 0 with recorded gaps and does not check SHA-256.
PHASE=35-verify-backup
source "$(dirname "$0")/lib.sh"
start_phase
start_stats_sampler
cfg="/evidence/$RUN_ID/configs/backup-primary.yaml"
out="$RUN_DIR/backup/validate-deep.txt"

step "kafka-backup validate --deep"
kbackup validate --config "$cfg" --deep > "$out" 2>&1 || true
grep -E '^(Segments|Records|Data Gaps|Pruned|Missing Topics|Result|Issues)' "$out" || true
check_validate_output "$out" backup

kbackup describe --config "$cfg" > "$RUN_DIR/backup/describe.txt" 2>&1 || true

step "Independent verification: manifest, object SHA-256, decoded content against the baseline"
verifier verify-backup --backup-id "$TOPIC" --anchor write \
  --seconds "$(json_get "$RUN_DIR/backup/summary-$TOPIC.json" 'd["seconds"]')"
