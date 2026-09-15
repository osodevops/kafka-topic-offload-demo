#!/usr/bin/env bash
# Phase 40: the gate a change approver signs. Nothing is changed here.
PHASE=40-prune-gate
source "$(dirname "$0")/lib.sh"
start_phase
mkdir -p "$RUN_DIR/prune"

step "Backup, seal and topic state; exact segment prediction for retention.ms=$RETENTION_AFTER_PRUNE_MS"
verifier prune-predict --backup-id "$TOPIC"
msg=$(json_get "$RUN_DIR/prune/prediction.json" '"delete {:,} records and {:.3f} GB; keep {:.3f} GB".format(d["deleted_records"], d["deleted_bytes"]/1e9, d["kept_bytes"]/1e9)')
info "$msg"

step "Consumer groups that would fall below the new log start"
verifier consumer-inventory --prediction "/evidence/$RUN_ID/prune/prediction.json"

log_dir_sizes source "$TOPIC" | sort -n > "$RUN_DIR/prune/log-dir-sizes-before.txt"
