#!/usr/bin/env bash
# Phase 75: the archive is a documented binary format, readable without kafka-backup.
PHASE=75-readable-archive
source "$(dirname "$0")/lib.sh"
start_phase

step "Decode one sealed segment from the audit month with the independent reader"
verifier decode-segment --bucket "$S3_SEALED_BUCKET" --backup-id "$TOPIC" --partition 0 \
  --at-ts "$(json_get "$RUN_DIR/restore/audit-window.json" 'd["audit_start"]')" \
  --schemas "/evidence/$RUN_ID/schemas/$TOPIC-schemas-export.json" --limit 20
head -n 2 "$RUN_DIR"/archive/*.jsonl | cut -c1-400
