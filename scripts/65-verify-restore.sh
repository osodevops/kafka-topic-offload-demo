#!/usr/bin/env bash
# Phase 65: prove the restored audit month is the source data, record for record.
PHASE=65-verify-restore
source "$(dirname "$0")/lib.sh"
start_phase
start_stats_sampler
window="$RUN_DIR/restore/audit-window.json"

step "Restored $TOPIC-audit against the baseline"
verifier verify-restore --target-topic "$TOPIC-audit" --label audit \
  --window-start "$(json_get "$window" 'd["window_start"]')" --window-end "$(json_get "$window" 'd["window_end"]')" \
  --exact-from "$(json_get "$window" 'd["exact_from"]')" --exact-to "$(json_get "$window" 'd["exact_to"]')"

step "Every schema ID in the restored data resolves to the same schema on the restore registry"
ids="$(json_get "$RUN_DIR/restore/verify-audit.json" '",".join(map(str, d["schema_ids"]))')"
verifier schema-check --ids "$ids" --label audit
