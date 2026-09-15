#!/usr/bin/env bash
# Phase 60: an auditor asks for one month from nine months ago, which the live topic no
# longer holds. Restore it from the sealed backup into a new topic on a different cluster.
PHASE=60-audit-restore
source "$(dirname "$0")/lib.sh"
start_phase
start_stats_sampler
mkdir -p "$RUN_DIR/restore"

step "Import the exported schemas into the restore registry, preserving IDs"
verifier schema-import --label audit

step "Audit window: $AUDIT_DAYS UTC days starting about $AUDIT_MONTHS_AGO months before the newest record"
window="$RUN_DIR/restore/audit-window.json"
python3 - "$RUN_DIR/seed/$TOPIC.json" "$window" "$AUDIT_MONTHS_AGO" "$AUDIT_DAYS" "$LATE_MAX_HOURS" <<'PY'
import json, sys
from datetime import datetime, timezone
DAY, HOUR = 86_400_000, 3_600_000
seed = json.load(open(sys.argv[1]))
months, days, late_h = int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
start = (seed["seed_end_ts"] - months * 30 * DAY) // DAY * DAY
end_excl = start + days * DAY
pad = late_h * HOUR
iso = lambda ms: datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
w = {"audit_start": start, "audit_end_exclusive": end_excl, "exact_from": iso(start), "exact_to": iso(end_excl - DAY),
     "pad_ms": pad, "window_start": start - pad, "window_end": end_excl - 1 + pad}
json.dump(w, open(sys.argv[2], "w"), indent=2)
print(f"audit {w['exact_from']}..{w['exact_to']} UTC, restore window {w['window_start']}..{w['window_end']} (padded {late_h}h each side)")
PY
WINDOW_START="$(json_get "$window" 'd["window_start"]')"
WINDOW_END="$(json_get "$window" 'd["window_end"]')"
export WINDOW_START WINDOW_END BACKUP_ID="$TOPIC" BACKUP_BUCKET="$S3_SEALED_BUCKET" S3_PREFIX=backups TARGET_TOPIC="$TOPIC-audit"
render "$REPO_ROOT/config/kafka-backup/audit-restore.yaml.tmpl" "$RUN_DIR/configs/restore-audit.yaml"

step "Target preflight"
if topic_exists target "$TARGET_TOPIC"; then gate_fail restore.audit.target_topic_is_new "$TARGET_TOPIC already exists"; fi
gate_pass restore.audit.target_topic_is_new "$TARGET_TOPIC does not exist on the target"
max_message="$(kcli target kafka-configs --bootstrap-server "$(bootstrap target)" --entity-type brokers --entity-name 1 --describe --all \
  | grep -o ' message.max.bytes=[0-9]*' | sed -n '1s/.*=//p')"  # every stage reads to the end: no broken pipe under pipefail
max_record="$(json_get "$RUN_DIR/backup/verify-$TOPIC.json" 'd["max_record_bytes"]')"
batch_bytes=$(( 250 * (max_record + 80) + 61 ))
gate restore.audit.batch_fits_message_max_bytes \
  "250 records x (largest record $max_record + 80 bytes overhead) = $batch_bytes <= message.max.bytes $max_message" [ "$batch_bytes" -le "$max_message" ]

step "Restore the audit window from the sealed backup"
t0="$(now_s)"
kbackup restore --config "/evidence/$RUN_ID/configs/restore-audit.yaml" 2>&1 | tee "$RUN_DIR/restore/restore-audit.log"
secs="$(elapsed_since "$t0")"
printf '{"seconds": %s, "target_topic": "%s"}\n' "$secs" "$TARGET_TOPIC" > "$RUN_DIR/restore/run-audit.json"

kcli target kafka-configs --bootstrap-server "$(bootstrap target)" --entity-type topics --entity-name "$TARGET_TOPIC" --describe \
  | tee "$RUN_DIR/restore/audit-topic-config.txt"
gate restore.audit.target_retention_infinite "retention.ms=-1 on $TARGET_TOPIC" grep -q 'retention.ms=-1' "$RUN_DIR/restore/audit-topic-config.txt"
