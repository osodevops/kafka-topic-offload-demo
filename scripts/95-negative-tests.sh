#!/usr/bin/env bash
# Phase 95: each scenario must fail the named gate. A gate that cannot fail proves nothing.
# Runs after phases 00 to 65 of the current run; scenarios 1 and 2 use their own small topics.
PHASE=95-negative-tests
source "$(dirname "$0")/lib.sh"
start_phase
NEG_DIR="$RUN_DIR/negative"
mkdir -p "$NEG_DIR/configs"

# expect_gate_fail SCENARIO GATE command...: the command must exit non-zero on exactly that gate.
expect_gate_fail() {
  local scenario="$1" gate_name="$2"; shift 2
  local log="$NEG_DIR/$scenario--$gate_name.log" rc
  set +e
  ( "$@" ) > "$log" 2>&1
  rc=$?
  set -e
  sed 's/^/    | /' "$log" | grep -E '\| (GATE|FINDING|Result:)' || true
  [[ $rc -ne 0 ]] || gate_fail "negative.$scenario" "expected GATE $gate_name to fail, but the step passed"
  grep -q "^GATE $gate_name: FAIL" "$log" || gate_fail "negative.$scenario" "step failed (exit $rc) but not on GATE $gate_name; see negative/$(basename "$log")"
  gate_pass "negative.$scenario" "GATE $gate_name failed as designed"
}

reset_negative_topic() {  # drop a negative test topic and its backups from any earlier attempt
  if topic_exists source "$1"; then
    kcli source kafka-topics --bootstrap-server "$(bootstrap source)" --delete --topic "$1"
    sleep 5
  fi
  mc_cmd "mc rm --recursive --force local/$S3_BUCKET/negative/$1/" >/dev/null 2>&1 || true
}

small_backup() {  # small_backup TOPIC [BACKUP_EXTRA]
  export BACKUP_ID="$1" TOPIC_SAVED="$TOPIC" SEGMENT_MAX_BYTES=4194304 BACKUP_BUCKET="$S3_BUCKET" S3_PREFIX=negative BACKUP_EXTRA="${2:-}"
  TOPIC="$1" render "$REPO_ROOT/config/kafka-backup/backup.yaml.tmpl" "$NEG_DIR/configs/backup-$1.yaml"
  if ! kbackup backup --config "/evidence/$RUN_ID/negative/configs/backup-$1.yaml" > "$NEG_DIR/backup-$1.log" 2>&1; then
    tail -n 15 "$NEG_DIR/backup-$1.log"
    fail "backup of $1 failed; see negative/backup-$1.log"
  fi
}

# ---------------------------------------------------------------------------------------------
step "1. Gap: retention removed data before the first backup"
T1=neg-gap
reset_negative_topic "$T1"
RUN_SUBDIR=/negative verifier seed --topic "$T1" --records 60000 --span-days 90 --segment-bytes 1048576 --late-rate 0
RUN_SUBDIR=/negative verifier baseline --topic "$T1"
kcli source kafka-configs --bootstrap-server "$(bootstrap source)" --entity-type topics --entity-name "$T1" --alter --add-config retention.ms=2592000000
wait_for "log start offsets on $T1 to move" 300 bash -c \
  "docker compose -f '$COMPOSE_FILE' --env-file '$ENV_FILE' exec -T source-kafka kafka-get-offsets --bootstrap-server source-kafka:29092 --topic $T1 --time -2 | awk -F: '\$3 == 0 {z=1} END {exit z}'"
# kafka-backup parses externally tagged enums with serde_yaml, which needs the YAML tag form.
extra="$(printf '  start_offset: !specific\n    %s:\n' "$T1"; for p in $(seq 0 $(( PARTITIONS - 1 ))); do printf '      %s: 0\n' "$p"; done)"
small_backup "$T1" "$extra"
export BACKUP_ID="$T1"
kbackup validate --config "/evidence/$RUN_ID/negative/configs/backup-$T1.yaml" --deep > "$NEG_DIR/validate-$T1.txt" 2>&1 && rc=0 || rc=$?
gate negative.gap.kafka_backup_validate_exits_zero_with_gaps "exit $rc: $(grep '^Result:' "$NEG_DIR/validate-$T1.txt")" \
  bash -c "[ $rc -eq 0 ] && grep -q '^Result: VALID (with' '$NEG_DIR/validate-$T1.txt'"
expect_gate_fail gap backup.validate_result_exactly_valid check_validate_output "$NEG_DIR/validate-$T1.txt" backup
RUN_SUBDIR=/negative expect_gate_fail gap backup.manifest_covers_log_start_to_high_watermark \
  verifier verify-backup --topic "$T1" --backup-id "$T1" --prefix negative --anchor none --group ''

# ---------------------------------------------------------------------------------------------
step "2. Corruption: one byte flipped in one stored segment"
T2=neg-corrupt
reset_negative_topic "$T2"
RUN_SUBDIR=/negative verifier seed --topic "$T2" --records 30000 --span-days 30 --segment-bytes 1048576 --late-rate 0
RUN_SUBDIR=/negative verifier baseline --topic "$T2"
small_backup "$T2"
RUN_SUBDIR=/negative verifier verify-backup --topic "$T2" --backup-id "$T2" --prefix negative --anchor none --sha-only --group '' --label .before-corruption
RUN_SUBDIR=/negative verifier corrupt-object --topic "$T2" --backup-id "$T2" --prefix negative --partition 0
export BACKUP_ID="$T2"
kbackup validate --config "/evidence/$RUN_ID/negative/configs/backup-$T2.yaml" --deep > "$NEG_DIR/validate-$T2.txt" 2>&1 || true
expect_gate_fail corruption backup.validate_result_exactly_valid check_validate_output "$NEG_DIR/validate-$T2.txt" backup
RUN_SUBDIR=/negative expect_gate_fail corruption backup.object_sha256_matches_manifest \
  verifier verify-backup --topic "$T2" --backup-id "$T2" --prefix negative --anchor none --sha-only --group ''

# ---------------------------------------------------------------------------------------------
step "3. Schema ID mismatch: restored data read through a registry that assigned its own IDs"
dc --profile negative up -d --wait negative-schema-registry
verifier schema-naive-register --target-sr http://negative-schema-registry:8081
window="$RUN_DIR/restore/audit-window.json"
ids="$(json_get "$RUN_DIR/restore/verify-audit.json" '",".join(map(str, d["schema_ids"]))')"
expect_gate_fail schema-id schemas.ids_resolve_identically.negative \
  verifier schema-check --ids "$ids" --target-sr http://negative-schema-registry:8081 --label negative
expect_gate_fail schema-id restore.negative-schema.avro_decodes_with_registry \
  verifier verify-restore --target-topic "$TOPIC-audit" --label negative-schema --registry http://negative-schema-registry:8081 \
    --window-start "$(json_get "$window" 'd["window_start"]')" --window-end "$(json_get "$window" 'd["window_end"]')" \
    --exact-from "$(json_get "$window" 'd["exact_from"]')" --exact-to "$(json_get "$window" 'd["exact_to"]')"

# ---------------------------------------------------------------------------------------------
step "4. Unacknowledged consumer impact blocks the prune gate"
expect_gate_fail unacked-consumer prune_gate.consumer_impact_acknowledged \
  verifier consumer-inventory --prediction "/evidence/$RUN_ID/prune/prediction.json" --ack "" --label=-negative

# ---------------------------------------------------------------------------------------------
step "5. Unpadded time window misses late records (kafka-backup 0.22.0 segment timestamp bounds)"
read -r day_start expected_missing <<<"$(python3 - "$RUN_DIR/backup/time-window-hazards-$TOPIC.json" <<'PY'
import json, sys
from collections import Counter
DAY = 86_400_000
c = Counter()
for p, off, ts, s_start, s_end in json.load(open(sys.argv[1]))["hazards"]:
    day = ts // DAY
    if s_start > (day + 1) * DAY - 1:
        c[day] += 1
day, n = c.most_common(1)[0] if c else (0, 0)
print(day * DAY, n)
PY
)"
[[ "$expected_missing" -gt 0 ]] || gate_fail negative.unpadded-window "no late record sits in a segment that starts on a later day; nothing to demonstrate"
info "day starting $day_start: $expected_missing late records sit in segments whose first record is on a later day"
export BACKUP_ID="$TOPIC" BACKUP_BUCKET="$S3_SEALED_BUCKET" S3_PREFIX=backups TARGET_TOPIC="$TOPIC-neg-window"
export WINDOW_START="$day_start" WINDOW_END=$(( day_start + 86400000 - 1 ))
render "$REPO_ROOT/config/kafka-backup/audit-restore.yaml.tmpl" "$NEG_DIR/configs/restore-unpadded.yaml"
topic_exists target "$TARGET_TOPIC" && kcli target kafka-topics --bootstrap-server "$(bootstrap target)" --delete --topic "$TARGET_TOPIC" && sleep 5
kbackup restore --config "/evidence/$RUN_ID/negative/configs/restore-unpadded.yaml" > "$NEG_DIR/restore-unpadded.log" 2>&1
day_iso="$(python3 -c 'import datetime,sys; print(datetime.datetime.fromtimestamp(int(sys.argv[1])/1000, datetime.timezone.utc).strftime("%Y-%m-%d"))' "$day_start")"
expect_gate_fail unpadded-window "restore.negative-window.buckets_match_baseline" \
  verifier verify-restore --target-topic "$TARGET_TOPIC" --label negative-window \
    --window-start "$WINDOW_START" --window-end "$WINDOW_END" --exact-from "$day_iso" --exact-to "$day_iso"
gate negative.unpadded-window.loss_explained_by_segment_bounds "every missing record is one the manifest bounds predict" \
  grep -q "^GATE restore.negative-window.every_difference_explained_by_segment_bounds: PASS" \
  "$NEG_DIR/unpadded-window--restore.negative-window.buckets_match_baseline.log"
