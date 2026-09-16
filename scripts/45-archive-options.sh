#!/usr/bin/env bash
# Phase 45: the choices you have about what to archive and what to bring back.
#   A. Archive only what arrived after a chosen instant (offsets for times -> start_offset).
#   B. Restore a time range, as at a chosen instant, without anything newer.
#   C. Keep a rolling range in object storage, with the archive's own retention.
# Runs after the backup is sealed and before retention is lowered, so the source still
# holds the full year. The sealed copy is never modified.
PHASE=45-archive-options
source "$(dirname "$0")/lib.sh"
start_phase
start_stats_sampler
mkdir -p "$RUN_DIR/archive-options" "$RUN_DIR/restore" "$RUN_DIR/archive"
seed_json="$RUN_DIR/seed/$TOPIC.json"
since_days="${ARCHIVE_SINCE_DAYS:-90}"
range_days="${ASAT_RANGE_DAYS:-30}"
pad_ms=$(( LATE_MAX_HOURS * 3600000 ))
since_ms="$(json_get "$seed_json" "d['seed_end_ts'] - ${since_days} * 86400000")"
since_iso="$(json_get "$seed_json" "__import__('datetime').datetime.fromtimestamp((d['seed_end_ts'] - ${since_days} * 86400000)/1000, __import__('datetime').timezone.utc).strftime('%Y-%m-%d %H:%M')")"

# ---------------------------------------------------------------------------------------------
step "A. Archive only what arrived after $since_iso UTC ($since_days days before the newest record)"
# Kafka turns an instant into the first offset at or after it, per partition. kafka-backup takes
# those offsets as its start, so the archive holds that instant onwards rather than all history.
kcli source kafka-get-offsets --bootstrap-server "$(bootstrap source)" --topic "$TOPIC" --time "$since_ms" \
  | tee "$RUN_DIR/archive-options/offsets-for-time.txt"
python3 - "$RUN_DIR/archive-options/offsets-for-time.txt" "$RUN_DIR/archive-options/offsets-since.json" <<'PY'
import json, sys
rows = {}
for line in open(sys.argv[1]):
    parts = line.strip().split(":")
    if len(parts) == 3 and parts[2].isdigit():
        rows[int(parts[1])] = int(parts[2])
json.dump(rows, open(sys.argv[2], "w"), indent=2, sort_keys=True)
print(f"offsets for time: {rows}")
PY
found="$(json_get "$RUN_DIR/archive-options/offsets-since.json" 'len(d)')"
gate archive.since.offsets_for_time_resolved "$found of $PARTITIONS partitions have an offset at or after the instant" [ "$found" -eq "$PARTITIONS" ]

extra="$(python3 - "$RUN_DIR/archive-options/offsets-since.json" "$TOPIC" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1]))
print(f"  start_offset: !specific\n    {sys.argv[2]}:")
for p, o in sorted(rows.items(), key=lambda kv: int(kv[0])):
    print(f"      {p}: {o}")
PY
)"
# This phase owns the "-since" archive and the "-asat" topic, so a re-run starts from clean.
if [[ -n "$(mc_cmd "mc ls --recursive local/$S3_BUCKET/backups/$TOPIC-since/ | head -n 1")" ]]; then
  info "removing the partial archive from an earlier attempt"
  mc_cmd "mc rm --recursive --force local/$S3_BUCKET/backups/$TOPIC-since/" >/dev/null
fi
export BACKUP_ID="$TOPIC-since" BACKUP_BUCKET="$S3_BUCKET" S3_PREFIX=backups BACKUP_EXTRA="$extra"
if [[ "$SCALE" == smoke ]]; then export SEGMENT_MAX_BYTES=16777216; else export SEGMENT_MAX_BYTES=134217728; fi
render "$REPO_ROOT/config/kafka-backup/backup.yaml.tmpl" "$RUN_DIR/configs/backup-since.yaml"
kbackup backup --config "/evidence/$RUN_ID/configs/backup-since.yaml" 2>&1 | tail -n 5
verifier manifest-summary --backup-id "$BACKUP_ID"
verifier verify-backup --backup-id "$BACKUP_ID" --anchor none --group '' --label .since \
  --from-offsets "/evidence/$RUN_ID/archive-options/offsets-since.json"
verifier archive-coverage --backup-id "$BACKUP_ID" --label ""
earliest="$(json_get "$RUN_DIR/archive/coverage-$BACKUP_ID.json" 'd["earliest_timestamp"]')"
gate archive.since.starts_at_the_instant \
  "earliest archived record is at $since_iso UTC minus at most the ${LATE_MAX_HOURS}h late window" \
  [ "$earliest" -ge $(( since_ms - pad_ms )) ]
full_records="$(json_get "$RUN_DIR/backup/summary-$TOPIC.json" 'd["records"]')"
part_records="$(json_get "$RUN_DIR/backup/summary-$BACKUP_ID.json" 'd["records"]')"
info "$part_records of $full_records records archived, the last $since_days days only"

# ---------------------------------------------------------------------------------------------
step "B. Restore the $range_days days ending at that instant, as at $since_iso UTC"
# Any restore needs the schemas its records were written with. Importing by ID is idempotent, so
# phase 60 finds them already present.
verifier schema-import --label asat
export BACKUP_ID="$TOPIC" BACKUP_BUCKET="$S3_SEALED_BUCKET" S3_PREFIX=backups TARGET_TOPIC="$TOPIC-asat"
export WINDOW_START=$(( since_ms - range_days * 86400000 )) WINDOW_END=$(( since_ms - 1 ))
render "$REPO_ROOT/config/kafka-backup/audit-restore.yaml.tmpl" "$RUN_DIR/configs/restore-asat.yaml"
if topic_exists target "$TARGET_TOPIC"; then
  info "removing $TARGET_TOPIC from an earlier attempt"
  kcli target kafka-topics --bootstrap-server "$(bootstrap target)" --delete --topic "$TARGET_TOPIC"
  sleep 5
fi
t0="$(now_s)"
kbackup restore --config "/evidence/$RUN_ID/configs/restore-asat.yaml" 2>&1 | tail -n 4
printf '{"seconds": %s, "target_topic": "%s", "window_start": %s, "window_end": %s}\n' \
  "$(elapsed_since "$t0")" "$TARGET_TOPIC" "$WINDOW_START" "$WINDOW_END" > "$RUN_DIR/restore/run-asat.json"
# The window is not padded: nothing newer than the instant may come back. Days at the edges can
# lose late records to the manifest's segment bounds, so only the interior days must match exactly
# and every edge difference has to be one those bounds predict.
exact_from="$(python3 -c 'import datetime,sys; print(datetime.datetime.fromtimestamp((int(sys.argv[1])+int(sys.argv[2]))/1000, datetime.timezone.utc).strftime("%Y-%m-%d"))' "$WINDOW_START" "$pad_ms")"
exact_to="$(python3 -c 'import datetime,sys; print(datetime.datetime.fromtimestamp((int(sys.argv[1])-int(sys.argv[2]))/1000, datetime.timezone.utc).strftime("%Y-%m-%d"))' "$WINDOW_END" "$pad_ms")"
verifier verify-restore --target-topic "$TARGET_TOPIC" --label asat \
  --window-start "$WINDOW_START" --window-end "$WINDOW_END" --exact-from "$exact_from" --exact-to "$exact_to"

# ---------------------------------------------------------------------------------------------
if [[ -f "$RUN_DIR/configs/backup-comparison.yaml" ]]; then
  step "C. Keep a rolling range in object storage: the archive's own retention"
  cmp_cfg="/evidence/$RUN_ID/configs/backup-comparison.yaml"
  cmp_id="$TOPIC-comparison"
  verifier archive-coverage --backup-id "$cmp_id" --label .before-prune
  before_bytes="$(json_get "$RUN_DIR/archive/coverage-$cmp_id.before-prune.json" 'd["compressed_bytes"]')"

  info "C1. Age based retention is measured from when each object was uploaded, not from record time"
  kbackup prune --config "$cmp_cfg" --older-than 30d > "$RUN_DIR/archive-options/prune-age-plan.txt" 2>&1 || true
  grep -E 'Prune plan|nothing to prune|Total:' "$RUN_DIR/archive-options/prune-age-plan.txt" || true
  gate archive.retention.age_is_upload_time \
    "an archive written minutes ago has nothing older than 30d, even though it holds a year of records" \
    grep -q 'nothing to prune' "$RUN_DIR/archive-options/prune-age-plan.txt"

  info "C2. Size based retention: keep the newest objects under a cap, oldest pruned first"
  cap=$(( before_bytes / 2 ))
  if ! kbackup prune --config "$cmp_cfg" --max-total-bytes "$cap" --execute > "$RUN_DIR/archive-options/prune-size.txt" 2>&1; then
    # The guard that refuses to prune what looks like a live backup run; this archive is finished.
    if grep -qi 'live\|in progress\|--force' "$RUN_DIR/archive-options/prune-size.txt"; then
      info "prune refused while the archive looked live; re-running with --force"
      kbackup prune --config "$cmp_cfg" --max-total-bytes "$cap" --execute --force \
        > "$RUN_DIR/archive-options/prune-size.txt" 2>&1 || { tail -n 5 "$RUN_DIR/archive-options/prune-size.txt"; fail "prune failed"; }
    else
      tail -n 5 "$RUN_DIR/archive-options/prune-size.txt"; fail "prune failed"
    fi
  fi
  grep -E 'Prune plan|Total:|Pruned' "$RUN_DIR/archive-options/prune-size.txt" | head -n 5 || true
  verifier archive-coverage --backup-id "$cmp_id" --label .after-prune
  after_bytes="$(json_get "$RUN_DIR/archive/coverage-$cmp_id.after-prune.json" 'd["compressed_bytes"]')"
  pruned_ranges="$(json_get "$RUN_DIR/archive/coverage-$cmp_id.after-prune.json" 'd["pruned_ranges"]')"
  gate archive.retention.size_cap_applied \
    "archive went from $before_bytes to $after_bytes bytes, cap $cap, $pruned_ranges pruned range(s) recorded in the manifest" \
    bash -c "[ '$after_bytes' -le '$cap' ] && [ '$pruned_ranges' -gt 0 ]"
  kbackup validate --config "$cmp_cfg" --deep > "$RUN_DIR/archive-options/validate-after-prune.txt" 2>&1 || true
  grep -E '^(Segments|Data Gaps|Pruned Ranges|Result)' "$RUN_DIR/archive-options/validate-after-prune.txt" || true
  gate archive.retention.pruned_archive_still_valid "deliberate retention is not corruption" bash -c \
    "grep -qx 'Result: VALID' '$RUN_DIR/archive-options/validate-after-prune.txt' && grep -Eq '^Data Gaps: +0$' '$RUN_DIR/archive-options/validate-after-prune.txt'"

  step "The sealed archive is untouched by all of this"
  verifier verify-backup --backup-id "$TOPIC" --bucket "$S3_SEALED_BUCKET" --anchor check --sha-only --label .after-archive-options
else
  info "no comparison archive in this run (SKIP_SEGMENT_COMPARISON=1), skipping the archive retention part"
fi
