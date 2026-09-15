#!/usr/bin/env bash
# Phase 30: snapshot backup of the topic to MinIO with kafka-backup, measuring time and requests.
PHASE=30-backup
source "$(dirname "$0")/lib.sh"
start_phase
start_stats_sampler
mkdir -p "$RUN_DIR/backup"

# Production sizing is 128 MiB. At smoke scale that is two objects per partition, too few to
# show segment level behaviour, so smoke uses 16 MiB. The comparison run is 4x larger.
case "$SCALE" in
  smoke) primary_bytes=16777216;  comparison_bytes=67108864 ;;
  *)     primary_bytes=134217728; comparison_bytes=536870912 ;;
esac

now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }

run_backup() {  # run_backup BACKUP_ID SEGMENT_MAX_BYTES LABEL
  export BACKUP_ID="$1" SEGMENT_MAX_BYTES="$2" BACKUP_BUCKET="$S3_BUCKET" S3_PREFIX=backups BACKUP_EXTRA=""
  local cfg="configs/backup-$3.yaml" existing t0 secs msg
  existing="$(mc_cmd "mc ls --recursive local/$S3_BUCKET/backups/$1/ | head -n 1")"
  [[ -z "$existing" ]] || gate_fail backup.prefix_is_empty "s3://$S3_BUCKET/backups/$1/ already has objects; run make clean for a fresh run"
  render "$REPO_ROOT/config/kafka-backup/backup.yaml.tmpl" "$RUN_DIR/$cfg"
  verifier s3-requests --label "before-$3"
  t0="$(now)"
  kbackup backup --config "/evidence/$RUN_ID/$cfg" 2>&1 | tee "$RUN_DIR/backup/backup-$3.log"
  secs="$(perl -e "printf '%.3f', $(now) - $t0")"
  verifier s3-requests --label "after-$3"
  verifier s3-requests --diff "before-$3" "after-$3" --label "backup-$3"
  verifier manifest-summary --backup-id "$1" --seconds "$secs"
  printf '{"label": "%s", "backup_id": "%s", "segment_max_bytes": %s, "seconds": %s}\n' "$3" "$1" "$2" "$secs" > "$RUN_DIR/backup/run-$3.json"
  msg=$(json_get "$RUN_DIR/backup/summary-$1.json" '"{segments} segments, {records:,} records, {uncompressed_bytes:,} bytes -> {compressed_bytes:,} ({compression_ratio}x), {seconds}s, {mb_per_s_uncompressed} MB/s".format(**d)')
  info "$msg"
}

step "Backup $TOPIC, $(( primary_bytes / 1048576 )) MiB segments"
run_backup "$TOPIC" "$primary_bytes" primary
gate backup.completed "kafka-backup exited 0; log in backup/backup-primary.log" grep -q "Backup completed successfully" "$RUN_DIR/backup/backup-primary.log"

if [[ "$SKIP_SEGMENT_COMPARISON" != 1 ]]; then
  step "Comparison backup at $(( comparison_bytes / 1048576 )) MiB segments (request counts for the cost model)"
  # Kept, not deleted: removing 43 GB from MinIO took about 15 minutes on Docker Desktop at 100g and
  # would dominate this phase's timing. `make clean` removes it with the volumes.
  run_backup "$TOPIC-comparison" "$comparison_bytes" comparison
fi
