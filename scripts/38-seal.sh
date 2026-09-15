#!/usr/bin/env bash
# Phase 38: write mutable, validate, then seal. kafka-backup rewrites manifest.json, offsets.db
# and the group snapshot during a run, so it cannot write into a WORM location directly.
# The validated backup is copied to an object lock bucket and put under retention.
PHASE=38-seal
source "$(dirname "$0")/lib.sh"
start_phase
mkdir -p "$RUN_DIR/seal"
src="local/$S3_BUCKET/backups/$TOPIC"
dst="local/$S3_SEALED_BUCKET/backups/$TOPIC"

count_objects() { mc_cmd "mc ls --recursive $1/ | wc -l" | tr -d ' \r'; }

step "Copy the verified backup into the object lock bucket"
src_count="$(count_objects "$src")"
dst_count="$(count_objects "$dst")"
if [[ "$dst_count" -gt 0 && "$dst_count" -eq "$src_count" ]]; then
  # Re-running the phase: never copy into a locked bucket twice, it only adds undeletable versions.
  info "sealed copy already holds all $dst_count objects; skipping copy and retention (verified below)"
else
  mc_cmd "mc cp --recursive --quiet $src/ $dst/" >/dev/null
  mc_cmd "mc retention set --recursive $SEAL_MODE $SEAL_RETENTION $dst" > "$RUN_DIR/seal/retention-set.txt"
  tail -n 2 "$RUN_DIR/seal/retention-set.txt"
fi

step "Record retention on every sealed object"
mc_cmd "mc retention info --recursive --json $dst" > "$RUN_DIR/seal/retention-info.jsonl"
total="$(grep -c '"status":"success"' "$RUN_DIR/seal/retention-info.jsonl" || true)"
locked="$(grep -c "\"mode\":\"$SEAL_MODE\"" "$RUN_DIR/seal/retention-info.jsonl" || true)"
gate seal.every_object_under_retention "$locked of $total objects in $SEAL_MODE mode for $SEAL_RETENTION" \
  bash -c "[ '$total' -gt 0 ] && [ '$locked' -eq '$total' ]"

step "A sealed object version cannot be deleted"
# Read the whole listing before choosing: exiting early breaks mc's pipe and pipefail stops the phase.
key="$(mc_cmd "mc ls --recursive --json $dst" | python3 -c 'import json,sys; lines = sys.stdin.read().splitlines(); print(next(json.loads(l)["key"] for l in lines if "segment-" in l))')"
version="$(mc_cmd "mc stat --json $dst/$key" | python3 -c 'import json,sys; print(json.load(sys.stdin)["versionID"])')"
[[ -n "$version" ]] || gate_fail seal.worm_blocks_version_delete "no version ID for $key; cannot attempt a permanent delete"
if mc_cmd "mc rm --version-id $version $dst/$key" > "$RUN_DIR/seal/delete-attempt.txt" 2>&1; then
  gate_fail seal.worm_blocks_version_delete "permanent delete of $key version $version succeeded"
fi
# Refused for the right reason, not a typo or a missing object.
refusal="$(grep -o "is WORM protected and cannot be overwritten" "$RUN_DIR/seal/delete-attempt.txt" | head -n 1)"
gate seal.worm_blocks_version_delete "delete of $key version $version refused: object ${refusal:-NOT reported as WORM protected}" [ -n "$refusal" ]

step "Verify the sealed copy against the manifest anchor taken in phase 35"
verifier verify-backup --backup-id "$TOPIC" --bucket "$S3_SEALED_BUCKET" --anchor check --sha-only --label .sealed
printf '{"passed": true, "bucket": "%s", "mode": "%s", "retention": "%s", "objects": %s}\n' \
  "$S3_SEALED_BUCKET" "$SEAL_MODE" "$SEAL_RETENTION" "$total" > "$RUN_DIR/seal/seal.json"
