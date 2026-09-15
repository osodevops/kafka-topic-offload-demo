#!/usr/bin/env bash
# Phase 70: restore everything into a new topic with consumer group offsets carried across,
# then extrapolate the measured rates to larger topics.
PHASE=70-full-restore
source "$(dirname "$0")/lib.sh"
start_phase
start_stats_sampler

export BACKUP_ID="$TOPIC" BACKUP_BUCKET="$S3_SEALED_BUCKET" S3_PREFIX=backups TARGET_TOPIC="$TOPIC-full"
render "$REPO_ROOT/config/kafka-backup/full-restore.yaml.tmpl" "$RUN_DIR/configs/restore-full.yaml"
if topic_exists target "$TARGET_TOPIC"; then gate_fail restore.full.target_topic_is_new "$TARGET_TOPIC already exists"; fi
gate_pass restore.full.target_topic_is_new "$TARGET_TOPIC does not exist on the target"

step "Full restore with auto_consumer_groups"
t0="$(now_s)"
kbackup restore --config "/evidence/$RUN_ID/configs/restore-full.yaml" 2>&1 | tee "$RUN_DIR/restore/restore-full.log"
secs="$(elapsed_since "$t0")"
printf '{"seconds": %s, "target_topic": "%s"}\n' "$secs" "$TARGET_TOPIC" > "$RUN_DIR/restore/run-full.json"
gate restore.full.consumer_group_reset_applied "$(grep -E '^Groups:' "$RUN_DIR/restore/restore-full.log" | tr -s ' ')" \
  grep -qx 'Result: APPLIED' "$RUN_DIR/restore/restore-full.log"

step "Verify the full restore and the telemetry-analytics offset mapping"
verifier verify-restore --target-topic "$TARGET_TOPIC" --label full --group telemetry-analytics
ids="$(json_get "$RUN_DIR/restore/verify-full.json" '",".join(map(str, d["schema_ids"]))')"
verifier schema-check --ids "$ids" --label full

step "Extrapolation to $EXTRAPOLATE_SIZES_TB TB topics on a $CONFLUENT_CKU CKU Dedicated cluster"
python3 - "$RUN_DIR" "$TOPIC" "$EXTRAPOLATE_SIZES_TB" "$CONFLUENT_CKU" <<'PY'
import json, sys
rd, topic, sizes_arg, cku = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
backup = json.load(open(f"{rd}/backup/summary-{topic}.json"))
restore = json.load(open(f"{rd}/restore/run-full.json"))
logical = backup["uncompressed_bytes"]
rates = {
    "backup_measured_mb_s": logical / backup["seconds"] / 1e6,
    "restore_measured_mb_s": logical / restore["seconds"] / 1e6,
}
# Confluent Cloud Dedicated guidance per CKU: 180 MB/s egress, 60 MB/s ingress. A backup must leave
# headroom for production clients, so it is shown at half the egress guidance.
caps = {"cku": cku, "egress_mb_s": 180 * cku, "ingress_mb_s": 60 * cku}
sizes = {f"{float(s):g} TB": float(s) * 1e12 for s in sizes_arg.split(",")}
rows = []
for name, size in sizes.items():
    for label, rate in [
        ("backup at local measured rate", rates["backup_measured_mb_s"]),
        (f"backup at min(measured, 50% of {cku} CKU egress)", min(rates["backup_measured_mb_s"], caps["egress_mb_s"] / 2)),
        ("restore at local measured rate", rates["restore_measured_mb_s"]),
        (f"restore into Confluent at min(measured, {cku} CKU ingress)", min(rates["restore_measured_mb_s"], caps["ingress_mb_s"])),
    ]:
        hours = size / 1e6 / rate / 3600
        rows.append({"size": name, "scenario": label, "mb_s": round(rate, 1), "hours": round(hours, 1),
                     "within_tier1_1h": hours <= 1, "within_tier2_24h": hours <= 24})
out = {"measured": {k: round(v, 1) for k, v in rates.items()}, "caps": caps, "rows": rows,
       "note": "local single node rates on this host; the Confluent caps are guidance, confirm with a quota test on the real cluster"}
json.dump(out, open(f"{rd}/restore/extrapolation.json", "w"), indent=2)
for r in rows:
    print(f"  {r['size']:<30} {r['scenario']:<58} {r['mb_s']:>7} MB/s {r['hours']:>8} h")
PY
