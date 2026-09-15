#!/usr/bin/env bash
# Phase 90: the Azure Blob client path against Azurite, at smoke scale.
# Claim is limited to "the Azure client path with shared key auth works". It does not prove
# Workload Identity, Private Endpoint, immutability policies or access tiers.
PHASE=90-azurite
source "$(dirname "$0")/lib.sh"
RUN_SUBDIR=/azurite
start_phase
mkdir -p "$RUN_DIR/azurite"

# Azurite's published development account key (documented by Microsoft; not a secret).
export AZURITE_ACCOUNT_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
export AZ_TOPIC=azurite-smoke BACKUP_ID=azurite-smoke

step "Start Azurite and seed a small topic"
dc --profile azurite up -d azurite
topic_exists source "$AZ_TOPIC" || TOPIC="$AZ_TOPIC" verifier seed --topic "$AZ_TOPIC" --records 30000 --span-days 30 --segment-bytes 1048576
dc --profile tools run --rm -T --entrypoint python verifier -c '
from azure.storage.blob import BlobServiceClient
conn = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey='"$AZURITE_ACCOUNT_KEY"';BlobEndpoint=http://azurite:10000/devstoreaccount1;"
svc = BlobServiceClient.from_connection_string(conn)
try:
    svc.create_container("kafka-backups")
except Exception as e:
    print("container:", type(e).__name__)
print("containers:", [c.name for c in svc.list_containers()])'

step "Backup to Azurite"
render "$REPO_ROOT/config/kafka-backup/azurite-backup.yaml.tmpl" "$RUN_DIR/azurite/configs/azurite-backup.yaml"
if kbackup backup --config "/evidence/$RUN_ID/azurite/configs/azurite-backup.yaml" > "$RUN_DIR/azurite/backup.log" 2>&1; then
  gate_pass azurite.backup "backup to http://azurite:10000 completed"
else
  tail -n 5 "$RUN_DIR/azurite/backup.log"
  gate_fail azurite.backup "kafka-backup $(docker run --rm "$KAFKA_BACKUP_IMAGE" --version | cut -d' ' -f2) cannot reach the emulator; needs Azure emulator support (use_emulator / allow_http) in kafka-backup"
fi

kbackup validate --config "/evidence/$RUN_ID/azurite/configs/azurite-backup.yaml" --deep > "$RUN_DIR/azurite/validate.txt" 2>&1 || true
check_validate_output "$RUN_DIR/azurite/validate.txt" azurite
