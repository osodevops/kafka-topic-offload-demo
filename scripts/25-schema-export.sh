#!/usr/bin/env bash
# Phase 25: export every schema ID the data uses. Confluent Cloud's managed registry has no
# _schemas topic to back up, so the IDs have to travel with the backup.
PHASE=25-schema-export
source "$(dirname "$0")/lib.sh"
start_phase

step "Export schemas by ID from the source registry"
verifier schema-export
info "$(json_get "$RUN_DIR/schemas/$TOPIC-export-meta.json" '"ids {} -> s3://{}/{} sha256 {}".format(d["ids"], d["object"]["bucket"], d["object"]["key"], d["object"]["sha256"][:16])')"
