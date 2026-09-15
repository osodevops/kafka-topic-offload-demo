#!/usr/bin/env bash
# Phase 80: assemble report.md and seal the evidence directory with SHA-256 sums.
PHASE=80-report
source "$(dirname "$0")/lib.sh"
start_phase

step "report.md"
verifier report
step "SHA256SUMS"
# timings.tsv and this phase's log are appended after hashing (by the exit trap), so they are left out.
( cd "$RUN_DIR" && find . -type f ! -name SHA256SUMS ! -path './logs/80-report.log' ! -path './timings.tsv' -print0 \
    | sort -z | xargs -0 shasum -a 256 > SHA256SUMS )
ok "$(wc -l < "$RUN_DIR/SHA256SUMS" | tr -d ' ') files hashed; report at evidence/$RUN_ID/report.md"
