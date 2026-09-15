SHELL := /usr/bin/env bash
SCALE ?= smoke
export SCALE

PHASES := 00-preflight 10-seed 20-baseline 25-schema-export 30-backup 35-verify-backup 38-seal \
          40-prune-gate 50-retention-prune 60-audit-restore 65-verify-restore 70-full-restore \
          75-readable-archive 80-report

.DEFAULT_GOAL := help
.PHONY: help all up down clean build-kafka-backup negative-tests azurite $(PHASES)

help:
	@echo "make all SCALE=smoke|10g|100g   run every phase with its gates"
	@echo "make <phase>                     run one phase against the current run:"
	@echo "                                 $(PHASES)"
	@echo "make negative-tests              five scenarios that must fail their named gate"
	@echo "make azurite                     Azure client path at smoke scale (needs kafka-backup with Azure emulator support)"
	@echo "make build-kafka-backup          build kafka-backup 0.22.0 natively (arm64 hosts)"
	@echo "make down | clean                stop the stack | stop and delete all volumes"

all:
	@set -e; for p in $(PHASES); do PHASE=$$p scripts/$$p.sh; done

$(PHASES):
	@PHASE=$@ scripts/$@.sh

negative-tests:
	@PHASE=95-negative-tests scripts/95-negative-tests.sh

azurite:
	@PHASE=90-azurite scripts/90-azurite.sh

build-kafka-backup:
	@scripts/build-kafka-backup.sh

up:
	@source scripts/lib.sh && dc up -d --wait source-kafka source-schema-registry target-kafka target-schema-registry minio && dc up minio-setup

down:
	@source scripts/lib.sh && dc --profile tools --profile azurite --profile negative down

clean:
	@source scripts/lib.sh && dc --profile tools --profile azurite --profile negative down -v --remove-orphans && rm -f evidence/.current-run
