# Walkthrough: offload a Kafka topic, lower retention, and restore it

This walkthrough runs the demo one phase at a time. For each phase it covers:

- what the phase is for;
- the command;
- the output to expect;
- how to inspect the result yourself.

Example output comes from a `smoke` run (1 GB). At `100g` the counts are about 100 times larger.

It takes about 20 minutes at smoke scale, reading as you go.

## 0. Before you start

**Tools**

- Docker with Compose v2
- `make`, `perl`, `python3` (standard library only), `git` and `shasum`

**Docker VM resources**

- Smoke: 12 GiB of memory and 6 GiB of free disk.
- 100 GB: 24 GiB of memory and 280 GiB of free disk.

**Ports** used on localhost:

| Port | Service |
|---|---|
| 9092 | Source Kafka |
| 9094 | Target Kafka |
| 8081 | Source Schema Registry |
| 8082 | Target Schema Registry |
| 9000 | MinIO API |
| 9001 | MinIO console |

```bash
git clone https://github.com/osodevops/kafka-topic-offload-demo.git
cd kafka-topic-offload-demo
```

`.env` is created from `.env.example` the first time any phase runs. Values set on the command line take precedence, for example `make 10-seed SCALE=10g`.

**How to read the output**

- Every check prints one line, `GATE <name>: PASS|FAIL - detail`.
- A phase stops at its first failure and ends with `PHASE <name>: PASS|FAIL`.
- Everything a phase writes lands in `evidence/<scale>-<UTC timestamp>/`, and `evidence/.current-run` names the current run.

## 1. Preflight: tools, resources, images and the stack

```bash
make 00-preflight SCALE=smoke
```

**What it does**

- Checks tools, Docker memory and disk inside the Docker VM.
- Pulls pinned images.
- Builds kafka-backup natively if needed; the published image is amd64 only.
- Builds the verifier image.
- Starts both clusters, both registries and MinIO, and creates two buckets, one with object lock enabled.
- Records versions and image digests in `environment.json`.

```
GATE preflight.docker_memory: PASS - 24 CPUs, 104 GiB for the Docker VM (need 12 GiB at smoke)
GATE preflight.kafka_backup_version: PASS - kafka-backup 0.22.0 (kafka-backup:0.22.0-arm64)
GATE preflight.kafka_backup_native: PASS - image arm64 on a arm64 Docker VM, no emulation
GATE preflight.stack_healthy: PASS - brokers, registries and MinIO up; buckets kafka-backups and kafka-backups-sealed (object lock)
```

Open the MinIO console at http://localhost:9001 (user `minioadmin`, password `minioadmin`, local only) to watch the buckets fill during later phases.

## 2. Seed a year of telemetry

```bash
make 10-seed
```

**What it creates**

- **Topic:** `device-telemetry`, 6 partitions, `retention.ms=-1`, sized so each partition has about 60 segments.
- **Records:** about 2 KB of Avro each, keyed by device ID, spanning 365 days up to an hour ago.
- **Late arrivals:** 0.5% arrive late, stamped 1 to 72 hours behind their partition's clock.
- **Boundary records:** some sit at exactly UTC midnight and one millisecond before it.
- **Schemas:** two versions, with unrelated schemas registered first so the telemetry schemas get IDs 3 and 4.
- **Consumer group:** `telemetry-analytics`, committed 60 days behind the newest record.

```
GATE seed.murmur2_matches_java_partitioner: PASS - {"21": {...}, "foobar": {...}, "abc": {...}}
GATE seed.watermarks: PASS - 536,870 records, low 0 and high == produced on every partition
GATE seed.no_future_timestamps: PASS - max ts 1789476847559 <= SEED_END_TS 1789477200000
GATE seed.consumer_group_committed_behind: PASS - 6 of 6 partitions committed at 2026-07-17T13:00:00.000 UTC
```

Inspect it:

```bash
docker compose -f compose/docker-compose.yml exec source-kafka \
  kafka-get-offsets --bootstrap-server source-kafka:29092 --topic device-telemetry
docker compose -f compose/docker-compose.yml exec source-kafka \
  kafka-consumer-groups --bootstrap-server source-kafka:29092 --describe --group telemetry-analytics
```

The dataset is deterministic: the same `SEED` and `SCALE` produce identical records.

## 3. Baseline: define "the same data" before changing anything

```bash
make 20-baseline
```

The verifier reads the whole topic once and writes `baseline/device-telemetry.json`:

- **Per partition:** log start offset, high watermark, offset contiguity, and how many timestamps go backwards (it must equal the late record count).
- **Per UTC day:** record count, minimum and maximum timestamp, and a running SHA-256 in offset order over partition, offset, timestamp, key, value and headers.
- **Per broker segment:** base offset, size, record count and minimum and maximum timestamp. The maximum is cross checked against the broker's `.timeindex` file.

```
GATE baseline.count_equals_offsets: PASS - 536,870 records read
GATE baseline.segment_max_ts_matches_timeindex: PASS - 390 segments; broker time index agrees with record scan
GATE baseline.only_late_cohort_goes_backwards: PASS - 2,559 late records, every other timestamp non-decreasing
GATE baseline.broker_size_equals_segment_files: PASS - kafka-log-dirs 1051863950 bytes, segment files 1051863950 bytes
```

Every later comparison uses this file.

## 4. Export schemas by ID

```bash
make 25-schema-export
```

Every schema ID found in the data is exported with its subjects and versions, stored next to the backup, and hashed. A managed Schema Registry has no `_schemas` topic you can back up, so this export is how schema IDs survive.

```
GATE schemas.exported: PASS - ids [3, 4]
```

## 5. Back up the topic

```bash
make 30-backup
```

**What it runs**

- kafka-backup, with [`config/kafka-backup/backup.yaml.tmpl`](../config/kafka-backup/backup.yaml.tmpl) rendered into `configs/backup-primary.yaml`.
- A snapshot backup: from the log start to the high watermark seen at start-up, then exit.
- zstd compression, captured topic configs, and a snapshot of consumer group offsets.
- A second backup with 4x larger segments, so the S3 request counts can be compared.

```
    72 segments, 536,870 records, 1,115,667,523 bytes -> 433,642,784 (2.573x), 1.4s, 825.8 MB/s
GATE backup.completed: PASS - kafka-backup exited 0; log in backup/backup-primary.log
```

**Look at the objects**

```bash
docker compose -f compose/docker-compose.yml --profile tools run --rm mc \
  "mc ls --recursive local/kafka-backups/backups/device-telemetry/ | head"
```

Each partition has `segment-<first offset>.bin.zst` objects plus `manifest.json` and `consumer-groups-snapshot.json`. `s3/requests-backup-primary.json` records the request counts used by the [cost model](cost-model.md).

## 6. Prove the backup

```bash
make 35-verify-backup
```

**Layer 1: kafka-backup's own deep validation**

The phase requires the exact line `Result: VALID` and zero missing, corrupted, gap, pruned and missing topic counts. kafka-backup 0.22.0 prints `Result: VALID (with N recorded data gaps ...)` and exits 0 for an incomplete backup, so the exit code alone proves nothing.

```
GATE backup.validate_result_exactly_valid: PASS - Result: VALID
GATE backup.validate_data_gaps_zero: PASS - Data Gaps: 0
```

**Layer 2: independent verification**

- **Manifest:** segment ranges are contiguous from the log start to the high watermark.
- **Objects:** every stored object's SHA-256 and size equal the manifest.
- **Records:** every record is decoded with the verifier's own reader, and the day bucket digests must equal the baseline.
- **Consumer groups:** the snapshot matches the committed offsets.

```
GATE backup.object_sha256_matches_manifest: PASS - 72 objects, 0.43 GB hashed
GATE backup.content_matches_source_baseline: PASS - 2196 partition-day buckets: count, min/max ts and SHA-256 identical
FINDING manifest segment timestamps are first/last record, not min/max: 55 of 72 segments; 118 records lie outside their segment's bounds
GATE backup.consumer_group_snapshot_matches_committed: PASS - groups ['telemetry-analytics']
```

The `FINDING` line matters for restores; see step 10.

## 7. Seal the backup

```bash
make 38-seal
```

kafka-backup rewrites its manifest during a run, so it cannot write straight into immutable storage. The phase uses the pattern production should follow: write to a mutable bucket, verify, then copy into a bucket with object lock and apply retention.

```
GATE seal.every_object_under_retention: PASS - 75 of 75 objects in GOVERNANCE mode for 30d
GATE seal.worm_blocks_version_delete: PASS - delete of ... refused: object is WORM protected and cannot be overwritten
GATE backup.object_sha256_matches_manifest.sealed: PASS - 72 objects, 0.43 GB hashed
```

Every later restore reads from the sealed copy.

## 8. The prune gate: predict exactly what Kafka will delete

```bash
make 40-prune-gate
```

This is the checklist a change approver signs. It changes nothing.

**Backup and topic state:** the gate confirms that

- the backup was verified, and its manifest checksum still matches in both buckets;
- the sealed manifest covers the live topic;
- the topic has not changed since the baseline.

**The prediction:** the gate applies Kafka's time retention rule (delete a segment when `now - largestTimestamp > retention.ms`, oldest first, stopping at the first keeper) to the real segment files.

- If a segment's newest timestamp is within 15 minutes of the cut, the gate waits until the answer cannot depend on when the broker checks.
- It refuses to empty a partition.
- It lists every consumer group committed below the predicted new log start, and blocks unless each one is named in `ACK_CONSUMER_IMPACT`.

```
GATE prune_gate.prediction_unambiguous: PASS - no segment's largest timestamp within 15 min of the cut
    delete 487,139 records and 0.954 GB; keep 0.098 GB
GATE prune_gate.consumer_impact_acknowledged: PASS - behind cut ['telemetry-analytics'], acknowledged ['telemetry-analytics']
```

The prediction is in `prune/prediction.json`, per partition.

## 9. Lower retention and check the broker against the prediction

```bash
make 50-retention-prune
```

The phase:

1. Re-runs the gate and requires the same prediction.
2. Sets `retention.ms` to 30 days.
3. Waits for the broker's retention check.
4. Compares the broker with the prediction.
5. Resumes the lagging consumer group with `auto.offset.reset=error`.

```
GATE prune.log_start_matches_prediction: PASS - p0 81192, p1 81188, p2 81196, p3 81185, p4 81186, p5 81192
GATE prune.bytes_reclaimed_match_prediction: PASS - 0.954 GB reclaimed, predicted 0.954 GB
GATE prune.only_data_older_than_retention_deleted: PASS - newest deleted record is 33.85 days old, retention 30 days
GATE prune.broker_reported_reclaim_matches_prediction: PASS - kafka-log-dirs 1051863950 -> 97752793 bytes, reclaimed 954111157, predicted 954111157
GATE consumer.offset_out_of_range_surfaced: PASS - 6 partitions below log start, 6 raised OffsetOutOfRange with auto.offset.reset=error
```

Check it yourself: `kafka-get-offsets --time -2` now shows the new log start offsets. Retention deletes whole segments, so the oldest record still on the broker can be a little older than 30 days; the report shows it per partition.

## 10. Restore an audit window from the sealed backup

```bash
make 60-audit-restore
```

An auditor asks for one month from nine months ago, which the live topic no longer holds.

**What the phase does**

1. Imports the exported schemas into the target registry in IMPORT mode, preserving IDs.
2. Computes the window: 30 UTC days, padded by 72 hours on both sides.
3. Checks that the target topic does not exist.
4. Checks that a produce batch of 250 of the largest records fits the target's `message.max.bytes`.
5. Restores from the sealed bucket into `device-telemetry-audit` on the target cluster, with `retention.ms=-1`.

**Why the pad:** kafka-backup 0.22.0 records each segment's first and last record timestamps, not the minimum and maximum. A late record whose timestamp is inside the window, in a segment that starts after the window ends, is otherwise skipped. The `FINDING` in step 6 counts those records, and negative test 5 shows the loss.

```
GATE schemas.imported_by_id: PASS - {"3": {"identical": true}, "4": {"identical": true}}
GATE restore.audit.batch_fits_message_max_bytes: PASS - 250 records x (largest record 2049 + 80 bytes overhead) = 532311 <= message.max.bytes 1048588
GATE restore.audit.target_retention_infinite: PASS - retention.ms=-1 on device-telemetry-audit
```

## 11. Prove the restored month is the source data

```bash
make 65-verify-restore
```

**What is checked**

- Restored records have new offsets, so each one is keyed by its `x-original-offset` header, an 8 byte little endian integer.
- Each record is hashed exactly as the baseline hashed the source.
- Every audit day must match the baseline bucket exactly.
- Any difference in the pad days must be one the manifest bounds predict.
- A sample of values is decoded through the restore registry, and each `eventTime` must equal the record's CreateTime.
- Every schema ID in the restored data must resolve to the same schema on both registries.

```
GATE restore.audit.original_offsets_strictly_increasing: PASS - every record carries x-original-offset; per partition strictly increasing, no duplicates
GATE restore.audit.buckets_match_baseline: PASS - 52,944 records; 30 days x 6 partitions compared exactly
GATE restore.audit.avro_decodes_with_registry: PASS - 3000 sampled records decoded via http://target-schema-registry:8081; eventTime equals CreateTime
FINDING 0 records in 2025-12-19..2026-01-17 would be missed by an unpadded window because their backup segment's first/last timestamps fall outside it
GATE schemas.ids_resolve_identically.audit: PASS - {"3": {"source": 200, "target": 200, "identical": true}}
```

At 100 GB, the same finding reports 88 records that only the pad recovered.

**Restore a different window**

```bash
make clean && make all SCALE=smoke AUDIT_MONTHS_AGO=3 AUDIT_DAYS=7
```

## 12. Full restore with consumer group offsets

```bash
make 70-full-restore
```

**What the phase does**

- Restores the entire sealed backup into `device-telemetry-full` with `auto_consumer_groups: true`.
- kafka-backup then resets the snapshot's consumer groups on the target, using the offset headers.
- The gate reads the target record at the group's new committed offset, and requires its `x-original-offset` to equal the source committed offset.

```
GATE restore.full.consumer_group_reset_applied: PASS - Groups: 1 Partitions reset: 6 Errors: 0
GATE restore.full.buckets_match_baseline: PASS - 536,870 records; 366 days x 6 partitions compared exactly
GATE restore.full.consumer_group_offset_mapped: PASS - telemetry-analytics: the target record at each committed offset carries the source committed offset
```

The phase also writes `restore/extrapolation.json`. It projects the measured backup and restore rates onto larger topics (`EXTRAPOLATE_SIZES_TB`), capped at Confluent Cloud's per CKU throughput guidance for `CONFLUENT_CKU`.

## 13. Read the archive without kafka-backup

```bash
make 75-readable-archive
```

A sealed segment is fetched from MinIO and decoded by [`tools/verifier/verifier/segments.py`](../tools/verifier/verifier/segments.py), about 60 lines that follow the documented format: 32 byte header, zstd payload, CRC32 footer. The values are then decoded as Avro with the exported schemas.

```
GATE archive.segment_readable_without_kafka_backup: PASS - CRC ok, 8084 records 16172..24255, 20 Avro rows decoded
```

Open `archive/*.jsonl` to read the records.

## 14. Report

```bash
make 80-report
cat "evidence/$(cat evidence/.current-run)/report.md"
```

`report.md` collects every gate, the headline numbers, S3 request counts, the per partition prune table, the consumer offset mapping, the extrapolation, timings, container resource peaks and image digests. `SHA256SUMS` lists every evidence file.

## 15. Show that the gates can fail

```bash
make negative-tests
```

Each scenario passes only if its named gate fails for the expected reason:

| Scenario | What is broken | Gate that must fail |
|---|---|---|
| Gap | Retention removes data before the first backup, and the backup asks for offset 0 | `backup.validate_result_exactly_valid` (kafka-backup itself exits 0 with `VALID (with 6 recorded data gaps ...)`), `backup.manifest_covers_log_start_to_high_watermark` |
| Corruption | One byte flipped in one stored object | `backup.validate_result_exactly_valid`, `backup.object_sha256_matches_manifest` |
| Schema ID mismatch | Restored data read through a registry that assigned its own IDs | `schemas.ids_resolve_identically.negative`, `restore.negative-schema.avro_decodes_with_registry` |
| Unacknowledged consumer | A group below the cut, not acknowledged | `prune_gate.consumer_impact_acknowledged` |
| Unpadded window | A one day time window restore without the pad | `restore.negative-window.buckets_match_baseline`, while every missing record is one the manifest bounds predict |

## 16. Azure Blob Storage path (optional)

```bash
make azurite
```

This phase seeds a small topic, starts Azurite and tries a backup to it. kafka-backup 0.22.0 cannot use the emulator or plain HTTP for Azure, so it currently fails with `HTTP error: builder error`. The report shows it outside the verdict. It becomes a working check once kafka-backup supports the emulator.

## 17. Run at 100 GB

```bash
make clean
make all SCALE=100g && make negative-tests && make 80-report
```

It takes about 25 minutes on a 32 core Mac with Docker Desktop. Allow up to 30 more if the prune gate has to wait for a segment near the cut, and keep the machine awake (`caffeinate -i` on macOS): a sleeping host pauses the Docker VM and stretches every timing. [`evidence/reference-100g`](../evidence/reference-100g) holds a committed run.

## 18. Clean up

```bash
make down      # stop the stack, keep the data
make clean     # stop the stack and delete all its volumes; evidence directories are kept
```

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `preflight.docker_disk_free` fails | Free disk is measured inside the Docker VM, not on the host. Raise the Docker Desktop disk limit or prune images |
| First run is slow on Apple silicon | kafka-backup is built from source once, because the published image is amd64 only |
| A port is already in use | Stop the other service, or change the host ports in `compose/docker-compose.yml` |
| `40-prune-gate` waits several minutes | A segment's newest timestamp is within 15 minutes of the cut. The gate waits up to 31 minutes for an answer that cannot depend on when the broker checks |
| `backup.prefix_is_empty` fails | A previous run's backup is still in MinIO. Run `make clean` |
| Peak container resources look low | `docker stats` answers slowly under heavy disk I/O, so samples are sparse during the largest phases |
| cp-server licence warnings after 30 days | Set `CP_SERVER_IMAGE=confluentinc/cp-kafka:8.3.1` |

## Next: production

- [How it works](how-it-works.md) explains every mechanism used above.
- The [Confluent Cloud runbook](runbook-confluent-cloud.md) turns the phases into a production change: access, quotas, schemas, immutable storage, staged retention and drills.
- [Limitations](limitations.md) lists what a local run cannot prove.
