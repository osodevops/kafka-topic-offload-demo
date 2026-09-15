# Topic offload and retention prune: evidence report

Run `100g-20260915T164407Z` at scale `100g`, started 2026-09-15 16:44 UTC.
Repo `5dd61ddc7a0f`, kafka-backup 0.22.0.

**Result: PASS.** 93 of 93 gates passed.

## Headline numbers

| What | Value | Detail |
|---|---|---|
| Records seeded | 53,687,091 | 6 partitions, one year to 2026-09-15 15:00 UTC |
| Source topic size | 105.20 GB | 354 broker segments |
| Late cohort | 266,156 | 0.5% of records, up to 72 h late |
| Backup | 43.27 GB stored | 2.58x zstd, 834 objects, 1m 12s, 1535.5 MB/s |
| Backup verification | every object SHA-256 and every record digest | 48.3s |
| Retention lowered to | 30 days | applied 2026-09-15 17:42 UTC |
| Bytes reclaimed | 95.96 GB | predicted 95.96 GB, 91.22% of the topic |
| Records removed from the live topic | 48,990,007 | all held in the sealed backup |
| Audit restore | 5,294,966 records | 2025-12-19 to 2026-01-17, 41.4s |
| Full restore | 53,687,091 records | 8m 45s, 212.3 MB/s |

## Gates

### 00-preflight

| Gate | Result | Detail |
|---|---|---|
| preflight.tools | PASS | docker 29.7.2, compose 5.5.1 |
| preflight.docker_memory | PASS | 24 CPUs, 104 GiB for the Docker VM (need 24 GiB at 100g) |
| preflight.docker_disk_free | PASS | 817 GiB free inside the Docker VM (need 280 GiB at 100g) |
| preflight.kafka_backup_version | PASS | kafka-backup 0.22.0 (kafka-backup:0.22.0-arm64) |
| preflight.kafka_backup_native | PASS | image arm64 on a arm64 Docker VM, no emulation |
| preflight.stack_healthy | PASS | brokers, registries and MinIO up; buckets kafka-backups and kafka-backups-sealed (object lock) |

### 10-seed

| Gate | Result | Detail |
|---|---|---|
| seed.murmur2_matches_java_partitioner | PASS | {"21": {"expected": -973932308, "actual": -973932308, "ok": true}, "foobar": {"expected": -790332482, "actual": -790332482, "ok": true}, "abc": {"expected": 479470107, "actual": 479470107, "ok": true}} |
| seed.watermarks | PASS | 53,687,091 records, low 0 and high == produced on every partition |
| seed.no_future_timestamps | PASS | max ts 1789484396475 <= SEED_END_TS 1789484400000 |
| seed.consumer_group_committed_behind | PASS | 6 of 6 partitions committed at 2026-07-17T15:00:00.000 UTC |

### 20-baseline

| Gate | Result | Detail |
|---|---|---|
| baseline.stable_watermarks | PASS | no writes or deletions while the baseline was read |
| baseline.count_equals_offsets | PASS | 53,687,091 records read |
| baseline.contiguous_offsets | PASS |  |
| baseline.create_time_and_wire_format | PASS |  |
| baseline.segment_max_ts_matches_timeindex | PASS | 354 segments; broker time index agrees with record scan |
| baseline.segment_counts | PASS |  |
| baseline.only_late_cohort_goes_backwards | PASS | 266,156 late records, every other timestamp non-decreasing |
| baseline.nothing_after_seed_end | PASS |  |
| baseline.broker_size_equals_segment_files | PASS | kafka-log-dirs 105195911169 bytes, segment files 105195911169 bytes |

### 25-schema-export

| Gate | Result | Detail |
|---|---|---|
| schemas.exported | PASS | ids [3, 4] |

### 30-backup

| Gate | Result | Detail |
|---|---|---|
| backup.completed | PASS | kafka-backup exited 0; log in backup/backup-primary.log |

### 35-verify-backup

| Gate | Result | Detail |
|---|---|---|
| backup.validate_result_exactly_valid | PASS | Result: VALID |
| backup.validate_segments_missing_zero | PASS | Segments Missing: 0 |
| backup.validate_segments_corrupted_zero | PASS | Segments Corrupted: 0 |
| backup.validate_data_gaps_zero | PASS | Data Gaps: 0 |
| backup.validate_pruned_ranges_zero | PASS | Pruned Ranges: 0 |
| backup.validate_missing_topics_zero | PASS | Missing Topics: 0 |
| backup.validate_no_issues | PASS | no 'Issues Found' section |
| backup.manifest_covers_log_start_to_high_watermark | PASS | 53,687,091 records in 834 segments |
| backup.manifest_no_gaps_or_pruned | PASS |  |
| backup.topic_config_captured | PASS | retention.ms=-1 partitions=6 |
| backup.object_sha256_matches_manifest | PASS | 834 objects, 43.27 GB hashed |
| backup.segments_decode_independently | PASS | CRC, header, record count and offsets agree for every segment |
| backup.offset_headers_little_endian | PASS | x-original-offset and x-original-timestamp decode as i64 LE and equal the record |
| backup.content_matches_source_baseline | PASS | 2196 partition-day buckets: count, min/max ts and SHA-256 identical |
| backup.consumer_group_snapshot_matches_committed | PASS | groups ['telemetry-analytics'] |

### 38-seal

| Gate | Result | Detail |
|---|---|---|
| seal.every_object_under_retention | PASS | 837 of 837 objects in GOVERNANCE mode for 30d |
| seal.worm_blocks_version_delete | PASS | delete of topics/device-telemetry/partition=0/segment-00000000000000000000.bin.zst version 267bae1f-610f-4015-8c21-730cdd470cca refused: object is WORM protected and cannot be overwritten |
| backup.manifest_anchor_intact.sealed | PASS | kafka-backups-sealed: manifest sha256 77e2b8d9f7eb25e7... vs anchor 77e2b8d9f7eb25e7... |
| backup.manifest_covers_log_start_to_high_watermark.sealed | PASS | 53,687,091 records in 834 segments |
| backup.manifest_no_gaps_or_pruned.sealed | PASS |  |
| backup.topic_config_captured.sealed | PASS | retention.ms=-1 partitions=6 |
| backup.object_sha256_matches_manifest.sealed | PASS | 834 objects, 43.27 GB hashed |

### 40-prune-gate

| Gate | Result | Detail |
|---|---|---|
| prune_gate.backup_verified | PASS | verify-device-telemetry.json |
| prune_gate.manifest_anchor_intact.kafka-backups | PASS |  |
| prune_gate.manifest_anchor_intact.kafka-backups-sealed | PASS |  |
| prune_gate.sealed_copy_verified | PASS |  |
| prune_gate.backup_covers_live_topic | PASS | sealed manifest spans the current log start to high watermark on every partition, no gaps |
| prune_gate.topic_unchanged_since_baseline | PASS | same segments, sizes and watermarks |
| prune_gate.prediction_unambiguous | PASS | no segment's largest timestamp within 15 min of the cut |
| prune_gate.newest_data_survives | PASS | SEED_END_TS is inside the new retention, so the topic is not emptied |
| prune_gate.no_partition_emptied | PASS |  |
| prune_gate.consumer_impact_acknowledged | PASS | behind cut ['telemetry-analytics'], acknowledged ['telemetry-analytics'] |

### 50-retention-prune

| Gate | Result | Detail |
|---|---|---|
| prune_gate.backup_verified | PASS | verify-device-telemetry.json |
| prune_gate.manifest_anchor_intact.kafka-backups | PASS |  |
| prune_gate.manifest_anchor_intact.kafka-backups-sealed | PASS |  |
| prune_gate.sealed_copy_verified | PASS |  |
| prune_gate.backup_covers_live_topic | PASS | sealed manifest spans the current log start to high watermark on every partition, no gaps |
| prune_gate.topic_unchanged_since_baseline | PASS | same segments, sizes and watermarks |
| prune_gate.prediction_unambiguous | PASS | no segment's largest timestamp within 15 min of the cut |
| prune_gate.newest_data_survives | PASS | SEED_END_TS is inside the new retention, so the topic is not emptied |
| prune_gate.no_partition_emptied | PASS |  |
| prune.prediction_unchanged_since_gate | PASS | gate prediction made 4s ago |
| prune_gate.consumer_impact_acknowledged | PASS | behind cut ['telemetry-analytics'], acknowledged ['telemetry-analytics'] |
| prune.log_start_matches_prediction | PASS | p0 8214611, p1 8213552, p2 8068095, p3 8213514, p4 8065654, p5 8214581 |
| prune.remaining_segments_match_prediction | PASS |  |
| prune.bytes_reclaimed_match_prediction | PASS | 95.962 GB reclaimed, predicted 95.962 GB |
| prune.only_data_older_than_retention_deleted | PASS | newest deleted record is 30.02 days old, retention 30 days |
| prune.sealed_backup_covers_every_deleted_offset | PASS |  |
| prune.broker_reported_reclaim_matches_prediction | PASS | kafka-log-dirs 105195911169 -> 9233703236 bytes, reclaimed 95962207933, predicted 95962207933 |
| consumer.offset_out_of_range_surfaced | PASS | 6 partitions below log start, 6 raised OffsetOutOfRange with auto.offset.reset=error |

### 60-audit-restore

| Gate | Result | Detail |
|---|---|---|
| schemas.imported_by_id | PASS | {"3": {"source": 200, "target": 200, "identical": true}, "4": {"source": 200, "target": 200, "identical": true}} |
| restore.audit.target_topic_is_new | PASS | device-telemetry-audit does not exist on the target |
| restore.audit.batch_fits_message_max_bytes | PASS | 250 records x (largest record 2050 + 80 bytes overhead) = 532561 <= message.max.bytes 1048588 |
| restore.audit.target_retention_infinite | PASS | retention.ms=-1 on device-telemetry-audit |

### 65-verify-restore

| Gate | Result | Detail |
|---|---|---|
| restore.audit.original_offsets_strictly_increasing | PASS | every record carries x-original-offset; per partition strictly increasing, no duplicates |
| restore.audit.create_time_preserved | PASS |  |
| restore.audit.no_records_outside_window | PASS | window 1765843200000..1768953599999 |
| restore.audit.buckets_match_baseline | PASS | 5,294,966 records; 30 days x 6 partitions compared exactly |
| restore.audit.every_difference_explained_by_segment_bounds | PASS | 182 records missing across 9 buckets, 182 predicted by manifest bounds |
| restore.audit.avro_decodes_with_registry | PASS | 3000 sampled records decoded via http://target-schema-registry:8081; eventTime equals CreateTime |
| schemas.ids_resolve_identically.audit | PASS | {"3": {"source": 200, "target": 200, "identical": true}} |

### 70-full-restore

| Gate | Result | Detail |
|---|---|---|
| restore.full.target_topic_is_new | PASS | device-telemetry-full does not exist on the target |
| restore.full.consumer_group_reset_applied | PASS | Groups: 1 Partitions reset: 6 Errors: 0 |
| restore.full.original_offsets_strictly_increasing | PASS | every record carries x-original-offset; per partition strictly increasing, no duplicates |
| restore.full.create_time_preserved | PASS |  |
| restore.full.no_records_outside_window | PASS | window None..None |
| restore.full.buckets_match_baseline | PASS | 53,687,091 records; 366 days x 6 partitions compared exactly |
| restore.full.every_difference_explained_by_segment_bounds | PASS | 0 records missing across 0 buckets, 0 predicted by manifest bounds |
| restore.full.avro_decodes_with_registry | PASS | 3000 sampled records decoded via http://target-schema-registry:8081; eventTime equals CreateTime |
| restore.full.consumer_group_offset_mapped | PASS | telemetry-analytics: the target record at each committed offset carries the source committed offset |
| schemas.ids_resolve_identically.full | PASS | {"3": {"source": 200, "target": 200, "identical": true}, "4": {"source": 200, "target": 200, "identical": true}} |

### 75-readable-archive

| Gate | Result | Detail |
|---|---|---|
| archive.segment_readable_without_kafka_backup | PASS | CRC ok, 64638 records 2262821..2327458, 20 Avro rows decoded |

## Findings

- `35-verify-backup`: manifest segment timestamps are first/last record, not min/max: 828 of 834 segments; 241,056 records lie outside their segment's bounds
- `65-verify-restore`: 88 records in 2025-12-19..2026-01-17 would be missed by an unpadded window because their backup segment's first/last timestamps fall outside it

## Object storage requests

| S3 API | Backup, 128 MiB segments | Backup, 512 MiB segments |
|---|---|---|
| getobject | 7 | 7 |
| headobject | 1 | 1 |
| putobject | 847 | 223 |

Objects: 834 at 128 MiB against 210 at 512 MiB; wall clock 1m 12s against 1m 11s.

## Retention prune per partition

| Partition | Log start before | Predicted log start | Log start after | Segments deleted | Bytes deleted | Oldest record still on the broker |
|---|---|---|---|---|---|---|
| 0 | 0 | 8,214,611 | 8,214,611 | 54 | 16.09 GB | 2026-08-13 21:32 UTC |
| 1 | 0 | 8,213,552 | 8,213,552 | 54 | 16.09 GB | 2026-08-13 19:58 UTC |
| 2 | 0 | 8,068,095 | 8,068,095 | 53 | 15.80 GB | 2026-08-07 19:55 UTC |
| 3 | 0 | 8,213,514 | 8,213,514 | 54 | 16.09 GB | 2026-08-13 19:27 UTC |
| 4 | 0 | 8,065,654 | 8,065,654 | 53 | 15.80 GB | 2026-08-07 18:14 UTC |
| 5 | 0 | 8,214,581 | 8,214,581 | 54 | 16.09 GB | 2026-08-13 22:09 UTC |

retention deletes whole segments: records older than the cut remain until their segment's newest record ages out

Consumer `telemetry-analytics` resumed with `auto.offset.reset=error`: 6 partitions raised OffsetOutOfRange instead of silently skipping data.

## Restores

Audit window 2025-12-19 to 2026-01-17 UTC, restored with a 72 h pad each side. Buckets that differ inside the padded window (pad days only are expected): 9; unexplained: 0.

| Partition | Source committed | Target committed | Target record's x-original-offset |
|---|---|---|---|
| 0 | 7,476,970 | 7,476,970 | 7,476,970 |
| 1 | 7,476,970 | 7,476,970 | 7,476,970 |
| 2 | 7,476,970 | 7,476,970 | 7,476,970 |
| 3 | 7,476,969 | 7,476,969 | 7,476,969 |
| 4 | 7,476,969 | 7,476,969 | 7,476,969 |
| 5 | 7,476,969 | 7,476,969 | 7,476,969 |

## Extrapolation

| Size | Scenario | MB/s | Hours | Tier 1 (1 h) | Tier 2 (24 h) |
|---|---|---|---|---|---|
| 1 TB | backup at local measured rate | 1535.5 | 0.2 | yes | yes |
| 1 TB | backup at min(measured, 50% of 2 CKU egress) | 180.0 | 1.5 | no | yes |
| 1 TB | restore at local measured rate | 212.3 | 1.3 | no | yes |
| 1 TB | restore into Confluent at min(measured, 2 CKU ingress) | 120 | 2.3 | no | yes |
| 10 TB | backup at local measured rate | 1535.5 | 1.8 | no | yes |
| 10 TB | backup at min(measured, 50% of 2 CKU egress) | 180.0 | 15.4 | no | yes |
| 10 TB | restore at local measured rate | 212.3 | 13.1 | no | yes |
| 10 TB | restore into Confluent at min(measured, 2 CKU ingress) | 120 | 23.1 | no | yes |
| 50 TB | backup at local measured rate | 1535.5 | 9.0 | no | yes |
| 50 TB | backup at min(measured, 50% of 2 CKU egress) | 180.0 | 77.2 | no | no |
| 50 TB | restore at local measured rate | 212.3 | 65.4 | no | no |
| 50 TB | restore into Confluent at min(measured, 2 CKU ingress) | 120 | 115.7 | no | no |

local single node rates on this host; the Confluent caps are guidance, confirm with a quota test on the real cluster

## Negative tests

Each scenario passes only when its named gate fails for the expected reason.

| Check | Result | Detail |
|---|---|---|
| negative.gap.kafka_backup_validate_exits_zero_with_gaps | PASS | exit 0: Result: VALID (with 6 recorded data gaps — backup is intact but incomplete) |
| negative.gap | PASS | GATE backup.manifest_covers_log_start_to_high_watermark failed as designed |
| negative.corruption | PASS | GATE backup.object_sha256_matches_manifest failed as designed |
| negative.schema-id | PASS | GATE restore.negative-schema.avro_decodes_with_registry failed as designed |
| negative.unacked-consumer | PASS | GATE prune_gate.consumer_impact_acknowledged failed as designed |
| negative.unpadded-window | PASS | GATE restore.negative-window.buckets_match_baseline failed as designed |
| negative.unpadded-window.loss_explained_by_segment_bounds | PASS | every missing record is one the manifest bounds predict |

Setup gates for the scenario topics: 15 of 15 passed.

## Azure client path (Azurite)

Not part of the verdict. kafka-backup 0.22.0 cannot use plain HTTP or emulator mode for Azure, so this phase is expected to fail until that change is released.

| Check | Result | Detail |
|---|---|---|
| azurite.backup | FAIL | kafka-backup 0.22.0 cannot reach the emulator; needs Azure emulator support (use_emulator / allow_http) in kafka-backup |

Setup gates for the scenario topics: 3 of 3 passed.

## Timings

| Phase | Wall clock | Result |
|---|---|---|
| 00-preflight | 22.0s | PASS |
| 10-seed | 1m 36s | PASS |
| 20-baseline | 1m 07s | PASS |
| 25-schema-export | 1.0s | PASS |
| 30-backup | 3m 18s | PASS |
| 35-verify-backup | 1m 51s | PASS |
| 38-seal | 29.0s | PASS |
| 40-prune-gate | 49m 10s | PASS |
| 50-retention-prune | 1m 24s | PASS |
| 60-audit-restore | 48.0s | PASS |
| 65-verify-restore | 15.0s | PASS |
| 70-full-restore | 10m 06s | PASS |
| 75-readable-archive | 2.0s | PASS |
| 80-report | 2.0s | PASS |
| 95-negative-tests | 59.0s | PASS |
| 90-azurite | 5.0s | FAIL |

## Peak container resources

| Container | Peak CPU % | Peak memory |
|---|---|---|
| offload-kafka-backup-run-9103eda630f2 | 98 | 217MiB |
| offload-kafka-backup-run-eac235ac1dc9 | 603 | 2.353GiB |
| offload-kafka-backup-run-f1b74600b240 | 155 | 2.941GiB |
| offload-kafka-backup-run-f20ed7c10742 | 161 | 6.006GiB |
| offload-minio-1 | 149 | 2.364GiB |
| offload-source-kafka-1 | 225 | 5.794GiB |
| offload-source-schema-registry-1 | 70 | 374.7MiB |
| offload-target-kafka-1 | 163 | 3.889GiB |
| offload-target-schema-registry-1 | 177 | 315.7MiB |
| offload-verifier-run-28430d94182c | 710 | 566.8MiB |
| offload-verifier-run-7971c84118ca | 331 | 2.322GiB |
| offload-verifier-run-882f3ec3d4bf | 476 | 4.549GiB |
| offload-verifier-run-f80850d42d9e | 598 | 2.402GiB |

## Environment

| Image | Reference | Architecture | Digest or ID |
|---|---|---|---|
| CP_KAFKA_IMAGE | confluentinc/cp-kafka:8.3.1 | arm64 | confluentinc/cp-kafka@sha256:0ad069035863aa1b090f4d9af47bfd2c08dc32864f3575d7d8579e3155c2586d |
| CP_SCHEMA_REGISTRY_IMAGE | confluentinc/cp-schema-registry:8.3.1 | arm64 | confluentinc/cp-schema-registry@sha256:f0cfd047a839c1ace54d93b92e3459f0d03dc3b5c9db1192a2246fd79b4f44c4 |
| CP_SERVER_IMAGE | confluentinc/cp-server:8.3.1 | arm64 | confluentinc/cp-server@sha256:62e3b04c4c88d03fbb484b9976e6145f751c2e908310f3f8b2db5ae7fcd3608f |
| KAFKA_BACKUP_IMAGE | kafka-backup:0.22.0-arm64 | arm64 | kafka-backup@sha256:ba7ddc11f4dec7968e21524e7f9246f34ac1a6cab553176bb6be22e3c3ad19f7 |
| MC_IMAGE | quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z | arm64 | minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727 |
| MINIO_IMAGE | quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z | arm64 | minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e |
| verifier | offload-verifier:local |  | sha256:b75151a9d2178a84a7140690adae67641fa01cab27d5cd7b71f3195c1dd70b66 |

Docker VM: 24 CPUs, 104 GiB; host Darwin arm64.

Every file in this directory is listed with its SHA-256 in `SHA256SUMS`.
