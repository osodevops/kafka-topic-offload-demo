# Topic offload and retention prune: evidence report

Run `100g-20260916T051309Z` at scale `100g`, started 2026-09-16 05:13 UTC.
Repo `95a13bc1efd5` (uncommitted changes), kafka-backup 0.22.0.

**Result: PASS.** 118 of 118 gates passed.

## Headline numbers

| What | Value | Detail |
|---|---|---|
| Records seeded | 53,687,091 | 6 partitions, one year to 2026-09-16 04:00 UTC |
| Source topic size | 105.20 GB | 354 broker segments |
| Late cohort | 266,068 | 0.5% of records, up to 72 h late |
| Backup | 43.27 GB stored | 2.58x zstd, 834 objects, 1m 12s, 1545.5 MB/s |
| Backup verification | every object SHA-256 and every record digest | 48.0s |
| Retention lowered to | 30 days | applied 2026-09-16 05:24 UTC |
| Bytes reclaimed | 95.37 GB | predicted 95.37 GB, 90.66% of the topic |
| Records removed from the live topic | 48,687,479 | all held in the sealed backup |
| Audit restore | 5,294,833 records | 2025-12-20 to 2026-01-18, 32.6s |
| Full restore | 53,687,091 records | 8m 05s, 230.1 MB/s |

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
| seed.no_future_timestamps | PASS | max ts 1789531196475 <= SEED_END_TS 1789531200000 |
| seed.consumer_group_committed_behind | PASS | 6 of 6 partitions committed at 2026-07-18T04:00:00.000 UTC |

### 20-baseline

| Gate | Result | Detail |
|---|---|---|
| baseline.stable_watermarks | PASS | no writes or deletions while the baseline was read |
| baseline.count_equals_offsets | PASS | 53,687,091 records read |
| baseline.contiguous_offsets | PASS |  |
| baseline.create_time_and_wire_format | PASS |  |
| baseline.segment_max_ts_matches_timeindex | PASS | 354 segments; broker time index agrees with record scan |
| baseline.segment_counts | PASS |  |
| baseline.only_late_cohort_goes_backwards | PASS | 266,068 late records, every other timestamp non-decreasing |
| baseline.nothing_after_seed_end | PASS |  |
| baseline.broker_size_equals_segment_files | PASS | kafka-log-dirs 105195905116 bytes, segment files 105195905116 bytes |

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
| backup.content_matches_source_baseline | PASS | 2196 fully covered partition-day buckets: count, min/max ts and SHA-256 identical |
| backup.consumer_group_snapshot_matches_committed | PASS | groups ['telemetry-analytics'] |

### 38-seal

| Gate | Result | Detail |
|---|---|---|
| seal.every_object_under_retention | PASS | 837 of 837 objects in GOVERNANCE mode for 30d |
| seal.worm_blocks_version_delete | PASS | delete of topics/device-telemetry/partition=0/segment-00000000000000000000.bin.zst version 42837873-9168-46f0-8daa-14c098d9af1a refused: object is WORM protected and cannot be overwritten |
| backup.manifest_anchor_intact.sealed | PASS | kafka-backups-sealed: manifest sha256 9ddf630a719fb866... vs anchor 9ddf630a719fb866... |
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

### 45-archive-options

| Gate | Result | Detail |
|---|---|---|
| archive.since.offsets_for_time_resolved | PASS | 6 of 6 partitions have an offset at or after the instant |
| backup.manifest_covers_log_start_to_high_watermark.since | PASS | 13,237,908 records in 210 segments, starting at the requested offsets {0: 6741531, 1: 6741531, 2: 6741531, 3: 6741530, 4: 6741530, 5: 6741530} |
| backup.manifest_no_gaps_or_pruned.since | PASS |  |
| backup.topic_config_captured.since | PASS | retention.ms=-1 partitions=6 |
| backup.object_sha256_matches_manifest.since | PASS | 210 objects, 10.69 GB hashed |
| backup.segments_decode_independently.since | PASS | CRC, header, record count and offsets agree for every segment |
| backup.offset_headers_little_endian.since | PASS | x-original-offset and x-original-timestamp decode as i64 LE and equal the record |
| backup.content_matches_source_baseline.since | PASS | 540 fully covered partition-day buckets: count, min/max ts and SHA-256 identical |
| backup.consumer_group_snapshot_matches_committed.since | PASS | groups ['telemetry-analytics'] |
| archive.since.starts_at_the_instant | PASS | earliest archived record is at 2026-06-18 04:00 UTC minus at most the 72h late window |
| schemas.imported_by_id | PASS | {"3": {"source": 200, "target": 200, "identical": true}, "4": {"source": 200, "target": 200, "identical": true}} |
| restore.asat.original_offsets_strictly_increasing | PASS | every record carries x-original-offset; per partition strictly increasing, no duplicates |
| restore.asat.create_time_preserved | PASS |  |
| restore.asat.no_records_outside_window | PASS | window 1779163200000..1781755199999 |
| restore.asat.buckets_match_baseline | PASS | 4,412,442 records; 25 days x 6 partitions compared exactly |
| restore.asat.every_difference_explained_by_segment_bounds | PASS | 143 records missing across 6 buckets, 189 predicted by manifest bounds |
| restore.asat.avro_decodes_with_registry | PASS | 3000 sampled records decoded via http://target-schema-registry:8081; eventTime equals CreateTime |
| archive.retention.age_is_upload_time | PASS | an archive written minutes ago has nothing older than 30d, even though it holds a year of records |
| archive.retention.size_cap_applied | PASS | archive went from 43265098531 to 21611435614 bytes, cap 21632549265, 6 pruned range(s) recorded in the manifest |
| archive.retention.pruned_archive_still_valid | PASS | deliberate retention is not corruption |
| backup.manifest_anchor_intact.after-archive-options | PASS | kafka-backups-sealed: manifest sha256 9ddf630a719fb866... vs anchor 9ddf630a719fb866... |
| backup.manifest_covers_log_start_to_high_watermark.after-archive-options | PASS | 53,687,091 records in 834 segments |
| backup.manifest_no_gaps_or_pruned.after-archive-options | PASS |  |
| backup.topic_config_captured.after-archive-options | PASS | retention.ms=-1 partitions=6 |
| backup.object_sha256_matches_manifest.after-archive-options | PASS | 834 objects, 43.27 GB hashed |

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
| prune.log_start_matches_prediction | PASS | p0 8063348, p1 8213570, p2 8068086, p3 8213513, p4 8065658, p5 8063304 |
| prune.remaining_segments_match_prediction | PASS |  |
| prune.bytes_reclaimed_match_prediction | PASS | 95.367 GB reclaimed, predicted 95.367 GB |
| prune.only_data_older_than_retention_deleted | PASS | newest deleted record is 30.01 days old, retention 30 days |
| prune.sealed_backup_covers_every_deleted_offset | PASS |  |
| prune.broker_reported_reclaim_matches_prediction | PASS | kafka-log-dirs 105195905116 -> 9828418722 bytes, reclaimed 95367486394, predicted 95367486394 |
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
| restore.audit.no_records_outside_window | PASS | window 1765929600000..1769039999999 |
| restore.audit.buckets_match_baseline | PASS | 5,294,833 records; 30 days x 6 partitions compared exactly |
| restore.audit.every_difference_explained_by_segment_bounds | PASS | 353 records missing across 12 buckets, 353 predicted by manifest bounds |
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

- `35-verify-backup`: manifest segment timestamps are first/last record, not min/max: 828 of 834 segments; 236,807 records lie outside their segment's bounds
- `45-archive-options`: manifest segment timestamps are first/last record, not min/max: 210 of 210 segments; 37,874 records lie outside their segment's bounds
- `45-archive-options`: 355 records in 2026-05-22..2026-06-15 would be missed by an unpadded window because their backup segment's first/last timestamps fall outside it
- `65-verify-restore`: 182 records in 2025-12-20..2026-01-18 would be missed by an unpadded window because their backup segment's first/last timestamps fall outside it

## Object storage requests

| S3 API | Backup, 128 MiB segments | Backup, 512 MiB segments |
|---|---|---|
| getobject | 7 | 7 |
| headobject | 1 | 1 |
| putobject | 847 | 223 |

Objects: 834 at 128 MiB against 210 at 512 MiB; wall clock 1m 12s against 1m 14s.

## Retention prune per partition

| Partition | Log start before | Predicted log start | Log start after | Segments deleted | Bytes deleted | Oldest record still on the broker |
|---|---|---|---|---|---|---|
| 0 | 0 | 8,063,348 | 8,063,348 | 53 | 15.79 GB | 2026-08-08 05:43 UTC |
| 1 | 0 | 8,213,570 | 8,213,570 | 54 | 16.09 GB | 2026-08-14 08:59 UTC |
| 2 | 0 | 8,068,086 | 8,068,086 | 53 | 15.80 GB | 2026-08-08 08:55 UTC |
| 3 | 0 | 8,213,513 | 8,213,513 | 54 | 16.09 GB | 2026-08-14 08:28 UTC |
| 4 | 0 | 8,065,658 | 8,065,658 | 53 | 15.80 GB | 2026-08-08 07:14 UTC |
| 5 | 0 | 8,063,304 | 8,063,304 | 53 | 15.79 GB | 2026-08-08 07:03 UTC |

retention deletes whole segments: records older than the cut remain until their segment's newest record ages out

Consumer `telemetry-analytics` resumed with `auto.offset.reset=error`: 6 partitions raised OffsetOutOfRange instead of silently skipping data.

## Restores

Audit window 2025-12-20 to 2026-01-18 UTC, restored with a 72 h pad each side. Buckets that differ inside the padded window (pad days only are expected): 12; unexplained: 0.

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
| 1 TB | backup at local measured rate | 1545.5 | 0.2 | yes | yes |
| 1 TB | backup at min(measured, 50% of 2 CKU egress) | 180.0 | 1.5 | no | yes |
| 1 TB | restore at local measured rate | 230.1 | 1.2 | no | yes |
| 1 TB | restore into Confluent at min(measured, 2 CKU ingress) | 120 | 2.3 | no | yes |
| 10 TB | backup at local measured rate | 1545.5 | 1.8 | no | yes |
| 10 TB | backup at min(measured, 50% of 2 CKU egress) | 180.0 | 15.4 | no | yes |
| 10 TB | restore at local measured rate | 230.1 | 12.1 | no | yes |
| 10 TB | restore into Confluent at min(measured, 2 CKU ingress) | 120 | 23.1 | no | yes |
| 50 TB | backup at local measured rate | 1545.5 | 9.0 | no | yes |
| 50 TB | backup at min(measured, 50% of 2 CKU egress) | 180.0 | 77.2 | no | no |
| 50 TB | restore at local measured rate | 230.1 | 60.4 | no | no |
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
| 10-seed | 1m 31s | PASS |
| 20-baseline | 1m 09s | PASS |
| 25-schema-export | 1.0s | PASS |
| 30-backup | 3m 21s | PASS |
| 35-verify-backup | 1m 50s | PASS |
| 38-seal | 31.0s | PASS |
| 45-archive-options | 2m 04s | PASS |
| 40-prune-gate | 5.0s | PASS |
| 50-retention-prune | 1m 28s | PASS |
| 60-audit-restore | 39.0s | PASS |
| 65-verify-restore | 12.0s | PASS |
| 70-full-restore | 9m 26s | PASS |
| 75-readable-archive | 2.0s | PASS |
| 80-report | 2.0s | PASS |
| 95-negative-tests | 1m 01s | PASS |
| 90-azurite | 6.0s | FAIL |

## Peak container resources

| Container | Peak CPU % | Peak memory |
|---|---|---|
| offload-kafka-backup-run-0f7283306533 | 169 | 5.651GiB |
| offload-kafka-backup-run-16f867264846 | 594 | 2.587GiB |
| offload-kafka-backup-run-8c101d593e21 | 540 | 1.9GiB |
| offload-kafka-backup-run-954e8f7bee3f | 167 | 3.506GiB |
| offload-kafka-backup-run-d6274ce91935 | 98 | 218.2MiB |
| offload-minio-1 | 156 | 2.688GiB |
| offload-source-kafka-1 | 245 | 5.902GiB |
| offload-source-schema-registry-1 | 74 | 386MiB |
| offload-target-kafka-1 | 223 | 4.338GiB |
| offload-target-schema-registry-1 | 7 | 320.3MiB |
| offload-verifier-run-075cf04f0494 | 323 | 2.126GiB |
| offload-verifier-run-094e924d3477 | 598 | 2.379GiB |
| offload-verifier-run-516d4453efba | 701 | 523.6MiB |
| offload-verifier-run-c671a8201589 | 406 | 4.558GiB |

## Environment

| Image | Reference | Architecture | Digest or ID |
|---|---|---|---|
| CP_KAFKA_IMAGE | confluentinc/cp-kafka:8.3.1 | arm64 | confluentinc/cp-kafka@sha256:0ad069035863aa1b090f4d9af47bfd2c08dc32864f3575d7d8579e3155c2586d |
| CP_SCHEMA_REGISTRY_IMAGE | confluentinc/cp-schema-registry:8.3.1 | arm64 | confluentinc/cp-schema-registry@sha256:f0cfd047a839c1ace54d93b92e3459f0d03dc3b5c9db1192a2246fd79b4f44c4 |
| CP_SERVER_IMAGE | confluentinc/cp-server:8.3.1 | arm64 | confluentinc/cp-server@sha256:62e3b04c4c88d03fbb484b9976e6145f751c2e908310f3f8b2db5ae7fcd3608f |
| KAFKA_BACKUP_IMAGE | kafka-backup:0.22.0-arm64 | arm64 | kafka-backup@sha256:ba7ddc11f4dec7968e21524e7f9246f34ac1a6cab553176bb6be22e3c3ad19f7 |
| MC_IMAGE | quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z | arm64 | minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727 |
| MINIO_IMAGE | quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z | arm64 | minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e |
| verifier | offload-verifier:local |  | sha256:90fb987fcc1a6c80738997157a34688009207a7f4673743d53e0a4452dd46ed7 |

Docker VM: 24 CPUs, 104 GiB; host Darwin arm64.

Every file in this directory is listed with its SHA-256 in `SHA256SUMS`.
