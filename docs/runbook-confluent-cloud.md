# Runbook: offload a topic from Confluent Cloud and lower its retention

This runbook turns the demo into a production procedure for one topic at a time. Every step has an owner, an artefact and a stop condition. Where the local demo relies on something Confluent Cloud does not expose, the Cloud equivalent is given.

Confirm Confluent Cloud limits and roles against current Confluent documentation at the time of the change. They are quoted here as planning guidance.

## 0. Preconditions (stop if any is missing)

- **Obligation and owner.** For each topic, record the retention obligation and a named data owner who signs off lowering retention. The obligation covers how long the data must be kept, in what form, and how quickly it must be produced on request.
- **Schema Registry.** Confirm the topic uses Confluent Cloud Schema Registry, and whether its format is Avro, JSON Schema or Protobuf.
- **Late data.** Establish whether late arriving records occur and the real maximum lateness, for example a site offline for a week. It sets the restore window pad.
- **Timestamp sanity.** Check each partition's newest record timestamp. One timestamp in the future keeps a whole segment, and everything behind it, on the cluster (see `how-it-works.md`, section 1).
- **kafka-backup version.** Prefer a release that records the minimum and maximum record timestamp per segment **before any production archive is written**. 0.22.0 records the first and last record's timestamps, manifests keep those bounds permanently, and time window restores from them can skip late records silently. With 0.22.0, always pad restore windows by the maximum lateness.
- **Rehearsal.** The demo has passed at `SCALE=100g` with the version you will run.

## 1. Access

- **Backup service account** (one per environment):
  - `DeveloperRead` on the topic prefix being offloaded.
  - `DeveloperRead` on the consumer group prefix kafka-backup uses.
  - Describe and Read on the consumer groups to be captured in the snapshot. Without Describe on a group, the snapshot omits it without an error.
- **Inventory principal:** able to list and describe every consumer group on the cluster. A principal that cannot see a group gets a shorter list, not an error.
- **Readers that do not commit:** the inventory must also cover consumers that never commit offsets, such as Flink jobs with offsets in checkpoints, `assign()` based readers, and sinks that keep offsets externally. Ask their owners directly.
- **Credentials:** store them in the secret store used by the runner (Key Vault, Kubernetes secret). Never put them in YAML files or repositories.

## 2. Throughput and cost guard rails

- **Egress guidance:** Dedicated clusters are sized at about 180 MB/s egress per CKU, so about 360 MB/s for a 2 CKU cluster. A backup shares that with production consumers.
- **Client quota:** set one on the backup service account, starting at no more than half the egress guidance. Alert on client throttling (`client_limit_milliseconds` in the Metrics API) and on consumer lag for production groups during the run.
- **Throttle by records:** kafka-backup's `rate_limit_bytes_per_sec` has no effect in 0.22.0, so throttle with the client quota and, if needed, `rate_limit_records_per_sec`.
- **Data Out:** every byte read is billed as Data Out, plus Azure Private Link processing where used. The initial full read is a one off cost; see `cost-model.md`.
- **Restores:** writing back into Confluent Cloud is bounded by ingress guidance (about 60 MB/s per CKU). A 2 TB topic at 120 MB/s takes about 4.6 hours before any other load.

## 3. Schemas

- **Export:** export every schema ID the topic's data uses, with its subjects and versions (`verifier schema-export`, or the Schema Registry API `GET /schemas/ids/{id}` and `/schemas/ids/{id}/versions`).
- **Store:** put the export next to the backup and record its SHA-256 in the change record.
- **Restore registry:** use a scratch registry, either self-managed or a separate Cloud context, switched to IMPORT mode so IDs are preserved. Schema Linking is the managed alternative.
- **Check:** after any restore, prove that every distinct ID in the restored data resolves to the same schema (`verifier schema-check`).

## 4. Storage

- **Account:** an Azure storage account in the same region as the cluster, reached through a private endpoint, with redundancy chosen to meet the obligation.
- **Layout:** one `backup_id` per topic, under one prefix per environment.
- **Write mutable, verify, then seal:**
  1. Back up into an ordinary container.
  2. Verify (section 7).
  3. Copy to an immutable container, or apply a time based immutability policy to the verified blobs.
  4. Lock the policy once the retention period is agreed.
- **Tiering:** apply lifecycle rules only to `*/topics/*` segment blobs, so `manifest.json` and the group snapshot stay online. Minimum storage periods before early deletion charges: Cool 30 days, Cold 90 days, Archive 180 days.
- **Archive tier:** blobs must be rehydrated before a restore can read them, which takes hours. Include rehydration time in the restore time you promise.
- **Never tier a backup that is still being written.** Incremental backup sets are pruned with `kafka-backup prune`, not lifecycle rules.
- **Archive retention:** decide how long the archive itself keeps data, separately from the topic:
  - `kafka-backup prune --older-than <duration>` or `--before <instant>`, and `--max-total-bytes`, planning first and acting with `--execute`;
  - or `backup.retention` (`max_age`, `max_total_bytes`, `keep_segments`) applied during each run.

  Pruned ranges are recorded in the manifest, so the archive stays valid and states what it no longer holds. Age is taken from each segment's upload time where recorded, so an initial bulk archive of old history ages as one cohort: use an explicit `--before` cutoff for that data rather than a relative age.
- **Do not point both lifecycle rules and `prune` at the same objects.** Pick one owner for deletion, and keep `manifest.json` out of any lifecycle rule.

## 5. Backup

Use the demo's `config/kafka-backup/backup.yaml.tmpl` as the starting point, changing only the source (bootstrap, SASL_SSL credentials from the secret store) and the storage backend:

- `stop_at_current_offsets: true` for the initial snapshot, which fixes the archive at the high watermarks taken at start-up.
- **Decide where the archive starts.** To archive only from a chosen instant, resolve it to offsets per partition with `kafka-get-offsets --time <epoch ms>` and pass them as `start_offset: !specific`. Record the instant and the offsets in the change record. There is no end bound; a run always reads to the high watermark.
- `require_topic_configs: true` and `consumer_group_snapshot: true`.
- `segment_max_bytes: 134217728`, with `segment_max_interval_ms` large for the snapshot. Larger segments mean fewer write transactions; see the request counts in the demo report and `cost-model.md`.
- Record the start and end time, the high watermark per partition at start, and the kafka-backup version and image digest in the change record.

## 6. Change record contents

One change record per topic. Attach:

- `environment.json` equivalent: tool versions, image digests, config files with secrets removed.
- The high watermark and log start offset per partition before the backup.
- Output of `kafka-backup validate --deep`.
- Output of the independent verification.
- The manifest SHA-256 anchor.
- The schema export and its SHA-256.
- The consumer inventory, with the owner acknowledgement for each group behind the cut.
- The staged retention plan and its approvals.

## 7. Verification at scale

A full second read of a multi terabyte topic is paid Data Out, so on Cloud verification is layered:

1. **Validate:** `kafka-backup validate --deep` must print exactly `Result: VALID`, with zero missing, corrupted, gap, pruned and missing topic counts. The exit code alone is not enough.
2. **Coverage:** manifest segment ranges are contiguous per partition, from the log start recorded before the backup to the high watermark recorded at start.
3. **Integrity:** the SHA-256 of every stored object equals the manifest. This reads storage, not Kafka, so there is no Data Out.
4. **Anchor:** store the SHA-256 of `manifest.json` in the change record.
5. **Content samples:** restore at least three sampled day windows (oldest, middle, newest) into a scratch topic, padded by the maximum lateness. Compare per day counts and digests with the same windows read directly from the source before retention changes. The demo's `verify-restore` does this against its baseline.
6. **Full content check:** where the obligation demands it, run a full content verification from storage (`verify-backup` decodes every object without touching Kafka), compared with a baseline taken during the backup window.

## 8. Lowering retention (staged)

- **Gate:** the backup is verified and sealed; the anchor matches; the manifest covers the current log start to the high watermark; every group behind the new cut is acknowledged by its owner.
- **Stage down:** from `-1` to 180 days, then 90 days, then 30 days. Each step is a separate approved change, applied only after the previous step settles.
- **Segment rolling on Cloud:** Confluent Cloud's topic configuration reference enforces a minimum `segment.ms` of 14,400,000 (4 hours) and defaults it to 7 days. A segment on a quiet partition can therefore hold up to 7 days of data, and retention only takes effect once the newest record in that segment is older than the cut. Expect retained bytes to fall in steps, not smoothly.
- **Watch after each step:**
  - retained bytes per topic in the Metrics API, which should fall to the expected level within hours;
  - consumer lag and errors for every production group;
  - the log start offset per partition (`kafka-get-offsets --time -2`).
- **Exact prediction is not possible on Cloud,** because segment files are not visible. Treat the Metrics API retained bytes after each stage as the evidence, and confirm the new log start offsets are inside the sealed backup's coverage.
- **Stop condition:** any consumer error, unexpected lag, or log start offset beyond the backup's coverage stops the sequence. Retention can be raised again, but deleted segments cannot be recovered from Kafka, only from the backup.

## 9. Steady state

- **Incremental backups:** run continuously or on a schedule that keeps the backup ahead of retention with margin. The backup's newest offset must always exceed the offset retention will delete next.
- **Alerts:** on backup lag against the retention horizon, not only on job failure.
- **Quarterly drill** (named owner): restore a sampled window into a `-restored` topic, verify it, record the time taken and the rehydration time, and file the evidence.
- **Point in time rollback:** restore into a new `-restored` topic and repoint consumers. Never restore into the live topic.

## 10. Pilot before production

Before the first production topic, run a small nonprod pilot against a real Azure storage account. It proves what the local demo cannot:

- Workload Identity or managed identity authentication;
- the private endpoint path;
- immutability policy behaviour;
- tier transitions and rehydration;
- real request and Data Out charges on the invoice.
