# How it works

This demo answers one question with evidence: can a regulated topic's history be moved into object storage, the live topic's retention lowered, and any part of that history brought back byte for byte, with nobody taking it on trust?

```
seed -> baseline -> export schemas -> back up -> verify -> seal -> gate -> lower retention -> restore -> verify
```

Every phase prints `GATE <name>: PASS|FAIL` lines and exits non-zero on the first failure. `report.md` is built only from those lines and the JSON files beside them.

## 1. What Kafka deletes when retention is lowered

Kafka never deletes records one by one. A partition is a sequence of segment files, and retention removes whole segments.

- **Rolling:** a segment closes when it reaches `segment.bytes`, or when the timestamps in it span more than `segment.ms`.
- **Deletion rule:** a segment is deletable when `now - largestTimestamp > retention.ms`.
  - `largestTimestamp` is the newest record timestamp in the segment, not the file time.
  - The broker walks segments oldest first and stops at the first one it cannot delete.
  - It never deletes past the high watermark.
  - If every segment qualifies, the active segment is rolled and deleted as well, emptying the partition.
- **Log start offset:** the new log start is the base offset of the first kept segment. A consumer committed below it gets `OffsetOutOfRange`, and with `auto.offset.reset=latest` or `earliest` it skips or replays data without an error.

Because the rule is deterministic, `40-prune-gate` predicts the result exactly before anything changes:

- Base offsets and sizes come from the broker's segment files.
- Each segment's largest timestamp comes from the baseline scan, cross-checked against the last entry of the broker's `.timeindex` file.
- The prediction is only accepted when no segment's largest timestamp is within 15 minutes of the cut, so the broker's check time cannot change the answer.

`50-retention-prune` then requires the broker to match the prediction exactly: log start offset per partition, surviving segments, and reclaimed bytes as reported by `kafka-log-dirs`.

Two consequences matter in production:

- **One bad timestamp pins a segment.** A record stamped in the future, or a device with a wrong clock, keeps its whole segment, and every newer segment behind it, until that timestamp ages out. Check the maximum timestamp per partition before relying on a retention change.
- **Old records outlive the cut.** Records older than the cut stay on the broker while they share a segment with newer ones. The report shows the oldest record still on each partition.

## 2. The dataset

`device-telemetry` mirrors energy device telemetry:

- **Topic:** 6 partitions, `cleanup.policy=delete`, `retention.ms=-1`, CreateTime.
- **Records:** about 2 KB of Avro each, keyed by one of 40,000 device IDs, spread over one year that ends at `SEED_END_TS`.
- **Partitioning:** keys are placed with Kafka's murmur2 partitioner. The seed checks its implementation against Kafka's own test vectors.
- **Segments:** `segment.bytes` is set so each partition has about 60 size rolled segments of about 6 days each.
- **Late cohort:** 0.5% of records carry a CreateTime 1 to 72 hours behind their partition's clock, as delayed device uploads do.
- **Boundaries:** records sit at exactly UTC midnight and one millisecond before it, to catch off-by-one window handling.
- **Schemas:** two versions of the schema. The last 30% of records use v2. Two unrelated schemas are registered first, so the telemetry schemas hold IDs 3 and 4 rather than 1 and 2.
- **Consumer group:** `telemetry-analytics` is committed 60 days behind the newest record.
- **Determinism:** everything is derived from `SEED`, so a rerun produces identical records.

## 3. What "the same data" means

`20-baseline` reads the source once, before any change, and records:

- **Per partition:** log start offset, high watermark, contiguity, and the number of timestamps that go backwards. That number must equal the late cohort exactly.
- **Per UTC day bucket:** record count, minimum and maximum timestamp, and a running SHA-256, in offset order, over each record's partition, source offset, timestamp, key, value and user headers.
- **Per broker segment:** record count, offsets, and minimum and maximum timestamp.

Every later comparison uses the same digest. Bucket boundaries are UTC midnight inclusive to the next midnight exclusive.

## 4. The backup

kafka-backup reads each partition from the log start to the high watermark seen at start-up, then exits (`stop_at_current_offsets`). It writes objects to storage as follows:

```
backups/<backup_id>/manifest.json
backups/<backup_id>/consumer-groups-snapshot.json
backups/<backup_id>/topics/<topic>/partition=<p>/segment-<start offset, 20 digits>.bin.zst
```

It also captures the topic's configuration (`require_topic_configs: true` fails the run if it cannot) and the committed offsets of every consumer group on the topic.

**Offset headers.** Each archived record gets two headers:

- `x-original-offset`: its source offset.
- `x-original-timestamp`: its source timestamp.

Both are 8 byte little endian signed integers. Restored records keep them, and that is how a restored record is matched to its source record, and how consumer group offsets are translated.

### The KBAK segment format

A segment object is a documented binary format, not gzip or a Kafka log file:

| Part | Layout |
|---|---|
| Header, 32 bytes | `KBAK`, version (1), compression (0 none, 1 zstd, 2 lz4), 2 reserved bytes, record count (u64 LE), first offset (i64 LE), last offset (i64 LE) |
| Payload | Compressed as a whole. Decompressed: records of `length u32`, `timestamp i64`, `offset i64`, `key_len i32` + key, `value_len i32` + value, `header_count u16`, then per header `key_len u16` + key, `value_len i32` + value. Lengths of -1 mean null. All little endian |
| Footer, 8 bytes | CRC32 (u32 LE) of every preceding byte, then `BKAE` |

`tools/verifier/verifier/segments.py` reads it in about 60 lines of Python with no kafka-backup code. `75-readable-archive` decodes a sealed segment to JSON lines with the exported schemas.

## 5. Proving the backup

`kafka-backup validate --deep` is necessary but not sufficient:

- It prints `Result: VALID (with N recorded data gaps ...)` and exits 0 when the backup is incomplete. The demo requires the exact line `Result: VALID` and zero missing, corrupted, gap, pruned and missing topic counts.
- It does not check the per segment SHA-256 that the manifest records.

`35-verify-backup` therefore also checks the backup independently:

- **Manifest:** segment ranges are contiguous from the log start to the high watermark, with no gaps or pruned ranges, and record counts add up.
- **Objects:** every stored object's SHA-256 and size equal the manifest. The digest is over the stored bytes, so `mc cat <object> | shasum -a 256` gives the same value.
- **Content:** every record is decoded with the independent reader. Per partition and day bucket, count, minimum and maximum timestamp and running SHA-256 must equal the source baseline.
- **Headers:** `x-original-offset` and `x-original-timestamp` decode to the record's own offset and timestamp.
- **Groups:** the consumer group snapshot equals the committed offsets on the source.
- **Anchor:** the SHA-256 of `manifest.json` is stored in the evidence directory. Every later phase checks the manifest against it.

## 6. Write mutable, validate, then seal

During a run kafka-backup rewrites `manifest.json`, the offsets database and the group snapshot. It therefore cannot write directly into an immutable location. The demo follows the pattern production should use:

1. Back up into an ordinary bucket.
2. Verify it (phase 35).
3. Copy it into a bucket with object lock, and apply retention to every object (`38-seal`).
4. Prove that a sealed object version cannot be deleted.
5. Check the sealed copy against the manifest anchor.

Restores read from the sealed copy.

## 7. The prune gate

`40-prune-gate` blocks unless all of these hold:

- The backup was verified and the manifest anchor matches in both buckets.
- The sealed manifest covers every partition from the current log start to the high watermark, with no gaps.
- The topic has not changed since the baseline (same segments, sizes and watermarks).
- The prediction is unambiguous and empties no partition.
- The newest data survives the new retention.
- Every consumer group committed below the predicted cut is named in `ACK_CONSUMER_IMPACT`.

`50-retention-prune` reruns the gate, requires the same deleted segments as the reviewed prediction, and only then changes `retention.ms`.

## 8. Restoring, and the time window defect in 0.22.0

A restore writes records to a new topic, so offsets are new. Verification keys every restored record by `x-original-offset` and recomputes the baseline digest, which proves the restored set equals the source set for each day bucket.

**The defect.** kafka-backup 0.22.0 stores each segment's `start_timestamp` and `end_timestamp` as the first and last record's timestamps, not the minimum and maximum. A time window restore selects segments by those bounds, then filters records by timestamp. A late record whose timestamp is inside the window, but whose segment starts after the window ends (or ends before it starts), is skipped silently.

The demo handles it in four ways:

- **Measurement:** `35-verify-backup` lists every record outside its segment's manifest bounds (`time-window-hazards-<backup_id>.json`) and reports how many segments have bounds that are not min and max.
- **Workaround:** `60-audit-restore` pads the window by the maximum lateness (72 hours) on both sides, then `65-verify-restore` compares the 30 audit days exactly. It prints how many records in the audit month were recovered only because of the pad.
- **Negative test:** negative test 5 restores a single day without padding. The bucket gate must fail, and every missing record must be one the manifest bounds predict.
- **Upstream fix:** kafka-backup needs to record the minimum and maximum timestamp per segment. Until a release does, pad every time window. Manifests written by 0.22.0 keep the first and last record bounds permanently.

**Other restore settings**

| Setting | Why |
|---|---|
| `produce_batch_size: 250` | 0.22.0 does not split produce batches by size. At the default 1,000 records of 2 KB, a batch exceeds the 1 MB `message.max.bytes`. The phase checks 250 times the largest record against the target's limit first |
| `topic_config_overrides: retention.ms "-1"` and `existing_topic_config_policy: fail` | A scratch topic never ages data out, and a restore never appends to or reconfigures an existing topic |
| `auto_consumer_groups: true` with `consumer_group_strategy: header-based` | In the full restore, the snapshot's groups are reset on the target after the data is written. The gate reads the target record at `telemetry-analytics`'s new committed offset and requires its `x-original-offset` to equal the source committed offset |

## 9. Choosing what to archive and what to bring back

**A backup is a snapshot.** With `stop_at_current_offsets`, kafka-backup records each partition's high watermark at start-up and stops there. The archive is therefore "the topic as at that moment", and the manifest holds the offsets that prove it.

**Starting from an instant.** Backup selection is by offset, not time: `start_offset` takes `earliest`, `latest`, or specific offsets per partition. Kafka's own offsets-for-times lookup turns an instant into those offsets:

```bash
kafka-get-offsets --bootstrap-server ... --topic <topic> --time <epoch ms>
```

It returns the first offset at or after the instant on each partition, which becomes the archive's start. Two details matter:

- A late record can carry a timestamp earlier than the instant while sitting after that offset, so the archive can begin slightly before it. The demo allows for the configured maximum lateness.
- There is no end bound in 0.22.0. A backup always runs to the high watermark, which is what makes it a snapshot.

**Bringing back a range.** `time_window_start` and `time_window_end` are epoch milliseconds, inclusive at both ends. Restore selects segments whose manifest bounds overlap the window, then filters records by timestamp. Because those bounds are the first and last record rather than the minimum and maximum (section 8), a window that is not padded can miss late records at its edges. Padding the start is always safe. Padding the end is not, because it would also admit records newer than the instant, so an "as at" restore is exact except for late records that straddle the end. The demo reports how many those are.

**Ageing the archive.** The archive has retention of its own, independent of the topic:

- `backup.retention` with `max_age` (`30d`, `12h`), `max_total_bytes`, and `keep_segments` (never fewer than this many newest segments per partition), applied during a run;
- `kafka-backup prune --older-than <duration> | --before <instant>` and `--max-total-bytes`, which plans by default and acts with `--execute`.

Pruned ranges are written into the manifest, so `validate` reports them as deliberate rather than as loss, and `describe` shows what the archive still holds. Age is taken from each segment's upload time where recorded, falling back to its last record timestamp, so a bulk import of old history ages as one cohort.

## 10. Schema IDs

Confluent wire format values start with a zero byte and a 4 byte schema ID. A restored value only means the same thing if its ID resolves to the same schema on the registry it is read with.

- **Export:** on Confluent Cloud the managed registry has no `_schemas` topic to back up. `25-schema-export` exports every ID the data uses, with its subjects and versions, next to the backup.
- **Import:** `60-audit-restore` imports them into the restore registry in IMPORT mode, so IDs are preserved.
- **Check:** `65-verify-restore` resolves every distinct ID found in the restored data on both registries and requires identical schemas. It also decodes a sample of restored records and checks that `eventTime` equals the record's CreateTime.
- **Negative test:** negative test 3 reads the same restored data through a registry that assigned its own IDs. Nothing errors at the Kafka level, and both schema gates must fail.

## 11. Evidence layout

```
evidence/<scale>-<UTC timestamp>/
  environment.json            versions, image digests, repo SHA, seed, host
  logs/<phase>.log            full output, including every GATE line
  timings.tsv, stats/         wall clock per phase, docker stats samples
  seed/ baseline/ schemas/    dataset description and baseline digests
  backup/ s3/ seal/           manifest summaries, validate output, verification, request counts, retention records
  prune/                      predictions, consumer inventory, broker sizes before and after, check
  consumers/ restore/         OffsetOutOfRange evidence, restore logs, verification, extrapolation
  archive/                    a decoded segment as JSON lines
  negative/                   negative test logs
  report.md, SHA256SUMS
```
