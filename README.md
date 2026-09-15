# Kafka Topic Offload: lower Kafka retention safely with verified backup and restore

[![smoke](https://github.com/osodevops/kafka-topic-offload-demo/actions/workflows/smoke.yml/badge.svg)](https://github.com/osodevops/kafka-topic-offload-demo/actions/workflows/smoke.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Confluent Platform 8.3.1](https://img.shields.io/badge/Confluent%20Platform-8.3.1-0074A2)](https://docs.confluent.io/platform/current/)
[![kafka-backup 0.22.0](https://img.shields.io/badge/kafka--backup-0.22.0-orange)](https://github.com/osodevops/kafka-backup)

**Move years of Apache Kafka topic history into cheap object storage, cut `retention.ms` on the live topic, and restore any time window later, with checksum level evidence at every step.**

Large Kafka topics that hold compliance data, IoT telemetry or audit trails often dominate the storage bill on Confluent Cloud and self managed clusters. Nobody wants to lower retention on regulated data on trust alone. This repository runs the whole offload on a local Confluent stack, at 1 GB in minutes or 100 GB in under an hour on a large workstation. It proves each step with gates that fail loudly:

```
seed -> baseline -> export schemas -> back up -> verify -> seal -> gate -> lower retention -> restore -> verify
```

- **Backup and restore:** [kafka-backup](https://github.com/osodevops/kafka-backup), open source and MIT licensed.
- **Kafka:** Confluent Platform 8.3.1 (KRaft) with Schema Registry.
- **Object storage:** MinIO with S3 object lock (WORM).
- **Verifier:** an independent Python verifier that never trusts the backup tool's own output.

## Contents

- [Who this is for](#who-this-is-for)
- [What it proves](#what-it-proves)
- [Results from the 100 GB reference run](#results-from-the-100-gb-reference-run)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [How to restore offloaded Kafka data](#how-to-restore-offloaded-kafka-data)
- [Cost](#cost)
- [FAQ](#faq)
- [Documentation](#documentation)
- [Repository layout](#repository-layout)

## Who this is for

- **Platform teams:** paying for terabytes of retained Kafka data they rarely read.
- **Data owners and compliance teams:** anyone who must keep records for years and prove they can be produced on request.
- **Confluent Cloud users:** looking for a cheaper home for cold topic history than cluster storage.
- **Kafka engineers:** anyone evaluating Kafka backup, archive to S3 or Azure Blob Storage, and point in time restore tools.

## What it proves

| Claim | How the demo proves it |
|---|---|
| The backup holds every record, byte for byte | `validate --deep` must print exactly `Result: VALID`. Then every stored object's SHA-256 is checked, and every record is decoded with an independent reader and compared per UTC day with a digest of the source taken before any change |
| Lowering `retention.ms` deletes exactly what was predicted, nothing newer | Kafka's segment deletion rule is applied to the real segment files first; the broker must then match the predicted log start offsets, surviving segments and reclaimed bytes exactly |
| No consumer is silently cut off | The prune gate blocks while any consumer group sits behind the new log start unless it is acknowledged; the affected group resumes with `auto.offset.reset=error` and gets `OffsetOutOfRange` |
| Deleted history can be restored on a different cluster, identical to the source | A month from nine months ago is restored from the sealed backup into a new topic and compared per day by digest; a full restore maps consumer group offsets |
| Schema IDs still mean the same schemas | Schemas are exported by ID, imported in IMPORT mode, and every ID found in the restored data is resolved on both registries |
| The archive is readable without the backup tool | A sealed segment is decoded with a 60 line Python reader of the documented binary format |
| The gates can fail | `make negative-tests` injects a data gap, a corrupted object, a schema ID mismatch, an unacknowledged consumer and an unpadded time window; each must fail its named gate |

## Results from the 100 GB reference run

Full report: [`evidence/reference-100g/report.md`](evidence/reference-100g/report.md). Every file is listed in `SHA256SUMS`, and [`NOTES.md`](evidence/reference-100g/NOTES.md) describes the run.

| Step | Result |
|---|---|
| Topic | 53,687,091 Avro records, 105 GB on the broker, one year of timestamps including 266,156 late arriving records |
| Backup | 834 objects, 43.3 GB stored (2.58x zstd), 73 seconds; every object checksum and every record verified against the baseline |
| Retention lowered to 30 days | 95.96 GB reclaimed, exactly as predicted to the byte; 48,990,007 records removed from the live topic, every one in the sealed backup |
| Audit restore | A month from nine months ago, 5,294,966 records in 41 seconds, identical to the source for every day; 88 of them recovered only because the window was padded |
| Full restore | 53,687,091 records in under 9 minutes, identical to the source, consumer group offsets mapped exactly |
| Negative tests | 5 of 5 failed their named gate as designed |

These are local, single node rates on a 32 core Mac. The [extrapolation](evidence/reference-100g/restore/extrapolation.json) caps them at Confluent Cloud's per CKU throughput guidance.

## Quick start

Requirements:

- **Tools:** Docker with Compose v2, `make`, `perl`, `python3` (standard library only), `git`, `shasum`.
- **Resources:** see the table below.

| Scale | Data | Docker memory | Free disk in the Docker VM | Wall clock (32 core Mac) |
|---|---|---|---|---|
| `smoke` | 1 GB | 12 GiB | 6 GiB | about 5 minutes |
| `10g` | 10 GB | 16 GiB | 35 GiB | not measured |
| `100g` | 100 GB | 24 GiB | 280 GiB | about 25 minutes, plus up to 30 if the prune gate has to wait |

```bash
git clone https://github.com/osodevops/kafka-topic-offload-demo.git
cd kafka-topic-offload-demo

make all SCALE=smoke        # every phase, each with its gates
make negative-tests         # five scenarios that must fail
cat "evidence/$(cat evidence/.current-run)/report.md"

make clean                  # stop the stack and delete its volumes (evidence is kept)
```

On arm64 hosts the first run builds kafka-backup 0.22.0 natively from its release tag (about 10 minutes), because the published image is amd64 only. On amd64, `KAFKA_BACKUP_IMAGE=osodevops/kafka-backup:v0.22.0` is used as is.

The [walkthrough](docs/walkthrough.md) runs every phase by hand and explains what to look at.

## How it works

```mermaid
flowchart LR
  subgraph source[Source cluster: cp-server]
    T[(device-telemetry<br/>1 year, 6 partitions)]
    SR1[Schema Registry]
  end
  subgraph storage[MinIO]
    B[(kafka-backups<br/>mutable)]
    S[(kafka-backups-sealed<br/>object lock)]
  end
  subgraph target[Target cluster: cp-kafka]
    A[(device-telemetry-audit)]
    F[(device-telemetry-full)]
    SR2[Schema Registry<br/>IMPORT mode]
  end
  V{{Python verifier<br/>baseline digests}}
  T -- kafka-backup --> B
  B -- verify, then copy --> S
  SR1 -- export by ID --> S
  S -- time window restore --> A
  S -- full restore + offsets --> F
  S -- import by ID --> SR2
  V -. compares .- T
  V -. compares .- B
  V -. compares .- A
  V -. compares .- F
```

| Phase | What happens | Key gates |
|---|---|---|
| `00-preflight` | Tools, Docker memory and disk, pinned images, native kafka-backup build, stack up, `environment.json` | `preflight.kafka_backup_native` |
| `10-seed` | One year of Avro device telemetry with 0.5% late records and midnight boundary records; a consumer group committed 60 days behind | `seed.watermarks` |
| `20-baseline` | One read of the source: digests per partition, per UTC day and per broker segment | `baseline.only_late_cohort_goes_backwards` |
| `25-schema-export` | Every schema ID the data uses, exported with subjects and versions | `schemas.exported` |
| `30-backup` | Snapshot backup to MinIO, timed, with S3 request counts, plus a comparison run at 4x larger segments | `backup.completed` |
| `35-verify-backup` | Strict `validate --deep`, then independent manifest, SHA-256 and record level verification | `backup.content_matches_source_baseline` |
| `38-seal` | Copy to an object lock bucket, apply retention, prove a sealed version cannot be deleted | `seal.worm_blocks_version_delete` |
| `40-prune-gate` | Backup and seal intact, topic unchanged, exact deletion prediction, consumer impact acknowledged | `prune_gate.*` |
| `50-retention-prune` | `retention.ms` lowered to 30 days; the broker compared with the prediction | `prune.bytes_reclaimed_match_prediction` |
| `60-audit-restore` | Schemas imported by ID; a padded month restored from the sealed copy to a new topic on another cluster | `restore.audit.batch_fits_message_max_bytes` |
| `65-verify-restore` | Restored records keyed by `x-original-offset` and compared with the baseline per day | `restore.audit.buckets_match_baseline` |
| `70-full-restore` | Everything restored with consumer group offsets; extrapolation to larger topics | `restore.full.consumer_group_offset_mapped` |
| `75-readable-archive` | A sealed segment decoded without kafka-backup | `archive.segment_readable_without_kafka_backup` |
| `80-report` | `report.md` and `SHA256SUMS` | |
| `95-negative-tests` | Five failure scenarios | `negative.*` |
| `90-azurite` | Azure Blob client path against Azurite, outside the verdict until kafka-backup supports the emulator | `azurite.backup` |

[How it works](docs/how-it-works.md) explains the Kafka mechanics behind the gates:
- exactly how `retention.ms` deletes segments;
- why one late or future timestamp pins a whole segment;
- how restored records are matched to their source offsets;
- the kafka-backup segment format.

## How to restore offloaded Kafka data

1. **Pick the time window** and pad it on both sides by the maximum lateness of your data (kafka-backup 0.22.0 selects segments by first and last record timestamp, so late records near the edges are otherwise skipped).
2. **Rehydrate first** if the objects are in an archive tier.
3. **Import schemas:** load the exported schemas into the restore registry in IMPORT mode so every schema ID is preserved.
4. **Restore into a new topic** with [`config/kafka-backup/audit-restore.yaml.tmpl`](config/kafka-backup/audit-restore.yaml.tmpl), which sets `retention.ms=-1` and fails if the topic already exists. For a full rollback with consumer group offsets, use [`full-restore.yaml.tmpl`](config/kafka-backup/full-restore.yaml.tmpl). Never restore into the live topic.
5. **Verify** with `verify-restore` and `schema-check` before anyone relies on the data.

The [Confluent Cloud runbook](docs/runbook-confluent-cloud.md) turns this into a production procedure, covering access, quotas, staged retention changes, immutable storage and drills.

## Cost

Measured at 100 GB: kafka-backup made **1.02 PUT requests per 128 MiB object**, so backing up 10 TB into Azure Blob Storage Hot costs about $0.38 in request charges in eastus. Storage then dominates:

| 10 TB logical, 2.58x compression | Hot | Cool | Cold | Archive |
|---|---|---|---|---|
| Storage per month (eastus, LRS) | $75 | $55 | $13 | $4 |

Small objects are what make archives expensive: 10 TB written as one object per 2 KB record would cost about $25,000 in write operations. See the [cost model](docs/cost-model.md). `tools/cost_model.py` pulls live prices for any Azure region.

## FAQ

**How do I reduce Kafka storage costs without losing data?**
Back up the topic to object storage, verify the backup independently, seal it, and only then lower `retention.ms`. This repository automates and gates each of those steps.

**Can I delete old data from a Kafka topic and restore it later?**
Yes. Kafka deletes whole segments once their newest record is older than `retention.ms`. The deleted range can be restored from the backup into a new topic, with original offsets and timestamps carried in record headers.

**How does Kafka `retention.ms` decide what to delete?**
A segment is deleted when `now - largestTimestamp > retention.ms`. Segments are checked oldest first, deletion stops at the first segment that must be kept, and never passes the high watermark. See [how it works](docs/how-it-works.md#1-what-kafka-deletes-when-retention-is-lowered).

**Do consumer group offsets survive a restore?**
Restored records get new offsets. kafka-backup stores each record's original offset in an `x-original-offset` header and resets consumer groups on the target through it. The demo proves the mapping record by record.

**What happens to Schema Registry schema IDs?**
Confluent wire format values embed a schema ID. The demo exports every ID the data uses and imports them in IMPORT mode, so the same ID resolves to the same schema after restore. A negative test shows silent mis-decoding when this is skipped.

**Does this work with Confluent Cloud?**
The mechanics are the same, but segment files are not visible on Confluent Cloud. Read the [runbook](docs/runbook-confluent-cloud.md) and [limitations](docs/limitations.md), and run a nonprod pilot first.

**Does it support Amazon S3 and Azure Blob Storage?**
kafka-backup supports S3, S3 compatible stores, Azure Blob Storage and Google Cloud Storage. The demo runs on MinIO (S3 API). The Azure path is scaffolded against Azurite, but the emulator is blocked in kafka-backup 0.22.0.

**Is kafka-backup free?**
Yes. [kafka-backup](https://github.com/osodevops/kafka-backup) is open source under the MIT licence.

## Documentation

- [Walkthrough](docs/walkthrough.md): run every phase step by step, with example output and how to inspect each result.
- [How it works](docs/how-it-works.md): retention mechanics, the digest, the backup segment format, the time window behaviour, schema IDs, evidence layout.
- [Runbook for Confluent Cloud](docs/runbook-confluent-cloud.md): the production procedure.
- [Cost model](docs/cost-model.md): storage, transactions and Data Out.
- [Limitations](docs/limitations.md): what a local run cannot prove, and kafka-backup 0.22.0 behaviour worked around here.

## Repository layout

```
compose/docker-compose.yml   source cp-server, target cp-kafka, two registries, MinIO, Azurite, tools
config/schemas/              Avro schemas for the generated telemetry
config/kafka-backup/         kafka-backup backup and restore config templates
scripts/                     lib.sh and one gated script per phase
tools/verifier/              Python evidence tooling: seed, baseline, verify, prune prediction, segment reader, report
tools/cost_model.py          cost tables from measured request counts and Azure retail prices
evidence/reference-100g/     committed evidence from the 100 GB run
docs/                        walkthrough, how it works, runbook, cost model, limitations
```

## Related projects

- [osodevops/kafka-backup](https://github.com/osodevops/kafka-backup): high performance Kafka backup and point in time restore.
- [osodevops/kafka-backup-demos](https://github.com/osodevops/kafka-backup-demos): shorter demos of individual kafka-backup features.
- [osodevops/kafka-backup-operator](https://github.com/osodevops/kafka-backup-operator): run kafka-backup on Kubernetes.

## License

MIT. Built by [OSO](https://oso.sh), specialists in Apache Kafka and event streaming.
