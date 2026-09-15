# Limitations

What this demo proves, what it does not, and the kafka-backup 0.22.0 behaviour it works around.

## The local stack is not Confluent Cloud

- **Single node:** one KRaft broker per cluster, replication factor 1. There is no replication, no rack placement, no tiered storage, and no network between client and broker.
- **Segments are visible locally:** the exact prune prediction reads the source broker's log directory, mounted read-only into the verifier. Confluent Cloud does not expose segment files, sizes or roll settings. On Cloud the equivalent is:
  - the backup manifest's coverage;
  - staged retention changes;
  - retained bytes from the Metrics API.

  See `runbook-confluent-cloud.md`.
- **Throughput is an upper bound:** local rates come from one host with local disks and no network. Confluent Cloud applies per CKU egress and ingress guidance and client quotas, and Azure adds network latency and Private Link processing. The extrapolation table shows both local rates and quota capped rates, but only a quota test on the real cluster is evidence.
- **Retention timing:** the local brokers check retention every 30 seconds and delete files after one second, so changes are visible within a minute. On Cloud the defaults are longer, and removal from tiered storage is asynchronous.
- **Licence:** `cp-server` runs a 30 day trial without a licence. Set `CP_SERVER_IMAGE=confluentinc/cp-kafka:8.3.1` to avoid it; the Confluent specific environment settings are then ignored with warnings.

## Data

- **Synthetic payload:** records are generated, half random bytes and half repetitive text. The zstd compression ratio says nothing about your real payloads, so measure it on a real sample.
- **Idealised lateness:** the late cohort is uniform between 1 and 72 hours. Real device lateness can be longer, for example a site offline for a week. The restore pad must cover the real maximum, which only your data can confirm.
- **Committing consumers only:** the consumer inventory sees groups with committed offsets. Readers that use `assign()` without committing, Flink jobs that keep offsets in checkpoints, and Connect sinks with external offsets are invisible to it. Find their owners another way.

## Object storage

- **Archived MinIO image:** the MinIO community edition repository is archived. The demo pins the last community image (`RELEASE.2025-09-07T16-13-09Z`), which kafka-backup's own large scale tests also used. Do not base anything long lived on it.
- **Garage not wired in:** a maintained alternative (`dxflrs/garage`) is not yet a compose profile.
- **Governance mode:** the seal uses MinIO object lock in `GOVERNANCE` mode, which a sufficiently privileged user can bypass. Production should use Azure immutable storage with a locked time based retention policy, or the equivalent of `COMPLIANCE` mode, applied only after verification.
- **Azurite:** kafka-backup 0.22.0 cannot reach Azurite. Its Azure backend never enables emulator mode or plain HTTP, so the first write fails with `Azure PUT failed ... HTTP error: builder error`, and `make azurite` fails on its first gate until kafka-backup supports the emulator.
- **What Azurite cannot prove, even once it works:** Azurite proves only that the Azure client path works with shared key authentication. It does not cover:
  - Workload Identity;
  - Private Endpoints;
  - immutability policies;
  - access tiers and rehydration;
  - Azure transaction pricing.

  A small smoke test against a real nonprod storage account covers those.

## kafka-backup 0.22.0 behaviour and the workarounds used

| Behaviour | Effect | Workaround in this demo | Upstream |
|---|---|---|---|
| Segment `start_timestamp` and `end_timestamp` are the first and last record, not min and max | Time window restores silently skip late records near the window edges | Pad windows by the maximum lateness; verify per day against the baseline; negative test 5 shows the loss | Record min and max per segment. Needed before production archives, because existing manifests keep the first and last bounds |
| Produce batches are not split by size (default 1,000 records) | With 2 KB records a batch exceeds `message.max.bytes` | `produce_batch_size: 250`, with a preflight gate against the target limit | Size aware batching |
| `rate_limit_bytes_per_sec` is accepted but never applied | A byte throttle has no effect | Throttle with `rate_limit_records_per_sec` and a Confluent client quota | Apply the byte limit |
| `validate` exits 0 and prints `Result: VALID (with N recorded data gaps ...)` for an incomplete backup | Automation that checks the exit code accepts a backup with holes | Require the exact line `Result: VALID` and zero counts; negative test 1 | Non-zero exit on gaps |
| `validate --deep` does not check the per segment SHA-256 the manifest records | Deep validation is weaker than its documentation says | Independent SHA-256 of every object; negative test 2 | Verify the recorded digest |
| `manifest.json`, the offsets database and the group snapshot are rewritten during a run | A WORM location blocks the backup | Write mutable, verify, then copy and seal | Document the pattern |
| Each backup run overwrites the captured topic configs | A backup taken after lowering retention carries the short retention into restores; negative test 1 shows `retention.ms=2592000000` captured | Take the first full backup before any retention change; restore with `topic_config_overrides` | Keep or version captured configs |
| Azure backend has no emulator or plain HTTP switch; `azure://container@account` parses to an empty container name | No local Azure testing; the documented URL form fails | Use `--config` files only; the Azurite phase is outside the verdict | Emulator and `allow_http` support; fix URL parsing |
| Externally tagged options such as `start_offset` need the YAML tag form (`start_offset: !specific`) | A map form fails to parse | Negative test 1 uses the tag form | Document it |
| Published Docker image is amd64 only | Emulated on arm64 hosts, which distorts timing | `make build-kafka-backup` builds the release tag natively | Publish multi-arch images |

## What the negative tests do and do not cover

The five negative tests show that the gates can fail:

1. a gap left by retention before the first backup;
2. a flipped byte in one stored object;
3. a registry that assigns its own schema IDs;
4. an unacknowledged consumer below the cut;
5. an unpadded time window.

They do not cover:

- a broker crash mid backup;
- storage outages or throttling;
- partial uploads;
- clock skew between hosts;
- a restore interrupted and resumed.
