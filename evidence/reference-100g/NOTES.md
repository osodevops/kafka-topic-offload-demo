# Reference run at 100 GB: notes

- **Run:** `100g-20260915T164407Z`, started 2026-09-15 16:44 UTC.
- **Commit:** `5dd61dd`, clean working tree (`environment.json` records `repo_dirty: false`).
- **Host:** 32 core Mac running Docker Desktop, with a Docker VM of 24 CPUs and 104 GiB.
- **kafka-backup:** 0.22.0, built natively for arm64 from its release tag.
- **Result:** `report.md` passes 93 of 93 gates. All five negative tests fail their named gates as designed. The Azurite phase fails as expected on kafka-backup 0.22.0 and sits outside the verdict.

Every phase ran once, in order, with no resumes.

## Reading the timings

- **`40-prune-gate` (49 minutes):**
  - One segment's newest timestamp was within 15 minutes of the 30 day cut, so the gate waited for an answer that cannot depend on when the broker runs its retention check. The planned wait was 1,779 seconds.
  - During the wait the Mac went into idle sleep: the macOS power log shows sleep from 17:09 UTC, with dark wakes until after 17:31 UTC. That paused the Docker VM and stretched the phase to 2,950 seconds.
  - Correctness is unaffected. The prediction was recomputed from wall clock time after the wait, and `50-retention-prune` recomputed it again and required the same deleted segments before changing retention.
  - The rest of the run was kept awake with `caffeinate`.
- **`30-backup` (198 seconds):**
  - The primary backup took 72.7 seconds; `backup/run-*.json` has both backups.
  - The rest is the comparison backup at 4x larger segments, plus a 12 second settle before each of the six MinIO request counter readings.
- **`38-seal` (29 seconds):** the server side copy of 837 objects into the object lock bucket, applying retention, the delete refusal test and re-hashing the sealed copy.
- **Total:** without the prune gate's wait, the phases add up to about 25 minutes.
- **Throughput:** local, single node, no network. See `restore/extrapolation.json` and `docs/limitations.md` before comparing with Confluent Cloud.
- **Container peaks:** `docker stats` answers slowly under heavy disk I/O, so samples are sparse in the busiest phases, and the peaks in `report.md` are a floor.

## Checking the evidence

```bash
cd evidence/reference-100g
shasum -a 256 -c SHA256SUMS
```

`SHA256SUMS` was regenerated when this copy was made, so it covers every file here, including `timings.tsv`, the report log and this file.
