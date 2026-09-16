# Reference run at 100 GB: notes

- **Run:** `100g-20260916T051309Z`, started 2026-09-16 05:13 UTC.
- **Host:** 32 core Mac running Docker Desktop, Docker VM with 24 CPUs and 104 GiB.
- **kafka-backup:** 0.22.0, built natively for arm64 from its release tag.
- **Result:** `report.md` passes 118 of 118 gates, including phase `45-archive-options`. All five negative tests fail their named gates as designed. The Azurite phase fails as expected on kafka-backup 0.22.0 and is outside the verdict.
- **Order:** every phase ran once, in order, with no resumes. The host was kept awake for the whole run.

## Which code ran

`environment.json` records commit `95a13bc` with uncommitted changes. The phase 45 work was committed as `b68de1c` while this run was in progress. The scripts, verifier, compose file, configs, Makefile and `.env.example` that ran are identical to `b68de1c`: `git diff b68de1c -- scripts tools compose config Makefile .env.example` is empty. Later commits change only documentation and this evidence.

## Reading the timings

- **`30-backup` (201 seconds):** the primary backup took 72.2 seconds. The rest is the comparison backup at 4x larger segments, plus a 12 second settle before each of the six MinIO request counter readings. `backup/run-*.json` has both.
- **`45-archive-options` (124 seconds):** covers three steps, each gated:
  - a 90 day archive of 13,237,908 records into 210 objects, verified record by record against the source baseline;
  - a 30 day restore as at an instant, 4,412,442 records, identical on every interior day;
  - archive retention taking the comparison archive from 43.3 GB to 21.6 GB, with 6 pruned ranges recorded and `validate` still `VALID`.
- **`40-prune-gate` (5 seconds):** no segment's newest timestamp fell near the cut, so the gate had nothing to wait for. Earlier runs waited up to 30 minutes for an unambiguous prediction.
- **`70-full-restore` (566 seconds):** the restore itself took 485 seconds; the rest is verifying all 53,687,091 records against the baseline.
- **Total:** the phases add up to about 23 minutes.
- **Throughput:** local, single node, no network. See `restore/extrapolation.json` and `docs/limitations.md` before comparing with Confluent Cloud.
- **Container peaks:** `docker stats` answers slowly under heavy disk I/O, so samples are sparse in the busiest phases, and the peaks in `report.md` are a floor.

## Checking the evidence

```bash
cd evidence/reference-100g
shasum -a 256 -c SHA256SUMS
```

`SHA256SUMS` was regenerated when this copy was made, so it covers every file here, including `timings.tsv`, the report log and this file.
