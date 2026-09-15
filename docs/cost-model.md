# Cost model: offloading Kafka topic history to Azure Blob Storage

This page covers what it costs to move a Kafka topic's history from Confluent Cloud into Azure Blob Storage, and why object size decides whether transaction charges matter.

Numbers come from two sources:

- **Measured:** request counts, objects and bytes from a run's evidence directory.
- **Published prices:** the public [Azure Retail Prices API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices).

`tools/cost_model.py` combines them for any region:

```bash
python3 tools/cost_model.py evidence/reference-100g --region eastus --sizes-tb 1,10,50
python3 tools/cost_model.py evidence/reference-100g --region westeurope --data-out-per-gb 0.05
python3 tools/cost_model.py evidence/reference-100g --offline     # embedded eastus snapshot
```

The tables below use eastus, LRS, USD, retrieved 2026-09-15. Confluent Cloud prices (storage, Data Out, Private Link) depend on your agreement, so take them from your invoice.

## Azure Blob Storage prices used (eastus, LRS, USD)

| Tier | Storage per GB per month | Write operations per 10,000 | Read operations per 10,000 | Data retrieval per GB | Minimum storage duration |
|---|---|---|---|---|---|
| Hot | 0.0208 (first 50 TB) | 0.05 | 0.004 | none | none |
| Cool | 0.0152 | 0.10 | 0.01 | 0.01 | 30 days |
| Cold | 0.0036 | 0.18 | 0.10 | 0.03 | 90 days |
| Archive | 0.00099 | 0.10 | 5.00 | 0.02 (standard), 0.10 (high priority) | 180 days |

Notes on how Azure bills these:

- **Lists** are billed at the write operation rate.
- **Tier changes:** moving a blob to a cooler tier is billed as a write operation at the destination tier's rate.
- **Early deletion:** deleting or moving a blob before its minimum duration is charged for the remaining days.
- **Archive reads:** Archive blobs must be rehydrated before they can be read. Standard rehydration can take up to 15 hours.

## Transaction cost follows object count, not data volume

kafka-backup writes one object per segment, sized by `segment_max_bytes`, so a topic's object count is roughly its logical size divided by the segment size.

**Measured at 100 GB** (`evidence/reference-100g`):

- **128 MiB segments:** 111.6 GB logical became 834 objects of 127.6 MiB logical each, using 847 PUT requests (1.02 per object) plus 7 GETs and 1 HEAD.
- **512 MiB segments:** the same data became 210 objects with 223 PUTs (1.06 per object).
- **Overhead:** manifest and offset store rewrites add a handful of requests per run, not per record.

Scaled up at 128 MiB:

| Logical size | Objects | Backup into Hot, all requests | Move to Cool | Move to Cold | Move to Archive |
|---|---|---|---|---|---|
| 1 TB | about 7,500 | $0.04 | $0.07 | $0.13 | $0.07 |
| 10 TB | about 74,700 | $0.38 | $0.75 | $1.34 | $0.75 |
| 50 TB | about 373,600 | $1.90 | $3.74 | $6.72 | $3.74 |

The same 10 TB written as one object per record (about 2 KB each) would be 5 billion objects, and $25,000 in Hot write operations alone. Any design that writes small objects (per record, per batch, or per short time interval) makes transactions the dominant cost. Check object size first when an archive project reports runaway storage transaction charges.

## Storage

Stored bytes are logical bytes divided by the compression ratio. The demo measured 2.58x with zstd, but on synthetic data, so treat it as a placeholder until you measure a sample of the real topic.

| Logical size | Stored at 2.58x | Hot per month | Cool per month | Cold per month | Archive per month |
|---|---|---|---|---|---|
| 1 TB | about 360 GiB | $7.51 | $5.49 | $1.30 | $0.36 |
| 10 TB | about 3,610 GiB | $75.08 | $54.87 | $13.00 | $3.57 |
| 50 TB | about 18,050 GiB | $375.42 | $274.34 | $64.98 | $17.87 |

The minimum storage charge applies if data is deleted or moved early: 30 days for Cool, 90 for Cold, 180 for Archive.

## Reading it back

Restoring everything costs read operations plus data retrieval:

| Logical size | Hot | Cool | Cold | Archive, standard | Archive, high priority |
|---|---|---|---|---|---|
| 1 TB | under $1 | $3.62 | $10.90 | $10.96 | $73.46 |
| 10 TB | under $1 | $36.17 | $109.04 | $109.55 | $734.57 |
| 50 TB | under $1 | $180.86 | $545.20 | $547.77 | $3,672.84 |

- **Archive timing:** Archive adds rehydration time before the restore can start: up to 15 hours at standard priority, usually under an hour at high priority for objects under 10 GB.
- **Partial restores:** a sampled window reads only the objects covering it, a small fraction of these figures.
- **Restores into Confluent Cloud** also incur Data In and storage on the cluster.

## The one off read from Confluent Cloud

The initial backup reads every byte of the topic once. That read is billed as Data Out, plus Azure Private Link data processing where the client connects privately:

```
logical GB x Data Out rate per GB  +  logical GB x Private Link rate per GB
```

Confluent's public pricing page lists Data In and Data Out between about $0.014 and $0.05 per GB, depending on cluster type and region. At those ends, a 10 TB read is roughly $140 to $500, and 50 TB is roughly $700 to $2,500, before Private Link. Use the rate on your invoice with `--data-out-per-gb` and `--private-link-per-gb`.

Incremental backups afterwards read only new data.

## Putting it together

For each topic, compare:

- **Keeping it on Confluent Cloud:** the retained bytes billed on the cluster each month (from your invoice).
- **Offloading it:**
  - the one off Data Out read;
  - the one off write and tiering transactions (small at 128 MiB objects);
  - storage per month at the chosen tier, respecting its minimum duration;
  - the cost and time of the restores your retention obligation requires, including rehydration.
