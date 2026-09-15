#!/usr/bin/env python3
"""Offload cost model built from a run's measured request counts and Azure Blob Storage retail prices.

    python3 tools/cost_model.py evidence/<run> [--region eastus] [--sizes-tb 1,10,50] [--data-out-per-gb 0.05]

Measured inputs, read from the run directory:
  backup/summary-<topic>.json        logical bytes, objects, compression ratio
  backup/run-primary.json            segment size used
  s3/requests-backup-primary.json    S3 requests made by the backup, by API

Azure prices are fetched live from the public Azure Retail Prices API for --region (block blobs,
LRS, USD). --offline uses the eastus snapshot below instead. Confluent Cloud Data Out and Private
Link prices are account specific, so pass them in. Standard library only.
"""
import argparse
import datetime
import json
import urllib.parse
import urllib.request
from pathlib import Path

GIB = 1024**3
TIERS = ("hot", "cool", "cold", "archive")
MIN_DAYS = {"hot": 0, "cool": 30, "cold": 90, "archive": 180}

# eastus, General Block Blob v2, LRS, USD, Azure Retail Prices API, retrieved 2026-09-15.
# Hot storage is the first 50 TB band.
EASTUS_SNAPSHOT = {
    "hot":     {"store_gb_month": 0.0208,  "write_10k": 0.05, "read_10k": 0.004, "retrieval_gb": 0.0,  "list_10k": 0.05},
    "cool":    {"store_gb_month": 0.0152,  "write_10k": 0.10, "read_10k": 0.01,  "retrieval_gb": 0.01},
    "cold":    {"store_gb_month": 0.0036,  "write_10k": 0.18, "read_10k": 0.10,  "retrieval_gb": 0.03},
    "archive": {"store_gb_month": 0.00099, "write_10k": 0.10, "read_10k": 5.00,  "retrieval_gb": 0.02},
}

# How S3 API calls map onto Azure billing classes (Azure does not charge for deletes).
WRITE_APIS = {"putobject", "putobjectpart", "newmultipartupload", "completemultipartupload", "copyobject", "copyobjectpart",
              "putobjectretention", "putobjecttagging"}
LIST_APIS = {"listobjectsv1", "listobjectsv2", "listobjectversions", "listbuckets", "listmultipartuploads"}
READ_APIS = {"getobject", "headobject", "getobjectattributes", "getobjectretention", "headbucket", "getbucketlocation"}


def fetch_prices(region: str) -> dict:
    flt = (f"serviceName eq 'Storage' and armRegionName eq '{region}' "
           "and productName eq 'General Block Blob v2' and priceType eq 'Consumption'")
    url = "https://prices.azure.com/api/retail/prices?currencyCode=USD&$filter=" + urllib.parse.quote(flt)
    items = []
    while url:
        with urllib.request.urlopen(url, timeout=60) as resp:
            page = json.load(resp)
        items += page.get("Items", [])
        url = page.get("NextPageLink")

    prices = {t: {} for t in TIERS}
    for item in items:
        sku, meter = item["skuName"], item["meterName"]
        tier = sku.split()[0].lower()
        if tier not in prices or not sku.endswith(" LRS") or "Priority" in meter or item["tierMinimumUnits"] != 0:
            continue
        p, price = prices[tier], item["retailPrice"]
        if "Data Stored" in meter:
            p["store_gb_month"] = price
        elif "List and Create" in meter:
            p["list_10k"] = price
        elif "Write Operations" in meter:
            p["write_10k"] = price
        elif "Read Operations" in meter:
            p["read_10k"] = price
        elif "Data Retrieval" in meter:
            p["retrieval_gb"] = price
    for tier, p in prices.items():
        p.setdefault("retrieval_gb", 0.0)
        missing = {"store_gb_month", "write_10k", "read_10k"} - set(p)
        if missing:
            raise SystemExit(f"no {tier} {sorted(missing)} price for region '{region}'; check the region name")
    prices["hot"].setdefault("list_10k", prices["hot"]["write_10k"])
    return prices


def usd(v):
    return f"${v:,.2f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run", help="evidence/<run> directory")
    ap.add_argument("--topic", default="device-telemetry")
    ap.add_argument("--region", default="eastus", help="Azure region name, for example eastus, westeurope, uksouth")
    ap.add_argument("--offline", action="store_true", help="use the embedded eastus snapshot instead of the live API")
    ap.add_argument("--sizes-tb", default="1,10,50", help="logical topic sizes in TB (10^12 bytes)")
    ap.add_argument("--compression-ratio", type=float, help="override the measured ratio (synthetic data)")
    ap.add_argument("--data-out-per-gb", type=float, help="Confluent Cloud Data Out, USD per GB, from your invoice")
    ap.add_argument("--private-link-per-gb", type=float, help="Private Link data processing, USD per GB")
    ap.add_argument("--out", help="write the markdown here as well as to stdout")
    args = ap.parse_args()

    if args.offline:
        prices, source = EASTUS_SNAPSHOT, "eastus snapshot retrieved 2026-09-15 (offline)"
    else:
        prices = fetch_prices(args.region)
        source = f"{args.region}, Azure Retail Prices API, retrieved {datetime.date.today().isoformat()}"

    rd = Path(args.run)
    summary = json.loads((rd / "backup" / f"summary-{args.topic}.json").read_text())
    requests = json.loads((rd / "s3" / "requests-backup-primary.json").read_text())
    run = json.loads((rd / "backup" / "run-primary.json").read_text())

    objects = summary["segments"]
    logical_per_object = summary["uncompressed_bytes"] / objects
    ratio = args.compression_ratio or summary["compression_ratio"]
    writes = sum(v for k, v in requests.items() if k in WRITE_APIS)
    lists = sum(v for k, v in requests.items() if k in LIST_APIS)
    reads = sum(v for k, v in requests.items() if k in READ_APIS)
    other = {k: v for k, v in requests.items() if k not in WRITE_APIS | LIST_APIS | READ_APIS}
    per_object = {"writes": writes / objects, "lists": lists / objects, "reads": reads / objects}

    L = ["# Offload cost model", ""]
    L.append(f"Measured in `{rd.name}`: {objects} objects at `segment_max_bytes` {run['segment_max_bytes'] // 1048576} MiB, "
             f"{logical_per_object / 1048576:.1f} MiB of logical data per object, compression {ratio}x"
             f"{' (overridden)' if args.compression_ratio else ' (synthetic data: measure on a real sample)'}.")
    L.append(f"S3 requests during the backup: {writes:,} write class, {lists:,} list, {reads:,} read"
             f"{f', other {other}' if other else ''}; {per_object['writes']:.2f} writes per object.")
    L.append(f"Azure Blob Storage prices: block blobs, LRS, USD, {source}.")
    L.append("")

    for size_tb in (float(s) for s in args.sizes_tb.split(",")):
        logical = size_tb * 1e12
        n_objects = logical / logical_per_object
        stored_gib = logical / ratio / GIB
        hot = prices["hot"]
        initial = n_objects * (per_object["writes"] * hot["write_10k"] + per_object["lists"] * hot["list_10k"]
                               + per_object["reads"] * hot["read_10k"]) / 1e4
        L.append(f"## {size_tb:g} TB logical")
        L.append("")
        L.append(f"About {n_objects:,.0f} objects, {stored_gib:,.0f} GiB stored.")
        L.append("")
        L.append(f"Initial backup into Hot, all request charges: **{usd(initial)}** (one off).")
        L.append("")
        L.append("| Tier | Move from Hot (one off) | Storage per month | Minimum duration | Minimum storage charge | Read everything back |")
        L.append("|---|---|---|---|---|---|")
        for tier in TIERS:
            p = prices[tier]
            tiering = 0.0 if tier == "hot" else n_objects * p["write_10k"] / 1e4
            monthly = stored_gib * p["store_gb_month"]
            minimum = monthly * MIN_DAYS[tier] / 30
            restore = n_objects * p["read_10k"] / 1e4 + stored_gib * p["retrieval_gb"]
            L.append(f"| {tier} | {usd(tiering)} | {usd(monthly)} | {f'{MIN_DAYS[tier]} days' if MIN_DAYS[tier] else 'none'} "
                     f"| {usd(minimum) if minimum else 'n/a'} | {usd(restore)} |")
        L.append("")
        if args.data_out_per_gb is not None:
            gb = logical / 1e9
            out = gb * args.data_out_per_gb + (gb * args.private_link_per_gb if args.private_link_per_gb else 0)
            L.append(f"Confluent Cloud Data Out for the initial read: {gb:,.0f} GB x {usd(args.data_out_per_gb)}"
                     f"{f' + Private Link {usd(args.private_link_per_gb)}/GB' if args.private_link_per_gb else ''} = **{usd(out)}** (one off).")
        else:
            L.append("Confluent Cloud Data Out for the initial read: logical GB x the Data Out rate on your invoice "
                     "(pass `--data-out-per-gb`), plus Private Link data processing where used.")
        L.append("")

    text = "\n".join(L)
    print(text)
    if args.out:
        Path(args.out).write_text(text + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
