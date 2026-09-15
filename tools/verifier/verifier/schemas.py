"""Schema Registry export and import by ID.

Confluent Cloud's managed registry has no _schemas topic in the customer cluster, so a
backup of the data alone cannot bring schemas back. Every schema ID the data uses is
exported with its subjects and versions, and imported into the restore registry in
IMPORT mode so each ID means the same schema on both sides.
"""
import argparse
import hashlib
import json
import os
import time
from pathlib import Path

import requests

from .common import gate, log, read_json, run_dir, write_json

HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def _get(url: str):
    r = requests.get(url, timeout=30)
    return r.status_code, (r.json() if r.content else None)


def _send(method: str, url: str, body: dict):
    r = requests.request(method, url, headers=HEADERS, data=json.dumps(body), timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"{method} {url}: HTTP {r.status_code} {r.text}")
    return r.json()


def wait_ready(sr: str, timeout_s: int = 180) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            if requests.get(f"{sr}/subjects", timeout=5).status_code == 200:
                return
        except requests.ConnectionError:
            pass
        if time.monotonic() > deadline:
            raise RuntimeError(f"schema registry {sr} not ready")
        time.sleep(2)


def canonical(schema_str: str) -> str:
    return json.dumps(json.loads(schema_str), sort_keys=True, separators=(",", ":"))


def export_ids(sr: str, ids) -> dict:
    out = {}
    for sid in sorted(ids):
        code, body = _get(f"{sr}/schemas/ids/{sid}")
        if code != 200:
            raise RuntimeError(f"schema id {sid} not found on {sr}: {body}")
        _, versions = _get(f"{sr}/schemas/ids/{sid}/versions")
        out[str(sid)] = {
            "id": sid,
            "schema": body["schema"],
            "schemaType": body.get("schemaType", "AVRO"),
            "references": body.get("references", []),
            "subjects": sorted(({"subject": v["subject"], "version": v["version"]} for v in versions),
                               key=lambda v: (v["subject"], v["version"])),
        }
    return out


def upload(path: Path, key: str) -> dict:
    import boto3

    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"])
    bucket = os.environ.get("S3_BUCKET", "kafka-backups")
    body = path.read_bytes()
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    return {"bucket": bucket, "key": key, "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}


def import_ids(sr: str, export: dict) -> dict:
    """Import in IMPORT mode. Existing identical IDs are skipped; a different schema on an ID fails."""
    todo, skipped = [], []
    for entry in export.values():
        code, body = _get(f"{sr}/schemas/ids/{entry['id']}")
        if code == 200:
            if canonical(body["schema"]) != canonical(entry["schema"]):
                raise RuntimeError(f"schema id {entry['id']} already holds a different schema on {sr}")
            skipped.append(entry["id"])
            continue
        for sv in entry["subjects"]:
            todo.append((sv["subject"], sv["version"], entry))
    if not todo:
        return {"imported": [], "skipped": skipped}

    _send("PUT", f"{sr}/mode", {"mode": "IMPORT"})
    imported = []
    try:
        for subject, version, entry in sorted(todo, key=lambda t: (t[0], t[1])):
            body = {"schema": entry["schema"], "schemaType": entry["schemaType"], "id": entry["id"], "version": version}
            if entry["references"]:
                body["references"] = entry["references"]
            got = _send("POST", f"{sr}/subjects/{subject}/versions", body)
            if got.get("id") != entry["id"]:
                raise RuntimeError(f"{subject} v{version}: registry returned id {got.get('id')}, expected {entry['id']}")
            imported.append({"subject": subject, "version": version, "id": entry["id"]})
    finally:
        _send("PUT", f"{sr}/mode", {"mode": "READWRITE"})
    return {"imported": imported, "skipped": skipped}


def check_ids(source_sr: str, target_sr: str, ids) -> dict:
    results = {}
    for sid in sorted(ids):
        s_code, s_body = _get(f"{source_sr}/schemas/ids/{sid}")
        t_code, t_body = _get(f"{target_sr}/schemas/ids/{sid}")
        same = s_code == 200 and t_code == 200 and canonical(s_body["schema"]) == canonical(t_body["schema"])
        results[str(sid)] = {"source": s_code, "target": t_code, "identical": same}
    return results


def main(cmd: str, argv) -> int:
    ap = argparse.ArgumentParser(prog=f"verifier {cmd}")
    ap.add_argument("--topic", default=os.environ.get("TOPIC", "device-telemetry"))
    ap.add_argument("--source-sr", default=os.environ.get("SOURCE_SR"))
    ap.add_argument("--target-sr", default=os.environ.get("TARGET_SR"))
    ap.add_argument("--ids", help="comma separated schema IDs; default: IDs seen in the baseline")
    ap.add_argument("--label", default="restore")
    ap.add_argument("--no-upload", action="store_true")
    args = ap.parse_args(argv)
    rd = run_dir()
    export_path = rd / "schemas" / f"{args.topic}-schemas-export.json"

    if args.ids:
        ids = [int(i) for i in args.ids.split(",") if i]
    elif (rd / "baseline" / f"{args.topic}.json").exists():
        ids = read_json(rd / "baseline" / f"{args.topic}.json")["schema_ids"]
    else:
        ids = []

    if cmd == "schema-export":
        wait_ready(args.source_sr)
        export = export_ids(args.source_sr, ids)
        write_json(export_path, export)
        meta = {"ids": ids, "local": str(export_path),
                "sha256": hashlib.sha256(export_path.read_bytes()).hexdigest()}
        if not args.no_upload:
            meta["object"] = upload(export_path, f"schema-exports/{args.topic}/schemas-export.json")
        write_json(rd / "schemas" / f"{args.topic}-export-meta.json", meta)
        ok = gate("schemas.exported", bool(ids) and len(export) == len(ids), f"ids {ids}")
        return 0 if ok else 1

    if cmd == "schema-import":
        wait_ready(args.target_sr)
        result = import_ids(args.target_sr, read_json(export_path))
        write_json(rd / "schemas" / f"{args.topic}-import-{args.label}.json", result)
        log(f"imported {result}")
        checks = check_ids(args.source_sr, args.target_sr, ids)
        ok = gate("schemas.imported_by_id", all(c["identical"] for c in checks.values()), json.dumps(checks))
        return 0 if ok else 1

    if cmd == "schema-check":
        checks = check_ids(args.source_sr, args.target_sr, ids)
        write_json(rd / "schemas" / f"{args.topic}-check-{args.label}.json", checks)
        ok = gate(f"schemas.ids_resolve_identically.{args.label}", bool(checks) and all(c["identical"] for c in checks.values()),
                  json.dumps(checks))
        return 0 if ok else 1

    if cmd == "schema-naive-register":
        # Negative test: a registry that assigns its own IDs. Unrelated schemas are
        # registered first so the data's IDs point at the wrong schemas.
        wait_ready(args.target_sr)
        export = read_json(export_path)
        for n in range(max(ids) if ids else 0):
            fake = {"type": "record", "name": f"Unrelated{n}", "namespace": "demo.negative",
                    "fields": [{"name": "f", "type": "string"}]}
            _send("POST", f"{args.target_sr}/subjects/negative-unrelated-{n}-value/versions", {"schema": json.dumps(fake)})
        for entry in export.values():
            for sv in entry["subjects"]:
                _send("POST", f"{args.target_sr}/subjects/{sv['subject']}/versions", {"schema": entry["schema"]})
        log("registered schemas without IMPORT mode")
        return 0

    raise SystemExit(f"unknown command {cmd}")
