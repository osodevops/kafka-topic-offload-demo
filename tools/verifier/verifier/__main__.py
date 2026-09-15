"""Entry point: python -m verifier <command> [options]."""
import importlib
import sys

COMMANDS = {
    "seed": ("seed", "create the topic and produce the deterministic dataset"),
    "baseline": ("baseline", "per partition, per day and per segment digest of a source topic"),
    "schema-export": ("schemas", "export every schema ID used by the data, with subjects and versions"),
    "schema-import": ("schemas", "import exported schemas by ID into a registry in IMPORT mode"),
    "schema-check": ("schemas", "prove schema IDs resolve to the same schema on two registries"),
    "schema-naive-register": ("schemas", "register schemas without IMPORT (negative test)"),
    "consumer-sim": ("consumer_sim", "resume a consumer group from its committed offsets"),
    "consumer-inventory": ("consumer_sim", "list groups whose committed offsets fall below a cut"),
    "verify-backup": ("backup_checks", "manifest coverage, contiguity and per object SHA-256"),
    "manifest-summary": ("backup_checks", "segment count, sizes and throughput for a backup"),
    "s3-requests": ("backup_checks", "MinIO request counters by API, or the difference of two"),
    "prune-predict": ("prune", "predict exactly which segments retention will delete"),
    "prune-check": ("prune", "compare the broker after the prune with the prediction"),
    "verify-restore": ("verify_restore", "compare a restored topic with the baseline"),
    "decode-segment": ("segments", "read a KBAK segment from object storage and decode it"),
    "corrupt-object": ("segments", "negative test: flip one byte in a stored segment"),
    "report": ("report", "assemble report.md from the evidence directory"),
}


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print("usage: python -m verifier <command> [options]\n", file=sys.stderr)
        for name, (_, desc) in COMMANDS.items():
            print(f"  {name:<22} {desc}", file=sys.stderr)
        return 2
    cmd = sys.argv[1]
    module = importlib.import_module(f"verifier.{COMMANDS[cmd][0]}")
    return module.main(cmd, sys.argv[2:])


if __name__ == "__main__":
    sys.exit(main())
