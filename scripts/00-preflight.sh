#!/usr/bin/env bash
# Phase 00: host tools and resources, pinned images, the stack, and a new run directory.
PHASE=00-preflight
source "$(dirname "$0")/lib.sh"

RUN_ID="${SCALE}-$(date -u +%Y%m%dT%H%M%SZ)"
export RUN_ID
RUN_DIR="$EVIDENCE_ROOT/$RUN_ID"
mkdir -p "$RUN_DIR/logs" "$RUN_DIR/configs"
echo "$RUN_ID" > "$EVIDENCE_ROOT/.current-run"
start_phase

case "$SCALE" in
  # Peak is during the full restore: source after the prune, primary, comparison and sealed
  # backups (about 0.39x logical each), and the restored topic (about 1.1x logical).
  smoke) need_disk_gb=6;   need_mem_gb=12 ;;
  10g)   need_disk_gb=35;  need_mem_gb=16 ;;
  100g)  need_disk_gb=280; need_mem_gb=24 ;;
  *) fail "unknown SCALE '$SCALE' (smoke, 10g or 100g)" ;;
esac

step "Host tools"
for t in docker perl python3 git shasum; do command -v "$t" >/dev/null || fail "$t is required"; done
compose_version="$(docker compose version --short)" || fail "docker compose v2 is required"
gate_pass preflight.tools "docker $(docker version --format '{{.Server.Version}}'), compose $compose_version"

step "Docker resources"
read -r ncpu mem_bytes arch <<<"$(docker info --format '{{.NCPU}} {{.MemTotal}} {{.Architecture}}')"
mem_gb=$(( mem_bytes / 1024 / 1024 / 1024 ))
gate preflight.docker_memory "$ncpu CPUs, $mem_gb GiB for the Docker VM (need $need_mem_gb GiB at $SCALE)" [ "$mem_gb" -ge "$need_mem_gb" ]
docker image inspect "$CP_KAFKA_IMAGE" >/dev/null 2>&1 || docker pull -q "$CP_KAFKA_IMAGE"
free_kb="$(docker run --rm --entrypoint df "$CP_KAFKA_IMAGE" -Pk / | awk 'NR==2 {print $4}')"
free_gb=$(( free_kb / 1024 / 1024 ))
gate preflight.docker_disk_free "$free_gb GiB free inside the Docker VM (need $need_disk_gb GiB at $SCALE)" [ "$free_gb" -ge "$need_disk_gb" ]

step "Images"
for var in CP_SERVER_IMAGE CP_KAFKA_IMAGE CP_SCHEMA_REGISTRY_IMAGE MINIO_IMAGE MC_IMAGE; do
  docker image inspect "${!var}" >/dev/null 2>&1 || docker pull -q "${!var}"
done
if ! docker image inspect "$KAFKA_BACKUP_IMAGE" >/dev/null 2>&1; then
  docker pull -q "$KAFKA_BACKUP_IMAGE" 2>/dev/null || "$REPO_ROOT/scripts/build-kafka-backup.sh"
fi
kb_version="$(docker run --rm "$KAFKA_BACKUP_IMAGE" --version)"
gate preflight.kafka_backup_version "$kb_version ($KAFKA_BACKUP_IMAGE)" [ "$kb_version" = "kafka-backup ${KAFKA_BACKUP_TAG#v}" ]
host_arch="$arch"; [[ "$host_arch" == aarch64 ]] && host_arch=arm64; [[ "$host_arch" == x86_64 ]] && host_arch=amd64
kb_arch="$(docker image inspect --format '{{.Architecture}}' "$KAFKA_BACKUP_IMAGE")"
gate preflight.kafka_backup_native "image $kb_arch on a $host_arch Docker VM, no emulation" [ "$kb_arch" = "$host_arch" ]
dc --profile tools build -q verifier
docker run --rm --entrypoint cat offload-verifier:local /app/pip-freeze.txt > "$RUN_DIR/verifier-pip-freeze.txt"

step "Stack"
dc up -d --wait source-kafka source-schema-registry target-kafka target-schema-registry minio
dc up --exit-code-from minio-setup minio-setup
gate_pass preflight.stack_healthy "brokers, registries and MinIO up; buckets $S3_BUCKET and $S3_SEALED_BUCKET (object lock)"

step "Environment record"
export REPO_ROOT
python3 - "$RUN_DIR/environment.json" <<'PY'
import json, os, platform, subprocess, sys, time

def sh(*args):
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=120).stdout.strip()
    except Exception as e:  # recorded, not fatal
        return f"error: {e}"

images = {}
for var in ["CP_SERVER_IMAGE", "CP_KAFKA_IMAGE", "CP_SCHEMA_REGISTRY_IMAGE", "MINIO_IMAGE", "MC_IMAGE", "KAFKA_BACKUP_IMAGE"]:
    ref = os.environ[var]
    images[var] = {
        "ref": ref,
        "id": sh("docker", "image", "inspect", "--format", "{{.Id}}", ref),
        "repo_digests": json.loads(sh("docker", "image", "inspect", "--format", "{{json .RepoDigests}}", ref) or "[]"),
        "architecture": sh("docker", "image", "inspect", "--format", "{{.Architecture}}", ref),
    }
images["verifier"] = {"ref": "offload-verifier:local", "id": sh("docker", "image", "inspect", "--format", "{{.Id}}", "offload-verifier:local")}
repo = os.environ["REPO_ROOT"]
record = {
    "run_id": os.environ["RUN_ID"],
    "started_at": int(time.time() * 1000),
    "scale": os.environ["SCALE"],
    "seed": os.environ["SEED"],
    "topic": os.environ["TOPIC"],
    "partitions": int(os.environ["PARTITIONS"]),
    "retention_after_prune_ms": int(os.environ["RETENTION_AFTER_PRUNE_MS"]),
    "repo_git_sha": sh("git", "-C", repo, "rev-parse", "--verify", "-q", "HEAD") or "no commit",
    "repo_dirty": bool(sh("git", "-C", repo, "status", "--porcelain")),
    "kafka_backup": {"version": sh("docker", "run", "--rm", os.environ["KAFKA_BACKUP_IMAGE"], "--version"),
                     "tag": os.environ["KAFKA_BACKUP_TAG"],
                     "revision_label": sh("docker", "image", "inspect", "--format",
                                          '{{index .Config.Labels "org.opencontainers.image.revision"}}', os.environ["KAFKA_BACKUP_IMAGE"])},
    "images": images,
    "docker": json.loads(sh("docker", "info", "--format", "{{json .}}") or "{}").get("ServerVersion"),
    "docker_vm": {"cpus": sh("docker", "info", "--format", "{{.NCPU}}"), "mem_bytes": sh("docker", "info", "--format", "{{.MemTotal}}")},
    "compose": sh("docker", "compose", "version", "--short"),
    "host": {"system": platform.system(), "release": platform.release(), "machine": platform.machine()},
}
with open(sys.argv[1], "w") as f:
    json.dump(record, f, indent=2, sort_keys=True)
PY
export REPO_ROOT
ok "run $RUN_ID recorded in evidence/$RUN_ID/environment.json"
