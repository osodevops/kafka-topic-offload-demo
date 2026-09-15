#!/usr/bin/env bash
# Build kafka-backup from its release tag for this host's architecture.
# The published osodevops/kafka-backup:v0.22.0 image is amd64 only; under emulation on
# arm64 its throughput numbers would be meaningless.
PHASE=build-kafka-backup
source "$(dirname "$0")/lib.sh"

step "Building $KAFKA_BACKUP_IMAGE from $KAFKA_BACKUP_TAG"
src="$KAFKA_BACKUP_SRC"
[[ "$src" = /* ]] || src="$REPO_ROOT/$src"
tmp=""
if [[ ! -d "$src/.git" ]]; then
  tmp="$(mktemp -d)"
  info "no local checkout at $src; cloning the tag"
  git clone --quiet --depth 1 --branch "$KAFKA_BACKUP_TAG" https://github.com/osodevops/kafka-backup.git "$tmp/kafka-backup"
  src="$tmp/kafka-backup"
fi
git -C "$src" rev-parse --verify --quiet "$KAFKA_BACKUP_TAG^{commit}" >/dev/null || fail "tag $KAFKA_BACKUP_TAG not found in $src"
commit="$(git -C "$src" rev-parse "$KAFKA_BACKUP_TAG^{commit}")"
info "commit $commit"

git -C "$src" archive --format=tar "$KAFKA_BACKUP_TAG" \
  | docker build --label "org.opencontainers.image.revision=$commit" \
      --label "org.opencontainers.image.version=$KAFKA_BACKUP_TAG" -t "$KAFKA_BACKUP_IMAGE" -
[[ -n "$tmp" ]] && rm -rf "$tmp"

version="$(docker run --rm "$KAFKA_BACKUP_IMAGE" --version)"
[[ "$version" == "kafka-backup ${KAFKA_BACKUP_TAG#v}" ]] || fail "unexpected version: $version"
ok "$version  $(docker image inspect --format '{{.Architecture}} {{.Id}}' "$KAFKA_BACKUP_IMAGE")"
