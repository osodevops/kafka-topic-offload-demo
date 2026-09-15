#!/usr/bin/env bash
# Shared helpers for the phase scripts. Source it; do not execute it.
#
# Every gate prints one line, "GATE <name>: PASS|FAIL - detail", to the phase log.
# The Python verifier prints the same format, so report.md is built from the logs alone.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/compose/docker-compose.yml"
ENV_FILE="$REPO_ROOT/.env"
EVIDENCE_ROOT="$REPO_ROOT/evidence"

[[ -f "$ENV_FILE" ]] || cp "$REPO_ROOT/.env.example" "$ENV_FILE"

# Load .env without overriding anything already set (so `make all SCALE=100g` wins).
while IFS= read -r _line || [[ -n "$_line" ]]; do
  _line="${_line%%#*}"
  [[ "$_line" =~ ^[[:space:]]*([A-Z_][A-Z0-9_]*)=(.*)$ ]] || continue
  _name="${BASH_REMATCH[1]}"
  _value="${BASH_REMATCH[2]}"
  _value="${_value%"${_value##*[![:space:]]}"}"
  [[ -n "${!_name+x}" ]] || export "$_name=$_value"
done < "$ENV_FILE"
unset _line _name _value

PHASE="${PHASE:-$(basename "${BASH_SOURCE[1]:-$0}" .sh)}"

if [[ -z "${RUN_ID:-}" && -f "$EVIDENCE_ROOT/.current-run" ]]; then
  RUN_ID="$(cat "$EVIDENCE_ROOT/.current-run")"
fi
export RUN_ID="${RUN_ID:-}"
RUN_DIR="$EVIDENCE_ROOT/${RUN_ID}"

BOLD=$'\033[1m'; GREEN=$'\033[32m'; RED=$'\033[31m'; BLUE=$'\033[34m'; RESET=$'\033[0m'
[[ -t 1 ]] || { BOLD=""; GREEN=""; RED=""; BLUE=""; RESET=""; }

step()    { printf '\n%s==> %s%s\n' "$BOLD$BLUE" "$*" "$RESET"; }
info()    { printf '    %s\n' "$*"; }
ok()      { printf '    %s✔ %s%s\n' "$GREEN" "$*" "$RESET"; }
fail()    { printf '    %s✘ %s%s\n' "$RED" "$*" "$RESET" >&2; exit 1; }

gate_pass() { printf 'GATE %s: PASS - %s\n' "$1" "${2:-}"; }
gate_fail() { printf 'GATE %s: FAIL - %s\n' "$1" "${2:-}"; exit 1; }
# gate NAME "detail" test-args...   e.g. gate backup.count "42 records" [ "$a" -eq "$b" ]
gate() {
  local name="$1" detail="$2"; shift 2
  if "$@"; then gate_pass "$name" "$detail"; else gate_fail "$name" "$detail"; fi
}

require_run() {
  [[ -n "$RUN_ID" && -d "$RUN_DIR" ]] || fail "no current run; start with: make preflight SCALE=$SCALE"
}

# Tee this phase's output into the run's log and record its wall clock time.
start_phase() {
  require_run
  mkdir -p "$RUN_DIR/logs"
  exec > >(tee -a "$RUN_DIR/logs/$PHASE.log") 2>&1
  PHASE_STARTED=$(date +%s)
  trap _phase_verdict EXIT
  step "$PHASE  (run $RUN_ID, scale $SCALE)"
}

_phase_verdict() {
  local rc=$? secs=$(( $(date +%s) - PHASE_STARTED ))
  stop_stats_sampler || true
  printf '%s\t%s\t%s\n' "$PHASE" "$secs" "$([[ $rc -eq 0 ]] && echo PASS || echo FAIL)" >> "$RUN_DIR/timings.tsv"
  if [[ $rc -eq 0 ]]; then
    printf '\n%sPHASE %s: PASS (%ss)%s\n' "$GREEN$BOLD" "$PHASE" "$secs" "$RESET"
  else
    printf '\n%sPHASE %s: FAIL (exit %s after %ss)%s\n' "$RED$BOLD" "$PHASE" "$rc" "$secs" "$RESET"
  fi
  sleep 0.2  # let tee flush
}

dc() { docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"; }

# Run the Python verifier in its container against this run's evidence directory.
VERIFIER_BUILT=""
verifier() {
  # Build once per script so a phase run on its own never uses stale verifier code.
  if [[ -z "$VERIFIER_BUILT" ]]; then dc --profile tools build -q verifier >/dev/null; VERIFIER_BUILT=1; fi
  # Run as the calling user: on Linux, files a root container writes into the bind-mounted
  # evidence directory would otherwise be unwritable for the phase scripts on the host.
  dc --profile tools run --rm -T --user "$(id -u):$(id -g)" -e HOME=/tmp \
    -e RUN_DIR="/evidence/$RUN_ID${RUN_SUBDIR:-}" -e TOPIC -e PARTITIONS -e SCALE -e SEED \
    -e LATE_MAX_HOURS -e ACK_CONSUMER_IMPACT -e RETENTION_AFTER_PRUNE_MS verifier "$@"
}

# Strict reading of `kafka-backup validate` output. validate prints
# "Result: VALID (with N recorded data gaps ...)" and exits 0 when the backup is
# incomplete, so its exit code alone proves nothing.
check_validate_output() {  # check_validate_output FILE [gate-prefix]
  local out="$1" prefix="${2:-backup}" label name
  gate "$prefix.validate_result_exactly_valid" "$(grep '^Result:' "$out" || echo 'no Result line')" grep -qx 'Result: VALID' "$out"
  for label in "Segments Missing" "Segments Corrupted" "Data Gaps" "Pruned Ranges" "Missing Topics"; do
    name="$(echo "$label" | tr 'A-Z ' 'a-z_')"
    gate "$prefix.validate_${name}_zero" "$(grep "^$label:" "$out" | tr -s ' ')" grep -Eq "^$label: +0$" "$out"
  done
  gate "$prefix.validate_no_issues" "no 'Issues Found' section" bash -c "! grep -q '^Issues Found:' '$out'"
}

topic_exists() {  # topic_exists source|target TOPIC
  kcli "$1" kafka-topics --bootstrap-server "$(bootstrap "$1")" --list | grep -qx "$2"
}

# Epoch milliseconds from a UTC day number, and back.
day_start_ms() { echo $(( $1 * 86400000 )); }

# Wall clock seconds with millisecond resolution (macOS date has no %N).
now_s() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }
elapsed_since() { perl -MTime::HiRes=time -e 'printf "%.3f", time - $ARGV[0]' "$1"; }

# Kafka CLI inside the broker containers. $1 is source or target.
kcli() {
  local side="$1"; shift
  dc exec -T "$side-kafka" "$@"
}
bootstrap() { echo "$1-kafka:29092"; }

kbackup() {
  dc --profile tools run --rm -T kafka-backup "$@"
}

mc_cmd() {
  dc --profile tools run --rm -T mc "$*"
}

# Render a config template: ${VAR} is replaced from the environment; an unset VAR is an error.
render() {
  local template="$1" out="$2"
  mkdir -p "$(dirname "$out")"
  RENDER_TEMPLATE="$template" perl -pe 's/\$\{([A-Z0-9_]+)\}/exists $ENV{$1} ? $ENV{$1} : die "unset variable $1 in $ENV{RENDER_TEMPLATE}\n"/ge' \
    "$template" > "$out"
}

json_get() {  # json_get FILE python-expression-on-d   e.g. json_get seed.json 'd["records"]'
  python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(eval(sys.argv[2]))' "$1" "$2"
}

wait_for() {  # wait_for "description" timeout_seconds command...
  local what="$1" timeout="$2"; shift 2
  local start; start=$(date +%s)
  until "$@" >/dev/null 2>&1; do
    (( $(date +%s) - start > timeout )) && fail "timed out after ${timeout}s waiting for $what"
    sleep 2
  done
}

STATS_PID=""
start_stats_sampler() {
  mkdir -p "$RUN_DIR/stats"
  local out="$RUN_DIR/stats/$PHASE.jsonl"
  # Only this compose project's containers; other workloads on the host stay out of the evidence.
  ( while true; do
      ids="$(docker ps -q --filter label=com.docker.compose.project=offload)"
      [[ -z "$ids" ]] || docker stats --no-stream --format '{{json .}}' $ids 2>/dev/null \
        | sed "s/^{/{\"ts\":\"$(date -u +%FT%TZ)\",/" >> "$out"
      sleep 15
    done ) &
  STATS_PID=$!
}
stop_stats_sampler() {
  if [[ -n "$STATS_PID" ]]; then kill "$STATS_PID" 2>/dev/null || true; wait "$STATS_PID" 2>/dev/null || true; STATS_PID=""; fi
}

# Partition size in bytes on a broker, as the broker reports it (sum of segment .log files).
log_dir_sizes() {  # log_dir_sizes source|target TOPIC  -> "partition size" lines
  kcli "$1" kafka-log-dirs --bootstrap-server "$(bootstrap "$1")" --describe --topic-list "$2" \
    | grep '^{' | python3 -c '
import json, sys
d = json.load(sys.stdin)
for b in d["brokers"]:
    for ld in b["logDirs"]:
        for p in ld["partitions"]:
            print(p["partition"].rsplit("-", 1)[1], p["size"])'
}

offsets() {  # offsets source|target TOPIC earliest|latest -> "partition offset" lines
  kcli "$1" kafka-get-offsets --bootstrap-server "$(bootstrap "$1")" --topic "$2" --time "$3" \
    | awk -F: '{print $2, $3}'
}
