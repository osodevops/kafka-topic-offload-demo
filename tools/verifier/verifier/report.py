"""Assemble report.md from a run's evidence directory. Every number is read from a file in it."""
import json
import re
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .common import DAY_MS, log, run_dir

GATE_RE = re.compile(r"^GATE (\S+): (PASS|FAIL)(?: - (.*))?$")
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def load(path):
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else None


def gb(b):
    return "n/a" if b is None else f"{b / 1e9:,.2f} GB"


def num(n):
    return "n/a" if n is None else f"{n:,}"


def iso(ms):
    return "n/a" if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def dur(seconds):
    if seconds is None:
        return "n/a"
    if float(seconds) < 60:
        return f"{float(seconds):.1f}s"
    s = int(float(seconds))
    return f"{s // 3600}h {s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m {s % 60:02d}s"


def table(headers, rows) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c).replace("|", "\\|") for c in r) + " |" for r in rows]
    return "\n".join(out)


def collect(rd: Path):
    gates, findings = OrderedDict(), []
    for logf in sorted((rd / "logs").glob("*.log")):
        for raw in logf.read_text(errors="replace").splitlines():
            line = ANSI.sub("", raw).rstrip()
            m = GATE_RE.match(line)
            if m:
                gates.pop((logf.stem, m.group(1)), None)  # a re-run phase: the latest result wins
                gates[(logf.stem, m.group(1))] = (m.group(2), m.group(3) or "")
            elif line.startswith("FINDING ") and (logf.stem, line[8:]) not in findings:
                findings.append((logf.stem, line[8:]))
    return gates, findings


def peak_stats(rd: Path):
    peaks = defaultdict(lambda: {"cpu": 0.0, "mem": ""})
    mem_bytes = {}
    units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "kB": 1e3, "MB": 1e6, "GB": 1e9}
    for f in (rd / "stats").glob("*.jsonl"):
        for line in f.read_text().splitlines():
            try:
                s = json.loads(line)
            except ValueError:
                continue
            name = s.get("Name", "?")
            cpu = float(s.get("CPUPerc", "0%").rstrip("%") or 0)
            used = s.get("MemUsage", "0B / 0B").split(" / ")[0]
            m = re.match(r"([\d.]+)([A-Za-z]+)", used)
            b = float(m.group(1)) * units.get(m.group(2), 1) if m else 0
            peaks[name]["cpu"] = max(peaks[name]["cpu"], cpu)
            if b >= mem_bytes.get(name, -1):
                mem_bytes[name] = b
                peaks[name]["mem"] = used
    return peaks


def main(_cmd: str, argv) -> int:
    rd = run_dir()
    env = load(rd / "environment.json") or {}
    topic = env.get("topic", "device-telemetry")
    gates, findings = collect(rd)
    # Negative tests and the Azurite phase are reported on their own and do not decide the verdict:
    # negative tests pass by failing inner gates, and Azurite cannot pass until kafka-backup supports the Azure emulator.
    main_gates = [(k, v) for k, v in gates.items() if k[0] not in ("95-negative-tests", "90-azurite")]
    neg_gates = [(k, v) for k, v in gates.items() if k[0] == "95-negative-tests"]
    azurite_gates = [(k, v) for k, v in gates.items() if k[0] == "90-azurite"]
    failed = [k for k, v in main_gates if v[0] == "FAIL"]

    seed = load(rd / "seed" / f"{topic}.json") or {}
    baseline = load(rd / "baseline" / f"{topic}.json") or {}
    backup = load(rd / "backup" / f"summary-{topic}.json") or {}
    comparison = load(rd / "backup" / f"summary-{topic}-comparison.json")
    run_primary = load(rd / "backup" / "run-primary.json") or {}
    run_comparison = load(rd / "backup" / "run-comparison.json") or {}
    verify = load(rd / "backup" / f"verify-{topic}.json") or {}
    req_primary = load(rd / "s3" / "requests-backup-primary.json") or {}
    req_comparison = load(rd / "s3" / "requests-backup-comparison.json") or {}
    prediction = load(rd / "prune" / "prediction-apply.json") or {}
    check = load(rd / "prune" / "check.json") or {}
    window = load(rd / "restore" / "audit-window.json") or {}
    audit = load(rd / "restore" / "verify-audit.json") or {}
    audit_run = load(rd / "restore" / "run-audit.json") or {}
    full = load(rd / "restore" / "verify-full.json") or {}
    full_run = load(rd / "restore" / "run-full.json") or {}
    extrap = load(rd / "restore" / "extrapolation.json") or {}
    consumer = load(rd / "consumers" / "telemetry-analytics.json") or {}

    source_bytes = sum(s["log_bytes"] for p in baseline.get("partitions", {}).values() for s in p["segments"]) or None
    L = []
    L.append(f"# Topic offload and retention prune: evidence report\n")
    L.append(f"Run `{env.get('run_id')}` at scale `{env.get('scale')}`, started {iso(env.get('started_at'))}.")
    L.append(f"Repo `{env.get('repo_git_sha', 'n/a')[:12]}`{' (uncommitted changes)' if env.get('repo_dirty') else ''}, "
             f"{env.get('kafka_backup', {}).get('version', 'kafka-backup n/a')}.\n")
    verdict = "PASS" if main_gates and not failed else "FAIL"
    L.append(f"**Result: {verdict}.** {sum(1 for _, v in main_gates if v[0] == 'PASS')} of {len(main_gates)} gates passed"
             + (f"; failed: {', '.join(g for _, g in failed)}" if failed else "") + ".\n")

    L.append("## Headline numbers\n")
    rows = [
        ["Records seeded", num(seed.get("records")), f"{seed.get('partitions')} partitions, one year to {iso(seed.get('seed_end_ts'))}"],
        ["Source topic size", gb(source_bytes), f"{sum(len(p['segments']) for p in baseline.get('partitions', {}).values())} broker segments"],
        ["Late cohort", num(sum(p.get("late", 0) for p in seed.get("per_partition", {}).values())),
         f"{seed.get('late_rate', 0) * 100:.1f}% of records, up to {seed.get('late_max_hours')} h late"],
        ["Backup", f"{gb(backup.get('compressed_bytes'))} stored", f"{backup.get('compression_ratio')}x zstd, {backup.get('segments')} objects, "
         f"{dur(backup.get('seconds'))}, {backup.get('mb_per_s_uncompressed')} MB/s"],
        ["Backup verification", "every object SHA-256 and every record digest", f"{dur((verify.get('summary') or {}).get('verify_seconds'))}"],
        ["Retention lowered to", f"{(prediction.get('retention_ms') or 0) / DAY_MS:.0f} days", f"applied {iso(check.get('first_seen_at'))}"],
        ["Bytes reclaimed", gb(check.get("bytes_reclaimed")), f"predicted {gb(prediction.get('deleted_bytes'))}, {check.get('reclaimed_pct')}% of the topic"],
        ["Records removed from the live topic", num(check.get("records_deleted")), "all held in the sealed backup"],
        ["Audit restore", f"{num(audit.get('records'))} records", f"{window.get('exact_from')} to {window.get('exact_to')}, {dur(audit_run.get('seconds'))}"],
        ["Full restore", f"{num(full.get('records'))} records", f"{dur(full_run.get('seconds'))}, {extrap.get('measured', {}).get('restore_measured_mb_s')} MB/s"],
    ]
    L.append(table(["What", "Value", "Detail"], rows) + "\n")

    L.append("## Gates\n")
    by_phase = OrderedDict()
    for (phase, name), (status, detail) in main_gates:
        by_phase.setdefault(phase, []).append([name, status, detail])
    for phase, rows in by_phase.items():
        L.append(f"### {phase}\n")
        L.append(table(["Gate", "Result", "Detail"], rows) + "\n")

    if findings:
        L.append("## Findings\n")
        L += [f"- `{phase}`: {text}" for phase, text in findings]
        L.append("")

    L.append("## Object storage requests\n")
    mib = lambda r: f"{(r.get('segment_max_bytes') or 0) // 1048576} MiB"
    apis = sorted(set(req_primary) | set(req_comparison))
    L.append(table(["S3 API", f"Backup, {mib(run_primary)} segments", f"Backup, {mib(run_comparison)} segments" if run_comparison else "Comparison"],
                   [[a, num(req_primary.get(a)), num(req_comparison.get(a)) if req_comparison else "skipped"] for a in apis]) + "\n")
    if comparison:
        L.append(f"Objects: {backup.get('segments')} at {mib(run_primary)} against {comparison.get('segments')} at {mib(run_comparison)}; "
                 f"wall clock {dur(run_primary.get('seconds'))} against {dur(run_comparison.get('seconds'))}.\n")

    if prediction.get("partitions"):
        L.append("## Retention prune per partition\n")
        rows = []
        for p, v in sorted(prediction["partitions"].items(), key=lambda kv: int(kv[0])):
            low = (check.get("watermarks") or {}).get(p, {}).get("low")
            rows.append([p, num(v["log_start"]), num(v["predicted_log_start"]), num(low), len(v["deleted_segments"]),
                         gb(v["deleted_bytes"]), iso(v["first_kept_min_ts"])])
        L.append(table(["Partition", "Log start before", "Predicted log start", "Log start after", "Segments deleted",
                        "Bytes deleted", "Oldest record still on the broker"], rows) + "\n")
        L.append(f"{check.get('note', '')}\n")
    if consumer:
        L.append(f"Consumer `telemetry-analytics` resumed with `auto.offset.reset=error`: {len(consumer.get('offset_out_of_range', {}))} "
                 f"partitions raised OffsetOutOfRange instead of silently skipping data.\n")

    if audit:
        L.append("## Restores\n")
        L.append(f"Audit window {window.get('exact_from')} to {window.get('exact_to')} UTC, restored with a "
                 f"{(window.get('pad_ms') or 0) / 3_600_000:.0f} h pad each side. Buckets that differ inside the padded window "
                 f"(pad days only are expected): {len(audit.get('differences', []))}; unexplained: {len(audit.get('unexplained', []))}.\n")
        if full.get("group"):
            L.append(table(["Partition", "Source committed", "Target committed", "Target record's x-original-offset"],
                           [[p, num(c.get("source")), num(c.get("target")), num(c.get("record_x_original_offset"))]
                            for p, c in sorted(full["group"].items(), key=lambda kv: int(kv[0]))]) + "\n")

    if extrap.get("rows"):
        L.append("## Extrapolation\n")
        L.append(table(["Size", "Scenario", "MB/s", "Hours", "Tier 1 (1 h)", "Tier 2 (24 h)"],
                       [[r["size"], r["scenario"], r["mb_s"], r["hours"], "yes" if r["within_tier1_1h"] else "no",
                         "yes" if r["within_tier2_24h"] else "no"] for r in extrap["rows"]]) + "\n")
        L.append(f"{extrap.get('note', '')}\n")

    def own_and_setup(gs, prefix):
        own = [[n, s, d] for (_, n), (s, d) in gs if n.startswith(prefix)]
        setup = [(n, s) for (_, n), (s, _) in gs if not n.startswith(prefix)]
        note = f"Setup gates for the scenario topics: {sum(1 for _, s in setup if s == 'PASS')} of {len(setup)} passed."
        return own, note

    if neg_gates:
        own, note = own_and_setup(neg_gates, "negative.")
        L.append("## Negative tests\n")
        L.append("Each scenario passes only when its named gate fails for the expected reason.\n")
        L.append(table(["Check", "Result", "Detail"], own) + "\n")
        L.append(note + "\n")

    if azurite_gates:
        own, note = own_and_setup(azurite_gates, "azurite.")
        L.append("## Azure client path (Azurite)\n")
        L.append("Not part of the verdict. kafka-backup 0.22.0 cannot use plain HTTP or emulator mode for Azure, "
                 "so this phase is expected to fail until that change is released.\n")
        L.append(table(["Check", "Result", "Detail"], own) + "\n")
        L.append(note + "\n")

    timings = rd / "timings.tsv"
    if timings.exists():
        L.append("## Timings\n")
        last = OrderedDict()
        for line in timings.read_text().splitlines():
            phase, secs, status = line.split("\t")
            last[phase] = (secs, status)
        L.append(table(["Phase", "Wall clock", "Result"], [[p, dur(s), st] for p, (s, st) in last.items()]) + "\n")

    peaks = peak_stats(rd)
    if peaks:
        L.append("## Peak container resources\n")
        L.append(table(["Container", "Peak CPU %", "Peak memory"], [[n, f"{v['cpu']:.0f}", v["mem"]] for n, v in sorted(peaks.items())]) + "\n")

    L.append("## Environment\n")
    L.append(table(["Image", "Reference", "Architecture", "Digest or ID"],
                   [[k, v.get("ref"), v.get("architecture", ""), (v.get("repo_digests") or [v.get("id")])[0]]
                    for k, v in env.get("images", {}).items()]) + "\n")
    L.append(f"Docker VM: {env.get('docker_vm', {}).get('cpus')} CPUs, {int(env.get('docker_vm', {}).get('mem_bytes') or 0) / 2**30:.0f} GiB; "
             f"host {env.get('host', {}).get('system')} {env.get('host', {}).get('machine')}.\n")
    L.append("Every file in this directory is listed with its SHA-256 in `SHA256SUMS`.\n")

    (rd / "report.md").write_text("\n".join(L))
    log(f"report.md: {verdict}, {len(main_gates)} gates, {len(findings)} findings")
    return 0 if verdict == "PASS" else 1
