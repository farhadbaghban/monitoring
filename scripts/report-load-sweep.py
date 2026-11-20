#!/usr/bin/env python3
"""Build comparable reports from one fresh Compose project per load stage."""
import argparse
import html
import json
import re
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("runs", type=Path)
parser.add_argument("output", type=Path)
args = parser.parse_args()
status = dict(line.split() for line in (args.runs / "exit-status.txt").read_text().splitlines())
label_pattern = re.compile(r"([1-9][0-9]*)(?:-r[1-9][0-9]*)?")
rows = []
resources = []
hosts = set()
for key, rc in status.items():
    label = label_pattern.fullmatch(key)
    if not label:
        continue
    n = int(label[1])
    root = args.runs / key
    result = root / "results.json"
    if not result.is_file() or result.stat().st_size == 0:
        rows.append(dict(devices=n, run=key, result="ERROR", exit_code=int(rc), reasons=["No stage data"]))
        continue
    raw = json.loads(result.read_text())
    seconds = raw["stage_seconds"]
    stage, = raw["stages"]
    if stage["devices"] != n or seconds * n != stage["offered"]:
        raise ValueError(f"Unexpected workload in {result}")
    s = stage
    reasons = []
    for ok, reason in (
        (s["acknowledged"] * 1000 >= s["offered"] * 999, "ACK < 99.9%"),
        (s["persisted"] == s["offered"], "Incomplete or unverified SQL count"),
        (0 < s["ingest_p99_ms"] < 200, "Ingest p99 unavailable or ≥ 200 ms"),
        (s["one_year_queries"] > 0, "No successful annual reads"),
        (s["chart_errors"] == 0, "Annual API errors"),
        (s["chart_p99_ms"] < 1000, "Annual API p99 ≥ 1000 ms"),
        (s["drain_seconds_from_end_of_offered_window"] <= 30, "Drain > 30 s"),
    ):
        if not ok:
            reasons.append(reason)
    if s["passed"] != (not reasons):
        raise ValueError(f"PASS criteria mismatch in {result}")
    if not s["acknowledged"]:
        s["ingest_p99_ms"] = None
    if not s["one_year_queries"]:
        s["chart_p99_ms"] = None
    if s["persisted"] is None:
        s["persisted_per_second_including_drain"] = None
    if s["passed"] and int(rc):
        reasons.append("Runner failed after measurement; inspect run.log")
    outcome = "PASS" if s["passed"] and not int(rc) else "ERROR" if s["passed"] else "FAIL"
    rows.append(dict(s, run=key, stage_seconds=seconds, result=outcome,
                     exit_code=int(rc), reasons=reasons))
    hosts.add((root / "host.txt").read_text().strip())
    peaks = {}
    for line in (root / "resources.jsonl").read_text().splitlines():
        record = json.loads(line)
        name = re.sub(r"^monitoring-load-\d+-\d+-|[-]\d+$", "", record["Name"])
        cpu = float(record["CPUPerc"].rstrip("%"))
        match = re.fullmatch(r"([\d.]+)\s*(B|KiB|MiB|GiB|TiB)", record["MemUsage"].split("/")[0].strip())
        if not match:
            raise ValueError(f"Unexpected Docker memory units: {record['MemUsage']}")
        mem = float(match[1]) * (1024 ** ("B KiB MiB GiB TiB".split().index(match[2]) - 2))
        p = peaks.setdefault(name, dict(devices=n, run=key, service=name, cpu_percent=0., memory_mib=0., samples=0))
        p.update(cpu_percent=max(p["cpu_percent"], cpu), memory_mib=max(p["memory_mib"], mem),
                 samples=p["samples"] + 1)
    resources.extend(peaks[k] for k in sorted(peaks))

rows.sort(key=lambda s: s["devices"])
if len({s["stage_seconds"] for s in rows if "stage_seconds" in s}) > 1 or len(hosts) > 1:
    raise ValueError("Expected one stage duration and one Docker host profile")
args.output.mkdir(parents=True, exist_ok=True)
(args.output / "results.json").write_text(json.dumps(dict(
    host=sorted(hosts), run_order=[k for k in status if label_pattern.fullmatch(k)],
    stages=rows, resource_peaks=resources,
    checks={k: int(v) for k, v in status.items() if not label_pattern.fullmatch(k)},
), indent=2, ensure_ascii=False) + "\n")


def fmt(v):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:,.2f}"
    if isinstance(v, int):
        return f"{v:,}"
    return html.escape(str(v))


def table(headers, data):
    return '<div class="scroll"><table><thead><tr>' + "".join(f"<th>{fmt(x)}</th>" for x in headers) + (
        "</tr></thead><tbody>" + "".join("<tr>" + "".join(f"<td>{fmt(x)}</td>" for x in r)
                                       + "</tr>" for r in data) + "</tbody></table></div>")


def run_label(s):
    suffix = s["run"].partition("-")[2]
    return fmt(s["devices"]) + (f" {suffix}" if suffix else "")


def plot(title, series, limit=None, *, plot_rows=None):
    plot_rows = rows if plot_rows is None else plot_rows
    points = [s.get(k) for s in plot_rows for _, k, _ in series if s.get(k) is not None]
    height = max(points + [1, limit or 0]) * 1.12
    width = max(880, 84 * len(rows) + 100)
    def y(value):
        return 270 - 225 * value / height
    out = [f'<h2>{title}</h2><div class="scroll"><svg role="img" aria-label="{title}" viewBox="0 0 {width} 322">']
    for i in range(6):
        v = height * i / 5
        out.append(f'<path d="M 70 {y(v)} H {width-20}" stroke="#ccd5df"/><text x="65" y="{y(v)+4}" text-anchor="end">{v:,.0f}</text>')
    if limit is not None:
        out.append(f'<path d="M 70 {y(limit)} H {width-20}" stroke="#a32b23" stroke-dasharray="5 4"/>')
    for i, s in enumerate(plot_rows):
        x = 96 + i * (width - 146) / max(1, len(rows)-1)
        out.append(f'<text x="{x}" y="290" text-anchor="middle">{run_label(s)}</text>'
                   f'<text x="{x}" y="310" text-anchor="middle" class="{s["result"]}">{s["result"]}</text>')
        for j, (label, key, color) in enumerate(series):
            v = s.get(key)
            if v is not None:
                out.append(f'<circle cx="{x + 10 * (j-(len(series)-1)/2)}" cy="{y(v)}" r="4" fill="{color}">'
                           f'<title>{fmt(s["run"])} · {label}: {fmt(v)}</title></circle>')
    return "".join(out) + "</svg></div><p>" + " · ".join(
        f'<span style="color:{color}">●</span> {label}' for label, _, color in series) + "</p>"


def write(name, title, content):
    head = """<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
    <style>body{font:16px system-ui;margin:2rem auto;max-width:1120px;padding:0 1rem;color:#192d43}
    .scroll{overflow-x:auto}svg{display:block;width:100%;min-width:880px;font-size:12px}.PASS{fill:#106336}.FAIL,.ERROR{fill:#a32b23}
    table{border-collapse:collapse;width:100%}td,th{padding:.45rem;border-bottom:1px solid #ccd5df;text-align:left}
    h2{margin-top:2rem}summary{cursor:pointer}</style>"""
    durations = sorted({s["stage_seconds"] for s in rows if "stage_seconds" in s})
    duration = f"<p>{durations[0]} seconds of scheduled writes per run.</p>" if durations else ""
    footer = "<p>Columns show individual runs, ordered by device count. r2 marks a repeat on another fresh stack.</p>"
    (args.output / name).write_text(f'{head}<title>{title}</title><h1>{title}</h1>{duration}{content}{footer}</html>')


def stage_table(columns):
    return table(["Run (devices)", "Result", *(label for label, _ in columns), "Failed criteria"],
                 [[run_label(s), s["result"], *(s.get(k) for _, k in columns), "; ".join(s["reasons"])]
                  for s in rows])


write("write-rate.html", "Ingest and persistence",
      "<p>Each point is a separate run with fresh volumes. Rates use different denominators. "
      "Acknowledgement counts can include responses after the scheduled window.</p>"
      + plot("Rows per second", [("Scheduled devices = offered rows/s", "devices", "#576775"),
              ("ACK count ÷ scheduled seconds", "ack_per_second", "#1c63b8"),
              ("SQL count ÷ (scheduled seconds + drain)", "persisted_per_second_including_drain", "#00765a")])
      + plot("Ingest p99 (ms)", [("Successful ingest p99; target below 200 ms", "ingest_p99_ms", "#1c63b8")], 200)
      + stage_table([("Scheduled", "offered"), ("ACK", "acknowledged"), ("Stored", "persisted"),
                     ("Ingest p99 (ms)", "ingest_p99_ms"),
                     ("Drain (s)", "drain_seconds_from_end_of_offered_window")]))
write("chart-latency.html", "One-year API latency",
      "<p>p99 uses completed successful API requests. Missing measurements are omitted, "
      "and completed read errors are shown separately. The red line is the 1,000 ms target. "
      "PASS/FAIL refers to the whole stage. Browser rendering is excluded.</p>"
      + plot("Successful annual API p99 (ms)", [("API p99", "chart_p99_ms", "#1c63b8")], 1000)
      + stage_table([("p99 (ms)", "chart_p99_ms"), ("Successful reads", "one_year_queries"),
                     ("Read errors", "chart_errors")]))
peak_table = ""
for name in sorted({r["service"] for r in resources}):
    service_rows = sorted((r for r in resources if r["service"] == name), key=lambda r: r["devices"])
    per_run = {r["run"]: r for r in service_rows}
    samples_by_stage = [dict(s, **{k: per_run.get(s["run"], {}).get(k)
                                  for k in ("cpu_percent", "memory_mib")}) for s in rows]
    peak_table += (f"<details><summary>{fmt(name)}</summary>"
        + plot("Peak CPU (%)", [("CPU", "cpu_percent", "#1c63b8")], plot_rows=samples_by_stage)
        + plot("Peak memory (MiB)", [("Memory", "memory_mib", "#00765a")], plot_rows=samples_by_stage)
        + table(
        ["Run (devices)", "Peak CPU (%)", "Peak memory (MiB)", "Samples"],
        [[run_label(r), r["cpu_percent"], r["memory_mib"], r["samples"]] for r in service_rows]) + "</details>")
for s in rows:
    samples = [r for r in resources if r["run"] == s["run"]]
    if samples:
        s["peak_cpu"] = max(r["cpu_percent"] for r in samples)
        s["peak_mem"] = max(r["memory_mib"] for r in samples)
write("resources.html", "Sampled container resources",
      "<p>Peak per container, sampled during fixture preparation, benchmark and drain. "
      "CPU at 100% equals one logical core. Individual peaks may occur at different times.</p>"
      + plot("Highest observed CPU in any one container (%)", [("CPU", "peak_cpu", "#1c63b8")])
      + plot("Highest observed memory in any one container (MiB)", [("Memory", "peak_mem", "#00765a")])
      + "<h2>Per service</h2>" + peak_table)
