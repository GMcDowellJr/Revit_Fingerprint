# -*- coding: utf-8 -*-
"""
build_probe_inventory.py

Consolidates raw tools/probes/probe_*.json breadth-probe output into a single
curated, cross-domain inventory: one representative row per (domain, key),
deduped/merged across however many dated probe runs exist for that domain.

This replaces the hand-assembled, single-snapshot
tools/probes/domain_probe_inventory_2024-02-05.md, which goes stale every
time a probe is rerun or a new probe is added (nothing regenerated it).

Each probe_<domain>_<date>.json already dedupes *within* a single run down to
one entry per param_key/member_key (see e.g. probe_dimension_types.py's
param_index). This tool performs the second layer of dedupe: merging those
per-run entries *across* runs/dates for the same domain, picking one
representative example per key using the same scoring heuristic the probes
already use internally.

This tool is pure Python 3 (no Revit/CLR dependency) and is meant to be run
from a developer machine against the probe JSON files checked into
tools/probes/.

Usage:
    python tools/probes/build_probe_inventory.py
    python tools/probes/build_probe_inventory.py --probes-dir tools/probes \
        --out-md tools/probes/PROBE_INVENTORY.md \
        --out-csv tools/probes/PROBE_INVENTORY.csv
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import OrderedDict

_FILENAME_RE = re.compile(r"^probe_(?P<domain>.+)_(?P<date>\d{4}-\d{2}-\d{2})\.json$")

# Record "kinds" this tool treats as key/value inventories to merge.
# "crosswalk" and any other kind are counted as diagnostics only.
_MERGE_KINDS = ("inventory", "reflection")


def _example_score(example):
    """Mirrors the _example_score heuristic already used inside the probes
    (see probe_dimension_types.py) so the "best" example picked across
    multiple probe runs is chosen the same way a single run would pick it."""
    if not example:
        return -1
    q = example.get("q")
    base = {"ok": 100, "missing": 10, "unreadable": 5}.get(q, 0)
    if example.get("display") not in (None, ""):
        base += 20
    if example.get("norm") is not None:
        base += 10
    if example.get("raw") is not None:
        base += 5
    return base


def discover_probe_files(probes_dir):
    """Return (matched, skipped) lists. matched = [(path, domain, date)]."""
    matched = []
    skipped = []
    try:
        names = sorted(os.listdir(probes_dir))
    except OSError as ex:
        return [], [("<probes_dir>", "could not list directory: {}".format(ex))]

    for name in names:
        if not name.endswith(".json"):
            continue
        m = _FILENAME_RE.match(name)
        if not m:
            skipped.append((name, "does not match probe_<domain>_<YYYY-MM-DD>.json"))
            continue
        matched.append((os.path.join(probes_dir, name), m.group("domain"), m.group("date")))
    return matched, skipped


def load_payload(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _merge_observed(agg, record_observed):
    if not record_observed:
        return
    storage_types = record_observed.get("storage_types")
    if storage_types:
        agg["storage_types"].update(storage_types)
    q_counts = record_observed.get("q_counts")
    if isinstance(q_counts, dict):
        for k, v in q_counts.items():
            try:
                agg["q_counts"][k] = agg["q_counts"].get(k, 0) + int(v)
            except (TypeError, ValueError):
                continue
    for count_field in ("unique_value_count", "ok_count", "error_count"):
        v = record_observed.get(count_field)
        if isinstance(v, (int, float)):
            agg[count_field] = agg.get(count_field, 0) + int(v)


def merge_probe_files(matched, warnings):
    """
    Returns an OrderedDict keyed by domain -> bucket -> key -> aggregate dict.
    bucket is one of "param", "reflection", "opaque".
    """
    domains = OrderedDict()
    diagnostics = OrderedDict()  # domain -> {"crosswalk_count": int, "runs": [...]}

    for path, fname_domain, date in matched:
        basename = os.path.basename(path)
        try:
            payload = load_payload(path)
        except (OSError, ValueError) as ex:
            warnings.append("{}: failed to parse JSON ({})".format(basename, ex))
            continue

        if not isinstance(payload, list):
            warnings.append("{}: top-level JSON is not a list; skipped".format(basename))
            continue

        for entry in payload:
            if not isinstance(entry, dict):
                continue
            kind = entry.get("kind")
            domain = entry.get("domain") or fname_domain
            if entry.get("domain") and entry.get("domain") != fname_domain:
                warnings.append(
                    "{}: declared domain '{}' != filename domain '{}'; using declared domain".format(
                        basename, entry.get("domain"), fname_domain
                    )
                )

            diag = diagnostics.setdefault(domain, {"crosswalk_count": 0, "opaque_count": 0, "runs": []})
            if date not in diag["runs"]:
                diag["runs"].append(date)

            if kind == "crosswalk":
                diag["crosswalk_count"] += len(entry.get("records") or [])
                continue

            if kind not in _MERGE_KINDS:
                continue

            bucket = "param" if kind == "inventory" else "reflection"
            records = entry.get("records") or []

            for rec in records:
                if not isinstance(rec, dict):
                    continue
                key = rec.get("param_key") or rec.get("member_key")
                if not key:
                    diag["opaque_count"] += 1
                    continue

                dbucket = domains.setdefault(domain, OrderedDict())
                bkeys = dbucket.setdefault(bucket, OrderedDict())
                agg = bkeys.get(key)
                if agg is None:
                    agg = {
                        "storage_types": set(),
                        "q_counts": {},
                        "example": None,
                        "dates_seen": set(),
                        "source_files": set(),
                        "member_kind": rec.get("member_kind"),
                        "type_label": rec.get("type_label"),
                    }
                    bkeys[key] = agg

                agg["dates_seen"].add(date)
                agg["source_files"].add(basename)
                if rec.get("member_kind"):
                    agg["member_kind"] = rec.get("member_kind")
                if rec.get("type_label"):
                    agg["type_label"] = rec.get("type_label")

                _merge_observed(agg, rec.get("observed"))

                candidate = rec.get("example")
                if _example_score(candidate) > _example_score(agg["example"]):
                    agg["example"] = candidate

    return domains, diagnostics


def _fmt_q_counts(q_counts):
    if not q_counts:
        return ""
    return ";".join("{}={}".format(k, q_counts[k]) for k in sorted(q_counts.keys()))


def _fmt_example(example):
    if not example:
        return {"q": None, "storage": None, "raw": None, "display": None, "norm": None}
    return example


def write_csv(domains, out_csv, warnings):
    rows = []
    for domain in sorted(domains.keys()):
        for bucket in ("param", "reflection"):
            bkeys = domains[domain].get(bucket)
            if not bkeys:
                continue
            for key in sorted(bkeys.keys()):
                agg = bkeys[key]
                ex = _fmt_example(agg["example"])
                rows.append(
                    {
                        "domain": domain,
                        "key_kind": bucket,
                        "key": key,
                        "member_kind": agg.get("member_kind") or "",
                        "type_label": agg.get("type_label") or "",
                        "storage_types": ";".join(sorted(agg["storage_types"])),
                        "q_counts": _fmt_q_counts(agg["q_counts"]),
                        "unique_value_count": agg.get("unique_value_count", 0),
                        "example_q": ex.get("q"),
                        "example_storage": ex.get("storage"),
                        "example_raw": ex.get("raw"),
                        "example_display": ex.get("display"),
                        "example_norm": ex.get("norm"),
                        "first_seen_date": min(agg["dates_seen"]) if agg["dates_seen"] else "",
                        "last_seen_date": max(agg["dates_seen"]) if agg["dates_seen"] else "",
                        "run_count": len(agg["dates_seen"]),
                        "source_files": ";".join(sorted(agg["source_files"])),
                    }
                )

    fieldnames = [
        "domain", "key_kind", "key", "member_kind", "type_label",
        "storage_types", "q_counts", "unique_value_count",
        "example_q", "example_storage", "example_raw", "example_display", "example_norm",
        "first_seen_date", "last_seen_date", "run_count", "source_files",
    ]
    try:
        os.makedirs(os.path.dirname(os.path.abspath(out_csv)), exist_ok=True)
    except OSError:
        pass
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    return len(rows)


def scan_domain_coverage(domains_dir):
    """Best-effort: list domain module names under domains_dir. Returns None
    if the directory can't be found (this tool must still work standalone)."""
    if not domains_dir or not os.path.isdir(domains_dir):
        return None
    names = []
    for fname in sorted(os.listdir(domains_dir)):
        if fname.endswith(".py") and fname != "__init__.py":
            names.append(fname[:-3])
    return names


def write_markdown(domains, diagnostics, out_md, matched, skipped, warnings, domain_module_names):
    lines = []
    lines.append("# Probe Inventory (auto-generated)")
    lines.append("")
    lines.append(
        "Generated by `tools/probes/build_probe_inventory.py`. Do not hand-edit — "
        "rerun the script after adding/updating `probe_*.json` files. This "
        "supersedes the manually-assembled `domain_probe_inventory_2024-02-05.md`, "
        "which is not kept in sync."
    )
    lines.append("")
    lines.append(
        "Each row is one representative observation per `(domain, key)`, merged "
        "across every dated probe run found for that domain. `key_kind=param` rows "
        "come from a probe's dynamic/curated Parameter or property capture; "
        "`key_kind=reflection` rows come from a `.NET` reflection sweep "
        "(properties/zero-arg methods) layered on top, where present."
    )
    lines.append("")

    lines.append("## Source runs")
    lines.append("")
    lines.append("| domain | runs | dates | crosswalk records | opaque records |")
    lines.append("|---|---|---|---|---|")
    for domain in sorted(diagnostics.keys()):
        diag = diagnostics[domain]
        dates = ", ".join(sorted(diag["runs"]))
        lines.append(
            "| `{}` | {} | {} | {} | {} |".format(
                domain, len(diag["runs"]), dates, diag["crosswalk_count"], diag["opaque_count"]
            )
        )
    lines.append("")

    if domain_module_names is not None:
        probed = set(diagnostics.keys())
        missing = sorted(set(domain_module_names) - probed)
        lines.append("## Domain coverage")
        lines.append("")
        lines.append(
            "Active domain modules under `domains/` with **no** probe JSON present at all "
            "(nothing to curate from — not the same as \"probed and found empty\"):"
        )
        lines.append("")
        if missing:
            for m in missing:
                lines.append("- `{}`".format(m))
        else:
            lines.append("- (none — every domain module has at least one probe run)")
        lines.append("")

    if skipped or warnings:
        lines.append("## Warnings")
        lines.append("")
        for name, reason in skipped:
            lines.append("- skipped `{}`: {}".format(name, reason))
        for w in warnings:
            lines.append("- {}".format(w))
        lines.append("")

    for domain in sorted(domains.keys()):
        lines.append("## domain — `{}`".format(domain))
        lines.append("")
        for bucket, label in (("param", None), ("reflection", "reflection sweep")):
            bkeys = domains[domain].get(bucket)
            if not bkeys:
                continue
            if label:
                lines.append("### {} ({})".format(domain, label))
                lines.append("")
            for key in sorted(bkeys.keys()):
                agg = bkeys[key]
                ex = _fmt_example(agg["example"])
                lines.append("- **key** — `{}`".format(key))
                if agg.get("member_kind"):
                    lines.append("  - member_kind — `{}`".format(agg["member_kind"]))
                if agg.get("type_label"):
                    lines.append("  - type_label — `{}`".format(agg["type_label"]))
                lines.append("  - storage_types — `{}`".format(", ".join(sorted(agg["storage_types"])) or "(none)"))
                lines.append("  - q_counts — `{}`".format(_fmt_q_counts(agg["q_counts"]) or "(none)"))
                lines.append("  - example — q=`{}` storage=`{}` raw=`{}` display=`{}` norm=`{}`".format(
                    ex.get("q"), ex.get("storage"), ex.get("raw"), ex.get("display"), ex.get("norm")
                ))
                lines.append("  - seen — {} run(s), {}–{}".format(
                    len(agg["dates_seen"]),
                    min(agg["dates_seen"]) if agg["dates_seen"] else "?",
                    max(agg["dates_seen"]) if agg["dates_seen"] else "?",
                ))
                lines.append("")

    try:
        os.makedirs(os.path.dirname(os.path.abspath(out_md)), exist_ok=True)
    except OSError:
        pass
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def build(probes_dir, out_md, out_csv, domains_dir):
    warnings = []
    matched, skipped = discover_probe_files(probes_dir)
    domains, diagnostics = merge_probe_files(matched, warnings)
    domain_module_names = scan_domain_coverage(domains_dir) if domains_dir else None
    row_count = write_csv(domains, out_csv, warnings)
    write_markdown(domains, diagnostics, out_md, matched, skipped, warnings, domain_module_names)
    return {
        "files_matched": len(matched),
        "files_skipped": len(skipped),
        "domains": len(domains),
        "csv_rows": row_count,
        "warnings": warnings,
        "skipped": skipped,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    default_probes_dir = os.path.dirname(os.path.abspath(__file__))
    parser.add_argument("--probes-dir", default=default_probes_dir)
    parser.add_argument("--out-md", default=None)
    parser.add_argument("--out-csv", default=None)
    parser.add_argument(
        "--domains-dir",
        default=None,
        help="Optional path to the domains/ directory, used only for the coverage "
        "appendix (which active domains have zero probe runs). Best-effort.",
    )
    args = parser.parse_args(argv)

    probes_dir = os.path.abspath(args.probes_dir)
    out_md = args.out_md or os.path.join(probes_dir, "PROBE_INVENTORY.md")
    out_csv = args.out_csv or os.path.join(probes_dir, "PROBE_INVENTORY.csv")
    domains_dir = args.domains_dir
    if domains_dir is None:
        # Best-effort default: tools/probes/../../domains
        candidate = os.path.abspath(os.path.join(probes_dir, "..", "..", "domains"))
        domains_dir = candidate if os.path.isdir(candidate) else None

    result = build(probes_dir, out_md, out_csv, domains_dir)

    print("Probe inventory build complete.")
    print("  probe files matched : {}".format(result["files_matched"]))
    print("  probe files skipped : {}".format(result["files_skipped"]))
    print("  domains covered     : {}".format(result["domains"]))
    print("  csv rows written    : {}".format(result["csv_rows"]))
    print("  markdown            : {}".format(out_md))
    print("  csv                 : {}".format(out_csv))
    if result["warnings"]:
        print("  warnings            : {}".format(len(result["warnings"])))
        for w in result["warnings"][:10]:
            print("    - {}".format(w))
    return 0


if __name__ == "__main__":
    sys.exit(main())
