# -*- coding: utf-8 -*-
"""
find_crosswalk_candidates.py

Step 2 of the probe workflow:
    1. Run probe_thin_runner.py in Revit -> raw probes_<version>_<run_id>.json
    2. build_probe_inventory.py -> PROBE_INVENTORY.csv/.md + PROBE_CROSSWALK.csv/.md
       THIS SCRIPT reads those two and ranks candidates for new crosswalk work.
    3. Implement crosswalk resolution for the candidates worth it, re-run
       probe_thin_runner.py in Revit, regenerate step 2's output, confirm the
       candidate disappeared from this report (now resolved) and shows up
       with a real resolution rate in PROBE_CROSSWALK.csv instead.

What "candidate" means here: any inventory/reflection key whose value is
ElementId-typed -- by definition a reference to something else in the
document -- that PROBE_CROSSWALK.csv doesn't already resolve for that domain.
Reflection and the real-Parameter inventory walk both auto-discover new
members as Revit's API surface changes; crosswalk resolution is hand-written
and does NOT auto-follow from that discovery (see the MaterialId /
VIEW_PHASE / GetFilters() crosswalks added this session -- none of them were
triggered by new data showing up, they were a person looking at the member
list and deciding to close the gap). This script is the "look at the member
list" step, automated and re-runnable, so that step doesn't depend on
someone remembering to re-derive it by hand each time.

Deliberately does NOT hardcode a list of already-resolved member names to
exclude (e.g. "materialid", "viewtemplateid", ...) -- that list would need
constant upkeep and silently go stale the moment a new crosswalk ships
(exactly the failure mode this tool exists to avoid one level up). Instead
it cross-references PROBE_CROSSWALK.csv directly: for each domain, every
crosswalk column name is normalized (lowercased, alnum-only) and any
inventory/reflection member whose normalized name overlaps a crosswalk
column for that SAME domain is treated as already resolved. Re-run this
script after any crosswalk addition and the newly-resolved member drops out
on its own.

Also excludes bare "Id" (the object's own identity, not a reference to
something else) -- everything else ending in "Id" or matching a named
ElementId-storage parameter is a genuine candidate.

This tool is pure Python 3 (no Revit/CLR dependency), reads only the CSVs
build_probe_inventory.py already produces, and is meant to be re-run any
time those are regenerated.

Usage:
    python tools/probes/find_crosswalk_candidates.py
    python tools/probes/find_crosswalk_candidates.py \
        --probe-inventory-csv tools/probes/PROBE_INVENTORY.csv \
        --probe-crosswalk-csv tools/probes/PROBE_CROSSWALK.csv \
        --out-csv tools/probes/CROSSWALK_CANDIDATES.csv \
        --out-md tools/probes/CROSSWALK_CANDIDATES.md
"""

import argparse
import csv
import os
import re
import sys
from collections import OrderedDict

# Element's own identity, not a reference to anything else.
_SELF_ID_NAMES = set(["id"])

_NORMALIZE_RE = re.compile(r"[^a-z0-9]")


def _normalize(s):
    if not s:
        return ""
    return _NORMALIZE_RE.sub("", s.lower())


def _member_name(key):
    """Last dot-segment of an inventory/reflection key.
    'refl.WallType.WorksetId' -> 'WorksetId'; 'p.Structural Material' -> 'Structural Material'."""
    return key.rsplit(".", 1)[-1]


def _read_csv_rows(path):
    if not path or not os.path.isfile(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _is_elementid_typed(row):
    if row.get("key_kind") == "reflection":
        # "ElementIdList" is a list of ElementIds (see _reflect_contract's
        # ElementId-collection branch) -- still a reference to something
        # else in the document, same as the bare "ElementId" scalar case.
        return row.get("example_storage") in ("ElementId", "ElementIdList")
    return "ElementId" in (row.get("storage_types") or "")


def build_resolved_index(crosswalk_rows):
    """domain -> set of normalized column-name tokens already resolved by crosswalk."""
    index = {}
    for r in crosswalk_rows:
        domain = r.get("domain")
        col = r.get("column")
        if not domain or not col:
            continue
        index.setdefault(domain, set()).add(_normalize(col))
    return index


def _already_resolved(domain, member_norm, resolved_index):
    cols = resolved_index.get(domain)
    if not cols:
        return False
    for col_norm in cols:
        if not col_norm:
            continue
        if member_norm in col_norm or col_norm in member_norm:
            return True
    return False


def find_candidates(inventory_rows, crosswalk_rows):
    """Returns (candidates, resolved_index) where candidates is a list of dicts:
    {member, member_norm, domain, key_kind, key, example_display, run_count}."""
    resolved_index = build_resolved_index(crosswalk_rows)

    candidates = []
    for row in inventory_rows:
        if not _is_elementid_typed(row):
            continue
        member = _member_name(row.get("key", ""))
        member_norm = _normalize(member)
        if not member_norm or member_norm in _SELF_ID_NAMES:
            continue
        domain = row.get("domain")
        if _already_resolved(domain, member_norm, resolved_index):
            continue
        candidates.append({
            "member": member,
            "member_norm": member_norm,
            "domain": domain,
            "key_kind": row.get("key_kind"),
            "key": row.get("key"),
            "example_display": row.get("example_display"),
            "run_count": row.get("run_count"),
        })
    return candidates, resolved_index


def group_by_member(candidates):
    """member_norm -> {member, domains: [(domain, key, example_display), ...]}"""
    groups = OrderedDict()
    for c in sorted(candidates, key=lambda c: (c["member_norm"], c["domain"])):
        g = groups.setdefault(c["member_norm"], {"member": c["member"], "rows": []})
        g["rows"].append(c)
    # Rank by domain count descending, then member name.
    ranked = sorted(
        groups.values(),
        key=lambda g: (-len(set(r["domain"] for r in g["rows"])), g["member"].lower()),
    )
    return ranked


def write_csv(ranked, out_csv):
    rows = []
    for g in ranked:
        domain_count = len(set(r["domain"] for r in g["rows"]))
        for r in g["rows"]:
            rows.append({
                "member": g["member"],
                "domain_count": domain_count,
                "domain": r["domain"],
                "key_kind": r["key_kind"],
                "key": r["key"],
                "example_display": r["example_display"],
                "run_count": r["run_count"],
            })
    fieldnames = ["member", "domain_count", "domain", "key_kind", "key", "example_display", "run_count"]
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


def write_markdown(ranked, out_md, min_domains):
    lines = []
    lines.append("# Crosswalk Candidates (auto-generated)")
    lines.append("")
    lines.append(
        "Generated by `tools/probes/find_crosswalk_candidates.py` -- step 2 of the probe "
        "workflow (probe -> find candidates -> implement + re-run). Do not hand-edit, "
        "rerun the script after PROBE_INVENTORY.csv/PROBE_CROSSWALK.csv update."
    )
    lines.append("")
    lines.append(
        "Each entry below is an `ElementId`-typed inventory/reflection member -- a "
        "reference to something else in the document -- that no crosswalk currently "
        "resolves for that domain. Ranked by how many domains it shows up in. This list "
        "is cross-referenced against PROBE_CROSSWALK.csv directly (not a hardcoded "
        "exclude list), so a member drops off automatically once a crosswalk resolves it "
        "and the report is regenerated."
    )
    lines.append("")

    shown = [g for g in ranked if len(set(r["domain"] for r in g["rows"])) >= min_domains]
    if not shown:
        lines.append("_No candidates at or above the domain-count threshold._")
    else:
        lines.append("| member | domains | example |")
        lines.append("|---|---|---|")
        for g in shown:
            domain_count = len(set(r["domain"] for r in g["rows"]))
            example = g["rows"][0]["example_display"]
            lines.append("| `{}` | {} | `{}` |".format(g["member"], domain_count, example))
        lines.append("")

        for g in shown:
            domain_count = len(set(r["domain"] for r in g["rows"]))
            lines.append("## `{}` ({} domain(s))".format(g["member"], domain_count))
            lines.append("")
            for r in g["rows"]:
                lines.append("- `{}` -- `{}` = `{}`".format(r["domain"], r["key"], r["example_display"]))
            lines.append("")

    try:
        os.makedirs(os.path.dirname(os.path.abspath(out_md)), exist_ok=True)
    except OSError:
        pass
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    default_dir = os.path.dirname(os.path.abspath(__file__))
    parser.add_argument("--probe-inventory-csv", default=os.path.join(default_dir, "PROBE_INVENTORY.csv"))
    parser.add_argument("--probe-crosswalk-csv", default=os.path.join(default_dir, "PROBE_CROSSWALK.csv"))
    parser.add_argument("--out-csv", default=os.path.join(default_dir, "CROSSWALK_CANDIDATES.csv"))
    parser.add_argument("--out-md", default=os.path.join(default_dir, "CROSSWALK_CANDIDATES.md"))
    parser.add_argument(
        "--min-domains", type=int, default=1,
        help="Only show candidates appearing in at least this many domains in the markdown "
        "summary (the CSV always lists everything). Default: 1 (show all).",
    )
    args = parser.parse_args(argv)

    inventory_rows = _read_csv_rows(args.probe_inventory_csv)
    if not inventory_rows:
        print("No inventory rows found at {} -- nothing to do.".format(args.probe_inventory_csv))
        return 1

    crosswalk_rows = _read_csv_rows(args.probe_crosswalk_csv)
    if not crosswalk_rows:
        print(
            "Warning: no crosswalk rows found at {} -- every ElementId-typed member will "
            "show up as a candidate (nothing to cross-reference against yet).".format(
                args.probe_crosswalk_csv
            )
        )

    candidates, resolved_index = find_candidates(inventory_rows, crosswalk_rows)
    ranked = group_by_member(candidates)

    csv_rows = write_csv(ranked, args.out_csv)
    write_markdown(ranked, args.out_md, args.min_domains)

    domains_with_resolved = sum(1 for cols in resolved_index.values() if cols)

    print("Crosswalk candidate scan complete.")
    print("  inventory rows scanned   : {}".format(len(inventory_rows)))
    print("  crosswalk columns known  : {} (across {} domain(s))".format(
        sum(len(v) for v in resolved_index.values()), domains_with_resolved
    ))
    print("  distinct candidate members: {}".format(len(ranked)))
    print("  candidate rows written   : {}".format(csv_rows))
    print("  csv                      : {}".format(args.out_csv))
    print("  markdown                 : {}".format(args.out_md))
    return 0


if __name__ == "__main__":
    sys.exit(main())
