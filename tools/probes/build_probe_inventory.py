# -*- coding: utf-8 -*-
"""
build_probe_inventory.py

Consolidates raw tools/probes/ breadth-probe output into a single curated,
cross-domain inventory: one representative row per (domain, key), deduped
and merged across however many probe runs exist for that domain.

Two file shapes are understood:

  "run" files -- probes_<revit_version>_<run_id>.json
      The current shape, written by runner/probe_thin_runner.py (a batch
      run covering every probe) or by an individual probe_*.py run with
      write_json=True. Structure:
          {"run_metadata": {"run_id":..., "extraction_date":...,
                             "revit_version":..., "tool_version":...,
                             "document": {...}, "probes_run": [...]},
           "domains": {"<domain>": [...same per-probe records as before...]}}
      extraction_date/revit_version/run_id live as JSON metadata here, not
      as filename tokens -- the filename only groups by Revit release
      (revit_version) plus an opaque run_id.

  "legacy" files -- probe_<domain>_<YYYY-MM-DD>.json
      The older shape, one file per probe per manual run, with the date
      baked into the filename and no run_metadata inside the JSON at all.
      Still supported here so historical probe data isn't orphaned; new
      runs should use the "run" shape above.

Each probe already dedupes *within* a single run down to one entry per
param_key/member_key (see e.g. probe_dimension_types.py's param_index).
This tool performs the second layer of dedupe: merging those per-run
entries *across* runs for the same domain, picking one representative
example per key using the same scoring heuristic the probes already use
internally.

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

_LEGACY_FILENAME_RE = re.compile(r"^probe_(?P<domain>.+)_(?P<date>\d{4}-\d{2}-\d{2})\.json$")
_RUN_FILENAME_RE = re.compile(r"^probes_(?P<revit_version>.+)_(?P<run_id>.+)\.json$")

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
    """Returns (run_files, legacy_files, skipped).
    run_files/legacy_files: [(path, meta_from_filename_dict)]
    skipped: [(name, reason)]
    """
    run_files = []
    legacy_files = []
    skipped = []
    try:
        names = sorted(os.listdir(probes_dir))
    except OSError as ex:
        return [], [], [("<probes_dir>", "could not list directory: {}".format(ex))]

    for name in names:
        if not name.endswith(".json"):
            continue
        m_run = _RUN_FILENAME_RE.match(name)
        if m_run:
            run_files.append((os.path.join(probes_dir, name), m_run.groupdict()))
            continue
        m_legacy = _LEGACY_FILENAME_RE.match(name)
        if m_legacy:
            legacy_files.append((os.path.join(probes_dir, name), m_legacy.groupdict()))
            continue
        skipped.append((name, "does not match probes_<revit_version>_<run_id>.json or probe_<domain>_<YYYY-MM-DD>.json"))
    return run_files, legacy_files, skipped


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


def _new_agg():
    return {
        "storage_types": set(),
        "q_counts": {},
        "example": None,
        "seen_tags": set(),  # human-readable "extraction_date|revit_version" or "date" tags
        "revit_versions": set(),
        "source_files": set(),
        "member_kind": None,
        "type_label": None,
    }


def _merge_entries_for_domain(domains, diagnostics, domain, entries, basename, seen_tag, revit_version, warn_prefix, warnings):
    diag = diagnostics.setdefault(domain, {"crosswalk_count": 0, "opaque_count": 0, "unrecognized_entry_count": 0, "seen_tags": []})
    if seen_tag not in diag["seen_tags"]:
        diag["seen_tags"].append(seen_tag)

    if not isinstance(entries, list):
        diag["unrecognized_entry_count"] += 1
        warnings.append("{}: domain '{}' entries is not a list; skipped".format(warn_prefix, domain))
        return

    for entry in entries:
        if not isinstance(entry, dict):
            diag["unrecognized_entry_count"] += 1
            continue

        kind = entry.get("kind")
        if kind == "crosswalk":
            diag["crosswalk_count"] += len(entry.get("records") or [])
            continue

        if kind not in _MERGE_KINDS:
            # Either an unrecognized "kind", or (in the "run" shape) a
            # nonstandard-shape probe's raw OUT dict with no "kind" at all
            # (e.g. a findings-style probe). Count it so it isn't silently
            # dropped without a trace, but there is nothing key-shaped to
            # merge.
            diag["unrecognized_entry_count"] += 1
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
                agg = _new_agg()
                bkeys[key] = agg

            agg["seen_tags"].add(seen_tag)
            if revit_version:
                agg["revit_versions"].add(revit_version)
            agg["source_files"].add(basename)
            if rec.get("member_kind"):
                agg["member_kind"] = rec.get("member_kind")
            if rec.get("type_label"):
                agg["type_label"] = rec.get("type_label")

            _merge_observed(agg, rec.get("observed"))

            candidate = rec.get("example")
            if _example_score(candidate) > _example_score(agg["example"]):
                agg["example"] = candidate


def merge_probe_files(run_files, legacy_files, warnings):
    """
    Returns (domains, diagnostics).
    domains: OrderedDict domain -> bucket ("param"|"reflection") -> key -> aggregate dict.
    diagnostics: OrderedDict domain -> {"crosswalk_count", "opaque_count",
                 "unrecognized_entry_count", "seen_tags": [...]}
    """
    domains = OrderedDict()
    diagnostics = OrderedDict()

    for path, fname_meta in run_files:
        basename = os.path.basename(path)
        try:
            payload = load_payload(path)
        except (OSError, ValueError) as ex:
            warnings.append("{}: failed to parse JSON ({})".format(basename, ex))
            continue

        if not isinstance(payload, dict) or "domains" not in payload:
            warnings.append("{}: does not look like a run-shaped file (missing 'domains'); skipped".format(basename))
            continue

        run_meta = payload.get("run_metadata") or {}
        revit_version = run_meta.get("revit_version") or fname_meta.get("revit_version")
        extraction_date = run_meta.get("extraction_date")
        run_id = run_meta.get("run_id") or fname_meta.get("run_id")
        seen_tag = extraction_date or run_id or basename

        domains_payload = payload.get("domains")
        if not isinstance(domains_payload, dict):
            warnings.append("{}: 'domains' is not an object; skipped".format(basename))
            continue

        for domain, entries in domains_payload.items():
            _merge_entries_for_domain(
                domains, diagnostics, domain, entries, basename, seen_tag, revit_version,
                warn_prefix=basename, warnings=warnings,
            )

    for path, fname_meta in legacy_files:
        basename = os.path.basename(path)
        try:
            payload = load_payload(path)
        except (OSError, ValueError) as ex:
            warnings.append("{}: failed to parse JSON ({})".format(basename, ex))
            continue

        if not isinstance(payload, list):
            warnings.append("{}: top-level JSON is not a list (legacy shape expected); skipped".format(basename))
            continue

        fname_domain = fname_meta.get("domain")
        date = fname_meta.get("date")

        # Legacy files are a flat list of {"kind":..., "domain":..., "records":[...]}
        # entries (no separate "domains" dict) -- group them by declared
        # domain (falling back to the filename domain) before merging.
        by_domain = OrderedDict()
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            d = entry.get("domain") or fname_domain
            if entry.get("domain") and entry.get("domain") != fname_domain:
                warnings.append(
                    "{}: declared domain '{}' != filename domain '{}'; using declared domain".format(
                        basename, entry.get("domain"), fname_domain
                    )
                )
            by_domain.setdefault(d, []).append(entry)

        for domain, entries in by_domain.items():
            _merge_entries_for_domain(
                domains, diagnostics, domain, entries, basename, date, None,
                warn_prefix=basename, warnings=warnings,
            )

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
                        "revit_versions_seen": ";".join(sorted(agg["revit_versions"])),
                        "first_seen": min(agg["seen_tags"]) if agg["seen_tags"] else "",
                        "last_seen": max(agg["seen_tags"]) if agg["seen_tags"] else "",
                        "run_count": len(agg["seen_tags"]),
                        "source_files": ";".join(sorted(agg["source_files"])),
                    }
                )

    fieldnames = [
        "domain", "key_kind", "key", "member_kind", "type_label",
        "storage_types", "q_counts", "unique_value_count",
        "example_q", "example_storage", "example_raw", "example_display", "example_norm",
        "revit_versions_seen", "first_seen", "last_seen", "run_count", "source_files",
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


def write_markdown(domains, diagnostics, out_md, skipped, warnings, domain_module_names, run_file_count, legacy_file_count):
    lines = []
    lines.append("# Probe Inventory (auto-generated)")
    lines.append("")
    lines.append(
        "Generated by `tools/probes/build_probe_inventory.py`. Do not hand-edit -- "
        "rerun the script after adding/updating probe output files."
    )
    lines.append("")
    lines.append(
        "Each row is one representative observation per `(domain, key)`, merged "
        "across every probe run found for that domain -- both current "
        "`probes_<revit_version>_<run_id>.json` run files ({} found) and legacy "
        "`probe_<domain>_<date>.json` files ({} found). `key_kind=param` rows come "
        "from a probe's dynamic/curated Parameter or property capture; "
        "`key_kind=reflection` rows come from a `.NET` reflection sweep "
        "(properties/zero-arg methods) layered on top, where present.".format(
            run_file_count, legacy_file_count
        )
    )
    lines.append("")

    lines.append("## Source runs")
    lines.append("")
    lines.append("| domain | runs | revit versions seen | crosswalk records | opaque records | unrecognized entries |")
    lines.append("|---|---|---|---|---|---|")
    for domain in sorted(diagnostics.keys()):
        diag = diagnostics[domain]
        tags = ", ".join(sorted(diag["seen_tags"]))
        versions = set()
        for bucket in ("param", "reflection"):
            for agg in domains.get(domain, {}).get(bucket, {}).values():
                versions.update(agg.get("revit_versions") or [])
        lines.append(
            "| `{}` | {} | {} | {} | {} | {} |".format(
                domain, len(diag["seen_tags"]),
                ", ".join(sorted(versions)) or "(unknown)",
                diag["crosswalk_count"], diag["opaque_count"], diag["unrecognized_entry_count"],
            )
        )
    lines.append("")
    if diagnostics:
        lines.append("`runs` counts distinct `extraction_date`/legacy-filename-date tags seen for that domain; see the CSV's `first_seen`/`last_seen` columns for the actual timestamps.")
        lines.append("")

    if domain_module_names is not None:
        probed = set(diagnostics.keys())
        missing = sorted(set(domain_module_names) - probed)
        lines.append("## Domain coverage")
        lines.append("")
        lines.append(
            "Active domain modules under `domains/` with **no** probe data present at all "
            "(nothing to curate from -- not the same as \"probed and found empty\"):"
        )
        lines.append("")
        if missing:
            for m in missing:
                lines.append("- `{}`".format(m))
        else:
            lines.append("- (none -- every domain module has at least one probe run)")
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
                lines.append("  - revit_versions_seen — `{}`".format(", ".join(sorted(agg["revit_versions"])) or "(unknown)"))
                lines.append("  - seen — {} run(s), {}–{}".format(
                    len(agg["seen_tags"]),
                    min(agg["seen_tags"]) if agg["seen_tags"] else "?",
                    max(agg["seen_tags"]) if agg["seen_tags"] else "?",
                ))
                lines.append("")

    try:
        os.makedirs(os.path.dirname(os.path.abspath(out_md)), exist_ok=True)
    except OSError:
        pass
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def build(probes_dir, out_md, out_csv, domains_dir, force=False):
    warnings = []
    run_files, legacy_files, skipped = discover_probe_files(probes_dir)
    domains, diagnostics = merge_probe_files(run_files, legacy_files, warnings)

    if not domains and not force:
        # Refuse to clobber a populated CSV/Markdown with empty output.
        # This covers two cases: (a) no probe_*.json/probes_*.json inputs
        # were found at all, and (b) inputs were found by filename but every
        # one of them failed to parse/validate (e.g. a truncated or
        # wrong-shaped probes_*.json) -- merge_probe_files() only records a
        # warning for (b) and would otherwise return empty domains here just
        # the same as (a). Pass force=True (--force on the CLI) if an empty
        # inventory is actually intended (e.g. a brand-new repo).
        return {
            "run_files_matched": len(run_files),
            "legacy_files_matched": len(legacy_files),
            "files_skipped": len(skipped),
            "domains": 0,
            "csv_rows": None,
            "warnings": warnings,
            "skipped": skipped,
            "refused_empty_rebuild": True,
        }

    domain_module_names = scan_domain_coverage(domains_dir) if domains_dir else None
    row_count = write_csv(domains, out_csv, warnings)
    write_markdown(domains, diagnostics, out_md, skipped, warnings, domain_module_names, len(run_files), len(legacy_files))
    return {
        "run_files_matched": len(run_files),
        "legacy_files_matched": len(legacy_files),
        "files_skipped": len(skipped),
        "domains": len(domains),
        "csv_rows": row_count,
        "warnings": warnings,
        "skipped": skipped,
        "refused_empty_rebuild": False,
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
    parser.add_argument(
        "--force",
        action="store_true",
        help="Write out_md/out_csv even when zero probe_*.json/probes_*.json inputs "
        "are found. Without this, a no-input run is refused (exit code 1) instead "
        "of silently overwriting a previously-populated inventory with an empty one.",
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

    result = build(probes_dir, out_md, out_csv, domains_dir, force=args.force)

    if result.get("refused_empty_rebuild"):
        matched = result["run_files_matched"] + result["legacy_files_matched"]
        if matched == 0:
            print("Refused: no probe_*.json/probes_*.json inputs found under {}.".format(probes_dir))
        else:
            print(
                "Refused: {} input file(s) matched under {} but none parsed into usable "
                "domain data (see warnings below).".format(matched, probes_dir)
            )
        print("Not overwriting {} / {} with empty output.".format(out_md, out_csv))
        if result["skipped"]:
            print("  files skipped         : {}".format(len(result["skipped"])))
            for name, reason in result["skipped"][:10]:
                print("    - {}: {}".format(name, reason))
        if result["warnings"]:
            print("  warnings              : {}".format(len(result["warnings"])))
            for w in result["warnings"][:10]:
                print("    - {}".format(w))
        print("Pass --force if an empty inventory is actually intended.")
        return 1

    print("Probe inventory build complete.")
    print("  run files matched    : {}".format(result["run_files_matched"]))
    print("  legacy files matched : {}".format(result["legacy_files_matched"]))
    print("  files skipped        : {}".format(result["files_skipped"]))
    print("  domains covered      : {}".format(result["domains"]))
    print("  csv rows written     : {}".format(result["csv_rows"]))
    print("  markdown             : {}".format(out_md))
    print("  csv                  : {}".format(out_csv))
    if result["warnings"]:
        print("  warnings             : {}".format(len(result["warnings"])))
        for w in result["warnings"][:10]:
            print("    - {}".format(w))
    return 0


if __name__ == "__main__":
    sys.exit(main())
