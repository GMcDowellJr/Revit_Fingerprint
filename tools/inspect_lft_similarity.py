"""
inspect_lft_similarity.py  (v5)

Single-pass analysis of loaded_family_types similarity across a container corpus.

Groups records by sig_hash (v3 policy). Optionally enriches with:
  - Dimensional profiles (width/height/depth etc from parameter values)
  - Classification profiles (manufacturer/model/material modal values)
  - Sub-group detection (sig_hash groups that split on classification values)

All parameter_rows enrichment happens in a single stream pass.

Inputs:
  records.csv         -- required
  parameter_rows.csv  -- optional; enables dimensional + classification enrichment
  file_metadata.csv   -- optional; unit_system per file (defaults to imperial)

Outputs:
  lft_exact_matches.csv    -- sig_hash groups + optional dim/classification columns
  lft_name_clusters.csv    -- name-similar clusters with sig_hash divergence
  lft_param_subgroups.csv  -- sig_hash groups split by stable classification values
                              (only written when --parameter-rows supplied)
  lft_detail_family_file_rows.csv
                            -- row-level mapping of sig_hash + actual family_name
                               + export_run_id for exact-match groups

Usage:
    python inspect_lft_similarity.py \
        --records         "C:\\...\\results\\records\\records.csv" \
        --out-dir         "C:\\...\\similarity_analysis" \
        [--parameter-rows "C:\\...\\results\\records\\parameter_rows.csv"] \
        [--file-metadata  "C:\\...\\results\\records\\file_metadata.csv"] \
        [--min-files 2] [--sim-threshold 0.5] [--max-diversity 0.3]
"""

import argparse
import csv
import os
import sys
import re
from collections import defaultdict, Counter


# ---------------------------------------------------------------------------
# Label helpers
# ---------------------------------------------------------------------------

def extract_category(label):
    return label.split(" : ", 1)[0].strip() if " : " in label else ""

def extract_family_name(label):
    return label.split(" : ", 1)[1].strip() if " : " in label else label.strip()

def normalise_name(name):
    name = name.lower()
    name = re.sub(r"\b\d[\d\.\-\s]*(?:mm|cm|m|in|ft)?\b", "", name)
    return frozenset(t for t in re.split(r"[\s\-_/\\]+", name) if len(t) > 1)

def token_overlap(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# ---------------------------------------------------------------------------
# Dimensional profiling
# ---------------------------------------------------------------------------

DIMENSION_BUCKETS = {
    "width":     ["width", "wide", "breadth"],
    "height":    ["height", "tall"],
    "depth":     ["depth", "projection"],
    "length":    ["length", "long"],
    "radius":    ["radius", "rad"],
    "diameter":  ["diameter", "dia", "bore"],
    "thickness": ["thickness", "thk", "thick"],
}

DIM_EXCLUDE_TOKENS = {
    "type", "name", "family", "mark", "comment", "category",
    "description", "keynote", "model", "manufacturer", "url",
    "image", "cost", "phase", "workset", "assembly",
    "angle", "rotation", "level", "area", "volume", "slope",
}

_FT_TO_MM = 304.8

def match_dim_bucket(param_name):
    tokens = set(re.split(r"[\s\-_/\(\)\.]+", param_name.lower())) - {""}
    if tokens & DIM_EXCLUDE_TOKENS:
        return None
    for bucket, keywords in DIMENSION_BUCKETS.items():
        for kw in keywords:
            if set(kw.split()) <= tokens:
                return bucket
    return None

def parse_raw_values(raw_set):
    if not raw_set:
        return []
    nums = []
    for part in raw_set.split("|"):
        try:
            v = float(part.strip())
            if v > 0:
                nums.append(v)
        except ValueError:
            pass
    return nums

def format_range(values, unit_system="imperial"):
    if not values:
        return ""
    if unit_system == "imperial":
        values = [v * _FT_TO_MM for v in values]
    mn, mx = min(values), max(values)
    fmt = "{:.0f}" if mn >= 1.0 else "{:.2f}"
    suffix = " mm"
    if abs(mx - mn) < 0.5:
        return fmt.format(mn) + suffix
    return "{} - {}{}".format(fmt.format(mn), fmt.format(mx), suffix)


# ---------------------------------------------------------------------------
# Classification profiling
# ---------------------------------------------------------------------------

CLASSIFICATION_KEYWORDS = {
    "manufacturer", "model", "description", "url",
    "material", "finish", "coating", "rating", "pressure",
    "type mark", "keynote", "omniclass",
}

CLASS_EXCLUDE_TOKENS = {
    "workset", "edited", "owner", "ifcguid", "phase",
    "design option", "assembly code",
}

def is_classification_param(param_name):
    name_lower = param_name.lower()
    if any(t in name_lower for t in CLASS_EXCLUDE_TOKENS):
        return False
    return any(k in name_lower for k in CLASSIFICATION_KEYWORDS)


# ---------------------------------------------------------------------------
# Unit system lookup
# ---------------------------------------------------------------------------

def load_unit_systems(file_metadata_path):
    us = {}
    if not file_metadata_path or not os.path.isfile(file_metadata_path):
        return us
    with open(file_metadata_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            us[row["export_run_id"]] = row.get("unit_system", "imperial") or "imperial"
    return us


# ---------------------------------------------------------------------------
# Step 1: Load records
# ---------------------------------------------------------------------------

def load_lft_records(records_path):
    rows           = []
    pk_to_sig      = {}       # record_pk -> sig_hash  (for param_rows join)
    pk_to_run      = {}       # record_pk -> export_run_id
    sig_to_run_ids = defaultdict(set)
    skipped        = 0

    print("  Streaming records.csv...")
    with open(records_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("domain") != "loaded_family_types":
                continue
            if row.get("status") == "blocked":
                skipped += 1
                continue
            label  = row.get("label_display", "")
            sh     = row.get("sig_hash", "")
            run_id = row["export_run_id"]
            pk     = row["record_pk"]
            rows.append({
                "export_run_id": run_id,
                "sig_hash":      sh,
                "category":      extract_category(label),
                "family_name":   extract_family_name(label),
            })
            if sh and pk:
                pk_to_sig[pk] = sh
                pk_to_run[pk] = run_id
                sig_to_run_ids[sh].add(run_id)

    distinct_files = len(set(r["export_run_id"] for r in rows))
    print("  LFT records: {:,}  |  blocked: {:,}  |  files: {:,}".format(
        len(rows), skipped, distinct_files))
    return rows, pk_to_sig, pk_to_run, sig_to_run_ids


# ---------------------------------------------------------------------------
# Step 2: Single-pass parameter_rows stream
#   - collects dimensional raw values per (sig_hash, bucket)
#   - collects classification value votes per (sig_hash, param_key, run_id)
# ---------------------------------------------------------------------------

def stream_parameter_rows(parameter_rows_path, pk_to_sig, pk_to_run, unit_systems):
    # Dimensional: sig_hash -> {bucket -> [raw float values]}
    dim_accum   = defaultdict(lambda: defaultdict(list))
    dim_pnames  = defaultdict(lambda: defaultdict(set))

    # Classification: (sig_hash, param_key) -> {run_id -> Counter(value)}
    class_accum = defaultdict(lambda: {
        "name": "", "run_counters": defaultdict(Counter)
    })

    rows_read = dim_hits = class_hits = 0
    print("  Streaming parameter_rows.csv (single pass)...")

    with open(parameter_rows_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_read += 1
            if rows_read % 5_000_000 == 0:
                print("    {:,}M rows  dim={:,}  class={:,}".format(
                    rows_read // 1_000_000, dim_hits, class_hits))

            pk = row.get("record_pk", "")
            if pk not in pk_to_sig:
                continue

            sh     = pk_to_sig[pk]
            run_id = pk_to_run[pk]
            pname  = row.get("lftp.name", "")
            if not pname:
                continue

            # --- Dimensional ---
            bucket = match_dim_bucket(pname)
            if bucket:
                vals = parse_raw_values(row.get("lftp.value_raw_set", ""))
                if vals:
                    dim_accum[sh][bucket].extend(vals)
                    dim_pnames[sh][bucket].add(pname)
                    dim_hits += 1

            # --- Classification ---
            if is_classification_param(pname):
                param_key = row.get("lftp.key", "")
                value_set = row.get("lftp.value_set", "") or ""
                values    = [v.strip() for v in value_set.split("|") if v.strip()]
                if values and param_key:
                    key = (sh, param_key)
                    class_accum[key]["name"] = pname
                    for v in values:
                        class_accum[key]["run_counters"][run_id][v] += 1
                    class_hits += 1

    print("  Rows read: {:,}  |  dim hits: {:,}  |  class hits: {:,}".format(
        rows_read, dim_hits, class_hits))
    return dim_accum, dim_pnames, class_accum


# ---------------------------------------------------------------------------
# Build dimensional summaries per sig_hash
# ---------------------------------------------------------------------------

def build_dim_summaries(dim_accum, dim_pnames, sig_to_run_ids, unit_systems):
    summaries = {}
    for sh, buckets in dim_accum.items():
        run_ids     = sig_to_run_ids.get(sh, set())
        us_votes    = [unit_systems.get(r, "imperial") for r in run_ids]
        unit_system = "metric" if us_votes.count("metric") > len(us_votes) / 2 else "imperial"
        row = {}
        for bucket in DIMENSION_BUCKETS:
            vals   = sorted(set(buckets.get(bucket, [])))
            pnames = sorted(dim_pnames[sh].get(bucket, set()))
            row["dim_{}".format(bucket)]             = format_range(vals, unit_system)
            row["dim_{}_param_names".format(bucket)] = " | ".join(pnames[:3])
        summaries[sh] = row
    return summaries


# ---------------------------------------------------------------------------
# Build classification modal profiles per (sig_hash, param_key)
# ---------------------------------------------------------------------------

def build_class_profiles(class_accum, sig_to_run_ids, max_diversity=0.3, min_modal_pct=0.5):
    """
    Returns:
      profiles: list of profile dicts (for lft_param_subgroups.csv)
      sig_class_summary: sig_hash -> {param_name -> modal_value}
                         (stable classification params only — for exact matches)
    """
    profiles          = []
    sig_class_summary = defaultdict(dict)

    for (sh, param_key), data in class_accum.items():
        run_counters = data["run_counters"]
        param_name   = data["name"]
        total_files  = len(sig_to_run_ids.get(sh, set()))
        files_with   = len(run_counters)
        if not files_with:
            continue

        # One vote per file = modal value within that file
        file_votes   = [c.most_common(1)[0][0] for c in run_counters.values() if c]
        vote_counter = Counter(file_votes)
        modal_val, modal_count = vote_counter.most_common(1)[0]
        modal_pct    = modal_count / files_with
        diversity    = len(vote_counter) / files_with
        coverage_pct = 100 * files_with / total_files if total_files else 0

        all_values = " | ".join(
            "{}({})".format(v, c) for v, c in vote_counter.most_common(8)
        )

        profiles.append({
            "sig_hash":           sh,
            "param_key":          param_key,
            "param_name":         param_name,
            "total_files":        total_files,
            "files_with_param":   files_with,
            "coverage_pct":       round(coverage_pct, 1),
            "modal_value":        modal_val,
            "modal_pct":          round(modal_pct, 3),
            "distinct_values":    len(vote_counter),
            "value_diversity":    round(diversity, 3),
            "all_values":         all_values,
        })

        # Add to summary if stable enough
        if (diversity <= max_diversity
                and modal_pct >= min_modal_pct
                and coverage_pct >= 50):
            sig_class_summary[sh][param_name] = modal_val

    profiles.sort(key=lambda r: (r["sig_hash"], r["modal_pct"]))
    return profiles, sig_class_summary


# ---------------------------------------------------------------------------
# Build sub-groups: sig_hashes that split on classification values
# ---------------------------------------------------------------------------

def build_subgroups(class_profiles, sig_to_run_ids, sig_to_label, sig_to_cat,
                    max_diversity=0.3, min_modal_pct=0.5):
    # Index stable discriminating params per sig_hash
    sig_params = defaultdict(list)
    for p in class_profiles:
        if (p["value_diversity"] <= max_diversity
                and p["modal_pct"] >= min_modal_pct
                and p["coverage_pct"] >= 50
                and p["distinct_values"] > 1):   # must actually split the population
            sig_params[p["sig_hash"]].append(p)

    subgroups = []
    sgid      = 0
    for sh, params in sig_params.items():
        # Pick most discriminating param (most distinct values while still stable)
        params_sorted = sorted(params, key=lambda p: -p["distinct_values"])
        best          = params_sorted[0]
        other_params  = " | ".join(p["param_name"] for p in params_sorted[1:5])

        subgroups.append({
            "subgroup_id":          "SG{:04d}".format(sgid),
            "sig_hash":             sh,
            "category":             sig_to_cat.get(sh, ""),
            "representative_name":  sig_to_label.get(sh, ""),
            "total_files":          len(sig_to_run_ids.get(sh, set())),
            "discriminating_param": best["param_name"],
            "value_diversity":      best["value_diversity"],
            "modal_value":          best["modal_value"],
            "modal_files":          round(best["modal_pct"] * best["files_with_param"]),
            "all_values":           best["all_values"],
            "other_stable_params":  other_params,
        })
        sgid += 1

    subgroups.sort(key=lambda r: (-r["total_files"], r["category"]))
    return subgroups


# ---------------------------------------------------------------------------
# Tier 1: Exact match table
# ---------------------------------------------------------------------------

def build_exact_match_table(rows, dim_summaries, sig_class_summary,
                             sig_to_run_ids, min_files=2):
    groups = defaultdict(lambda: {
        "run_ids": set(), "names": defaultdict(int), "category": ""
    })
    for row in rows:
        sh = row["sig_hash"]
        if not sh:
            continue
        g = groups[sh]
        g["run_ids"].add(row["export_run_id"])
        if row["family_name"]:
            g["names"][row["family_name"]] += 1
        if row["category"] and not g["category"]:
            g["category"] = row["category"]

    results = []
    for sig_hash, data in groups.items():
        fc = len(data["run_ids"])
        if fc < min_files:
            continue
        names    = data["names"]
        rep      = max(names, key=names.get) if names else ""
        distinct = sorted(names.keys())
        result   = {
            "sig_hash":            sig_hash,
            "category":            data["category"],
            "representative_name": rep,
            "file_count":          fc,
            "name_variant_count":  len(distinct),
            "name_variants":       " | ".join(distinct[:10]),
            "file_ids":            " | ".join(sorted(data["run_ids"])),
        }
        # Dimensional columns
        if dim_summaries:
            dims = dim_summaries.get(sig_hash, {})
            for bucket in DIMENSION_BUCKETS:
                result["dim_{}".format(bucket)]             = dims.get("dim_{}".format(bucket), "")
                result["dim_{}_param_names".format(bucket)] = dims.get("dim_{}_param_names".format(bucket), "")
        # Classification columns — top 3 stable classification params
        if sig_class_summary:
            class_vals = sig_class_summary.get(sig_hash, {})
            # Prioritise manufacturer, model, material
            priority = ["manufacturer", "model", "material", "finish",
                        "rating", "pressure", "description"]
            shown = []
            for key in priority:
                for pname, val in class_vals.items():
                    if key in pname.lower() and len(shown) < 3:
                        shown.append("{}={}".format(pname, val))
            # Fill remaining slots from any other stable params
            for pname, val in class_vals.items():
                if len(shown) >= 3:
                    break
                entry = "{}={}".format(pname, val)
                if entry not in shown:
                    shown.append(entry)
            result["classification_profile"] = " | ".join(shown[:3])
            result["classification_splits"]  = "yes" if len(class_vals) > 0 else "no"
        results.append(result)

    results.sort(key=lambda r: (-r["file_count"], r["category"], r["representative_name"]))
    return results


# ---------------------------------------------------------------------------
# Tier 2: Name cluster table
# ---------------------------------------------------------------------------

def build_name_cluster_table(rows, sig_class_summary, sim_threshold=0.5):
    by_cat = defaultdict(list)
    for row in rows:
        if row["category"] and row["family_name"] and row["sig_hash"]:
            by_cat[row["category"]].append(
                (row["family_name"], row["sig_hash"], row["export_run_id"])
            )

    clusters = []
    cid = 0
    for cat, entries in sorted(by_cat.items()):
        ns = defaultdict(set)
        for fname, sh, run_id in entries:
            ns[(fname, sh)].add(run_id)

        unique = [(f, sh, len(files)) for (f, sh), files in ns.items()]
        tok    = [(f, sh, fc, normalise_name(f)) for f, sh, fc in unique]
        seen   = [False] * len(tok)

        for i, (fi, shi, fci, toki) in enumerate(tok):
            if seen[i]:
                continue
            seen[i] = True
            members = [(fi, shi, fci)]
            for j, (fj, shj, fcj, tokj) in enumerate(tok):
                if seen[j] or i == j:
                    continue
                if token_overlap(toki, tokj) >= sim_threshold:
                    members.append((fj, shj, fcj))
                    seen[j] = True
            if len(members) < 2:
                continue

            shs   = sorted(set(m[1] for m in members))
            names = sorted(set(m[0] for m in members))

            # Classification context: do the sig_hashes in this cluster
            # differ on a stable classification parameter?
            class_discrimination = ""
            if sig_class_summary and len(shs) > 1:
                # Find params where different sig_hashes have different modal values
                discriminators = []
                all_params = set()
                for sh in shs:
                    all_params.update(sig_class_summary.get(sh, {}).keys())
                for pname in all_params:
                    vals = set()
                    for sh in shs:
                        v = sig_class_summary.get(sh, {}).get(pname)
                        if v:
                            vals.add(v)
                    if len(vals) > 1:
                        discriminators.append("{}:{}".format(
                            pname, " vs ".join(sorted(vals)[:3])))
                class_discrimination = " | ".join(discriminators[:3])

            clusters.append({
                "cluster_id":             "C{:04d}".format(cid),
                "category":               cat,
                "member_count":           len(members),
                "sig_hash_count":         len(shs),
                "total_file_appearances": sum(m[2] for m in members),
                "fragmented":             "yes" if len(shs) > 1 else "no",
                "class_discrimination":   class_discrimination,
                "sig_hashes":             " | ".join(shs[:5]),
                "family_names":           " | ".join(names[:10]),
            })
            cid += 1

    clusters.sort(key=lambda c: (
        -c["sig_hash_count"], -c["total_file_appearances"], c["category"]
    ))
    return clusters


# ---------------------------------------------------------------------------
# Detail file writers — normalise pipe-delimited fields to one row per value
# ---------------------------------------------------------------------------

def write_detail_file(path, rows, id_field, id_label, value_field, value_label,
                      extra_fields=None):
    """
    Explode a pipe-delimited column into a normalised detail file.
    extra_fields: list of (field_name, label) to carry through from parent row.
    """
    extra_fields = extra_fields or []
    fieldnames   = [id_label] + [f[1] for f in extra_fields] + [value_label]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    written = 0
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            raw = row.get(value_field, "") or ""
            values = [v.strip() for v in raw.split("|") if v.strip()]
            for val in values:
                out = {id_label: row[id_field], value_label: val}
                for src, lbl in extra_fields:
                    out[lbl] = row.get(src, "")
                w.writerow(out)
                written += 1
    print("  Written: {} ({:,} rows)".format(path, written))
    return written


def write_detail_file_kv(path, rows, id_field, id_label, value_field, value_label,
                          count_label=None, extra_fields=None):
    """
    Explode pipe-delimited 'value(count)' entries into normalised rows.
    Format: "value1(count1) | value2(count2)"
    """
    import re as _re
    extra_fields = extra_fields or []
    fieldnames   = [id_label] + [f[1] for f in extra_fields] + [value_label]
    if count_label:
        fieldnames.append(count_label)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    written = 0
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            raw = row.get(value_field, "") or ""
            for entry in raw.split("|"):
                entry = entry.strip()
                if not entry:
                    continue
                m = _re.match(r"^(.*)\((\d+)\)$", entry)
                if m:
                    val, count = m.group(1).strip(), int(m.group(2))
                else:
                    val, count = entry, None
                out = {id_label: row[id_field], value_label: val}
                if count_label:
                    out[count_label] = count if count is not None else ""
                for src, lbl in extra_fields:
                    out[lbl] = row.get(src, "")
                w.writerow(out)
                written += 1
    print("  Written: {} ({:,} rows)".format(path, written))
    return written


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

def write_csv(path, rows, fieldnames):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print("  Written: {} ({:,} rows)".format(path, len(rows)))



def build_family_file_detail(rows, exact_rows):
    """
    Preserve the row-level relationship needed to trace suspected duplicate
    families back to the exact container files where each family name appears.

    Existing detail outputs intentionally normalize names and file ids into
    separate tables, which is useful for summaries but loses this pairing:

        sig_hash + family_name + export_run_id

    This detail table restores that pairing for every sig_hash retained in
    lft_exact_matches.csv after --min-files / --top-n filtering.
    """
    exact_meta = {
        r["sig_hash"]: {
            "representative_name": r.get("representative_name", ""),
            "duplicate_file_count": r.get("file_count", ""),
            "name_variant_count": r.get("name_variant_count", ""),
            "classification_profile": r.get("classification_profile", ""),
            "classification_splits": r.get("classification_splits", ""),
        }
        for r in exact_rows
        if r.get("sig_hash")
    }
    exact_sig_hashes = set(exact_meta.keys())

    counts = Counter()
    for row in rows:
        sh = row.get("sig_hash", "")
        if not sh or sh not in exact_sig_hashes:
            continue
        key = (
            sh,
            row.get("category", ""),
            row.get("family_name", ""),
            row.get("export_run_id", ""),
        )
        counts[key] += 1

    detail_rows = []
    for (sh, category, family_name, export_run_id), record_count in sorted(counts.items()):
        meta = exact_meta.get(sh, {})
        detail_rows.append({
            "sig_hash": sh,
            "category": category,
            "family_name": family_name,
            "export_run_id": export_run_id,
            "record_count": record_count,
            "representative_name": meta.get("representative_name", ""),
            "duplicate_file_count": meta.get("duplicate_file_count", ""),
            "name_variant_count": meta.get("name_variant_count", ""),
            "classification_profile": meta.get("classification_profile", ""),
            "classification_splits": meta.get("classification_splits", ""),
        })
    return detail_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="LFT similarity — exact matches, clusters, dimensions, classification."
    )
    parser.add_argument("--records",         required=True)
    parser.add_argument("--out-dir",         required=True)
    parser.add_argument("--parameter-rows",  default=None)
    parser.add_argument("--file-metadata",   default=None)
    parser.add_argument("--min-files",       type=int,   default=2)
    parser.add_argument("--sim-threshold",   type=float, default=0.5)
    parser.add_argument("--max-diversity",   type=float, default=0.3)
    parser.add_argument("--min-modal-pct",   type=float, default=0.5)
    parser.add_argument("--top-n",           type=int,   default=None)
    args = parser.parse_args()

    if not os.path.isfile(args.records):
        print("ERROR: records not found:", args.records)
        sys.exit(1)

    print("Step 1: Load LFT records")
    rows, pk_to_sig, pk_to_run, sig_to_run_ids = load_lft_records(args.records)

    # Build sig_to_label / sig_to_cat from rows
    sig_to_cat   = {}
    sig_to_label = {}
    for r in rows:
        sh = r["sig_hash"]
        if sh and sh not in sig_to_cat:
            sig_to_cat[sh]   = r["category"]
            sig_to_label[sh] = r["family_name"]

    dim_summaries    = {}
    sig_class_summary = {}
    subgroups        = []

    if args.parameter_rows:
        if not os.path.isfile(args.parameter_rows):
            print("ERROR: parameter-rows not found:", args.parameter_rows)
            sys.exit(1)

        print("\nStep 2: Load unit systems")
        unit_systems = load_unit_systems(args.file_metadata)
        print("  Unit systems: {:,} files  ({})".format(
            len(unit_systems),
            "from file_metadata" if unit_systems else "defaulting to imperial"))

        print("\nStep 3: Single-pass parameter_rows stream")
        dim_accum, dim_pnames, class_accum = stream_parameter_rows(
            args.parameter_rows, pk_to_sig, pk_to_run, unit_systems)

        print("\nStep 4: Build dimensional summaries")
        dim_summaries = build_dim_summaries(dim_accum, dim_pnames, sig_to_run_ids, unit_systems)
        print("  Sig hashes with dimensional data: {:,}".format(len(dim_summaries)))

        print("\nStep 5: Build classification profiles")
        class_profiles, sig_class_summary = build_class_profiles(
            class_accum, sig_to_run_ids,
            max_diversity=args.max_diversity,
            min_modal_pct=args.min_modal_pct,
        )
        stable_class = sum(1 for sh, d in sig_class_summary.items() if d)
        print("  Sig hashes with stable classification params: {:,}".format(stable_class))

        print("\nStep 6: Build sub-groups")
        subgroups = build_subgroups(
            class_profiles, sig_to_run_ids, sig_to_label, sig_to_cat,
            max_diversity=args.max_diversity,
            min_modal_pct=args.min_modal_pct,
        )
        print("  Sub-groups (sig_hashes splitting on classification): {:,}".format(
            len(subgroups)))
        if subgroups:
            print("\n  Top 10 sub-groups:")
            for sg in subgroups[:10]:
                print("    {:>3}fc | {:<22} | {:<30} | {} | {}".format(
                    sg["total_files"], sg["category"][:22],
                    sg["representative_name"][:30],
                    sg["discriminating_param"][:20],
                    sg["all_values"][:40]))
    else:
        print("\n(--parameter-rows not supplied — skipping enrichment)")

    print("\nTier 1: Exact matches (min_files={})".format(args.min_files))
    exact = build_exact_match_table(
        rows, dim_summaries, sig_class_summary, sig_to_run_ids,
        min_files=args.min_files)
    if args.top_n:
        exact = exact[:args.top_n]
    print("  Found: {:,}".format(len(exact)))
    if exact:
        print("\n  Top 10:")
        for r in exact[:10]:
            cp = r.get("classification_profile", "")
            dims = [b for b in DIMENSION_BUCKETS if r.get("dim_{}".format(b))]
            print("    {:>3}fc | {:>2}n | {:<22} | {:<30} | dims:[{}] class:[{}]".format(
                r["file_count"], r["name_variant_count"],
                r["category"][:22], r["representative_name"][:30],
                ",".join(d[:3] for d in dims) if dims else "-",
                cp[:35] if cp else "-"))

    base_fields = ["sig_hash", "category", "representative_name",
                   "file_count", "name_variant_count", "name_variants", "file_ids"]
    dim_fields  = [f for b in DIMENSION_BUCKETS
                   for f in ("dim_{}".format(b), "dim_{}_param_names".format(b))
                   ] if dim_summaries else []
    class_fields = ["classification_profile", "classification_splits"] \
                   if sig_class_summary else []

    # Drop pipe-delimited fields from primary CSV; write to detail files instead
    exact_primary_fields = (
        ["sig_hash", "category", "representative_name",
         "file_count", "name_variant_count", "file_count"]
        + ["sig_hash", "category", "representative_name",
           "file_count", "name_variant_count", "classification_profile",
           "classification_splits"]
        + ["dim_{}".format(b) for b in DIMENSION_BUCKETS]
    )
    # Build clean primary field list (no pipe-delimited multi-value columns)
    exact_clean_fields = (
        ["sig_hash", "category", "representative_name",
         "file_count", "name_variant_count", "classification_profile",
         "classification_splits"]
        + ["dim_{}".format(b) for b in DIMENSION_BUCKETS]
    )
    if not dim_summaries:
        exact_clean_fields = ["sig_hash", "category", "representative_name",
                               "file_count", "name_variant_count",
                               "classification_profile", "classification_splits"]
    if not sig_class_summary:
        exact_clean_fields = [f for f in exact_clean_fields
                               if f not in ("classification_profile", "classification_splits")]

    write_csv(os.path.join(args.out_dir, "lft_exact_matches.csv"),
              exact, exact_clean_fields)

    # Detail: exact row-level mapping of suspected duplicate sig_hashes
    # back to the actual family names and container files where they occur.
    # This is the safe join table for:
    #   sig_hash -> family_name -> export_run_id
    family_file_detail = build_family_file_detail(rows, exact)
    write_csv(
        os.path.join(args.out_dir, "lft_detail_family_file_rows.csv"),
        family_file_detail,
        [
            "sig_hash", "category", "family_name", "export_run_id",
            "record_count", "representative_name", "duplicate_file_count",
            "name_variant_count", "classification_profile",
            "classification_splits",
        ],
    )

    # Detail: name variants per sig_hash
    write_detail_file(
        os.path.join(args.out_dir, "lft_detail_name_variants.csv"),
        exact, "sig_hash", "sig_hash", "name_variants", "family_name",
        extra_fields=[("category", "category"), ("file_count", "file_count")],
    )
    # Detail: file_ids per sig_hash
    write_detail_file(
        os.path.join(args.out_dir, "lft_detail_file_ids.csv"),
        exact, "sig_hash", "sig_hash", "file_ids", "export_run_id",
        extra_fields=[("category", "category"), ("representative_name", "representative_name")],
    )
    # Detail: dim param names per sig_hash + bucket
    if dim_summaries:
        dim_pname_rows = []
        for row in exact:
            for b in DIMENSION_BUCKETS:
                pnames = row.get("dim_{}_param_names".format(b), "")
                if pnames:
                    dim_pname_rows.append({
                        "sig_hash": row["sig_hash"],
                        "bucket":   b,
                        "dim_{}_param_names".format(b): pnames,
                    })
        write_detail_file(
            os.path.join(args.out_dir, "lft_detail_dim_param_names.csv"),
            dim_pname_rows, "sig_hash", "sig_hash",
            # use first matching bucket col — handled by iterating all buckets
            "dim_width_param_names", "param_name",
            extra_fields=[("bucket", "bucket")],
        ) if False else None  # replaced by loop below
        # Better: flatten all bucket param_names into one detail file
        import os as _os
        dp_path = _os.path.join(args.out_dir, "lft_detail_dim_param_names.csv")
        _os.makedirs(_os.path.dirname(dp_path), exist_ok=True)
        dp_written = 0
        with open(dp_path, "w", encoding="utf-8", newline="") as _f:
            _w = csv.DictWriter(_f, fieldnames=["sig_hash","bucket","param_name"])
            _w.writeheader()
            for row in exact:
                for b in DIMENSION_BUCKETS:
                    raw = row.get("dim_{}_param_names".format(b), "") or ""
                    for pn in raw.split("|"):
                        pn = pn.strip()
                        if pn:
                            _w.writerow({"sig_hash": row["sig_hash"],
                                         "bucket": b, "param_name": pn})
                            dp_written += 1
        print("  Written: {} ({:,} rows)".format(dp_path, dp_written))

    print("\nTier 2: Name clusters (threshold={})".format(args.sim_threshold))
    clusters = build_name_cluster_table(rows, sig_class_summary,
                                         sim_threshold=args.sim_threshold)
    if args.top_n:
        clusters = clusters[:args.top_n]
    frag = sum(1 for c in clusters if c["fragmented"] == "yes")
    with_class = sum(1 for c in clusters if c.get("class_discrimination"))
    print("  Clusters: {:,}  ({:,} fragmented, {:,} with class discrimination)".format(
        len(clusters), frag, with_class))

    cluster_primary_fields = ["cluster_id", "category", "member_count",
                              "sig_hash_count", "total_file_appearances", "fragmented"]
    write_csv(os.path.join(args.out_dir, "lft_name_clusters.csv"),
              clusters, cluster_primary_fields)

    # Detail: sig_hashes per cluster
    write_detail_file(
        os.path.join(args.out_dir, "lft_detail_cluster_sig_hashes.csv"),
        clusters, "cluster_id", "cluster_id", "sig_hashes", "sig_hash",
        extra_fields=[("category","category"), ("sig_hash_count","sig_hash_count")],
    )
    # Detail: family names per cluster
    write_detail_file(
        os.path.join(args.out_dir, "lft_detail_cluster_family_names.csv"),
        clusters, "cluster_id", "cluster_id", "family_names", "family_name",
        extra_fields=[("category","category"), ("fragmented","fragmented")],
    )
    # Detail: class discrimination per cluster
    write_detail_file(
        os.path.join(args.out_dir, "lft_detail_cluster_discrimination.csv"),
        [c for c in clusters if c.get("class_discrimination","").strip()],
        "cluster_id", "cluster_id", "class_discrimination", "discrimination",
        extra_fields=[("category","category"), ("sig_hash_count","sig_hash_count"),
                      ("total_file_appearances","total_file_appearances")],
    )

    if subgroups:
        subgroup_primary_fields = [
            "subgroup_id", "sig_hash", "category", "representative_name",
            "total_files", "discriminating_param", "value_diversity",
            "modal_value", "modal_files", "other_stable_params",
        ]
        write_csv(os.path.join(args.out_dir, "lft_param_subgroups.csv"),
                  subgroups, subgroup_primary_fields)
        # Detail: all_values per subgroup (value + file count)
        write_detail_file_kv(
            os.path.join(args.out_dir, "lft_detail_subgroup_values.csv"),
            subgroups, "subgroup_id", "subgroup_id",
            "all_values", "param_value", count_label="file_count",
            extra_fields=[("category","category"),
                          ("representative_name","representative_name"),
                          ("discriminating_param","discriminating_param")],
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
