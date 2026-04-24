#!/usr/bin/env python3
"""
compare_view_templates.py

Compare view templates (and their category overrides) across two export JSONs
without relying on name matching.

Usage:
    python compare_view_templates.py file_a.json file_b.json [--out report.html]

For each template in file A, finds the best-matching template in file B using
def_signature Jaccard similarity (stratified by view_type_family), then diffs
the matched pairs into:
  - diverged:         same key, different values
  - a_only / b_only:  key present on one side only
  - matched:          identical (collapsed in HTML)

For each matched VT pair, also diffs VCO records (model + annotation) joined
by vco.baseline_category_path. Flags when vco.baseline_sig_hash differs between
files (baseline divergence — override comparison may not be apples-to-apples).
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VT_SPLIT_DOMAINS = [
    "view_templates_floor_structural_area_plans",
    "view_templates_ceiling_plans",
    "view_templates_elevations_sections_detail",
    "view_templates_renderings_drafting",
    "view_templates_schedules",
]

VCO_DOMAINS = [
    "view_category_overrides_model",
    "view_category_overrides_annotation",
]


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _extract_records(data, domain_names):
    """Pull records for a list of domain names from a monolithic export JSON.
    Tries top-level keys first, then nested under 'domains'."""
    records = []
    for domain_name in domain_names:
        domain_data = data.get(domain_name, {})
        for rec in domain_data.get("records", []):
            rec["_domain"] = domain_name
            records.append(rec)
    if not records:
        domains = data.get("domains", {})
        for domain_name in domain_names:
            domain_data = domains.get(domain_name, {})
            for rec in domain_data.get("records", []):
                rec["_domain"] = domain_name
                records.append(rec)
    return records


def _get_label_display(rec):
    try:
        return rec["label"]["display"] or ""
    except (KeyError, TypeError):
        return rec.get("record_id", "<unknown>")


def _get_label_component(rec, key):
    try:
        return rec["label"]["components"].get(key, "") or ""
    except (KeyError, TypeError):
        return ""


def _get_view_type_family(rec):
    """Pull vt.view_type_family from phase2.coordination_items or coordination_items."""
    for items in [
        (rec.get("phase2") or {}).get("coordination_items", []),
        rec.get("coordination_items", []),
    ]:
        for item in items:
            if item.get("k") == "vt.view_type_family":
                return str(item.get("v", ""))
    return rec.get("_domain", "unknown")


def _get_template_uid(rec):
    """Get the template UniqueId from a VT record's phase2.unknown_items."""
    for item in (rec.get("phase2") or {}).get("unknown_items", []):
        if item.get("k") == "vt.source_unique_id":
            return str(item.get("v", "") or "")
    return ""


def _get_vco_template_uid(rec):
    """Get the owning template UniqueId from a VCO record's unknown_items."""
    for item in (rec.get("phase2") or {}).get("unknown_items", []):
        if item.get("k") == "vco.template_unique_id":
            return str(item.get("v", "") or "")
    return _get_label_component(rec, "template_uid")


def _get_vco_category_path(rec):
    """Get vco.baseline_category_path from identity_basis.items."""
    for item in (rec.get("identity_basis") or {}).get("items", []):
        if item.get("k") == "vco.baseline_category_path":
            return str(item.get("v", "") or "")
    return _get_label_display(rec)


def _parse_vt_signature(rec):
    """Parse def_signature list of 'key=value' strings into a dict."""
    result = {}
    for entry in rec.get("def_signature", []):
        if "=" in entry:
            k, _, v = entry.partition("=")
            result[k.strip()] = v.strip()
        else:
            result[entry.strip()] = ""
    return result


def _parse_vco_items(rec):
    """Parse identity_basis.items into a key->value dict."""
    result = {}
    for item in (rec.get("identity_basis") or {}).get("items", []):
        k = item.get("k", "")
        if not k:
            continue
        q = item.get("q", "")
        v = item.get("v")
        if q == "ok":
            result[k] = str(v) if v is not None else ""
        elif q == "missing":
            result[k] = "<missing>"
        elif q == "unreadable":
            result[k] = "<unreadable>"
    return result


# ---------------------------------------------------------------------------
# VCO index
# ---------------------------------------------------------------------------

def _index_vco_by_template(vco_records):
    """Build {template_unique_id: [vco_rec, ...]} index."""
    idx = defaultdict(list)
    for rec in vco_records:
        uid = _get_vco_template_uid(rec)
        if uid:
            idx[uid].append(rec)
    return idx


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def _jaccard(sig_a, sig_b):
    """Token-level Jaccard on the set of 'key=value' pairs."""
    set_a = set("{}={}".format(k, v) for k, v in sig_a.items())
    set_b = set("{}={}".format(k, v) for k, v in sig_b.items())
    if not set_a and not set_b:
        return 1.0
    return len(set_a & set_b) / len(set_a | set_b)


def _best_match_index(sources, targets):
    """
    For each record in sources, find the best-scoring record in targets
    within the same view_type_family.
    Returns {id(source_rec): (target_rec, score, sig_target)}.
    """
    targets_by_family = defaultdict(list)
    for rec in targets:
        targets_by_family[_get_view_type_family(rec)].append(rec)

    result = {}
    for rec in sources:
        family = _get_view_type_family(rec)
        sig = _parse_vt_signature(rec)
        candidates = targets_by_family.get(family, [])

        best_score = -1.0
        best_rec = None
        best_sig = None

        for cand in candidates:
            cand_sig = _parse_vt_signature(cand)
            score = _jaccard(sig, cand_sig)
            if score > best_score:
                best_score = score
                best_rec = cand
                best_sig = cand_sig

        if best_rec is not None:
            result[id(rec)] = (best_rec, best_score, best_sig)

    return result


def _make_pair(rec_a, rec_b, vco_idx_a, vco_idx_b, confirmed, score, source):
    """Build a single match dict for a rec_a -> rec_b pairing."""
    sig_a = _parse_vt_signature(rec_a)
    sig_b = _parse_vt_signature(rec_b)
    uid_a = _get_template_uid(rec_a)
    uid_b = _get_template_uid(rec_b)
    pair_vco_a = vco_idx_a.get(uid_a, [])
    pair_vco_b = vco_idx_b.get(uid_b, [])
    return {
        "name_a": _get_label_display(rec_a),
        "name_b": _get_label_display(rec_b),
        "family": _get_view_type_family(rec_a),
        "score": score,
        "matched": True,
        "confirmed": confirmed,
        "source": source,  # "mapping" or "jaccard"
        "diff": _diff_dicts(sig_a, sig_b),
        "vco_diff": _diff_vco(pair_vco_a, pair_vco_b) if (pair_vco_a or pair_vco_b) else None,
    }


def match_templates(vt_a, vt_b, vco_a, vco_b, mapping=None):
    """
    Match VT records from A to B.

    If mapping is provided (list of {a: name, b: [names]}), those pairs are
    used directly and marked confirmed/source=mapping. One A can map to
    multiple B templates — each produces a separate diff row.

    For A templates not covered by the mapping, mutual Jaccard best-match is
    used (confirmed only if B also picks A back).
    """
    vco_idx_a = _index_vco_by_template(vco_a)
    vco_idx_b = _index_vco_by_template(vco_b)

    # Index records by display name for mapping lookups
    a_by_name = {_get_label_display(r): r for r in vt_a}
    b_by_name = {_get_label_display(r): r for r in vt_b}

    matches = []
    mapped_a_names = set()

    if mapping:
        for entry in mapping:
            name_a = entry["a"]
            names_b = entry["b"]
            rec_a = a_by_name.get(name_a)
            if rec_a is None:
                print("WARNING: mapping A name not found in file: {!r}".format(name_a))
                continue
            mapped_a_names.add(name_a)
            for name_b in names_b:
                rec_b = b_by_name.get(name_b)
                if rec_b is None:
                    print("WARNING: mapping B name not found in file: {!r}".format(name_b))
                    continue
                matches.append(_make_pair(rec_a, rec_b, vco_idx_a, vco_idx_b,
                                          confirmed=True, score=1.0, source="mapping"))

    # Jaccard fallback for unmapped A templates
    unmapped_a = [r for r in vt_a if _get_label_display(r) not in mapped_a_names]
    if unmapped_a:
        a_to_b = _best_match_index(unmapped_a, vt_b)
        b_to_a = _best_match_index(vt_b, unmapped_a)

        for rec_a in unmapped_a:
            family = _get_view_type_family(rec_a)
            if id(rec_a) not in a_to_b:
                matches.append({
                    "name_a": _get_label_display(rec_a),
                    "name_b": None,
                    "family": family,
                    "score": -1.0,
                    "matched": False,
                    "confirmed": False,
                    "source": "jaccard",
                    "diff": None,
                    "vco_diff": None,
                })
                continue

            best_rec_b, best_score, _ = a_to_b[id(rec_a)]
            b_best_back = b_to_a.get(id(best_rec_b))
            confirmed = b_best_back is not None and id(b_best_back[0]) == id(rec_a)
            matches.append(_make_pair(rec_a, best_rec_b, vco_idx_a, vco_idx_b,
                                      confirmed=confirmed, score=best_score, source="jaccard"))

    # Sort: mapping first, then confirmed jaccard, then unconfirmed, by score
    def _sort_key(m):
        if m.get("source") == "mapping":
            return (0, -m["score"])
        if m.get("confirmed"):
            return (1, -m["score"])
        return (2, -m["score"])

    matches.sort(key=_sort_key)
    return matches



# ---------------------------------------------------------------------------
# Diffing
# ---------------------------------------------------------------------------

def _diff_dicts(dict_a, dict_b):
    """Diff two flat key->value dicts into matched/diverged/a_only/b_only."""
    all_keys = set(dict_a.keys()) | set(dict_b.keys())
    matched, diverged, a_only, b_only = [], [], [], []

    for key in sorted(all_keys):
        in_a = key in dict_a
        in_b = key in dict_b
        if in_a and in_b:
            if dict_a[key] == dict_b[key]:
                matched.append({"key": key, "value": dict_a[key]})
            else:
                diverged.append({"key": key, "val_a": dict_a[key], "val_b": dict_b[key]})
        elif in_a:
            a_only.append({"key": key, "val_a": dict_a[key]})
        else:
            b_only.append({"key": key, "val_b": dict_b[key]})

    return {"matched": matched, "diverged": diverged, "a_only": a_only, "b_only": b_only}


def _diff_vco(vco_a, vco_b):
    """
    Diff VCO records for a matched VT pair.
    Joins by vco.baseline_category_path (stable cross-file).
    Returns per-category diffs plus summary counts.
    vco.baseline_sig_hash divergence is flagged but still diffed.
    """
    idx_a = {_get_vco_category_path(r): r for r in vco_a}
    idx_b = {_get_vco_category_path(r): r for r in vco_b}
    all_paths = sorted(set(idx_a.keys()) | set(idx_b.keys()))

    category_diffs = []
    for path in all_paths:
        rec_a = idx_a.get(path)
        rec_b = idx_b.get(path)

        if rec_a is None:
            category_diffs.append({"path": path, "status": "b_only", "diff": None, "baseline_diverged": False})
            continue
        if rec_b is None:
            category_diffs.append({"path": path, "status": "a_only", "diff": None, "baseline_diverged": False})
            continue

        items_a = _parse_vco_items(rec_a)
        items_b = _parse_vco_items(rec_b)

        baseline_diverged = items_a.get("vco.baseline_sig_hash", "") != items_b.get("vco.baseline_sig_hash", "")
        diff = _diff_dicts(items_a, items_b)
        identical = not diff["diverged"] and not diff["a_only"] and not diff["b_only"]

        category_diffs.append({
            "path": path,
            "status": "identical" if identical else "diverged",
            "diff": diff,
            "baseline_diverged": baseline_diverged,
        })

    return {
        "category_diffs": category_diffs,
        "n_identical": sum(1 for c in category_diffs if c["status"] == "identical"),
        "n_diverged": sum(1 for c in category_diffs if c["status"] == "diverged"),
        "n_a_only": sum(1 for c in category_diffs if c["status"] == "a_only"),
        "n_b_only": sum(1 for c in category_diffs if c["status"] == "b_only"),
        "n_baseline_warn": sum(1 for c in category_diffs if c["baseline_diverged"]),
    }


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

def _print_pair(m):
    diff = m["diff"]
    n_div = len(diff["diverged"])
    n_asym = len(diff["a_only"]) + len(diff["b_only"])
    vco = m.get("vco_diff")
    identity = "IDENTICAL" if n_div == 0 and n_asym == 0 else ""

    print()
    print("  A: {}".format(m["name_a"]))
    print("  B: {}".format(m["name_b"]))
    print("  Family: {}   Score: {:.3f}   VT diverged: {}   VT asymmetric: {}  {}".format(
        m["family"], m["score"], n_div, n_asym, identity))

    if diff["diverged"]:
        print()
        print("  VT DIVERGED:")
        for d in diff["diverged"]:
            print("    {:50s}  A={!r:20s}  B={!r}".format(d["key"], d["val_a"], d["val_b"]))

    if diff["a_only"]:
        print("  VT A ONLY:")
        for d in diff["a_only"]:
            print("    {:50s}  A={!r}".format(d["key"], d["val_a"]))

    if diff["b_only"]:
        print("  VT B ONLY:")
        for d in diff["b_only"]:
            print("    {:50s}  B={!r}".format(d["key"], d["val_b"]))

    if vco:
        print()
        print("  VCO: {} categories — {} diverged  {} A-only  {} B-only  {} identical{}".format(
            len(vco["category_diffs"]),
            vco["n_diverged"], vco["n_a_only"], vco["n_b_only"], vco["n_identical"],
            "  ⚠ {} baseline hash diff(s)".format(vco["n_baseline_warn"]) if vco["n_baseline_warn"] else "",
        ))
        for cat in vco["category_diffs"]:
            if cat["status"] == "identical":
                continue
            warn = " ⚠ baseline differs" if cat["baseline_diverged"] else ""
            print()
            print("    [{}] {}{}".format(cat["status"].upper(), cat["path"], warn))
            if cat["diff"]:
                for d in cat["diff"]["diverged"]:
                    print("      {:45s}  A={!r:15s}  B={!r}".format(d["key"], d["val_a"], d["val_b"]))
                for d in cat["diff"]["a_only"]:
                    print("      {:45s}  A={!r}".format(d["key"], d["val_a"]))
                for d in cat["diff"]["b_only"]:
                    print("      {:45s}  B={!r}".format(d["key"], d["val_b"]))


def _print_report(matches, name_a, name_b):
    print()
    print("=" * 72)
    print("VIEW TEMPLATE COMPARISON")
    print("  File A: {}".format(name_a))
    print("  File B: {}".format(name_b))
    print("=" * 72)

    confirmed = [m for m in matches if m["matched"] and m.get("confirmed")]
    unconfirmed = [m for m in matches if m["matched"] and not m.get("confirmed")]
    unmatched = [m for m in matches if not m["matched"]]

    mapped = [m for m in confirmed if m.get("source") == "mapping"]
    jaccard_confirmed = [m for m in confirmed if m.get("source") != "mapping"]

    if mapped:
        print()
        print("MAPPED PAIRS ({})".format(len(mapped)))
        print("-" * 72)
        for m in mapped:
            _print_pair(m)

    if jaccard_confirmed:
        print()
        print("CONFIRMED MUTUAL MATCHES ({})".format(len(jaccard_confirmed)))
        print("-" * 72)
        for m in jaccard_confirmed:
            _print_pair(m)

    if unconfirmed:
        print()
        print("UNCONFIRMED (A best-matches B but not mutual) ({})".format(len(unconfirmed)))
        print("-" * 72)
        for m in unconfirmed:
            print("  A: {:45s}  -> B: {}  [score {:.3f}]".format(
                m["name_a"], m["name_b"], m["score"]))

    if unmatched:
        print()
        print("NO MATCH IN B ({})".format(len(unmatched)))
        print("-" * 72)
        for m in unmatched:
            print("  {:50s}  family={}".format(m["name_a"], m["family"]))

    print()


# ---------------------------------------------------------------------------
# HTML output
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>View Template Comparison</title>
<style>
  body {{ font-family: system-ui, sans-serif; font-size: 13px; margin: 24px; background: #fafafa; color: #1a1a1a; }}
  h1 {{ font-size: 18px; margin-bottom: 4px; }}
  .meta {{ color: #555; margin-bottom: 24px; font-size: 12px; }}
  .pair {{ background: #fff; border: 1px solid #ddd; border-radius: 6px; margin-bottom: 12px; overflow: hidden; }}
  .pair-header {{ padding: 10px 14px; background: #f0f4f8; border-bottom: 1px solid #ddd; display: flex; justify-content: space-between; align-items: center; cursor: pointer; user-select: none; }}
  .pair-names {{ font-weight: 600; }}
  .pair-names .arrow {{ color: #888; margin: 0 8px; }}
  .badges {{ display: flex; gap: 6px; font-size: 11px; flex-wrap: wrap; }}
  .badge {{ padding: 2px 8px; border-radius: 10px; font-weight: 500; white-space: nowrap; }}
  .badge-family {{ background: #e8edf2; color: #456; }}
  .badge-score {{ background: #e8f4ea; color: #2a6b38; }}
  .badge-identical {{ background: #d4edda; color: #155724; }}
  .badge-diverged {{ background: #fff3cd; color: #856404; }}
  .badge-asym {{ background: #fde8e8; color: #7b2020; }}
  .badge-warn {{ background: #fde8e8; color: #7b2020; }}
  .badge-vco {{ background: #ede8f8; color: #4a2080; }}
  .badge-unconfirmed {{ background: #f0e0c0; color: #7a4a00; }}
  .unconfirmed-pair .pair-header {{ background: #fdf6ec; border-left: 3px solid #e0a040; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th {{ background: #f5f5f5; text-align: left; padding: 6px 10px; font-size: 11px; color: #555; border-bottom: 1px solid #e0e0e0; }}
  td {{ padding: 5px 10px; border-bottom: 1px solid #f0f0f0; font-family: monospace; font-size: 12px; vertical-align: top; }}
  .section-label {{ padding: 6px 14px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; background: #fafafa; border-bottom: 1px solid #eee; color: #555; }}
  .section-label-vco {{ padding: 6px 14px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; background: #f4f0fc; border-bottom: 1px solid #e0d8f8; color: #4a2080; }}
  .cat-header td {{ background: #faf8ff; font-weight: 600; color: #4a2080; font-size: 11px; padding: 4px 14px; }}
  .div-row td {{ background: #fffbea; }}
  .a-only-row td {{ background: #f0f7ff; }}
  .b-only-row td {{ background: #fff0f6; }}
  .val-a {{ color: #c0392b; }}
  .val-b {{ color: #27ae60; }}
  .note-row td {{ color: #888; font-size: 11px; padding: 3px 14px; }}
  .unmatched {{ background: #fff; border: 1px solid #f0c0c0; border-radius: 6px; padding: 12px 14px; margin-bottom: 8px; color: #7b2020; font-size: 12px; }}
  .section-h2 {{ font-size: 14px; font-weight: 600; margin: 24px 0 10px; }}
  .pair-body {{ display: none; }}
  .pair.open .pair-body {{ display: block; }}
</style>
<script>
  function togglePair(el) {{ el.closest('.pair').classList.toggle('open'); }}
</script>
</head>
<body>
<h1>View Template Comparison</h1>
<div class="meta">
  <strong>File A:</strong> {name_a} &nbsp;|&nbsp;
  <strong>File B:</strong> {name_b} &nbsp;|&nbsp;
  <strong>Mapped:</strong> {n_mapped} &nbsp;|&nbsp;
  <strong>Confirmed:</strong> {n_confirmed} &nbsp;|&nbsp;
  <strong>Unconfirmed:</strong> {n_unconfirmed} &nbsp;|&nbsp;
  <strong>No match:</strong> {n_unmatched}
</div>
{mapped_html}
<div class="section-h2">Confirmed Mutual Matches ({n_confirmed}) &mdash; click to expand</div>
{confirmed_html}
{unconfirmed_html}
{no_match_html}
</body>
</html>"""


def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _vt_diff_rows(diff):
    rows = []
    if diff["diverged"]:
        rows.append('<tr><td colspan="3" class="section-label">VT Diverged</td></tr>')
        rows.append("<tr><th>Key</th><th>Value in A</th><th>Value in B</th></tr>")
        for d in diff["diverged"]:
            rows.append('<tr class="div-row"><td>{}</td><td class="val-a">{}</td><td class="val-b">{}</td></tr>'.format(
                _esc(d["key"]), _esc(d["val_a"]), _esc(d["val_b"])))
    if diff["a_only"]:
        rows.append('<tr><td colspan="3" class="section-label">VT A only</td></tr>')
        rows.append("<tr><th>Key</th><th>Value in A</th><th></th></tr>")
        for d in diff["a_only"]:
            rows.append('<tr class="a-only-row"><td>{}</td><td class="val-a">{}</td><td></td></tr>'.format(
                _esc(d["key"]), _esc(d["val_a"])))
    if diff["b_only"]:
        rows.append('<tr><td colspan="3" class="section-label">VT B only</td></tr>')
        rows.append("<tr><th>Key</th><th></th><th>Value in B</th></tr>")
        for d in diff["b_only"]:
            rows.append('<tr class="b-only-row"><td>{}</td><td></td><td class="val-b">{}</td></tr>'.format(
                _esc(d["key"]), _esc(d["val_b"])))
    if diff["matched"]:
        rows.append('<tr><td colspan="3" class="section-label">VT Matched &mdash; {} keys identical</td></tr>'.format(
            len(diff["matched"])))
        rows.append("<tr><th>Key</th><th colspan='2'>Value (identical)</th></tr>")
        for d in diff["matched"]:
            rows.append("<tr style='opacity:0.5'><td>{}</td><td colspan='2'>{}</td></tr>".format(
                _esc(d["key"]), _esc(d["value"])))
    return rows


def _vco_rows(vco):
    rows = []
    rows.append('<tr><td colspan="3" class="section-label-vco">Category Overrides (VCO)</td></tr>')
    for cat in vco["category_diffs"]:
        status = cat["status"]
        path = cat["path"]
        warn = " &nbsp;<span style='color:#7b2020;font-size:10px'>⚠ baseline differs</span>" if cat["baseline_diverged"] else ""

        if status == "identical":
            continue

        label = {"a_only": "[A only]", "b_only": "[B only]", "diverged": ""}.get(status, status)
        color = {"a_only": "#c0392b", "b_only": "#27ae60", "diverged": "#4a2080"}.get(status, "inherit")
        rows.append('<tr class="cat-header"><td colspan="3">{} <span style="color:{}">{}</span>{}</td></tr>'.format(
            _esc(path), color, label, warn))

        if cat["diff"]:
            d = cat["diff"]
            if d["diverged"]:
                rows.append("<tr><th>Key</th><th>Value in A</th><th>Value in B</th></tr>")
                for item in d["diverged"]:
                    rows.append('<tr class="div-row"><td>{}</td><td class="val-a">{}</td><td class="val-b">{}</td></tr>'.format(
                        _esc(item["key"]), _esc(item["val_a"]), _esc(item["val_b"])))
            if d["a_only"]:
                rows.append("<tr><th>Key</th><th>Value in A</th><th></th></tr>")
                for item in d["a_only"]:
                    rows.append('<tr class="a-only-row"><td>{}</td><td class="val-a">{}</td><td></td></tr>'.format(
                        _esc(item["key"]), _esc(item["val_a"])))
            if d["b_only"]:
                rows.append("<tr><th>Key</th><th></th><th>Value in B</th></tr>")
                for item in d["b_only"]:
                    rows.append('<tr class="b-only-row"><td>{}</td><td></td><td class="val-b">{}</td></tr>'.format(
                        _esc(item["key"]), _esc(item["val_b"])))

    n_id = vco["n_identical"]
    if n_id:
        rows.append('<tr class="note-row"><td colspan="3">{} category override(s) identical</td></tr>'.format(n_id))
    return rows


def _pair_html(m, confirmed=True):
    diff = m["diff"]
    n_div = len(diff["diverged"])
    n_asym = len(diff["a_only"]) + len(diff["b_only"])
    vco = m.get("vco_diff")

    badges = [
        '<span class="badge badge-family">{}</span>'.format(_esc(m["family"])),
        '<span class="badge badge-score">score {:.3f}</span>'.format(m["score"]),
    ]
    if not confirmed:
        badges.append('<span class="badge badge-unconfirmed">unconfirmed</span>')

    vco_has_diff = vco and (vco["n_diverged"] or vco["n_a_only"] or vco["n_b_only"])
    if n_div == 0 and n_asym == 0 and not vco_has_diff:
        badges.append('<span class="badge badge-identical">Identical</span>')
    else:
        if n_div:
            badges.append('<span class="badge badge-diverged">{} VT diverged</span>'.format(n_div))
        if n_asym:
            badges.append('<span class="badge badge-asym">{} VT asymmetric</span>'.format(n_asym))
        if vco_has_diff:
            parts = []
            if vco["n_diverged"]:
                parts.append("{} cat diverged".format(vco["n_diverged"]))
            if vco["n_a_only"]:
                parts.append("{} A-only".format(vco["n_a_only"]))
            if vco["n_b_only"]:
                parts.append("{} B-only".format(vco["n_b_only"]))
            badges.append('<span class="badge badge-vco">VCO: {}</span>'.format(", ".join(parts)))
        if vco and vco["n_baseline_warn"]:
            badges.append('<span class="badge badge-warn">⚠ {} baseline</span>'.format(vco["n_baseline_warn"]))

    pair_class = "pair" if confirmed else "pair unconfirmed-pair"
    header = '<div class="pair-header" onclick="togglePair(this)"><div class="pair-names">{}<span class="arrow">&rarr;</span>{}</div><div class="badges">{}</div></div>'.format(
        _esc(m["name_a"]), _esc(m["name_b"]), "".join(badges))

    rows = _vt_diff_rows(diff)
    if vco:
        rows += _vco_rows(vco)

    if not rows:
        rows.append('<tr><td colspan="3" style="color:#2a6b38;padding:10px">All keys identical.</td></tr>')

    body = '<div class="pair-body"><table>{}</table></div>'.format("".join(rows))
    return '<div class="{}">{}{}</div>'.format(pair_class, header, body)


def _build_html(matches, name_a, name_b):
    mapped = [m for m in matches if m["matched"] and m.get("source") == "mapping"]
    confirmed = [m for m in matches if m["matched"] and m.get("confirmed") and m.get("source") != "mapping"]
    unconfirmed = [m for m in matches if m["matched"] and not m.get("confirmed")]
    unmatched = [m for m in matches if not m["matched"]]

    mapped_html = "\n".join(_pair_html(m, confirmed=True) for m in mapped) if mapped else ""
    if mapped_html:
        mapped_html = '<div class="section-h2">Mapped Pairs ({})</div>\n'.format(len(mapped)) + mapped_html

    confirmed_html = "\n".join(_pair_html(m, confirmed=True) for m in confirmed) if confirmed else "<p>No confirmed mutual matches.</p>"

    unconfirmed_html = ""
    if unconfirmed:
        items = "\n".join(_pair_html(m, confirmed=False) for m in unconfirmed)
        unconfirmed_html = '<div class="section-h2">Unconfirmed — A best-matches B but not mutual ({})</div>\n{}'.format(len(unconfirmed), items)

    no_match_html = ""
    if unmatched:
        items = "\n".join(
            '<div class="unmatched">&#9888; A: <strong>{}</strong> &mdash; no B candidates in same family</div>'.format(_esc(m["name_a"]))
            for m in unmatched)
        no_match_html = '<div class="section-h2">No match in B ({})</div>\n{}'.format(len(unmatched), items)

    return _HTML_TEMPLATE.format(
        name_a=_esc(name_a), name_b=_esc(name_b),
        n_mapped=len(mapped), n_confirmed=len(confirmed),
        n_unconfirmed=len(unconfirmed), n_unmatched=len(unmatched),
        mapped_html=mapped_html,
        confirmed_html=confirmed_html,
        unconfirmed_html=unconfirmed_html,
        no_match_html=no_match_html,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Compare view templates (and category overrides) across two export JSONs."
    )
    ap.add_argument("file_a", help="Path to export JSON for file A")
    ap.add_argument("file_b", help="Path to export JSON for file B")
    ap.add_argument("--out", help="Optional path for HTML report output (e.g. report.html)")
    ap.add_argument("--mapping", help="Optional path to mapping JSON file (list of {a, b[]} pairs)")
    args = ap.parse_args()

    path_a = Path(args.file_a)
    path_b = Path(args.file_b)

    for p in (path_a, path_b):
        if not p.exists():
            print("ERROR: File not found: {}".format(p), file=sys.stderr)
            sys.exit(1)

    with open(path_a, encoding="utf-8") as fh:
        data_a = json.load(fh)
    with open(path_b, encoding="utf-8") as fh:
        data_b = json.load(fh)

    vt_a = _extract_records(data_a, VT_SPLIT_DOMAINS)
    vt_b = _extract_records(data_b, VT_SPLIT_DOMAINS)
    vco_a = _extract_records(data_a, VCO_DOMAINS)
    vco_b = _extract_records(data_b, VCO_DOMAINS)

    print("Loaded {} VT + {} VCO from A,  {} VT + {} VCO from B".format(
        len(vt_a), len(vco_a), len(vt_b), len(vco_b)))

    if not vt_a:
        print("ERROR: No view template records found in file A.", file=sys.stderr)
        sys.exit(1)
    if not vt_b:
        print("ERROR: No view template records found in file B.", file=sys.stderr)
        sys.exit(1)

    name_a = path_a.stem
    name_b = path_b.stem

    mapping = None
    if args.mapping:
        mapping_path = Path(args.mapping)
        if not mapping_path.exists():
            print("ERROR: Mapping file not found: {}".format(mapping_path), file=sys.stderr)
            sys.exit(1)
        with open(mapping_path, encoding="utf-8") as fh:
            mapping = json.load(fh)
        print("Loaded {} mapping entries".format(len(mapping)))

    matches = match_templates(vt_a, vt_b, vco_a, vco_b, mapping=mapping)
    _print_report(matches, name_a, name_b)

    if args.out:
        html = _build_html(matches, name_a, name_b)
        out_path = Path(args.out)
        out_path.write_text(html, encoding="utf-8")
        print("HTML report written to: {}".format(out_path))


if __name__ == "__main__":
    main()