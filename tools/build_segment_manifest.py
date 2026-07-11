from __future__ import annotations
from itertools import combinations
import argparse,csv,hashlib,re,sys
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict,Iterable,List,Sequence

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from na_token import is_na_token

SEED_ROLES={"Template","Container"}
REQUIRED_COLUMNS={"export_run_id","unit_system","client_label","governance_role"}
MANIFEST_FIELDNAMES=["segment_id","parent_segment_id","segment_level","unit_system","governance_role","client_label","discipline_label","business_center_label","collection_label","extra_dimensions","ancestor_segment_ids","run_type","file_count","has_seed_file","population_hash","notes","segment_purpose","segment_label"]
REGISTRY_FIELDNAMES=["segment_id","parent_segment_id","run_type","population_hash","conformance_reference_mode","output_folder","status","last_run_utc","notes","segment_purpose","segment_label"]
MEMBERSHIP_FIELDNAMES=["segment_id","export_run_id","is_seed"]
DIMENSION_CONFIG = [
    {"field": "unit_system", "type": "root"},
    {"field": "governance_role", "type": "governance"},
    {"field": "client_label", "type": "cut"},
    {"field": "discipline_label", "type": "cut"},
    {"field": "business_center_label", "type": "cut"},
    # collection_label distinguishes multiple named standards libraries that
    # share the same business_center_label (or the same client_label) — e.g.
    # a business center that houses both its own general standards and a
    # separately-named legacy collection. business_center_label/client_label
    # alone cannot tell those apart; collection_label can.
    {"field": "collection_label", "type": "cut"},
    # Future cut dimensions added here:
    # {"field": "region", "type": "cut"},
    # {"field": "office_location", "type": "cut"},
]

def _read_csv(path: Path) -> tuple:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [{str(k): ("" if v is None else str(v)) for k, v in row.items()} for row in reader]
        fieldnames = list(reader.fieldnames or [])
    return fieldnames, rows

def _atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=str(path.parent), suffix=".tmp") as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.DictWriter(tmp, fieldnames=list(fieldnames));writer.writeheader()
        for row in rows: writer.writerow({name: row.get(name, "") for name in fieldnames})
    tmp_path.replace(path)

def _population_hash(export_run_ids: List[str]) -> str:
    token="|".join(sorted(export_run_ids));return hashlib.sha1(token.encode()).hexdigest()

_UNSAFE_FOLDER_CHARS = re.compile(r'[|/\\:*?"<>=\s]+')
def _sanitize_folder(segment_id:str)->str:return _UNSAFE_FOLDER_CHARS.sub("_",segment_id).lower().strip("_")

def _build_membership_rows(manifest_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Flatten each manifest row's internal export_run_ids/seed_export_run_ids
    (pipe-delimited, not written to segment_manifest.csv) into one
    (segment_id, export_run_id, is_seed) row per membership pair.

    Covers every segment in manifest_rows, not just run_registry-eligible
    (bundle/reference) ones, so segment_membership.csv joins cleanly against
    the full segment_manifest.csv the same way file_count does today.
    """
    rows: List[Dict[str, str]] = []
    for r in manifest_rows:
        sid = r["segment_id"]
        eids = [x for x in (r.get("export_run_ids") or "").split("|") if x]
        seeds = {x for x in (r.get("seed_export_run_ids") or "").split("|") if x}
        for eid in eids:
            rows.append({"segment_id": sid, "export_run_id": eid, "is_seed": "true" if eid in seeds else "false"})
    rows.sort(key=lambda row: (row["segment_id"], row["export_run_id"]))
    return rows

def _membership_by_segment(membership_rows: List[Dict[str, str]]) -> Dict[str, List[str]]:
    """Group membership CSV rows into segment_id -> sorted export_run_ids, for
    reconstructing a prior run's per-segment population (used by _build_registry's
    population_changed diffing in place of the old registry-embedded export_run_ids)."""
    grouped: Dict[str, List[str]] = defaultdict(list)
    for row in membership_rows:
        sid = (row.get("segment_id") or "").strip()
        eid = (row.get("export_run_id") or "").strip()
        if sid and eid:
            grouped[sid].append(eid)
    return {sid: sorted(eids) for sid, eids in grouped.items()}

def _append_note(row,k,v=""):
    note=f"{k}:{v}" if v else k
    if row.get("notes"): row["notes"] += f"|{note}"
    else: row["notes"]=note

_GOVERNANCE_ROLE_CANONICAL = {
    "project": "Project", "template": "Template", "container": "Container", "generic": "Generic",
}

def _normalize_rows(rows: List[Dict[str, str]]) -> "tuple[List[Dict[str, str]], List[tuple]]":
    """Case-normalize DIMENSION_CONFIG fields before they enter segment_id
    construction, so a manual-edit typo (e.g. "Imperial" vs "imperial" during
    the Run A -> Run B annotation pause) does not silently fragment one
    population into two shadow segments that never merge.

    Normalization rule per field:
      - unit_system: folds to lowercase — the established canonical form
        ("imperial" / "metric") used throughout the pipeline.
      - governance_role: values matching a KNOWN_ROLES member case-insensitively
        fold to that member's canonical casing (Project/Template/Container/
        Generic). A value that is NOT a case variant of a known role is not
        assumed to be a typo of one — it falls through to the same
        first-seen-casing fold as client_label/discipline_label below, so
        repeated variants of an unrecognized role still converge on one
        segment instead of fragmenting (main()'s unrecognized-role warning
        still fires for it either way).
      - client_label / discipline_label: no fixed enum. Case-insensitive fold
        to the casing of the first occurrence in row order. `rows` is a list,
        not a set/dict, so "first occurrence" is deterministic regardless of
        any hash/iteration order.
      - Any DIMENSION_CONFIG field whose value is a recognized "not applicable"
        spelling (see na_token.is_na_token) folds to blank ("") ahead of the
        field-specific rule above. Blank means "not yet filled in" (a manual-
        entry todo); an explicit N/A spelling means "reviewed, does not apply" —
        both must behave identically for segment-key purposes (neither should
        leak into segment_id as a literal token), but only the N/A case is
        folded here since blank cells are already blank.

    Returns (normalized_rows, changes) where changes is a list of
    (field, raw_value, normalized_value) tuples, one per row-field whose value
    was altered by normalization (duplicates included — callers aggregate).
    """
    fields = [d["field"] for d in DIMENSION_CONFIG]
    first_seen: Dict[str, Dict[str, str]] = {f: {} for f in fields}
    changes: List[tuple] = []
    normalized_rows: List[Dict[str, str]] = []

    for row in rows:
        new_row = dict(row)
        for field in fields:
            raw = (row.get(field) or "").strip()
            if not raw:
                continue
            if is_na_token(raw):
                changes.append((field, raw, ""))
                new_row[field] = ""
                continue
            if field == "unit_system":
                canon = raw.lower()
            elif field == "governance_role":
                canon = _GOVERNANCE_ROLE_CANONICAL.get(raw.lower())
                if canon is None:
                    canon = first_seen[field].setdefault(raw.lower(), raw)
            else:
                canon = first_seen[field].setdefault(raw.lower(), raw)
            if canon != raw:
                changes.append((field, raw, canon))
            new_row[field] = canon
        normalized_rows.append(new_row)

    return normalized_rows, changes

def _build_segments(rows:List[Dict[str,str]],min_files:int,enable_cross_org_template_bundles:bool=False,enable_parent_bundle_runs:bool=False)->List[Dict[str,str]]:
    root_dims = [d for d in DIMENSION_CONFIG if d["type"] == "root"]
    gov_dims = [d for d in DIMENSION_CONFIG if d["type"] == "governance"]
    cut_dims = [d for d in DIMENSION_CONFIG if d["type"] == "cut"]
    if len(root_dims) != 1 or len(gov_dims) != 1:
        raise ValueError("DIMENSION_CONFIG must have exactly one root and one governance dimension")
    root_field = root_dims[0]["field"]
    governance_field = gov_dims[0]["field"]
    client_field = "client_label"
    cfg_fields = [d["field"] for d in DIMENSION_CONFIG]

    populations = defaultdict(list)
    seed_pops = defaultdict(list)
    project_presence_by_l2: Dict[str, bool] = defaultdict(bool)

    # collection_label is free text (unlike client_label/discipline_label/
    # business_center_label, which draw from comparatively disjoint,
    # controlled-ish vocabularies), so its value is markedly more likely to
    # collide with another dimension's value at the same position in
    # _subset_to_id()'s output — e.g. a business-center-scoped
    # "imperial|Template||Shared" and a collection-scoped
    # "imperial|Template||Shared" (business_center_label="Shared" vs.
    # collection_label="Shared") render identically today, since the join
    # encodes which VALUES were selected but not which FIELDS supplied them.
    # That is a real, pre-existing structural gap in _subset_to_id() (any two
    # dimensions could in principle collide this way), but collection_label
    # is the first field free-text enough to make it likely in practice.
    # Namespace collection_label's contribution rather than rewriting the id
    # scheme for every dimension — segment_id is parsed positionally
    # elsewhere (tools/generate_governance_narrative.py) and hardcoded
    # verbatim across dozens of existing tests and any already-run
    # run_registry.csv folder mappings, so changing the format for the four
    # pre-existing dimensions would be a much larger, hash-breaking-style
    # change for a collision risk that hasn't manifested there in practice.
    _SUBSET_ID_NAMESPACED_FIELDS = {"collection_label": "collection"}

    def _subset_to_id(key: frozenset) -> str:
        kv = dict(key)
        parts = []
        for f in cfg_fields:
            if f not in kv:
                continue
            value = kv[f]
            ns = _SUBSET_ID_NAMESPACED_FIELDS.get(f)
            parts.append(f"{ns}:{value}" if ns and value else value)
        return "|".join(parts)

    normalized_rows, _changes = _normalize_rows(rows)

    for row in normalized_rows:
        export_run_id = (row.get("export_run_id") or "").strip()
        if not export_run_id:
            continue
        dim_values = {}
        for field in cfg_fields:
            value = (row.get(field) or "").strip()
            if value:
                dim_values[field] = value
            elif field == client_field:
                dim_values[field] = ""
        root_value = dim_values.get(root_field, "")
        if not root_value:
            continue
        # Blank client_label participates in subset generation like any other
        # value (as the pair (client_field, "")) — it is not special-cased to
        # strip governance_role/discipline_label/business_center_label out of
        # non_root_pairs. Previously, a blank-client row only ever populated
        # the root and root+client("") keys, so it was invisible to every
        # role-scoped and business-center-scoped segment and only ever landed
        # in one governance-agnostic pool at (root, client="").
        non_root_pairs = [(f, dim_values[f]) for f in cfg_fields if f != root_field and f in dim_values]
        for size in range(len(non_root_pairs) + 1):
            for subset in combinations(non_root_pairs, size):
                key = frozenset([(root_field, root_value), *subset])
                populations[key].append(export_run_id)
                governance_value = dim_values.get(governance_field, "")
                if governance_value in SEED_ROLES:
                    seed_pops[key].append(export_run_id)
                if size == 1 and subset and subset[0][0] == client_field and governance_value == "Project":
                    project_presence_by_l2[_subset_to_id(key)] = True

    keys = sorted(populations.keys(), key=lambda k: (len(k), _subset_to_id(k)))
    key_set = set(keys)
    rows_out = []
    key_to_row = {}
    key_to_children = defaultdict(list)
    row_to_key = {}
    for key in keys:
        dim_map = dict(key)
        non_root_fields_present = [f for f in cfg_fields if f != root_field and f in dim_map]
        segment_id = _subset_to_id(key)
        if not non_root_fields_present:
            parent_id = ""
        else:
            parent_key = frozenset((f, v) for f, v in key if f != non_root_fields_present[-1])
            parent_id = _subset_to_id(parent_key)
        ancestor_ids = []
        for field in non_root_fields_present:
            anc_key = frozenset((f, v) for f, v in key if f != field)
            if anc_key in key_set:
                ancestor_ids.append(_subset_to_id(anc_key))
        ancestor_ids = sorted(ancestor_ids)
        eids = sorted(set(populations[key]))
        seeds = sorted(set(seed_pops.get(key, [])))
        extra = []
        for d in cut_dims:
            if d["field"] == client_field:
                continue
            if d["field"] in dim_map:
                extra.append(f"{d['field']}={dim_map[d['field']]}")
        row = {
            "segment_id": segment_id,
            "parent_segment_id": parent_id,
            "segment_level": str(len(key)),
            "unit_system": dim_map.get("unit_system", ""),
            "governance_role": dim_map.get(governance_field, ""),
            "client_label": dim_map.get(client_field, ""),
            "discipline_label": dim_map.get("discipline_label", ""),
            "business_center_label": dim_map.get("business_center_label", ""),
            "collection_label": dim_map.get("collection_label", ""),
            "extra_dimensions": "|".join(extra),
            "ancestor_segment_ids": "|".join(ancestor_ids),
            "run_type": "",
            "file_count": str(len(eids)),
            "export_run_ids": "|".join(eids),
            "has_seed_file": "true" if seeds else "false",
            "seed_export_run_ids": "|".join(seeds),
            "population_hash": _population_hash(eids),
            "notes": "",
            "segment_purpose": "",
            "segment_label": "",
        }
        rows_out.append(row)
        key_to_row[key] = row
        row_to_key[id(row)] = key

    for parent_key in keys:
        parent_size = len(parent_key)
        for child_key in keys:
            if len(child_key) == parent_size + 1 and parent_key.issubset(child_key):
                key_to_children[parent_key].append(child_key)

    for r in rows_out:
        fc=int(r["file_count"]); role=r["governance_role"]
        notes = []
        if fc < min_files:
            notes.append("below_min_files")
        if r["segment_level"] == "2" and r["has_seed_file"] == "true":
            if not role and not project_presence_by_l2.get(r["segment_id"], False):
                notes.append("seed_only")
            elif role and role != "Project":
                notes.append("seed_only")
        if notes:
            r["notes"] = "|".join(notes)
        seg = r["segment_id"]
        key = row_to_key[id(r)]
        has = bool(key_to_children.get(key))
        if has:
            is_cross_org_template = (
                enable_cross_org_template_bundles
                and r["segment_level"] == "2"
                and r["governance_role"] == "Template"
                and not r["client_label"]
            )
            is_role_fixed_parent = (
                enable_parent_bundle_runs
                and r["segment_level"] == "2"
                and r["governance_role"] != ""
                and not r["client_label"]
                and fc >= min_files
            )
            is_scoped_leaf = (
                int(r["segment_level"]) >= 3
                and r["governance_role"] != ""
            )
            if not is_cross_org_template and not is_role_fixed_parent and not is_scoped_leaf:
                r["run_type"] = "registration"; continue
        if fc>=min_files: r["run_type"]="bundle"
        elif role in {"Template","Container","Generic"}: r["run_type"]="reference"
        elif role=="Project": r["run_type"]="skip"
        elif role == "":
            r["run_type"] = "registration"
        else: r["run_type"]="registration"
    # purpose/label
    def child_span(r):
        row_key = row_to_key[id(r)]
        # A distinct scope is a real client_label, business_center_label, or
        # collection_label on a level-3 child — client/business_center are
        # mutually exclusive per row today, but collection_label can ride
        # alongside either one, so it's checked independently rather than
        # folded into the same `or` chain (two children that share a
        # business_center but differ only by collection must still count as
        # distinct). Without counting these here, a Template pool whose only
        # children are BC- or collection-scoped (no client-having children at
        # all) would look like it has zero distinct children and get wrongly
        # collapsed as "redundant_single_child" even when multiple distinct
        # scopes are present.
        cs={
            (key_to_row[k]["client_label"] or key_to_row[k]["business_center_label"], key_to_row[k]["collection_label"])
            for k in key_to_children.get(row_key,[])
            if key_to_row[k]["segment_level"]=="3"
            and (key_to_row[k]["client_label"] or key_to_row[k]["business_center_label"] or key_to_row[k]["collection_label"])
        }
        return "multi_client" if len(cs)>1 else "single_client"
    for r in rows_out:
        pur="insufficient_population" if r["run_type"]=="skip" else ""
        lev,role,rt=int(r["segment_level"]),r["governance_role"],r["run_type"]
        disc=r["discipline_label"]
        bc=r["business_center_label"]
        coll=r["collection_label"]
        # client_label and business_center_label are mutually exclusive "why
        # was this captured" scopes; each is a distinct differentiator, so a
        # level-3/4 segment is only "disc cut" or "bc cut" when the other
        # scope isn't also present in that segment's key. collection_label is
        # a further, independent differentiator (a business center or client
        # can host more than one named collection), so every non-collection
        # predicate below also requires collection to be absent — otherwise a
        # collection-scoped segment would be mislabeled by the coarser
        # (collection-agnostic) branch that happens to come first.
        is_disc_cut=bool(disc and not r["client_label"] and not bc and not coll)
        is_client_disc_cut=bool(disc and r["client_label"] and not coll)
        is_bc_cut=bool(bc and not r["client_label"] and not disc and not coll)
        is_bc_disc_cut=bool(bc and disc and not r["client_label"] and not coll)
        # True when a role-scoped key has no other dimension in play — covers
        # both the plain level-2 {role} key and its level-3 "twin"
        # {role, client=""} produced by client_label's always-present-even-
        # blank dim_values treatment (see non_root_pairs comment above).
        is_role_alone=bool(not disc and not bc and not coll and not r["client_label"])
        # collection_label alone, or combined with discipline/business_center,
        # never with client_label. Level ranges mirror the
        # business_center_label branches one dimension over, for the same
        # blank-client-twin reason.
        is_coll_cut=bool(coll and not r["client_label"] and not bc and not disc)
        is_coll_disc_cut=bool(coll and disc and not r["client_label"] and not bc)
        is_bc_coll_cut=bool(bc and coll and not r["client_label"] and not disc)
        is_bc_coll_disc_cut=bool(bc and coll and disc and not r["client_label"])
        # client_label + collection_label: a client can host more than one
        # named collection (e.g. "Sutter Standards" plus some other
        # separately-named collection under the same client), so
        # collection_label still differentiates even though client_label
        # already names the segment. Unlike the business_center_label
        # combos, client_label has no forced-blank-twin level here — it is
        # only ever included in a subset when its value is real, so these
        # land on a single exact level rather than a (lev, lev+1) range.
        is_client_coll_cut=bool(r["client_label"] and coll and not disc)
        is_client_coll_disc_cut=bool(r["client_label"] and coll and disc)
        if lev==1: pur="population_denominator"
        elif lev == 2 and r["client_label"] and not role:
            pur = "client_population"
        elif lev == 2 and bc and not role:
            pur = "business_center_population"
        elif lev == 2 and coll and not role:
            pur = "collection_population"
        elif lev in (2,3) and is_role_alone and role=="Template":
            if rt=="bundle": pur="cross_template_agreement"
            elif rt in {"registration","reference"}: pur="cross_org_template_pool" if child_span(r)=="multi_client" else "redundant_single_child"
        elif lev in (2,3) and is_role_alone and role=="Project": pur="cross_project_practice" if rt=="bundle" else "practiced_standards_corpus"
        elif lev in (2,3) and is_role_alone and role=="Container": pur="coordination_corpus"
        elif lev in (2,3) and is_role_alone and role=="Generic" and rt=="reference": pur="generic_reference_corpus"
        # lev in (3,4)/(4,5) rather than a single level: client_label is the
        # only field always present in dim_values (even blank), so every
        # combo not involving a real client has a "twin" one level deeper
        # with a redundant (client, "") pair tacked on — e.g. both
        # {role,discipline} (lev 3) and {role,client="",discipline} (lev 4)
        # are reachable for a blank-client row. The two are population-
        # identical and pass5 below collapses whichever is shallower into a
        # "redundant_single_child" pointer at the other — but which one
        # survives as the real "bundle"/"reference" segment depends on
        # sibling shape, so both levels need the same purpose branch.
        elif lev in (3,4) and is_disc_cut and role=="Template" and rt in {"bundle","reference"}: pur="discipline_templates"
        elif lev in (3,4) and is_disc_cut and role=="Project": pur="discipline_practice" if rt=="bundle" else "insufficient_population"
        elif lev in (3,4) and is_disc_cut and role=="Container": pur="discipline_coordination"
        elif lev in (3,4) and is_disc_cut and role=="Generic" and rt=="reference": pur="discipline_reference"
        elif lev in (3,4) and is_bc_cut and role=="Template" and rt in {"bundle","reference"}: pur="business_center_standard_anchor"
        elif lev in (3,4) and is_bc_cut and role=="Project": pur="business_center_practice" if rt=="bundle" else "insufficient_population"
        elif lev in (3,4) and is_bc_cut and role=="Container": pur="business_center_coordination"
        elif lev in (3,4) and is_bc_cut and role=="Generic" and rt=="reference": pur="business_center_reference"
        elif lev==3 and role=="Template" and r["client_label"] and rt in {"bundle","reference"}: pur="client_standard_anchor"
        elif lev==3 and role=="Project" and r["client_label"]: pur="client_practice" if rt=="bundle" else "insufficient_population"
        elif lev==3 and role=="Container" and r["client_label"]: pur="client_coordination"
        elif lev==3 and role=="Generic" and r["client_label"] and rt=="reference": pur="client_reference"
        elif lev==4 and is_client_disc_cut and role=="Template" and rt in {"bundle","reference"}: pur="client_discipline_standard_anchor"
        elif lev==4 and is_client_disc_cut and role=="Project": pur="client_discipline_practice" if rt=="bundle" else "insufficient_population"
        elif lev==4 and is_client_disc_cut and role=="Container": pur="client_discipline_coordination"
        elif lev==4 and is_client_disc_cut and role=="Generic" and rt=="reference": pur="client_discipline_reference"
        elif lev==4 and is_client_coll_cut and role=="Template" and rt in {"bundle","reference"}: pur="client_collection_standard_anchor"
        elif lev==4 and is_client_coll_cut and role=="Project": pur="client_collection_practice" if rt=="bundle" else "insufficient_population"
        elif lev==4 and is_client_coll_cut and role=="Container": pur="client_collection_coordination"
        elif lev==4 and is_client_coll_cut and role=="Generic" and rt=="reference": pur="client_collection_reference"
        elif lev==5 and is_client_coll_disc_cut and role=="Template" and rt in {"bundle","reference"}: pur="client_collection_discipline_standard_anchor"
        elif lev==5 and is_client_coll_disc_cut and role=="Project": pur="client_collection_discipline_practice" if rt=="bundle" else "insufficient_population"
        elif lev==5 and is_client_coll_disc_cut and role=="Container": pur="client_collection_discipline_coordination"
        elif lev==5 and is_client_coll_disc_cut and role=="Generic" and rt=="reference": pur="client_collection_discipline_reference"
        elif lev in (4,5) and is_bc_disc_cut and role=="Template" and rt in {"bundle","reference"}: pur="business_center_discipline_standard_anchor"
        elif lev in (4,5) and is_bc_disc_cut and role=="Project": pur="business_center_discipline_practice" if rt=="bundle" else "insufficient_population"
        elif lev in (4,5) and is_bc_disc_cut and role=="Container": pur="business_center_discipline_coordination"
        elif lev in (4,5) and is_bc_disc_cut and role=="Generic" and rt=="reference": pur="business_center_discipline_reference"
        # collection_label branches: same role/level shape as the
        # business_center_label branches above, shifted one dimension over.
        elif lev in (3,4) and is_coll_cut and role=="Template" and rt in {"bundle","reference"}: pur="collection_standard_anchor"
        elif lev in (3,4) and is_coll_cut and role=="Project": pur="collection_practice" if rt=="bundle" else "insufficient_population"
        elif lev in (3,4) and is_coll_cut and role=="Container": pur="collection_coordination"
        elif lev in (3,4) and is_coll_cut and role=="Generic" and rt=="reference": pur="collection_reference"
        elif lev in (4,5) and is_coll_disc_cut and role=="Template" and rt in {"bundle","reference"}: pur="collection_discipline_standard_anchor"
        elif lev in (4,5) and is_coll_disc_cut and role=="Project": pur="collection_discipline_practice" if rt=="bundle" else "insufficient_population"
        elif lev in (4,5) and is_coll_disc_cut and role=="Container": pur="collection_discipline_coordination"
        elif lev in (4,5) and is_coll_disc_cut and role=="Generic" and rt=="reference": pur="collection_discipline_reference"
        elif lev in (4,5) and is_bc_coll_cut and role=="Template" and rt in {"bundle","reference"}: pur="business_center_collection_standard_anchor"
        elif lev in (4,5) and is_bc_coll_cut and role=="Project": pur="business_center_collection_practice" if rt=="bundle" else "insufficient_population"
        elif lev in (4,5) and is_bc_coll_cut and role=="Container": pur="business_center_collection_coordination"
        elif lev in (4,5) and is_bc_coll_cut and role=="Generic" and rt=="reference": pur="business_center_collection_reference"
        elif lev in (5,6) and is_bc_coll_disc_cut and role=="Template" and rt in {"bundle","reference"}: pur="business_center_collection_discipline_standard_anchor"
        elif lev in (5,6) and is_bc_coll_disc_cut and role=="Project": pur="business_center_collection_discipline_practice" if rt=="bundle" else "insufficient_population"
        elif lev in (5,6) and is_bc_coll_disc_cut and role=="Container": pur="business_center_collection_discipline_coordination"
        elif lev in (5,6) and is_bc_coll_disc_cut and role=="Generic" and rt=="reference": pur="business_center_collection_discipline_reference"
        r["segment_purpose"]=pur
        unit=r["unit_system"].title(); client=r["client_label"]; sid=r["segment_id"]
        templates={"population_denominator":f"All {unit} files","cross_org_template_pool":f"{unit} templates — all organisations (registration only)","cross_template_agreement":f"{unit} templates — cross-template agreement","practiced_standards_corpus":f"{unit} projects — full corpus","cross_project_practice":f"{unit} projects — cross-project practice","coordination_corpus":f"{unit} coordination files","generic_reference_corpus":f"{unit} generic reference","client_population":f"{client} — all roles combined","client_standard_anchor":f"{client} templates — standards as authored","client_practice":f"{client} projects — standards as practiced","client_coordination":f"{client} coordination files","client_reference":f"{client} generic reference","insufficient_population":f"{sid} — below minimum file threshold","discipline_practice":f"{disc} projects — standards as practiced","discipline_templates":f"{disc} templates — standards as authored","discipline_coordination":f"{disc} coordination files","discipline_reference":f"{disc} generic reference","client_discipline_standard_anchor":f"{client} {disc} templates — standards as authored","client_discipline_practice":f"{client} {disc} projects — standards as practiced","client_discipline_coordination":f"{client} {disc} coordination files","client_discipline_reference":f"{client} {disc} generic reference","business_center_population":f"{bc} — all roles combined","business_center_standard_anchor":f"{bc} templates — standards as authored","business_center_practice":f"{bc} projects — standards as practiced","business_center_coordination":f"{bc} coordination files","business_center_reference":f"{bc} generic reference","business_center_discipline_standard_anchor":f"{bc} {disc} templates — standards as authored","business_center_discipline_practice":f"{bc} {disc} projects — standards as practiced","business_center_discipline_coordination":f"{bc} {disc} coordination files","business_center_discipline_reference":f"{bc} {disc} generic reference","collection_population":f"{coll} — all roles combined","collection_standard_anchor":f"{coll} templates — standards as authored","collection_practice":f"{coll} projects — standards as practiced","collection_coordination":f"{coll} coordination files","collection_reference":f"{coll} generic reference","collection_discipline_standard_anchor":f"{coll} {disc} templates — standards as authored","collection_discipline_practice":f"{coll} {disc} projects — standards as practiced","collection_discipline_coordination":f"{coll} {disc} coordination files","collection_discipline_reference":f"{coll} {disc} generic reference","business_center_collection_standard_anchor":f"{bc} — {coll} templates — standards as authored","business_center_collection_practice":f"{bc} — {coll} projects — standards as practiced","business_center_collection_coordination":f"{bc} — {coll} coordination files","business_center_collection_reference":f"{bc} — {coll} generic reference","business_center_collection_discipline_standard_anchor":f"{bc} — {coll} {disc} templates — standards as authored","business_center_collection_discipline_practice":f"{bc} — {coll} {disc} projects — standards as practiced","business_center_collection_discipline_coordination":f"{bc} — {coll} {disc} coordination files","business_center_collection_discipline_reference":f"{bc} — {coll} {disc} generic reference","client_collection_standard_anchor":f"{client} — {coll} templates — standards as authored","client_collection_practice":f"{client} — {coll} projects — standards as practiced","client_collection_coordination":f"{client} — {coll} coordination files","client_collection_reference":f"{client} — {coll} generic reference","client_collection_discipline_standard_anchor":f"{client} — {coll} {disc} templates — standards as authored","client_collection_discipline_practice":f"{client} — {coll} {disc} projects — standards as practiced","client_collection_discipline_coordination":f"{client} — {coll} {disc} coordination files","client_collection_discipline_reference":f"{client} — {coll} {disc} generic reference"}
        if r["segment_purpose"]:
            r["segment_label"]=templates.get(r["segment_purpose"],sid)
        else:
            r["segment_label"]=sid
    # pass5 redundant hash
    for r in rows_out:
        if r["run_type"] not in {"bundle", "registration", "reference"}: continue
        row_key = row_to_key[id(r)]
        direct_children = [key_to_row[k] for k in key_to_children.get(row_key, [])]
        if len(direct_children) > 1:
            continue
        matches = [c for c in direct_children if c["population_hash"] == r["population_hash"]]
        if len(direct_children) == 1 and len(matches) == 1:
            ch=matches[0]["segment_id"]; _append_note(r,"redundant_single_child",ch)
            r["run_type"]="registration"; r["segment_purpose"]="redundant_single_child"; r["segment_label"]=f"{r['segment_id']} — same population as {ch}"
    rows_out.sort(key=lambda r:(int(r["segment_level"]),r["segment_id"]))
    return rows_out

# preserve remaining functions from original manually omitted

def _build_registry(
    manifest_rows: List[Dict[str, str]],
    existing_registry: List[Dict[str, str]] | None = None,
    existing_membership: Dict[str, List[str]] | None = None,
) -> List[Dict[str, str]]:
    """Build run_registry.csv rows from freshly computed manifest rows.

    When existing_registry is supplied, prior segment_id -> output_folder
    mappings are reused verbatim (folder-name stability across runs), and
    status/last_run_utc are carried over unless population_hash or run_type
    changed for that segment_id (population-hash-based incremental skip).
    A run_type change (e.g. a --min-files threshold change turning a
    "reference" segment into a "bundle" with the same file population) must
    also reset status, since population_hash alone would miss it and the
    orchestrator would keep skipping a segment that now needs a different
    analysis to be produced.

    When population_hash changes, the reason is diffed against the prior run's
    export_run_ids and recorded as new_files and/or removed_files counts
    alongside the population_changed marker, so the run plan states why a
    segment went stale rather than just that it did. The prior population is
    read from existing_membership (segment_id -> export_run_ids, built from a
    previously-written segment_membership.csv) rather than from
    existing_registry — run_registry.csv no longer carries a per-segment
    member list. A segment with no entry in existing_membership (e.g. the
    first registry rebuild after this migration, before any
    segment_membership.csv existed) is treated as having an empty prior
    population, so its notes will show every current file as new_files with
    no removed_files on that one transitional run.
    conformance_reference_mode is carried over verbatim (defaulting to "latest"
    for new segments and for older registries written before this field
    existed); no other mode is implemented yet.
    """
    existing_membership = existing_membership or {}
    existing_by_id: Dict[str, Dict[str, str]] = {}
    if existing_registry:
        for row in existing_registry:
            sid = (row.get("segment_id") or "").strip()
            if sid:
                existing_by_id[sid] = row

    eligible_rows = [r for r in manifest_rows if r["run_type"] in {"bundle", "reference"}]
    new_ids = {r["segment_id"] for r in eligible_rows}

    # Reserve every folder the old registry ever assigned — carried-over AND
    # dropped segments alike — before assigning any folder to a genuinely new
    # segment. A dropped segment's directory under segments/ still holds its
    # old records/markers/analysis output (the caller is only warned to review
    # it for manual cleanup, not to delete it), so a new segment must never be
    # handed that same folder name — it would start writing into a directory
    # still full of a different segment's stale data.
    assigned_folders: set = set()
    for old_row in existing_by_id.values():
        of = old_row.get("output_folder", "")
        if of:
            assigned_folders.add(of)

    registry = []
    for row in eligible_rows:
        sid = row["segment_id"]
        old = existing_by_id.get(sid)
        if old is not None:
            folder = old.get("output_folder", "") or _sanitize_folder(sid)
            reg_row = {
                "segment_id": sid,
                "parent_segment_id": row["parent_segment_id"],
                "run_type": row["run_type"],
                "population_hash": row["population_hash"],
                # "latest" is the only mode implemented: conformance comparisons
                # (tools/compare_cross_segment.py) always resolve reference segments
                # dynamically against current output. A pinned/snapshot mode is
                # deferred until Phase-2 baseline authority is established (see
                # CLAUDE.md "current operating mode").
                "conformance_reference_mode": old.get("conformance_reference_mode", "") or "latest",
                "output_folder": folder,
                "status": old.get("status", "pending"),
                "last_run_utc": old.get("last_run_utc", ""),
                "notes": old.get("notes", ""),
                "segment_purpose": row.get("segment_purpose", ""),
                "segment_label": row.get("segment_label", ""),
            }
            population_changed = old.get("population_hash", "") != row["population_hash"]
            run_type_changed = old.get("run_type", "") != row["run_type"]
            if population_changed or run_type_changed:
                reg_row["status"] = "pending"
                reg_row["last_run_utc"] = ""
                reg_row["notes"] = row.get("notes", "")
                if population_changed:
                    _append_note(reg_row, "population_changed")
                    old_export_ids = set(existing_membership.get(sid, []))
                    new_export_ids = {x for x in row.get("export_run_ids", "").split("|") if x}
                    added = new_export_ids - old_export_ids
                    removed = old_export_ids - new_export_ids
                    # A population_hash change is purely a function of the
                    # export_run_id set, so at least one of added/removed is always
                    # non-empty here — this also covers a metadata edit that moves a
                    # file between segments (e.g. a corrected client_label): it
                    # surfaces as removed_files on the file's old segment and
                    # new_files on its new segment, with no separate detection path
                    # needed for "metadata change" as its own reason.
                    if added:
                        _append_note(reg_row, "new_files", str(len(added)))
                    if removed:
                        _append_note(reg_row, "removed_files", str(len(removed)))
                if run_type_changed:
                    _append_note(reg_row, "run_type_changed", f"{old.get('run_type', '')}->{row['run_type']}")
        else:
            base = _sanitize_folder(sid)
            folder = base
            n = 2
            while folder in assigned_folders:
                folder = f"{base}_{n}"
                n += 1
            assigned_folders.add(folder)
            reg_row = {
                "segment_id": sid,
                "parent_segment_id": row["parent_segment_id"],
                "run_type": row["run_type"],
                "population_hash": row["population_hash"],
                "conformance_reference_mode": "latest",
                "output_folder": folder,
                "status": "pending",
                "last_run_utc": "",
                "notes": row.get("notes", ""),
                "segment_purpose": row.get("segment_purpose", ""),
                "segment_label": row.get("segment_label", ""),
            }
        registry.append(reg_row)

    dropped_ids = sorted(set(existing_by_id) - new_ids)
    if dropped_ids:
        sys.stderr.write(
            f"[WARN] Segment(s) removed from registry (no longer in manifest): "
            f"{', '.join(dropped_ids)} — review corresponding folders under segments/ "
            f"for manual cleanup\n"
        )

    return registry


def _print_summary(
    manifest_path: Path,
    registry_path: Path,
    manifest_rows: List[Dict[str, str]],
    min_files: int,
) -> None:
    bundles = [r for r in manifest_rows if r["run_type"] == "bundle"]
    refs = [r for r in manifest_rows if r["run_type"] == "reference"]
    skips = [r for r in manifest_rows if r["run_type"] == "skip"]
    regs = [r for r in manifest_rows if r["run_type"] == "registration"]

    print(f"Segment manifest written: {manifest_path}")
    print(f"Run registry written: {registry_path}")
    print()
    print(f"Run plan ({len(bundles) + len(refs)} segments):")

    print("\n  Bundle runs:")
    for r in bundles:
        print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")

    print("\n  Reference runs:")
    for r in refs:
        print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")

    if skips:
        print(f"\n  Skipped (below min_files={min_files}):")
        for r in skips:
            print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")

    if regs:
        print("\n  Registration only (hierarchy anchors):")
        for r in regs:
            print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build segment_manifest.csv and run_registry.csv from file_metadata.csv.",
    )
    parser.add_argument("--metadata-file", required=True, help="Path to file_metadata.csv")
    parser.add_argument("--out-dir", required=True, help="Directory to write output files")
    parser.add_argument("--min-files", type=int, default=3, help="Minimum file count for a segment (default: 3)")
    parser.add_argument("--enable-cross-org-template-bundles", action="store_true", help="Allow cross-org level-2 Template segments to run as bundle/reference")
    parser.add_argument(
        "--enable-parent-bundle-runs",
        action="store_true",
        help="Allow level-2 role-fixed segments (e.g. imperial|Project) to run bundle analysis even when they have child segments."
    )
    args = parser.parse_args(argv)

    metadata_path = Path(args.metadata_file)
    if not metadata_path.is_file():
        sys.stderr.write(f"[ERROR] --metadata-file not found: {metadata_path}\n")
        return 1

    out_dir = Path(args.out_dir)
    min_files: int = args.min_files

    fieldnames, rows = _read_csv(metadata_path)
    # Validate headers unconditionally — even a header-only file must declare the required columns.
    if not fieldnames:
        sys.stderr.write(f"[WARN] file_metadata.csv is completely empty (no header): {metadata_path}\n")
    else:
        missing_columns = REQUIRED_COLUMNS - set(fieldnames)
        if missing_columns:
            sys.stderr.write(
                f"[ERROR] file_metadata.csv is missing required columns: {sorted(missing_columns)}\n"
            )
            return 1
        if not rows:
            sys.stderr.write(f"[WARN] file_metadata.csv has a valid header but no data rows: {metadata_path}\n")

    skipped_blank_us = sum(1 for r in rows if not (r.get("unit_system") or "").strip())
    if skipped_blank_us:
        sys.stderr.write(f"[WARN] Excluded {skipped_blank_us} row(s) with blank unit_system\n")

    skipped_blank_eid = sum(
        1 for r in rows
        if (r.get("unit_system") or "").strip()      # unit_system present (not already counted above)
        and not (r.get("export_run_id") or "").strip()
    )
    if skipped_blank_eid:
        sys.stderr.write(f"[WARN] Excluded {skipped_blank_eid} row(s) with blank export_run_id\n")

    # Normalize once here — for the KNOWN_ROLES check and the aggregated
    # normalization-change warning below — routing both through the same
    # helper _build_segments() uses internally, so there is one source of
    # truth for "what does this dimension value canonically mean" rather than
    # two independent checks that could drift apart. _build_segments() still
    # normalizes rows itself when called directly (e.g. from tests), so
    # passing the original (un-normalized) rows through to it here is safe —
    # normalization is idempotent.
    normalized_rows, normalization_changes = _normalize_rows(rows)

    KNOWN_ROLES = {"Project", "Template", "Container", "Generic", ""}
    unknown_roles = {
        (r.get("governance_role") or "").strip()
        for r in normalized_rows
        if (r.get("governance_role") or "").strip() not in KNOWN_ROLES
    }
    for role in sorted(unknown_roles):
        sys.stderr.write(f"[WARN] Unrecognised governance_role value in metadata: '{role}' — rows with this role will create unexpected segments\n")

    if normalization_changes:
        agg: Dict[tuple, int] = defaultdict(int)
        for field, raw, canon in normalization_changes:
            agg[(field, raw, canon)] += 1
        for (field, raw, canon), count in sorted(agg.items()):
            sys.stderr.write(
                f"[WARN] Normalized {field} '{raw}' -> '{canon}' ({count} row(s)) — "
                f"check file_metadata.csv for manual-edit typos\n"
            )

    manifest_rows = _build_segments(rows, min_files, args.enable_cross_org_template_bundles, args.enable_parent_bundle_runs)

    for r in manifest_rows:
        if r["run_type"] == "bundle" and int(r["file_count"]) < min_files:
            sys.stderr.write(f"[WARN] Bundle below min_files: {r['segment_id']} ({r['file_count']} < {min_files})\n")

    ids = {r["segment_id"] for r in manifest_rows}
    for r in manifest_rows:
        if r["segment_level"] == "3" and r["parent_segment_id"] not in ids:
            sys.stderr.write(f"[WARN] Orphaned level-3 segment missing parent: {r['segment_id']} -> {r['parent_segment_id']}\n")

    bundle_by_hash = defaultdict(list)
    for r in manifest_rows:
        if r["run_type"] == "bundle":
            bundle_by_hash[r["population_hash"]].append(r["segment_id"])
    for pop_hash, segs in bundle_by_hash.items():
        if len(segs) > 1:
            sys.stderr.write(f"[WARN] Duplicate bundle population_hash {pop_hash}: {', '.join(sorted(segs))}\n")

    manifest_path = out_dir / "segment_manifest.csv"
    registry_path = out_dir / "run_registry.csv"
    membership_path = out_dir / "segment_membership.csv"

    existing_registry_rows: List[Dict[str, str]] | None = None
    if registry_path.is_file():
        _, existing_registry_rows = _read_csv(registry_path)

    existing_membership: Dict[str, List[str]] | None = None
    if membership_path.is_file():
        _, existing_membership_rows = _read_csv(membership_path)
        existing_membership = _membership_by_segment(existing_membership_rows)

    registry_rows = _build_registry(
        manifest_rows,
        existing_registry=existing_registry_rows,
        existing_membership=existing_membership,
    )
    membership_rows = _build_membership_rows(manifest_rows)

    _atomic_write_csv(manifest_path, MANIFEST_FIELDNAMES, manifest_rows)
    _atomic_write_csv(registry_path, REGISTRY_FIELDNAMES, registry_rows)
    _atomic_write_csv(membership_path, MEMBERSHIP_FIELDNAMES, membership_rows)

    _print_summary(manifest_path, registry_path, manifest_rows, min_files)
    return 0


if __name__ == "__main__":
    sys.exit(main())
