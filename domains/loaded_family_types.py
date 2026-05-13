# -*- coding: utf-8 -*-
"""
Loaded family types discovery-stage semantic exporter.

Stable domain name: ``loaded_family_types``.

Records are emitted at family granularity (one record per Family element), not
per FamilySymbol.  All types belonging to a family share the same parameter
schema; value variation across types is captured as value distributions in
``parameter_rows`` (``lftp.value_set``, ``lftp.value_uniform``,
``lftp.value_distinct_count``).

Governance posture:
- Broad extraction is allowed.
- Broad governance is not.
- Parameter values are validation/debug evidence only and are excluded from
  governed semantic hash inputs.

sig_hash composition: category + schema_hash + is_in_place + is_editable.
family_name is label-only — not included in sig or join hashes.

Future steps:
1. Validate category foundation schemas across multiple projects.
2. Promote category-local stable parameters into governed identity contracts.
3. Add category-specific allow/block lists by parameter role.
4. Add targeted deep inspection mode for selected families/schema hashes.
5. Add value-level comparison only inside promoted category/type gates.
6. Add nested family/material/connector relationships as separate join layers.
7. Add compatibility/versioning rules before governed compliance use.
8. Surface individual parameter values as identity items for greedy/Pareto
   join discovery (current schema_hash is opaque to discovery tools).
"""

import os
import sys
from collections import defaultdict

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.collect import collect_types
from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    canonicalize_str,
    canonicalize_int,
    canonicalize_bool,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)

try:
    from Autodesk.Revit.DB import FamilySymbol, ParameterElement, SharedParameterElement
except ImportError:
    FamilySymbol = None
    ParameterElement = None
    SharedParameterElement = None

DOMAIN_NAME = "loaded_family_types"
_CLASSIFICATION_NAMES = {
    "Type Mark", "Assembly Code", "Assembly Description", "OmniClass Number",
    "OmniClass Title", "Code Name", "Keynote", "Manufacturer", "Model", "Description", "URL",
}
_OPERATIONAL_TOKENS = ("workset", "edited by", "owner", "ownership", "worksharing")


def _safe_attr(obj, attr, default=None):
    try:
        return getattr(obj, attr, default)
    except Exception:
        return default


def _param_id_int(param):
    try:
        return int(getattr(getattr(param, "Id", None), "IntegerValue", 0))
    except Exception:
        return 0


def _read_param_value(p):
    """Return (storage_type, has_value, value_display, value_raw) for a parameter.

    storage_type: lowercase StorageType name ("string","double","integer","elementid","none")
    has_value:    "true"/"false"/"unreadable"
    value_display: AsValueString() — works for all storage types; Revit's UI representation
    value_raw:    typed raw value as string — double kept full precision, elementid as int string
    """
    storage_type = "none"
    has_value = "false"
    value_display = None
    value_raw = None
    try:
        st = getattr(p, "StorageType", None)
        if st is not None:
            storage_type = safe_str(st).split(".")[-1].lower()
        hv = getattr(p, "HasValue", None)
        has_value = "true" if hv else ("false" if hv is not None else "unreadable")
    except Exception:
        has_value = "unreadable"

    if has_value != "true":
        return storage_type, has_value, None, None

    try:
        v = p.AsValueString()
        value_display = safe_str(v) if v is not None else None
    except Exception:
        pass

    try:
        if storage_type == "string":
            v = p.AsString()
            value_raw = safe_str(v) if v is not None else None
        elif storage_type == "double":
            v = p.AsDouble()
            value_raw = repr(v) if v is not None else None
        elif storage_type == "integer":
            v = p.AsInteger()
            value_raw = str(v) if v is not None else None
        elif storage_type == "elementid":
            eid = p.AsElementId()
            iv = getattr(eid, "IntegerValue", None)
            value_raw = str(iv) if iv is not None else None
    except Exception:
        pass

    return storage_type, has_value, value_display, value_raw


def _binding_scope(doc, param, guid, pid):
    if pid < 0:
        return "builtin_parameter"
    if guid:
        try:
            if SharedParameterElement is not None and doc.GetElement(param.Id) and isinstance(doc.GetElement(param.Id), SharedParameterElement):
                return "shared_project_parameter"
        except Exception:
            pass
        return "shared_parameter"
    if pid > 0:
        try:
            pe = doc.GetElement(param.Id)
            if pe is not None and ParameterElement is not None and isinstance(pe, ParameterElement):
                return "project_parameter"
        except Exception:
            pass
        return "local_or_family_parameter"
    return "unknown"


def _semantic_role(name, scope):
    low = safe_str(name).strip().lower()
    if any(t in low for t in _OPERATIONAL_TOKENS):
        return "operational"
    if safe_str(name) in _CLASSIFICATION_NAMES:
        return "classification"
    if scope in ("builtin_parameter", "shared_project_parameter", "shared_parameter", "project_parameter"):
        return "candidate_semantic"
    return "unknown"


def _safe_guid_str(param):
    """Return GUID string when parameter is shared; empty string otherwise."""
    try:
        raw = getattr(param, "GUID", None)
    except Exception:
        return ""
    return safe_str(raw)


def _build_param_key(pname, guid, pid, dtype, scope):
    if guid:
        return "guid:%s" % guid.lower()
    if pid < 0:
        return "bip:%s" % pid
    return "name:%s|dt:%s|scope:%s" % (pname, dtype, scope)


def _extract_param_meta(p, doc):
    """Extract stable metadata for a parameter definition (schema-level, not value-level)."""
    pname = safe_str(getattr(getattr(p, "Definition", None), "Name", None))
    guid = _safe_guid_str(p)
    pid = _param_id_int(p)
    scope = _binding_scope(doc, p, guid, pid)
    dtype = safe_str(
        getattr(getattr(p, "Definition", None), "ParameterType", None)
        or getattr(getattr(p, "Definition", None), "GetDataType", lambda: None)()
    )
    key = _build_param_key(pname, guid, pid, dtype, scope)
    role = _semantic_role(pname, scope)
    storage_type, _, _, _ = _read_param_value(p)
    return key, {
        "lftp.key": key,
        "lftp.name": pname,
        "lftp.guid": guid or None,
        "lftp.id": pid,
        "lftp.id_sign": "negative" if pid < 0 else ("positive" if pid > 0 else "zero"),
        "lftp.storage_type": storage_type,
        "lftp.data_type": dtype,
        "lftp.binding_scope": scope,
        "lftp.semantic_role": role,
        "lftp.source": "type_parameter",
    }


def extract(doc, ctx=None):
    info = {"count": 0, "records": [], "signature_hashes_v2": [], "hash_v2": None, "raw_count": 0}
    if FamilySymbol is None:
        return info

    try:
        symbols = list(collect_types(
            doc,
            of_class=FamilySymbol,
            cctx=(ctx or {}).get("_collect") if ctx else None,
            cache_key="loaded_family_types:FamilySymbol:types",
        ))
    except Exception:
        symbols = []
    info["raw_count"] = len(symbols)

    # Group FamilySymbols by Family element ID.
    # All types of a family share the same parameter schema; collapsing to
    # family granularity reduces record count and enables value distributions.
    fam_groups = defaultdict(list)
    for sym in symbols:
        fam = _safe_attr(sym, "Family", None)
        fam_id_int = _safe_attr(_safe_attr(fam, "Id", None), "IntegerValue", None)
        group_key = str(fam_id_int) if fam_id_int is not None else "__no_family__"
        fam_groups[group_key].append(sym)

    category_sigs = defaultdict(list)

    for _group_key, fam_syms in sorted(fam_groups.items()):
        first = fam_syms[0]

        fam = _safe_attr(first, "Family", None)
        cat = getattr(first, "Category", None)
        cat_name_v, cat_name_q = canonicalize_str(_safe_attr(cat, "Name", None))
        cat_id_v, cat_id_q = canonicalize_int(_safe_attr(_safe_attr(cat, "Id", None), "IntegerValue", None))

        fam_name_v, fam_name_q = canonicalize_str(_safe_attr(fam, "Name", None))
        fam_is_in_place_v, fam_is_in_place_q = canonicalize_bool(_safe_attr(fam, "IsInPlace", None))
        fam_is_editable_v, fam_is_editable_q = canonicalize_bool(_safe_attr(fam, "IsEditable", None))

        fam_symbol_count_raw = None
        try:
            sym_ids = fam.GetFamilySymbolIds() if fam is not None else None
            fam_symbol_count_raw = len(list(sym_ids)) if sym_ids is not None else None
        except Exception:
            pass
        fam_symbol_count_v, fam_symbol_count_q = canonicalize_int(fam_symbol_count_raw)
        type_count_v, type_count_q = canonicalize_int(len(fam_syms))

        # --- Parameter schema from first type ---
        # Schema is stable per family; all types share the same parameter definitions.
        param_meta = {}
        for p in list(getattr(first, "Parameters", []) or []):
            key, meta = _extract_param_meta(p, doc)
            param_meta[key] = meta

        # --- Value distributions across all types ---
        # Collect (has_value, value_display, value_raw) per parameter key per type.
        param_values = defaultdict(list)
        for sym in fam_syms:
            for p in list(getattr(sym, "Parameters", []) or []):
                pname = safe_str(getattr(getattr(p, "Definition", None), "Name", None))
                guid = _safe_guid_str(p)
                pid = _param_id_int(p)
                scope = _binding_scope(doc, p, guid, pid)
                dtype = safe_str(
                    getattr(getattr(p, "Definition", None), "ParameterType", None)
                    or getattr(getattr(p, "Definition", None), "GetDataType", lambda: None)()
                )
                key = _build_param_key(pname, guid, pid, dtype, scope)
                _, has_value, value_display, value_raw = _read_param_value(p)
                param_values[key].append((has_value, value_display, value_raw))

        # --- Build provenance rows with distributions ---
        prov_rows = []
        for key in sorted(param_meta.keys()):
            meta = dict(param_meta[key])
            vals = param_values.get(key, [])
            display_vals = [v[1] for v in vals if v[1] is not None]
            raw_vals = [v[2] for v in vals if v[2] is not None]
            distinct_display = sorted(set(display_vals))
            distinct_raw = sorted(set(raw_vals))

            any_true = any(v[0] == "true" for v in vals)
            all_true = all(v[0] == "true" for v in vals) if vals else False
            has_value_agg = "true" if all_true else ("partial" if any_true else "false")

            meta["lftp.has_value"] = has_value_agg
            meta["lftp.value_uniform"] = "true" if len(distinct_display) <= 1 else "false"
            meta["lftp.value_distinct_count"] = str(len(distinct_display))
            meta["lftp.value_set"] = "|".join(distinct_display) if distinct_display else None
            meta["lftp.value_raw_set"] = "|".join(distinct_raw) if distinct_raw else None
            prov_rows.append(meta)

        schema_basis = [
            "%s|%s|%s|%s" % (r["lftp.key"], r["lftp.binding_scope"], r["lftp.semantic_role"], r["lftp.source"])
            for r in prov_rows
        ]
        type_schema_hash = make_hash(schema_basis)
        type_schema_hash_v, type_schema_hash_q = canonicalize_str(type_schema_hash)
        type_param_count_v, type_param_count_q = canonicalize_int(len(prov_rows))

        # Identity items: category + schema structure + family flags.
        # family_name is label-only — excluded so sig_hash is name-independent
        # and two families with the same behavioral definition compare equal.
        identity_items = [
            make_identity_item("lft.shape_gate.category", cat_name_v, cat_name_q),
            make_identity_item("lft.shape_gate.category_id", cat_id_v, cat_id_q),
            make_identity_item("lft.type_parameter_schema_hash", type_schema_hash_v, type_schema_hash_q),
            make_identity_item("lft.type_parameter_count", type_param_count_v, type_param_count_q),
            make_identity_item("lft.family_is_in_place", fam_is_in_place_v, fam_is_in_place_q),
            make_identity_item("lft.family_is_editable", fam_is_editable_v, fam_is_editable_q),
            make_identity_item("lft.family_symbol_count", fam_symbol_count_v, fam_symbol_count_q),
            make_identity_item("lft.type_count", type_count_v, type_count_q),
        ]

        status_reasons = []
        any_incomplete = False
        for it in identity_items:
            q = it.get("q")
            if q != ITEM_Q_OK:
                any_incomplete = True
                status_reasons.append("identity.incomplete:{}:{}".format(q, it.get("k")))
        status = STATUS_OK if not any_incomplete else STATUS_DEGRADED

        preimage = serialize_identity_items(identity_items)
        sig_hash = make_hash(preimage)

        label_display = fam_name_v if fam_name_v else "loaded_family_type"
        if cat_name_v:
            label_display = "%s : %s" % (cat_name_v, label_display)
        label_quality = "human" if fam_name_v else "placeholder_missing"

        label = {
            "display": label_display,
            "quality": label_quality,
            "provenance": "revit.FamilySymbol.Family.Name",
            "components": {
                "category": cat_name_v or "",
                "family_name": fam_name_v or "",
            },
        }

        # record_id includes family_name for file-local uniqueness; it is NOT
        # a cross-project identifier (record_id_scope = file_local).
        record_id = "%s|%s|%s" % (
            cat_name_v or "none",
            fam_name_v or "none",
            type_schema_hash,
        )
        required_qs = [cat_name_q, type_schema_hash_q]

        rec = build_record_v2(
            domain=DOMAIN_NAME,
            record_id=record_id,
            record_id_alg="loaded_family_types_composite_v1",
            record_id_scope="file_local",
            status=status,
            status_reasons=sorted(set(status_reasons)),
            sig_hash=sig_hash,
            identity_items=identity_items,
            required_qs=required_qs,
            label=label,
        )

        rec["parameter_rows"] = [
            dict({"param_index": i}, **r)
            for i, r in enumerate(prov_rows)
        ]

        info["records"].append(rec)
        info["signature_hashes_v2"].append(sig_hash)
        category_sigs[cat_name_v or "none"].append(sig_hash)

    info["count"] = len(info["records"])
    info["signature_hashes_v2"].sort()
    info["category_hashes"] = {k: make_hash(sorted(v)) for k, v in sorted(category_sigs.items())}
    info["hash_v2"] = make_hash(info["signature_hashes_v2"]) if info["signature_hashes_v2"] else None
    return info
