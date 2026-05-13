# -*- coding: utf-8 -*-
"""
Loaded family types discovery-stage semantic exporter.

Stable domain name: ``loaded_family_types``.

This domain intentionally uses category as the first shape gate and emits
parameter schema/provenance evidence for each loaded FamilySymbol.

Governance posture:
- Broad extraction is allowed.
- Broad governance is not.
- Parameter values are validation/debug evidence only and are excluded from
  governed semantic hash inputs.

Future steps:
1. Validate category foundation schemas across multiple projects.
2. Promote category-local stable parameters into governed identity contracts.
3. Add category-specific allow/block lists by parameter role.
4. Add targeted deep inspection mode for selected families/schema hashes.
5. Add value-level comparison only inside promoted category/type gates.
6. Add nested family/material/connector relationships as separate join layers.
7. Add compatibility/versioning rules before governed compliance use.
"""

import os
import sys
from collections import defaultdict

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str

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


def _param_id_int(param):
    try:
        return int(getattr(getattr(param, "Id", None), "IntegerValue", 0))
    except Exception:
        return 0


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


def extract(doc, ctx=None):
    info = {"count": 0, "records": [], "signature_hashes_v2": [], "hash_v2": None, "raw_count": 0}
    if FamilySymbol is None:
        return info

    try:
        symbols = list(__import__('core.collect', fromlist=['collect_types']).collect_types(doc, of_class=FamilySymbol, cctx=(ctx or {}).get('_collect') if ctx else None, cache_key='loaded_family_types:FamilySymbol:types'))
    except Exception:
        symbols = []
    info["raw_count"] = len(symbols)

    category_sigs = defaultdict(list)
    for sym in symbols:
        def _safe_attr(obj, attr, default=None):
            try:
                return getattr(obj, attr, default)
            except Exception:
                return default

        cat = getattr(sym, "Category", None)
        cat_name = safe_str(_safe_attr(cat, "Name", None)) or "none"
        cat_id = safe_str(_safe_attr(_safe_attr(cat, "Id", None), "IntegerValue", None)) or ""
        fam = _safe_attr(sym, "Family", None)
        fam_name = safe_str(_safe_attr(fam, "Name", None))
        type_name = safe_str(_safe_attr(sym, "Name", None))

        prov_rows = []
        for p in list(getattr(sym, "Parameters", []) or []):
            pname = safe_str(getattr(getattr(p, "Definition", None), "Name", None))
            guid = _safe_guid_str(p)
            pid = _param_id_int(p)
            scope = _binding_scope(doc, p, guid, pid)
            dtype = safe_str(getattr(getattr(p, "Definition", None), "ParameterType", None) or getattr(getattr(p, "Definition", None), "GetDataType", lambda: None)())
            key = ("guid:%s" % guid.lower()) if guid else (("bip:%s" % pid) if pid < 0 else ("name:%s|dt:%s|scope:%s" % (pname, dtype, scope)))
            role = _semantic_role(pname, scope)
            prov_rows.append({
                "param.name": pname,
                "param.key": key,
                "param.guid": guid or None,
                "param.id": pid,
                "param.id_sign": "negative" if pid < 0 else ("positive" if pid > 0 else "zero"),
                "param.data_type": dtype,
                "param.binding_scope": scope,
                "param.semantic_role": role,
                "param.source": "type_parameter",
                "param.value_preview": safe_str(getattr(p, "AsValueString", lambda: None)() or getattr(p, "AsString", lambda: None)()),
            })

        prov_rows = sorted(prov_rows, key=lambda r: (r["param.key"], r["param.name"]))
        schema_basis = ["%s|%s|%s|%s" % (r["param.key"], r["param.binding_scope"], r["param.semantic_role"], r["param.source"]) for r in prov_rows]
        type_schema_hash = make_hash(schema_basis)

        semantic_basis = [
            "shape_gate.category=%s|%s" % (cat_name, cat_id),
            "family.name=%s" % fam_name,
            "family.is_editable=%s" % safe_str(getattr(fam, "IsEditable", None)).lower(),
            "family.is_in_place=%s" % safe_str(getattr(fam, "IsInPlace", None)).lower(),
            "family.symbol_count=%s" % safe_str(getattr(fam, "GetFamilySymbolIds", lambda: [])() and len(list(fam.GetFamilySymbolIds()))),
            "type.name=%s" % type_name,
            "type.parameter_schema_hash=%s" % type_schema_hash,
        ] + ["prov:%s" % s for s in schema_basis if "|operational|" not in s]
        sig_hash = make_hash(semantic_basis)

        rec = {
            "record_id": "%s|%s|%s|%s" % (cat_name, fam_name, type_name, type_schema_hash),
            "category": cat_name,
            "family_name": fam_name,
            "type_name": type_name,
            "type_parameter_schema_hash": type_schema_hash,
            "signature_hash_v2": sig_hash,
            "identity_items": [
                {"k": "shape_gate.category", "v": cat_name},
                {"k": "shape_gate.category_id", "v": cat_id},
                {"k": "family.name", "v": fam_name},
                {"k": "type.name", "v": type_name},
                {"k": "type.parameter_schema_hash", "v": type_schema_hash},
            ],
            "join_items": [{"k": "type.parameter_key", "v": r["param.key"]} for r in prov_rows],
            "validation_items": [{"k": "param.provenance", "v": r} for r in prov_rows],
            "debug_items": [{"k": "param.value_preview", "v": r["param.value_preview"], "param_key": r["param.key"]} for r in prov_rows if r.get("param.value_preview")],
        }
        info["records"].append(rec)
        info["signature_hashes_v2"].append(sig_hash)
        category_sigs[cat_name].append(sig_hash)

    info["count"] = len(info["records"])
    info["signature_hashes_v2"].sort()
    info["category_hashes"] = {k: make_hash(sorted(v)) for k, v in sorted(category_sigs.items())}
    info["hash_v2"] = make_hash(info["signature_hashes_v2"])
    return info
