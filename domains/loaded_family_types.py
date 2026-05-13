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
        symbols = list(collect_types(
            doc,
            of_class=FamilySymbol,
            cctx=(ctx or {}).get("_collect") if ctx else None,
            cache_key="loaded_family_types:FamilySymbol:types",
        ))
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
        cat_name_v, cat_name_q = canonicalize_str(_safe_attr(cat, "Name", None))
        cat_id_v, cat_id_q = canonicalize_int(_safe_attr(_safe_attr(cat, "Id", None), "IntegerValue", None))

        fam = _safe_attr(sym, "Family", None)
        fam_name_v, fam_name_q = canonicalize_str(_safe_attr(fam, "Name", None))
        type_name_v, type_name_q = canonicalize_str(_safe_attr(sym, "Name", None))

        fam_is_in_place_v, fam_is_in_place_q = canonicalize_bool(_safe_attr(fam, "IsInPlace", None))
        fam_is_editable_v, fam_is_editable_q = canonicalize_bool(_safe_attr(fam, "IsEditable", None))

        fam_symbol_count_raw = None
        try:
            sym_ids = fam.GetFamilySymbolIds() if fam is not None else None
            fam_symbol_count_raw = len(list(sym_ids)) if sym_ids is not None else None
        except Exception:
            pass
        fam_symbol_count_v, fam_symbol_count_q = canonicalize_int(fam_symbol_count_raw)

        prov_rows = []
        for p in list(getattr(sym, "Parameters", []) or []):
            pname = safe_str(getattr(getattr(p, "Definition", None), "Name", None))
            guid = _safe_guid_str(p)
            pid = _param_id_int(p)
            scope = _binding_scope(doc, p, guid, pid)
            dtype = safe_str(
                getattr(getattr(p, "Definition", None), "ParameterType", None)
                or getattr(getattr(p, "Definition", None), "GetDataType", lambda: None)()
            )
            key = (
                ("guid:%s" % guid.lower()) if guid
                else (("bip:%s" % pid) if pid < 0
                else ("name:%s|dt:%s|scope:%s" % (pname, dtype, scope)))
            )
            role = _semantic_role(pname, scope)
            prov_rows.append({
                "lftp.key": key,
                "lftp.name": pname,
                "lftp.guid": guid or None,
                "lftp.id": pid,
                "lftp.id_sign": "negative" if pid < 0 else ("positive" if pid > 0 else "zero"),
                "lftp.data_type": dtype,
                "lftp.binding_scope": scope,
                "lftp.semantic_role": role,
                "lftp.source": "type_parameter",
            })

        prov_rows = sorted(prov_rows, key=lambda r: (r["lftp.key"], r["lftp.name"]))
        schema_basis = [
            "%s|%s|%s|%s" % (r["lftp.key"], r["lftp.binding_scope"], r["lftp.semantic_role"], r["lftp.source"])
            for r in prov_rows
        ]
        type_schema_hash = make_hash(schema_basis)
        type_schema_hash_v, type_schema_hash_q = canonicalize_str(type_schema_hash)
        type_param_count_v, type_param_count_q = canonicalize_int(len(prov_rows))

        identity_items = [
            make_identity_item("lft.shape_gate.category", cat_name_v, cat_name_q),
            make_identity_item("lft.shape_gate.category_id", cat_id_v, cat_id_q),
            make_identity_item("lft.family_name", fam_name_v, fam_name_q),
            make_identity_item("lft.type_name", type_name_v, type_name_q),
            make_identity_item("lft.type_parameter_schema_hash", type_schema_hash_v, type_schema_hash_q),
            make_identity_item("lft.type_parameter_count", type_param_count_v, type_param_count_q),
            make_identity_item("lft.family_is_in_place", fam_is_in_place_v, fam_is_in_place_q),
            make_identity_item("lft.family_is_editable", fam_is_editable_v, fam_is_editable_q),
            make_identity_item("lft.family_symbol_count", fam_symbol_count_v, fam_symbol_count_q),
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

        label_parts = []
        if fam_name_v:
            label_parts.append(fam_name_v)
        if type_name_v:
            label_parts.append(type_name_v)
        label_display = " : ".join(label_parts) if label_parts else "loaded_family_type"
        label_quality = "human" if (fam_name_v and type_name_v) else "placeholder_missing"

        label = {
            "display": label_display,
            "quality": label_quality,
            "provenance": "revit.FamilySymbol.Family.Name+Name",
            "components": {
                "category": cat_name_v or "",
                "family_name": fam_name_v or "",
                "type_name": type_name_v or "",
            },
        }

        record_id = "%s|%s|%s|%s" % (
            cat_name_v or "none",
            fam_name_v or "none",
            type_name_v or "none",
            type_schema_hash,
        )
        required_qs = [cat_name_q, fam_name_q, type_name_q]

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
