# -*- coding: utf-8 -*-
"""
Units domain extractor.

Captures project units settings including:
- Length, area, volume format options
- Unit types and symbols
- Accuracy settings

Identity is emitted as record.v2 per-spec records (length/area/volume),
with per-record sig_hash derived from identity_items.
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import (
    canon_str,
    canon_num,
    canon_bool,
    canon_id,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)

from core.record_v2 import (
    canonicalize_str,
    canonicalize_enum,
    canonicalize_float,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED,
    build_record_v2,
    make_identity_item,
    serialize_identity_items,
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
)
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

try:
    from Autodesk.Revit.DB import SpecTypeId
except ImportError:
    SpecTypeId = None

from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
)


# Pilot: use identity_basis.items as the single canonical evidence superset.
# sig_hash is derived from these semantic selectors (not from join-key material).
UNITS_SEMANTIC_KEYS = tuple(
    sorted(
        {
            "units.accuracy",
            "units.rounding_method",
            "units.spec",
            "units.symbol_type_id",
            "units.unit_type_id",
        }
    )
)


def extract(doc, ctx=None):
    """
    Extract Units fingerprint from document.

    Version-safe units snapshot (Revit 2022+).
    - 'repr' is the raw Units.ToString() for quick sanity.
    - 'specs' holds explicit Length/Area/Volume format options.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain)

    Returns:
        Dictionary with repr, specs, and hash
    """
    result = {
        "repr": None,
        "specs": {},
        
        # record.v2 per-record emission
        "records": [],
        "record_rows": [],

        # v2 (contract semantic hash)
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    try:
        u = doc.GetUnits()
    except Exception:
        # No API reachability: caller/runner will decide domain status; we only emit explicit v2 block.
        result["debug_v2_blocked"] = True
        result["debug_v2_block_reasons"] = {"units_unreadable": True}
        return result

    result["repr"] = safe_str(u)

    # ---- record.v2 per-spec records ----
    v2_records = []
    v2_sig_hashes = []  # non-null only
    v2_block_reasons = {}

    if SpecTypeId is None:
        # Cannot even reference required specs deterministically.
        result["debug_v2_blocked"] = True
        result["debug_v2_block_reasons"] = {"SpecTypeId_unavailable": True}
        return result

    specs = [
        ("length", SpecTypeId.Length),
        ("area",   SpecTypeId.Area),
        ("volume", SpecTypeId.Volume),
    ]

    for label, spec_id in specs:
        record_id = "units:{}".format(label)

        # Default identity items (explicit) — required keys are always present as items.
        spec_v, spec_q = canonicalize_str(label)
        items = [make_identity_item("units.spec", spec_v, spec_q)]

        fmt = None
        try:
            fmt = u.GetFormatOptions(spec_id)
        except Exception:
            fmt = None

        # unit_type_id (required)
        if fmt is None:
            unit_v, unit_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                unit_v, unit_q = canonicalize_str(safe_str(fmt.GetUnitTypeId()))
            except Exception:
                unit_v, unit_q = (None, ITEM_Q_UNREADABLE)
        items.append(make_identity_item("units.unit_type_id", unit_v, unit_q))

        # symbol_type_id (optional)
        if fmt is None:
            sym_v, sym_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                sym_v, sym_q = canonicalize_str(safe_str(fmt.GetSymbolTypeId()))
            except Exception:
                sym_v, sym_q = (None, ITEM_Q_UNREADABLE)
        items.append(make_identity_item("units.symbol_type_id", sym_v, sym_q))

        # accuracy (optional)
        if fmt is None:
            acc_v, acc_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                acc_v, acc_q = canonicalize_float(getattr(fmt, "Accuracy", None))
            except Exception:
                acc_v, acc_q = (None, ITEM_Q_UNREADABLE)
        items.append(make_identity_item("units.accuracy", acc_v, acc_q))

        # rounding_method (optional)
        if fmt is None:
            rm_v, rm_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                rm_v, rm_q = canonicalize_enum(getattr(fmt, "RoundingMethod", None))
            except Exception:
                rm_v, rm_q = (None, ITEM_Q_UNREADABLE)
        items.append(make_identity_item("units.rounding_method", rm_v, rm_q))

        # Sort items by k for validator determinism.
        items_sorted = sorted(items, key=lambda it: it.get("k", ""))

        # Minima: block if any required key q != ok
        required_qs = [spec_q, unit_q]
        required_keys = ["units.spec", "units.unit_type_id"]
        required_kq = list(zip(required_keys, required_qs))
        blocked = any(q != ITEM_Q_OK for (_, q) in required_kq)

        status_reasons = []
        any_incomplete = False
        for it in items_sorted:
            q = it.get("q")
            if q != ITEM_Q_OK:
                any_incomplete = True
                k = it.get("k")
                status_reasons.append("identity.incomplete:{}:{}".format(q, k))

        label_quality = "system"
        label_prov = "revit.SpecTypeId"
        label_display = "Units ({})".format(label)
        if blocked:
            label_quality = "placeholder_unreadable" if (unit_q == ITEM_Q_UNREADABLE) else "placeholder_missing"

        spec_name = label  # preserve the string loop key before building record label dict

        rec_label = {
            "display": label_display,
            "quality": label_quality,
            "provenance": label_prov,
            "components": {"spec": spec_name},
        }

        # Semantic basis selector for sig_hash; evidence remains canonical in identity_basis.items.
        semantic_items = [it for it in items_sorted if it.get("k") in set(UNITS_SEMANTIC_KEYS)]

        if blocked:
            rec = build_record_v2(
                domain="units",
                record_id=record_id,
                status=STATUS_BLOCKED,
                status_reasons=sorted(set(status_reasons)) or ["minima.required_not_ok"],
                sig_hash=None,
                identity_items=items_sorted,
                required_qs=(),
                label=rec_label,
            )
            rec["is_purgeable"] = None
            rec["is_purgeable_q"] = "unsupported_not_applicable"
            # Domain-level signal: v2 cannot be complete if any required key unreadable/missing.
            v2_block_reasons["record_blocked:{}".format(label)] = True
        else:
            status = STATUS_DEGRADED if any_incomplete else STATUS_OK
            preimage = serialize_identity_items(semantic_items)
            sig_hash = make_hash(preimage)
            rec = build_record_v2(
                domain="units",
                record_id=record_id,
                status=status,
                status_reasons=sorted(set(status_reasons)),
                sig_hash=sig_hash,
                identity_items=items_sorted,
                required_qs=required_qs,
                label=rec_label,
            )
            rec["is_purgeable"] = None
            rec["is_purgeable_q"] = "unsupported_not_applicable"
            v2_sig_hashes.append(sig_hash)

        # ----------------------------
        # Phase-2 additive emission (no effect on sig_hash / identity_basis)
        # ----------------------------
        pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "units")
        rec["join_key"], _missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=items_sorted,
            include_optional_items=False,
            emit_keys_used=True,
            hash_optional_items=False,
            emit_items=False,
            emit_selectors=True,
        )

        # Hypotheses only (grouping_basis=phase2.hypothesis):
        # - semantic: spec identity + unit type + numeric formatting options
        # - cosmetic: symbol selection (presentation-focused; may still affect downstream display)
        # - unknown: (none currently declared)
        semantic_keys = {
            "units.spec",
            "units.unit_type_id",
            "units.rounding_method",
        }
        cosmetic_keys = {
            "units.symbol_type_id",
            "units.accuracy",
        }

        # Selector-based explainability: use key lists instead of duplicating k/q/v evidence.
        cosmetic_items = phase2_sorted_items([dict(it) for it in items_sorted if it.get("k") in cosmetic_keys])
        unknown_items = phase2_sorted_items([dict(it) for it in items_sorted if it.get("k") not in (semantic_keys | cosmetic_keys)])

        rec["phase2"] = {
            "schema": "phase2.units.v1",
            "grouping_basis": "phase2.hypothesis",
            # Deprecated duplication path: semantic evidence is canonical in identity_basis.items.
            "cosmetic_items": cosmetic_items,
            "coordination_items": phase2_sorted_items([]),
            "unknown_items": unknown_items,
        }
        rec["sig_basis"] = {
            "schema": "units.sig_basis.v1",
            "keys_used": list(UNITS_SEMANTIC_KEYS),
        }

        v2_records.append(rec)

    # record.v2 surfaces
    result["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
    result["record_rows"] = [
        {
            "record_key": safe_str(r.get("record_id", "")),
            "sig_hash": r.get("sig_hash", None),
            "name": safe_str(r.get("label", {}).get("display", "")),
        }
        for r in result["records"]
    ]

    if v2_sig_hashes:
        result["hash_v2"] = make_hash(sorted(v2_sig_hashes))
        result["debug_v2_blocked"] = False
        result["debug_v2_block_reasons"] = {}
    else:
        result["hash_v2"] = None
        result["debug_v2_blocked"] = True
        result["debug_v2_block_reasons"] = v2_block_reasons or {"no_nonblocked_records": True}

    return result
