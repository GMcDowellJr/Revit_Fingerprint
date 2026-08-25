# -*- coding: utf-8 -*-
"""
Units domain extractor.

Captures project units settings including:
- Format options for 38 specs across all Revit disciplines
  (Common, Electrical, HVAC, Piping, Structural)
- Unit types and symbols
- Accuracy, rounding, and formatting-flag settings

All specs are extracted for every document regardless of discipline.
GetFormatOptions returns a valid FormatOptions object for all specs
on any live document. ITEM_Q_UNREADABLE is a defensive fallback only
and is not expected to fire in normal execution.

Identity is emitted as record.v2 per-spec records (domain="units"),
with per-record sig_hash derived from identity_items.

extract_units_doc() emits a second, independent top-level domain
(domain="units_doc") for the 3 document-level formatting fields
(decimal_symbol, digit_grouping_amount, digit_grouping_symbol) that come
off doc.GetUnits() directly rather than any per-spec FormatOptions. This
mirrors domains/worksets.py's worksets/worksets_doc split: a synthetic
doc-level record has no real units.spec/units.unit_type_id to supply, and
those are required_keys for domain "units" (block_if_any_required_not_ok),
so the doc-level record must live under its own domain with its own
(empty) required_keys rather than being folded into a "units:_doc"
record_id under domain "units". record_id still uses the same
family-prefixed "_doc" suffix convention as worksets_doc for
readability; only the `domain` differs.
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.sig_hash_policy import resolve_sig_hash_keys
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
    canonicalize_bool,
    canonicalize_int,
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
            "units.use_default",
            "units.use_digit_grouping",
            "units.use_plus_prefix",
            "units.suppress_leading_zeros",
            "units.suppress_spaces",
            "units.suppress_trailing_zeros",
        }
    )
)

# Document-level (domain="units_doc") semantic keys -- all 3 fields are
# genuine formatting behavior (not session/presentation state), so all 3
# drive units_doc's sig_hash, unlike worksets_doc's active_workset_name
# exclusion.
UNITS_DOC_SEMANTIC_KEYS = tuple(
    sorted(
        {
            "units_doc.decimal_symbol",
            "units_doc.digit_grouping_amount",
            "units_doc.digit_grouping_symbol",
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

    def _resolve_spec(path_fn):
        """Resolve a SpecTypeId attribute path once. Returns None if unavailable."""
        try:
            return path_fn()
        except Exception:
            return None

    specs_raw = [
        # ── Common (discipline: autodesk.spec:discipline-1.0.0) ─────────────────
        ("length",              _resolve_spec(lambda: SpecTypeId.Length)),
        ("area",                _resolve_spec(lambda: SpecTypeId.Area)),
        ("volume",              _resolve_spec(lambda: SpecTypeId.Volume)),
        ("angle",               _resolve_spec(lambda: SpecTypeId.Angle)),
        ("slope",               _resolve_spec(lambda: SpecTypeId.Slope)),
        ("speed",               _resolve_spec(lambda: SpecTypeId.Speed)),
        ("time",                _resolve_spec(lambda: SpecTypeId.Time)),
        ("mass_density",        _resolve_spec(lambda: SpecTypeId.MassDensity)),
        ("currency",            _resolve_spec(lambda: SpecTypeId.Currency)),
        ("rotation_angle",      _resolve_spec(lambda: SpecTypeId.RotationAngle)),
        ("distance",            _resolve_spec(lambda: SpecTypeId.Distance)),

        # ── Electrical (discipline: autodesk.spec.discipline:electrical-1.0.0) ──
        ("electrical_apparent_power",    _resolve_spec(lambda: SpecTypeId.ApparentPower)),
        ("electrical_current",           _resolve_spec(lambda: SpecTypeId.Current)),
        ("electrical_potential",         _resolve_spec(lambda: SpecTypeId.ElectricalPotential)),
        ("electrical_frequency",         _resolve_spec(lambda: SpecTypeId.ElectricalFrequency)),
        ("electrical_power",             _resolve_spec(lambda: SpecTypeId.ElectricalPower)),
        ("electrical_temperature",       _resolve_spec(lambda: SpecTypeId.ElectricalTemperature)),
        ("electrical_wattage",           _resolve_spec(lambda: SpecTypeId.Wattage)),

        # ── HVAC (discipline: autodesk.spec.discipline:hvac-1.0.0) ──────────────
        ("hvac_air_flow",                _resolve_spec(lambda: SpecTypeId.AirFlow)),
        ("hvac_cooling_load",            _resolve_spec(lambda: SpecTypeId.CoolingLoad)),
        ("hvac_heating_load",            _resolve_spec(lambda: SpecTypeId.HeatingLoad)),
        ("hvac_pressure",                _resolve_spec(lambda: SpecTypeId.HvacPressure)),
        ("hvac_temperature",             _resolve_spec(lambda: SpecTypeId.HvacTemperature)),
        ("hvac_velocity",                _resolve_spec(lambda: SpecTypeId.HvacVelocity)),
        ("hvac_duct_size",               _resolve_spec(lambda: SpecTypeId.DuctSize)),
        ("hvac_power",                   _resolve_spec(lambda: SpecTypeId.HvacPower)),

        # ── Piping (discipline: autodesk.spec.discipline:piping-1.0.0) ──────────
        ("piping_flow",                  _resolve_spec(lambda: SpecTypeId.Flow)),
        ("piping_pressure",              _resolve_spec(lambda: SpecTypeId.PipingPressure)),
        ("piping_temperature",           _resolve_spec(lambda: SpecTypeId.PipingTemperature)),
        ("piping_velocity",              _resolve_spec(lambda: SpecTypeId.PipingVelocity)),
        ("piping_pipe_size",             _resolve_spec(lambda: SpecTypeId.PipeSize)),

        # ── Structural (discipline: autodesk.spec.discipline:structural-1.0.0) ──
        ("structural_force",               _resolve_spec(lambda: SpecTypeId.Force)),
        ("structural_moment",              _resolve_spec(lambda: SpecTypeId.Moment)),
        ("structural_stress",              _resolve_spec(lambda: SpecTypeId.Stress)),
        ("structural_bar_diameter",        _resolve_spec(lambda: SpecTypeId.BarDiameter)),
        ("structural_reinforcement_cover", _resolve_spec(lambda: SpecTypeId.ReinforcementCover)),
        ("structural_section_dimension",   _resolve_spec(lambda: SpecTypeId.SectionDimension)),
        ("structural_displacement",        _resolve_spec(lambda: SpecTypeId.Displacement)),
    ]

    # Filter to specs that resolved successfully in this Revit version
    specs = [(label, sid) for label, sid in specs_raw if sid is not None]

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
                forge_id = fmt.GetUnitTypeId()
                uid_str = forge_id.TypeId  # direct access — getattr fails in IronPython 2.7
                unit_v, unit_q = canonicalize_str(safe_str(uid_str))
            except Exception:
                unit_v, unit_q = (None, ITEM_Q_UNREADABLE)
        items.append(make_identity_item("units.unit_type_id", unit_v, unit_q))

        # symbol_type_id (optional)
        if fmt is None:
            sym_v, sym_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                sym_forge_id = fmt.GetSymbolTypeId()
                sym_str = sym_forge_id.TypeId  # direct access — getattr fails in IronPython 2.7
                sym_v, sym_q = canonicalize_str(safe_str(sym_str))
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

        # use_default (optional)
        if fmt is None:
            ud_v, ud_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                ud_v, ud_q = canonicalize_bool(getattr(fmt, "UseDefault", None))
            except Exception:
                ud_v, ud_q = (None, ITEM_Q_UNREADABLE)
        items.append(make_identity_item("units.use_default", ud_v, ud_q))

        # use_digit_grouping (optional)
        if fmt is None:
            udg_v, udg_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                udg_v, udg_q = canonicalize_bool(getattr(fmt, "UseDigitGrouping", None))
            except Exception:
                udg_v, udg_q = (None, ITEM_Q_UNREADABLE)
        items.append(make_identity_item("units.use_digit_grouping", udg_v, udg_q))

        # use_plus_prefix (optional)
        if fmt is None:
            upp_v, upp_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                upp_v, upp_q = canonicalize_bool(getattr(fmt, "UsePlusPrefix", None))
            except Exception:
                upp_v, upp_q = (None, ITEM_Q_UNREADABLE)
        items.append(make_identity_item("units.use_plus_prefix", upp_v, upp_q))

        # suppress_leading_zeros (optional)
        if fmt is None:
            slz_v, slz_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                slz_v, slz_q = canonicalize_bool(getattr(fmt, "SuppressLeadingZeros", None))
            except Exception:
                slz_v, slz_q = (None, ITEM_Q_UNREADABLE)
        items.append(make_identity_item("units.suppress_leading_zeros", slz_v, slz_q))

        # suppress_spaces (optional)
        if fmt is None:
            ssp_v, ssp_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                ssp_v, ssp_q = canonicalize_bool(getattr(fmt, "SuppressSpaces", None))
            except Exception:
                ssp_v, ssp_q = (None, ITEM_Q_UNREADABLE)
        items.append(make_identity_item("units.suppress_spaces", ssp_v, ssp_q))

        # suppress_trailing_zeros (optional)
        if fmt is None:
            stz_v, stz_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                stz_v, stz_q = canonicalize_bool(getattr(fmt, "SuppressTrailingZeros", None))
            except Exception:
                stz_v, stz_q = (None, ITEM_Q_UNREADABLE)
        items.append(make_identity_item("units.suppress_trailing_zeros", stz_v, stz_q))

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
        # Resolved from ctx["sig_hash_policies"] (policies/domain_sig_hash_policies.json) when
        # available, with UNITS_SEMANTIC_KEYS as the fallback -- see core/sig_hash_policy.py's
        # resolve_sig_hash_keys() and DECISIONS.md D-039/D-040.
        sig_hash_keys = set(resolve_sig_hash_keys((ctx or {}).get("sig_hash_policies"), "units", UNITS_SEMANTIC_KEYS))
        semantic_items = [it for it in items_sorted if it.get("k") in sig_hash_keys]

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
            "units.use_default",
            "units.use_digit_grouping",
            "units.use_plus_prefix",
            "units.suppress_leading_zeros",
            "units.suppress_spaces",
            "units.suppress_trailing_zeros",
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


def extract_units_doc(doc, ctx=None):
    """
    Extract the single document-level units summary record
    (domain="units_doc").

    Emitted as its own top-level domain rather than a "units:_doc"-tagged
    record folded into extract()'s "units" payload: a synthetic doc-level
    record has no real units.spec/units.unit_type_id to supply, and those
    are domain "units"'s required_keys (block_if_any_required_not_ok), so
    it would fail contract validation there. It also would not flatten
    under the right domain regardless -- tools/export_to_flat_tables.py
    derives each flattened row's `domain` from the top-level fingerprint
    key the runner stores the payload under, not from the record's own
    `domain` field (see domains/worksets.py's extract_worksets_doc, which
    solves the identical problem the same way).

    Document-level fields are optional (never block): a doc-level read
    hiccup must not leave the summary record entirely absent.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused)

    Returns:
        Dictionary with count, hash_v2, records, record_rows.
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "records": [],
        "record_rows": [],

        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    try:
        u = doc.GetUnits()
    except Exception:
        u = None

    if u is None:
        ds_v, ds_q = (None, ITEM_Q_UNREADABLE)
        dga_v, dga_q = (None, ITEM_Q_UNREADABLE)
        dgs_v, dgs_q = (None, ITEM_Q_UNREADABLE)
    else:
        try:
            ds_v, ds_q = canonicalize_enum(getattr(u, "DecimalSymbol", None))
        except Exception:
            ds_v, ds_q = (None, ITEM_Q_UNREADABLE)
        try:
            dga_v, dga_q = canonicalize_int(getattr(u, "DigitGroupingAmount", None))
        except Exception:
            dga_v, dga_q = (None, ITEM_Q_UNREADABLE)
        try:
            dgs_v, dgs_q = canonicalize_enum(getattr(u, "DigitGroupingSymbol", None))
        except Exception:
            dgs_v, dgs_q = (None, ITEM_Q_UNREADABLE)

    doc_items = [
        make_identity_item("units_doc.decimal_symbol", ds_v, ds_q),
        make_identity_item("units_doc.digit_grouping_amount", dga_v, dga_q),
        make_identity_item("units_doc.digit_grouping_symbol", dgs_v, dgs_q),
    ]
    doc_items_sorted = sorted(doc_items, key=lambda it: it.get("k", ""))

    status_reasons = []
    any_incomplete = False
    for it in doc_items_sorted:
        if it.get("q") != ITEM_Q_OK:
            any_incomplete = True
            status_reasons.append("identity.incomplete:{}:{}".format(it.get("q"), it.get("k")))

    status = STATUS_DEGRADED if any_incomplete else STATUS_OK
    doc_sig_hash_keys = set(resolve_sig_hash_keys((ctx or {}).get("sig_hash_policies"), "units_doc", UNITS_DOC_SEMANTIC_KEYS))
    semantic_items = [it for it in doc_items_sorted if it.get("k") in doc_sig_hash_keys]
    sig_hash = make_hash(serialize_identity_items(semantic_items))

    rec = build_record_v2(
        domain="units_doc",
        record_id="units:_doc",
        status=status,
        status_reasons=sorted(set(status_reasons)),
        sig_hash=sig_hash,
        identity_items=doc_items_sorted,
        required_qs=[],
        label={
            "display": "Units (Document Summary)",
            "quality": "system",
            "provenance": "none",
            "components": {},
        },
    )
    rec["is_purgeable"] = None
    rec["is_purgeable_q"] = "unsupported_not_applicable"

    pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "units_doc")
    rec["join_key"], _missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=doc_items_sorted,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )

    rec["phase2"] = {
        "schema": "phase2.units_doc.v1",
        "grouping_basis": "phase2.hypothesis",
        "cosmetic_items": phase2_sorted_items([]),
        "coordination_items": phase2_sorted_items([]),
        "unknown_items": phase2_sorted_items([]),
    }
    rec["sig_basis"] = {
        "schema": "units_doc.sig_basis.v1",
        "keys_used": list(UNITS_DOC_SEMANTIC_KEYS),
    }

    info["records"] = [rec]
    info["count"] = 1
    info["raw_count"] = 1
    info["record_rows"] = [{
        "record_key": safe_str(rec.get("record_id", "")),
        "sig_hash": rec.get("sig_hash", None),
        "name": safe_str((rec.get("label", {}) or {}).get("display", "")),
    }]

    if rec.get("sig_hash"):
        info["hash_v2"] = rec["sig_hash"]
        info["debug_v2_blocked"] = False
        info["debug_v2_block_reasons"] = {}
    else:
        info["hash_v2"] = None
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"record_blocked:units:_doc": True}

    return info
