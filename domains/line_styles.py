# -*- coding: utf-8 -*-
"""
Line Styles domain extractor.

Fingerprints Line Styles (subcategories under Lines category) including:
- Projection/cut lineweights
- Line color
- Line pattern reference

Per-record identity: line style name (name-based, not UniqueId)
Ordering: order-insensitive (sorted before hashing)
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
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED,
    canonicalize_str,
    canonicalize_int,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)
from core.feature_items import make_feature_item
from core.stratum_features import build_stratum_features_v1
from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
)

from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

try:
    from Autodesk.Revit.DB import Category, BuiltInCategory, GraphicsStyleType, ElementId
except ImportError:
    Category = None
    BuiltInCategory = None
    GraphicsStyleType = None
    ElementId = None

LINE_STYLE_SEMANTIC_KEYS = sorted([
    "line_style.pattern_ref.kind",
    "line_style.pattern_ref.sig_hash",
    "line_style.weight.projection",
])

def extract(doc, ctx=None):
    """
    Extract Line Styles fingerprint from document.

    record.v2 surfaces:
      - info["hash_v2"], info["records"] (record.v2 dicts), info["signature_hashes_v2"]

    Pattern references:
      - Uses line_patterns record.v2 sig_hash via ctx["line_pattern_uid_to_hash"].
      - No sentinel literals are injected into identity items.
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "names": [],
        # record.v2
        "records": [],
        "signature_hashes_v2": [],
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},

        # debug counters
        "debug_fail_get_lines_cat": 0,
        "debug_fail_subcats": 0,
        "debug_skipped_no_name": 0,
        "debug_fail_record_build": 0,
    }

    if Category is None or BuiltInCategory is None or GraphicsStyleType is None or ElementId is None:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"revit_api_unavailable": True}
        return info

    # Dependency map: LinePatternElement.UniqueId -> line_patterns record.v2 sig_hash
    lp_uid_to_sig_hash_v2 = None
    try:
        lp_uid_to_sig_hash_v2 = (ctx or {}).get("line_pattern_uid_to_hash", None) if ctx is not None else None
        if not isinstance(lp_uid_to_sig_hash_v2, dict) or not lp_uid_to_sig_hash_v2:
            lp_uid_to_sig_hash_v2 = None
    except Exception:
        lp_uid_to_sig_hash_v2 = None

    # Collect line style subcategories under OST_Lines
    try:
        lines_cat = Category.GetCategory(doc, BuiltInCategory.OST_Lines)
    except Exception:
        info["debug_fail_get_lines_cat"] += 1
        lines_cat = None

    subs = []
    try:
        subs = list(getattr(lines_cat, "SubCategories", []) or []) if lines_cat is not None else []
    except Exception:
        info["debug_fail_subcats"] += 1
        subs = []

    info["raw_count"] = len(subs)

    names = []
    v2_records = []
    v2_sig_hashes = []
    v2_any_blocked = False
    v2_block_reasons = {}

    for sc in subs:
        try:
            sc_name = canon_str(getattr(sc, "Name", None))
            if not sc_name:
                info["debug_skipped_no_name"] += 1
                continue
            names.append(sc_name)

            # weights (legacy: None -> S_MISSING in signature)
            try:
                w_proj = sc.GetLineWeight(GraphicsStyleType.Projection)
            except Exception:
                w_proj = None
            try:
                w_cut = sc.GetLineWeight(GraphicsStyleType.Cut)
            except Exception:
                w_cut = None

            # color (legacy uses S_MISSING sentinel on exception)
            try:
                c = sc.LineColor
                rgb_sig = "{}-{}-{}".format(int(c.Red), int(c.Green), int(c.Blue))
            except Exception:
                rgb_sig = S_MISSING

            # -------------------------
            # record.v2 identity + sig_hash (NO sentinel literals; q marks missing/unreadable)
            # -------------------------
            status_reasons = []
            status_v2 = STATUS_OK

            identity_items = []

            # Path is part of authoritative identity for this domain (name-derived, but required to reproduce sig_hash).
            path_v_raw = "Lines|{}".format(sc_name)
            path_v, path_q = canonicalize_str(path_v_raw)

            identity_items = []
            required_qs = []

            identity_items.append(make_identity_item("line_style.path", path_v, path_q))

            # Optional: weights
            wproj_v, wproj_q = canonicalize_int(w_proj)
            if wproj_q != ITEM_Q_OK:
                status_v2 = STATUS_DEGRADED
                status_reasons.append("weight_projection_missing_or_unreadable")
            identity_items.append(make_identity_item("line_style.weight.projection", wproj_v, wproj_q))

            # Line styles (subcategories under Lines) do not have a Cut lineweight surface in Revit UI.
            # Treat as not applicable; do not degrade.
            # wcut_v, wcut_q = (None, ITEM_Q_UNSUPPORTED)
            # identity_items.append(make_identity_item("line_style.weight.cut", wcut_v, wcut_q))

            # Optional: color rgb
            rgb_v, rgb_q = canonicalize_str(None if rgb_sig in {S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE} else rgb_sig)
            if rgb_q != ITEM_Q_OK:
                status_v2 = STATUS_DEGRADED
                status_reasons.append("color_rgb_missing_or_unreadable")
            identity_items.append(make_identity_item("line_style.color.rgb", rgb_v, rgb_q))

            # Optional: pattern reference (sig_hash from line_patterns record.v2)
            lp_kind_v = None
            lp_sig_hash_v = None
            lp_sig_hash_q = ITEM_Q_MISSING

            lp_id = None
            try:
                lp_id = sc.GetLinePatternId(GraphicsStyleType.Projection)
            except Exception:
                status_v2 = STATUS_DEGRADED
                status_reasons.append("get_line_pattern_id_failed")
                lp_id = None

            # Determine projection line pattern.
            # SOLID has no LinePatternElement; treat non-resolvable ids as solid to avoid false "uid_missing".
            lp_elem = None
            lp_uid = None

            is_solid = False
            if lp_id is None:
                is_solid = True
            else:
                try:
                    if lp_id == ElementId.InvalidElementId:
                        is_solid = True
                except Exception:
                    # If comparison itself is unreliable, fall back to integer check
                    try:
                        is_solid = (int(getattr(lp_id, "IntegerValue", -1)) in (-1, 0))
                    except Exception:
                        is_solid = True

            if not is_solid:
                try:
                    lp_elem = doc.GetElement(lp_id)
                except Exception:
                    lp_elem = None

                if lp_elem is None:
                    # Treat as solid-ish: id present but cannot resolve element
                    is_solid = True

            if is_solid:
                lp_kind_v = "solid"
                # Deterministic, non-referential sentinel so SOLID can join cleanly.
                # This is NOT a UID/name/id and is stable across files.
                lp_sig_hash_v = "SOLID"
                lp_sig_hash_q = ITEM_Q_OK
            else:
                lp_kind_v = "ref"
                if lp_uid_to_sig_hash_v2 is None:
                    status_v2 = STATUS_DEGRADED
                    status_reasons.append("dependency_missing_line_patterns_v2_sig_hash")
                else:
                    try:
                        lp_uid = getattr(lp_elem, "UniqueId", None) if lp_elem else None
                    except Exception:
                        lp_uid = None

                    if lp_uid:
                        lp_sig_hash_v = lp_uid_to_sig_hash_v2.get(lp_uid, None)
                        if lp_sig_hash_v:
                            lp_sig_hash_q = ITEM_Q_OK
                        else:
                            status_v2 = STATUS_DEGRADED
                            status_reasons.append("dependency_unmapped_line_pattern_v2_sig_hash")
                    else:
                        # If we have an element but it has no UID, that's unusual; degrade explicitly.
                        status_v2 = STATUS_DEGRADED
                        status_reasons.append("line_pattern_uid_missing")

            # kind always present (stable surface)
            kind_v, kind_q = canonicalize_str(lp_kind_v)
            if kind_q != ITEM_Q_OK:
                status_v2 = STATUS_DEGRADED
                status_reasons.append("pattern_kind_missing_or_unreadable")
            identity_items.append(make_identity_item("line_style.pattern_ref.kind", kind_v, kind_q))

            # sig_hash item is always present so downstream tables have a stable column surface.
            # For SOLID or unresolved refs, v=None with q=ITEM_Q_MISSING.
            if lp_sig_hash_q == ITEM_Q_OK:
                lp_sig_hash_v, lp_sig_hash_q = canonicalize_str(lp_sig_hash_v)
            else:
                lp_sig_hash_v, lp_sig_hash_q = (None, ITEM_Q_MISSING)

            identity_items.append(make_identity_item("line_style.pattern_ref.sig_hash", lp_sig_hash_v, lp_sig_hash_q))

            # Enforce minima: required not-ok => blocked
            if any(q != ITEM_Q_OK for q in required_qs):
                status_v2 = STATUS_BLOCKED
                status_reasons.append("required_identity_not_ok")

            # Canonical evidence source for this domain is identity_basis.items.
            # Selectors (join_key.keys_used, phase2.semantic_keys, sig_basis.keys_used)
            # define subsets without duplicating k/q/v payloads.
            identity_items_sorted = sorted(identity_items, key=lambda d: str(d.get("k", "")))
            semantic_items = [
                it for it in identity_items_sorted
                if safe_str(it.get("k", "")) in set(LINE_STYLE_SEMANTIC_KEYS)
            ]
            preimage_v2 = serialize_identity_items(semantic_items)
            sig_hash_v2 = make_hash(preimage_v2)

            # ---------------------------
            # Discovery feature surface (Phase-2 computes stats / classification)
            # ---------------------------
            def _pick_identity_item(items, key):
                for it in (items or []):
                    if it.get("k") == key:
                        return it
                return None

            features_items = []
            for _k, _t in (
                ("line_style.parent_category_name", "s"),
                ("line_style.color_rgb", "s"),
                ("line_style.weight_projection", "i"),
                ("line_style.pattern_name", "s"),
            ):
                _it = _pick_identity_item(identity_items_sorted, _k)
                if _it is not None:
                    features_items.append(make_feature_item(_k, _t, _it.get("v"), _it.get("q")))
                    
            rec_v2 = build_record_v2(
                domain="line_styles",
                record_id=safe_str(sc_name),
                status=status_v2,
                status_reasons=sorted(set(status_reasons)),
                sig_hash=sig_hash_v2,
                identity_items=identity_items_sorted,
                required_qs=required_qs,
                label={
                    "display": safe_str(sc_name),
                    "quality": "human",
                    "provenance": "computed.path",
                    "components": {"path": safe_str(path_v_raw)},
                },
                features_items=features_items,
                debug={
                    "stratum_features": build_stratum_features_v1(
                        domain="line_styles",
                        identity_items=identity_items_sorted,
                    ),
                },
            )

            # -------------------------
            # Phase-2 additive surfaces (join_key + phase2)
            # -------------------------
            pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "line_styles")
            rec_v2["join_key"], _missing = build_join_key_from_policy(
                domain_policy=pol,
                identity_items=identity_items_sorted,
                include_optional_items=False,
                emit_keys_used=True,
                hash_optional_items=False,
                emit_items=False,
                emit_selectors=True,
            )

            # Hypothesis-only partitioning: semantic vs cosmetic vs unknown.
            # No heuristics; these are emitted for empirical clustering and attribute stability analysis.
            p2_cosmetic = []
            p2_unknown = []

            # color is cosmetic (visual only; excluded from join-key candidates)
            rgb_p2_v, rgb_p2_q = phase2_qv_from_legacy_sentinel_str(rgb_sig, allow_empty=False)
            p2_cosmetic.append(make_identity_item("line_style.color.rgb", rgb_p2_v, rgb_p2_q))

            # path is cosmetic (name-derived) but emitted for clustering/labeling
            p2_cosmetic.append(make_identity_item("line_style.path", path_v, path_q))

            # cut weight is not surfaced for line styles; keep explicit as unknown partition for analysis.
            # p2_unknown.append(make_identity_item("line_style.weight.cut", wcut_v, wcut_q))

            # Traceability fields (metadata only — never in hash/sig/join)
            try:
                _eid_raw = getattr(getattr(sc, "Id", None), "IntegerValue", None)
                _eid_v, _eid_q = canonicalize_int(_eid_raw)
            except Exception:
                _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
            try:
                _uid_raw = getattr(sc, "UniqueId", None)
                _uid_v, _uid_q = canonicalize_str(_uid_raw)
            except Exception:
                _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
            p2_unknown.append(make_identity_item("line_style.source_element_id", _eid_v, _eid_q))
            p2_unknown.append(make_identity_item("line_style.source_unique_id", _uid_v, _uid_q))

            rec_v2["phase2"] = {
                "schema": "phase2.line_styles.v1",
                "grouping_basis": "phase2.hypothesis",
                # Semantic selector references canonical identity_basis.items.
                # Deprecated direction: semantic_items should not duplicate canonical evidence.
                "cosmetic_items": phase2_sorted_items(p2_cosmetic),
                "coordination_items": phase2_sorted_items([]),
                "unknown_items": phase2_sorted_items(p2_unknown),
            }
            rec_v2["sig_basis"] = {
                "schema": "line_styles.sig_basis.v1",
                "keys_used": LINE_STYLE_SEMANTIC_KEYS,
            }

            v2_records.append(rec_v2)
            v2_sig_hashes.append(sig_hash_v2)
            if status_v2 == STATUS_BLOCKED:
                v2_any_blocked = True

        except Exception:
            info["debug_fail_record_build"] += 1
            continue

    info["names"] = sorted(set(names))
    info["count"] = len(v2_records)

    # Per-row legacy signature hashes (metadata; NOT used in global hash)
    # record.v2 finalize (domain hash is hash of sig_hashes; block if any record blocked)
    info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
    info["signature_hashes_v2"] = sorted(v2_sig_hashes)

    if v2_any_blocked:
        info["debug_v2_blocked"] = True
        v2_block_reasons["one_or_more_records_blocked"] = True

    if (not v2_any_blocked) and info["signature_hashes_v2"]:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])
    else:
        if (not v2_any_blocked) and (not info["signature_hashes_v2"]):
            info["debug_v2_blocked"] = True
            v2_block_reasons["no_v2_records"] = True
        info["hash_v2"] = None

    if v2_block_reasons:
        info["debug_v2_block_reasons"] = v2_block_reasons

    # record_rows for quick index/diff
    info["record_rows"] = [
        {"record_key": safe_str(r.get("record_id", "")), "sig_hash": r.get("sig_hash", None)}
        for r in info["records"]
    ]

    return info
