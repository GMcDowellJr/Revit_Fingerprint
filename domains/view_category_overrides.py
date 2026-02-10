# -*- coding: utf-8 -*-
"""
View Category Overrides domain extractor.

Captures category override deltas relative to object_styles baseline.
This domain is designed for reuse across view templates and future view-level overrides.
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import canon_str
from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
    canonicalize_str,
    canonicalize_int,
)
from core.phase2 import phase2_sorted_items
from core.deps import require_domain, Blocked
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy
from core.graphic_overrides import (
    extract_projection_graphics,
    extract_cut_graphics,
    extract_halftone,
    extract_transparency,
)

try:
    from Autodesk.Revit.DB import View, OverrideGraphicSettings
except ImportError:
    View = None
    OverrideGraphicSettings = None

from core.collect import collect_instances


def _compute_override_properties_hash(identity_items):
    """
    Compute canonical hash of ALL override properties as a behavioral unit.

    Two records with the same set of overrides but different templates/categories
    will have the same override_properties_hash, enabling pattern detection.

    Args:
        identity_items: List of IdentityItem dicts (full identity_basis items)

    Returns:
        str: MD5 hash of canonical override signature
    """
    import hashlib

    # Extract only delta properties (exclude baseline references)
    override_items = [
        item for item in identity_items
        if not item.get('k', '').startswith('vco.baseline')
           and item.get('k', '').startswith('vco.')
    ]

    # Sort by key for canonical ordering
    sorted_items = sorted(override_items, key=lambda x: x.get('k', ''))

    # Build canonical signature (same format as other domains)
    signature_parts = []
    for item in sorted_items:
        k = item.get('k', '')
        q = item.get('q', '')
        v = item.get('v')
        # Handle None values explicitly
        v_str = '' if v is None else str(v)
        signature_parts.append("{}={}:{}".format(k, q, v_str))

    signature = "|".join(signature_parts)

    # Compute MD5 hash matching hash_alg policy
    return hashlib.md5(signature.encode('utf-8')).hexdigest()


def _phase2_partition_items(items):
    """Partition IdentityItems into semantic/cosmetic/unknown buckets.

    Semantic: Baseline references + override hash (defines the pattern)
    Cosmetic: Individual delta properties (forensic detail)
    Unknown: Template/category context
    """
    semantic = []
    cosmetic = []
    unknown = []

    for it in (items or []):
        k = safe_str(it.get("k", ""))

        # SEMANTIC: Baseline refs and derived override hash
        # These define WHAT the override pattern is
        if k in ["vco.baseline_category_path",
                 "vco.baseline_sig_hash",
                 "vco.override_properties_hash"]:
            semantic.append(it)
            continue

        # COSMETIC: Individual delta properties
        # These are forensic detail ABOUT the pattern
        if (
            k.startswith("vco.projection.")
            or k.startswith("vco.cut.")
            or k.startswith("vco.halftone")
            or k.startswith("vco.transparency")
        ):
            cosmetic.append(it)
            continue

        # UNKNOWN: Everything else (should be minimal)
        unknown.append(it)

    return (
        phase2_sorted_items(semantic),
        phase2_sorted_items(cosmetic),
        phase2_sorted_items(unknown),
    )


def _compute_delta_items(override_items, baseline_record, key_prefix):
    """
    Compare override to baseline, return only changed properties.

    Args:
        override_items: List[IdentityItem] from extract_*_graphics()
        baseline_record: Full record dict from object_styles
        key_prefix: "vco" or "vco.projection" etc.

    Returns:
        List[IdentityItem]: Only items that differ from baseline
    """
    delta_items = []

    baseline_items = (baseline_record or {}).get("identity_basis", {}).get("items", []) or []
    baseline_map = {item.get("k"): item.get("v") for item in baseline_items}

    prefix = "{}.".format(safe_str(key_prefix).strip(".")) if key_prefix else ""

    for override_item in (override_items or []):
        override_key = safe_str(override_item.get("k", ""))
        if not override_key:
            continue

        if prefix and override_key.startswith(prefix):
            baseline_key = "obj_style.{}".format(override_key[len(prefix):])
        elif override_key.startswith("vco."):
            baseline_key = "obj_style.{}".format(override_key[len("vco."):])
        else:
            baseline_key = override_key

        baseline_value = baseline_map.get(baseline_key)
        override_value = override_item.get("v")

        if override_value != baseline_value:
            delta_items.append(override_item)

    return delta_items


def extract(doc, ctx=None):
    """
    Extract view category override deltas relative to object_styles baseline.

    Args:
        doc: Revit document
        ctx: context dict with mappings from other domains

    Returns:
        Dictionary with records, hashes, and debug counters
    """
    info = {
        "count": 0,
        "records": [],
        "hash_v2": None,
        "signature_hashes_v2": [],

        # Debug counters
        "debug_templates_processed": 0,
        "debug_categories_checked": 0,
        "debug_overrides_found": 0,
        "debug_no_baseline": 0,
        "debug_no_change": 0,
        "debug_v2_blocked": False,
    }

    if View is None or OverrideGraphicSettings is None:
        info["debug_v2_blocked"] = True
        return info

    try:
        require_domain((ctx or {}).get("_domains", {}), "object_styles")
        require_domain((ctx or {}).get("_domains", {}), "line_patterns")
        require_domain((ctx or {}).get("_domains", {}), "fill_patterns")
    except Blocked:
        info["debug_v2_blocked"] = True
        return info

    baseline_sig_map = ctx.get("object_styles_category_to_sig_hash", {}) if ctx else {}
    baseline_records = ctx.get("object_styles_records", {}) if ctx else {}

    if not baseline_sig_map:
        info["debug_v2_blocked"] = True
        return info

    templates = []
    try:
        templates = list(
            collect_instances(
                doc,
                of_class=View,
                where_key="view_category_overrides.views",
            )
        )
    except Exception:
        templates = list(getattr(doc, "AllViews", []) or [])

    templates = [v for v in templates if getattr(v, "IsTemplate", False)]
    info["debug_templates_processed"] = len(templates)

    override_patterns = {}
    blocked_records = []
    signature_hashes_v2 = []
    v2_any_blocked = False

    categories = []
    try:
        categories = list(getattr(doc.Settings, "Categories", []) or [])
    except Exception:
        categories = []

    for template in templates:
        template_name = canon_str(getattr(template, "Name", None))

        for category in categories:
            category_name = canon_str(getattr(category, "Name", None))
            category_path = "{}|self".format(category_name)

            info["debug_categories_checked"] += 1

            baseline_sig_hash = baseline_sig_map.get(category_path)
            if not baseline_sig_hash:
                info["debug_no_baseline"] += 1
                continue

            baseline_record = baseline_records.get(baseline_sig_hash)
            if not baseline_record:
                info["debug_no_baseline"] += 1
                continue

            try:
                ogs = template.GetCategoryOverrides(category.Id)
            except Exception:
                continue

            if not ogs:
                continue

            override_items = []

            proj_items = extract_projection_graphics(doc, ogs, ctx, "vco.projection")
            cut_items = extract_cut_graphics(doc, ogs, ctx, "vco.cut")
            halftone_items = extract_halftone(ogs, "vco.halftone")
            trans_items = extract_transparency(ogs, "vco.transparency")

            override_items.extend(proj_items)
            override_items.extend(cut_items)
            override_items.extend(halftone_items)
            override_items.extend(trans_items)

            delta_items = _compute_delta_items(override_items, baseline_record, "vco")

            if not delta_items:
                info["debug_no_change"] += 1
                continue

            info["debug_overrides_found"] += 1

            base_path_v, base_path_q = canonicalize_str(category_path)
            base_sig_v, base_sig_q = canonicalize_str(baseline_sig_hash)

            identity_items = [
                make_identity_item("vco.baseline_category_path", base_path_v, base_path_q),
                make_identity_item("vco.baseline_sig_hash", base_sig_v, base_sig_q),
            ]
            identity_items.extend(delta_items)

            # Compute override_properties_hash before sorting
            override_props_hash = _compute_override_properties_hash(identity_items)
            identity_items.append(
                make_identity_item(
                    "vco.override_properties_hash",
                    override_props_hash,
                    ITEM_Q_OK
                )
            )

            # Canonical evidence source for this domain is identity_basis.items.
            # Selectors (join_key.keys_used, phase2.semantic_keys, sig_basis.keys_used)
            # reference subsets without duplicating k/q/v payloads.
            identity_items_sorted = sorted(identity_items, key=lambda it: str(it.get("k", "")))
            semantic_keys = sorted({safe_str(it.get("k", "")) for it in identity_items_sorted if safe_str(it.get("k", ""))})
            semantic_items = [it for it in identity_items_sorted if safe_str(it.get("k", "")) in set(semantic_keys)]
            preimage = serialize_identity_items(semantic_items)
            sig_hash = make_hash(preimage) if preimage is not None else None

            required_keys = ["vco.baseline_category_path", "vco.baseline_sig_hash", "vco.override_properties_hash"]
            item_by_k = {it.get("k"): it for it in identity_items_sorted}
            required_qs = [safe_str(item_by_k.get(rk, {}).get("q", ITEM_Q_MISSING)) for rk in required_keys]

            status_reasons = []
            any_incomplete = False
            for it in identity_items_sorted:
                q = it.get("q")
                if q != ITEM_Q_OK:
                    any_incomplete = True
                    status_reasons.append("identity.incomplete:{}:{}".format(q, it.get("k")))

            blocked = any(q != ITEM_Q_OK for q in required_qs)

            record_id = "vco_{}_{}".format(safe_str(category_name), safe_str(sig_hash or "blocked")[:8])
            label = {
                "display": "{}: {} changes".format(category_path, len(delta_items)),
                "quality": "human",
                "provenance": "computed.delta",
                "components": {
                    "template": safe_str(template_name),
                    "category_path": safe_str(category_path),
                },
            }

            if blocked or sig_hash is None:
                v2_any_blocked = True
                rec = build_record_v2(
                    domain="view_category_overrides",
                    record_id=record_id,
                    status=STATUS_BLOCKED,
                    status_reasons=sorted(set(status_reasons)) or ["minima.required_not_ok"],
                    sig_hash=None,
                    identity_items=identity_items_sorted,
                    required_qs=(),
                    label=label,
                )
                blocked_records.append(rec)
            else:
                status = STATUS_DEGRADED if any_incomplete else STATUS_OK
                rec = build_record_v2(
                    domain="view_category_overrides",
                    record_id=record_id,
                    status=status,
                    status_reasons=sorted(set(status_reasons)),
                    sig_hash=sig_hash,
                    identity_items=identity_items_sorted,
                    required_qs=required_qs,
                    label=label,
                )

                if sig_hash in override_patterns:
                    continue

                override_patterns[sig_hash] = rec
                signature_hashes_v2.append(sig_hash)

            p2_semantic, p2_cosmetic, p2_unknown = _phase2_partition_items(identity_items_sorted)

            # Traceability fields (metadata only — never in hash/sig/join)
            try:
                _tpl_eid_raw = getattr(getattr(template, "Id", None), "IntegerValue", None)
                _tpl_eid_v, _tpl_eid_q = canonicalize_int(_tpl_eid_raw)
            except Exception:
                _tpl_eid_v, _tpl_eid_q = (None, ITEM_Q_UNREADABLE)
            try:
                _tpl_uid_raw = getattr(template, "UniqueId", None)
                _tpl_uid_v, _tpl_uid_q = canonicalize_str(_tpl_uid_raw)
            except Exception:
                _tpl_uid_v, _tpl_uid_q = (None, ITEM_Q_UNREADABLE)
            try:
                _cat_eid_raw = getattr(getattr(category, "Id", None), "IntegerValue", None)
                _cat_eid_v, _cat_eid_q = canonicalize_int(_cat_eid_raw)
            except Exception:
                _cat_eid_v, _cat_eid_q = (None, ITEM_Q_UNREADABLE)
            p2_unknown.append({"k": "vco.category_source_element_id", "q": _cat_eid_q, "v": _cat_eid_v})
            p2_unknown.append({"k": "vco.template_source_element_id", "q": _tpl_eid_q, "v": _tpl_eid_v})
            p2_unknown.append({"k": "vco.template_source_unique_id", "q": _tpl_uid_q, "v": _tpl_uid_v})

            rec["phase2"] = {
                "schema": "phase2.view_category_overrides.v1",
                "grouping_basis": "join_key.join_hash",
                # Semantic selector references identity_basis.items.
                # Deprecated direction: semantic_items should not duplicate canonical evidence.
                                "cosmetic_items": phase2_sorted_items(p2_cosmetic),
                "coordination_items": phase2_sorted_items([]),
                "unknown_items": phase2_sorted_items(p2_unknown),
            }

            pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "view_category_overrides")
            rec["join_key"], _missing = build_join_key_from_policy(
                domain_policy=pol,
                identity_items=identity_items_sorted,
                include_optional_items=False,
                emit_keys_used=True,
                hash_optional_items=False,
                emit_items=False,
                emit_selectors=True,
            )

            rec["sig_basis"] = {
                "schema": "view_category_overrides.sig_basis.v1",
                "keys_used": semantic_keys,
            }

    records = list(override_patterns.values()) + blocked_records
    records = sorted(records, key=lambda r: safe_str(r.get("record_id", "")))

    info["records"] = records
    info["count"] = len(records)
    info["signature_hashes_v2"] = sorted(signature_hashes_v2)

    if v2_any_blocked:
        info["debug_v2_blocked"] = True

    if signature_hashes_v2 and not v2_any_blocked:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])
    else:
        info["hash_v2"] = None

    if ctx is not None:
        ctx["view_category_overrides_sig_hash"] = dict(override_patterns)

    return info
