# -*- coding: utf-8 -*-
"""Policy-driven, analysis-side reconstruction of the Canonical Name Identity Projection.

Mirrors core/sig_hash_builder.py's role for sig_hash: a second, independent code path that
recomputes the same value (here, join_key_name_identity's join_hash) from already-exported
record.v2 JSON, without requiring re-extraction through domains/*.py + runner/run_dynamo.py.

This works because every value the projection needs -- identity_basis.items, phase2 bucket
items (cosmetic/coordination/unknown/semantic/lineage), and label.display -- is already
present in existing *.details.json exports today, for every eligible domain. See
audit_results/audit_6_name_key_step0_within_pr1.md for the per-domain trace of where each
domain's own name value actually lives.

Reuses, rather than reimplements: core.canonical_items.build_flat_items() for the bucket
merge (same logic as core.canonical_items.canonicalize_record(), just non-destructive and
without the identity_basis/phase2/join_key/sig_hash stripping that function also does), and
core.join_key_builder.build_join_key_from_policy() for the actual join_hash computation --
the identical mechanism the inline extractor call sites use.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.canonical_items import build_flat_items
from core.record_v2 import canonicalize_str
from core.join_key_builder import build_join_key_from_policy, compute_projection_status
from core.join_key_policy import get_domain_join_key_policy


# Domains whose name-key required item is not present in identity_basis.items or any
# phase2 bucket in existing exports -- the raw name value lives only in label.display.
# (Every other domain in policies/domain_name_key_policies.json needs no entry here: its
# name value already surfaces via flat_items_for_record()'s identity_basis/phase2 merge.)
LABEL_ONLY_NAME_KEYS: Dict[str, str] = {
    "arrowheads": "arrowhead.name",
    "loaded_family_types": "lft.family_name",
    "view_filter_definitions": "vf.name",
    "view_templates_ceiling_plans": "view_template.name",
    "view_templates_elevations_sections_detail": "view_template.name",
    "view_templates_floor_structural_area_plans": "view_template.name",
    "view_templates_renderings_drafting": "view_template.name",
    "view_templates_schedules": "view_template.name",
    "dimension_types_linear": "dim_type.name",
    "dimension_types_angular": "dim_type.name",
    "dimension_types_radial": "dim_type.name",
    "dimension_types_diameter": "dim_type.name",
    "dimension_types_spot_slope": "dim_type.name",
}


def flat_items_for_record(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Merge identity_basis.items + phase2 buckets into one flat items list, non-destructively.

    Mirrors core.canonical_items.canonicalize_record()'s item-merging logic exactly, but does
    not mutate or strip the input record -- this is a read-only projection helper, not a
    migration step.
    """
    if not isinstance(record, dict):
        return []
    existing_items = record.get("items") if isinstance(record.get("items"), list) else []
    ib = record.get("identity_basis") if isinstance(record.get("identity_basis"), dict) else {}
    identity_items = ib.get("items") if isinstance(ib.get("items"), list) else []
    phase2 = record.get("phase2") if isinstance(record.get("phase2"), dict) else {}
    return build_flat_items(
        existing_items,
        identity_items,
        phase2.get("semantic_items", []) if isinstance(phase2.get("semantic_items"), list) else [],
        phase2.get("lineage_items", []) if isinstance(phase2.get("lineage_items"), list) else [],
        phase2.get("cosmetic_items", []) if isinstance(phase2.get("cosmetic_items"), list) else [],
        phase2.get("coordination_items", []) if isinstance(phase2.get("coordination_items"), list) else [],
        phase2.get("unknown_items", []) if isinstance(phase2.get("unknown_items"), list) else [],
    )


def build_name_key_for_record(
    record: Dict[str, Any],
    domain_name: str,
    name_key_policies: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Reconstruct the join_key_name_identity dict for one already-exported record.

    Returns None if domain_name has no entry in name_key_policies (out of scope for this
    projection entirely -- policies/domain_name_key_policies.json is the eligibility
    allow-list; a missing entry means "no policy," not "blocked").
    """
    pol = get_domain_join_key_policy(name_key_policies, domain_name)
    if pol is None:
        return None

    items = flat_items_for_record(record)

    label_key = LABEL_ONLY_NAME_KEYS.get(domain_name)
    if label_key:
        label = record.get("label") if isinstance(record.get("label"), dict) else {}
        raw = label.get("display") if isinstance(label, dict) else None
        v, q = canonicalize_str(raw)
        items = items + [{"k": label_key, "v": v, "q": q}]

    join_key, missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )
    join_key["status"] = compute_projection_status(pol, missing)
    return join_key
