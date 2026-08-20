# -*- coding: utf-8 -*-
"""Policy-driven, analysis-side reconstruction of the Canonical Name Identity Projection.

Mirrors core/sig_hash_builder.py's role for sig_hash: a second, independent code path that
recomputes the same value (here, join_key_name_identity's join_hash) from already-exported
record.v2 JSON, without requiring re-extraction through domains/*.py + runner/run_dynamo.py.

This works because every value the projection needs -- identity_basis.items, phase2 bucket
items (cosmetic/coordination/unknown/semantic/lineage), and label.display -- is already
present in existing *.details.json exports today, for every eligible domain. See
DECISIONS.md D-037 for the per-domain trace of where each
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
# phase2 bucket in existing exports -- the raw name value lives only under label.*.
# (Every other domain in policies/domain_name_key_policies.json needs no entry here: its
# name value already surfaces via flat_items_for_record()'s identity_basis/phase2 merge.)
#
# "component" names the label.components.<key> path holding the UNDECORATED raw name the
# inline extractor actually hashes. label.display is NOT always that raw value -- e.g.
# loaded_family_types decorates it as "category : family_name", and
# view_filter_definitions decorates it as "View Filter Definition (name)". Domains with no
# "component" entry have no separate raw component in the export at all; for those,
# label.display genuinely IS the same raw, undecorated value the inline extractor hashes
# (view_templates, dimension_types), so falling back to display is correct there.
LABEL_ONLY_NAME_KEYS: Dict[str, Dict[str, str]] = {
    "arrowheads": {"item_key": "arrowhead.name", "component": "type_name"},
    "loaded_family_types": {"item_key": "lft.family_name", "component": "family_name"},
    "view_filter_definitions": {"item_key": "vf.name", "component": "name"},
    "view_templates_ceiling_plans": {"item_key": "view_template.name"},
    "view_templates_elevations_sections_detail": {"item_key": "view_template.name"},
    "view_templates_floor_structural_area_plans": {"item_key": "view_template.name"},
    "view_templates_renderings_drafting": {"item_key": "view_template.name"},
    "view_templates_schedules": {"item_key": "view_template.name"},
    "dimension_types_linear": {"item_key": "dim_type.name"},
    "dimension_types_angular": {"item_key": "dim_type.name"},
    "dimension_types_radial": {"item_key": "dim_type.name"},
    "dimension_types_diameter": {"item_key": "dim_type.name"},
    "dimension_types_spot_slope": {"item_key": "dim_type.name"},
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


def _has_detail_data(record: Dict[str, Any]) -> bool:
    """True if record carries record-level detail (identity_basis, phase2, or canonical
    flat items), as opposed to summary-only/index-level data.

    Per input-format-priority rule, summary-only exports are degraded and
    "records without [identity_basis/phase2] are skipped, not silently treated as
    complete." A label-only domain's name value lives in label.*, which a summary-only
    record could still carry -- without this gate, such a record would synthesize a
    superficially "ok" join_hash that isn't actually a details-based reconstruction.
    """
    return (
        isinstance(record.get("identity_basis"), dict)
        or isinstance(record.get("phase2"), dict)
        or isinstance(record.get("items"), list)
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

    label_spec = LABEL_ONLY_NAME_KEYS.get(domain_name)
    if label_spec and _has_detail_data(record):
        label = record.get("label") if isinstance(record.get("label"), dict) else {}
        component = label_spec.get("component")
        if component:
            components = label.get("components") if isinstance(label.get("components"), dict) else {}
            raw = components.get(component)
        else:
            raw = label.get("display")
        v, q = canonicalize_str(raw)
        items = items + [{"k": label_spec["item_key"], "v": v, "q": q}]

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
