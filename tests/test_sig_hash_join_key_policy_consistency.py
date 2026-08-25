# -*- coding: utf-8 -*-
"""Cross-policy drift guard between domain_sig_hash_policies.json and
domain_join_key_policies.json (D-039 follow-up, decision-log item 2).

Root cause this guards against: tools/generate_sig_hash_policy.py falls back
to a domain's full contracts/domain_identity_keys_v2.json `allowed_keys` list
as the sig_hash preimage whenever that domain has no `sig_hash_keys`
override (see generate_sig_hash_policy.py:24-30). For a domain whose
extractor hand-classifies captured fields into semantic/coordination/cosmetic
buckets and hashes the semantic bucket only inline (wall_types, floor_types,
roof_types, ceiling_types before D-039), that silent fallback produces a
compiled sig_hash policy wider than the extractor's own inline hash --
concretely, it can reintroduce a type's own display name into sig_hash,
which is exactly the "names are metadata only" violation D-039 fixed.

This test does not know, a priori, which fields are "cosmetic" for a given
domain -- inferring that from source would require parsing each extractor's
Python (fragile, and the codebase deliberately avoids that kind of coupling
elsewhere, e.g. tools/pattern_id_utils.py's docstring on why it doesn't
import tools/extractor.py's private _stable_pattern_id()). Instead it uses a
signal that already exists and is already curated: domain_join_key_policies.json's
`explicitly_excluded_items`. A domain's join-key policy is where
"this field does not define behavioral identity for cross-file comparison"
already gets declared explicitly, per-field, by a human. If a field is
excluded from the join key AND allowed into the sig_hash preimage, that is
either (a) a reviewed, intentional divergence -- sig_hash and join_hash can
legitimately answer different questions for the same domain, and several
domains already do this on purpose (see ACCEPTED_SIG_HASH_JOIN_KEY_OVERLAPS
below) -- or (b) an unreviewed regen artifact, which is what happened to the
four compound_types domains. This test doesn't try to tell those apart by
inspection; it requires every instance of (a) to be explicitly listed and
explained here, and fails on anything not listed, so a *new* occurrence must
be a deliberate, reviewed decision (add it to the allowlist with a reason)
rather than a silent side effect of a mechanical policy regen.
"""
import os

from core.sig_hash_policy import load_sig_hash_policies
from core.join_key_policy import load_join_key_policies

SIG_HASH_POLICY_PATH = os.path.join("policies", "domain_sig_hash_policies.json")
JOIN_KEY_POLICY_PATH = os.path.join("policies", "domain_join_key_policies.json")

# Reviewed, intentional cases where a domain's sig_hash preimage includes a
# field its own join-key policy explicitly excludes. Each entry must cite
# the reason two policies for the same domain legitimately diverge here --
# not just "it's been like this," but why sig_hash and join_hash are
# answering different questions for that specific field.
ACCEPTED_SIG_HASH_JOIN_KEY_OVERLAPS = {
    # D-025: project_info.* fully participates in sig_hash (project-specific
    # fingerprint) by design, but is deliberately excluded from join_hash
    # (which stays scoped to is_workshared/revit_version_number/revit_build
    # so cross-project template/config matching isn't broken by ordinary
    # project-info edits like an address correction).
    "identity": {
        "project_info.address",
        "project_info.building_name",
        "project_info.business_center",
        "project_info.client_name",
        "project_info.ifc_building_guid",
        "project_info.ifc_project_guid",
        "project_info.ifc_site_guid",
        "project_info.issue_date",
        "project_info.name",
        "project_info.number",
        "project_info.organization_description",
        "project_info.organization_name",
        "project_info.status",
    },
    # D-017/D-038: sig_hash is the exact-scale line_pattern.segments_def_hash;
    # join_hash is the scale-invariant segments_norm_hash. These are two
    # deliberately different identity concepts for the same domain, not a
    # narrower/wider split of one concept.
    "line_patterns": {"line_pattern.segments_def_hash"},
    # D-047: domain_join_key_policies.json's own notes for line_styles: "pattern_ref.kind
    # is too coarse and produces high collisions; it must not be used for joins." That's
    # a join-clustering concern, not a "this isn't behavioral" judgment -- the extractor's
    # own inline sig_hash (LINE_STYLE_SEMANTIC_KEYS) has always included pattern_ref.kind
    # as part of a line style's full behavioral definition.
    "line_styles": {"line_style.pattern_ref.kind"},
    # domain_join_key_policies.json's own notes: join is family-granularity
    # and must match functionally-identical families across projects
    # regardless of current per-file counts/activation state; those counts
    # remain legitimate parts of the family's own sig_hash fingerprint.
    "loaded_family_types": {
        "lft.family_symbol_count",
        "lft.is_active",
        "lft.shape_gate.category_id",
        "lft.structural_material_type",
        "lft.type_count",
        "lft.type_parameter_count",
    },
    # view_category_overrides' join key matches to a baseline category/
    # object-style pattern (see notes: "Baseline: object_styles_*_row_key_to_sig_hash
    # ctx map"); the override VALUES themselves (line weight/halftone/
    # transparency) are excluded from join for that reason but remain part
    # of the record's own sig_hash, which fingerprints the full override state.
    "view_category_overrides_annotation": {
        "vco.halftone",
        "vco.projection.line_weight",
        "vco.transparency",
    },
    "view_category_overrides_model": {
        "vco.cut.line_weight",
        "vco.halftone",
        "vco.projection.line_weight",
        "vco.transparency",
    },
    # domain_join_key_policies.json's own notes: vf.def_hash (the join key)
    # is itself computed FROM vf.categories + vf.logic_root + vf.rule_count +
    # ordered rule sigs -- these leaves are rolled up into a single join
    # value, not excluded as cosmetic, and stay in sig_hash as forensic detail.
    "view_filter_definitions": {
        "vf.categories",
        "vf.logic_root",
        "vf.rule_count",
    },
}


def _sig_hash_allowed_items(domain_policy):
    return set(domain_policy.get("allowed_items") or [])


def test_no_undocumented_sig_hash_join_key_overlap():
    sig_policies = load_sig_hash_policies(SIG_HASH_POLICY_PATH)["domains"]
    join_policies = load_join_key_policies(JOIN_KEY_POLICY_PATH)["domains"]

    undocumented = {}
    for domain, spol in sig_policies.items():
        jpol = join_policies.get(domain)
        if not isinstance(jpol, dict):
            continue
        excluded = set(jpol.get("explicitly_excluded_items") or [])
        overlap = excluded & _sig_hash_allowed_items(spol)
        accepted = ACCEPTED_SIG_HASH_JOIN_KEY_OVERLAPS.get(domain, set())
        extra = overlap - accepted
        if extra:
            undocumented[domain] = sorted(extra)

    assert not undocumented, (
        "sig_hash allowed_items includes field(s) the domain's own join-key policy "
        "explicitly_excluded_items already flags as non-behavioral, with no reviewed "
        "entry in ACCEPTED_SIG_HASH_JOIN_KEY_OVERLAPS explaining why: {}. "
        "This is the exact drift class D-039 fixed for wall_types/floor_types/"
        "roof_types/ceiling_types (a mechanical policy regen silently widening "
        "sig_hash to include a name/cosmetic field). Either close the gap the way "
        "D-039 did (add a sig_hash_keys override in "
        "contracts/domain_identity_keys_v2.json and narrow the compiled policy to "
        "match the extractor's own inline hash), or -- if this divergence is "
        "genuinely intentional -- add it to ACCEPTED_SIG_HASH_JOIN_KEY_OVERLAPS "
        "here with a documented reason.".format(undocumented)
    )


def test_accepted_overlaps_allowlist_has_no_stale_entries():
    """The inverse check: every allowlisted field must still actually overlap.

    Guards the allowlist itself from rotting silently -- e.g. if a field is
    later removed from a join-key policy's explicitly_excluded_items (so it's
    no longer excluded there) or from a sig_hash policy's allowed_items (so
    it's no longer hashed), the corresponding allowlist entry becomes a
    stale, meaningless placeholder rather than a real reviewed exception.
    """
    sig_policies = load_sig_hash_policies(SIG_HASH_POLICY_PATH)["domains"]
    join_policies = load_join_key_policies(JOIN_KEY_POLICY_PATH)["domains"]

    stale = {}
    for domain, accepted_fields in ACCEPTED_SIG_HASH_JOIN_KEY_OVERLAPS.items():
        spol = sig_policies.get(domain) or {}
        jpol = join_policies.get(domain) or {}
        excluded = set(jpol.get("explicitly_excluded_items") or [])
        allowed = _sig_hash_allowed_items(spol)
        actual_overlap = excluded & allowed
        missing = accepted_fields - actual_overlap
        if missing:
            stale[domain] = sorted(missing)

    assert not stale, (
        "ACCEPTED_SIG_HASH_JOIN_KEY_OVERLAPS lists field(s) that no longer actually "
        "overlap between the two policies -- remove the stale entries: {}".format(stale)
    )
