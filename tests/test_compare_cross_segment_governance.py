"""Tests for governance semantics in tools/compare_cross_segment.py."""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from enterprise_policy import load_enterprise_policy  # noqa: E402
POLICY = load_enterprise_policy()

from compare_cross_segment import (  # noqa: E402
    _bc_of,
    _classify_governance_state,
    _comparison_role_semantics,
    _normalize_bc_label,
    _recommended_primary_view,
    _scope_level,
    _usage_interpretable_for_role,
    REUSE_BUCKET_THRESHOLDS,
    _reuse_bucket_for,
    build_pair_domain_work_items,
    build_pattern_reuse_distribution_rows,
    build_union_inventory_rows,
    deduplicate_pairs,
    discover_client_cross_bc,
    discover_domains_for_segment,
    discover_governance_chain,
    discover_sibling_segments,
    discover_within_project,
    drop_legacy_siblings_covered_by_peer_comparisons,
    load_file_join_hashes,
    main as compare_main,
    make_comparison_run_id,
    run_pooled_comparison,
    sort_pair_detail_rows,
    sort_summary_rows,
)


def _seg(role: str, client: str = "Acme", unit: str = "imperial", discipline: str = "Arch"):
    return {
        "governance_role": role,
        "client_label": client,
        "unit_system": unit,
        "discipline_label": discipline,
        "run_type": "bundle",
    }


def test_discover_governance_chain_includes_generic_upstream_roles():
    manifest = {
        "g": _seg("Generic", client="Global"),
        "gh": _seg("Generic-Host", client="Global"),
        "t": _seg("Template"),
        "c": _seg("Container"),
        "p": _seg("Project"),
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    assert ("g", "t", "generic_to_template") in pairs
    assert ("g", "c", "generic_to_container") in pairs
    assert ("g", "p", "generic_to_project") in pairs
    assert ("gh", "t", "generic_to_template") in pairs
    assert ("t", "p", "template_to_project") in pairs
    assert ("t", "c", "template_to_container") in pairs
    assert ("c", "p", "container_to_project") in pairs


def test_discover_governance_chain_falls_back_to_collection_label_for_na_client():
    manifest = {
        "bc_t": {**_seg("Template", client="__NOT_APPLICABLE__"), "collection_label": "BC_2270 Standards"},
        "bc_c": {**_seg("Container", client="n/a"), "collection_label": "BC_2270 Standards"},
        "acme_t": _seg("Template", client="Acme"),
        "acme_p": _seg("Project", client="Acme"),
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    # BC_2270's Template/Container (client blank/NA, various spellings) group
    # with each other via collection_label instead of pooling under "".
    assert ("bc_t", "bc_c", "template_to_container") in pairs
    # A real client is entirely unaffected by the fallback.
    assert ("acme_t", "acme_p", "template_to_project") in pairs
    # BC content must not cross-pollinate with an unrelated real client pool.
    assert ("bc_t", "acme_p", "template_to_project") not in pairs


def test_discover_governance_chain_prefers_business_center_label_over_collection_label():
    manifest = {
        "bc_t": {
            **_seg("Template", client="__NOT_APPLICABLE__"),
            "business_center_label": "BC_2270",
            "collection_label": "BC_2270 Standards",
        },
        "bc_c": {
            **_seg("Container", client="n/a"),
            "business_center_label": "BC_2270",
            "collection_label": "BC_2270 Standards",
        },
        "other_bc_t": {
            **_seg("Template", client=""),
            "business_center_label": "BC_9999",
            "collection_label": "BC_2270 Standards",
        },
        "acme_t": _seg("Template", client="Acme"),
        "acme_p": _seg("Project", client="Acme"),
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    # BC_2270's Template/Container (client blank/NA) now group via
    # business_center_label, not collection_label.
    assert ("bc_t", "bc_c", "template_to_container") in pairs
    # A different business_center_label sharing the same collection_label
    # must NOT be pooled together now that business_center_label wins.
    assert ("other_bc_t", "bc_c", "template_to_container") not in pairs
    # A real client is entirely unaffected by the fallback.
    assert ("acme_t", "acme_p", "template_to_project") in pairs


def test_discover_governance_chain_namespaces_business_center_fallback_from_real_client():
    # A real client whose name happens to match a business_center_label text
    # (e.g. both literally "BC_2270") must not be pooled with the
    # business-center-scoped rows that fall back to that same text via
    # business_center_label. client_label and business_center_label are
    # distinct cut dimensions with independent namespaces.
    manifest = {
        "bc_t": {
            **_seg("Template", client="__NOT_APPLICABLE__"),
            "business_center_label": "BC_2270",
        },
        "bc_c": {
            **_seg("Container", client="n/a"),
            "business_center_label": "BC_2270",
        },
        "real_client_t": _seg("Template", client="BC_2270"),
        "real_client_p": _seg("Project", client="BC_2270"),
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    # The business-center rows still group with each other.
    assert ("bc_t", "bc_c", "template_to_container") in pairs
    # The real "BC_2270" client rows still group with each other.
    assert ("real_client_t", "real_client_p", "template_to_project") in pairs
    # But the two namespaces must never cross-pollinate despite sharing text.
    assert ("bc_t", "real_client_p", "template_to_project") not in pairs
    assert ("real_client_t", "bc_c", "template_to_container") not in pairs


def test_discover_governance_chain_preserves_collection_scope_within_business_center():
    # A single business center can house more than one named collection (its
    # own general standards plus a separately-named legacy collection, per
    # build_segment_manifest.py's collection_label cut dimension). Two rows
    # sharing business_center_label but differing in collection_label must
    # not be pooled together, or Page's standards and the firm-wide
    # "InternalEnterprise Standards" collection (both business_center=BC_0000 in
    # practice) would get spurious template_to_project/container_to_project
    # pairs against each other.
    manifest = {
        "page_t": {
            **_seg("Template", client="__NOT_APPLICABLE__"),
            "business_center_label": "BC_0000",
            "collection_label": "Page Standards",
        },
        "page_c": {
            **_seg("Container", client="n/a"),
            "business_center_label": "BC_0000",
            "collection_label": "Page Standards",
        },
        "internalenterprise_t": {
            **_seg("Template", client="__NOT_APPLICABLE__"),
            "business_center_label": "BC_0000",
            "collection_label": "InternalEnterprise Standards",
        },
        "internalenterprise_c": {
            **_seg("Container", client="n/a"),
            "business_center_label": "BC_0000",
            "collection_label": "InternalEnterprise Standards",
        },
        # No collection_label at all — still groups purely on business_center,
        # unaffected by the new collection-scoping.
        "bc_only_t": {
            **_seg("Template", client="__NOT_APPLICABLE__"),
            "business_center_label": "BC_2270",
        },
        "bc_only_c": {
            **_seg("Container", client="n/a"),
            "business_center_label": "BC_2270",
        },
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    # Same business_center AND same collection: still group.
    assert ("page_t", "page_c", "template_to_container") in pairs
    assert ("internalenterprise_t", "internalenterprise_c", "template_to_container") in pairs
    # Same business_center, DIFFERENT collection: must not cross-pollinate.
    assert ("page_t", "internalenterprise_c", "template_to_container") not in pairs
    assert ("internalenterprise_t", "page_c", "template_to_container") not in pairs
    # business_center-only rows (no collection_label) are unaffected.
    assert ("bc_only_t", "bc_only_c", "template_to_container") in pairs


def test_discover_governance_chain_final_fallback_normalizes_na_spelling():
    # When client_label, business_center_label, and collection_label are all
    # blank/NA, the final fallback must return a canonical blank key, not the
    # raw NA token. Two rows spelled differently ("__NOT_APPLICABLE__" vs
    # "n/a") but otherwise identically blank must still group together —
    # every NA spelling is documented as equivalent to blank for grouping.
    manifest = {
        "na_t": _seg("Template", client="__NOT_APPLICABLE__"),
        "na_c": _seg("Container", client="n/a"),
        "na_p": _seg("Project", client="NA"),
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    assert ("na_t", "na_c", "template_to_container") in pairs
    assert ("na_t", "na_p", "template_to_project") in pairs
    assert ("na_c", "na_p", "container_to_project") in pairs


def test_discover_governance_chain_collection_match_is_soft_for_client_scope():
    # Mirrors real data: a client's own Container/Template rows are tagged
    # with that client's collection_label (e.g. "ClientBeta Standards"), but its
    # Project rows typically carry no collection_label at all. Hard-
    # partitioning by collection_label would put those in different _key()
    # buckets and silently stop producing template_to_project/
    # container_to_project pairs — the tool's primary comparison. A soft
    # match (required only when both sides are populated) must still pair
    # them, while two DIFFERENT populated collections under the same client
    # must not cross-pollinate.
    manifest = {
        "clientbeta_t": {**_seg("Template", client="ClientBeta"), "collection_label": "ClientBeta Standards"},
        "clientbeta_c": {**_seg("Container", client="ClientBeta"), "collection_label": "ClientBeta Standards"},
        # Project rows: no collection_label at all, matching real data.
        "clientbeta_p": _seg("Project", client="ClientBeta"),
        # A second, differently-named collection under the SAME client must
        # not silently pair with "ClientBeta Standards" — two populated,
        # different values are a genuine mismatch.
        "clientbeta_legacy_t": {**_seg("Template", client="ClientBeta"), "collection_label": "ClientBeta Legacy"},
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    # Collection-tagged standards still pair with collection-blank usage.
    assert ("clientbeta_t", "clientbeta_p", "template_to_project") in pairs
    assert ("clientbeta_c", "clientbeta_p", "container_to_project") in pairs
    assert ("clientbeta_t", "clientbeta_c", "template_to_container") in pairs
    # Two different, both-populated collections under the same client don't
    # cross-pollinate.
    assert ("clientbeta_legacy_t", "clientbeta_c", "template_to_container") not in pairs
    # But the differently-collectioned template still reaches the
    # collection-blank project, since blank is permissive on one side.
    assert ("clientbeta_legacy_t", "clientbeta_p", "template_to_project") in pairs


def test_discover_governance_chain_rollup_does_not_wildcard_match_specific_collection():
    # Mirrors real data: business_center_label="BC_0000" hosts two distinct
    # collections (Page Standards, InternalEnterprise Standards). build_segment_manifest.py
    # keeps a runnable, collection-blank aggregate Template alongside its
    # collection-specific children whenever the aggregate's population isn't
    # identical to either child's (i.e. the BC hosts more than one
    # collection). That aggregate's blank collection_label must NOT act as a
    # wildcard against a specific-collection Container — doing so would mix
    # the pooled (both-collections) population into a comparison meant to
    # isolate one collection's own population.
    manifest = {
        "bc_t_rollup": {
            **_seg("Template", client="__NOT_APPLICABLE__"),
            "business_center_label": "BC_0000",
            "segment_id": "bc_t_rollup",
            "parent_segment_id": "",
        },
        # This child's parent_segment_id points back at bc_t_rollup and
        # carries a populated collection_label — that's what marks
        # bc_t_rollup as a roll-up rather than a genuinely collection-blank
        # segment.
        "bc_t_page": {
            **_seg("Template", client="__NOT_APPLICABLE__"),
            "business_center_label": "BC_0000",
            "collection_label": "Page Standards",
            "segment_id": "bc_t_page",
            "parent_segment_id": "bc_t_rollup",
        },
        "bc_c_page": {
            **_seg("Container", client="n/a"),
            "business_center_label": "BC_0000",
            "collection_label": "Page Standards",
        },
        "bc_c_internalenterprise": {
            **_seg("Container", client="n/a"),
            "business_center_label": "BC_0000",
            "collection_label": "InternalEnterprise Standards",
        },
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    # The roll-up must not wildcard-match ANY specific-collection Container.
    assert ("bc_t_rollup", "bc_c_page", "template_to_container") not in pairs
    assert ("bc_t_rollup", "bc_c_internalenterprise", "template_to_container") not in pairs
    # A collection-specific Template still correctly pairs with the matching
    # collection's Container, and not with a different one.
    assert ("bc_t_page", "bc_c_page", "template_to_container") in pairs
    assert ("bc_t_page", "bc_c_internalenterprise", "template_to_container") not in pairs


def test_scope_level_derivation():
    # Scope level is derived from explicit, literal client_label/
    # business_center_label values -- orthogonal to governance_role.
    assert _scope_level({**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"}, POLICY) == "enterprise"
    assert _scope_level({**_seg("Template", client="InternalEnterprise"), "business_center_label": "2270"}, POLICY) == "business_center"
    assert _scope_level({**_seg("Project", client="Acme"), "business_center_label": "2270"}, POLICY) == "client_business_center"
    # Role never enters into the classification -- a client+bc segment can
    # be Template, Container, or Project; scope alone doesn't imply role.
    assert _scope_level({**_seg("Container", client="Acme"), "business_center_label": "2270"}, POLICY) == "client_business_center"
    # Either dimension not cut at all (blank) is a roll-up, not a defined
    # scope level.
    assert _scope_level(_seg("Template", client=""), POLICY) is None
    assert _scope_level({**_seg("Template", client="Acme"), "business_center_label": ""}, POLICY) is None


def test_0000_flows_through_as_literal_enterprise_value():
    # Under the explicit-metadata contract, "0000" is a real, literal
    # business_center_label value (the Enterprise identity) -- it must not
    # be folded to blank anymore.
    assert _normalize_bc_label("0000") == "0000"
    assert _normalize_bc_label("BC_1234") == "BC_1234"
    assert _bc_of({**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"}) == "0000"
    assert _scope_level({**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"}, POLICY) == "enterprise"


def test_bc_0000_spelling_variants_canonicalize_to_0000():
    # "0000"/"BC_0000" (any case) are spelling variants of the same
    # enterprise-bookkeeping value elsewhere in the pipeline (e.g. the
    # extraction completeness gate documents both) -- they must canonicalize
    # to the SAME literal "0000", not fragment into two distinct-looking
    # business centers, and must not fold to blank either.
    for token in ("BC_0000", "bc_0000", "Bc_0000"):
        assert _normalize_bc_label(token) == "0000"
        row = {**_seg("Template", client="InternalEnterprise"), "business_center_label": token}
        assert _bc_of(row) == "0000"
        assert _scope_level(row, POLICY) == "enterprise"


def test_na_spelled_business_center_labels_normalize_to_blank():
    # A missing business_center_label spelled as an NA token (n/a, NA,
    # __NOT_APPLICABLE__, ...) must still normalize to blank -- this is a
    # distinct mechanism (is_blank_or_na()) from the removed "0000" fold, and
    # a blank bc means the segment is a roll-up (not cut on bc), not a
    # defined scope level.
    for token in ("n/a", "NA", "__NOT_APPLICABLE__", "not applicable"):
        assert _normalize_bc_label(token) == ""
        row = {**_seg("Template", client="InternalEnterprise"), "business_center_label": token}
        assert _bc_of(row) == ""
        assert _scope_level(row, POLICY) is None


def test_discover_governance_chain_enterprise_to_project_reaches_every_scope():
    # An enterprise-scoped Template/Container (InternalEnterprise/"0000") has no real
    # client/bc narrowing of its own and applies across the whole business —
    # it must reach every Project regardless of that project's own client/bc
    # scope.
    manifest = {
        "ent_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"},
        "proj_a": {**_seg("Project", client="Acme"), "business_center_label": "BC_1234"},
        "proj_b": {**_seg("Project", client="Widgets"), "business_center_label": "BC_9999"},
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    assert ("ent_t", "proj_a", "enterprise_to_project") in pairs
    assert ("ent_t", "proj_b", "enterprise_to_project") in pairs


def test_discover_governance_chain_bc_to_project_scoped_to_matching_bc_only():
    # A business_center-scoped Template (InternalEnterprise + a real bc) only reaches
    # Projects within the SAME (normalized) business center — not projects
    # in a different bc, even though both are still "downstream" of the
    # enterprise.
    manifest = {
        "bc_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "BC_1234"},
        "proj_same_bc": {**_seg("Project", client="Acme"), "business_center_label": "BC_1234"},
        "proj_other_bc": {**_seg("Project", client="Widgets"), "business_center_label": "BC_9999"},
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    assert ("bc_t", "proj_same_bc", "bc_to_project") in pairs
    assert ("bc_t", "proj_other_bc", "bc_to_project") not in pairs


def test_discover_governance_chain_enterprise_to_bc_and_client_are_same_role_only():
    # enterprise_to_bc / enterprise_to_client are standard-to-standard
    # (Template vs Template, Container vs Container) — they must not mix
    # roles (Template vs Container).
    manifest = {
        "ent_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"},
        "ent_c": {**_seg("Container", client="InternalEnterprise"), "business_center_label": "0000"},
        "bc_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "BC_1234"},
        "bc_c": {**_seg("Container", client="InternalEnterprise"), "business_center_label": "BC_1234"},
        # No business_center_label at all -- a client-wide roll-up, a valid
        # (distinct) enterprise_to_client target alongside any
        # client_business_center-scoped standard the client might also have.
        "client_t": _seg("Template", client="Acme"),
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    assert ("ent_t", "bc_t", "enterprise_to_bc") in pairs
    assert ("ent_c", "bc_c", "enterprise_to_bc") in pairs
    assert ("ent_t", "bc_c", "enterprise_to_bc") not in pairs
    assert ("ent_t", "client_t", "enterprise_to_client") in pairs


def test_enterprise_to_bc_and_sibling_template_survive_with_distinct_run_ids():
    # An enterprise (InternalEnterprise/0000) standard and a real-BC standard of the
    # same role sharing a parent_segment_id get paired BOTH as
    # sibling_templates (discover_sibling_segments, symmetric Jaccard) AND
    # as enterprise_to_bc (discover_governance_chain, directed reference-
    # union containment) -- these are genuinely distinct measurements of the
    # same two segments, not duplicates, so neither drop_legacy_siblings_
    # covered_by_peer_comparisons() nor anything else should suppress
    # either row. "seg_0000" sorts before "seg_bc001" alphabetically, so
    # both discover_sibling_segments()'s sorted-ID pairing and
    # discover_governance_chain()'s enterprise-then-bc pairing land on the
    # exact same (seg_a, seg_b) orientation -- the scenario that used to
    # collide on comparison_run_id.
    manifest = {
        "seg_0000": {
            **_seg("Template", client="InternalEnterprise"),
            "business_center_label": "0000",
            "parent_segment_id": "parent1",
        },
        "seg_bc001": {
            **_seg("Template", client="InternalEnterprise"),
            "business_center_label": "BC1",
            "parent_segment_id": "parent1",
        },
    }

    sibling_pairs = discover_sibling_segments(POLICY, manifest)
    governance_pairs = discover_governance_chain(POLICY, manifest)
    assert ("seg_0000", "seg_bc001", "sibling_templates") in sibling_pairs
    assert ("seg_0000", "seg_bc001", "enterprise_to_bc") in governance_pairs

    pairs = deduplicate_pairs(sibling_pairs + governance_pairs)
    pairs = drop_legacy_siblings_covered_by_peer_comparisons(pairs)

    surviving = {ctype for a, b, ctype in pairs if {a, b} == {"seg_0000", "seg_bc001"}}
    assert surviving == {"sibling_templates", "enterprise_to_bc"}

    executed_utc = "2026-07-20T00:00:00Z"
    ids = {
        ctype: make_comparison_run_id("seg_0000", "seg_bc001", executed_utc, ctype)
        for ctype in surviving
    }
    assert len(set(ids.values())) == len(ids)


def test_make_comparison_run_id_differs_by_comparison_type_for_same_pair_and_timestamp():
    executed_utc = "2026-07-20T00:00:00Z"
    id_a = make_comparison_run_id("s1", "s2", executed_utc, "sibling_templates")
    id_b = make_comparison_run_id("s1", "s2", executed_utc, "enterprise_to_bc")
    assert id_a != id_b
    # Deterministic given identical inputs.
    assert id_a == make_comparison_run_id("s1", "s2", executed_utc, "sibling_templates")


def test_discover_governance_chain_excludes_generic_from_scope_fanout():
    # Generic/Generic-Host already pairs unconditionally against every
    # Template/Container/Project via the existing generic_ids loop — it must
    # not also get a redundant enterprise/bc/client-scoped edge.
    manifest = {
        "g": _seg("Generic", client="Global"),
        "proj": _seg("Project", client="Acme"),
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    assert ("g", "proj", "generic_to_project") in pairs
    assert not any(ctype in ("enterprise_to_project", "bc_to_project") for _a, _b, ctype in pairs if _a == "g")


def test_discover_governance_chain_excludes_ancestor_descendant_from_scope_fanout():
    # The scope-fanout loops group purely by scope level, ignoring
    # parent_segment_id — so an ancestor and its own descendant (e.g. an
    # enterprise-scoped Template and a bc-scoped Template nested directly
    # under it) can otherwise land on opposite sides of one of these edges,
    # even though a descendant's data is always a subset of its ancestor's.
    manifest = {
        "ent_t": {
            **_seg("Template", client="InternalEnterprise"), "business_center_label": "0000",
            "parent_segment_id": "",
        },
        "bc_t_child": {
            **_seg("Template", client="InternalEnterprise"), "business_center_label": "BC_1234",
            "parent_segment_id": "ent_t",
        },
        "proj_grandchild": {
            **_seg("Project", client="Acme"), "business_center_label": "BC_1234",
            "parent_segment_id": "bc_t_child",
        },
        "bc_t_unrelated": {
            **_seg("Template", client="InternalEnterprise"), "business_center_label": "BC_1234",
            "parent_segment_id": "",
        },
        "proj_unrelated": {
            **_seg("Project", client="Widgets"), "business_center_label": "BC_1234",
            "parent_segment_id": "",
        },
    }

    pairs = set(discover_governance_chain(POLICY, manifest))

    # Ancestor/descendant pairs excluded from all four scope-fanout edges.
    assert ("ent_t", "bc_t_child", "enterprise_to_bc") not in pairs
    assert ("ent_t", "proj_grandchild", "enterprise_to_project") not in pairs
    assert ("bc_t_child", "proj_grandchild", "bc_to_project") not in pairs
    # Unrelated peers (no shared lineage) still pair normally.
    assert ("ent_t", "bc_t_unrelated", "enterprise_to_bc") in pairs
    assert ("ent_t", "proj_unrelated", "enterprise_to_project") in pairs
    assert ("bc_t_unrelated", "proj_unrelated", "bc_to_project") in pairs


def test_discover_governance_chain_enterprise_to_bc_reaches_every_real_bc():
    # An enterprise-scoped Template must fan out to EVERY real business
    # center's same-role Template, not just one -- a fixture with 3+ BCs must
    # show all 3, not a fixed pair.
    manifest = {
        "ent_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"},
        "bc1_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "1450"},
        "bc2_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "2270"},
        "bc3_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "9999"},
    }

    pairs = {(a, b) for a, b, ctype in discover_governance_chain(POLICY, manifest) if ctype == "enterprise_to_bc"}

    assert pairs == {("ent_t", "bc1_t"), ("ent_t", "bc2_t"), ("ent_t", "bc3_t")}


def test_discover_governance_chain_bc_to_bc_pairs_every_peer_business_center():
    # Purpose-built BC-to-BC peer discovery: every pair of real business
    # centers' same-role, same-discipline Template populations.
    manifest = {
        "bc1_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "1450"},
        "bc2_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "2270"},
        "bc3_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "9999"},
    }

    pairs = {(a, b) for a, b, ctype in discover_governance_chain(POLICY, manifest) if ctype == "bc_to_bc"}

    assert pairs == {("bc1_t", "bc2_t"), ("bc1_t", "bc3_t"), ("bc2_t", "bc3_t")}


def test_discover_governance_chain_bc_to_bc_excludes_same_bc_and_enterprise():
    manifest = {
        "bc1_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "1450"},
        "bc1_t_dup": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "1450"},
        "ent_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"},
    }

    pairs = {(a, b, ctype) for a, b, ctype in discover_governance_chain(POLICY, manifest) if ctype == "bc_to_bc"}

    # Same normalized bc on both sides is not a peer pair.
    assert not any({a, b} == {"bc1_t", "bc1_t_dup"} for a, b, _ in pairs)
    # Enterprise scope never participates in bc_to_bc.
    assert not any("ent_t" in (a, b) for a, b, _ in pairs)


def test_discover_governance_chain_disc_match_has_no_blank_wildcard():
    # A row with blank discipline_label must not wildcard-pair with a
    # populated-discipline row under discipline-gated comparison types --
    # the removed _disc_match() blank wildcard must not silently reappear.
    manifest = {
        "bc1_t_blank": {
            **_seg("Template", client="InternalEnterprise", discipline=""), "business_center_label": "1450",
        },
        "bc2_t_arch": {
            **_seg("Template", client="InternalEnterprise", discipline="Architectural"), "business_center_label": "2270",
        },
    }

    pairs = {(a, b) for a, b, ctype in discover_governance_chain(POLICY, manifest) if ctype == "bc_to_bc"}

    assert ("bc1_t_blank", "bc2_t_arch") not in pairs
    assert ("bc2_t_arch", "bc1_t_blank") not in pairs


def test_discover_client_cross_bc_multi_bc_enumeration():
    # A client present in 3 BCs produces pairs across all 3 (not a fixed
    # two-BC comparison) -- derived from the data, not hardcoded.
    manifest = {
        "acme_bc1": {**_seg("Project", client="Acme"), "business_center_label": "1450"},
        "acme_bc2": {**_seg("Project", client="Acme"), "business_center_label": "2270"},
        "acme_bc3": {**_seg("Project", client="Acme"), "business_center_label": "9999"},
    }

    pairs = {(a, b) for a, b, ctype in discover_client_cross_bc(POLICY, manifest) if ctype == "client_cross_bc"}

    assert pairs == {("acme_bc1", "acme_bc2"), ("acme_bc1", "acme_bc3"), ("acme_bc2", "acme_bc3")}


def test_discover_client_cross_bc_single_bc_produces_no_pairs():
    manifest = {
        "acme_bc1": {**_seg("Project", client="Acme"), "business_center_label": "1450"},
        "widgets_bc1": {**_seg("Project", client="Widgets"), "business_center_label": "1450"},
    }

    assert discover_client_cross_bc(POLICY, manifest) == []


def test_discover_client_cross_bc_and_bc_to_bc_do_not_reference_collection_label():
    # Regression guard: these new pair-discovery functions must not gate on
    # collection_label -- PR1 left it an always-blank column, and neither
    # function should depend on it being populated (or its absence) to find
    # a pair a differing collection_label value would otherwise be expected
    # to block, since neither reads the field at all.
    manifest = {
        "acme_bc1": {
            **_seg("Project", client="Acme"), "business_center_label": "1450",
            "collection_label": "Some Collection",
        },
        "acme_bc2": {
            **_seg("Project", client="Acme"), "business_center_label": "2270",
            "collection_label": "A Totally Different Collection",
        },
        "bc1_t": {
            **_seg("Template", client="InternalEnterprise"), "business_center_label": "1450",
            "collection_label": "Some Collection",
        },
        "bc2_t": {
            **_seg("Template", client="InternalEnterprise"), "business_center_label": "2270",
            "collection_label": "A Totally Different Collection",
        },
    }

    assert ("acme_bc1", "acme_bc2", "client_cross_bc") in discover_client_cross_bc(POLICY, manifest)
    assert ("bc1_t", "bc2_t", "bc_to_bc") in discover_governance_chain(POLICY, manifest)


def test_pooled_comparison_bc_scope_pools_across_clients_ignoring_client(tmp_path):
    segments_root = tmp_path / "segments"
    domain = "line_patterns"
    _write_segment(
        segments_root, "proj_a", domain,
        [("p1", "shared", "Shared"), ("p2", "a_only", "A Only")],
        [{"export_run_id": "proj_a_file", "pattern_id": "p1"}, {"export_run_id": "proj_a_file", "pattern_id": "p2"}],
        [{"export_run_id": "proj_a_file", "pattern_id": "p1"}],
        ["p1", "p2"],
    )
    _write_segment(
        segments_root, "proj_b", domain,
        [("p1", "shared", "Shared"), ("p2", "b_only", "B Only")],
        [{"export_run_id": "proj_b_file", "pattern_id": "p1"}, {"export_run_id": "proj_b_file", "pattern_id": "p2"}],
        [{"export_run_id": "proj_b_file", "pattern_id": "p1"}],
        ["p1", "p2"],
    )
    manifest = {
        "proj_a": {**_seg("Project", client="Acme"), "business_center_label": "BC_1234", "segment_label": "Proj A"},
        "proj_b": {**_seg("Project", client="Widgets"), "business_center_label": "BC_1234", "segment_label": "Proj B"},
    }
    registry = {
        "proj_a": {"output_folder": "proj_a", "run_type": "bundle"},
        "proj_b": {"output_folder": "proj_b", "run_type": "bundle"},
    }

    rows = run_pooled_comparison(POLICY, manifest, registry, segments_root, min_patterns=1, executed_utc="2026-07-13T00:00:00Z")

    # No shared parent_segment_id and different clients — only the bc-scoped
    # pool should fire, not parent_sibling or client.
    assert {r["pool_scope"] for r in rows} == {"bc"}
    by_sid = {r["segment_id"]: r for r in rows}
    assert by_sid["proj_a"]["n_shared_join_hash"] == "1"
    assert by_sid["proj_a"]["all_containment_focal_in_pool"] == "0.500000"


def test_pooled_comparison_bc_scope_pools_enterprise_0000_segments(tmp_path):
    # Before this PR, business_center_label=="0000" normalized to blank via
    # _normalize_bc_label(), so `if bc:` at the bc_groups gate in
    # run_pooled_comparison() was always False for Enterprise-scoped rows --
    # they were silently excluded from bc-scoped pooling entirely (not
    # pooled under a "blank" bucket, just never added to bc_groups at all).
    # _bc_of() (used here, same as everywhere else in this file) now returns
    # the literal "0000", so two Enterprise segments correctly pool together
    # under their own real bc bucket.
    segments_root = tmp_path / "segments"
    domain = "line_patterns"
    _write_segment(
        segments_root, "ent_a", domain,
        [("p1", "shared", "Shared"), ("p2", "a_only", "A Only")],
        [{"export_run_id": "ent_a_file", "pattern_id": "p1"}, {"export_run_id": "ent_a_file", "pattern_id": "p2"}],
        [{"export_run_id": "ent_a_file", "pattern_id": "p1"}],
        ["p1", "p2"],
    )
    _write_segment(
        segments_root, "ent_b", domain,
        [("p1", "shared", "Shared"), ("p2", "b_only", "B Only")],
        [{"export_run_id": "ent_b_file", "pattern_id": "p1"}, {"export_run_id": "ent_b_file", "pattern_id": "p2"}],
        [{"export_run_id": "ent_b_file", "pattern_id": "p1"}],
        ["p1", "p2"],
    )
    manifest = {
        "ent_a": {**_seg("Project", client="InternalEnterprise"), "business_center_label": "0000", "segment_label": "Ent A"},
        "ent_b": {**_seg("Project", client="InternalEnterprise"), "business_center_label": "0000", "segment_label": "Ent B"},
    }
    registry = {
        "ent_a": {"output_folder": "ent_a", "run_type": "bundle"},
        "ent_b": {"output_folder": "ent_b", "run_type": "bundle"},
    }

    rows = run_pooled_comparison(POLICY, manifest, registry, segments_root, min_patterns=1, executed_utc="2026-07-13T00:00:00Z")

    # Same client (both "InternalEnterprise") -- both bc and client pools fire, since
    # "0000" is no longer folded away and client_groups still fires
    # independently.
    assert {r["pool_scope"] for r in rows} == {"bc", "client"}
    by_sid_scope = {(r["segment_id"], r["pool_scope"]): r for r in rows}
    bc_row = by_sid_scope[("ent_a", "bc")]
    assert bc_row["business_center_label"] == "0000"
    assert bc_row["n_shared_join_hash"] == "1"
    assert bc_row["all_containment_focal_in_pool"] == "0.500000"


def test_pooled_comparison_client_scope_pools_across_bcs_ignoring_bc(tmp_path):
    segments_root = tmp_path / "segments"
    domain = "line_patterns"
    _write_segment(
        segments_root, "proj_a", domain,
        [("p1", "shared", "Shared"), ("p2", "a_only", "A Only")],
        [{"export_run_id": "proj_a_file", "pattern_id": "p1"}, {"export_run_id": "proj_a_file", "pattern_id": "p2"}],
        [{"export_run_id": "proj_a_file", "pattern_id": "p1"}],
        ["p1", "p2"],
    )
    _write_segment(
        segments_root, "proj_b", domain,
        [("p1", "shared", "Shared"), ("p2", "b_only", "B Only")],
        [{"export_run_id": "proj_b_file", "pattern_id": "p1"}, {"export_run_id": "proj_b_file", "pattern_id": "p2"}],
        [{"export_run_id": "proj_b_file", "pattern_id": "p1"}],
        ["p1", "p2"],
    )
    manifest = {
        "proj_a": {**_seg("Project", client="Acme"), "business_center_label": "BC_1234", "segment_label": "Proj A"},
        "proj_b": {**_seg("Project", client="Acme"), "business_center_label": "BC_9999", "segment_label": "Proj B"},
    }
    registry = {
        "proj_a": {"output_folder": "proj_a", "run_type": "bundle"},
        "proj_b": {"output_folder": "proj_b", "run_type": "bundle"},
    }

    rows = run_pooled_comparison(POLICY, manifest, registry, segments_root, min_patterns=1, executed_utc="2026-07-13T00:00:00Z")

    # Same client, different bc, no shared parent_segment_id — only the
    # client-scoped pool should fire.
    assert {r["pool_scope"] for r in rows} == {"client"}
    by_sid = {r["segment_id"]: r for r in rows}
    assert by_sid["proj_b"]["n_shared_join_hash"] == "1"
    assert by_sid["proj_b"]["all_containment_focal_in_pool"] == "0.500000"


def test_pooled_comparison_excludes_rollup_ancestor_from_bc_pool(tmp_path):
    # A collection-blank BC roll-up and its collection-specific child share
    # the same normalized business_center_label, so a naive bc-pool grouping
    # would put the child in a pool that includes its own ancestor — whose
    # population already contains (a superset of) the child's own data.
    # The child's real peer ("peer", no lineage relation) must be the only
    # pool member; if the rollup leaked in, focal-in-pool containment would
    # be 1.0 instead of 0.0 (peer shares nothing with the child).
    segments_root = tmp_path / "segments"
    domain = "line_patterns"
    _write_segment(
        segments_root, "rollup", domain,
        [("r1", "jh_child", "Child Pattern")],
        [{"export_run_id": "rollup_file", "pattern_id": "r1"}],
        [{"export_run_id": "rollup_file", "pattern_id": "r1"}],
        ["r1"],
    )
    _write_segment(
        segments_root, "child", domain,
        [("c1", "jh_child", "Child Pattern")],
        [{"export_run_id": "child_file", "pattern_id": "c1"}],
        [{"export_run_id": "child_file", "pattern_id": "c1"}],
        ["c1"],
    )
    _write_segment(
        segments_root, "peer", domain,
        [("p1", "jh_peer_only", "Peer Only")],
        [{"export_run_id": "peer_file", "pattern_id": "p1"}],
        [{"export_run_id": "peer_file", "pattern_id": "p1"}],
        ["p1"],
    )
    manifest = {
        "rollup": {
            **_seg("Template", client=""), "business_center_label": "BC_1234",
            "segment_label": "Rollup", "parent_segment_id": "",
        },
        "child": {
            **_seg("Template", client=""), "business_center_label": "BC_1234",
            "segment_label": "Child", "parent_segment_id": "rollup",
        },
        "peer": {
            **_seg("Template", client=""), "business_center_label": "BC_1234",
            "segment_label": "Peer", "parent_segment_id": "",
        },
    }
    registry = {
        "rollup": {"output_folder": "rollup", "run_type": "bundle"},
        "child": {"output_folder": "child", "run_type": "bundle"},
        "peer": {"output_folder": "peer", "run_type": "bundle"},
    }

    rows = run_pooled_comparison(POLICY, manifest, registry, segments_root, min_patterns=1, executed_utc="2026-07-13T00:00:00Z")

    child_row = [r for r in rows if r["segment_id"] == "child" and r["pool_scope"] == "bc"][0]
    assert child_row["n_files_pool"] == "1"
    assert child_row["n_shared_join_hash"] == "0"
    assert child_row["all_containment_focal_in_pool"] == "0.000000"


def test_project_target_governance_state_uses_target_used():
    assert _usage_interpretable_for_role("Project") is True
    assert _recommended_primary_view("Template", "Project", "template_to_project") == "used"
    assert (
        _classify_governance_state(True, True, False, True, True)
        == "provided_but_passive"
    )
    assert (
        _classify_governance_state(False, True, True, True, True)
        == "local_active"
    )


def test_standards_carrier_target_avoids_passive_bloat_label():
    assert _usage_interpretable_for_role("Template") is False
    assert _recommended_primary_view("Generic", "Template", "generic_to_template") == "all"
    assert (
        _classify_governance_state(True, True, False, True, False)
        == "provided_configured"
    )
    assert "all-view is primary" in _comparison_role_semantics(
        "Generic", "Template", "generic_to_template"
    )


def _write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise AssertionError("test helper requires at least one row")
    import csv

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_segment(seg_root: Path, folder: str, domain: str, patterns, all_rows, used_rows, bundle_all):
    base = seg_root / folder / "results"
    _write_csv(
        base / "analysis" / "domain_patterns.csv",
        [
            {
                "domain": domain,
                "pattern_id": pid,
                "source_cluster_id": f"src|{jh}",
                "pattern_label_human": label,
                "pattern_label": label,
            }
            for pid, jh, label in patterns
        ],
    )
    _write_csv(base / "bundle_analysis" / "all" / domain / "membership_matrix.csv", all_rows)
    _write_csv(base / "bundle_analysis" / "used" / domain / "membership_matrix.csv", used_rows)
    _write_csv(
        base / "bundle_analysis" / "all" / domain / "bundle_membership.csv",
        [{"pattern_id": pid} for pid in bundle_all],
    )


def _write_reference_analysis_segment(
    seg_root: Path,
    folder: str,
    domain: str,
    rows,
    export_run_ids=None,
    presence_rows=None,
):
    base = seg_root / folder
    if export_run_ids is not None:
        base.mkdir(parents=True, exist_ok=True)
        (base / "export_run_ids.txt").write_text(
            "\n".join(export_run_ids) + "\n", encoding="utf-8"
        )
    _write_csv(
        base / "results" / "analysis" / "domain_patterns.csv",
        [
            {
                "domain": row.get("domain", domain),
                "pattern_id": row["pattern_id"],
                "source_cluster_id": f"src|{row['join_hash']}",
                "pattern_label_human": row.get("label", row["pattern_id"]),
                "pattern_label": row.get("label", row["pattern_id"]),
                **(
                    {"export_run_id": row["export_run_id"]}
                    if "export_run_id" in row
                    else {}
                ),
            }
            for row in rows
        ],
    )
    if presence_rows is not None:
        _write_csv(
            base / "results" / "analysis" / "pattern_presence_file.csv",
            [
                {
                    "export_run_id": row["export_run_id"],
                    "domain": row.get("domain", domain),
                    "pattern_id": row["pattern_id"],
                }
                for row in presence_rows
            ],
        )


def test_reference_analysis_segment_discovers_domains_without_bundle_outputs(tmp_path):
    segments_root = tmp_path / "segments"
    registry = {"generic": {"output_folder": "generic"}}
    _write_reference_analysis_segment(
        segments_root,
        "generic",
        "line_patterns",
        [{"pattern_id": "g1", "join_hash": "provided_a"}],
        export_run_ids=["generic_file"],
    )

    assert discover_domains_for_segment(segments_root, registry, "generic") == {
        "line_patterns"
    }


def test_reference_analysis_segment_loads_all_view_from_domain_patterns(tmp_path):
    segments_root = tmp_path / "segments"
    registry = {"generic": {"output_folder": "generic"}}
    _write_reference_analysis_segment(
        segments_root,
        "generic",
        "line_patterns",
        [
            {"pattern_id": "g1", "join_hash": "provided_a"},
            {"pattern_id": "g2", "join_hash": "provided_b"},
        ],
        export_run_ids=["generic_file"],
    )

    assert load_file_join_hashes(
        segments_root, registry, "generic", "line_patterns", "all"
    ) == {"generic_file": {"provided_a", "provided_b"}}
    assert (
        load_file_join_hashes(
            segments_root, registry, "generic", "line_patterns", "used"
        )
        == {}
    )


def test_reference_analysis_segment_groups_fallback_by_export_run_id_column(tmp_path):
    segments_root = tmp_path / "segments"
    registry = {"generic": {"output_folder": "generic"}}
    _write_reference_analysis_segment(
        segments_root,
        "generic",
        "line_patterns",
        [
            {"pattern_id": "g1", "join_hash": "provided_a", "export_run_id": "file_a"},
            {"pattern_id": "g2", "join_hash": "provided_b", "export_run_id": "file_b"},
        ],
        export_run_ids=["file_a", "file_b"],
    )

    assert load_file_join_hashes(
        segments_root, registry, "generic", "line_patterns", "all"
    ) == {
        "file_a": {"provided_a"},
        "file_b": {"provided_b"},
    }


def test_reference_analysis_segment_uses_presence_for_multi_file_fallback(tmp_path):
    segments_root = tmp_path / "segments"
    registry = {"generic": {"output_folder": "generic"}}
    _write_reference_analysis_segment(
        segments_root,
        "generic",
        "line_patterns",
        [
            {"pattern_id": "g1", "join_hash": "provided_a"},
            {"pattern_id": "g2", "join_hash": "provided_b"},
        ],
        export_run_ids=["file_a", "file_b"],
        presence_rows=[
            {"export_run_id": "file_a", "pattern_id": "g1"},
            {"export_run_id": "file_b", "pattern_id": "g2"},
        ],
    )

    assert load_file_join_hashes(
        segments_root, registry, "generic", "line_patterns", "all"
    ) == {
        "file_a": {"provided_a"},
        "file_b": {"provided_b"},
    }


def test_build_governance_state_rows_include_inherited_unused_and_local_active(tmp_path):
    from compare_cross_segment import build_governance_state_outputs  # noqa: E402

    domain = "line_patterns"
    segments_root = tmp_path / "segments"
    _write_segment(
        segments_root,
        "ref",
        domain,
        [("r1", "provided_used", "Provided Used"), ("r2", "provided_passive", "Provided Passive")],
        [
            {"export_run_id": "ref_file", "pattern_id": "r1"},
            {"export_run_id": "ref_file", "pattern_id": "r2"},
        ],
        [{"export_run_id": "ref_file", "pattern_id": "r1"}],
        ["r1", "r2"],
    )
    _write_segment(
        segments_root,
        "tgt",
        domain,
        [
            ("t1", "provided_used", "Provided Used"),
            ("t2", "provided_passive", "Provided Passive"),
            ("t3", "local_active", "Local Active"),
        ],
        [
            {"export_run_id": "target_file", "pattern_id": "t1"},
            {"export_run_id": "target_file", "pattern_id": "t2"},
            {"export_run_id": "target_file", "pattern_id": "t3"},
        ],
        [
            {"export_run_id": "target_file", "pattern_id": "t1"},
            {"export_run_id": "target_file", "pattern_id": "t3"},
        ],
        ["t1", "t2", "t3"],
    )
    manifest = {
        "ref": {**_seg("Template"), "segment_label": "Template"},
        "tgt": {**_seg("Project"), "segment_label": "Project"},
    }
    registry = {
        "ref": {"output_folder": "ref", "run_type": "bundle"},
        "tgt": {"output_folder": "tgt", "run_type": "bundle"},
    }

    rows, summary = build_governance_state_outputs(
        POLICY,
        "cmp_test",
        "ref",
        "tgt",
        "template_to_project",
        domain,
        manifest,
        registry,
        segments_root,
        "2026-05-29T00:00:00Z",
    )

    states = {row["join_hash"]: row["state"] for row in rows}
    assert states == {
        "provided_used": "provided_and_used",
        "provided_passive": "provided_but_passive",
        "local_active": "local_active",
    }
    assert summary["provided_to_configured_containment"] == "1.000000"
    assert summary["provided_to_used_containment"] == "0.500000"
    assert summary["provided_passive_share"] == "0.500000"
    assert summary["local_active_share"] == "0.500000"


def test_pair_domain_work_items_use_pair_domain_union(tmp_path):
    segments_root = tmp_path / "segments"
    registry = {
        "a": {"output_folder": "a"},
        "b": {"output_folder": "b"},
        "c": {"output_folder": "c"},
    }
    for folder, domain in [("a", "domain_a"), ("b", "domain_b"), ("c", "domain_c")]:
        (segments_root / folder / "results" / "bundle_analysis" / "all" / domain).mkdir(
            parents=True
        )

    work_items, _domains_by_segment, active_domains = build_pair_domain_work_items(
        [("a", "b", "sibling_projects"), ("a", "c", "sibling_projects")],
        segments_root,
        registry,
    )

    assert work_items == [
        ("a", "b", "sibling_projects", "domain_a"),
        ("a", "b", "sibling_projects", "domain_b"),
        ("a", "c", "sibling_projects", "domain_a"),
        ("a", "c", "sibling_projects", "domain_c"),
    ]
    assert active_domains == ["domain_a", "domain_b", "domain_c"]


def test_output_row_sort_helpers_are_stable_by_content():
    summary_rows = [
        {"comparison_type": "z", "segment_id_a": "b", "segment_id_b": "a", "domain": "d2"},
        {"comparison_type": "a", "segment_id_a": "b", "segment_id_b": "a", "domain": "d1"},
    ]
    sort_summary_rows(summary_rows)
    assert [row["comparison_type"] for row in summary_rows] == ["a", "z"]

    pair_rows = [
        {
            "_comparison_type": "sibling_projects",
            "segment_id_a": "b",
            "segment_id_b": "c",
            "domain": "d",
            "export_run_id_a": "2",
            "export_run_id_b": "1",
        },
        {
            "_comparison_type": "sibling_projects",
            "segment_id_a": "a",
            "segment_id_b": "c",
            "domain": "d",
            "export_run_id_a": "1",
            "export_run_id_b": "1",
        },
    ]
    sort_pair_detail_rows(pair_rows)
    assert [row["segment_id_a"] for row in pair_rows] == ["a", "b"]


def test_non_project_target_blanks_used_summary_shares(tmp_path):
    from compare_cross_segment import build_governance_state_outputs  # noqa: E402

    domain = "line_patterns"
    segments_root = tmp_path / "segments"
    _write_segment(
        segments_root,
        "generic",
        domain,
        [("g1", "provided_a", "Provided A"), ("g2", "provided_b", "Provided B")],
        [
            {"export_run_id": "generic_file", "pattern_id": "g1"},
            {"export_run_id": "generic_file", "pattern_id": "g2"},
        ],
        [{"export_run_id": "generic_file", "pattern_id": "g1"}],
        ["g1", "g2"],
    )
    _write_segment(
        segments_root,
        "template",
        domain,
        [
            ("t1", "provided_a", "Provided A"),
            ("t2", "provided_b", "Provided B"),
            ("t3", "template_local", "Template Local"),
        ],
        [
            {"export_run_id": "template_file", "pattern_id": "t1"},
            {"export_run_id": "template_file", "pattern_id": "t2"},
            {"export_run_id": "template_file", "pattern_id": "t3"},
        ],
        [{"export_run_id": "template_file", "pattern_id": "t3"}],
        ["t1", "t2", "t3"],
    )
    manifest = {
        "generic": {**_seg("Generic"), "segment_label": "Generic"},
        "template": {**_seg("Template"), "segment_label": "Template"},
    }
    registry = {
        "generic": {"output_folder": "generic", "run_type": "bundle"},
        "template": {"output_folder": "template", "run_type": "bundle"},
    }

    rows, summary = build_governance_state_outputs(
        POLICY,
        "cmp_test",
        "generic",
        "template",
        "generic_to_template",
        domain,
        manifest,
        registry,
        segments_root,
        "2026-05-29T00:00:00Z",
    )

    states = {row["join_hash"]: row["state"] for row in rows}
    assert states == {
        "provided_a": "provided_configured",
        "provided_b": "provided_configured",
        "template_local": "local_configured",
    }
    assert summary["target_usage_interpretable"] == "false"
    assert summary["provided_to_configured_containment"] == "1.000000"
    assert summary["provided_to_used_containment"] == ""
    assert summary["provided_passive_share"] == ""
    assert summary["local_active_share"] == ""
    assert summary["provided_and_used_pct_of_reference_all"] == ""
    assert summary["provided_but_passive_pct_of_reference_all"] == ""
    assert summary["local_active_pct_of_target_used"] == ""


def test_main_emits_governance_states_when_pair_skipped_by_min_patterns(tmp_path, monkeypatch):
    import csv

    domain = "sparse_line_patterns"
    records_dir = tmp_path / "records"
    segments_root = tmp_path / "segments"
    out_dir = tmp_path / "out"
    records_dir.mkdir()

    _write_csv(
        records_dir / "segment_manifest.csv",
        [
            {
                "segment_id": "generic_sparse",
                "segment_label": "Generic",
                "governance_role": "Generic",
                "client_label": "Global",
                "discipline_label": "Arch",
                "unit_system": "imperial",
                "run_type": "bundle",
                "segment_level": "2",
                "parent_segment_id": "imperial",
            },
            {
                "segment_id": "project_sparse",
                "segment_label": "Project",
                "governance_role": "Project",
                "client_label": "Acme",
                "discipline_label": "Arch",
                "unit_system": "imperial",
                "run_type": "bundle",
                "segment_level": "2",
                "parent_segment_id": "imperial",
            },
        ],
    )
    _write_csv(
        records_dir / "run_registry.csv",
        [
            {"segment_id": "generic_sparse", "output_folder": "generic_sparse", "run_type": "bundle"},
            {"segment_id": "project_sparse", "output_folder": "project_sparse", "run_type": "bundle"},
        ],
    )
    _write_csv(records_dir / "file_metadata.csv", [{"export_run_id": "generic_file", "project_label": ""}])
    _write_segment(
        segments_root,
        "generic_sparse",
        domain,
        [("g1", "provided_missing_a", "Provided Missing A"), ("g2", "provided_missing_b", "Provided Missing B")],
        [
            {"export_run_id": "generic_file", "pattern_id": "g1"},
            {"export_run_id": "generic_file", "pattern_id": "g2"},
        ],
        [{"export_run_id": "generic_file", "pattern_id": "g1"}],
        ["g1", "g2"],
    )
    (segments_root / "project_sparse").mkdir(parents=True)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compare_cross_segment.py",
            "--segments-root",
            str(segments_root),
            "--records-dir",
            str(records_dir),
            "--out-dir",
            str(out_dir),
            "--governance-chain",
            "--domain",
            domain,
            "--min-patterns",
            "3",
            "--workers",
            "1",
            "--no-delta",
        ],
    )

    assert compare_main() == 0

    summary_path = out_dir / "cross_segment_summary.csv"
    states_path = out_dir / "cross_segment_governance_states.csv"
    state_summary_path = out_dir / "cross_segment_governance_state_summary.csv"
    # project_sparse has zero readable files (not merely below min_patterns) --
    # this is now the explicit blocked case: a real, schema-complete summary
    # row is emitted with comparison_status="blocked" rather than the pair
    # being suppressed outright. Governance-state outputs are unaffected --
    # they run through a separate code path from cross_segment_summary.csv.
    assert summary_path.exists()
    with summary_path.open("r", encoding="utf-8", newline="") as f:
        summary_rows = list(csv.DictReader(f))
    assert len(summary_rows) == 1
    assert summary_rows[0]["comparison_status"] == "blocked"
    assert summary_rows[0]["all_pairwise_jaccard_mean"] == ""
    assert states_path.exists()
    assert state_summary_path.exists()

    with states_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert {row["state"] for row in rows} == {"provided_but_missing"}
    assert {row["join_hash"] for row in rows} == {"provided_missing_a", "provided_missing_b"}

    with state_summary_path.open("r", encoding="utf-8", newline="") as f:
        summary_rows = list(csv.DictReader(f))
    assert summary_rows[0]["provided_but_missing_count"] == "2"
    assert summary_rows[0]["provided_missing_share"] == "1.000000"


def test_main_skips_delta_generation_for_blocked_reference(tmp_path, monkeypatch):
    import csv

    domain = "delta_blocked_domain"
    records_dir = tmp_path / "records"
    segments_root = tmp_path / "segments"
    out_dir = tmp_path / "out"
    records_dir.mkdir()

    _write_csv(
        records_dir / "segment_manifest.csv",
        [
            {
                "segment_id": "template_ref",
                "segment_label": "Template",
                "governance_role": "Template",
                "client_label": "Acme",
                "discipline_label": "",
                "unit_system": "imperial",
                "run_type": "bundle",
                "segment_level": "2",
                "parent_segment_id": "imperial",
            },
            {
                "segment_id": "project_tgt",
                "segment_label": "Project",
                "governance_role": "Project",
                "client_label": "Acme",
                "discipline_label": "",
                "unit_system": "imperial",
                "run_type": "bundle",
                "segment_level": "2",
                "parent_segment_id": "imperial",
            },
        ],
    )
    _write_csv(
        records_dir / "run_registry.csv",
        [
            {"segment_id": "template_ref", "output_folder": "template_ref", "run_type": "bundle"},
            {"segment_id": "project_tgt", "output_folder": "project_tgt", "run_type": "bundle"},
        ],
    )
    _write_csv(records_dir / "file_metadata.csv", [{"export_run_id": "tgt_file", "project_label": ""}])
    # template_ref: zero readable files -- the reference side is blocked.
    (segments_root / "template_ref").mkdir(parents=True)
    # project_tgt: real patterns the (blocked) reference has no knowledge of.
    _write_segment(
        segments_root,
        "project_tgt",
        domain,
        [("p1", "tgt_a", "Target A"), ("p2", "tgt_b", "Target B")],
        [
            {"export_run_id": "tgt_file", "pattern_id": "p1"},
            {"export_run_id": "tgt_file", "pattern_id": "p2"},
        ],
        [
            {"export_run_id": "tgt_file", "pattern_id": "p1"},
            {"export_run_id": "tgt_file", "pattern_id": "p2"},
        ],
        ["p1"],
    )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compare_cross_segment.py",
            "--segments-root", str(segments_root),
            "--records-dir", str(records_dir),
            "--out-dir", str(out_dir),
            "--governance-chain",
            "--domain", domain,
            "--min-patterns", "1",
            "--workers", "1",
            # deliberately no --no-delta: delta generation must be active
            # for this comparison_type so the fix is actually exercised.
        ],
    )

    assert compare_main() == 0

    summary_path = out_dir / "cross_segment_summary.csv"
    delta_path = out_dir / "cross_segment_delta.csv"

    with summary_path.open("r", encoding="utf-8", newline="") as f:
        summary_rows = [r for r in csv.DictReader(f) if r["comparison_type"] == "template_to_project"]
    assert len(summary_rows) == 1
    assert summary_rows[0]["comparison_status"] == "blocked"
    assert summary_rows[0]["n_files_a"] == "0"
    assert summary_rows[0]["n_files_b"] == "1"

    # The blocked reference must not produce delta rows -- with an empty
    # ref_union, tgt_a/tgt_b would otherwise both be misreported as locally
    # drifted patterns instead of "reference unknown."
    if delta_path.exists():
        with delta_path.open("r", encoding="utf-8", newline="") as f:
            delta_rows = [
                r for r in csv.DictReader(f)
                if r["segment_id_reference"] == "template_ref" and r["segment_id_target"] == "project_tgt"
            ]
        assert delta_rows == []



def _union_rows_for(tmp_path, manifest, registry, domain="line_patterns"):
    import compare_cross_segment as ccs

    ccs._jh_cache.clear()
    ccs._pattern_label_cache.clear()
    return build_union_inventory_rows(
        manifest,
        registry,
        {},
        tmp_path / "segments",
        "2026-06-22T00:00:00Z",
        domain_filter=domain,
    )


def test_union_inventory_project_all_view_normalized_union(tmp_path):
    domain = "line_patterns"
    segments_root = tmp_path / "segments"
    _write_segment(
        segments_root,
        "project",
        domain,
        [("p1", "join_a", "Join A"), ("p2", "join_b", "Join B")],
        [
            {"export_run_id": "file_1", "pattern_id": "p1"},
            {"export_run_id": "file_2", "pattern_id": "p2"},
        ],
        [{"export_run_id": "file_1", "pattern_id": "p1"}],
        ["p1", "p2"],
    )
    manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
    registry = {"project": {"output_folder": "project", "run_type": "bundle"}}

    rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "all"]

    assert [r["join_hash"] for r in rows] == ["join_a", "join_b"]
    assert {r["inventory_status"] for r in rows} == {"ok"}
    assert rows[0]["usage_interpretable"] == "true"


def test_union_inventory_project_used_view_normalized_union(tmp_path):
    domain = "line_patterns"
    segments_root = tmp_path / "segments"
    _write_segment(
        segments_root,
        "project",
        domain,
        [("p1", "join_a", "Join A"), ("p2", "join_b", "Join B")],
        [
            {"export_run_id": "file_1", "pattern_id": "p1"},
            {"export_run_id": "file_2", "pattern_id": "p2"},
        ],
        [{"export_run_id": "file_1", "pattern_id": "p1"}],
        ["p1", "p2"],
    )
    manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
    registry = {"project": {"output_folder": "project", "run_type": "bundle"}}

    rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "used"]

    assert len(rows) == 1
    assert rows[0]["join_hash"] == "join_a"
    assert rows[0]["n_files_present"] == "1"
    assert rows[0]["pct_files_present"] == "1.000000"


def test_union_inventory_non_project_used_view_not_active_usage(tmp_path):
    domain = "line_patterns"
    segments_root = tmp_path / "segments"
    _write_segment(
        segments_root,
        "template",
        domain,
        [("t1", "join_a", "Join A")],
        [{"export_run_id": "template_file", "pattern_id": "t1"}],
        [{"export_run_id": "template_file", "pattern_id": "t1"}],
        ["t1"],
    )
    manifest = {"template": {**_seg("Template"), "segment_label": "Template"}}
    registry = {"template": {"output_folder": "template", "run_type": "bundle"}}

    used_rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "used"]

    assert used_rows[0]["usage_interpretable"] == "false"
    assert used_rows[0]["inventory_status"] == "not_interpretable"


def test_union_inventory_duplicate_join_hash_collapses_counts(tmp_path):
    domain = "line_patterns"
    segments_root = tmp_path / "segments"
    _write_segment(
        segments_root,
        "project_a",
        domain,
        [("p1", "same_join", "Same"), ("p2", "same_join", "Same")],
        [
            {"export_run_id": "file_1", "pattern_id": "p1"},
            {"export_run_id": "file_2", "pattern_id": "p2"},
        ],
        [{"export_run_id": "file_1", "pattern_id": "p1"}],
        ["p1", "p2"],
    )
    _write_segment(
        segments_root,
        "project_b",
        domain,
        [("x1", "same_join", "Same")],
        [{"export_run_id": "file_3", "pattern_id": "x1"}],
        [{"export_run_id": "file_3", "pattern_id": "x1"}],
        ["x1"],
    )
    manifest = {
        "project_a": {**_seg("Project"), "segment_label": "Project A"},
        "project_b": {**_seg("Project"), "segment_label": "Project B"},
    }
    registry = {
        "project_a": {"output_folder": "project_a", "run_type": "bundle"},
        "project_b": {"output_folder": "project_b", "run_type": "bundle"},
    }

    rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "all"]

    assert len(rows) == 1
    assert rows[0]["join_hash"] == "same_join"
    assert rows[0]["n_segments_present"] == "2"
    assert rows[0]["n_files_present"] == "3"


def test_union_inventory_pattern_id_not_cross_segment_identity(tmp_path):
    domain = "line_patterns"
    segments_root = tmp_path / "segments"
    _write_segment(
        segments_root,
        "project_a",
        domain,
        [("same_pid", "join_a", "A")],
        [{"export_run_id": "file_1", "pattern_id": "same_pid"}],
        [{"export_run_id": "file_1", "pattern_id": "same_pid"}],
        ["same_pid"],
    )
    _write_segment(
        segments_root,
        "project_b",
        domain,
        [("same_pid", "join_b", "B")],
        [{"export_run_id": "file_2", "pattern_id": "same_pid"}],
        [{"export_run_id": "file_2", "pattern_id": "same_pid"}],
        ["same_pid"],
    )
    manifest = {
        "project_a": {**_seg("Project"), "segment_label": "Project A"},
        "project_b": {**_seg("Project"), "segment_label": "Project B"},
    }
    registry = {
        "project_a": {"output_folder": "project_a", "run_type": "bundle"},
        "project_b": {"output_folder": "project_b", "run_type": "bundle"},
    }

    rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "all"]

    assert [r["join_hash"] for r in rows] == ["join_a", "join_b"]


def test_union_inventory_missing_source_cluster_status_no_synthetic_pattern(tmp_path):
    domain = "line_patterns"
    base = tmp_path / "segments" / "project" / "results"
    _write_csv(
        base / "analysis" / "domain_patterns.csv",
        [{"domain": domain, "pattern_id": "p1", "source_cluster_id": "", "pattern_label_human": "", "pattern_label": ""}],
    )
    _write_csv(
        base / "bundle_analysis" / "all" / domain / "membership_matrix.csv",
        [{"export_run_id": "file_1", "pattern_id": "p1"}],
    )
    manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
    registry = {"project": {"output_folder": "project", "run_type": "bundle"}}

    rows = _union_rows_for(tmp_path, manifest, registry, domain)

    assert all(row["join_hash"] == "" for row in rows)
    assert {row["source_status"] for row in rows} == {"missing_source_cluster_id"}
    assert "no_patterns" in {row["inventory_status"] for row in rows}


def test_union_inventory_output_order_is_deterministic(tmp_path):
    domain = "line_patterns"
    segments_root = tmp_path / "segments"
    _write_segment(
        segments_root,
        "project",
        domain,
        [("p2", "join_b", "B"), ("p1", "join_a", "A")],
        [
            {"export_run_id": "file_2", "pattern_id": "p2"},
            {"export_run_id": "file_1", "pattern_id": "p1"},
        ],
        [
            {"export_run_id": "file_2", "pattern_id": "p2"},
            {"export_run_id": "file_1", "pattern_id": "p1"},
        ],
        ["p2", "p1"],
    )
    manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
    registry = {"project": {"output_folder": "project", "run_type": "bundle"}}

    first = _union_rows_for(tmp_path, manifest, registry, domain)
    second = _union_rows_for(tmp_path, manifest, registry, domain)

    assert first == second
    assert [(r["view_scope"], r["join_hash"]) for r in first] == [
        ("all", "join_a"),
        ("all", "join_b"),
        ("used", "join_a"),
        ("used", "join_b"),
    ]


def test_union_inventory_used_view_unavailable_keeps_source_status_ok(tmp_path):
    domain = "line_patterns"
    base = tmp_path / "segments" / "project" / "results"
    _write_csv(
        base / "analysis" / "domain_patterns.csv",
        [{"domain": domain, "pattern_id": "p1", "source_cluster_id": "src|join_a", "pattern_label_human": "A", "pattern_label": "A"}],
    )
    _write_csv(
        base / "bundle_analysis" / "all" / domain / "membership_matrix.csv",
        [{"export_run_id": "file_1", "pattern_id": "p1"}],
    )
    manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
    registry = {"project": {"output_folder": "project", "run_type": "bundle"}}

    rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "used"]

    assert rows == [
        {
            "governance_role": "Project",
            "client_label": "Acme",
            "discipline_label": "Arch",
            "unit_system": "imperial",
            "domain": domain,
            "view_scope": "used",
            "join_hash": "",
            "pattern_label": "",
            "n_segments_present": "0",
            "n_files_present": "0",
            "n_files_denominator": "0",
            "pct_files_present": "0.000000",
            "n_projects_present": "0",
            "n_projects_denominator": "0",
            "n_clients_present": "1",
            "n_clients_denominator": "1",
            "pct_clients_present": "1.000000",
            "pct_projects_present": "0.000000",
            "usage_interpretable": "true",
            "inventory_status": "used_view_unavailable",
            "source_status": "ok",
            "executed_utc": "2026-06-22T00:00:00Z",
        }
    ]



def test_union_inventory_client_denominator_includes_status_rows_used_by_reuse(tmp_path):
    domain = "line_patterns"
    segments_root = tmp_path / "segments"
    _write_segment(
        segments_root,
        "project_a",
        domain,
        [("p1", "shared", "Shared")],
        [{"export_run_id": "file_a", "pattern_id": "p1"}],
        [{"export_run_id": "file_a", "pattern_id": "p1"}],
        ["p1"],
    )
    base_b = segments_root / "project_b" / "results"
    _write_csv(
        base_b / "analysis" / "domain_patterns.csv",
        [{"domain": domain, "pattern_id": "p1", "source_cluster_id": "src|shared", "pattern_label_human": "Shared", "pattern_label": "Shared"}],
    )
    _write_csv(
        base_b / "bundle_analysis" / "all" / domain / "membership_matrix.csv",
        [{"export_run_id": "file_b", "pattern_id": "p1"}],
    )
    manifest = {
        "project_a": {**_seg("Project", client="A"), "segment_label": "Project A"},
        "project_b": {**_seg("Project", client="B"), "segment_label": "Project B"},
    }
    registry = {
        "project_a": {"output_folder": "project_a", "run_type": "bundle"},
        "project_b": {"output_folder": "project_b", "run_type": "bundle"},
    }

    union_rows = _union_rows_for(tmp_path, manifest, registry, domain)
    shared = [r for r in union_rows if r["view_scope"] == "used" and r["join_hash"] == "shared"][0]
    reuse = build_pattern_reuse_distribution_rows(union_rows, "2026-06-22T00:00:00Z")
    reuse_shared = [r for r in reuse if r["view_scope"] == "used" and r["join_hash"] == "shared"][0]

    assert shared["n_clients_present"] == "1"
    assert shared["n_clients_denominator"] == "2"
    assert shared["pct_clients_present"] == "0.500000"
    status_row = [r for r in union_rows if r["view_scope"] == "used" and r["inventory_status"] == "used_view_unavailable"][0]
    reuse_status = [r for r in reuse if r["view_scope"] == "used" and r["inventory_status"] == "used_view_unavailable"][0]

    assert reuse_shared["n_clients_denominator"] == shared["n_clients_denominator"]
    assert reuse_shared["pct_clients_present"] == shared["pct_clients_present"]
    assert status_row["n_clients_present"] == "1"
    assert status_row["n_clients_denominator"] == "2"
    assert status_row["pct_clients_present"] == "0.500000"
    assert reuse_status["n_clients_present"] == status_row["n_clients_present"]
    assert reuse_status["n_clients_denominator"] == status_row["n_clients_denominator"]
    assert reuse_status["pct_clients_present"] == status_row["pct_clients_present"]

def test_union_inventory_missing_domain_patterns_keeps_source_status_ok(tmp_path):
    domain = "line_patterns"
    (tmp_path / "segments" / "project").mkdir(parents=True)
    manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
    registry = {"project": {"output_folder": "project", "run_type": "bundle"}}

    rows = _union_rows_for(tmp_path, manifest, registry, domain)

    assert {row["inventory_status"] for row in rows} == {"missing_domain_patterns"}
    assert {row["source_status"] for row in rows} == {"ok"}


def test_pattern_reuse_many_files_gets_broad_classification():
    rows = [
        {
            "view_scope": "used", "governance_role": "Project", "client_label": "Acme",
            "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
            "join_hash": "broad", "pattern_label": "Broad", "n_files_present": "4",
            "n_files_denominator": "5", "n_projects_present": "2", "n_projects_denominator": "2",
            "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
            "usage_interpretable": "true", "inventory_status": "ok",
        }
    ]

    out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")

    assert out[0]["reuse_bucket"] == "client_wide"
    assert out[0]["bucket_basis"] == "files_in_role_client_domain"
    assert out[0]["pct_files_present"] == "0.800000"


def test_pattern_reuse_one_file_gets_single_file_classification():
    rows = [
        {
            "view_scope": "all", "governance_role": "Project", "client_label": "Acme",
            "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
            "join_hash": "one", "pattern_label": "One", "n_files_present": "1",
            "n_files_denominator": "3", "n_projects_present": "1", "n_projects_denominator": "2",
            "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
            "usage_interpretable": "true", "inventory_status": "ok",
        }
    ]

    out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")

    assert out[0]["reuse_bucket"] == "single_file"
    assert out[0]["bucket_basis"] == "files_in_role_client_domain"


def test_project_used_view_uses_project_and_file_denominators_for_emerging_bucket():
    rows = [
        {
            "view_scope": "used", "governance_role": "Project", "client_label": "Acme",
            "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
            "join_hash": "multi", "pattern_label": "Multi", "n_files_present": "2",
            "n_files_denominator": "5", "n_projects_present": "2", "n_projects_denominator": "3",
            "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
            "usage_interpretable": "true", "inventory_status": "ok",
        }
    ]

    out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")

    assert out[0]["reuse_bucket"] == "emerging"
    assert out[0]["bucket_basis"] == "files_in_role_client_domain"
    assert out[0]["pct_files_present"] == "0.400000"
    assert out[0]["pct_projects_present"] == "0.666667"


def test_single_project_reuse_takes_precedence_over_emerging():
    rows = [
        {
            "view_scope": "used", "governance_role": "Project", "client_label": "Acme",
            "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
            "join_hash": "single_project", "pattern_label": "Single Project",
            "n_files_present": "2", "n_files_denominator": "5",
            "n_projects_present": "1", "n_projects_denominator": "3",
            "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
            "usage_interpretable": "true", "inventory_status": "ok",
        }
    ]

    out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")

    assert out[0]["reuse_bucket"] == "single_project"
    assert out[0]["bucket_basis"] == "projects_in_client_domain"


def test_missing_source_identity_degrades_reuse_classification():
    rows = [
        {
            "view_scope": "all", "governance_role": "Project", "client_label": "Acme",
            "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
            "join_hash": "partial", "pattern_label": "Partial",
            "n_files_present": "4", "n_files_denominator": "4",
            "n_projects_present": "2", "n_projects_denominator": "2",
            "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
            "usage_interpretable": "true", "inventory_status": "ok",
            "source_status": "missing_source_cluster_id",
        }
    ]

    out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")

    assert out[0]["reuse_bucket"] == "unclassified"
    assert out[0]["bucket_basis"] == "source_status"
    assert out[0]["classification_status"] == "degraded_missing_source_cluster_id"


def test_template_all_view_is_not_interpreted_as_active_usage():
    rows = [
        {
            "view_scope": "all", "governance_role": "Template", "client_label": "Acme",
            "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
            "join_hash": "stock", "pattern_label": "Stock", "n_files_present": "1",
            "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1",
            "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
            "usage_interpretable": "false", "inventory_status": "ok",
        }
    ]

    out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")

    assert out[0]["usage_interpretable"] == "false"
    assert out[0]["reuse_bucket"] == "client_wide"


def test_reuse_zero_denominator_is_degraded_unclassified():
    bucket, basis, status = _reuse_bucket_for(
        n_files=0, n_files_den=0, n_projects=0, n_projects_den=0, n_clients=0, n_clients_den=0
    )

    assert bucket == "unclassified"
    assert basis == "denominator_unavailable"
    assert status == "degraded_zero_denominator"


def test_reuse_distribution_order_is_deterministic():
    rows = [
        {
            "view_scope": "all", "governance_role": "Project", "client_label": "Acme",
            "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
            "join_hash": jh, "pattern_label": jh, "n_files_present": "1",
            "n_files_denominator": "2", "n_projects_present": "1", "n_projects_denominator": "1",
            "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
            "usage_interpretable": "true", "inventory_status": "ok",
        }
        for jh in ["b", "a"]
    ]

    first = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")
    second = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")

    assert first == second
    assert [r["join_hash"] for r in first] == ["a", "b"]


def test_reuse_thresholds_are_centralized_and_used():
    assert "client_wide_min_pct_files" in REUSE_BUCKET_THRESHOLDS
    bucket, basis, status = _reuse_bucket_for(
        n_files=4, n_files_den=5, n_projects=1, n_projects_den=2, n_clients=1, n_clients_den=1
    )

    assert bucket == "client_wide"
    assert basis == "files_in_role_client_domain"
    assert status == "ok"


def test_explicit_matrices_union_jaccard_differs_from_mean_file_pair():
    from compare_cross_segment import build_explicit_matrix_outputs

    union_rows = [
        {"governance_role": "Project", "client_label": "A", "discipline_label": "Arch", "unit_system": "imperial", "domain": "d", "view_scope": "all", "join_hash": j, "n_files_present": "1", "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1", "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000", "inventory_status": "ok"}
        for j in ("x", "y")
    ] + [
        {"governance_role": "Project", "client_label": "B", "discipline_label": "Arch", "unit_system": "imperial", "domain": "d", "view_scope": "all", "join_hash": j, "n_files_present": "1", "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1", "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000", "inventory_status": "ok"}
        for j in ("x", "y")
    ]
    summary = [{
        "governance_role_a": "Project", "governance_role_b": "Project",
        "client_label_a": "A", "client_label_b": "B",
        "discipline_label_a": "Arch", "discipline_label_b": "Arch", "unit_system": "imperial",
        "segment_label_a": "Project A", "segment_label_b": "Project B",
        "domain": "d", "all_pairwise_jaccard_mean": "0.000000", "used_pairwise_jaccard_mean": "",
    }]

    matrices, frag, manifest = build_explicit_matrix_outputs(summary, [], union_rows, "2026-06-22T00:00:00Z")

    union_ab = [r for r in matrices["project_union_jaccard_matrix.csv"] if r["row_id"] == "Project A" and r["column_id"] == "Project B"][0]
    pair_ab = [r for r in matrices["project_mean_file_pair_jaccard_matrix.csv"] if r["row_id"] == "Project A" and r["column_id"] == "Project B" and r["domain"] == "d"][0]
    assert union_ab["value"] == "1.000000"
    assert pair_ab["value"] == "0.000000"
    frag_ab = [r for r in frag if r["row_id"] == "Project A" and r["column_id"] == "Project B"][0]
    assert frag_ab["fragmentation_diagnostic"] == "1.000000"
    assert frag_ab["domain"] == "ALL_DOMAINS"
    assert [m["matrix_name"] for m in manifest] == sorted(m["matrix_name"] for m in manifest)


def test_fragmentation_diagnostic_uses_all_domains_file_pair_aggregate():
    from compare_cross_segment import build_explicit_matrix_outputs

    union_rows = []
    for client in ("A", "B"):
        for domain, hashes in {"d1": ["shared"], "d2": [f"{client}_unique"]}.items():
            for jh in hashes:
                union_rows.append({"governance_role": "Project", "client_label": client, "discipline_label": "Arch", "unit_system": "imperial", "domain": domain, "view_scope": "all", "join_hash": jh, "n_files_present": "1", "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1", "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000", "inventory_status": "ok"})
    summary = [
        {"governance_role_a": "Project", "governance_role_b": "Project", "client_label_a": "A", "client_label_b": "B", "discipline_label_a": "Arch", "discipline_label_b": "Arch", "unit_system": "imperial", "segment_label_a": "Project A", "segment_label_b": "Project B", "domain": "d2", "all_pairwise_jaccard_mean": "0.000000"},
        {"governance_role_a": "Project", "governance_role_b": "Project", "client_label_a": "A", "client_label_b": "B", "discipline_label_a": "Arch", "discipline_label_b": "Arch", "unit_system": "imperial", "segment_label_a": "Project A", "segment_label_b": "Project B", "domain": "d1", "all_pairwise_jaccard_mean": "1.000000"},
    ]

    matrices, frag, _ = build_explicit_matrix_outputs(summary, [], union_rows, "2026-06-22T00:00:00Z")

    aggregate = [r for r in matrices["project_mean_file_pair_jaccard_matrix.csv"] if r["row_id"] == "Project A" and r["column_id"] == "Project B" and r["domain"] == "ALL_DOMAINS"][0]
    assert aggregate["value"] == "0.500000"
    frag_ab = [r for r in frag if r["row_id"] == "Project A" and r["column_id"] == "Project B"][0]
    assert frag_ab["domain"] == "ALL_DOMAINS"
    assert frag_ab["exact_identity_overlap"] == "0.500000"


def test_density_similarity_uses_domain_density_vectors_not_containment():
    from compare_cross_segment import build_explicit_matrix_outputs

    union_rows = []
    for client, domains in {"A": {"d1": ["a"], "d2": ["b", "c"]}, "B": {"d1": ["x"], "d2": ["y", "z"]}}.items():
        for domain, hashes in domains.items():
            for jh in hashes:
                union_rows.append({"governance_role": "Project", "client_label": client, "discipline_label": "Arch", "unit_system": "imperial", "domain": domain, "view_scope": "all", "join_hash": jh, "n_files_present": "1", "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1", "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000", "inventory_status": "ok"})
    pooled = [{"governance_role": "Project", "segment_label": "A", "domain": "d1", "all_containment_focal_in_pool": "0.123456"}]

    matrices, _, _ = build_explicit_matrix_outputs([], pooled, union_rows, "2026-06-22T00:00:00Z")
    density_ab = [r for r in matrices["project_density_similarity_matrix.csv"] if r["row_id"] == "Project|A|Arch|imperial" and r["column_id"] == "Project|B|Arch|imperial"][0]
    pool_row = matrices["project_pool_containment_similarity_matrix.csv"][0]
    assert density_ab["metric"] == "density_similarity"
    assert density_ab["value"] == "1.000000"
    assert pool_row["metric"] == "pool_containment_similarity"
    assert pool_row["value"] == "0.123456"


def test_pool_matrix_keeps_pool_scopes_distinct_for_same_project():
    # A project can appear once per applicable pool_scope grain
    # (parent_sibling/bc/client). Different grains must land on distinct
    # matrix coordinates instead of colliding on identical
    # (row_id, col_id, view, domain) with different values.
    from compare_cross_segment import build_explicit_matrix_outputs

    pooled = [
        {
            "governance_role": "Project", "segment_label": "A", "domain": "d1",
            "pool_scope": "parent_sibling",
            "all_containment_focal_in_pool": "0.111111",
            "used_containment_focal_in_pool": "",
        },
        {
            "governance_role": "Project", "segment_label": "A", "domain": "d1",
            "pool_scope": "bc",
            "all_containment_focal_in_pool": "0.222222",
            "used_containment_focal_in_pool": "",
        },
        {
            "governance_role": "Project", "segment_label": "A", "domain": "d1",
            "pool_scope": "client",
            "all_containment_focal_in_pool": "0.333333",
            "used_containment_focal_in_pool": "",
        },
    ]

    matrices, _, _ = build_explicit_matrix_outputs([], pooled, [], "2026-07-13T00:00:00Z")
    rows = [
        r for r in matrices["project_pool_containment_similarity_matrix.csv"]
        if r["row_id"] == "A" and r["view_scope"] == "all" and r["domain"] == "d1"
    ]

    coords = {(r["row_id"], r["column_id"], r["view_scope"], r["domain"]) for r in rows}
    assert len(coords) == len(rows) == 3
    by_col = {r["column_id"]: r["value"] for r in rows}
    assert by_col["peer_pool:parent_sibling:A"] == "0.111111"
    assert by_col["peer_pool:bc:A"] == "0.222222"
    assert by_col["peer_pool:client:A"] == "0.333333"


def test_fragmentation_diagnostic_unavailable_without_required_inputs():
    from compare_cross_segment import build_explicit_matrix_outputs

    _, frag, _ = build_explicit_matrix_outputs([], [], [], "2026-06-22T00:00:00Z")

    assert frag == [{
        "matrix_name": "project_fragmentation_diagnostic.csv",
        "row_id": "unavailable",
        "column_id": "unavailable",
        "view_scope": "unavailable",
        "domain": "ALL_DOMAINS",
        "footprint_similarity": "",
        "exact_identity_overlap": "",
        "fragmentation_diagnostic": "",
        "value_status": "unavailable_required_inputs",
        "interpretation": "Requires both union_jaccard and mean_file_pair_jaccard inputs.",
        "executed_utc": "2026-06-22T00:00:00Z",
    }]


def test_non_project_union_inventory_blocks_project_union_matrices():
    from compare_cross_segment import build_explicit_matrix_outputs

    union_rows = [{
        "governance_role": "Template",
        "client_label": "A",
        "discipline_label": "Arch",
        "unit_system": "imperial",
        "domain": "d",
        "view_scope": "all",
        "join_hash": "template_only",
        "n_files_present": "1",
        "n_files_denominator": "1",
        "n_projects_present": "1",
        "n_projects_denominator": "1",
        "n_clients_present": "1",
        "n_clients_denominator": "1",
        "pct_clients_present": "1.000000",
        "inventory_status": "ok",
    }]

    matrices, _, _ = build_explicit_matrix_outputs([], [], union_rows, "2026-06-22T00:00:00Z")

    assert matrices["project_union_jaccard_matrix.csv"][0]["value_status"] == "blocked_no_ok_project_union_inventory"
    assert matrices["project_density_similarity_matrix.csv"][0]["value_status"] == "blocked_no_ok_project_union_inventory"


def test_mean_file_pair_matrix_adds_synthetic_diagonal_cells():
    from compare_cross_segment import build_explicit_matrix_outputs

    summary = [{
        "governance_role_a": "Project",
        "governance_role_b": "Project",
        "segment_label_a": "Project A",
        "segment_label_b": "Project B",
        "domain": "d",
        "all_pairwise_jaccard_mean": "0.250000",
    }]

    matrices, _, _ = build_explicit_matrix_outputs(summary, [], [], "2026-06-22T00:00:00Z")
    rows = matrices["project_mean_file_pair_jaccard_matrix.csv"]

    diagonal = [r for r in rows if r["row_id"] == "Project A" and r["column_id"] == "Project A" and r["domain"] == "ALL_DOMAINS"][0]
    assert diagonal["value"] == "1.000000"
    assert diagonal["value_status"] == "synthetic_self_comparison"
    assert diagonal["self_comparison"] == "true"


def test_mean_file_pair_diagonals_limited_to_project_observed_domains():
    from compare_cross_segment import build_explicit_matrix_outputs

    summary = [
        {"governance_role_a": "Project", "governance_role_b": "Project", "segment_label_a": "Project A", "segment_label_b": "Project B", "domain": "d1", "all_pairwise_jaccard_mean": "0.250000"},
        {"governance_role_a": "Project", "governance_role_b": "Project", "segment_label_a": "Project C", "segment_label_b": "Project D", "domain": "d2", "all_pairwise_jaccard_mean": "0.500000"},
    ]

    matrices, _, _ = build_explicit_matrix_outputs(summary, [], [], "2026-06-22T00:00:00Z")
    rows = matrices["project_mean_file_pair_jaccard_matrix.csv"]

    assert [r for r in rows if r["row_id"] == "Project A" and r["column_id"] == "Project A" and r["domain"] == "d1"]
    assert not [r for r in rows if r["row_id"] == "Project A" and r["column_id"] == "Project A" and r["domain"] == "d2"]
    assert [r for r in rows if r["row_id"] == "Project C" and r["column_id"] == "Project C" and r["domain"] == "d2"]
    assert not [r for r in rows if r["row_id"] == "Project C" and r["column_id"] == "Project C" and r["domain"] == "d1"]


def test_mean_file_pair_matrix_emits_symmetric_cells():
    from compare_cross_segment import build_explicit_matrix_outputs

    summary = [{
        "governance_role_a": "Project",
        "governance_role_b": "Project",
        "segment_label_a": "Project A",
        "segment_label_b": "Project B",
        "domain": "d",
        "all_pairwise_jaccard_mean": "0.250000",
    }]

    matrices, _, _ = build_explicit_matrix_outputs(summary, [], [], "2026-06-22T00:00:00Z")
    rows = matrices["project_mean_file_pair_jaccard_matrix.csv"]

    forward = [r for r in rows if r["row_id"] == "Project A" and r["column_id"] == "Project B" and r["domain"] == "ALL_DOMAINS"][0]
    reverse = [r for r in rows if r["row_id"] == "Project B" and r["column_id"] == "Project A" and r["domain"] == "ALL_DOMAINS"][0]
    assert forward["value"] == reverse["value"] == "0.250000"


def test_missing_union_inventory_blocks_union_matrix_with_explicit_status():
    from compare_cross_segment import build_explicit_matrix_outputs

    matrices, _, _ = build_explicit_matrix_outputs([], [], [], "2026-06-22T00:00:00Z")

    row = matrices["project_union_jaccard_matrix.csv"][0]
    assert row["value_status"] == "blocked_missing_union_inventory"
    assert row["value"] == ""


def test_matrix_manifest_and_diagonal_are_deterministic():
    from compare_cross_segment import build_explicit_matrix_outputs

    union_rows = [{"governance_role": "Project", "client_label": "A", "discipline_label": "Arch", "unit_system": "imperial", "domain": "d", "view_scope": "all", "join_hash": "x", "n_files_present": "1", "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1", "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000", "inventory_status": "ok"}]

    first = build_explicit_matrix_outputs([], [], union_rows, "2026-06-22T00:00:00Z")
    second = build_explicit_matrix_outputs([], [], union_rows, "2026-06-22T00:00:00Z")

    assert first == second
    diagonal = first[0]["project_union_jaccard_matrix.csv"][0]
    assert diagonal["row_id"] == diagonal["column_id"]
    assert diagonal["self_comparison"] == "true"
    assert diagonal["value"] == "1.000000"
    assert {"matrix_name", "governance_role", "view_scope", "source_file", "source_grain", "metric", "identity_unit", "aggregation_method", "interpretation", "known_limitations", "executed_utc"} == set(first[2][0])


def _write_within_project_segment(segments_root: Path, folder: str, domain: str, export_run_ids):
    base = segments_root / folder / "results" / "bundle_analysis" / "all" / domain
    _write_csv(
        base / "membership_matrix.csv",
        [{"export_run_id": eid} for eid in export_run_ids],
    )


def test_discover_within_project_na_spellings_do_not_group(tmp_path):
    # Mirrors test_discover_governance_chain_final_fallback_normalizes_na_spelling,
    # but for Mode D: unlike governance chain's canonical-blank merge, an
    # unassigned project_label must NOT let unrelated files collide into one
    # fake "project" — every NA spelling (and repeats of the same spelling)
    # must fall back to its own per-file singleton, so no within_project
    # pair is ever discovered for a segment where every file is NA-labeled.
    segments_root = tmp_path / "segments"
    domain = "line_patterns"
    _write_within_project_segment(
        segments_root, "mixed_na", domain,
        ["na_t", "na_c", "na_p", "na_dup1", "na_dup2"],
    )
    manifest = {"mixed_na": {}}
    registry = {"mixed_na": {"output_folder": "mixed_na", "run_type": "bundle"}}
    file_metadata = {
        "na_t": {"project_label": "__NOT_APPLICABLE__"},
        "na_c": {"project_label": "n/a"},
        "na_p": {"project_label": "NA"},
        # Same exact NA spelling repeated: pre-fix these collapsed into one
        # fake project too and must also NOT pair post-fix.
        "na_dup1": {"project_label": "__NOT_APPLICABLE__"},
        "na_dup2": {"project_label": "__NOT_APPLICABLE__"},
    }

    pairs = discover_within_project(manifest, registry, file_metadata, segments_root)

    assert ("mixed_na", "mixed_na", "within_project") not in pairs


def test_discover_within_project_real_shared_label_still_groups(tmp_path):
    segments_root = tmp_path / "segments"
    domain = "line_patterns"
    _write_within_project_segment(
        segments_root, "renown", domain,
        ["r1", "r2", "na_extra"],
    )
    manifest = {"renown": {}}
    registry = {"renown": {"output_folder": "renown", "run_type": "bundle"}}
    file_metadata = {
        "r1": {"project_label": "Renown"},
        "r2": {"project_label": "Renown"},
        "na_extra": {"project_label": "__NOT_APPLICABLE__"},
    }

    pairs = discover_within_project(manifest, registry, file_metadata, segments_root)

    assert ("renown", "renown", "within_project") in pairs
