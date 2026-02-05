# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.record_v2 import serialize_identity_items


def test_phase_filters_selectors_and_hashing_use_canonical_identity_items():
    # Canonical evidence superset for this pilot.
    canonical_items = sorted(
        [
            {"k": "phase_filter.name", "q": "ok", "v": "Show Complete"},
            {"k": "phase_filter.new.presentation_id", "q": "ok", "v": 2},
            {"k": "phase_filter.existing.presentation_id", "q": "ok", "v": 1},
            {"k": "phase_filter.demolished.presentation_id", "q": "ok", "v": 0},
            {"k": "phase_filter.temporary.presentation_id", "q": "ok", "v": 3},
        ],
        key=lambda it: it["k"],
    )

    policy = {
        "join_key_schema": "phase_filters.join_key.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "required_items": ["phase_filter.name"],
        "optional_items": [],
        "explicitly_excluded_items": [],
    }

    join_key, missing = build_join_key_from_policy(
        domain_policy=policy,
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    assert join_key["keys_used"] == ["phase_filter.name"]
    assert [it["k"] for it in join_key["items"]] == ["phase_filter.name"]

    # join_hash is recomputed from canonical evidence + selector keys_used.
    join_items = [it for it in canonical_items if it["k"] in set(join_key["keys_used"])]
    join_preimage = serialize_identity_items(join_items)
    assert join_key["join_hash"] == make_hash(join_preimage)

    semantic_keys = sorted(
        [
            "phase_filter.demolished.presentation_id",
            "phase_filter.existing.presentation_id",
            "phase_filter.new.presentation_id",
            "phase_filter.temporary.presentation_id",
        ]
    )
    semantic_items = [it for it in canonical_items if it["k"] in set(semantic_keys)]
    sig_hash = make_hash(serialize_identity_items(semantic_items))

    # Semantic basis is distinct from join basis in this pilot.
    assert sig_hash != join_key["join_hash"]
