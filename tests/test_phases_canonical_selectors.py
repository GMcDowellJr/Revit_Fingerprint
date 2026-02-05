# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items
from domains.phases import _phase2_build_phase2_payload


def _phases_policy():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, "phases")


def test_phases_join_hash_uses_policy_required_only_and_semantic_selector_is_separate():
    # Canonical evidence superset is identity_basis.items.
    canonical_items = [
        make_identity_item("phase.name", "Existing", ITEM_Q_OK),
        make_identity_item("phase.seq", "2", ITEM_Q_OK),
    ]

    join_key, missing = build_join_key_from_policy(
        domain_policy=_phases_policy(),
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    # phases policy: required=phase.name; optional=phase.seq
    assert join_key["keys_used"] == ["phase.name"]
    # Back-compat: retained join_key.items contains only hashed items.
    assert [it["k"] for it in join_key["items"]] == ["phase.name"]

    hashed_items = [
        it for it in canonical_items if it.get("k") in set(join_key["keys_used"])
    ]
    assert join_key["join_hash"] == make_hash(serialize_identity_items(hashed_items))

    # Semantic basis is declared via selector keys (not duplicated semantic_items k/q/v).
    phase2 = _phase2_build_phase2_payload(phase_name="Existing", seq=2, uid="uid-1")
    assert phase2["semantic_keys"] == ["phase.name", "phase.seq"]
    assert "semantic_items" not in phase2

    semantic_items = [it for it in canonical_items if it.get("k") in set(phase2["semantic_keys"])]
    sig_hash = make_hash(serialize_identity_items(semantic_items))
    assert sig_hash != join_key["join_hash"]
