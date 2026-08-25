# -*- coding: utf-8 -*-

import json

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import load_join_key_policies, get_domain_join_key_policy
from core.record_v2 import ITEM_Q_OK, build_record_v2, make_identity_item, serialize_identity_items
from domains.line_styles import LINE_STYLE_SEMANTIC_KEYS
from validators.record_v2 import validate_record_v2


def _line_styles_policy():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, "line_styles")


def test_line_styles_canonical_evidence_selectors_and_hashing():
    # Canonical evidence superset (identity_basis.items) includes join + semantic + cosmetic evidence.
    canonical_items = [
        make_identity_item("line_style.path", "Lines|Thin Lines", ITEM_Q_OK),
        make_identity_item("line_style.weight.projection", "1", ITEM_Q_OK),
        make_identity_item("line_style.color.rgb", "255-0-0", ITEM_Q_OK),
        make_identity_item("line_style.pattern_ref.kind", "ref", ITEM_Q_OK),
        make_identity_item("line_style.pattern_ref.sig_hash", "a" * 32, ITEM_Q_OK),
    ]

    join_key, missing = build_join_key_from_policy(
        domain_policy=_line_styles_policy(),
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    assert join_key["keys_used"] == [
        "line_style.color.rgb",
        "line_style.pattern_ref.sig_hash",
        "line_style.weight.projection",
    ]
    assert sorted([it["k"] for it in join_key["items"]]) == join_key["keys_used"]

    join_items = [it for it in canonical_items if it.get("k") in set(join_key["keys_used"])]
    assert join_key["join_hash"] == make_hash(serialize_identity_items(join_items))

    semantic_items = [it for it in canonical_items if it.get("k") in set(LINE_STYLE_SEMANTIC_KEYS)]
    sig_hash = make_hash(serialize_identity_items(semantic_items))

    # Semantic basis intentionally differs from policy join basis for this pilot.
    assert sig_hash != join_key["join_hash"]


def test_line_style_pattern_ref_kind_passes_contract_validation():
    # D-047 (P2 review finding on the D-046 PR): line_style.pattern_ref.kind is
    # part of LINE_STYLE_SEMANTIC_KEYS (the extractor's real inline sig_hash
    # preimage) and appears in every real line_styles record's
    # identity_basis.items, but contracts/domain_identity_keys_v2.json's
    # line_styles allowed_keys never included it -- validate_record_v2()
    # rejected every real-export line_styles record with
    # identity.key.not_allowed:line_style.pattern_ref.kind. No test in the
    # suite called validate_record_v2() against line_styles before this,
    # which is why this shipped unnoticed.
    with open("contracts/domain_identity_keys_v2.json", "r") as f:
        registry = json.load(f)

    identity_items = sorted(
        [
            make_identity_item("line_style.weight.projection", "2", ITEM_Q_OK),
            make_identity_item("line_style.color.rgb", "0-0-0", ITEM_Q_OK),
            make_identity_item("line_style.pattern_ref.sig_hash", "a" * 32, ITEM_Q_OK),
            make_identity_item("line_style.pattern_ref.kind", "ref", ITEM_Q_OK),
        ],
        key=lambda it: it["k"],
    )
    rec = build_record_v2(
        domain="line_styles",
        record_id="test-line-style",
        status="ok",
        status_reasons=[],
        sig_hash=make_hash(serialize_identity_items(
            [it for it in identity_items if it["k"] in set(LINE_STYLE_SEMANTIC_KEYS)]
        )),
        identity_items=identity_items,
        required_qs=[ITEM_Q_OK],
        label={"display": "Test Line Style", "quality": "human", "provenance": "computed.path", "components": {}},
    )
    rec["sig_basis"] = {"schema": "line_styles.sig_basis.v1", "keys_used": sorted(LINE_STYLE_SEMANTIC_KEYS)}
    assert validate_record_v2(rec, registry) == []


def test_line_styles_sig_hash_keys_override_matches_actual_inline_preimage():
    # D-047 drift guard: contracts/domain_identity_keys_v2.json's line_styles
    # sig_hash_keys override must keep matching LINE_STYLE_SEMANTIC_KEYS (the
    # extractor's own hardcoded inline preimage) -- if these ever diverge, a
    # future tools/generate_sig_hash_policy.py regen would silently produce a
    # compiled sig_hash policy that doesn't match what the extractor actually
    # hashes, recreating the exact class of bug this decision fixed.
    with open("contracts/domain_identity_keys_v2.json", "r") as f:
        registry = json.load(f)
    assert set(registry["domains"]["line_styles"]["sig_hash_keys"]) == set(LINE_STYLE_SEMANTIC_KEYS)
