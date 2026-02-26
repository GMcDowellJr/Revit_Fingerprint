# -*- coding: utf-8 -*-

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import load_join_key_policies, get_domain_join_key_policy
from core.record_v2 import make_identity_item, serialize_identity_items
from domains.line_patterns import _line_pattern_segments_def_hash, _line_pattern_segments_norm_hash


class _Seg(object):
    def __init__(self, seg_type, length):
        self.Type = seg_type
        self.Length = length


def _line_patterns_policy():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, "line_patterns")


def test_line_patterns_canonical_evidence_selectors_and_hashing():
    segments = [_Seg(0, 1.25), _Seg(2, 999.0)]
    segments_def_hash_v, segments_def_hash_q = _line_pattern_segments_def_hash(segments=segments)
    segments_norm_hash_v, segments_norm_hash_q = _line_pattern_segments_norm_hash(segments=segments)

    # Canonical evidence superset (identity_basis.items) includes both semantic and optional detail.
    canonical_items = [
        make_identity_item("line_pattern.segment_count", "2", "ok"),
        make_identity_item("line_pattern.seg[000].kind", "0", "ok"),
        make_identity_item("line_pattern.seg[000].length", "1.250000000", "ok"),
        make_identity_item("line_pattern.seg[001].kind", "2", "ok"),
        make_identity_item("line_pattern.seg[001].length", "0.000000000", "ok"),
        make_identity_item("line_pattern.segments_def_hash", segments_def_hash_v, segments_def_hash_q),
        make_identity_item("line_pattern.segments_norm_hash", segments_norm_hash_v, segments_norm_hash_q),
    ]

    join_key, missing = build_join_key_from_policy(
        domain_policy=_line_patterns_policy(),
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    assert join_key["keys_used"] == ["line_pattern.segments_norm_hash"]

    hashed_items = [
        it for it in canonical_items if it.get("k") in set(join_key["keys_used"])
    ]
    assert join_key["join_hash"] == make_hash(serialize_identity_items(hashed_items))

    semantic_keys = ["line_pattern.segment_count", "line_pattern.segments_def_hash", "line_pattern.segments_norm_hash"]
    semantic_items = [it for it in canonical_items if it.get("k") in set(semantic_keys)]
    sig_hash = make_hash(serialize_identity_items(semantic_items))

    assert sig_hash != join_key["join_hash"]


def test_line_pattern_segments_norm_hash_scale_invariant():
    segs_a = [_Seg(0, 1.0), _Seg(1, 0.5)]
    segs_b = [_Seg(0, 2.0), _Seg(1, 1.0)]
    segs_diff = [_Seg(1, 1.0), _Seg(0, 0.5)]

    a_v, a_q = _line_pattern_segments_norm_hash(segments=segs_a)
    b_v, b_q = _line_pattern_segments_norm_hash(segments=segs_b)
    d_v, d_q = _line_pattern_segments_norm_hash(segments=segs_diff)

    assert a_q == "ok"
    assert b_q == "ok"
    assert d_q == "ok"
    assert a_v == b_v
    assert d_v != a_v


def test_line_pattern_segments_norm_hash_all_dots():
    segs = [_Seg(2, 999.0), _Seg(2, 999.0)]
    v, q = _line_pattern_segments_norm_hash(segments=segs)
    assert q == "ok"
    assert v is not None
    assert isinstance(v, str)
    assert len(v) == 32
