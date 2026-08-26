# -*- coding: utf-8 -*-
"""Coverage for D-049's P2 fix in tools/label_synthesis/domain_prompts/arrowheads.py.

build_identity_items_lookup.py picks one arbitrary representative record per
(domain, join_hash) group before this prompt ever runs. Since D-049 made
extraction unconditional, a style-specific field like fill_tick can now read
a real, genuine value on a SizeOnly (e.g. Dot) representative record even
though it isn't part of that style's join key -- other records sharing the
same join_hash may read differently for it, invisibly to this prompt. Naming
a whole join-hash group off one representative's non-join-key reading would
mislabel the rest of the group. _format_identity_items() must hard-exclude
any style-specific field that isn't part of the record's own class's join
key, so the synthesis LLM never even sees it for a non-owning class.
"""
from core.record_v2 import ITEM_Q_OK, ITEM_Q_MISSING, make_identity_item
from tools.label_synthesis.domain_prompts.arrowheads import build_prompt, _format_identity_items


def _dot_items_with_genuine_fill_tick():
    # Mirrors a real post-D-049 SizeOnly record: fill_tick reads q=ok/true
    # even though Dot's join key is style + tick_size_in only.
    return [
        make_identity_item("arrowhead.style", "Dot", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_size_in", "0.125000", ITEM_Q_OK),
        make_identity_item("arrowhead.width_angle_deg", None, ITEM_Q_MISSING),
        make_identity_item("arrowhead.fill_tick", "true", ITEM_Q_OK),
        make_identity_item("arrowhead.arrow_closed", None, ITEM_Q_MISSING),
        make_identity_item("arrowhead.tick_mark_centered", None, ITEM_Q_MISSING),
        make_identity_item("arrowhead.heavy_end_pen_weight", None, ITEM_Q_MISSING),
    ]


def _arrow_items():
    return [
        make_identity_item("arrowhead.style", "Arrow", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_size_in", "0.125000", ITEM_Q_OK),
        make_identity_item("arrowhead.width_angle_deg", "30", ITEM_Q_OK),
        make_identity_item("arrowhead.fill_tick", "true", ITEM_Q_OK),
        make_identity_item("arrowhead.arrow_closed", "true", ITEM_Q_OK),
    ]


def test_format_identity_items_excludes_non_owned_style_specific_key_even_when_ok():
    lines = _format_identity_items(_dot_items_with_genuine_fill_tick(), "SizeOnly")
    joined = "\n".join(lines)
    assert "Filled" not in joined
    assert any(l.startswith("Style:") for l in lines)
    assert any(l.startswith("Size") for l in lines)


def test_format_identity_items_includes_owned_style_specific_key():
    lines = _format_identity_items(_arrow_items(), "Arrow")
    joined = "\n".join(lines)
    assert "Filled: true" in joined


def test_build_prompt_never_surfaces_fill_tick_for_a_sizeonly_record():
    prompt = build_prompt(
        join_hash="a" * 32,
        observed_labels=[{"label_v": "Dot", "files_count": 3}],
        identity_items=_dot_items_with_genuine_fill_tick(),
    )
    assert "Filled" not in prompt
    assert "SizeOnly arrowhead style" in prompt


def test_build_prompt_surfaces_fill_tick_for_an_arrow_record():
    prompt = build_prompt(
        join_hash="b" * 32,
        observed_labels=[{"label_v": "Filled Arrow", "files_count": 2}],
        identity_items=_arrow_items(),
    )
    assert "Filled: true" in prompt
