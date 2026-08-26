# -*- coding: utf-8 -*-

import importlib

try:
    import pytest
except ImportError:  # pragma: no cover
    pytest = None

from core.hashing import make_hash
from core.record_v2 import ITEM_Q_OK, ITEM_Q_MISSING, make_identity_item, serialize_identity_items
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import load_join_key_policies, get_domain_join_key_policy
from domains.arrowheads import (
    _build_common_identity_items,
    _build_arrow_identity_items,
    _build_tick_identity_items,
    _get_arrowhead_style,
)


# ---------------------------------------------------------------------------
# Minimal extract()-level mock harness (D-049).
#
# The tests above exercise the identity-item builder functions in isolation;
# the tests below exercise domains.arrowheads.extract() end to end to prove
# what actually lands in a real record's identity_basis.items/sig_basis --
# the builder functions alone can't show whether extract() *calls* them for
# a given style bucket.
# ---------------------------------------------------------------------------

class _Id(object):
    def __init__(self, i):
        self.IntegerValue = i


class _Param(object):
    def __init__(self, value_string=None, int_val=None, double_val=None, has_value=True):
        self._vs = value_string
        self._iv = int_val
        self._dv = double_val
        self.HasValue = has_value

    def AsValueString(self):
        return self._vs

    def AsInteger(self):
        return self._iv

    def AsDouble(self):
        return self._dv

    def AsString(self):
        return None


class _ArrowType(object):
    def __init__(self, name, params, elem_id=1):
        self.Name = name
        self._params = params
        self.Id = _Id(elem_id)
        self.UniqueId = "uid-{}".format(elem_id)

    def LookupParameter(self, name):
        return self._params.get(name)

    def get_Parameter(self, bip):
        return None


def _extract_one(monkeypatch, style, params, elem_id=1, name="Test Arrowhead"):
    m = importlib.import_module("domains.arrowheads")
    monkeypatch.setattr(m, "ElementType", object)
    full_params = {"Arrow Style": _Param(value_string=style)}
    full_params.update(params)
    t = _ArrowType(name=name, params=full_params, elem_id=elem_id)
    monkeypatch.setattr(m, "collect_types", lambda *a, **k: [t])
    monkeypatch.setattr(m, "_is_arrowhead_type", lambda doc, tt: True)
    out = m.extract(doc=None, ctx=None)
    return out["records"][0]


# Every style-specific field, with a params dict that gives each a genuine
# readable (q=ok) value, regardless of which style bucket "owns" it -- this
# is the scenario D-049 fixes: previously these were discarded outright for
# non-owning buckets instead of merely excluded from the hash.
_ALL_STYLE_SPECIFIC_PARAMS = {
    "Tick Size": _Param(double_val=0.0208333),
    "Arrow Width Angle": _Param(double_val=0.5236, value_string="30.00°"),
    "Fill Tick": _Param(int_val=1),
    "Arrow Closed": _Param(int_val=1),
    "Tick Mark Centered": _Param(int_val=1),
    "Heavy End Pen Weight": _Param(int_val=2),
}

_GATED_KEYS = frozenset({
    "arrowhead.width_angle_deg",
    "arrowhead.fill_tick",
    "arrowhead.arrow_closed",
    "arrowhead.tick_mark_centered",
    "arrowhead.heavy_end_pen_weight",
})


def test_style_discriminator_first():
    common = _build_common_identity_items(
        style_v="Arrow",
        style_q=ITEM_Q_OK,
        tick_in_v="0.25",
        tick_in_q=ITEM_Q_OK,
    )
    assert common[0]["k"] == "arrowhead.style"
    assert common[0]["v"] == "Arrow"


def test_style_specific_keys_are_omitted_when_not_applicable():
    # This exercises _build_tick_identity_items() in isolation, which by
    # construction never emits arrow-specific keys -- true independent of
    # D-049 (extract() below now calls both builders unconditionally for
    # every record; see test_style_specific_keys_are_no_longer_omitted_from_identity_items).
    common = _build_common_identity_items(
        style_v="Tick",
        style_q=ITEM_Q_OK,
        tick_in_v="0.25",
        tick_in_q=ITEM_Q_OK,
    )
    tick_specific = _build_tick_identity_items(
        centered_v="true",
        centered_q=ITEM_Q_OK,
        pen_v="2",
        pen_q=ITEM_Q_OK,
    )
    keys = [it["k"] for it in (common + tick_specific)]
    assert "arrowhead.width_angle_deg" not in keys
    assert "arrowhead.fill_tick" not in keys
    assert "arrowhead.arrow_closed" not in keys


def test_style_specific_keys_are_no_longer_omitted_from_identity_items(monkeypatch):
    # D-049 (inverts the old assumption behind the test above, at the
    # extract() level where it actually matters): a SizeOnly-style record
    # (Dot) now carries every style-specific field in identity_basis.items,
    # with real q/v -- fill_tick genuinely varies on Dot in the field
    # ("Dot Filled-Small"), so silently dropping it was the bug.
    rec = _extract_one(monkeypatch, "Dot", _ALL_STYLE_SPECIFIC_PARAMS)
    keys = {it["k"]: it for it in rec["identity_basis"]["items"]}

    for k in _GATED_KEYS:
        assert k in keys, "{} missing from identity_basis.items for a Dot record".format(k)
    assert keys["arrowhead.fill_tick"]["q"] == ITEM_Q_OK
    assert keys["arrowhead.fill_tick"]["v"] == "true"

    # But none of them fed sig_hash -- SizeOnly owns no style-specific keys.
    assert not (_GATED_KEYS & set(rec["sig_basis"]["keys_used"]))


def test_gated_fields_only_hash_for_their_owning_style_bucket(monkeypatch):
    # Same fully-populated param set, three different styles -- only the
    # style-owning subset of the 5 gated keys should feed sig_hash in each
    # case, even though all 5 are always present in identity_basis.items.
    rec_arrow = _extract_one(monkeypatch, "Arrow", _ALL_STYLE_SPECIFIC_PARAMS, elem_id=1)
    rec_tick = _extract_one(monkeypatch, "Heavy end tick mark", _ALL_STYLE_SPECIFIC_PARAMS, elem_id=2)
    rec_size_only = _extract_one(monkeypatch, "Box", _ALL_STYLE_SPECIFIC_PARAMS, elem_id=3)

    for rec in (rec_arrow, rec_tick, rec_size_only):
        keys = {it["k"] for it in rec["identity_basis"]["items"]}
        assert _GATED_KEYS.issubset(keys)

    assert set(rec_arrow["sig_basis"]["keys_used"]) & _GATED_KEYS == {
        "arrowhead.width_angle_deg", "arrowhead.fill_tick", "arrowhead.arrow_closed",
    }
    assert set(rec_tick["sig_basis"]["keys_used"]) & _GATED_KEYS == {
        "arrowhead.tick_mark_centered", "arrowhead.heavy_end_pen_weight",
    }
    assert set(rec_size_only["sig_basis"]["keys_used"]) & _GATED_KEYS == set()


def test_drift_guard_every_computed_field_reaches_identity_items(monkeypatch):
    # D-049 drift guard: every field extract() computes must reach
    # identity_basis.items for every style bucket, so a future
    # reintroduction of a per-bucket "class_items = []" discard (the exact
    # bug this PR fixes for fill_tick et al.) is caught immediately. Run
    # against every record-class bucket, including Unknown.
    for style in ("Arrow", "Heavy end tick mark", "Dot", "SomeFutureUnknownStyle"):
        rec = _extract_one(monkeypatch, style, _ALL_STYLE_SPECIFIC_PARAMS)
        keys = {it["k"] for it in rec["identity_basis"]["items"]}
        missing = _GATED_KEYS - keys
        assert not missing, "style={!r} is missing computed fields from identity_basis.items: {}".format(
            style, sorted(missing)
        )


def test_no_missing_for_unrelated_style_properties():
    common = _build_common_identity_items(
        style_v="Arrow",
        style_q=ITEM_Q_OK,
        tick_in_v="0.25",
        tick_in_q=ITEM_Q_OK,
    )
    arrow_specific = _build_arrow_identity_items(
        width_angle_v="45",
        width_angle_q=ITEM_Q_OK,
        fill_v="true",
        fill_q=ITEM_Q_OK,
        closed_v="false",
        closed_q=ITEM_Q_OK,
    )
    items = common + arrow_specific
    assert all(it["q"] != ITEM_Q_MISSING for it in items)


def test_join_key_builder_additional_required_only_for_shape():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    pol = get_domain_join_key_policy(policies, "arrowheads")

    items = [
        make_identity_item("arrowhead.style", "Tick", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_size_in", "0.25", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_mark_centered", "true", ITEM_Q_OK),
        make_identity_item("arrowhead.heavy_end_pen_weight", "2", ITEM_Q_OK),
    ]

    jk, missing = build_join_key_from_policy(domain_policy=pol, identity_items=items)
    assert "arrowhead.tick_mark_centered" not in missing
    assert "arrowhead.heavy_end_pen_weight" not in missing
    assert "arrowhead.width_angle_deg" not in missing
    assert jk["shape_gating"]["shape_value"] == "Tick"


def test_get_arrowhead_style_fallback():
    # String inputs are returned as-is (display strings are canonical).
    # The "Other" fallback only applies to unrecognized integer enum codes.
    style_v, style_q = _get_arrowhead_style(999, ITEM_Q_OK)
    assert style_v == "Other"
    assert style_q == ITEM_Q_OK


def test_join_key_builder_other_style():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    pol = get_domain_join_key_policy(policies, "arrowheads")

    items = [
        make_identity_item("arrowhead.style", "Other", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_size_in", "0.25", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_mark_centered", "true", ITEM_Q_OK),
        make_identity_item("arrowhead.heavy_end_pen_weight", "2", ITEM_Q_OK),
    ]

    jk, missing = build_join_key_from_policy(domain_policy=pol, identity_items=items)
    assert "arrowhead.tick_mark_centered" not in missing
    assert "arrowhead.heavy_end_pen_weight" not in missing
    assert jk["shape_gating"]["shape_value"] == "Other"


def test_join_key_keys_used_and_hash_for_arrow_style():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    pol = get_domain_join_key_policy(policies, "arrowheads")

    items = [
        make_identity_item("arrowhead.style", "Arrow", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_size_in", "0.25", ITEM_Q_OK),
        make_identity_item("arrowhead.width_angle_deg", "45", ITEM_Q_OK),
        make_identity_item("arrowhead.fill_tick", "true", ITEM_Q_OK),
        make_identity_item("arrowhead.arrow_closed", "false", ITEM_Q_OK),
        make_identity_item("arrowhead.tick_mark_centered", "true", ITEM_Q_OK),
        make_identity_item("arrowhead.heavy_end_pen_weight", "2", ITEM_Q_OK),
    ]

    jk, _missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
    )

    expected_keys_used = [
        "arrowhead.arrow_closed",
        "arrowhead.fill_tick",
        "arrowhead.style",
        "arrowhead.tick_size_in",
        "arrowhead.width_angle_deg",
    ]
    assert jk["keys_used"] == expected_keys_used

    item_keys = [it["k"] for it in jk["items"]]
    assert sorted(item_keys) == expected_keys_used
    assert "arrowhead.tick_mark_centered" not in item_keys
    assert "arrowhead.heavy_end_pen_weight" not in item_keys

    sig_hash = make_hash(serialize_identity_items(items))
    join_hash = jk["join_hash"]
    join_preimage = serialize_identity_items([it for it in items if it["k"] in jk["keys_used"]])
    assert join_hash == make_hash(join_preimage)
    assert join_hash != sig_hash
