# -*- coding: utf-8 -*-

import importlib
import sys
import types

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import serialize_identity_items


def _install_fake_revit_db():
    autodesk = types.ModuleType("Autodesk")
    revit = types.ModuleType("Autodesk.Revit")
    db = types.ModuleType("Autodesk.Revit.DB")

    class _T(object):
        pass

    db.ElementId = _T
    db.ElementParameterFilter = _T
    db.LogicalAndFilter = _T
    db.LogicalOrFilter = _T
    db.ParameterFilterElement = _T
    db.SharedParameterElement = _T

    autodesk.Revit = revit
    revit.DB = db

    sys.modules["Autodesk"] = autodesk
    sys.modules["Autodesk.Revit"] = revit
    sys.modules["Autodesk.Revit.DB"] = db


def test_view_filter_definitions_join_hash_uses_policy_required_keys_only():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    policy = get_domain_join_key_policy(policies, "view_filter_definitions")

    canonical_items = sorted(
        [
            {"k": "vf.logic_root", "q": "ok", "v": "Linear"},
            {"k": "vf.rule_count", "q": "ok", "v": 3},
            {"k": "vf.categories", "q": "ok", "v": "-2000011"},
            {"k": "vf.def_hash", "q": "ok", "v": "0123456789abcdef0123456789abcdef"},
        ],
        key=lambda it: it["k"],
    )

    join_key, missing = build_join_key_from_policy(
        domain_policy=policy,
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=True,
        emit_selectors=True,
    )

    assert missing == []
    if policy.get("join_key_schema") == "view_filter_definitions.join_key.v3":
        assert "vf.logic_root" in join_key["keys_used"]
        assert "vf.rule_count" in join_key["keys_used"]
        assert "vf.def_hash" not in join_key["keys_used"]
    else:
        assert join_key["keys_used"] == ["vf.def_hash"]

    join_items = [it for it in canonical_items if it["k"] in set(join_key["keys_used"])]
    join_preimage = serialize_identity_items(join_items)
    assert join_key["join_hash"] == make_hash(join_preimage)


def test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges(monkeypatch):
    _install_fake_revit_db()
    vfd = importlib.import_module("domains.view_filter_definitions")

    class FakeLeafRule(object):
        def __init__(self, token):
            self.token = token

    class FakeElementParameterFilter(object):
        def __init__(self, rules):
            self._rules = rules

        def GetRules(self):
            return self._rules

    class FakeFilterInverseRule(object):
        def __init__(self, inner):
            self._inner = inner

        def GetInnerRule(self):
            return self._inner

    monkeypatch.setattr(vfd, "ElementParameterFilter", FakeElementParameterFilter)
    monkeypatch.setattr(vfd, "FilterInverseRule", FakeFilterInverseRule)

    positive_rule = FakeLeafRule("rule_string_category_equals_walls")
    positive_filter = FakeElementParameterFilter([positive_rule])
    negated_filter = FakeFilterInverseRule(positive_rule)

    out_positive = []
    out_negated = []
    ok_pos, reason_pos = vfd._walk_rules(positive_filter, out_positive, doc=None)
    ok_neg, reason_neg = vfd._walk_rules(negated_filter, out_negated, doc=None)

    assert ok_pos is True and reason_pos is None
    assert ok_neg is True and reason_neg is None
    assert out_positive == [{"rule": positive_rule, "prefix": ""}]
    assert out_negated == [{"rule": positive_rule, "prefix": "NOT."}]

    pos_string = "{}{}".format(out_positive[0]["prefix"], positive_rule.token)
    neg_string = "{}{}".format(out_negated[0]["prefix"], positive_rule.token)
    assert neg_string == "NOT.rule_string_category_equals_walls"

    pos_sig_hash = make_hash(
        serialize_identity_items(
            [
                {"k": "vf.rule_count", "q": "ok", "v": 1},
                {"k": "vf.rule[000].sig", "q": "ok", "v": pos_string},
            ]
        )
    )
    neg_sig_hash = make_hash(
        serialize_identity_items(
            [
                {"k": "vf.rule_count", "q": "ok", "v": 1},
                {"k": "vf.rule[000].sig", "q": "ok", "v": neg_string},
            ]
        )
    )
    assert pos_sig_hash != neg_sig_hash
