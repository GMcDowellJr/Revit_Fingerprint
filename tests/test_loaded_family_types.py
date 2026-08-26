# -*- coding: utf-8 -*-
"""
Tests for domains/loaded_family_types.py's PR2/Tier 1 capability-flag additions
(lft.can_have_structural_section / lft.has_thermal_properties).

CanHaveStructuralSection/HasThermalProperties are zero-arg FamilySymbol *methods*
(confirmed via the Area 12 probe's reflection sweep -- audit_results/
audit_11_domain_extractor_delta_step0_findings.md §12 -- as method_kind="method",
never invoked because they weren't on the probe's method-invocation allowlist), so
there is no probe evidence either way on family-vs-symbol constancy. This file
treats them conservatively as per-symbol (type) capability queries, aggregated
across a family's types with the same any/all -> true/partial/false pattern
already used for lft.is_active -- not the read-off-first-symbol pattern used for
lft.structural_material_type (see the module's own comment at the call site).

There is no Autodesk.Revit.DB available in this test environment (extract()
short-circuits when FamilySymbol is None), so this file tests the new pure
helper functions directly (_safe_call, _aggregate_bool_pairs) plus contract
validation of hand-built identity_items -- the same pattern already used by
tests/test_object_styles_canonical_selectors.py for domain fields that don't
have a Revit-backed extract() test path.
"""

import json

from core.hashing import make_hash
from core.record_v2 import (
    ITEM_Q_MISSING,
    ITEM_Q_OK,
    ITEM_Q_UNREADABLE,
    build_record_v2,
    canonicalize_bool,
    make_identity_item,
    serialize_identity_items,
)
from domains.loaded_family_types import _aggregate_bool_pairs, _safe_call
from validators.record_v2 import validate_record_v2


# ---------------------------------------------------------------------------
# _safe_call
# ---------------------------------------------------------------------------

class _RaisesOnCall(object):
    def flaky_method(self):
        raise RuntimeError("boom")


class _ReturnsTrue(object):
    def flag_method(self):
        return True


def test_safe_call_invokes_zero_arg_method_and_returns_value():
    assert _safe_call(_ReturnsTrue(), "flag_method") is True


def test_safe_call_returns_default_when_method_missing():
    assert _safe_call(object(), "no_such_method") is None
    assert _safe_call(object(), "no_such_method", default="sentinel") == "sentinel"


def test_safe_call_returns_default_when_method_raises():
    assert _safe_call(_RaisesOnCall(), "flaky_method") is None
    assert _safe_call(_RaisesOnCall(), "flaky_method", default="sentinel") == "sentinel"


# ---------------------------------------------------------------------------
# _aggregate_bool_pairs -- true/false/partial/missing/unreadable states
# ---------------------------------------------------------------------------

def test_aggregate_bool_pairs_all_true_is_true():
    pairs = [canonicalize_bool(True), canonicalize_bool(True)]
    assert _aggregate_bool_pairs(pairs) == ("true", ITEM_Q_OK)


def test_aggregate_bool_pairs_all_false_is_false():
    pairs = [canonicalize_bool(False), canonicalize_bool(False)]
    assert _aggregate_bool_pairs(pairs) == ("false", ITEM_Q_OK)


def test_aggregate_bool_pairs_mixed_is_partial():
    pairs = [canonicalize_bool(True), canonicalize_bool(False)]
    assert _aggregate_bool_pairs(pairs) == ("partial", ITEM_Q_OK)


def test_aggregate_bool_pairs_any_unreadable_dominates():
    # canonicalize_bool(object()) -> (None, "unreadable")
    pairs = [canonicalize_bool(True), canonicalize_bool(object())]
    assert _aggregate_bool_pairs(pairs) == (None, ITEM_Q_UNREADABLE)


def test_aggregate_bool_pairs_unreadable_dominates_over_missing():
    pairs = [canonicalize_bool(None), canonicalize_bool(object())]
    assert _aggregate_bool_pairs(pairs) == (None, ITEM_Q_UNREADABLE)


def test_aggregate_bool_pairs_missing_without_unreadable_reports_missing():
    # A single missing read must not be silently dropped from the aggregate --
    # it must not fall through to the OK-only true/false/partial branch.
    pairs = [canonicalize_bool(True), canonicalize_bool(None)]
    assert _aggregate_bool_pairs(pairs) == (None, ITEM_Q_MISSING)


def test_aggregate_bool_pairs_empty_list_is_false_ok():
    # Defensive: an empty symbol group (should not occur in practice, since
    # fam_syms always has >=1 entry) must not raise on all()/any() over [].
    assert _aggregate_bool_pairs([]) == ("false", ITEM_Q_OK)


# ---------------------------------------------------------------------------
# Contract validation of the two new identity items
# ---------------------------------------------------------------------------

def _domain_identity_registry_v2():
    with open("contracts/domain_identity_keys_v2.json", "r") as f:
        return json.load(f)


def _base_identity_items(*, can_have_structural_section, has_thermal_properties):
    csec_v, csec_q = can_have_structural_section
    therm_v, therm_q = has_thermal_properties
    items = [
        make_identity_item("lft.shape_gate.category", "Structural Framing", ITEM_Q_OK),
        make_identity_item("lft.shape_gate.category_id", "-2001330", ITEM_Q_OK),
        make_identity_item("lft.type_parameter_schema_hash", "a" * 32, ITEM_Q_OK),
        make_identity_item("lft.type_parameter_count", "4", ITEM_Q_OK),
        make_identity_item("lft.family_is_in_place", "false", ITEM_Q_OK),
        make_identity_item("lft.family_is_editable", "true", ITEM_Q_OK),
        make_identity_item("lft.family_symbol_count", "3", ITEM_Q_OK),
        make_identity_item("lft.type_count", "3", ITEM_Q_OK),
        make_identity_item("lft.structural_material_type", "Steel", ITEM_Q_OK),
        make_identity_item("lft.is_active", "true", ITEM_Q_OK),
        make_identity_item("lft.can_have_structural_section", csec_v, csec_q),
        make_identity_item("lft.has_thermal_properties", therm_v, therm_q),
    ]
    # The validator requires identity_basis.items to already be sorted by k
    # (validators/record_v2.py's own serialize_identity_items() -- a separate,
    # non-sorting implementation from core/record_v2.py's -- documents "Input
    # MUST already be sorted by k; validator enforces this").
    return sorted(items, key=lambda it: it["k"])


def _build_and_validate(identity_items):
    registry = _domain_identity_registry_v2()
    sig_hash = make_hash(serialize_identity_items(identity_items))
    rec = build_record_v2(
        domain="loaded_family_types",
        record_id="Structural Framing|Wide Flange|" + "a" * 32,
        record_id_alg="loaded_family_types_composite_v1",
        record_id_scope="file_local",
        status="ok",
        status_reasons=[],
        sig_hash=sig_hash,
        identity_items=identity_items,
        required_qs=[ITEM_Q_OK, ITEM_Q_OK],
        label={
            "display": "Structural Framing : Wide Flange",
            "quality": "human",
            "provenance": "revit.FamilyName+Name",
            "components": {"category": "Structural Framing", "family_name": "Wide Flange"},
        },
    )
    return rec, validate_record_v2(rec, registry)


def test_can_have_structural_section_and_has_thermal_properties_pass_contract_validation_ok():
    identity_items = _base_identity_items(
        can_have_structural_section=("true", ITEM_Q_OK),
        has_thermal_properties=("false", ITEM_Q_OK),
    )
    rec, violations = _build_and_validate(identity_items)
    assert violations == []
    by_key = {it["k"]: it for it in rec["identity_basis"]["items"]}
    assert by_key["lft.can_have_structural_section"] == {"k": "lft.can_have_structural_section", "v": "true", "q": ITEM_Q_OK}
    assert by_key["lft.has_thermal_properties"] == {"k": "lft.has_thermal_properties", "v": "false", "q": ITEM_Q_OK}


def test_can_have_structural_section_and_has_thermal_properties_pass_contract_validation_missing():
    identity_items = _base_identity_items(
        can_have_structural_section=(None, ITEM_Q_MISSING),
        has_thermal_properties=(None, ITEM_Q_MISSING),
    )
    rec, violations = _build_and_validate(identity_items)
    assert violations == []
    by_key = {it["k"]: it for it in rec["identity_basis"]["items"]}
    assert by_key["lft.can_have_structural_section"]["v"] is None
    assert by_key["lft.can_have_structural_section"]["q"] == ITEM_Q_MISSING
    assert by_key["lft.has_thermal_properties"]["v"] is None
    assert by_key["lft.has_thermal_properties"]["q"] == ITEM_Q_MISSING


def test_can_have_structural_section_and_has_thermal_properties_pass_contract_validation_unreadable():
    identity_items = _base_identity_items(
        can_have_structural_section=(None, ITEM_Q_UNREADABLE),
        has_thermal_properties=(None, ITEM_Q_UNREADABLE),
    )
    rec, violations = _build_and_validate(identity_items)
    assert violations == []
    by_key = {it["k"]: it for it in rec["identity_basis"]["items"]}
    assert by_key["lft.can_have_structural_section"]["q"] == ITEM_Q_UNREADABLE
    assert by_key["lft.has_thermal_properties"]["q"] == ITEM_Q_UNREADABLE


def test_can_have_structural_section_and_has_thermal_properties_pass_contract_validation_partial():
    # true/partial/false aggregation state (any/all across symbols in the family).
    identity_items = _base_identity_items(
        can_have_structural_section=("partial", ITEM_Q_OK),
        has_thermal_properties=("partial", ITEM_Q_OK),
    )
    rec, violations = _build_and_validate(identity_items)
    assert violations == []
    by_key = {it["k"]: it for it in rec["identity_basis"]["items"]}
    assert by_key["lft.can_have_structural_section"]["v"] == "partial"
    assert by_key["lft.has_thermal_properties"]["v"] == "partial"


def test_new_items_are_registered_as_allowed_and_optional_not_required():
    registry = _domain_identity_registry_v2()
    block = registry["domains"]["loaded_family_types"]
    for key in ("lft.can_have_structural_section", "lft.has_thermal_properties"):
        assert key in block["allowed_keys"]
        assert key in block["optional"]
        assert key not in block["required_keys"]


def test_sig_hash_schema_bumped_for_hash_breaking_change():
    registry = _domain_identity_registry_v2()
    assert registry["domains"]["loaded_family_types"]["sig_hash_schema"] == "loaded_family_types.sig_hash.v3"
