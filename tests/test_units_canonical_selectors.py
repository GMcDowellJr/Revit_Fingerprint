# -*- coding: utf-8 -*-

import enum
import json

from core.hashing import make_hash
from core.join_key_builder import build_join_key_from_policy
from core.join_key_policy import get_domain_join_key_policy, load_join_key_policies
from core.record_v2 import ITEM_Q_OK, make_identity_item, serialize_identity_items
from domains.units import UNITS_SEMANTIC_KEYS, UNITS_DOC_SEMANTIC_KEYS, extract_units_doc
from validators.record_v2 import validate_record_v2


def _units_policy():
    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    return get_domain_join_key_policy(policies, "units")


def _domain_identity_registry_v2():
    with open("contracts/domain_identity_keys_v2.json", "r") as f:
        return json.load(f)


def test_units_join_selectors_and_sig_basis_are_distinct():
    # identity_basis.items is the canonical evidence superset for units.
    canonical_items = [
        make_identity_item("units.spec", "length", ITEM_Q_OK),
        make_identity_item("units.unit_type_id", "autodesk.unit.unit:feetFractionalInches-1.0.1", ITEM_Q_OK),
        make_identity_item("units.rounding_method", "nearest", ITEM_Q_OK),
        make_identity_item("units.accuracy", "0.125000000", ITEM_Q_OK),
        make_identity_item("units.symbol_type_id", "autodesk.unit.symbol:ft-1.0.1", ITEM_Q_OK),
    ]

    join_key, missing = build_join_key_from_policy(
        domain_policy=_units_policy(),
        identity_items=canonical_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        preserve_single_def_hash_passthrough=False,
    )

    assert missing == []
    assert join_key["keys_used"] == [
        "units.rounding_method",
        "units.spec",
        "units.unit_type_id",
    ]
    assert sorted([it["k"] for it in join_key["items"]]) == join_key["keys_used"]

    join_items = [it for it in canonical_items if it.get("k") in set(join_key["keys_used"])]
    assert join_key["join_hash"] == make_hash(serialize_identity_items(join_items))

    semantic_items = [it for it in canonical_items if it.get("k") in set(UNITS_SEMANTIC_KEYS)]
    sig_hash = make_hash(serialize_identity_items(semantic_items))

    # Pilot requirement: semantic hash should remain independent from join hash.
    assert sig_hash != join_key["join_hash"]


def test_units_boolean_formatting_flags_are_semantic():
    for key in (
        "units.use_default",
        "units.use_digit_grouping",
        "units.use_plus_prefix",
        "units.suppress_leading_zeros",
        "units.suppress_spaces",
        "units.suppress_trailing_zeros",
    ):
        assert key in UNITS_SEMANTIC_KEYS


class _FakeDecimalSymbol(enum.Enum):
    Dot = 0
    Comma = 1


class _FakeDigitGroupingSymbol(enum.Enum):
    Comma = 0
    Period = 1


class _FakeUnits(object):
    DecimalSymbol = _FakeDecimalSymbol.Dot
    DigitGroupingAmount = 3
    DigitGroupingSymbol = _FakeDigitGroupingSymbol.Comma


class _FakeDoc(object):
    def GetUnits(self):
        return _FakeUnits()


class _FakeDocUnitsUnreadable(object):
    def GetUnits(self):
        raise RuntimeError("boom")


def test_extract_units_doc_emits_exactly_one_populated_record():
    result = extract_units_doc(_FakeDoc(), ctx={})

    assert result["count"] == 1
    assert len(result["records"]) == 1
    assert result["debug_v2_blocked"] is False

    rec = result["records"][0]
    assert rec["record_id"] == "units:_doc"
    assert rec["domain"] == "units_doc"
    assert rec["status"] == "ok"
    assert rec["sig_hash"] is not None

    items_by_k = {it["k"]: it for it in rec["identity_basis"]["items"]}
    assert set(items_by_k.keys()) == set(UNITS_DOC_SEMANTIC_KEYS)
    for k, it in items_by_k.items():
        assert it["q"] == ITEM_Q_OK, "expected q=ok for {}".format(k)

    assert items_by_k["units_doc.decimal_symbol"]["v"] == "Dot"
    assert items_by_k["units_doc.digit_grouping_amount"]["v"] == "3"
    assert items_by_k["units_doc.digit_grouping_symbol"]["v"] == "Comma"

    violations = validate_record_v2(rec, _domain_identity_registry_v2())
    assert violations == []


def test_extract_units_doc_never_blocks_on_read_failure():
    result = extract_units_doc(_FakeDocUnitsUnreadable(), ctx={})

    assert result["count"] == 1
    assert result["debug_v2_blocked"] is False

    rec = result["records"][0]
    assert rec["status"] == "degraded"
    assert rec["sig_hash"] is not None
    for it in rec["identity_basis"]["items"]:
        assert it["q"] == "unreadable"


def test_units_doc_sig_hash_reads_allowed_items_from_ctx_sig_hash_policies_when_present():
    """D-040: units/units_doc resolve their sig_hash preimage key set from
    ctx["sig_hash_policies"] when present, falling back to
    UNITS_SEMANTIC_KEYS/UNITS_DOC_SEMANTIC_KEYS otherwise -- prove the ctx
    path actually drives the computed hash (not just the fallback)."""
    ctx = {
        "sig_hash_policies": {
            "domains": {
                "units_doc": {
                    "sig_hash_schema": "units_doc.sig_hash.v1",
                    "hash_alg": "md5_utf8_join_pipe",
                    "allowed_items": ["units_doc.digit_grouping_amount"],
                    "allowed_item_prefixes": [],
                    "required_items": [],
                    "minima": {"block_if_any_required_not_ok": False},
                }
            }
        }
    }
    rec = extract_units_doc(_FakeDoc(), ctx=ctx)["records"][0]

    narrowed_item = [it for it in rec["identity_basis"]["items"] if it["k"] == "units_doc.digit_grouping_amount"]
    assert rec["sig_hash"] == make_hash(serialize_identity_items(narrowed_item))

    default_rec = extract_units_doc(_FakeDoc(), ctx={})["records"][0]
    assert rec["sig_hash"] != default_rec["sig_hash"]
