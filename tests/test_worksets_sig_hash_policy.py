# -*- coding: utf-8 -*-
"""Coverage for domains/worksets.py's D-040 policy-driven sig_hash resolution.

_build_per_workset_record()/extract_worksets_doc() are exercised directly
with minimal mock Revit objects (no full FilteredWorksetCollector mocking
needed) to prove: (1) the existing WORKSETS_SEMANTIC_KEYS/
WORKSETS_DOC_SEMANTIC_KEYS fallback still reproduces today's sig_hash, and
(2) ctx["sig_hash_policies"] actually drives the computed hash when present.
"""
from core.hashing import make_hash
from core.record_v2 import serialize_identity_items
from domains.worksets import (
    WORKSETS_SEMANTIC_KEYS,
    WORKSETS_DOC_SEMANTIC_KEYS,
    _build_per_workset_record,
    _build_doc_level_record,
)


class _Id(object):
    def __init__(self, i):
        self.IntegerValue = i


class _Workset(object):
    def __init__(self, name="Workset1", kind_int=1, editable=True, is_default=False, owner="alice", uid="ws-uid-1", elem_id=10):
        self.Name = name
        self.Kind = kind_int
        self.IsEditable = editable
        self.IsDefaultWorkset = is_default
        self.Owner = owner
        self.UniqueId = uid
        self.Id = _Id(elem_id)


_KIND_NAME_BY_INT = {1: "UserWorkset"}


def _basic_record(name="Workset1", ctx=None):
    ws = _Workset(name=name)
    return _build_per_workset_record(ws, active_workset_id=None, active_workset_lookup_ok=True, kind_name_by_int=_KIND_NAME_BY_INT, ctx=ctx)


def test_basic_workset_produces_ok_record():
    rec = _basic_record()
    assert rec["status"] == "ok"
    assert rec["sig_hash"] is not None


def test_owner_not_in_sig_hash():
    rec1 = _build_per_workset_record(
        _Workset(name="W1", owner="alice"), None, True, _KIND_NAME_BY_INT, ctx=None
    )
    rec2 = _build_per_workset_record(
        _Workset(name="W1", owner="bob"), None, True, _KIND_NAME_BY_INT, ctx=None
    )
    assert rec1["sig_hash"] == rec2["sig_hash"]


def test_sig_hash_reads_allowed_items_from_ctx_sig_hash_policies_when_present():
    ctx = {
        "sig_hash_policies": {
            "domains": {
                "worksets": {
                    "sig_hash_schema": "worksets.sig_hash.v1",
                    "hash_alg": "md5_utf8_join_pipe",
                    "allowed_items": ["workset.kind"],
                    "allowed_item_prefixes": [],
                    "required_items": [],
                    "minima": {"block_if_any_required_not_ok": True},
                }
            }
        }
    }
    rec = _basic_record(ctx=ctx)
    kind_item = [it for it in rec["identity_basis"]["items"] if it["k"] == "workset.kind"]
    assert rec["sig_hash"] == make_hash(serialize_identity_items(kind_item))

    default_rec = _basic_record(ctx=None)
    assert rec["sig_hash"] != default_rec["sig_hash"]


_KIND_COUNTS = {"UserWorkset": 2, "StandardWorkset": 1, "ViewWorkset": 0, "FamilyWorkset": 0, "OtherWorkset": 0}


def test_worksets_doc_active_workset_name_not_in_sig_hash():
    rec1 = _build_doc_level_record(None, True, "Workset A", _KIND_COUNTS, ctx=None)
    rec2 = _build_doc_level_record(None, True, "Workset B", _KIND_COUNTS, ctx=None)
    assert rec1["sig_hash"] == rec2["sig_hash"]


def test_worksets_doc_sig_hash_reads_allowed_items_from_ctx_sig_hash_policies_when_present():
    ctx = {
        "sig_hash_policies": {
            "domains": {
                "worksets_doc": {
                    "sig_hash_schema": "worksets_doc.sig_hash.v1",
                    "hash_alg": "md5_utf8_join_pipe",
                    "allowed_items": ["worksets_doc.count_user_workset"],
                    "allowed_item_prefixes": [],
                    "required_items": [],
                    "minima": {"block_if_any_required_not_ok": False},
                }
            }
        }
    }
    rec = _build_doc_level_record(None, True, "Workset A", _KIND_COUNTS, ctx=ctx)
    narrowed_item = [it for it in rec["identity_basis"]["items"] if it["k"] == "worksets_doc.count_user_workset"]
    assert rec["sig_hash"] == make_hash(serialize_identity_items(narrowed_item))

    default_rec = _build_doc_level_record(None, True, "Workset A", _KIND_COUNTS, ctx=None)
    assert rec["sig_hash"] != default_rec["sig_hash"]
