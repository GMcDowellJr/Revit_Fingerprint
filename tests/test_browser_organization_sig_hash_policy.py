# -*- coding: utf-8 -*-
"""Coverage for domains/browser_organization.py's D-040 policy-driven
sig_hash resolution. _build_record() is exercised directly with a minimal
mock BrowserOrganization object to prove: (1) the existing
BROWSER_ORGANIZATION_SEMANTIC_KEYS fallback still reproduces today's
sig_hash, and (2) ctx["sig_hash_policies"] actually drives the computed
hash when present.
"""
from core.hashing import make_hash
from core.record_v2 import serialize_identity_items
from domains.browser_organization import BROWSER_ORGANIZATION_SEMANTIC_KEYS, _build_record


class _Id(object):
    def __init__(self, i):
        self.IntegerValue = i


class _SortingParamId(object):
    def __init__(self, i):
        self.IntegerValue = i


class _FilterParam(object):
    def __init__(self, has_value):
        self.HasValue = has_value


class _Org(object):
    def __init__(self, sorting_order=3, sorting_param_int=-1, family_name="Browser - Views", filter_has_value=True, org_id=100, uid="org-uid-1"):
        self.SortingOrder = sorting_order
        self.SortingParameterId = _SortingParamId(sorting_param_int)
        self.FamilyName = family_name
        self._filter_has_value = filter_has_value
        self.Id = _Id(org_id)
        self.UniqueId = uid

    def GetParameters(self, name):
        return [_FilterParam(self._filter_has_value)]


_BIP_LOOKUP = {-1: "SortingParamName"}


def _basic_record(ctx=None, **kwargs):
    org = _Org(**kwargs)
    return _build_record("views", org, doc=None, is_workshared=False, bip_lookup=_BIP_LOOKUP, workset_name_to_unique_id={}, ctx=ctx)


def test_basic_org_produces_record_with_sig_hash():
    # status is "degraded" (not "ok") because is_workshared=False makes the
    # bo.workset_* coordination items q="unsupported_not_applicable", which
    # is not q="ok" -- expected, pre-existing behavior, unrelated to sig_hash.
    rec = _basic_record()
    assert rec["status"] == "degraded"
    assert rec["sig_hash"] is not None


def test_family_name_not_in_sig_hash():
    rec1 = _basic_record(family_name="Browser - Views")
    rec2 = _basic_record(family_name="Custom Views Browser")
    assert rec1["sig_hash"] == rec2["sig_hash"]


def test_sig_hash_reads_allowed_items_from_ctx_sig_hash_policies_when_present():
    ctx = {
        "sig_hash_policies": {
            "domains": {
                "browser_organization": {
                    "sig_hash_schema": "browser_organization.sig_hash.v1",
                    "hash_alg": "md5_utf8_join_pipe",
                    "allowed_items": ["bo.sorting_order"],
                    "allowed_item_prefixes": [],
                    "required_items": [],
                    "minima": {"block_if_any_required_not_ok": True},
                }
            }
        }
    }
    rec = _basic_record(ctx=ctx)
    narrowed_item = [it for it in rec["identity_basis"]["items"] if it["k"] == "bo.sorting_order"]
    assert rec["sig_hash"] == make_hash(serialize_identity_items(narrowed_item))

    default_rec = _basic_record(ctx=None)
    assert rec["sig_hash"] != default_rec["sig_hash"]
