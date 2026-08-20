# -*- coding: utf-8 -*-
"""Tests for the project_info.* identity items added to domains/identity.py (D-025)."""

import json

import pytest

import domains.identity as identity_module
from core.join_key_policy import load_join_key_policies
from core.record_v2 import ITEM_Q_MISSING, ITEM_Q_OK, ITEM_Q_UNREADABLE, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE


class FakeParameter:
    def __init__(self, value):
        self._value = value

    def AsString(self):
        return self._value

    def AsValueString(self):
        return self._value


class FakeBuiltInParameter:
    """Stand-in for Autodesk.Revit.DB.BuiltInParameter's enum members used here."""
    PROJECT_NUMBER = "BIP.PROJECT_NUMBER"
    PROJECT_STATUS = "BIP.PROJECT_STATUS"
    PROJECT_ADDRESS = "BIP.PROJECT_ADDRESS"
    PROJECT_ISSUE_DATE = "BIP.PROJECT_ISSUE_DATE"
    CLIENT_NAME = "BIP.CLIENT_NAME"
    PROJECT_BUILDING_NAME = "BIP.PROJECT_BUILDING_NAME"
    PROJECT_ORGANIZATION_NAME = "BIP.PROJECT_ORGANIZATION_NAME"
    PROJECT_ORGANIZATION_DESCRIPTION = "BIP.PROJECT_ORGANIZATION_DESCRIPTION"
    IFC_BUILDING_GUID = "BIP.IFC_BUILDING_GUID"
    IFC_PROJECT_GUID = "BIP.IFC_PROJECT_GUID"
    IFC_SITE_GUID = "BIP.IFC_SITE_GUID"


class FakeGuid:
    """Stand-in for System.Guid: equality/hash by string value."""

    def __init__(self, s):
        self.s = str(s)

    def __eq__(self, other):
        return isinstance(other, FakeGuid) and self.s == other.s

    def __hash__(self):
        return hash(self.s)

    def __repr__(self):
        return "FakeGuid({!r})".format(self.s)


@pytest.fixture(autouse=True)
def _system_guid_available(monkeypatch):
    """Normal extraction tests model production's required System.Guid surface."""
    monkeypatch.setattr(identity_module, "Guid", FakeGuid)


class FakeProjectInformation:
    """Fake ProjectInformation element.

    builtin_values: dict of BIP token -> raw string (all built-ins present & ok).
    named_present: set of shared/custom parameter display names that exist on
        this document (i.e. LookupParameter would find a Parameter object).
    named_values: dict of display name -> raw string (only consulted for names
        in named_present; a name present with value None simulates a defined-
        but-blank parameter).
    guid_values: dict of GUID string -> raw string, consulted when get_Parameter
        is called with a FakeGuid (i.e. the code took the GUID-based read path);
        a GUID absent from this dict simulates the shared parameter definition
        not being loaded on this document.
    """

    def __init__(self, name, builtin_values, named_present, named_values=None, guid_values=None):
        self.Name = name
        self._builtin_values = builtin_values
        self._named_present = named_present
        self._named_values = named_values or {}
        self._guid_values = guid_values or {}

    def get_Parameter(self, token):
        if isinstance(token, FakeGuid):
            if token.s not in self._guid_values:
                return None
            return FakeParameter(self._guid_values[token.s])
        if token not in self._builtin_values:
            return None
        return FakeParameter(self._builtin_values[token])

    def LookupParameter(self, name):
        if name not in self._named_present:
            return None
        return FakeParameter(self._named_values.get(name))


_ALL_BUILTIN_VALUES = {
    FakeBuiltInParameter.PROJECT_NUMBER: "2014351100",
    FakeBuiltInParameter.PROJECT_STATUS: "DD",
    FakeBuiltInParameter.PROJECT_ADDRESS: "123 Main St",
    FakeBuiltInParameter.PROJECT_ISSUE_DATE: "2026-08-10",
    FakeBuiltInParameter.CLIENT_NAME: "Acme Co",
    FakeBuiltInParameter.PROJECT_BUILDING_NAME: "Tower A",
    FakeBuiltInParameter.PROJECT_ORGANIZATION_NAME: "InternalEnterprise",
    FakeBuiltInParameter.PROJECT_ORGANIZATION_DESCRIPTION: "Architecture",
    FakeBuiltInParameter.IFC_BUILDING_GUID: "1AB2c3D4e5F6G7H8I9J0Kl",
    FakeBuiltInParameter.IFC_PROJECT_GUID: "1AB2c3D4e5F6G7H8I9J0Km",
    FakeBuiltInParameter.IFC_SITE_GUID: "1AB2c3D4e5F6G7H8I9J0Kn",
}


_BUSINESS_CENTER_SHARED_PARAM_GUID = "11111111-2222-4333-8444-555555555555"


def _enterprise_like_pi():
    return FakeProjectInformation(
        name="Test Project",
        builtin_values=_ALL_BUILTIN_VALUES,
        named_present={"Business Center"},
        named_values={"Business Center": "BusinessCenter01"},
        # Kept consistent with named_values so the fixture behaves the same
        # whether identity_module.Guid is available (GUID-based read) or not
        # (LookupParameter-by-name fallback).
        guid_values={_BUSINESS_CENTER_SHARED_PARAM_GUID: "BusinessCenter01"},
    )


def _non_enterprise_pi():
    # IFC GUID fields are true BuiltInParameter members (confirmed via
    # tools/archetype/bip_lookup.json), unrelated to firm identity -- they
    # still resolve from the same _ALL_BUILTIN_VALUES here. What actually
    # distinguishes a "non-InternalEnterprise" project is the absence of the Business Center
    # shared parameter definition (named_present stays empty).
    return FakeProjectInformation(
        name="Non-InternalEnterprise Project",
        builtin_values=_ALL_BUILTIN_VALUES,
        named_present=set(),
    )


_CONFIG_CTX = {
    "project_info_shared_parameters": [
        {
            "key": "project_info.business_center",
            "name": "Business Center",
            "guid": _BUSINESS_CENTER_SHARED_PARAM_GUID,
        }
    ]
}


def _extract_items(doc, ctx=_CONFIG_CTX):
    return identity_module._extract_project_info_items(doc, ctx=ctx)

_EXPECTED_KEYS = {
    "project_info.name",
    "project_info.number",
    "project_info.status",
    "project_info.address",
    "project_info.issue_date",
    "project_info.client_name",
    "project_info.building_name",
    "project_info.organization_name",
    "project_info.organization_description",
    "project_info.ifc_building_guid",
    "project_info.ifc_project_guid",
    "project_info.ifc_site_guid",
    "project_info.business_center",
}


def _as_dict(items):
    return {it["k"]: it for it in items}


class _DocForPI:
    """Minimal doc stand-in exposing only .ProjectInformation, for unit-level
    tests of _extract_project_info_items() (which reads doc.ProjectInformation
    internally rather than taking a ProjectInformation object directly)."""

    def __init__(self, project_information):
        self.ProjectInformation = project_information


class FakeDoc:
    def __init__(self, project_information):
        self.Title = "TestDoc"
        self.ProjectInformation = project_information
        self.IsWorkshared = False
        self.PathName = "C:/models/TestDoc.rvt"

        class _App:
            VersionNumber = "2025"
            VersionName = "Autodesk Revit 2025"
            VersionBuild = "25.0.0.123"

        self.Application = _App()


def test_extract_project_info_items_covers_exact_expected_keys(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    items = _extract_items(_DocForPI(_enterprise_like_pi()))
    keys = [it["k"] for it in items]
    assert set(keys) == _EXPECTED_KEYS
    assert len(keys) == len(set(keys)), "duplicate project_info.* keys emitted"


def test_no_deployment_configuration_emits_only_builtin_fields(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    items = _as_dict(identity_module._extract_project_info_items(_DocForPI(_enterprise_like_pi())))
    assert "project_info.business_center" not in items
    assert "project_info.client_name" in items


def test_malformed_configured_guid_is_rejected(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    ctx = {"project_info_shared_parameters": [
        {"key": "project_info.business_center", "name": "Business Center", "guid": "not-a-guid"}
    ]}
    import pytest
    with pytest.raises(ValueError, match="malformed GUID"):
        identity_module._extract_project_info_items(_DocForPI(_enterprise_like_pi()), ctx=ctx)


def test_extract_project_info_items_keys_are_registered_in_contract():
    with open("contracts/domain_identity_keys_v2.json") as f:
        registry = json.load(f)
    allowed = set(registry["domains"]["identity"]["allowed_keys"])
    assert _EXPECTED_KEYS.issubset(allowed)


def test_builtin_fields_resolve_ok_on_a_enterprise_like_project(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    items = _as_dict(_extract_items(_DocForPI(_enterprise_like_pi())))

    for key, expected_v in (
        ("project_info.name", "Test Project"),
        ("project_info.number", "2014351100"),
        ("project_info.status", "DD"),
        ("project_info.address", "123 Main St"),
        ("project_info.issue_date", "2026-08-10"),
        ("project_info.client_name", "Acme Co"),
        ("project_info.building_name", "Tower A"),
        ("project_info.organization_name", "InternalEnterprise"),
        ("project_info.organization_description", "Architecture"),
    ):
        assert items[key]["q"] == ITEM_Q_OK, "{} expected q=ok, got {}".format(key, items[key]["q"])
        assert items[key]["v"] == expected_v


def test_business_center_and_ifc_guids_resolve_ok_when_present(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    items = _as_dict(_extract_items(_DocForPI(_enterprise_like_pi())))

    assert items["project_info.business_center"]["q"] == ITEM_Q_OK
    assert items["project_info.business_center"]["v"] == "BusinessCenter01"
    assert items["project_info.ifc_building_guid"]["q"] == ITEM_Q_OK
    assert items["project_info.ifc_project_guid"]["q"] == ITEM_Q_OK
    assert items["project_info.ifc_site_guid"]["q"] == ITEM_Q_OK


def test_business_center_is_read_via_shared_parameter_guid_when_available(monkeypatch):
    """Business Center must be read via Element.get_Parameter(Guid), not LookupParameter
    by display name -- LookupParameter can resolve to an arbitrary same-named
    parameter if a project happens to contain more than one "Business Center" definition.
    Proven here by making LookupParameter("Business Center") resolve to nothing while the
    confirmed GUID resolves to a real value; only the GUID-based read succeeds."""
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    monkeypatch.setattr(identity_module, "Guid", FakeGuid)

    pi = FakeProjectInformation(
        name="GUID Business Center Project",
        builtin_values=_ALL_BUILTIN_VALUES,
        named_present=set(),  # no "Business Center" resolvable by display name
        guid_values={_BUSINESS_CENTER_SHARED_PARAM_GUID: "BusinessCenter01"},
    )
    items = _as_dict(_extract_items(_DocForPI(pi)))
    assert items["project_info.business_center"]["q"] == ITEM_Q_OK
    assert items["project_info.business_center"]["v"] == "BusinessCenter01"


def test_guid_configuration_fails_closed_when_guid_type_unavailable(monkeypatch):
    """A configured GUID never silently degrades to ambiguous name lookup."""
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    monkeypatch.setattr(identity_module, "Guid", None)

    with pytest.raises(RuntimeError, match="System.Guid"):
        _extract_items(_DocForPI(_enterprise_like_pi()))


def test_business_center_is_not_applicable_when_shared_param_absent_not_unreadable(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    items = _as_dict(_extract_items(_DocForPI(_non_enterprise_pi())))

    assert items["project_info.business_center"]["q"] == ITEM_Q_UNSUPPORTED_NOT_APPLICABLE, (
        "project_info.business_center should be legitimately-absent not_applicable on a project "
        "without the shared parameter loaded, not unreadable/missing"
    )
    assert items["project_info.business_center"]["v"] is None

    # Built-ins -- including the IFC GUID fields, which are true BuiltInParameter
    # members unrelated to Business Center/firm identity -- are unaffected by Business Center's
    # shared parameter definition being absent.
    assert items["project_info.client_name"]["q"] == ITEM_Q_OK
    assert items["project_info.ifc_building_guid"]["q"] == ITEM_Q_OK
    assert items["project_info.ifc_project_guid"]["q"] == ITEM_Q_OK
    assert items["project_info.ifc_site_guid"]["q"] == ITEM_Q_OK


def test_ifc_guid_builtins_follow_same_semantics_as_other_builtins(monkeypatch):
    """IFC GUID fields are true BuiltInParameter members (PR review follow-up),
    not shared/custom parameters -- so their absence semantics must match the
    other built-ins (missing Parameter object => unreadable, blank value =>
    missing), not the shared-parameter not_applicable semantics Business Center uses."""
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)

    blank_builtins = dict(_ALL_BUILTIN_VALUES)
    blank_builtins[FakeBuiltInParameter.IFC_BUILDING_GUID] = None
    del blank_builtins[FakeBuiltInParameter.IFC_PROJECT_GUID]  # simulate no Parameter object at all

    pi = FakeProjectInformation(
        name="Mixed IFC Project",
        builtin_values=blank_builtins,
        named_present=set(),
    )
    items = _as_dict(_extract_items(_DocForPI(pi)))

    assert items["project_info.ifc_building_guid"]["q"] == ITEM_Q_MISSING
    assert items["project_info.ifc_building_guid"]["v"] is None
    assert items["project_info.ifc_project_guid"]["q"] == ITEM_Q_UNREADABLE
    assert items["project_info.ifc_site_guid"]["q"] == ITEM_Q_OK


def test_named_field_present_but_blank_is_missing_not_not_applicable(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    pi = FakeProjectInformation(
        name="Blank Business Center Project",
        builtin_values=_ALL_BUILTIN_VALUES,
        named_present={"Business Center"},
        named_values={"Business Center": None},
        guid_values={_BUSINESS_CENTER_SHARED_PARAM_GUID: None},
    )
    items = _as_dict(_extract_items(_DocForPI(pi)))
    assert items["project_info.business_center"]["q"] == ITEM_Q_MISSING
    assert items["project_info.business_center"]["v"] is None


def test_builtin_field_unreadable_when_no_builtinparameter_enum(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", None)
    items = _as_dict(_extract_items(_DocForPI(_enterprise_like_pi())))
    assert items["project_info.client_name"]["q"] == ITEM_Q_UNREADABLE
    # project_info.name doesn't depend on BuiltInParameter (uses pi.Name directly).
    assert items["project_info.name"]["q"] == ITEM_Q_OK


def test_project_information_missing_marks_every_field_unreadable(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    items = _as_dict(_extract_items(_DocForPI(None)))
    assert set(items.keys()) == _EXPECTED_KEYS
    assert all(it["q"] == ITEM_Q_UNREADABLE for it in items.values())


def test_extract_end_to_end_includes_project_info_in_sig_hash_and_leaves_status_ok(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    doc = FakeDoc(_enterprise_like_pi())

    result = identity_module.extract(doc, ctx=_CONFIG_CTX)
    rec = result["records"][0]

    items_by_k = _as_dict(rec["identity_basis"]["items"])
    assert _EXPECTED_KEYS.issubset(items_by_k.keys())

    # Every project_info.* field is ok on this fully-populated InternalEnterprise-like fake.
    assert all(items_by_k[k]["q"] == ITEM_Q_OK for k in _EXPECTED_KEYS)

    # status/identity_quality remain governed by the pre-existing core items only.
    assert rec["status"] == "ok"
    assert rec["identity_quality"] == "complete"
    assert rec["sig_hash"] == result["sig_hash"]

    # sig_basis.keys_used must describe exactly what was hashed.
    assert set(rec["sig_basis"]["keys_used"]) == set(items_by_k.keys())


def test_phase2_semantic_keys_excludes_project_info_and_stays_the_pre_d025_core(monkeypatch):
    """phase2.semantic_keys ("Phase-2 behavior-defining") must stay decoupled
    from sig_basis.keys_used ("what sig_hash actually hashes") -- project_info.*
    is naming/label metadata included in sig_hash as an explicit D-025
    exception, not Phase-2-semantic content, and identity.revit_version_name
    must stay excluded exactly as it was pre-D-025 (PR review follow-up)."""
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    doc = FakeDoc(_enterprise_like_pi())

    result = identity_module.extract(doc, ctx=_CONFIG_CTX)
    rec = result["records"][0]

    assert rec["phase2"]["semantic_keys"] == [
        "identity.is_workshared",
        "identity.revit_build",
        "identity.revit_version_number",
    ]
    assert set(rec["phase2"]["semantic_keys"]).isdisjoint(_EXPECTED_KEYS)

    # sig_basis.keys_used, in contrast, DOES include every project_info.* key
    # (that's the actual D-025 hash-composition change).
    assert _EXPECTED_KEYS.issubset(set(rec["sig_basis"]["keys_used"]))
    assert "identity.revit_version_name" in rec["sig_basis"]["keys_used"]
    assert "identity.revit_version_name" not in rec["phase2"]["semantic_keys"]


def test_extract_end_to_end_non_enterprise_project_stays_status_ok(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    doc = FakeDoc(_non_enterprise_pi())

    result = identity_module.extract(doc, ctx=_CONFIG_CTX)
    rec = result["records"][0]

    items_by_k = _as_dict(rec["identity_basis"]["items"])
    assert items_by_k["project_info.business_center"]["q"] == ITEM_Q_UNSUPPORTED_NOT_APPLICABLE

    # A non-InternalEnterprise project legitimately lacking the shared parameter must not
    # degrade this domain's record status -- see D-025 / domains/identity.py comments.
    assert rec["status"] == "ok"
    assert rec["identity_quality"] == "complete"


def test_join_key_and_name_key_are_unaffected_by_project_info_fields(monkeypatch):
    """project_info.* is explicitly_excluded from the identity join-key policy;
    adding it to identity_items must not change join_key/join_key_name_identity
    at all, since neither is listed as required/optional there."""
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    doc = FakeDoc(_enterprise_like_pi())

    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    ctx = {"join_key_policies": policies, "name_key_policies": policies, **_CONFIG_CTX}

    result = identity_module.extract(doc, ctx=ctx)
    rec = result["records"][0]

    used_keys = set(
        rec["join_key"]["selectors"]["required_keys"] + rec["join_key"]["selectors"]["optional_keys"]
    )
    assert used_keys, "sanity: join_key should still resolve some keys"
    assert used_keys.isdisjoint(_EXPECTED_KEYS)

    name_used_keys = set(
        rec["join_key_name_identity"]["selectors"]["required_keys"]
        + rec["join_key_name_identity"]["selectors"]["optional_keys"]
    )
    assert name_used_keys.isdisjoint(_EXPECTED_KEYS)
