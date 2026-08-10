# -*- coding: utf-8 -*-
"""Tests for the project_info.* identity items added to domains/identity.py (D-025)."""

import json

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


class FakeProjectInformation:
    """Fake ProjectInformation element.

    builtin_values: dict of BIP token -> raw string (all built-ins present & ok).
    named_present: set of shared/custom parameter display names that exist on
        this document (i.e. LookupParameter would find a Parameter object).
    named_values: dict of display name -> raw string (only consulted for names
        in named_present; a name present with value None simulates a defined-
        but-blank parameter).
    """

    def __init__(self, name, builtin_values, named_present, named_values=None):
        self.Name = name
        self._builtin_values = builtin_values
        self._named_present = named_present
        self._named_values = named_values or {}

    def get_Parameter(self, bip_token):
        if bip_token not in self._builtin_values:
            return None
        return FakeParameter(self._builtin_values[bip_token])

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
    FakeBuiltInParameter.PROJECT_ORGANIZATION_NAME: "Stantec",
    FakeBuiltInParameter.PROJECT_ORGANIZATION_DESCRIPTION: "Architecture",
}


def _stantec_like_pi():
    return FakeProjectInformation(
        name="Test Project",
        builtin_values=_ALL_BUILTIN_VALUES,
        named_present={"IfcBuilding GUID", "IfcProject GUID", "IfcSite GUID", "Office"},
        named_values={
            "IfcBuilding GUID": "1AB2c3D4e5F6G7H8I9J0Kl",
            "IfcProject GUID": "1AB2c3D4e5F6G7H8I9J0Km",
            "IfcSite GUID": "1AB2c3D4e5F6G7H8I9J0Kn",
            "Office": "Denver",
        },
    )


def _non_stantec_pi():
    return FakeProjectInformation(
        name="Non-Stantec Project",
        builtin_values=_ALL_BUILTIN_VALUES,
        named_present=set(),
    )


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
    "project_info.office",
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
    items = identity_module._extract_project_info_items(_DocForPI(_stantec_like_pi()))
    keys = [it["k"] for it in items]
    assert set(keys) == _EXPECTED_KEYS
    assert len(keys) == len(set(keys)), "duplicate project_info.* keys emitted"


def test_extract_project_info_items_keys_are_registered_in_contract():
    with open("contracts/domain_identity_keys_v2.json") as f:
        registry = json.load(f)
    allowed = set(registry["domains"]["identity"]["allowed_keys"])
    assert _EXPECTED_KEYS.issubset(allowed)


def test_builtin_fields_resolve_ok_on_a_stantec_like_project(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    items = _as_dict(identity_module._extract_project_info_items(_DocForPI(_stantec_like_pi())))

    for key, expected_v in (
        ("project_info.name", "Test Project"),
        ("project_info.number", "2014351100"),
        ("project_info.status", "DD"),
        ("project_info.address", "123 Main St"),
        ("project_info.issue_date", "2026-08-10"),
        ("project_info.client_name", "Acme Co"),
        ("project_info.building_name", "Tower A"),
        ("project_info.organization_name", "Stantec"),
        ("project_info.organization_description", "Architecture"),
    ):
        assert items[key]["q"] == ITEM_Q_OK, "{} expected q=ok, got {}".format(key, items[key]["q"])
        assert items[key]["v"] == expected_v


def test_office_and_ifc_guids_resolve_ok_when_present(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    items = _as_dict(identity_module._extract_project_info_items(_DocForPI(_stantec_like_pi())))

    assert items["project_info.office"]["q"] == ITEM_Q_OK
    assert items["project_info.office"]["v"] == "Denver"
    assert items["project_info.ifc_building_guid"]["q"] == ITEM_Q_OK
    assert items["project_info.ifc_project_guid"]["q"] == ITEM_Q_OK
    assert items["project_info.ifc_site_guid"]["q"] == ITEM_Q_OK


def test_office_and_ifc_guids_are_not_applicable_when_absent_not_unreadable(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    items = _as_dict(identity_module._extract_project_info_items(_DocForPI(_non_stantec_pi())))

    for key in (
        "project_info.office",
        "project_info.ifc_building_guid",
        "project_info.ifc_project_guid",
        "project_info.ifc_site_guid",
    ):
        assert items[key]["q"] == ITEM_Q_UNSUPPORTED_NOT_APPLICABLE, (
            "{} should be legitimately-absent not_applicable on a project without the "
            "shared parameter loaded, not unreadable/missing".format(key)
        )
        assert items[key]["v"] is None

    # Built-ins are unaffected by the shared parameter being absent.
    assert items["project_info.client_name"]["q"] == ITEM_Q_OK


def test_named_field_present_but_blank_is_missing_not_not_applicable(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    pi = FakeProjectInformation(
        name="Blank Office Project",
        builtin_values=_ALL_BUILTIN_VALUES,
        named_present={"Office"},
        named_values={"Office": None},
    )
    items = _as_dict(identity_module._extract_project_info_items(_DocForPI(pi)))
    assert items["project_info.office"]["q"] == ITEM_Q_MISSING
    assert items["project_info.office"]["v"] is None


def test_builtin_field_unreadable_when_no_builtinparameter_enum(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", None)
    items = _as_dict(identity_module._extract_project_info_items(_DocForPI(_stantec_like_pi())))
    assert items["project_info.client_name"]["q"] == ITEM_Q_UNREADABLE
    # project_info.name doesn't depend on BuiltInParameter (uses pi.Name directly).
    assert items["project_info.name"]["q"] == ITEM_Q_OK


def test_project_information_missing_marks_every_field_unreadable(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    items = _as_dict(identity_module._extract_project_info_items(_DocForPI(None)))
    assert set(items.keys()) == _EXPECTED_KEYS
    assert all(it["q"] == ITEM_Q_UNREADABLE for it in items.values())


def test_extract_end_to_end_includes_project_info_in_sig_hash_and_leaves_status_ok(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    doc = FakeDoc(_stantec_like_pi())

    result = identity_module.extract(doc, ctx=None)
    rec = result["records"][0]

    items_by_k = _as_dict(rec["identity_basis"]["items"])
    assert _EXPECTED_KEYS.issubset(items_by_k.keys())

    # Every project_info.* field is ok on this fully-populated Stantec-like fake.
    assert all(items_by_k[k]["q"] == ITEM_Q_OK for k in _EXPECTED_KEYS)

    # status/identity_quality remain governed by the pre-existing core items only.
    assert rec["status"] == "ok"
    assert rec["identity_quality"] == "complete"
    assert rec["sig_hash"] == result["sig_hash"]

    # sig_basis.keys_used must describe exactly what was hashed.
    assert set(rec["sig_basis"]["keys_used"]) == set(items_by_k.keys())


def test_extract_end_to_end_non_stantec_project_stays_status_ok(monkeypatch):
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    doc = FakeDoc(_non_stantec_pi())

    result = identity_module.extract(doc, ctx=None)
    rec = result["records"][0]

    items_by_k = _as_dict(rec["identity_basis"]["items"])
    assert items_by_k["project_info.office"]["q"] == ITEM_Q_UNSUPPORTED_NOT_APPLICABLE

    # A non-Stantec project legitimately lacking the shared parameter must not
    # degrade this domain's record status -- see D-025 / domains/identity.py comments.
    assert rec["status"] == "ok"
    assert rec["identity_quality"] == "complete"


def test_join_key_and_name_key_are_unaffected_by_project_info_fields(monkeypatch):
    """project_info.* is explicitly_excluded from the identity join-key policy;
    adding it to identity_items must not change join_key/join_key_name_identity
    at all, since neither is listed as required/optional there."""
    monkeypatch.setattr(identity_module, "BuiltInParameter", FakeBuiltInParameter)
    doc = FakeDoc(_stantec_like_pi())

    policies = load_join_key_policies("policies/domain_join_key_policies.json")
    ctx = {"join_key_policies": policies, "name_key_policies": policies}

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
