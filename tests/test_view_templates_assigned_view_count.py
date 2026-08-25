# -*- coding: utf-8 -*-
"""Coverage for domains/view_templates.py's D-040 promotion of
vt.assigned_view_count into identity_basis.items.

_append_assigned_view_count_cosmetic_item() is exercised directly (it's a
standalone helper shared by all 5 view_templates partitions) to prove: (1)
it still appends to phase2.cosmetic_items as before, (2) it now ALSO
appends to identity_basis.items (previously absent -- structurally
invisible to discover_hash_policy.py's pareto search), and (3) doing so
does not retroactively change rec["sig_hash"], since sig_hash is already
computed by the time this helper runs.

Note: the helper is always called via the `domains.view_templates` module
object (`m._append_assigned_view_count_cosmetic_item`) rather than imported
by name at module scope -- another test elsewhere in this suite leaves
`domains.view_templates` re-imported such that a name bound at collection
time can go stale relative to what `monkeypatch.setattr(m, ...)` patches on
the live module object.
"""
import importlib
import json

from core.hashing import make_hash
from core.record_v2 import ITEM_Q_OK, build_record_v2, make_identity_item, serialize_identity_items
from validators.record_v2 import validate_record_v2


class _Id(object):
    def __init__(self, i):
        self.IntegerValue = i

    def __eq__(self, other):
        return isinstance(other, _Id) and self.IntegerValue == other.IntegerValue

    def __hash__(self):
        return hash(self.IntegerValue)


class _View(object):
    def __init__(self, is_template, template_id):
        self.IsTemplate = is_template
        self.ViewTemplateId = template_id


class _Template(object):
    def __init__(self, elem_id):
        self.Id = _Id(elem_id)


def _base_rec(sig_hash="fixed-sig-hash-already-computed"):
    return {
        "sig_hash": sig_hash,
        "identity_basis": {"items": [{"k": "vt.some_field", "v": "1", "q": "ok"}]},
        "phase2": {"cosmetic_items": []},
    }


def _setup_module(monkeypatch, views):
    m = importlib.import_module("domains.view_templates")
    monkeypatch.setattr(m, "collect_instances", lambda *a, **k: views)
    return m


def test_assigned_view_count_appended_to_both_cosmetic_items_and_identity_basis(monkeypatch):
    template = _Template(elem_id=10)
    views = [_View(False, _Id(10)), _View(False, _Id(10)), _View(False, _Id(99)), _View(True, _Id(10))]
    m = _setup_module(monkeypatch, views)

    rec = _base_rec()
    m._append_assigned_view_count_cosmetic_item(rec, doc=None, v=template, ctx={})

    cosmetic_keys = {it["k"] for it in rec["phase2"]["cosmetic_items"]}
    assert "vt.assigned_view_count" in cosmetic_keys

    identity_keys = {it["k"] for it in rec["identity_basis"]["items"]}
    assert "vt.assigned_view_count" in identity_keys

    assigned_item = [it for it in rec["identity_basis"]["items"] if it["k"] == "vt.assigned_view_count"][0]
    assert assigned_item["v"] == "2"


def test_assigned_view_count_does_not_change_already_computed_sig_hash(monkeypatch):
    m = _setup_module(monkeypatch, [])
    rec = _base_rec(sig_hash="abc123")
    m._append_assigned_view_count_cosmetic_item(rec, doc=None, v=_Template(elem_id=1), ctx={})
    assert rec["sig_hash"] == "abc123"


def test_assigned_view_count_identity_items_stay_sorted(monkeypatch):
    m = _setup_module(monkeypatch, [])
    rec = _base_rec()
    rec["identity_basis"]["items"] = [
        {"k": "vt.zzz_last", "v": "1", "q": "ok"},
        {"k": "vt.aaa_first", "v": "1", "q": "ok"},
    ]
    m._append_assigned_view_count_cosmetic_item(rec, doc=None, v=_Template(elem_id=1), ctx={})
    keys = [it["k"] for it in rec["identity_basis"]["items"]]
    assert keys == sorted(keys)


def test_vt_assigned_view_count_passes_contract_validation_for_every_partition():
    # P1 Codex review finding on the D-042 PR: vt.assigned_view_count was
    # promoted into identity_basis.items (D-040) but never registered in
    # contracts/domain_identity_keys_v2.json's allowed_keys for any of the 5
    # view_templates_* partitions, so validate_record_v2() rejected every
    # real-export view-template record with
    # identity.key.not_allowed:vt.assigned_view_count. No test caught this
    # because no view_templates test called validate_record_v2() against the
    # real registry -- this closes that coverage gap.
    with open("contracts/domain_identity_keys_v2.json", "r") as f:
        registry = json.load(f)

    partitions = [
        "view_templates_floor_structural_area_plans",
        "view_templates_ceiling_plans",
        "view_templates_elevations_sections_detail",
        "view_templates_renderings_drafting",
        "view_templates_schedules",
    ]
    for domain in partitions:
        identity_items = sorted(
            [
                make_identity_item("view_template.def_hash", "a" * 32, ITEM_Q_OK),
                make_identity_item("vt.assigned_view_count", "3", ITEM_Q_OK),
            ],
            key=lambda it: it["k"],
        )
        rec = build_record_v2(
            domain=domain,
            record_id="test-view-template",
            status="ok",
            status_reasons=[],
            sig_hash=make_hash(serialize_identity_items(identity_items)),
            identity_items=identity_items,
            required_qs=[ITEM_Q_OK],
            label={"display": "Test Template", "quality": "human", "provenance": "computed.path", "components": {}},
        )
        assert validate_record_v2(rec, registry) == [], domain
