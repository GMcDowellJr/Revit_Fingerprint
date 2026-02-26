# -*- coding: utf-8 -*-

from tools.join_key_discovery.eval import build_candidate_join_key_with_details


def test_shape_gating_matches_bool_case_variants():
    identity_items_by_record = {
        "r1": {
            "view_template.sig.include_phase_filter": ("ok", "true"),
            "view_template.sig.phase_filter": ("ok", "abc"),
            "view_template.sig.include_filters": ("ok", "true"),
            "view_template.sig.include_vg": ("ok", "false"),
        }
    }
    gates = {
        "required_fields": [
            "view_template.sig.include_vg",
            "view_template.sig.include_filters",
            "view_template.sig.include_phase_filter",
        ],
        "discriminator_key": "view_template.sig.include_phase_filter",
        "shape_requirements": {
            "True": {"additional_required": ["view_template.sig.phase_filter"]}
        },
    }

    status, _selected, _reason, details = build_candidate_join_key_with_details(
        identity_items_by_record,
        "r1",
        selected_fields=[],
        gates=gates,
    )

    assert status == "ok"
    assert details["shape_matched"] is True
    assert "view_template.sig.phase_filter" in details["effective_required_fields"]


def test_shape_gating_does_not_require_phase_filter_for_false():
    identity_items_by_record = {
        "r1": {
            "view_template.sig.include_phase_filter": ("ok", "false"),
            "view_template.sig.include_filters": ("ok", "true"),
            "view_template.sig.include_vg": ("ok", "false"),
        }
    }
    gates = {
        "required_fields": [
            "view_template.sig.include_vg",
            "view_template.sig.include_filters",
            "view_template.sig.include_phase_filter",
        ],
        "discriminator_key": "view_template.sig.include_phase_filter",
        "shape_requirements": {
            "True": {"additional_required": ["view_template.sig.phase_filter"]}
        },
    }

    status, _selected, _reason, details = build_candidate_join_key_with_details(
        identity_items_by_record,
        "r1",
        selected_fields=[],
        gates=gates,
    )

    assert status == "ok"
    assert details["shape_matched"] is False
    assert "view_template.sig.phase_filter" not in details["effective_required_fields"]


def test_identity_index_keeps_q_only_rows_for_required_presence():
    from tools.join_key_discovery.eval import build_identity_index

    rows = [
        {
            "record_pk": "r1",
            "item_key": "view_template.sig.include_phase_filter",
            "item_value_type": "ok",
            "item_value": "true",
        },
        {
            "record_pk": "r1",
            "item_key": "view_template.sig.phase_filter",
            "item_value_type": "unsupported_not_applicable",
            "item_value": "",
        },
    ]

    idx = build_identity_index(rows)
    assert "r1" in idx
    assert "view_template.sig.phase_filter" in idx["r1"]
    assert idx["r1"]["view_template.sig.phase_filter"][0] == "unsupported_not_applicable"
