import json
from domains import text_types
from tools.migration.reformat_to_flat_items import transform_record


class _Id:
    def __init__(self, v):
        self.IntegerValue = v


class _Type:
    def __init__(self):
        self.Id = _Id(101)
        self.UniqueId = "uid-101"


def _extract_record(monkeypatch):
    monkeypatch.setattr(text_types, "collect_types", lambda *a, **k: [_Type()])
    monkeypatch.setattr(text_types, "collect_instances", lambda *a, **k: [])
    monkeypatch.setattr(text_types, "get_type_display_name", lambda *a, **k: "Notes-Medium")
    monkeypatch.setattr(text_types, "first_param", lambda *a, **k: None)
    monkeypatch.setattr(text_types, "_as_string", lambda *a, **k: "Arial")
    monkeypatch.setattr(text_types, "_as_double", lambda *a, **k: 1.0)
    monkeypatch.setattr(text_types, "_as_int", lambda *a, **k: 1)
    monkeypatch.setattr(text_types, "_as_bool_from_param", lambda *a, **k: False)
    monkeypatch.setattr(text_types, "format_len_inches", lambda v: 1.0)
    monkeypatch.setattr(text_types, "try_get_color_rgb_from_elem", lambda *a, **k: (0, "0-0-0"))
    monkeypatch.setattr(text_types, "purge_lookup", lambda *a, **k: (False, "ok"))
    monkeypatch.setattr(text_types, "get_domain_join_key_policy", lambda *a, **k: {})
    return text_types.extract(doc=object(), ctx={})["records"][0]


def test_converted_old_and_new_records_converge(monkeypatch):
    rec = _extract_record(monkeypatch)
    legacy = dict(rec)
    legacy["identity_basis"] = {"items": rec["items"]}
    legacy["phase2"] = {
        "semantic_items": [],
        "cosmetic_items": [],
        "coordination_items": [],
        "unknown_items": [],
    }
    converted, *_ = transform_record(legacy, "text_types")

    for k in ("identity_basis", "phase2", "join_key", "sig_hash", "sig_basis", "identity_quality", "record_id_alg", "record_id_scope", "schema_version"):
        assert k not in converted
        assert k not in rec

    assert all("role" not in it for it in converted["items"])
    assert all("role" not in it for it in rec["items"])

    converted_set = {(it["k"], it.get("v"), it.get("q")) for it in converted["items"]}
    new_set = {(it["k"], it.get("v"), it.get("q")) for it in rec["items"]}
    assert converted_set == new_set

    expected_keys = {
        "text_type.background", "text_type.bold", "text_type.color_int", "text_type.color_rgb", "text_type.font",
        "text_type.italic", "text_type.leader_arrowhead_name", "text_type.leader_arrowhead_sig_hash", "text_type.leader_arrowhead_uid",
        "text_type.leader_border_offset_in", "text_type.line_weight", "text_type.name", "text_type.show_border", "text_type.size_in",
        "text_type.source_element_id", "text_type.source_unique_id", "text_type.tab_size_in", "text_type.type_id", "text_type.type_uid",
        "text_type.underline", "text_type.width_factor",
    }
    assert expected_keys.issubset({it["k"] for it in rec["items"]})
