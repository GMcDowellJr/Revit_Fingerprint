from domains import text_types
import json


class _Id:
    def __init__(self, v):
        self.IntegerValue = v


class _Type:
    def __init__(self):
        self.Id = _Id(101)
        self.UniqueId = "uid-101"


def test_text_types_extract_emits_flat_items_only(monkeypatch):
    monkeypatch.setattr(text_types, "collect_types", lambda *a, **k: [_Type()])
    monkeypatch.setattr(text_types, "collect_instances", lambda *a, **k: [])
    monkeypatch.setattr(text_types, "get_type_display_name", lambda *a, **k: "Text A")
    monkeypatch.setattr(text_types, "first_param", lambda *a, **k: None)
    monkeypatch.setattr(text_types, "_as_string", lambda *a, **k: "Arial")
    monkeypatch.setattr(text_types, "_as_double", lambda *a, **k: 1.0)
    monkeypatch.setattr(text_types, "_as_int", lambda *a, **k: 1)
    monkeypatch.setattr(text_types, "_as_bool_from_param", lambda *a, **k: False)
    monkeypatch.setattr(text_types, "format_len_inches", lambda v: 1.0)
    monkeypatch.setattr(text_types, "try_get_color_rgb_from_elem", lambda *a, **k: (0, "0-0-0"))
    monkeypatch.setattr(text_types, "purge_lookup", lambda *a, **k: (False, "ok"))
    monkeypatch.setattr(text_types, "get_domain_join_key_policy", lambda *a, **k: {})

    out = text_types.extract(doc=object(), ctx={"role_policy": {"text_types": {"identity": ["text_type.name"]}}})
    rec = out["records"][0]

    assert "items" in rec and isinstance(rec["items"], list)
    assert rec.get("schema_version") == "record.v2"
    assert "identity_basis" not in rec
    assert "phase2" not in rec
    assert "join_key" not in rec
    assert "sig_hash" not in rec
    assert all("role" not in it for it in rec["items"])
    assert [it["k"] for it in rec["items"]] == sorted([it["k"] for it in rec["items"]])
    policy = json.load(open("policies/domain_sig_hash_policies.json", "r"))
    domain_policy = (policy.get("domains") or {}).get("text_types") or {}
    required = set(domain_policy.get("required_items") or [])
    keys = {it["k"] for it in rec["items"]}
    assert required.issubset(keys)
