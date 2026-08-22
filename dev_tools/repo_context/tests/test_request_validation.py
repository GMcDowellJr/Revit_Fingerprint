"""White-box tests for rc_request.py's schema validation (no CLI/subprocess
needed for pure validation logic)."""
import json

from conftest import TOOL_DIR  # ensures TOOL_DIR is on sys.path via conftest import
import rc_request as rr


def _valid_base():
    return {
        "schema_version": "1.0",
        "question": "Why is X absent?",
        "selectors": {"files": ["a/b.py"], "symbols": [], "search_terms": [], "lines": []},
    }


def test_valid_minimal_request_passes():
    errors = rr.validate_request_dict(_valid_base())
    assert errors == []


def test_valid_full_request_passes():
    data = _valid_base()
    data["selectors"]["symbols"] = [{"name": "foo", "file": "a/b.py"}]
    data["selectors"]["search_terms"] = ["needle"]
    data["selectors"]["lines"] = [{"file": "a/b.py", "line": 3, "end_line": 5}]
    data["expansion"] = {
        "include_callers": True, "include_callees": False, "include_imports": True,
        "include_related_tests": True, "include_graphify": False, "search_as_regex": False, "max_hops": 2,
    }
    data["limits"] = {"max_estimated_tokens": 5000, "max_files": 4}
    data["strict"] = True
    errors = rr.validate_request_dict(data)
    assert errors == []


def test_missing_schema_version_is_rejected():
    data = _valid_base()
    del data["schema_version"]
    errors = rr.validate_request_dict(data)
    assert any("schema_version" in e for e in errors)


def test_unsupported_schema_version_is_rejected():
    data = _valid_base()
    data["schema_version"] = "9.9"
    errors = rr.validate_request_dict(data)
    assert any("unsupported schema_version" in e for e in errors)


def test_missing_question_is_rejected():
    data = _valid_base()
    del data["question"]
    errors = rr.validate_request_dict(data)
    assert any("question" in e for e in errors)


def test_unknown_top_level_field_is_rejected():
    data = _valid_base()
    data["mystery"] = 1
    errors = rr.validate_request_dict(data)
    assert any("mystery" in e for e in errors)


def test_empty_selectors_are_rejected():
    data = _valid_base()
    data["selectors"] = {"files": [], "symbols": [], "search_terms": [], "lines": []}
    errors = rr.validate_request_dict(data)
    assert any("at least one non-empty selector" in e for e in errors)


def test_path_traversal_in_files_is_rejected():
    data = _valid_base()
    data["selectors"]["files"] = ["../../etc/passwd"]
    errors = rr.validate_request_dict(data)
    assert any("outside the scanned repository" in e for e in errors)


def test_absolute_unix_path_is_rejected():
    data = _valid_base()
    data["selectors"]["files"] = ["/etc/passwd"]
    errors = rr.validate_request_dict(data)
    assert any("outside the scanned repository" in e for e in errors)


def test_windows_drive_letter_path_is_rejected():
    data = _valid_base()
    data["selectors"]["files"] = ["C:\\Windows\\System32\\config"]
    errors = rr.validate_request_dict(data)
    assert any("outside the scanned repository" in e for e in errors)


def test_invalid_line_range_end_before_start_is_rejected():
    data = _valid_base()
    data["selectors"] = {"files": [], "symbols": [], "search_terms": [],
                          "lines": [{"file": "a/b.py", "line": 10, "end_line": 3}]}
    errors = rr.validate_request_dict(data)
    assert any("end_line" in e for e in errors)


def test_negative_line_number_is_rejected():
    data = _valid_base()
    data["selectors"] = {"files": [], "symbols": [], "search_terms": [],
                          "lines": [{"file": "a/b.py", "line": -1}]}
    errors = rr.validate_request_dict(data)
    assert any("positive integer" in e for e in errors)


def test_excessive_expansion_depth_is_rejected():
    data = _valid_base()
    data["expansion"] = {"max_hops": 99}
    errors = rr.validate_request_dict(data)
    assert any("max_hops" in e for e in errors)


def test_unknown_field_under_selectors_is_rejected():
    data = _valid_base()
    data["selectors"]["bogus"] = ["x"]
    errors = rr.validate_request_dict(data)
    assert any("selectors" in e and "bogus" in e for e in errors)


def test_unknown_field_under_symbol_object_is_rejected():
    data = _valid_base()
    data["selectors"]["symbols"] = [{"name": "foo", "regex": True}]
    errors = rr.validate_request_dict(data)
    assert any("unknown field" in e for e in errors)


def test_symbol_missing_name_is_rejected():
    data = _valid_base()
    data["selectors"]["symbols"] = [{"file": "a/b.py"}]
    errors = rr.validate_request_dict(data)
    assert any("name" in e for e in errors)


def test_non_boolean_strict_is_rejected():
    data = _valid_base()
    data["strict"] = "yes"
    errors = rr.validate_request_dict(data)
    assert any("strict" in e for e in errors)


def test_non_integer_limit_is_rejected():
    data = _valid_base()
    data["limits"] = {"max_files": "many"}
    errors = rr.validate_request_dict(data)
    assert any("max_files" in e for e in errors)


def test_parse_and_validate_request_rejects_malformed_json():
    resolved, errors = rr.parse_and_validate_request("{not json")
    assert resolved is None
    assert any("not valid JSON" in e for e in errors)


def test_parse_and_validate_request_applies_defaults():
    resolved, errors = rr.parse_and_validate_request(json.dumps(_valid_base()))
    assert errors == []
    assert resolved.max_hops == 1
    assert resolved.max_estimated_tokens == 12000
    assert resolved.max_files == 12
    assert resolved.strict is False
    assert resolved.include_callers is True
