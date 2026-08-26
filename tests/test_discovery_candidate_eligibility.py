from tools.discovery_candidate_eligibility import classify_candidate, diagnostic_fields, filter_candidates


def test_traceability_identifiers_are_excluded_but_semantic_value_remains():
    result = filter_candidates("example", ["semantic.value", "example.source_unique_id", "example.source_element_id"])
    assert result["eligible"] == ["semantic.value"]
    assert {d.classification for d in result["excluded"]} == {"traceability_identifier"}


def test_stable_semantic_ids_are_not_excluded_by_substring_heuristic():
    assert classify_candidate("example", "example.presentation_id").eligible
    assert classify_candidate("example", "example.sorting_parameter_id").eligible


def test_explicit_arrowhead_aliases_are_suppressed():
    result = filter_candidates("arrowheads", ["arrowhead.style", "arrowhead.arrow_style_raw_int", "arrowhead.arrow_style_display"])
    assert result["eligible"] == ["arrowhead.style"]
    assert {d.canonical_item for d in result["alias_suppressed"]} == {"arrowhead.style"}


def test_routing_metadata_is_observable_but_not_candidate():
    result = filter_candidates("arrowheads", ["arrowhead.record_class", "arrowhead.tick_size_in"])
    assert result["eligible"] == ["arrowhead.tick_size_in"]
    assert result["excluded"][0].classification == "routing_metadata"
    fields = diagnostic_fields(result)
    assert "routing" in fields["candidate_fields_excluded"]
    assert fields["candidate_fields_excluded_count"] == "1"


def test_domain_rule_does_not_leak_and_results_are_deterministic():
    values = ["arrowhead.record_class", "z", "A", "z"]
    assert classify_candidate("other", "arrowhead.record_class").eligible
    assert filter_candidates("arrowheads", values) == filter_candidates("arrowheads", list(reversed(values)))
