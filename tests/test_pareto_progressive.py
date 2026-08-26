from tools.join_key_discovery.eval import build_identity_index
from tools.pareto_joinkey_search import pareto_search


def _row(pk, key, value):
    return {"record_pk": pk, "item_key": key, "item_value": value, "item_value_type": "str"}


def _xor_fixture():
    records = [{"record_pk": str(i), "sig_hash": f"s{i}"} for i in range(4)]
    items = []
    for i, (a, b) in enumerate((("0", "0"), ("0", "1"), ("1", "0"), ("1", "1"))):
        items += [_row(str(i), "a", a), _row(str(i), "b", b)]
    return records, build_identity_index(items)


def test_accepted_k1_stops_before_deeper_search():
    records = [{"record_pk": str(i), "sig_hash": f"s{i}"} for i in range(3)]
    index = build_identity_index([_row(str(i), "unique", str(i)) for i in range(3)] + [_row(str(i), "noise", "x") for i in range(3)])
    result = pareto_search(records, index, ["noise", "unique"], {"max_k": 2, "progress": False})
    assert result["chosen"]["keys"] == "unique"
    assert result["diagnostics"]["k_levels_attempted"] == [1]
    assert result["diagnostics"]["subsets_evaluated"] == 2
    assert result["diagnostics"]["stop_reason"] == "accepted_finalist"


def test_no_k1_acceptance_expands_to_k2_and_honors_ceiling():
    records, index = _xor_fixture()
    result = pareto_search(records, index, ["a", "b"], {"max_k": 2, "progress": False})
    assert result["chosen"]["keys"] == "a|b"
    assert result["diagnostics"]["k_levels_attempted"] == [1, 2]
    assert result["diagnostics"]["max_k_attempted"] == 2


def test_failed_full_finalist_continues_and_verification_is_not_combinatorial():
    records, index = _xor_fixture()
    calls = []
    def verify(finalist):
        calls.append(finalist["keys"])
        return {"accepted_full": finalist["keys"] == "a|b", "diverges": finalist["keys"] != "a|b", "metrics": finalist["metrics"]}
    # Make a k=1 field sample-acceptable while the k=2 candidate is also valid.
    sample = records[:2]
    result = pareto_search(sample, index, ["b", "a"], {"max_k": 2, "progress": False, "finalist_verifier": verify})
    assert calls == ["b", "a|b"]
    assert result["chosen"]["keys"] == "a|b"
    assert result["diagnostics"]["k_levels_attempted"] == [1, 2]


def test_frontier_and_work_are_bounded_and_deterministic():
    records, index = _xor_fixture()
    cfg = {"max_k": 2, "progress": False, "frontier_limit": 1, "work_budget": 8}
    first = pareto_search(records, index, ["b", "a"], cfg)
    second = pareto_search(records, index, ["a", "b"], cfg)
    assert first["frontier"] == second["frontier"]
    assert len(first["frontier"]) <= 1
    assert first["diagnostics"]["stop_reason"] == "work_budget_exhausted"
    assert first["diagnostics"]["estimated_search_work"] == 8
