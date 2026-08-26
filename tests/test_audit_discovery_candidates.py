from collections import defaultdict
from pathlib import Path

from tools.audit_discovery_candidates import _policy_roles, _static_items

REPO = Path(__file__).resolve().parents[1]


def _evidence():
    join = _policy_roles(REPO / "policies/domain_join_key_policies.json")
    sig = _policy_roles(REPO / "policies/domain_sig_hash_policies.json")
    domains = defaultdict(set)
    for domain, key in set(join) | set(sig):
        domains[key].add(domain)
    return _static_items(REPO, domains)


def test_dictionary_traceability_items_are_included():
    evidence = _evidence()
    assert ("phases", "phase.source_element_id") in evidence
    assert ("phases", "phase.source_unique_id") in evidence


def test_multi_domain_module_items_are_attributed_to_emitted_domains():
    evidence = _evidence()
    assert not any(domain == "dimension_types" for domain, _ in evidence)
    assert any(domain == "dimension_types_linear" for domain, _ in evidence)
    assert any(domain == "dimension_types_angular" for domain, _ in evidence)
    assert ("units_doc", "units_doc.decimal_symbol") in evidence
    assert not any(domain == "object_styles" for domain, _ in evidence)
    assert ("object_styles_model", "obj_style.source_element_id") in evidence
    assert ("object_styles_annotation", "obj_style.source_element_id") in evidence
    assert not any(domain == "view_templates" for domain, _ in evidence)
    assert ("view_templates_ceiling_plans", "vt.assigned_view_count") in evidence
