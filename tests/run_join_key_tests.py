#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Standalone tests for join_key migration.
Can run without Revit API.
"""

import sys
import hashlib
import json
import os

# Get repo root
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)

print('=== Join Key Migration Tests (Standalone) ===')
print()

# Test 1: Reimplement _compute_override_properties_hash logic for testing
def _compute_override_properties_hash(identity_items):
    override_items = [
        item for item in identity_items
        if not item.get('k', '').startswith('vco.baseline')
           and item.get('k', '').startswith('vco.')
    ]
    sorted_items = sorted(override_items, key=lambda x: x.get('k', ''))
    signature_parts = []
    for item in sorted_items:
        k = item.get('k', '')
        q = item.get('q', '')
        v = item.get('v')
        v_str = '' if v is None else str(v)
        signature_parts.append('{}={}:{}'.format(k, q, v_str))
    signature = '|'.join(signature_parts)
    return hashlib.md5(signature.encode('utf-8')).hexdigest()

# Test override hash is order-independent
items1 = [
    {'k': 'vco.baseline_category_path', 'q': 'ok', 'v': 'Walls|self'},
    {'k': 'vco.cut.line_weight', 'q': 'ok', 'v': '-1'},
    {'k': 'vco.projection.line_weight', 'q': 'ok', 'v': '1'}
]

items2 = [
    {'k': 'vco.projection.line_weight', 'q': 'ok', 'v': '1'},
    {'k': 'vco.baseline_category_path', 'q': 'ok', 'v': 'Walls|self'},
    {'k': 'vco.cut.line_weight', 'q': 'ok', 'v': '-1'}
]

hash1 = _compute_override_properties_hash(items1)
hash2 = _compute_override_properties_hash(items2)

print('Test 1: Override hash is order-independent')
print('  hash1:', hash1)
print('  hash2:', hash2)
assert hash1 == hash2, 'FAIL: Hash must be order-independent'
assert len(hash1) == 32, 'FAIL: MD5 hash must be 32 hex chars'
print('  PASS')

# Test 2: Baseline items excluded
items_with_baseline = [
    {'k': 'vco.baseline_category_path', 'q': 'ok', 'v': 'Walls|self'},
    {'k': 'vco.baseline_sig_hash', 'q': 'ok', 'v': 'abc123'},
    {'k': 'vco.cut.line_weight', 'q': 'ok', 'v': '-1'},
]

items_without_baseline = [
    {'k': 'vco.cut.line_weight', 'q': 'ok', 'v': '-1'},
]

hash3 = _compute_override_properties_hash(items_with_baseline)
hash4 = _compute_override_properties_hash(items_without_baseline)

print('Test 2: Baseline items excluded from hash')
print('  hash_with_baseline:', hash3)
print('  hash_without_baseline:', hash4)
assert hash3 == hash4, 'FAIL: Baseline items must be excluded from hash'
print('  PASS')

# Test 3: None values handled
items_with_none = [
    {'k': 'vco.cut.line_weight', 'q': 'ok', 'v': None},
    {'k': 'vco.projection.line_weight', 'q': 'ok', 'v': '1'}
]
hash5 = _compute_override_properties_hash(items_with_none)
print('Test 3: None values handled gracefully')
print('  hash:', hash5)
assert len(hash5) == 32, 'FAIL: MD5 hash must be 32 hex chars'
print('  PASS')

# Test 4: Different values produce different hashes
items_a = [{'k': 'vco.cut.line_weight', 'q': 'ok', 'v': '-1'}]
items_b = [{'k': 'vco.cut.line_weight', 'q': 'ok', 'v': '2'}]

hash_a = _compute_override_properties_hash(items_a)
hash_b = _compute_override_properties_hash(items_b)

print('Test 4: Different values produce different hashes')
print('  hash_a:', hash_a)
print('  hash_b:', hash_b)
assert hash_a != hash_b, 'FAIL: Different values must produce different hashes'
print('  PASS')

# Test 5: Policy exists and has correct structure
print('Test 5: Join key policies exist and have correct structure')
policies_path = os.path.join(repo_root, 'policies', 'domain_join_key_policies.json')
with open(policies_path) as f:
    policies = json.load(f)

assert 'view_category_overrides' in policies['domains'], 'FAIL: VCO policy missing'
assert 'view_templates' in policies['domains'], 'FAIL: VT policy missing'

vco_policy = policies['domains']['view_category_overrides']
vt_policy = policies['domains']['view_templates']

assert vco_policy['join_key_schema'] == 'view_category_overrides.join_key.v1', 'FAIL: VCO schema wrong'
assert 'vco.baseline_category_path' in vco_policy['required_items'], 'FAIL: VCO missing baseline_category_path'
assert 'vco.baseline_sig_hash' in vco_policy['required_items'], 'FAIL: VCO missing baseline_sig_hash'
assert 'vco.override_properties_hash' in vco_policy['required_items'], 'FAIL: VCO missing override_properties_hash'

assert vt_policy['join_key_schema'] == 'view_templates.join_key.v1', 'FAIL: VT schema wrong'
assert 'view_template.def_hash' in vt_policy['required_items'], 'FAIL: VT missing def_hash'
print('  PASS')

# Test 6: VCO excluded items are correct
print('Test 6: VCO excluded items correct')
excluded = vco_policy.get('explicitly_excluded_items', [])
assert 'vco.projection.line_weight' in excluded, 'FAIL: projection.line_weight should be excluded'
assert 'vco.cut.line_weight' in excluded, 'FAIL: cut.line_weight should be excluded'
print('  PASS')

# Test 7: VT excluded items are correct
print('Test 7: VT excluded items correct')
excluded = vt_policy.get('explicitly_excluded_items', [])
assert 'view_template.name' in excluded, 'FAIL: name should be excluded'
assert 'view_template.uid' in excluded, 'FAIL: uid should be excluded'
print('  PASS')

print()
print('=== All 7 tests PASSED! ===')
