# -*- coding: utf-8 -*-
"""
mapping/ -- Revit-side downstream utility that materializes governance mapping
elements from Fingerprint bundle-pattern-detail evidence.

This package is deliberately separate from core/ domains/ runner/ tools/:
- It is NOT extraction (does not live in domains/, is not run by runner/run_dynamo.py).
- It is NOT analysis (unlike tools/, it has a hard Revit API dependency and writes
  to the currently open Revit document).

Scope (see docs/line_pattern_mapping.md for the full design note):
- Only the line_patterns domain is supported in this PR.
- Input is the CSV triple produced by tools/export_bundle_pattern_detail.py
  (bundle_pattern_inventory.csv / pattern_settings.csv / pattern_names.csv).
- Output is a set of LinePatternElement objects created/reused in the currently
  open document, plus a deterministic CSV report.

Dependency direction: mapping/ -> core/ + domains/line_patterns.py (read-only reuse
of its canonicalization/hashing helpers). Nothing in core/, domains/, or runner/
may import from mapping/.
"""
