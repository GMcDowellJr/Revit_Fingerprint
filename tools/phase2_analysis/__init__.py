"""Phase-2 (post-export) analysis helpers.

Empirical, explanatory, reversible.
No intent inference, no enforcement, no prescriptions.

This package assumes Phase-2-instrumented record.v2 exports contain:
- record.join_key.join_hash for joining
- record.phase2.{semantic_items,cosmetic_items,coordination_items,unknown_items} for comparisons

All ambiguity is preserved and reported explicitly.
"""
