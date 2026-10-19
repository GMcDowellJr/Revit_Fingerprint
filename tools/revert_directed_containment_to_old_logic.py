#!/usr/bin/env python3
"""revert_directed_containment_to_old_logic.py

Reconstructs what compare_directed_file()'s pre-fix (buggy) output would
have been, from the CURRENT (fixed) cross_segment CSVs -- so you can run
generate_governance_narrative.py once against the real files and once
against these reverted copies, with the swap as the ONLY variable. Same
corpus, same generator commit, same every other input.

Why this works without re-running compare_cross_segment.py at all: the
Step 0 audit confirmed the bug was a pure value swap inside
compare_directed_file() -- all_pairwise_containment_a_in_b_mean and
all_pairwise_containment_b_in_a_mean (and their used_/_min siblings) held
the mathematically opposite quantity from what those names mean on
symmetric rows. The fix swapped the values back; it did NOT change the
formula, add a new computation, or touch symmetric/pooled rows at all.
That means today's *_b_in_a_mean value on a directed row is *exactly*
what *_a_in_b_mean held before the fix, and vice versa -- reading the
current file with those two columns exchanged, on directed rows only,
reconstructs the old output exactly, with no approximation.

This intentionally does NOT touch:
  - Symmetric/pooled comparison_type rows (peer segments, bc_to_bc,
    cross_client, N-1 pooled, etc.) -- compare_symmetric_file() and
    _build_pooled_row() were never buggy; swapping those would introduce
    a NEW error, not reconstruct history.
  - Any column that isn't one of the a_in_b/b_in_a pairs (jaccard,
    reference_union_pattern_count, reference_core_share, etc. are
    untouched either way -- compare_directed_file() never emitted a
    jaccard field, and the reference-union heterogeneity diagnostics
    were not implicated in the swap).

Usage
-----
    python revert_directed_containment_to_old_logic.py \\
        --input  C:\\...\\cross_segment\\cross_segment_summary.csv \\
        --input  C:\\...\\cross_segment\\cross_segment_governance_states.csv \\
        --out-dir C:\\...\\cross_segment_old_logic_counterfactual

Then re-run generate_governance_narrative.py exactly as you normally
would, except point --summary / --governance-states (whichever of the
--input files you passed) at the files written into --out-dir, while
leaving --pooled and every other flag pointed at the real, current
files. Diff that package's governance_domain_summary.csv against the
real, current one -- every difference is attributable to the fix, and
nothing else.

Requires no third-party packages (stdlib csv only, matching this
project's convention in compare_cross_segment.py / generate_governance_
narrative.py).
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Fallback list, mirrored from tools/compare_cross_segment.py's DIRECTED_TYPES
# at the time this script was written. If that constant is importable (this
# script run from within a repo checkout with tools/ on sys.path), the live
# value is used instead and this fallback is ignored -- keeping this script
# correct automatically if DIRECTED_TYPES is ever edited.
_FALLBACK_DIRECTED_TYPES = {
    "generic_to_template",
    "generic_to_container",
    "generic_to_project",
    "template_to_project",
    "template_to_container",
    "container_to_project",
    "parent_sibling_roles",
    "governance_chain",  # confirmed dead (never assigned as a row's comparison_type)
    "enterprise_to_project",
    "bc_to_project",
    "enterprise_to_bc",
    "enterprise_to_client",
}


def _resolve_directed_types(tools_dir: str | None) -> Tuple[Set[str], str]:
    """Returns (directed_types, source_description)."""
    if tools_dir:
        sys.path.insert(0, tools_dir)
    try:
        from compare_cross_segment import DIRECTED_TYPES  # type: ignore

        return set(DIRECTED_TYPES), "imported live from compare_cross_segment.DIRECTED_TYPES"
    except Exception:
        return set(_FALLBACK_DIRECTED_TYPES), "fallback list embedded in this script (import failed)"


def _find_swap_pairs(fieldnames: List[str]) -> List[Tuple[str, str]]:
    """Finds every (a_in_b_column, b_in_a_column) pair present in the header.

    Generic by construction -- matches all_pairwise_containment_a_in_b_mean /
    all_pairwise_containment_b_in_a_mean, used_pairwise_containment_a_in_b_mean /
    used_pairwise_containment_b_in_a_mean, any _min sibling, and anything else
    following the same naming convention, without hardcoding the exact column
    list. This means it also stays correct if a future column is added.
    """
    field_set = set(fieldnames)
    seen: Set[frozenset] = set()
    pairs: List[Tuple[str, str]] = []
    for f in fieldnames:
        if "a_in_b" not in f:
            continue
        counterpart = f.replace("a_in_b", "b_in_a")
        if counterpart in field_set:
            key = frozenset((f, counterpart))
            if key not in seen:
                seen.add(key)
                pairs.append((f, counterpart))
    return pairs


def revert_file(
    input_path: Path,
    out_path: Path,
    directed_types: Set[str],
    comparison_type_column: str,
    dry_run: bool,
) -> Dict[str, int]:
    with input_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    if comparison_type_column not in fieldnames:
        print(
            f"  [skip] {input_path.name}: no '{comparison_type_column}' column -- "
            "nothing to key the directed-row filter on, leaving file untouched.",
            file=sys.stderr,
        )
        if not dry_run:
            out_path.write_bytes(input_path.read_bytes())
        return {}

    swap_pairs = _find_swap_pairs(fieldnames)
    if not swap_pairs:
        print(
            f"  [skip] {input_path.name}: no a_in_b/b_in_a column pairs found -- "
            "leaving file untouched (this is expected for cross_segment_pooled.csv "
            "and any other symmetric-only file).",
            file=sys.stderr,
        )
        if not dry_run:
            out_path.write_bytes(input_path.read_bytes())
        return {}

    print(f"  {input_path.name}: swap pairs found: {swap_pairs}")

    per_type_counts: Dict[str, int] = {}
    for row in rows:
        ctype = (row.get(comparison_type_column, "") or "").strip()
        if ctype not in directed_types:
            continue
        for col_a, col_b in swap_pairs:
            val_a = row.get(col_a, "")
            val_b = row.get(col_b, "")
            # Only swap if at least one side is non-blank -- avoid manufacturing
            # a "0.0 vs blank" distinction where the original row had neither.
            if val_a == "" and val_b == "":
                continue
            row[col_a], row[col_b] = val_b, val_a
        per_type_counts[ctype] = per_type_counts.get(ctype, 0) + 1

    if not dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return per_type_counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--input",
        action="append",
        required=True,
        help="Path to a current cross_segment CSV to revert (repeatable -- e.g. "
        "pass this once for cross_segment_summary.csv and once for "
        "cross_segment_governance_states.csv if you feed both into "
        "generate_governance_narrative.py). Do NOT pass cross_segment_pooled.csv "
        "-- it has no a_in_b/b_in_a columns and will just be copied through unchanged "
        "if you do, harmlessly, but there's no need.",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Directory to write reverted copies into (original filenames preserved).",
    )
    parser.add_argument(
        "--comparison-type-column",
        default="comparison_type",
        help="Column name identifying each row's comparison_type (default: comparison_type).",
    )
    parser.add_argument(
        "--tools-dir",
        default=None,
        help="Path to the repo's tools/ directory, so this script can import the live "
        "DIRECTED_TYPES constant instead of using its embedded fallback list. "
        "Optional but recommended.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be swapped, per file and per comparison_type, without writing output.",
    )
    args = parser.parse_args()

    directed_types, source = _resolve_directed_types(args.tools_dir)
    print(f"Directed types ({source}):")
    for t in sorted(directed_types):
        print(f"  - {t}")
    print()

    out_dir = Path(args.out_dir)
    total_rows_reverted = 0
    for input_str in args.input:
        input_path = Path(input_str)
        if not input_path.exists():
            print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
            sys.exit(1)
        out_path = out_dir / input_path.name
        print(f"Processing {input_path} -> {out_path}{' (dry run)' if args.dry_run else ''}")
        counts = revert_file(
            input_path,
            out_path,
            directed_types,
            args.comparison_type_column,
            args.dry_run,
        )
        for ctype, n in sorted(counts.items()):
            print(f"    reverted {n:4d} row(s) for comparison_type={ctype}")
        total_rows_reverted += sum(counts.values())
        print()

    print(f"Total rows reverted across all inputs: {total_rows_reverted}")
    if args.dry_run:
        print("(dry run -- no files written)")
    else:
        print(f"Reverted files written to: {out_dir}")
        print(
            "\nNext step: re-run generate_governance_narrative.py with --summary "
            "(and --governance-states, if applicable) pointed at the files in "
            "that directory instead of the real ones, and --pooled plus every "
            "other flag left pointed at your real, current inputs. Diff the "
            "resulting governance_domain_summary.csv against your real, current "
            "one -- every difference is attributable to the compare_directed_file() "
            "fix, and nothing else."
        )


if __name__ == "__main__":
    main()
