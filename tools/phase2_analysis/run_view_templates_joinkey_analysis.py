"""
View Templates Join Key Discovery

Tests three hypotheses:
A) Override stack only (visual treatment defines identity)
B) Functional properties (settings define identity)
C) Combined (both define identity)

Measures:
- Fragmentation: how many groups are created
- Collision rate: multiple sig_hash values per group
- Pareto coverage: how many groups cover 80% of records
- Practical utility: can we cluster a "Demo Plan" pattern across projects?
"""

import argparse
import json
from collections import Counter, defaultdict
from statistics import mean, median
from typing import Dict, Iterable, List, Optional, Tuple


def _identity_items(record: Dict[str, object]) -> Dict[str, str]:
    items = record.get("identity_basis", {}).get("items", [])
    return {
        item.get("k"): item.get("v")
        for item in items
        if isinstance(item, dict) and item.get("k") is not None
    }


def _build_join_key(
    identity_items: Dict[str, str],
    required_keys: Iterable[str],
    optional_keys: Iterable[str],
) -> Optional[str]:
    join_parts: List[str] = []
    for key in required_keys:
        if key not in identity_items:
            return None
        join_parts.append(f"{key}={identity_items[key]}")

    for key in optional_keys:
        if key in identity_items:
            join_parts.append(f"{key}={identity_items[key]}")

    return "|".join(sorted(join_parts)) if join_parts else None


def _detect_demo_plan(record: Dict[str, object]) -> bool:
    label_candidates = [
        record.get("label"),
        record.get("name"),
        record.get("view_template_name"),
    ]
    for value in label_candidates:
        if isinstance(value, str) and "demo plan" in value.lower():
            return True
    return False


def _project_identifier(record: Dict[str, object]) -> Optional[str]:
    candidates = [
        record.get("project_id"),
        record.get("source_project_id"),
        record.get("project"),
        record.get("source_project"),
    ]
    for value in candidates:
        if isinstance(value, str) and value:
            return value
    return None


def _pareto_cover(counts: List[int], target_ratio: float = 0.8) -> int:
    if not counts:
        return 0
    total = sum(counts)
    target = total * target_ratio
    cumulative = 0
    for idx, count in enumerate(sorted(counts, reverse=True), start=1):
        cumulative += count
        if cumulative >= target:
            return idx
    return len(counts)


def test_join_key(
    records: List[Dict[str, object]],
    required_keys: Iterable[str],
    optional_keys: Iterable[str] = (),
) -> Dict[str, object]:
    """
    Simulate join key grouping.

    Returns:
        - groups: dict of join_key → list of sig_hashes
        - fragmentation: number of groups
        - collisions: groups with >1 sig_hash
        - collision_rate: collisions / fragmentation
        - pareto_cover: groups needed for 80% of records
        - demo_plan_clusters: join keys that include demo plan records
        - demo_plan_cross_project: demo plan groups spanning >1 project
    """

    groups: Dict[str, List[str]] = defaultdict(list)
    demo_plan_groups: Dict[str, List[str]] = defaultdict(list)

    for rec in records:
        identity_items = _identity_items(rec)
        join_key = _build_join_key(identity_items, required_keys, optional_keys)
        if join_key is None:
            continue

        sig_hash = rec.get("sig_hash", "")
        if isinstance(sig_hash, str):
            groups[join_key].append(sig_hash)
        else:
            groups[join_key].append("")

        if _detect_demo_plan(rec):
            project_id = _project_identifier(rec) or "unknown_project"
            demo_plan_groups[join_key].append(project_id)

    fragmentation = len(groups)
    collisions = sum(1 for sigs in groups.values() if len(set(sigs)) > 1)
    collision_rate = collisions / fragmentation if fragmentation > 0 else 0

    group_sizes = [len(sigs) for sigs in groups.values()]
    pareto_cover = _pareto_cover(group_sizes)

    demo_plan_clusters = len(demo_plan_groups)
    demo_plan_cross_project = sum(
        1 for projects in demo_plan_groups.values() if len(set(projects)) > 1
    )

    return {
        "groups": groups,
        "fragmentation": fragmentation,
        "collisions": collisions,
        "collision_rate": collision_rate,
        "pareto_cover": pareto_cover,
        "demo_plan_clusters": demo_plan_clusters,
        "demo_plan_cross_project": demo_plan_cross_project,
        "demo_plan_project_counts": {
            join_key: len(set(projects))
            for join_key, projects in demo_plan_groups.items()
        },
    }


def _print_option_summary(label: str, result: Dict[str, object]) -> None:
    fragmentation = result["fragmentation"]
    collisions = result["collisions"]
    collision_rate = result["collision_rate"]
    pareto_cover = result["pareto_cover"]
    demo_plan_clusters = result["demo_plan_clusters"]
    demo_plan_cross_project = result["demo_plan_cross_project"]

    print(label)
    print("-" * 48)
    print(f"Fragmentation: {fragmentation} groups")
    print(f"Collisions: {collisions} ({collision_rate:.1%})")
    print(f"Pareto (80% coverage): {pareto_cover} groups")
    print(f"Demo Plan clusters: {demo_plan_clusters}")
    print(f"Demo Plan cross-project clusters: {demo_plan_cross_project}")
    print()


def _recommend_option(options: List[Tuple[str, Dict[str, object]]]) -> Tuple[str, str]:
    """
    Rank by collision rate, then fragmentation, then demo plan cross-project coverage.
    """
    ranked = sorted(
        options,
        key=lambda item: (
            item[1]["collision_rate"],
            item[1]["fragmentation"],
            -item[1]["demo_plan_cross_project"],
        ),
    )
    best_label, best_result = ranked[0]
    explanation = (
        f"Lowest collision rate ({best_result['collision_rate']:.1%}) "
        f"with {best_result['fragmentation']} groups; "
        f"demo-plan cross-project clusters: {best_result['demo_plan_cross_project']}."
    )
    return best_label, explanation


def _print_sample_interpretation() -> None:
    print("Sample output interpretation")
    print("-" * 48)
    print("- Lower collision rate means fewer mismatched sig_hashes per join key.")
    print("- Lower fragmentation means fewer distinct groups to track.")
    print("- Pareto coverage indicates reuse concentration (smaller is better).")
    print("- Demo Plan cross-project clusters show reusable patterns across projects.")
    print(
        "- Prefer the option that minimizes collisions while keeping fragmentation "
        "reasonable and still capturing cross-project Demo Plan clusters."
    )
    print()


def analyze_view_templates(export_path: str) -> None:
    """Run analysis on all three join key options."""

    with open(export_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    records = (
        data.get("domains", {})
        .get("view_templates", {})
        .get("records", [])
    )

    print("View Templates Join Key Discovery")
    print("=" * 48)
    print(f"Total view templates: {len(records)}")
    print()

    group_sizes = []
    for record in records:
        group_sizes.append(len(record.get("identity_basis", {}).get("items", [])))

    if group_sizes:
        print(
            "Identity basis size (items per record): "
            f"avg={mean(group_sizes):.2f}, median={median(group_sizes):.2f}"
        )
        print()

    result_a = test_join_key(
        records,
        ["view_template.category_overrides_def_hash"],
    )
    _print_option_summary("OPTION A: Override Stack Only", result_a)

    result_b = test_join_key(
        records,
        ["view_template.detail_level", "view_template.discipline"],
        ["view_template.category_overrides_def_hash"],
    )
    _print_option_summary("OPTION B: Functional Properties", result_b)

    result_c = test_join_key(
        records,
        [
            "view_template.detail_level",
            "view_template.discipline",
            "view_template.category_overrides_def_hash",
        ],
        ["view_template.scale"],
    )
    _print_option_summary("OPTION C: Combined", result_c)

    best_label, explanation = _recommend_option(
        [
            ("A", result_a),
            ("B", result_b),
            ("C", result_c),
        ]
    )

    print("RECOMMENDATION")
    print("-" * 48)
    print(f"Best option: {best_label}")
    print(f"Reasoning: {explanation}")
    print()

    _print_sample_interpretation()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Pareto analysis for view template join key discovery."
    )
    parser.add_argument("export", help="Path to export.json")
    args = parser.parse_args()
    analyze_view_templates(args.export)


if __name__ == "__main__":
    main()
