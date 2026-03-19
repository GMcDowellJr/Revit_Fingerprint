"""
View Templates Join Key Discovery

Evaluates policy-driven join keys for view template reuse.

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
from typing import Dict, List, Optional


def _join_key_from_record(record: Dict[str, object]) -> Optional[str]:
    join_key = record.get("join_key")
    if isinstance(join_key, dict):
        join_hash = join_key.get("join_hash")
        return join_hash if isinstance(join_hash, str) and join_hash else None
    if isinstance(join_key, str):
        return join_key or None
    return None


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


def test_join_key(records: List[Dict[str, object]]) -> Dict[str, object]:
    """
    Analyze policy-driven join key grouping.

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
        join_key = _join_key_from_record(rec)
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


def _print_sample_interpretation() -> None:
    print("Sample output interpretation")
    print("-" * 48)
    print("- Lower collision rate means fewer mismatched sig_hashes per join key.")
    print("- Lower fragmentation means fewer distinct groups to track.")
    print("- Pareto coverage indicates reuse concentration (smaller is better).")
    print("- Demo Plan cross-project clusters show reusable patterns across projects.")
    print()


def analyze_view_templates(export_path: str) -> None:
    """Run analysis on policy-driven join keys."""

    with open(export_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    _vt_data = (
        data.get("_compat_view_templates")
        or data.get("domains", {}).get("view_templates", {})
        or {}
    )
    records = _vt_data.get("records", [])

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

    result = test_join_key(records)
    _print_option_summary("POLICY JOIN KEY", result)

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
