"""
View Category Overrides Join Key Discovery

Hypothesis: Override identity = baseline + delta hash

Analysis:
1. Group overrides by baseline_sig_hash
2. Within each baseline group, count distinct delta patterns
3. Measure reuse: how many templates share same override pattern?
4. Identify common patterns (e.g., "demo override", "RCP override")
"""

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from statistics import mean, median
from typing import Dict, Iterable, List, Tuple


def _stable_delta_hash(delta_items: Iterable[Dict[str, str]]) -> str:
    """Compute a stable hash for delta items (k/v pairs) to model delta_sig_hash."""
    normalized = sorted((item.get("k"), item.get("v")) for item in delta_items)
    payload = json.dumps(normalized, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _extract_override_record(record: Dict[str, object]) -> Tuple[str, str, List[Dict[str, str]], str, str]:
    """Return (baseline_sig, delta_sig, delta_items, record_id, label)."""
    baseline_sig = None
    delta_sig = None
    delta_items: List[Dict[str, str]] = []

    for item in record.get("identity_basis", {}).get("items", []):
        key = item.get("k")
        if key == "vco.baseline_sig_hash":
            baseline_sig = item.get("v")
        elif key == "vco.delta_sig_hash":
            delta_sig = item.get("v")
        elif key and not key.startswith("vco.baseline"):
            delta_items.append(item)

    if delta_sig is None:
        delta_sig = _stable_delta_hash(delta_items)

    return (
        baseline_sig,
        delta_sig,
        delta_items,
        record.get("record_id", ""),
        record.get("label", ""),
    )


def analyze_override_patterns(export_path: str) -> None:
    """
    Analyze view_category_overrides for join key discovery.

    Metrics:
    - Baseline coverage: how many baselines have overrides?
    - Delta reuse: how many overrides share same delta?
    - Common patterns: most frequent override combinations
    """

    with open(export_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    records = (
        data.get("domains", {})
        .get("view_category_overrides", {})
        .get("records", [])
    )

    by_baseline: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    delta_sig_counts: Counter = Counter()
    delta_items_by_sig: Dict[str, List[Dict[str, str]]] = {}
    delta_label_by_sig: Dict[str, str] = {}

    for record in records:
        baseline_sig, delta_sig, delta_items, record_id, label = _extract_override_record(record)
        if not baseline_sig:
            continue
        by_baseline[baseline_sig].append(
            {
                "record_id": record_id,
                "sig_hash": record.get("sig_hash", ""),
                "delta_sig": delta_sig,
                "delta_items": delta_items,
                "label": label,
            }
        )
        delta_sig_counts[delta_sig] += 1
        delta_items_by_sig.setdefault(delta_sig, delta_items)
        if label:
            delta_label_by_sig.setdefault(delta_sig, label)

    baseline_counts = [len(overrides) for overrides in by_baseline.values()]
    baseline_fragmented = [count for count in baseline_counts if count > 1]

    reused_delta = [(sig, count) for sig, count in delta_sig_counts.items() if count > 1]
    reused_delta.sort(key=lambda x: x[1], reverse=True)

    total_records = len(records)
    total_baselines = len(by_baseline)
    reuse_rate = (len(reused_delta) / len(delta_sig_counts)) if delta_sig_counts else 0
    repeated_records = sum(count for _, count in reused_delta)
    repeated_record_rate = (repeated_records / total_records) if total_records else 0

    # Pareto (80/20) analysis: how many deltas cover 80% of records?
    sorted_delta_counts = sorted(delta_sig_counts.values(), reverse=True)
    cumulative = 0
    pareto_target = 0.8 * total_records if total_records else 0
    pareto_cover = 0
    for idx, count in enumerate(sorted_delta_counts, start=1):
        cumulative += count
        if cumulative >= pareto_target:
            pareto_cover = idx
            break

    print("View Category Overrides Join Key Discovery")
    print("=" * 48)
    print(f"Total override records: {total_records}")
    print(f"Distinct baselines: {total_baselines}")
    print(f"Distinct delta patterns: {len(delta_sig_counts)}")
    print()

    print("Baseline coverage")
    print("-" * 48)
    print(f"Baselines with overrides: {total_baselines}")
    if baseline_counts:
        print(f"Average overrides per baseline: {mean(baseline_counts):.2f}")
        print(f"Median overrides per baseline: {median(baseline_counts):.2f}")
        print(f"Baselines with >1 override: {len(baseline_fragmented)}")
    print()

    print("Delta reuse")
    print("-" * 48)
    print(f"Reusable delta patterns (>1 use): {len(reused_delta)}")
    print(f"Reuse rate (pattern-based): {reuse_rate:.2%}")
    print(f"Reuse rate (record coverage): {repeated_record_rate:.2%}")
    print()

    print("Top 10 most reused delta patterns")
    print("-" * 48)
    for delta_sig, count in reused_delta[:10]:
        label = delta_label_by_sig.get(delta_sig, "")
        delta_items = delta_items_by_sig.get(delta_sig, [])
        label_hint = label or (delta_items[0].get("k") if delta_items else delta_sig[:16])
        print(f"  {count} templates: {label_hint}")
    print()

    print("Baseline fragmentation")
    print("-" * 48)
    fragmented = [(base, len(overrides)) for base, overrides in by_baseline.items() if len(overrides) > 1]
    fragmented.sort(key=lambda x: x[1], reverse=True)
    for base, count in fragmented[:10]:
        print(f"  {base[:16]}: {count} distinct override patterns")
    print()

    print("Pareto analysis")
    print("-" * 48)
    if total_records:
        print(f"Delta patterns needed for 80% of records: {pareto_cover} of {len(delta_sig_counts)}")
    else:
        print("No records available for Pareto analysis.")
    print()

    print("Visualization suggestions")
    print("-" * 48)
    print("- Bar chart: top 20 delta patterns by frequency")
    print("- Pareto chart: cumulative coverage of delta patterns")
    print("- Histogram: overrides per baseline (fragmentation)")
    print()

    print("JOIN KEY RECOMMENDATION")
    print("-" * 48)
    if reuse_rate >= 0.3 or repeated_record_rate >= 0.5:
        print("HIGH REUSE detected - referential model justified!")
        print("Join key: baseline_sig_hash + delta_sig_hash")
    elif total_baselines > 0 and mean(baseline_counts) > 1.5:
        print("MODERATE reuse detected - consider join key in lookup table.")
        print("Join key: baseline_sig_hash + delta_sig_hash (validate with domain owners)")
    else:
        print("LOW reuse detected - consider embedding overrides directly in view_templates.")
        print("Join key: baseline_sig_hash only (delta not reused)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Pareto analysis for view category override join key discovery.")
    parser.add_argument("export", help="Path to export.json")
    args = parser.parse_args()
    analyze_override_patterns(args.export)


if __name__ == "__main__":
    main()
