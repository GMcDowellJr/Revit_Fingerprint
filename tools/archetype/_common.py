"""Shared IO/logging helpers for the cross-domain archetype discovery pipeline.

Conventions (mirrors tools/compare_cross_segment.py):
- Atomic CSV/JSON writes (write to .tmp then os.replace).
- All outputs land under Fingerprint_Out/archetype_analysis/.
- Config lives under config/archetype/.
- Stage-level logging goes to stderr with a "[archetype:<stage>]" prefix.
"""
from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

SCHEMA_VERSION = "1.0"

# dimension_types_* partitions whose tick_mark__arrowheads edge collapses
# onto dimension_types_linear (see build_edge_aliases).
DIM_TYPE_VARIANTS = ("linear", "angular", "radial", "diameter")

# item_value / item_value_type (q) markers that indicate "no usable value".
INVALID_ITEM_VALUES = {"", "<NONE>", "<MISSING>", "<UNREADABLE>", "<NOT_APPLICABLE>"}
INVALID_ITEM_QUALITIES = {
    "missing",
    "unreadable",
    "unsupported",
    "unsupported.not_applicable",
    "unsupported.not_implemented",
}


def log(stage: str, msg: str) -> None:
    sys.stderr.write(f"[archetype:{stage}] {msg}\n")


def is_valid_item(item_value: str, item_value_type: str) -> bool:
    """True if an identity_items row carries usable evidence of a value."""
    if item_value in INVALID_ITEM_VALUES:
        return False
    if item_value_type in INVALID_ITEM_QUALITIES:
        return False
    return True


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.is_file():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [
            {str(k): ("" if v is None else str(v)) for k, v in row.items()}
            for row in csv.DictReader(f)
        ]


def read_json(path: Path, default: Any = None) -> Any:
    if not path.is_file():
        return default
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w", encoding="utf-8", newline="", delete=False,
        dir=str(path.parent), suffix=".tmp",
    ) as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.DictWriter(tmp, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})
    tmp_path.replace(path)


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w", encoding="utf-8", delete=False,
        dir=str(path.parent), suffix=".tmp",
    ) as tmp:
        tmp_path = Path(tmp.name)
        json.dump(obj, tmp, indent=2, sort_keys=True)
        tmp.write("\n")
    tmp_path.replace(path)


def field_matches(item_key: str, source_field: str, field_match: str) -> bool:
    """Match an identity_items item_key against an edge's source_field.

    field_match == "exact": item_key == source_field
    field_match == "indexed": source_field contains a literal "[*]" placeholder;
        match any item_key sharing the prefix/suffix around it
        (e.g. "vfa.stack[*].filter_sig_hash" matches "vfa.stack[002].filter_sig_hash").
    """
    if field_match == "indexed":
        prefix, sep, suffix = source_field.partition("[*]")
        if not sep:
            return item_key == source_field
        return item_key.startswith(prefix) and item_key.endswith(suffix)
    return item_key == source_field


def strip_partition_suffix(target_domain: str) -> Optional[str]:
    """Strip a trailing "_drafting"/"_model" suffix; None if neither present."""
    for suffix in ("_drafting", "_model"):
        if target_domain.endswith(suffix):
            return target_domain[: -len(suffix)]
    return None


def build_edge_aliases(edges_by_id: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """Build edge_id -> canonical_edge_id and canonical -> [collapsed edge_ids].

    Two grouping passes:
      1. fill_patterns_drafting/_model partition collapse: edges sharing
         (source_domain, source_field) and a target_domain differing only by
         a "_drafting"/"_model" suffix collapse onto the "_drafting" edge.
      2. dimension_types_{linear,angular,radial,diameter} tick_mark variant
         collapse: edges sharing (source_field, target_domain) where
         source_domain is one of the four dimension_types_* partitions in
         DIM_TYPE_VARIANTS collapse onto the dimension_types_linear edge.
    """
    alias_of: Dict[str, str] = {}
    collapsed: Dict[str, List[str]] = defaultdict(list)

    # Pass 1: fill_patterns_drafting / fill_patterns_model collapse.
    fill_groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
    for edge_id, edge in edges_by_id.items():
        prefix = strip_partition_suffix(edge.get("target_domain", ""))
        if prefix is None:
            continue
        fill_groups[(edge.get("source_domain", ""), edge.get("source_field", ""), prefix)].append(edge_id)

    for edge_ids in fill_groups.values():
        if len(edge_ids) < 2:
            continue
        canonical = next(
            (e for e in edge_ids if edges_by_id[e].get("target_domain", "").endswith("_drafting")),
            sorted(edge_ids)[0],
        )
        for e in edge_ids:
            if e != canonical:
                alias_of[e] = canonical
                collapsed[canonical].append(e)

    # Pass 2: dimension_types_{variant} tick_mark collapse (skip edges already aliased).
    dim_groups: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for edge_id, edge in edges_by_id.items():
        if edge_id in alias_of:
            continue
        source_domain = edge.get("source_domain", "")
        if any(source_domain == f"dimension_types_{variant}" for variant in DIM_TYPE_VARIANTS):
            dim_groups[(edge.get("source_field", ""), edge.get("target_domain", ""))].append(edge_id)

    for edge_ids in dim_groups.values():
        if len(edge_ids) < 2:
            continue
        canonical = next(
            (e for e in edge_ids if edges_by_id[e].get("source_domain") == "dimension_types_linear"),
            sorted(edge_ids)[0],
        )
        for e in edge_ids:
            if e != canonical:
                alias_of[e] = canonical
                collapsed[canonical].append(e)

    for canonical in collapsed:
        collapsed[canonical].sort()

    return alias_of, dict(collapsed)


def slugify(value: str) -> str:
    out = []
    for ch in value:
        if ch.isalnum():
            out.append(ch.lower())
        elif ch in ("_", ".", "-"):
            out.append("_")
        else:
            out.append("_")
    slug = "".join(out)
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_")
