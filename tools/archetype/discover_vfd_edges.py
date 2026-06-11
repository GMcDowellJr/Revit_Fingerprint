#!/usr/bin/env python3
"""Discover View Filter Definition dynamic edges from flat identity_items CSV exports.

Inputs:
  - identity_items_by_domain/view_filter_definitions.csv (or
    view_filter_definitions_identity_items.csv)
  - bip_lookup.json
  - vfd_category_domain_map.json
  - vfd_bip_target_domain_hints.json
  - shared_param_names.json (optional)
  - file_metadata.csv (optional; required when --dump-unresolved-files is
    set), keyed by export_run_id with client_label, governance_role,
    unit_system columns.

Outputs:
  - vfd_param_inventory.csv
  - vfd_dynamic_edges.csv, one row per discovered (edge, param_id) with
    scope_conditions.category_ids listing every supported category and
    category_file_counts giving the per-category file support, so
    generate_reference_graph.py can rebuild dynamic scope_conditions.
  - vfd_unresolved_files.csv (optional; written when --dump-unresolved-files
    is set), one row per (unresolved shared-parameter GUID, export_run_id)
    joined to file_metadata.csv, to help locate source files for resolving
    shared_param_names.json.

The output CSVs are intended as offline inputs to generate_reference_graph.py.
All paths are supplied at runtime; the category/domain reference files default
next to this script.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

STAGE = "vfd_discover"
RULE_KEY_RE = re.compile(r"^vf\.rule\[(\d+)\]\.param_ref\.(kind|id)$")
BIP_RE = re.compile(r"^bip:-\d+$")
GUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
EDGE_ID_RE = re.compile(r"^vfd\.[a-z0-9_]+__[A-Za-z0-9_]+$")
SENTINEL_IDS = {"", "null", "none", "missing", "unreadable", "<none>", "<missing>", "<unreadable>"}
INVALID_ITEM_VALUES = {"", "<NONE>", "<MISSING>", "<UNREADABLE>", "<NOT_APPLICABLE>"}
INVALID_ITEM_QUALITIES = {
    "missing",
    "unreadable",
    "unsupported",
    "unsupported.not_applicable",
    "unsupported.not_implemented",
}

INVENTORY_FIELDS = [
    "param_id",
    "param_kind",
    "param_name",
    "name_resolved",
    "target_domain",
    "target_domain_source",
    "target_domain_verified",
    "category_set",
    "category_names",
    "unrecognized_category_ids",
    "has_unextracted_domain",
    "has_unverified_category_mapping",
    "file_count",
    "rule_count",
    "meets_threshold",
    "requires_human_review",
]

EDGE_FIELDS = [
    "edge_id",
    "param_id",
    "param_kind",
    "param_name",
    "param_name_normalized",
    "target_domain",
    "scope_conditions",
    "category_file_counts",
    "file_count",
    "rule_count",
    "name_resolved",
    "target_domain_source",
    "target_domain_verified",
    "requires_human_review",
]

UNRESOLVED_FILE_FIELDS = [
    "param_id",
    "export_run_id",
    "client_label",
    "governance_role",
    "unit_system",
    "rule_count",
]


@dataclass(frozen=True)
class RawObservation:
    export_run_id: str
    record_pk: str
    param_id: str
    param_kind: str
    categories_raw: str


@dataclass(frozen=True)
class ResolvedParam:
    param_id: str
    param_kind: str
    param_name: Optional[str]
    name_resolved: bool


@dataclass(frozen=True)
class DomainHint:
    target_domain: Optional[str]
    source: str
    verified: bool


@dataclass(frozen=True)
class ParsedCategories:
    category_set: str
    category_ids: Tuple[str, ...]
    category_names: str
    unrecognized_category_ids: str
    has_unextracted_domain: bool
    has_unverified_category_mapping: bool


def warn(message: str) -> None:
    sys.stderr.write(f"WARNING [{STAGE}] {message}\n")


def read_json_required(path: Path, label: str) -> Dict[str, Any]:
    if not path.is_file():
        raise SystemExit(f"ERROR [{STAGE}] required {label} not found: {path}")
    with path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise SystemExit(f"ERROR [{STAGE}] {label} must be a JSON object: {path}")
    return data


def read_json_optional(path: Optional[Path], label: str) -> Dict[str, Any]:
    if path is None:
        return {}
    if not path.is_file():
        warn(f"optional {label} not found at {path}; GUIDs will remain unresolved.")
        return {}
    with path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise SystemExit(f"ERROR [{STAGE}] {label} must be a JSON object: {path}")
    return data


def atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=str(path.parent), suffix=".tmp") as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.DictWriter(tmp, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})
    tmp_path.replace(path)


def load_file_metadata(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.is_file():
        raise SystemExit(f"ERROR [{STAGE}] required file_metadata.csv not found: {path}")
    metadata: Dict[str, Dict[str, str]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        required = {"export_run_id", "client_label", "governance_role", "unit_system"}
        missing = required.difference(fieldnames)
        if missing:
            raise SystemExit(f"ERROR [{STAGE}] file_metadata.csv is missing required columns: {sorted(missing)}")
        for row in reader:
            export_run_id = (row.get("export_run_id") or "").strip()
            if not export_run_id:
                continue
            metadata[export_run_id] = {
                "client_label": (row.get("client_label") or "").strip(),
                "governance_role": (row.get("governance_role") or "").strip(),
                "unit_system": (row.get("unit_system") or "").strip(),
            }
    return metadata


def bool_s(value: bool) -> str:
    return "true" if value else "false"


def find_identity_items_path(identity_items_dir: Path) -> Path:
    candidates = [
        identity_items_dir / "view_filter_definitions.csv",
        identity_items_dir / "view_filter_definitions_identity_items.csv",
    ]
    for path in candidates:
        if path.is_file():
            return path
    raise SystemExit(
        "ERROR [vfd_discover] view_filter_definitions identity_items CSV not found. Checked: "
        + ", ".join(str(p) for p in candidates)
    )


def is_bad_param_id(value: str) -> bool:
    stripped = (value or "").strip()
    return stripped.lower() in SENTINEL_IDS or stripped.startswith("<")


def row_quality(row: Dict[str, str]) -> str:
    return (row.get("item_value_type") or row.get("item_quality") or "").strip()


def is_usable_identity_item_value(item_value: str, quality: str) -> bool:
    stripped = (item_value or "").strip()
    if stripped.upper() in INVALID_ITEM_VALUES:
        return False
    if quality.strip().lower() in INVALID_ITEM_QUALITIES:
        return False
    return True


def canonical_param_kind(param_id: str, raw_kind: str) -> str:
    kind = (raw_kind or "").strip().lower()
    if BIP_RE.match(param_id):
        return "builtin"
    if GUID_RE.match(param_id):
        return "shared"
    if kind in {"builtin", "shared"}:
        return kind
    return "unresolved"


def flush_record(record_key: Optional[Tuple[str, str]], categories_raw: str, rules: Dict[str, Dict[str, str]]) -> Iterator[RawObservation]:
    if record_key is None:
        return
    export_run_id, record_pk = record_key
    for rule in rules.values():
        param_id = (rule.get("id") or "").strip()
        raw_kind = (rule.get("kind") or "").strip()
        if not param_id or "kind" not in rule or "id" not in rule or is_bad_param_id(param_id):
            continue
        yield RawObservation(
            export_run_id=export_run_id,
            record_pk=record_pk,
            param_id=param_id,
            param_kind=canonical_param_kind(param_id, raw_kind),
            categories_raw=categories_raw,
        )


def stream_observations(path: Path) -> Tuple[List[RawObservation], int, Set[str]]:
    observations: List[RawObservation] = []
    rows_read = 0
    export_run_ids: Set[str] = set()
    record_order: List[Tuple[str, str]] = []
    record_states: Dict[Tuple[str, str], Dict[str, Any]] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        required = {"export_run_id", "record_pk", "item_key", "item_value"}
        missing = required.difference(fieldnames)
        if missing:
            raise SystemExit(f"ERROR [{STAGE}] identity_items CSV is missing required columns: {sorted(missing)}")
        if "item_value_type" not in fieldnames and "item_quality" not in fieldnames:
            raise SystemExit(
                f"ERROR [{STAGE}] identity_items CSV is missing required quality column: "
                "expected item_value_type (preferred) or item_quality"
            )

        for row in reader:
            rows_read += 1
            export_run_id = (row.get("export_run_id") or "").strip()
            record_pk = (row.get("record_pk") or "").strip()
            if export_run_id:
                export_run_ids.add(export_run_id)

            item_key = (row.get("item_key") or "").strip()
            match = RULE_KEY_RE.match(item_key)
            if item_key != "vf.categories" and not match:
                continue

            row_key = (export_run_id, record_pk)
            if row_key not in record_states:
                record_order.append(row_key)
                record_states[row_key] = {"categories": "", "rules": defaultdict(dict)}
            state = record_states[row_key]
            item_value = (row.get("item_value") or "").strip()
            if not is_usable_identity_item_value(item_value, row_quality(row)):
                continue

            if item_key == "vf.categories":
                state["categories"] = item_value
            elif match:
                index, leaf = match.groups()
                state["rules"][index][leaf] = item_value

    for record_key in record_order:
        state = record_states[record_key]
        observations.extend(flush_record(record_key, state["categories"], state["rules"]))
    return observations, rows_read, export_run_ids

def resolve_params(
    observations: Sequence[RawObservation],
    bip_lookup: Dict[str, Any],
    shared_param_names: Dict[str, Any],
) -> Dict[str, ResolvedParam]:
    files_by_param: Dict[str, Set[str]] = defaultdict(set)
    kind_by_param: Dict[str, str] = {}
    for obs in observations:
        files_by_param[obs.param_id].add(obs.export_run_id)
        kind_by_param.setdefault(obs.param_id, obs.param_kind)

    resolved: Dict[str, ResolvedParam] = {}
    for param_id in sorted(files_by_param):
        kind = canonical_param_kind(param_id, kind_by_param.get(param_id, ""))
        if BIP_RE.match(param_id):
            name = bip_lookup.get(param_id) or bip_lookup.get(param_id[len("bip:"):])
            if name:
                resolved[param_id] = ResolvedParam(param_id, "builtin", str(name), True)
            else:
                warn(f"{param_id} not in bip_lookup.json ({len(files_by_param[param_id])} files). Extend bip_lookup.json to resolve.")
                resolved[param_id] = ResolvedParam(param_id, "builtin", None, False)
        elif GUID_RE.match(param_id):
            name = shared_param_names.get(param_id) or shared_param_names.get(param_id.lower()) or shared_param_names.get(param_id.upper())
            if name:
                resolved[param_id] = ResolvedParam(param_id, "shared", str(name), True)
            else:
                warn(f"GUID {param_id} unresolved ({len(files_by_param[param_id])} files). Provide --shared-param-names to resolve.")
                resolved[param_id] = ResolvedParam(param_id, "shared", None, False)
        else:
            resolved[param_id] = ResolvedParam(param_id, "unresolved", None, False)
    return resolved


def load_bip_hints(path: Path) -> Dict[str, Any]:
    hints = read_json_required(path, "vfd_bip_target_domain_hints.json")

    exact = hints.get("exact_bip_id", {})
    if isinstance(exact, dict):
        hints["exact_bip_id"] = {
            str(key): value
            for key, value in exact.items()
            if not str(key).startswith("_")
        }

    name_rules = hints.get("name_contains", [])
    if isinstance(name_rules, list):
        filtered_rules = []
        for rule in name_rules:
            if isinstance(rule, dict) and any(str(key).startswith("_comment") for key in rule):
                continue
            filtered_rules.append(rule)
        hints["name_contains"] = filtered_rules
    elif isinstance(name_rules, dict):
        hints["name_contains"] = {
            str(key): value
            for key, value in name_rules.items()
            if not str(key).startswith("_")
        }

    return hints


def hint_target_and_verify(entry: Any) -> Tuple[Optional[str], bool]:
    if isinstance(entry, str):
        return entry, True
    if isinstance(entry, dict):
        target = entry.get("target_domain") or entry.get("domain")
        return (str(target) if target not in (None, "") else None), not bool(entry.get("_verify", False))
    return None, True


def iter_name_contains_rules(hints: Dict[str, Any]) -> Iterator[Tuple[str, Any]]:
    rules = hints.get("name_contains", [])
    if isinstance(rules, dict):
        for substring, entry in rules.items():
            if not str(substring).startswith("_"):
                yield str(substring), entry
    elif isinstance(rules, list):
        for entry in rules:
            if not isinstance(entry, dict):
                continue
            if any(str(key).startswith("_comment") for key in entry):
                continue
            substring = entry.get("substring") or entry.get("contains") or entry.get("name_contains")
            if substring and not str(substring).startswith("_"):
                yield str(substring), entry


def infer_domain(param_id: str, param_name: Optional[str], hints: Dict[str, Any]) -> DomainHint:
    exact = hints.get("exact_bip_id", {})
    if isinstance(exact, dict):
        entry = exact.get(param_id)
        if entry is not None:
            target, verified = hint_target_and_verify(entry)
            return DomainHint(target, "exact_bip_id", verified)

    if param_name:
        param_name_lower = param_name.lower()
        for substring, entry in iter_name_contains_rules(hints):
            if substring.lower() in param_name_lower:
                target, verified = hint_target_and_verify(entry)
                return DomainHint(target, "name_contains", verified)

    return DomainHint(None, "unresolved", True)


def parse_category_tokens(raw: str) -> Optional[List[str]]:
    value = (raw or "").strip()
    if not value:
        return []

    if value.startswith("["):
        try:
            data = json.loads(value)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, list):
            return None
        tokens: List[str] = []
        for item in data:
            if isinstance(item, int):
                tokens.append(str(item))
            elif isinstance(item, str):
                token = item.strip()
                if not re.fullmatch(r"[-+]?\d+", token):
                    return None
                tokens.append(token)
            else:
                return None
        return tokens

    tokens = [part.strip() for part in value.split(",")]
    if tokens and all(token and re.fullmatch(r"[-+]?\d+", token) for token in tokens):
        return tokens
    return None


def sort_category_tokens(tokens: Iterable[str]) -> Tuple[str, ...]:
    return tuple(sorted({str(token).strip() for token in tokens}, key=lambda token: int(token)))


def parse_category_ints(raw: str) -> Optional[List[int]]:
    tokens = parse_category_tokens(raw)
    if tokens is None:
        return None
    return [int(token) for token in tokens]

def category_entry_name(entry: Any) -> Optional[str]:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        name = entry.get("name") or entry.get("category_name") or entry.get("built_in_category") or entry.get("bic")
        return str(name) if name not in (None, "") else None
    return None


def parse_categories(
    raw: str,
    category_map: Dict[str, Any],
    warned_unparseable: Set[str],
    recognized_distinct: Set[int],
    unrecognized_distinct: Set[int],
) -> ParsedCategories:
    parsed = parse_category_tokens(raw)
    if parsed is None:
        if raw not in warned_unparseable:
            warn(f"unparseable vf.categories value {raw!r}; category scope will be empty for matching rows.")
            warned_unparseable.add(raw)
        parsed = []

    category_ids = sort_category_tokens(parsed)
    if "-2000011" in category_map and "-2000011" in category_ids:
        assert "-2000011" in category_map
    names: List[str] = []
    unrecognized: List[int] = []
    has_unextracted = False
    has_unverified = False

    for category_id in category_ids:
        lookup_key = str(category_id).strip()
        entry = category_map.get(lookup_key)
        if entry is None:
            unrecognized.append(int(lookup_key))
            unrecognized_distinct.add(int(lookup_key))
            continue
        recognized_distinct.add(int(lookup_key))
        name = category_entry_name(entry)
        if name:
            names.append(name)
        if isinstance(entry, dict):
            if entry.get("domain_extracted") is False:
                has_unextracted = True
            if entry.get("_verify") is True:
                has_unverified = True

    return ParsedCategories(
        category_set="|".join(category_ids),
        category_ids=category_ids,
        category_names="|".join(names),
        unrecognized_category_ids="|".join(str(i) for i in unrecognized),
        has_unextracted_domain=has_unextracted,
        has_unverified_category_mapping=has_unverified,
    )


def normalize_param_name(name: str) -> str:
    slug = []
    previous_underscore = False
    for ch in name.strip().lower():
        if ch.isalnum():
            slug.append(ch)
            previous_underscore = False
        elif ch.isspace() or ch == "_":
            if not previous_underscore:
                slug.append("_")
                previous_underscore = True
        # other punctuation is stripped, not replaced
    return "".join(slug).strip("_")


def parse_category_set(category_set: str) -> List[int]:
    out: List[int] = []
    for part in (category_set or "").split("|"):
        if part:
            out.append(int(part))
    return out


def _resolve_target_domain_from_categories(
    category_ids: Sequence[int],
    category_file_counts: Dict[str, int],
    category_map: Dict[str, Any],
    support_threshold: int,
) -> Tuple[Optional[str], str]:
    qualifying = [
        category_id for category_id in category_ids
        if category_file_counts.get(str(category_id), 0) >= support_threshold
    ]
    if not qualifying:
        return None, "category_map_no_signal"

    domains: Set[Optional[str]] = set()
    for category_id in qualifying:
        entry = category_map.get(str(category_id))
        target: Optional[str] = None
        if (
            isinstance(entry, dict)
            and entry.get("domain_extracted") is True
            and entry.get("_verify") is not True
        ):
            target = entry.get("target_domain") or None
        domains.add(target)

    if domains == {None}:
        return None, "category_map_no_signal"
    if len(domains) == 1:
        return next(iter(domains)), "category_map_consensus"
    return None, "category_map_conflict"


def _validate_domain_has_identity_items(target_domain: str, identity_items_dir: Path) -> bool:
    shard_path = identity_items_dir / f"{target_domain}.csv"
    if not shard_path.is_file():
        return False
    with shard_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)
        except StopIteration:
            return False
        for _ in reader:
            return True
    return False


def build_inventory_rows(
    observations: Sequence[RawObservation],
    resolved: Dict[str, ResolvedParam],
    hints: Dict[str, Any],
    category_map: Dict[str, Any],
    support_min_files: int,
    identity_items_dir: Optional[Path] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    warned_unparseable: Set[str] = set()
    recognized_distinct: Set[int] = set()
    unrecognized_distinct: Set[int] = set()
    category_cache: Dict[str, ParsedCategories] = {}
    groups: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}

    for obs in observations:
        param = resolved[obs.param_id]
        if obs.categories_raw not in category_cache:
            category_cache[obs.categories_raw] = parse_categories(
                obs.categories_raw, category_map, warned_unparseable, recognized_distinct, unrecognized_distinct,
            )
        cats = category_cache[obs.categories_raw]
        key = (
            obs.param_id,
            param.param_kind,
            param.param_name or "",
            cats.category_set,
        )
        group = groups.setdefault(
            key,
            {
                "param_id": obs.param_id,
                "param_kind": param.param_kind,
                "param_name": param.param_name or "",
                "name_resolved": param.name_resolved,
                "category_set": cats.category_set,
                "category_ids": cats.category_ids,
                "category_names": cats.category_names,
                "unrecognized_category_ids": cats.unrecognized_category_ids,
                "has_unextracted_domain": cats.has_unextracted_domain,
                "has_unverified_category_mapping": cats.has_unverified_category_mapping,
                "export_run_ids": set(),
                "rule_count": 0,
            },
        )
        group["export_run_ids"].add(obs.export_run_id)
        group["rule_count"] += 1

    # Per-param category file support, aggregated across every category_set
    # this param_id was observed under, for category-consensus resolution.
    category_files_by_param: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for group in groups.values():
        for category_id in group["category_ids"]:
            category_files_by_param[group["param_id"]][category_id].update(group["export_run_ids"])
    category_file_counts_by_param: Dict[str, Dict[str, int]] = {
        param_id: {cat_id: len(files) for cat_id, files in cat_counts.items()}
        for param_id, cat_counts in category_files_by_param.items()
    }

    rows: List[Dict[str, Any]] = []
    for group in groups.values():
        param_id = group["param_id"]
        hint = infer_domain(param_id, group["param_name"] or None, hints)
        target_domain = hint.target_domain or ""
        target_domain_source = hint.source
        target_domain_verified = bool(hint.verified)

        if not target_domain:
            category_ids_int = [int(cat_id) for cat_id in group["category_ids"]]
            consensus_domain, consensus_source = _resolve_target_domain_from_categories(
                category_ids_int,
                category_file_counts_by_param.get(param_id, {}),
                category_map,
                support_min_files,
            )
            if consensus_source == "category_map_consensus" and consensus_domain:
                consensus_verified = True
                if identity_items_dir is not None and identity_items_dir.is_dir():
                    consensus_verified = _validate_domain_has_identity_items(consensus_domain, identity_items_dir)
                if consensus_verified:
                    target_domain = consensus_domain
                    target_domain_source = "category_map_consensus"
                    target_domain_verified = True
                else:
                    target_domain = ""
                    target_domain_source = "unresolved"
                    target_domain_verified = True
            else:
                target_domain = ""
                target_domain_source = "unresolved"
                target_domain_verified = True

        file_count = len(group["export_run_ids"])
        meets_threshold = file_count >= support_min_files
        name_resolved = bool(group["name_resolved"])
        rows.append({
            "_export_run_ids": set(group["export_run_ids"]),
            "param_id": param_id,
            "param_kind": group["param_kind"],
            "param_name": group["param_name"],
            "name_resolved": bool_s(name_resolved),
            "target_domain": target_domain,
            "target_domain_source": target_domain_source,
            "target_domain_verified": bool_s(target_domain_verified),
            "category_set": group["category_set"],
            "category_names": group["category_names"],
            "unrecognized_category_ids": group["unrecognized_category_ids"],
            "has_unextracted_domain": bool_s(bool(group["has_unextracted_domain"])),
            "has_unverified_category_mapping": bool_s(bool(group["has_unverified_category_mapping"])),
            "file_count": file_count,
            "rule_count": group["rule_count"],
            "meets_threshold": bool_s(meets_threshold),
            "requires_human_review": bool_s(target_domain == "" and name_resolved),
        })

    rows.sort(key=lambda r: (r["param_kind"], r["param_id"], r["target_domain"], r["category_set"]))
    stats = {
        "recognized_distinct": recognized_distinct,
        "unrecognized_distinct": unrecognized_distinct,
    }
    return rows, stats


def build_edge_rows(
    inventory_rows: Sequence[Dict[str, Any]],
    include_unresolved: bool,
    support_min_files: int,
) -> List[Dict[str, Any]]:
    edge_groups: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    name_domain_param_ids: Dict[Tuple[str, str], Set[str]] = defaultdict(set)

    for row in inventory_rows:
        name_resolved = row["name_resolved"] == "true"
        target_domain = str(row["target_domain"])
        if not name_resolved:
            continue
        if not target_domain and not include_unresolved:
            continue

        normalized = normalize_param_name(str(row["param_name"]))
        if not normalized:
            continue

        param_id = str(row["param_id"])
        edge_domain_component = target_domain or "unresolved"
        key = (normalized, target_domain, param_id)
        name_domain_param_ids[(normalized, target_domain)].add(param_id)
        group = edge_groups.setdefault(
            key,
            {
                "param_id": param_id,
                "param_kind": row["param_kind"],
                "param_name": row["param_name"],
                "normalized": normalized,
                "target_domain": target_domain,
                "edge_domain_component": edge_domain_component,
                "category_files": defaultdict(set),
                "category_rule_counts": defaultdict(int),
                "all_export_run_ids": set(),
                "rule_count": 0,
                "target_domain_sources": [],
                "target_domain_verified": True,
                "requires_human_review": False,
            },
        )

        export_run_ids = set(row.get("_export_run_ids", set()))
        group["all_export_run_ids"].update(export_run_ids)
        group["rule_count"] += int(row["rule_count"])

        category_ids = parse_category_set(str(row["category_set"]))
        for category_id in category_ids:
            group["category_files"][category_id].update(export_run_ids)
            group["category_rule_counts"][category_id] += int(row["rule_count"])

        source = str(row["target_domain_source"])
        if source not in group["target_domain_sources"]:
            group["target_domain_sources"].append(source)
        if row["target_domain_verified"] != "true":
            group["target_domain_verified"] = False
        if row["requires_human_review"] == "true":
            group["requires_human_review"] = True

    for (normalized, target_domain), param_ids in sorted(name_domain_param_ids.items()):
        guid_ids = [param_id for param_id in sorted(param_ids) if GUID_RE.match(param_id)]
        if len(guid_ids) > 1:
            warn(
                f'Multiple param_ids resolve to same normalized name "{normalized}" for target_domain '
                f'"{target_domain or "null"}": {sorted(param_ids)}. Emitting separate param/category scopes; '
                "verify these are the same parameter before manually grouping them."
            )

    rows: List[Dict[str, Any]] = []
    for (normalized, target_domain, param_id), group in sorted(edge_groups.items()):
        supported_category_ids = sorted(
            (
                category_id
                for category_id, files in group["category_files"].items()
                if category_id != "" and len(files) >= support_min_files
            ),
            key=lambda category_id: int(category_id),
        )
        if not supported_category_ids:
            continue

        edge_id = f"vfd.{normalized}__{group['edge_domain_component']}"
        category_file_counts = {
            str(category_id): len(group["category_files"][category_id])
            for category_id in supported_category_ids
        }
        supported_files: Set[str] = set()
        total_rule_count = 0
        for category_id in supported_category_ids:
            supported_files.update(group["category_files"][category_id])
            total_rule_count += int(group["category_rule_counts"][category_id])

        scope_conditions = json.dumps(
            {"param_ids": [param_id], "category_ids": supported_category_ids},
            separators=(",", ":"),
        )
        rows.append({
            "edge_id": edge_id,
            "param_id": param_id,
            "param_kind": group["param_kind"],
            "param_name": group["param_name"],
            "param_name_normalized": normalized,
            "target_domain": target_domain,
            "scope_conditions": scope_conditions,
            "category_file_counts": json.dumps(category_file_counts, separators=(",", ":")),
            "file_count": len(supported_files),
            "rule_count": total_rule_count,
            "name_resolved": "true",
            "target_domain_source": "|".join(group["target_domain_sources"]),
            "target_domain_verified": bool_s(bool(group["target_domain_verified"])),
            "requires_human_review": bool_s(bool(group["requires_human_review"])),
        })
    return rows


def verify_outputs(edge_rows: Sequence[Dict[str, Any]], inventory_rows: Sequence[Dict[str, Any]], total_files: int) -> None:
    for row in edge_rows:
        edge_id = str(row["edge_id"])
        if not EDGE_ID_RE.match(edge_id) or " " in edge_id or "null" in edge_id.lower():
            raise SystemExit(f"ERROR [{STAGE}] invalid edge_id generated: {edge_id}")
        try:
            scope = json.loads(str(row["scope_conditions"]))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"ERROR [{STAGE}] invalid scope_conditions JSON for {edge_id}: {exc}") from exc
        if not isinstance(scope, dict) or not isinstance(scope.get("param_ids"), list) or not isinstance(scope.get("category_ids"), list):
            raise SystemExit(f"ERROR [{STAGE}] scope_conditions missing param_ids/category_ids lists for {edge_id}")
        if int(row["file_count"]) > total_files:
            raise SystemExit(f"ERROR [{STAGE}] edge {edge_id} file_count exceeds total unique export_run_ids")
        if row["name_resolved"] != "true":
            raise SystemExit(f"ERROR [{STAGE}] edge {edge_id} has name_resolved=false")

    if edge_rows and not any(row["param_kind"] == "builtin" and row["name_resolved"] == "true" for row in inventory_rows):
        warn("inventory contains no resolved builtin rows; expected BIP rows will only appear if present in the corpus.")


def print_summary(
    rows_read: int,
    export_run_ids: Set[str],
    observations: Sequence[RawObservation],
    resolved: Dict[str, ResolvedParam],
    inventory_rows: Sequence[Dict[str, Any]],
    edge_rows: Sequence[Dict[str, Any]],
    category_stats: Dict[str, Any],
    support_min_files: int,
    out_dir: Path,
) -> None:
    builtin_obs = [o for o in observations if o.param_kind == "builtin"]
    shared_obs = [o for o in observations if o.param_kind == "shared"]
    unresolved_obs = [o for o in observations if o.param_kind == "unresolved"]
    builtin_params = [p for p in resolved.values() if p.param_kind == "builtin"]
    shared_params = [p for p in resolved.values() if p.param_kind == "shared"]
    bip_resolved = sum(1 for p in builtin_params if p.name_resolved)
    guid_resolved = sum(1 for p in shared_params if p.name_resolved)
    groups_with_domain = sum(1 for r in inventory_rows if r["target_domain"])
    groups_without_domain = len(inventory_rows) - groups_with_domain
    unextracted_edge_candidates = sum(1 for r in edge_rows if any(
        inv["has_unextracted_domain"] == "true"
        and inv["meets_threshold"] == "true"
        and inv["name_resolved"] == "true"
        and (inv["target_domain"] == r["target_domain"] or (not inv["target_domain"] and r["requires_human_review"] == "true"))
        for inv in inventory_rows
    ))

    print("VFD Edge Discovery Summary")
    print("--------------------------")
    print(f"Identity items read:       {rows_read} rows")
    print(f"Unique export_run_ids:     {len(export_run_ids)}")
    print(f"Param refs extracted:      {len(observations)}")
    print(f"  builtin:                 {len(builtin_obs)} ({len({o.param_id for o in builtin_obs})} unique bip_ids)")
    print(f"  shared:                  {len(shared_obs)} ({len({o.param_id for o in shared_obs})} unique GUIDs)")
    print(f"  unresolved:              {len(unresolved_obs)}")
    print("Name resolution:")
    print(f"  BIP resolved:            {bip_resolved}/{len(builtin_params)}  ({len(builtin_params) - bip_resolved} unresolved BIP ints)")
    print(f"  GUID resolved:           {guid_resolved}/{len(shared_params)}  ({len(shared_params) - guid_resolved} unresolved GUIDs)")
    print("Target domain:")
    print(f"  With domain:             {groups_with_domain} groups")
    print(f"  null (classification):   {groups_without_domain} groups")
    print(f"Threshold (>= {support_min_files} files):")
    print(f"  Inventory total:         {len(inventory_rows)} rows")
    print(f"  Edge candidates:         {len(edge_rows)} rows")
    print("Categories:")
    print(f"  Recognized integers:     {len(category_stats['recognized_distinct'])} distinct")
    print(f"  Unrecognized integers:   {len(category_stats['unrecognized_distinct'])} distinct")
    print(f"  Unextracted domains:     {unextracted_edge_candidates} edge candidates")
    print("Output:")
    print(f"  {out_dir / 'vfd_param_inventory.csv'}")
    print(f"  {out_dir / 'vfd_dynamic_edges.csv'}")


def build_unresolved_file_rows(
    observations: Sequence[RawObservation],
    resolved: Dict[str, ResolvedParam],
    file_metadata: Dict[str, Dict[str, str]],
) -> List[Dict[str, Any]]:
    rule_counts: Dict[Tuple[str, str], int] = defaultdict(int)
    files_by_param: Dict[str, Set[str]] = defaultdict(set)

    for obs in observations:
        if not GUID_RE.match(obs.param_id):
            continue
        param = resolved.get(obs.param_id)
        if param is None or param.name_resolved:
            continue
        rule_counts[(obs.param_id, obs.export_run_id)] += 1
        files_by_param[obs.param_id].add(obs.export_run_id)

    warned_missing: Set[str] = set()
    rows: List[Dict[str, Any]] = []
    for (param_id, export_run_id), rule_count in rule_counts.items():
        meta = file_metadata.get(export_run_id)
        if meta is None:
            if export_run_id not in warned_missing:
                warn(f"export_run_id {export_run_id} not found in file_metadata.csv")
                warned_missing.add(export_run_id)
            client_label = "unknown"
            governance_role = "unknown"
            unit_system = "unknown"
        else:
            client_label = meta["client_label"] or "unknown"
            governance_role = meta["governance_role"] or "unknown"
            unit_system = meta["unit_system"] or "unknown"

        rows.append({
            "param_id": param_id,
            "export_run_id": export_run_id,
            "client_label": client_label,
            "governance_role": governance_role,
            "unit_system": unit_system,
            "rule_count": rule_count,
        })

    rows.sort(key=lambda r: (r["param_id"], -len(files_by_param[r["param_id"]]), r["export_run_id"]))
    return rows


def print_unresolved_summary(rows: Sequence[Dict[str, Any]]) -> None:
    distinct_guids: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        distinct_guids[str(row["param_id"])].append(row)

    print("Unresolved GUID file mapping")
    print("----------------------------")
    print(f"Distinct unresolved GUIDs:  {len(distinct_guids)}")
    print(f"Total file×GUID rows:       {len(rows)}")
    print()

    print("Top GUIDs by file count:")
    top_guids = sorted(distinct_guids.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:10]
    for param_id, guid_rows in top_guids:
        clients = sorted({str(r["client_label"]) for r in guid_rows})
        print(f"  {param_id}  {len(guid_rows)} files  clients: {', '.join(clients)}")
    print()

    print("Recommended source files (Template role, highest GUID coverage):")
    rows_by_client: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_client[str(row["client_label"])].append(row)

    for client_label in sorted(rows_by_client):
        client_rows = rows_by_client[client_label]
        guids_by_file: Dict[str, Set[str]] = defaultdict(set)
        role_by_file: Dict[str, str] = {}
        for row in client_rows:
            export_run_id = str(row["export_run_id"])
            guids_by_file[export_run_id].add(str(row["param_id"]))
            role_by_file[export_run_id] = str(row["governance_role"])

        template_files = [f for f in guids_by_file if role_by_file[f].lower() == "template"]
        candidates = template_files or list(guids_by_file)
        best_file = max(candidates, key=lambda f: (len(guids_by_file[f]), f))
        print(f"  client={client_label}  file={best_file}  resolves {len(guids_by_file[best_file])} distinct GUIDs")


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--identity-items-dir", required=True, help="Parent directory containing identity_items shards")
    ap.add_argument("--bip-lookup", required=True, help="Path to bip_lookup.json")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--category-domain-map", default=str(script_dir / "vfd_category_domain_map.json"), help="Path to vfd_category_domain_map.json")
    ap.add_argument("--bip-hints", default=str(script_dir / "vfd_bip_target_domain_hints.json"), help="Path to vfd_bip_target_domain_hints.json")
    ap.add_argument("--shared-param-names", default=None, help="Optional path to shared_param_names.json")
    ap.add_argument("--support-min-files", type=int, default=10, help="Minimum distinct export_run_ids for edge candidates")
    ap.add_argument("--include-unresolved", action="store_true", help="Include target_domain=null rows in vfd_dynamic_edges.csv for review")
    ap.add_argument("--dump-unresolved-files", default=None, help="Optional path for unresolved-GUID file-mapping CSV (vfd_unresolved_files.csv)")
    ap.add_argument("--file-metadata", default=None, help="Path to file_metadata.csv (required when --dump-unresolved-files is set)")
    args = ap.parse_args()
    if args.support_min_files < 1:
        raise SystemExit("ERROR [vfd_discover] --support-min-files must be >= 1")
    if args.dump_unresolved_files and not args.file_metadata:
        raise SystemExit("ERROR [vfd_discover] --file-metadata is required when --dump-unresolved-files is set")
    return args


def main() -> int:
    args = parse_args()
    identity_items_dir = Path(args.identity_items_dir)
    identity_path = find_identity_items_path(identity_items_dir)
    bip_lookup_path = Path(args.bip_lookup)
    out_dir = Path(args.out_dir)
    category_map_path = Path(args.category_domain_map)
    bip_hints_path = Path(args.bip_hints)
    shared_param_names_path = Path(args.shared_param_names) if args.shared_param_names else None

    bip_lookup = read_json_required(bip_lookup_path, "bip_lookup.json")
    category_map = read_json_required(category_map_path, "vfd_category_domain_map.json")
    bip_hints = load_bip_hints(bip_hints_path)
    shared_param_names = read_json_optional(shared_param_names_path, "shared_param_names.json")
    bip_name_to_id = {str(name): str(param_id) for param_id, name in bip_lookup.items()}
    if len(bip_name_to_id) != len(bip_lookup):
        warn("bip_lookup.json contains duplicate BIP names; reverse lookup is non-unique.")

    observations, rows_read, export_run_ids = stream_observations(identity_path)
    resolved = resolve_params(observations, bip_lookup, shared_param_names)
    inventory_rows, category_stats = build_inventory_rows(
        observations, resolved, bip_hints, category_map, args.support_min_files,
        identity_items_dir=identity_items_dir,
    )
    edge_rows = build_edge_rows(inventory_rows, args.include_unresolved, args.support_min_files)
    verify_outputs(edge_rows, inventory_rows, len(export_run_ids))

    atomic_write_csv(out_dir / "vfd_param_inventory.csv", INVENTORY_FIELDS, inventory_rows)
    atomic_write_csv(out_dir / "vfd_dynamic_edges.csv", EDGE_FIELDS, edge_rows)
    print_summary(
        rows_read, export_run_ids, observations, resolved, inventory_rows, edge_rows,
        category_stats, args.support_min_files, out_dir,
    )

    if args.dump_unresolved_files:
        file_metadata = load_file_metadata(Path(args.file_metadata))
        unresolved_rows = build_unresolved_file_rows(observations, resolved, file_metadata)
        atomic_write_csv(Path(args.dump_unresolved_files), UNRESOLVED_FILE_FIELDS, unresolved_rows)
        print()
        print_unresolved_summary(unresolved_rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
