from __future__ import annotations

import argparse
import csv
import itertools
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

STATUS_ALLOWED = {"ok", "degraded"}


@dataclass(frozen=True)
class DomainSimilarityRow:
    file_a: str
    file_b: str
    domain: str
    comparable: bool
    not_comparable_reason: Optional[str]
    a_total: Optional[int]
    b_total: Optional[int]
    matched: Optional[int]
    added_in_b: Optional[int]
    removed_from_a: Optional[int]
    union_mass: Optional[int]
    set_jaccard: Optional[float]
    multiset_jaccard: Optional[float]


def _set_jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    sa = set(a)
    sb = set(b)
    if not sa and not sb:
        return 1.0
    return len(sa & sb) / len(sa | sb)


def _multiset_jaccard(ca: Counter, cb: Counter) -> Tuple[float, int, int, int, int]:
    keys = set(ca) | set(cb)
    if not keys:
        return 1.0, 0, 0, 0, 0
    matched = 0
    union_mass = 0
    added_in_b = 0
    removed_from_a = 0
    for k in keys:
        a = ca.get(k, 0)
        b = cb.get(k, 0)
        matched += min(a, b)
        union_mass += max(a, b)
        if b > a:
            added_in_b += b - a
        if a > b:
            removed_from_a += a - b
    score = (matched / union_mass) if union_mass else 1.0
    return score, matched, added_in_b, removed_from_a, union_mass


def _load_metadata(path: Path) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            export_run_id = (row.get("export_run_id") or "").strip()
            if not export_run_id:
                continue
            out[export_run_id] = row
    return out


def _load_records_grouped(path: Path) -> Dict[Tuple[str, str], List[str]]:
    grouped: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            status = (row.get("status") or "").strip().lower()
            sig_hash = (row.get("sig_hash") or "").strip()
            export_run_id = (row.get("export_run_id") or "").strip()
            domain = (row.get("domain") or "").strip()
            if not export_run_id or not domain:
                continue
            if status not in STATUS_ALLOWED:
                continue
            if not sig_hash:
                continue
            grouped[(export_run_id, domain)].append(sig_hash)
    return grouped


def _pair_type(metadata: Dict[str, dict], file_a: str, file_b: str) -> str:
    role_a = (metadata.get(file_a, {}).get("governance_role") or "Unknown").strip() or "Unknown"
    role_b = (metadata.get(file_b, {}).get("governance_role") or "Unknown").strip() or "Unknown"
    lo, hi = (role_a, role_b) if role_a <= role_b else (role_b, role_a)
    return f"{lo} vs {hi}"


def _passes_filters(meta: dict, roles: Optional[set], unit_system: Optional[str], clients: Optional[set]) -> bool:
    if roles is not None and (meta.get("governance_role") or "") not in roles:
        return False
    if unit_system is not None and (meta.get("unit_system") or "") != unit_system:
        return False
    if clients is not None and (meta.get("client_label") or "") not in clients:
        return False
    return True


def _build_file_universe(
    metadata: Dict[str, dict],
    grouped: Dict[Tuple[str, str], List[str]],
    roles: Optional[set],
    unit_system: Optional[str],
    clients: Optional[set],
) -> List[str]:
    file_ids_from_records = {file_id for (file_id, _domain) in grouped.keys()}
    file_ids: List[str] = []

    for file_id in sorted(file_ids_from_records):
        meta = metadata.get(file_id)
        # Keep record-backed files even when metadata is missing; only apply
        # filters when metadata exists (except when filters are explicitly provided).
        if meta is None:
            if roles is not None or unit_system is not None or clients is not None:
                continue
            file_ids.append(file_id)
            continue

        if _passes_filters(meta, roles, unit_system, clients):
            file_ids.append(file_id)

    return file_ids


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute pairwise domain similarity from flattened CSV inputs.")
    parser.add_argument("--records", required=True, help="Path to records.csv")
    parser.add_argument("--metadata", required=True, help="Path to file_metadata.csv")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument("--roles", nargs="+", help="Optional governance_role filter")
    parser.add_argument("--unit-system", help="Optional unit_system filter")
    parser.add_argument("--client", nargs="+", help="Optional client_label filter")
    parser.add_argument("--log-every", type=int, default=100, help="Log every N pairs")
    args = parser.parse_args()

    metadata = _load_metadata(Path(args.metadata))
    grouped = _load_records_grouped(Path(args.records))
    domains_by_file: Dict[str, set] = defaultdict(set)
    for file_id, domain in grouped:
        domains_by_file[file_id].add(domain)

    roles = set(args.roles) if args.roles else None
    clients = set(args.client) if args.client else None
    unit_system = args.unit_system

    file_ids = _build_file_universe(metadata, grouped, roles, unit_system, clients)
    pair_count = math.comb(len(file_ids), 2)
    print(f"[similarity_compare] files={len(file_ids)} pairs={pair_count}")

    domain_rows: List[DomainSimilarityRow] = []
    pairwise_rows: List[dict] = []

    for idx, (file_a, file_b) in enumerate(itertools.combinations(file_ids, 2), start=1):
        domains_a = domains_by_file.get(file_a, set())
        domains_b = domains_by_file.get(file_b, set())
        domains = sorted(domains_a | domains_b)

        comparable_rows: List[DomainSimilarityRow] = []

        for domain in domains:
            sig_a = grouped.get((file_a, domain), [])
            sig_b = grouped.get((file_b, domain), [])

            if not sig_a or not sig_b:
                row = DomainSimilarityRow(file_a, file_b, domain, False, "missing_in_one_file", len(sig_a), len(sig_b), None, None, None, None, None, None)
                domain_rows.append(row)
                continue

            ca = Counter(sig_a)
            cb = Counter(sig_b)
            ms_score, matched, added_in_b, removed_from_a, union_mass = _multiset_jaccard(ca, cb)
            set_score = _set_jaccard(ca.keys(), cb.keys())
            row = DomainSimilarityRow(
                file_a=file_a,
                file_b=file_b,
                domain=domain,
                comparable=True,
                not_comparable_reason=None,
                a_total=len(sig_a),
                b_total=len(sig_b),
                matched=matched,
                added_in_b=added_in_b,
                removed_from_a=removed_from_a,
                union_mass=union_mass,
                set_jaccard=set_score,
                multiset_jaccard=ms_score,
            )
            domain_rows.append(row)
            comparable_rows.append(row)

        domains_compared = len(comparable_rows)
        if domains_compared:
            total_weight = sum(r.union_mass or 0 for r in comparable_rows)
            similarity_multiset = sum((r.multiset_jaccard or 0.0) * (r.union_mass or 0) for r in comparable_rows) / total_weight if total_weight else None
            similarity_set = sum(r.set_jaccard or 0.0 for r in comparable_rows) / domains_compared
        else:
            similarity_multiset = None
            similarity_set = None

        pairwise_rows.append({
            "file_a": file_a,
            "file_b": file_b,
            "pair_type": _pair_type(metadata, file_a, file_b),
            "domains_compared": domains_compared,
            "similarity_multiset": similarity_multiset,
            "similarity_set": similarity_set,
        })

        if idx % max(args.log_every, 1) == 0:
            print(f"[similarity_compare] processed_pairs={idx}/{pair_count}")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    domain_out = out_dir / "domain_similarity.csv"
    with domain_out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "file_a", "file_b", "domain", "comparable", "not_comparable_reason", "a_total", "b_total",
            "matched", "added_in_b", "removed_from_a", "union_mass", "set_jaccard", "multiset_jaccard",
        ])
        writer.writeheader()
        for r in domain_rows:
            writer.writerow(r.__dict__)

    pair_out = out_dir / "pairwise_similarity.csv"
    with pair_out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "file_a", "file_b", "pair_type", "domains_compared", "similarity_multiset", "similarity_set",
        ])
        writer.writeheader()
        writer.writerows(pairwise_rows)

    print(f"[similarity_compare] wrote {domain_out} and {pair_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
