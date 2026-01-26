import argparse
import csv
import glob
import hashlib
import json
import os
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple

DOMAIN = "dimension_types"


def md5_utf8(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def norm_revit_unique_id(uid: Optional[str]) -> Optional[str]:
    """
    Revit UniqueId is typically '{GUID}-{8hex elementId}'.
    GUID itself contains hyphens, so split on the LAST hyphen only.
    """
    if uid is None:
        return None
    if not isinstance(uid, str):
        uid = str(uid)
    uid = uid.strip()
    if "-" not in uid:
        return uid
    head, tail = uid.rsplit("-", 1)
    # tail should be 8 hex chars; if not, we still return head for "best effort" normalization
    return head


def canonical_item_str(k: Any, q: Any, v: Any) -> str:
    # keep it stable and simple; v can be None
    if v is None:
        v_str = "null"
    elif isinstance(v, bool):
        v_str = "true" if v else "false"
    else:
        v_str = str(v)
    return f"{k}={q}:{v_str}"


def semantic_hash_from_identity_items(items: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for it in items:
        k = it.get("k")
        q = it.get("q")
        v = it.get("v")

        if k == "dim_type.uid":
            v = norm_revit_unique_id(v)
        elif k == "dim_type.tick_mark_uid":
            v = norm_revit_unique_id(v)

        parts.append(canonical_item_str(k, q, v))

    joined = "|".join(parts)
    return md5_utf8(joined)


def try_extract_domain_records(root: Any, domain: str) -> Optional[List[Dict[str, Any]]]:
    """
    Flexible extractor to handle different bundle shapes.
    Expected shapes include:
      - root["records"][domain] = [ ... ]
      - root["domains"][domain]["records"] = [ ... ]
      - root[domain] = [ ... ]
    Returns a list of record dicts, or None if not found.
    """
    if isinstance(root, dict):
        # common: records dict
        recs = root.get("records")
        if isinstance(recs, dict):
            val = recs.get(domain)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]

        # sometimes: domains dict
        doms = root.get("domains")
        if isinstance(doms, dict):
            dom = doms.get(domain)
            if isinstance(dom, dict):
                val = dom.get("records")
                if isinstance(val, list):
                    return [x for x in val if isinstance(x, dict)]

        # direct key
        val = root.get(domain)
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]

        # direct key (dict containing records)
        val = root.get(domain)
        if isinstance(val, dict):
            recs = val.get("records")
            if isinstance(recs, list):
                return [x for x in recs if isinstance(x, dict)]

    return None


def iter_json_files(root_dir: str) -> Iterable[str]:
    # recursive .json search
    for p in glob.glob(os.path.join(root_dir, "**", "*.json"), recursive=True):
        yield p


def load_json(path: str) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def compute_population_stats(hashes: List[str]) -> Dict[str, Any]:
    c = Counter(hashes)
    total = sum(c.values())
    unique = len(c)
    top_share = (max(c.values()) / total) if total else 0.0
    # HHI = sum(p_i^2)
    hhi = 0.0
    if total:
        for v in c.values():
            p = v / total
            hhi += p * p
    return {
        "total_records": total,
        "unique_hashes": unique,
        "top_hash_share": round(top_share, 6),
        "hhi": round(hhi, 6),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Folder containing exported fingerprint JSON bundles (recursive).")
    ap.add_argument("--out", default=None, help="Output folder (default: <root>\\_semantic_sig_out).")
    args = ap.parse_args()

    root_dir = args.root
    out_dir = args.out or os.path.join(root_dir, "_semantic_sig_out")
    os.makedirs(out_dir, exist_ok=True)

    per_file_rows: List[Dict[str, Any]] = []

    instance_hashes_all: List[str] = []
    semantic_hashes_all: List[str] = []

    files_scanned = 0
    files_with_domain = 0

    for path in iter_json_files(root_dir):
        files_scanned += 1
        data = load_json(path)
        if data is None:
            continue

        records = try_extract_domain_records(data, DOMAIN)
        if not records:
            continue

        files_with_domain += 1

        inst_hashes: List[str] = []
        sem_hashes: List[str] = []

        for r in records:
            # instance sig hash (if present)
            sig = r.get("sig_hash")
            if isinstance(sig, str) and sig:
                inst_hashes.append(sig)

            # semantic hash from identity_basis.items (if present)
            ib = r.get("identity_basis")
            if isinstance(ib, dict):
                items = ib.get("items")
                if isinstance(items, list) and items and all(isinstance(x, dict) for x in items):
                    sem_hashes.append(semantic_hash_from_identity_items(items))

        # accumulate population
        instance_hashes_all.extend(inst_hashes)
        semantic_hashes_all.extend(sem_hashes)

        # per-file stats
        row = {
            "file": os.path.relpath(path, root_dir),
            "domain_records": len(records),
            "instance_sig_records": len(inst_hashes),
            "semantic_sig_records": len(sem_hashes),
        }
        row.update({f"instance_{k}": v for k, v in compute_population_stats(inst_hashes).items()})
        row.update({f"semantic_{k}": v for k, v in compute_population_stats(sem_hashes).items()})
        per_file_rows.append(row)

    # write per-file CSV
    per_file_csv = os.path.join(out_dir, f"{DOMAIN}.semantic_sig.per_file.csv")
    if per_file_rows:
        fieldnames = list(per_file_rows[0].keys())
        with open(per_file_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(per_file_rows)

    # write population summary CSV
    pop_csv = os.path.join(out_dir, f"{DOMAIN}.semantic_sig.population_summary.csv")
    inst_stats = compute_population_stats(instance_hashes_all)
    sem_stats = compute_population_stats(semantic_hashes_all)

    with open(pop_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "hash_kind",
                "files_scanned",
                "files_with_domain",
                "total_records",
                "unique_hashes",
                "top_hash_share",
                "hhi",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "hash_kind": "instance_sig_hash",
                "files_scanned": files_scanned,
                "files_with_domain": files_with_domain,
                **inst_stats,
            }
        )
        w.writerow(
            {
                "hash_kind": "semantic_sig_hash_guid_only",
                "files_scanned": files_scanned,
                "files_with_domain": files_with_domain,
                **sem_stats,
            }
        )

    print(f"Wrote:\n  {pop_csv}\n  {per_file_csv if per_file_rows else '(no per-file rows)'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
