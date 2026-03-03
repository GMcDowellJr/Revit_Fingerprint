import argparse
import csv
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from typing import Any, Dict, Iterable, List, Optional, Tuple
import hashlib
import re

from tools.io_export import (
    get_definition_items,
    get_domain_records as io_get_domain_records,
    get_id_join_hash,
    get_id_sig_hash,
    get_top_contract,
    get_record_label,
    iter_domains as io_iter_domains,
)


# UID-ish key detection (best-effort). Used only to normalize UID-like values in identity items.
# Note: exporter contract says sig_hash is already UID-free; this is for legacy/forensic normalization only.
_UID_KEY_RE = re.compile(r"(?:^|[._-])uid(?:$|[._-])|uniqueid|unique_id", re.IGNORECASE)


def _is_scalar(v: Any) -> bool:
    return v is None or isinstance(v, (str, int, float, bool))


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _iter_json_paths(root_dir: str) -> List[str]:
    root = Path(root_dir).resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"Not a directory: {root}")

    paths: List[str] = []

    # Top-level only (non-recursive): avoid ingesting analysis artifacts in subfolders.
    for p in root.glob("*.json"):
        if not p.is_file():
            continue

        name = p.name.lower()
        if name.endswith(".legacy.json"):
            continue

        paths.append(str(p))

    if not paths:
        raise FileNotFoundError(
            f"No export JSON files found in top-level of {root} (excluding *legacy.json)"
        )

    return sorted(paths, key=lambda s: str(Path(s)).lower())


def _get_contract(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return get_top_contract(d)


def _get_domain_payload(d: Dict[str, Any], domain: str) -> Optional[Dict[str, Any]]:
    v = d.get(domain)
    return v if isinstance(v, dict) else None


def _get_domain_records(d: Dict[str, Any], domain: str) -> List[Dict[str, Any]]:
    return io_get_domain_records(d, domain)


def _iter_domains(d: Dict[str, Any], explicit: Optional[List[str]]) -> List[str]:
    if explicit:
        return [str(x) for x in explicit]
    doms = [x for x in io_iter_domains(d) if isinstance(x, str)]
    if doms:
        return sorted(doms, key=str.lower)
    return []


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)
    if not isinstance(d, dict):
        raise TypeError(f"Export JSON root must be an object: {path}")
    return d


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root_dir", required=True, help="Folder containing exported *.json (non-recursive)")
    ap.add_argument("--out_dir", required=True, help="Output folder for CSVs")
    ap.add_argument("--domains", default="", help="Comma-separated domain list (blank = auto-detect per file)")
    ap.add_argument("--file_id_mode", default="basename", choices=["basename", "stem", "fullpath"])
   
    ap.add_argument(
        "--emit",
        default="runs,records,status_reasons,identity_items,label_components",
        help="Comma-separated outputs to write: runs,records,status_reasons,identity_items,label_components",
    )
    ap.add_argument(
        "--split_by_domain",
        action="store_true",
        help="If set, write domain-scoped CSVs (e.g. records__<domain>.csv) instead of combined files.",
    )
    ap.add_argument(
        "--synthetic_domains",
        default="",
        help=(
            "Optional comma-separated domains for synthetic key augmentation after CSV export "
            "(currently supports: line_patterns)."
        ),
    )

    args = ap.parse_args()
    
    root_dir = str(Path(args.root_dir).resolve())
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    explicit_domains = [x.strip() for x in args.domains.split(",") if x.strip()] or None

    # Accumulators
    runs_rows: List[Dict[str, str]] = []
    records_rows: List[Dict[str, str]] = []
    reasons_rows: List[Dict[str, str]] = []
    items_rows: List[Dict[str, str]] = []
    label_comp_rows: List[Dict[str, str]] = []
    seen_file_ids: set[str] = set()

    for path in _iter_json_paths(root_dir):
        data = _read_json(path)

        if args.file_id_mode == "basename":
            file_id = Path(path).name
        elif args.file_id_mode == "stem":
            file_id = Path(path).stem
        else:
            file_id = str(Path(path).resolve())

        # Collision-safe file_id: if two different files share the same basename/stem,
        # append a short hash of the full resolved path.
        if file_id in seen_file_ids:
            path_hash = hashlib.md5(str(Path(path).resolve()).encode("utf-8")).hexdigest()[:8]
            file_id = f"{file_id}|{path_hash}"
        seen_file_ids.add(file_id)

        c = _get_contract(data) or {}
        runs_rows.append({
            "file_id": file_id,
            "path": str(Path(path).resolve()),
            "schema_version": _safe_str(c.get("schema_version")),
            "run_status": _safe_str(c.get("run_status")),
            "hash_mode": _safe_str(data.get("_hash_mode")),
            "tool_version": _safe_str(data.get("tool_version") or data.get("version") or data.get("_tool_version")),
        })

        domains = _iter_domains(data, explicit_domains)

        for domain in domains:
            recs = _get_domain_records(data, domain)
            for rec_ordinal, r in enumerate(recs):
                record_ordinal = f"{rec_ordinal:06d}"
                record_pk = f"{file_id}|{domain}|{record_ordinal}"

                record_id = _safe_str(r.get("record_id") or r.get("id") or r.get("name"))
                status = _safe_str(r.get("status"))
                identity_quality = _safe_str(r.get("identity_quality"))
                sig_hash = _safe_str(get_id_sig_hash(r))

                join_key = r.get("join_key")

                join_hash = _safe_str(get_id_join_hash(r))
                join_key_schema = _safe_str(join_key.get("schema")) if isinstance(join_key, dict) else ""

                # Trust upstream exporter: sig_hash is already UID-free by contract.

                label_obj = r.get("label") if isinstance(r.get("label"), dict) else {}
                records_rows.append({
                    "file_id": file_id,
                    "domain": domain,
                    "record_id": record_id,
                    "record_ordinal": record_ordinal,
                    "record_pk": record_pk,
                    "status": status,
                    "identity_quality": identity_quality,
                    "sig_hash": sig_hash,
                    "join_hash": join_hash,
                    "join_key_schema": join_key_schema,
                    "label_display": _safe_str(label_obj.get("display") or get_record_label(r)),
                    "label_quality": _safe_str(label_obj.get("quality")),
                    "label_provenance": _safe_str(label_obj.get("provenance")),
                })



                # status_reasons
                sr = r.get("status_reasons")
                if isinstance(sr, list):
                    for reason in sr:
                        if isinstance(reason, str) and reason:
                            reasons_rows.append({
                                "file_id": file_id,
                                "domain": domain,
                                "record_id": record_id,
                                "record_ordinal": record_ordinal,
                                "record_pk": record_pk,
                                "reason": reason,
                            })

                # identity_items
                items = get_definition_items(r)
                if isinstance(items, list):
                    for idx, it in enumerate(items):
                        if not isinstance(it, dict):
                            continue
                        items_rows.append({
                            "file_id": file_id,
                            "domain": domain,
                            "record_id": record_id,
                            "record_ordinal": record_ordinal,
                            "record_pk": record_pk,
                            "item_index": str(idx),
                            "k": _safe_str(it.get("k")),
                            "q": _safe_str(it.get("q")),
                            "v": _safe_str(it.get("v")),
                        })
                        
                label = r.get("label")

                # label.components (best-effort)
                comps = label.get("components") if isinstance(label, dict) else None
                if isinstance(comps, dict):
                    for ck, cv in comps.items():
                        if not isinstance(ck, str) or not ck:
                            continue
                        # keep it BI-friendly: only scalars; stringify otherwise
                        if not _is_scalar(cv):
                            cv = json.dumps(cv, ensure_ascii=False, sort_keys=True)
                        label_comp_rows.append({
                            "file_id": file_id,
                            "domain": domain,
                            "record_id": record_id,
                            "record_ordinal": record_ordinal,
                            "record_pk": record_pk,
                            "component_key": ck,
                            "component_value": _safe_str(cv),
                        })


    def _write_csv(name: str, rows: List[Dict[str, str]], fieldnames: List[str]) -> str:
        p = out_dir / name
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in fieldnames})
        return str(p)

    def _safe_name(s: str) -> str:
        s = (s or "").strip()
        if not s:
            return "unknown"
        # filesystem-safe-ish
        return (
            s.replace(" ", "_")
             .replace("/", "_")
             .replace("\\", "_")
             .replace(":", "_")
        )

    emit_set = {x.strip() for x in str(args.emit).split(",") if x.strip()}
    synthetic_domains = {x.strip() for x in str(args.synthetic_domains).split(",") if x.strip()}

    wrote_paths: List[str] = []

    if not args.split_by_domain:
        if "runs" in emit_set:
            wrote_paths.append(_write_csv(
                "runs.csv",
                runs_rows,
                ["file_id", "path", "schema_version", "run_status", "hash_mode", "tool_version"],
            ))

        if "records" in emit_set:
            wrote_paths.append(_write_csv(
                "records.csv",
                records_rows,
                ["file_id", "domain", "record_id", "record_ordinal", "record_pk",
                 "status", "identity_quality", "sig_hash",
                 "join_hash", "join_key_schema",
                 "label_display", "label_quality", "label_provenance"],
            ))

        if "status_reasons" in emit_set:
            wrote_paths.append(_write_csv(
                "status_reasons.csv",
                reasons_rows,
                ["file_id", "domain", "record_id", "record_ordinal", "record_pk", "reason"],
            ))

        if "identity_items" in emit_set:
            wrote_paths.append(_write_csv(
                "identity_items.csv",
                items_rows,
                ["file_id", "domain", "record_id", "record_ordinal", "record_pk", "item_index", "k", "q", "v"],
            ))

        if "label_components" in emit_set:
            wrote_paths.append(_write_csv(
                "label_components.csv",
                label_comp_rows,
                ["file_id", "domain", "record_id", "record_ordinal", "record_pk", "component_key", "component_value"],
            ))

    else:
        # domain-split outputs
        domains_seen = sorted({r.get("domain", "") for r in records_rows if r.get("domain")}, key=str.lower)

        if "runs" in emit_set:
            # runs are not domain-scoped; still write once
            wrote_paths.append(_write_csv(
                "runs.csv",
                runs_rows,
                ["file_id", "path", "schema_version", "run_status", "hash_mode", "tool_version"],
            ))

        for dom in domains_seen:
            dom_safe = _safe_name(dom)

            if "records" in emit_set:
                dom_records = [r for r in records_rows if r.get("domain") == dom]
                wrote_paths.append(_write_csv(
                    f"records__{dom_safe}.csv",
                    dom_records,
                    ["file_id", "domain", "record_id", "record_ordinal", "record_pk",
                     "status", "identity_quality", "sig_hash",
                     "join_hash", "join_key_schema",
                     "label_display", "label_quality", "label_provenance"],
                ))


            if "status_reasons" in emit_set:
                dom_reasons = [r for r in reasons_rows if r.get("domain") == dom]
                wrote_paths.append(_write_csv(
                    f"status_reasons__{dom_safe}.csv",
                    dom_reasons,
                    ["file_id", "domain", "record_id", "record_ordinal", "record_pk", "reason"],
                ))


            if "identity_items" in emit_set:
                dom_items = [r for r in items_rows if r.get("domain") == dom]
                wrote_paths.append(_write_csv(
                    f"identity_items__{dom_safe}.csv",
                    dom_items,
                    ["file_id", "domain", "record_id", "record_ordinal", "record_pk", "item_index", "k", "q", "v"],
                ))


            if "label_components" in emit_set:
                dom_comps = [r for r in label_comp_rows if r.get("domain") == dom]
                wrote_paths.append(_write_csv(
                    f"label_components__{dom_safe}.csv",
                    dom_comps,
                    ["file_id", "domain", "record_id", "record_ordinal", "record_pk", "component_key", "component_value"],
                ))


    print("Wrote:")
    for p in wrote_paths:
        print(f"  {p}")

    # Optional synthetic-key augmentation step for discovery workflows.
    # This runs only when identity_items outputs exist and the requested synthetic domain file was written.
    if synthetic_domains:
        try:
            import pandas as pd
            from tools.flatten.utils.compute_synthetic_keys import compute_synthetic_keys, EXPECTED_COLUMNS
        except Exception as ex:
            print(
                "WARNING: synthetic key augmentation skipped "
                f"(unable to import pandas/compute_synthetic_keys: {ex})",
                file=sys.stderr,
            )
            return

        synthetic_wrote: List[str] = []

        if args.split_by_domain:
            for dom in sorted(synthetic_domains):
                dom_safe = _safe_name(dom)
                in_path = out_dir / f"identity_items__{dom_safe}.csv"
                out_path = out_dir / f"identity_items__{dom_safe}__augmented.csv"
                if not in_path.exists():
                    continue
                if dom != "line_patterns":
                    print(f"WARNING: synthetic domain not supported yet: {dom}", file=sys.stderr)
                    continue

                items_df = pd.read_csv(str(in_path), dtype=str, keep_default_na=False)
                synthetic_df, _ok, _missing = compute_synthetic_keys(items_df, dom)
                augmented_df = pd.concat([items_df[EXPECTED_COLUMNS], synthetic_df], ignore_index=True)
                augmented_df.to_csv(str(out_path), index=False)
                synthetic_wrote.append(str(out_path))
        else:
            # Combined identity_items.csv can still be augmented per-domain into a domain-named output.
            in_path = out_dir / "identity_items.csv"
            if in_path.exists():
                items_df = pd.read_csv(str(in_path), dtype=str, keep_default_na=False)
                for dom in sorted(synthetic_domains):
                    if dom != "line_patterns":
                        print(f"WARNING: synthetic domain not supported yet: {dom}", file=sys.stderr)
                        continue
                    out_path = out_dir / f"identity_items__{_safe_name(dom)}__augmented.csv"
                    synthetic_df, _ok, _missing = compute_synthetic_keys(items_df, dom)
                    augmented_df = pd.concat([items_df[EXPECTED_COLUMNS], synthetic_df], ignore_index=True)
                    augmented_df.to_csv(str(out_path), index=False)
                    synthetic_wrote.append(str(out_path))

        if synthetic_wrote:
            print("Wrote synthetic augmentations:")
            for p in synthetic_wrote:
                print(f"  {p}")



if __name__ == "__main__":
    main()
