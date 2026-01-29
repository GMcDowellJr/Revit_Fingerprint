import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import hashlib
import re


_UID_KEY_RE = re.compile(r"uid", re.IGNORECASE)


def _is_uid_like_key(k: str) -> bool:
    return bool(_UID_KEY_RE.search(k or ""))


def _strip_last_dash_suffix(s: str) -> str:
    """
    If s contains dashes, remove the last dash and everything after it.
    Example: 'aaa-bbb-ccc' -> 'aaa-bbb'
    If no dash is present, return s unchanged.
    """
    if not s:
        return s
    i = s.rfind("-")
    return s[:i] if i > -1 else s


def _md5_utf8_join_pipe(parts: List[str]) -> str:
    joined = "|".join(parts)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


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
    for p in root.rglob("*.json"):
        if not p.is_file():
            continue

        name = p.name.lower()

        # Strict: only parse *detail.json artifacts (exclude *index.json)
        if name.endswith(".index.json"):
            continue
        if not name.endswith(".details.json"):
            continue

        paths.append(str(p))

    if not paths:
        raise FileNotFoundError(
            f"No *detail.json files found under {root} (excluding *index.json)"
        )

    return sorted(paths, key=lambda s: str(Path(s).parent).lower())


def _get_contract(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    c = d.get("_contract")
    return c if isinstance(c, dict) else None


def _get_domain_payload(d: Dict[str, Any], domain: str) -> Optional[Dict[str, Any]]:
    v = d.get(domain)
    return v if isinstance(v, dict) else None


def _get_domain_records(d: Dict[str, Any], domain: str) -> List[Dict[str, Any]]:
    payload = _get_domain_payload(d, domain)
    if not isinstance(payload, dict):
        return []
    recs = payload.get("records")
    if not isinstance(recs, list):
        return []
    out: List[Dict[str, Any]] = []
    for r in recs:
        if isinstance(r, dict):
            out.append(r)
    return out


def _iter_domains(d: Dict[str, Any], explicit: Optional[List[str]]) -> List[str]:
    if explicit:
        return [str(x) for x in explicit]

    # Prefer contract domains listing if present.
    c = _get_contract(d)
    if isinstance(c, dict):
        doms = c.get("domains")
        if isinstance(doms, dict):
            return sorted([k for k, v in doms.items() if isinstance(k, str)], key=str.lower)

    # Fallback: any top-level key whose value is an object with a "records" list.
    domains = []
    for k, v in d.items():
        if not isinstance(k, str) or k.startswith("_"):
            continue
        if isinstance(v, dict) and isinstance(v.get("records"), list):
            domains.append(k)
    return sorted(domains, key=str.lower)


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
            for r in recs:
                record_id = _safe_str(r.get("record_id"))
                status = _safe_str(r.get("status"))
                identity_quality = _safe_str(r.get("identity_quality"))
                sig_hash = _safe_str(r.get("sig_hash"))

                # Recompute a "no-UID" signature hash from identity_basis.items
                # without modifying the upstream sig_hash.
                sig_hash_no_uid = ""
                ib = r.get("identity_basis") if isinstance(r.get("identity_basis"), dict) else None
                items = ib.get("items") if isinstance(ib, dict) else None
                if isinstance(items, list):
                    parts: List[str] = []
                    for it in items:
                        if not isinstance(it, dict):
                            continue
                        k = _safe_str(it.get("k"))
                        v = _safe_str(it.get("v"))

                        # Normalize UID-like values by removing the last dash suffix
                        if _is_uid_like_key(k):
                            v = _strip_last_dash_suffix(v)

                        parts.append(f"{k}={v}")

                    # Deterministic order: sort by k then by full part string
                    parts = sorted(parts, key=lambda p: p.lower())
                    sig_hash_no_uid = _md5_utf8_join_pipe(parts)

                records_rows.append({
                    "file_id": file_id,
                    "domain": domain,
                    "record_id": record_id,
                    "status": status,
                    "identity_quality": identity_quality,
                    "sig_hash": sig_hash,
                    "sig_hash_no_uid": sig_hash_no_uid,
                    "label_display": _safe_str(r.get("label", {}).get("display")),
                    "label_quality": _safe_str(r.get("label", {}).get("quality")),
                    "label_provenance": _safe_str(r.get("label", {}).get("provenance")),
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
                                "reason": reason,
                            })

                # identity_items
                ib = r.get("identity_basis") if isinstance(r.get("identity_basis"), dict) else None
                items = ib.get("items") if isinstance(ib, dict) else None
                if isinstance(items, list):
                    for idx, it in enumerate(items):
                        if not isinstance(it, dict):
                            continue
                        items_rows.append({
                            "file_id": file_id,
                            "domain": domain,
                            "record_id": record_id,
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
                ["file_id", "domain", "record_id", "status", "identity_quality", "sig_hash", "sig_hash_no_uid",
                 "label_display", "label_quality", "label_provenance"],
            ))

        if "status_reasons" in emit_set:
            wrote_paths.append(_write_csv(
                "status_reasons.csv",
                reasons_rows,
                ["file_id", "domain", "record_id", "reason"],
            ))

        if "identity_items" in emit_set:
            wrote_paths.append(_write_csv(
                "identity_items.csv",
                items_rows,
                ["file_id", "domain", "record_id", "item_index", "k", "q", "v"],
            ))

        if "label_components" in emit_set:
            wrote_paths.append(_write_csv(
                "label_components.csv",
                label_comp_rows,
                ["file_id", "domain", "record_id", "component_key", "component_value"],
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
                    ["file_id", "domain", "record_id", "status", "identity_quality", "sig_hash", "sig_hash_no_uid",
                     "label_display", "label_quality", "label_provenance"],
                ))

            if "status_reasons" in emit_set:
                dom_reasons = [r for r in reasons_rows if r.get("domain") == dom]
                wrote_paths.append(_write_csv(
                    f"status_reasons__{dom_safe}.csv",
                    dom_reasons,
                    ["file_id", "domain", "record_id", "reason"],
                ))

            if "identity_items" in emit_set:
                dom_items = [r for r in items_rows if r.get("domain") == dom]
                wrote_paths.append(_write_csv(
                    f"identity_items__{dom_safe}.csv",
                    dom_items,
                    ["file_id", "domain", "record_id", "item_index", "k", "q", "v"],
                ))

            if "label_components" in emit_set:
                dom_comps = [r for r in label_comp_rows if r.get("domain") == dom]
                wrote_paths.append(_write_csv(
                    f"label_components__{dom_safe}.csv",
                    dom_comps,
                    ["file_id", "domain", "record_id", "component_key", "component_value"],
                ))

    print("Wrote:")
    for p in wrote_paths:
        print(f"  {p}")



if __name__ == "__main__":
    main()
