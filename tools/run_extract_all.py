#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from emit_element_dominance import emit_element_dominance
from extractor import emit_analysis, emit_records
from bundle_analysis.common import atomic_write_csv, read_csv_rows
from bundle_analysis.reference_bundle import write_sidecar
from core.sig_hash_policy import load_sig_hash_policies, get_domain_sig_hash_policy
from core.sig_hash_builder import build_sig_hash_from_policy

try:
    csv.field_size_limit(sys.maxsize)
except Exception:
    pass

SUPPRESSED_DOWNSTREAM_DOMAINS = {"object_styles_imported"}


_LP_SEGMENT_KEY_RE = re.compile(r"^line_pattern\.(?:seg|segment)\[(\d{3})\]\.(kind|length)$")
_LP_SEGMENT_COUNT_KEY = "line_pattern.segment_count"


def _rebuild_monolithic_identity_items(items_csv: Path, shard_dir: Path) -> None:
    """Rebuild identity_items.csv by streaming all domain shards in sorted order and refresh sentinel."""
    fieldnames: Optional[List[str]] = None
    with items_csv.open("w", encoding="utf-8", newline="") as mono_f:
        mono_writer: Optional[csv.DictWriter] = None
        for shard in sorted(shard_dir.glob("*.csv")):
            with shard.open("r", encoding="utf-8-sig", newline="") as sf:
                reader = csv.DictReader(sf)
                if fieldnames is None:
                    fieldnames = list(reader.fieldnames or [])
                    mono_writer = csv.DictWriter(mono_f, fieldnames=fieldnames)
                    mono_writer.writeheader()
                if mono_writer is not None:
                    for row in reader:
                        mono_writer.writerow({k: row.get(k, "") for k in fieldnames})
    (shard_dir / ".complete").write_text(str(items_csv.stat().st_mtime), encoding="utf-8")


def _append_line_pattern_synthetic_norm_hash(items_csv: Path) -> Dict[str, int]:
    """Append synthetic line_pattern.segments_norm_hash rows to identity_items.csv."""
    if not items_csv.is_file():
        return {"total": 0, "ok": 0, "missing": 0}

    shard_dir = items_csv.parent / "identity_items_by_domain"
    lp_shard = shard_dir / "line_patterns.csv"
    use_shard = (shard_dir / ".complete").is_file() and lp_shard.is_file()

    if use_shard:
        with lp_shard.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
            fieldnames = list(rows[0].keys() if rows else [
                "schema_version", "export_run_id", "domain", "record_pk",
                "item_key", "item_value", "item_value_type", "item_role",
            ])
    else:
        with items_csv.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
            fieldnames = list((rows[0].keys() if rows else [
                "schema_version", "export_run_id", "file_id", "domain", "record_id", "record_ordinal", "record_pk", "item_index", "k", "q", "v",
            ]))
    key_col = "k" if "k" in fieldnames else "item_key"
    quality_col = "q" if "q" in fieldnames else "item_value_type"
    value_col = "v" if "v" in fieldnames else "item_value"
    item_index_col = "item_index" if "item_index" in fieldnames else "item_role"

    grouped: Dict[str, List[Dict[str, str]]] = {}
    already_augmented: set = set()
    for r in rows:
        if str(r.get("domain", "")) != "line_patterns":
            continue
        pk = str(r.get("record_pk", ""))
        if str(r.get(key_col, "")) == "line_pattern.segments_norm_hash":
            already_augmented.add(pk)
        grouped.setdefault(pk, []).append(r)

    out_rows: List[Dict[str, str]] = []
    ok = 0
    missing = 0
    for record_pk, group in grouped.items():
        if record_pk in already_augmented:
            continue
        seg_rows = [r for r in group if _LP_SEGMENT_KEY_RE.match(str(r.get(key_col, "")))]
        status = "ok"
        hash_v = ""

        if not seg_rows:
            seg_count_rows = [r for r in group if str(r.get(key_col, "")) == _LP_SEGMENT_COUNT_KEY]
            seg_count_v = str(seg_count_rows[0].get(value_col, "")).strip() if seg_count_rows else ""
            seg_count_q = str(seg_count_rows[0].get(quality_col, "")).strip() if seg_count_rows else ""
            try:
                seg_count_is_zero = int(seg_count_v) == 0
            except Exception:
                seg_count_is_zero = False

            if seg_count_q == "ok" and seg_count_is_zero:
                hash_v = hashlib.md5("segment_count=0".encode("utf-8")).hexdigest()
                ok += 1
            else:
                status = "missing"
                missing += 1
        elif any(str(r.get(quality_col, "")) != "ok" for r in seg_rows):
            status = "missing"
            missing += 1
        else:
            segments: Dict[int, Dict[str, float]] = {}
            parse_error = False
            for r in seg_rows:
                m = _LP_SEGMENT_KEY_RE.match(str(r.get(key_col, "")))
                if not m:
                    continue
                idx = int(m.group(1))
                key = m.group(2)
                segments.setdefault(idx, {})
                try:
                    if key == "kind":
                        segments[idx]["kind"] = int(str(r.get(value_col, "")))
                    else:
                        segments[idx]["length"] = float(str(r.get(value_col, "")))
                except Exception:
                    parse_error = True
                    break

            if parse_error or any("kind" not in d or "length" not in d for d in segments.values()):
                status = "missing"
                missing += 1
            else:
                ordered = [(idx, int(v["kind"]), float(v["length"])) for idx, v in sorted(segments.items())]
                non_dot_total = sum(length for _, kind, length in ordered if kind != 2)
                has_non_dot = any(kind != 2 for _, kind, _ in ordered)
                dot_count = sum(1 for _, kind, _ in ordered if kind == 2)
                eff_total = non_dot_total if has_non_dot else float(dot_count)
                tokens: List[str] = []
                for idx, kind, length in ordered:
                    if kind == 2:
                        eff_length = 0.0 if has_non_dot else 1.0
                    else:
                        eff_length = length
                    norm = (eff_length / eff_total) if eff_total > 0 else 0.0
                    tokens.append(f"seg[{idx:03d}].kind={kind}")
                    tokens.append(f"seg[{idx:03d}].norm_length={norm:.6f}")
                hash_v = hashlib.md5("|".join(tokens).encode("utf-8")).hexdigest()
                ok += 1

        base = group[0]
        out_rows.append({
            "schema_version": str(base.get("schema_version", "")),
            "export_run_id": str(base.get("export_run_id", "")),
            "file_id": str(base.get("file_id", "")),
            "domain": "line_patterns",
            "record_id": str(base.get("record_id", "")),
            "record_ordinal": str(base.get("record_ordinal", "")),
            "record_pk": record_pk,
            item_index_col: "synthetic",
            key_col: "line_pattern.segments_norm_hash",
            quality_col: status,
            value_col: hash_v,
        })

    if out_rows:
        rows.extend(out_rows)
        rows = sorted(
            rows,
            key=lambda r: (
                str(r.get("export_run_id", "")),
                str(r.get("domain", "")),
                str(r.get("record_pk", "")),
                str(r.get(key_col, "")),
                str(r.get(value_col, "")),
            ),
        )
        if use_shard:
            with lp_shard.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                for r in rows:
                    w.writerow({k: r.get(k, "") for k in fieldnames})
            _rebuild_monolithic_identity_items(items_csv, shard_dir)
        else:
            with items_csv.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                for r in rows:
                    w.writerow({k: r.get(k, "") for k in fieldnames})

    return {"total": len(out_rows), "ok": ok, "missing": missing}


def _discover_domains_from_exports(exports_dir: Path) -> List[str]:
    """
    Best-effort discovery of domains from fingerprint JSON exports.
    Assumes domains are top-level keys excluding meta keys (leading underscore) and known non-domain keys.
    Deterministic: returns sorted list.
    """
    exports_dir = Path(exports_dir)
    domains: set[str] = set()

    # Prefer fingerprint files; fall back to generic .json if none found.
    candidates = sorted(exports_dir.glob("*__fingerprint.json"))
    if not candidates:
        candidates = [p for p in exports_dir.glob("*.json") if not p.name.lower().endswith(".legacy.json")]
    if not candidates:
        return []

    for p in candidates[:200]:
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        if not isinstance(data, dict):
            continue

        for k, v in data.items():
            if not isinstance(k, str):
                continue
            if k.startswith("_"):
                continue
            if k in ("artifacts",):
                continue
            # Domain payloads are typically dict-like.
            if isinstance(v, dict):
                domains.add(k)

    return sorted(domains, key=lambda s: s.lower())


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _resolve_sig_hash_policy_path(explicit: Optional[str], out_root: Path) -> Optional[Path]:
    if explicit:
        p = Path(explicit).resolve()
        return p if p.is_file() else None
    candidate1 = (out_root / "results" / "policies" / "domain_sig_hash_policies.json").resolve()
    if candidate1.is_file():
        return candidate1
    # CWD-relative (works when invoked from repo root)
    candidate2 = Path("policies/domain_sig_hash_policies.json").resolve()
    if candidate2.is_file():
        return candidate2
    # Repo-root-relative (works regardless of CWD)
    candidate3 = (REPO_ROOT / "policies" / "domain_sig_hash_policies.json").resolve()
    if candidate3.is_file():
        return candidate3
    return None


def _apply_sig_hash_to_phase0(phase0_dir: Path, policy_path: Path, domains: Optional[List[str]] = None):
    policies = load_sig_hash_policies(str(policy_path))
    dom_filter = set(domains or [])
    diag = {
        "policy_path": str(policy_path),
        "files_processed": 0,
        "records_processed": 0,
        "records_hashed": 0,
        "domains_without_policy": [],
        "records_blocked": 0,
        "records_degraded": 0,
    }
    domains_without = set()
    records_csv = phase0_dir / "records.csv"
    items_csv = phase0_dir / "identity_items.csv"
    if not records_csv.is_file() or not items_csv.is_file():
        return diag
    records = _read_csv_rows(records_csv)
    shard_dir = _ensure_domain_scoped_identity_items(phase0_dir)
    grouped_cache: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    def _load_items_for_domain(domain: str) -> Dict[str, List[Dict[str, Any]]]:
        if domain in grouped_cache:
            return grouped_cache[domain]
        out: Dict[str, List[Dict[str, Any]]] = {}
        src = (shard_dir / f"{domain}.csv") if shard_dir is not None else items_csv
        if not src.is_file():
            grouped_cache[domain] = out
            return out
        for r in _iter_csv_rows(src):
            if str(r.get("domain", "")).strip() != domain:
                continue
            pk = str(r.get("record_pk", ""))
            k = str(r.get("item_key", "") or r.get("k", ""))
            if not pk or not k:
                continue
            out.setdefault(pk, []).append({"k": k, "v": r.get("v", r.get("item_value")), "q": r.get("q", r.get("item_value_type"))})
        grouped_cache[domain] = out
        return out
    basis_item_rows: List[Dict[str, str]] = []
    for row in records:
        dom = str(row.get("domain", "")).strip()
        if dom_filter and dom not in dom_filter:
            continue
        pol = get_domain_sig_hash_policy(policies, dom)
        if not isinstance(pol, dict):
            domains_without.add(dom)
            continue
        pk = str(row.get("record_pk", ""))
        rec_items = _load_items_for_domain(dom).get(pk, [])
        diag["records_processed"] += 1
        sig_hash, status, reasons, hash_items = build_sig_hash_from_policy(
            domain_policy=pol,
            items=rec_items,
            status_reasons=[],
        )
        row["sig_hash"] = "" if sig_hash is None else str(sig_hash)
        if str(row.get("join_key_schema", "")) == "sig_hash_as_join_key.v1":
            row["join_hash"] = row["sig_hash"]
        prior_status = str(row.get("status", "")).strip()
        if prior_status == "blocked":
            # Extractor-blocked records are sticky — the sig_hash stage cannot upgrade them.
            # Exception: records blocked by a *prior apply run* must be re-evaluated so that
            # policy corrections or updated identity_items can take effect.
            # Distinguisher: the apply stage writes "identity.incomplete:required_not_ok:<k>";
            # extractors write "identity.incomplete:<q>:<k>" (e.g. "identity.incomplete:missing:…").
            # Matching on the apply-specific "required_not_ok" middle segment avoids
            # misclassifying genuine extractor blocks as apply-stage blocks.
            prior_reasons = [r for r in str(row.get("status_reasons", "")).split("|") if r]
            apply_stage_blocked = any(r.startswith("identity.incomplete:required_not_ok:") for r in prior_reasons)
            if not apply_stage_blocked:
                pass  # genuine extractor block — preserve it
            else:
                row["status"] = str(status)
                row["status_reasons"] = "|".join(reasons)
        else:
            row["status"] = str(status)
            row["status_reasons"] = "|".join(reasons)
        row["sig_basis_schema"] = str(pol.get("sig_hash_schema") or "")
        for ordinal, it in enumerate(hash_items):
            k = it.get("k")
            if isinstance(k, str) and k:
                basis_item_rows.append({"record_pk": pk, "domain": dom, "item_key": k, "ordinal": str(ordinal)})
        if sig_hash is not None:
            diag["records_hashed"] += 1
        if status == "blocked":
            diag["records_blocked"] += 1
        elif status == "degraded":
            diag["records_degraded"] += 1
    if records:
        fieldnames = list(records[0].keys())
        for extra in ("sig_hash", "sig_basis_schema", "status", "status_reasons"):
            if extra not in fieldnames:
                fieldnames.append(extra)
        # Drop sig_basis_keys_used if it was written by a prior run; key traceability
        # is now in sig_basis_items.csv which is more query-friendly at scale.
        fieldnames = [f for f in fieldnames if f != "sig_basis_keys_used"]
        with records_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in records:
                w.writerow({k: r.get(k, "") for k in fieldnames})
    if basis_item_rows:
        basis_csv = phase0_dir / "sig_basis_items.csv"
        basis_fields = ["record_pk", "domain", "item_key", "ordinal"]
        with basis_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=basis_fields)
            w.writeheader()
            for r in basis_item_rows:
                w.writerow(r)
        diag["sig_basis_items_written"] = len(basis_item_rows)
    diag["files_processed"] = 1
    diag["domains_without_policy"] = sorted(domains_without)
    if domains_without:
        sys.stderr.write(
            "[WARN extract_all] sig_hash stage: {} domain(s) have no policy entry — "
            "sig_hash will be empty for their records: {}\n".format(
                len(domains_without), ", ".join(sorted(domains_without))
            )
        )
    return diag


def _run(cmd: List[str], *, env: Dict[str, str]) -> None:
    start = time.time()
    print(f"[extract_all] RUN: {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True, env=env)
    print(f"[extract_all] DONE ({time.time() - start:.1f}s): {cmd[1] if len(cmd) > 1 else cmd[0]}", flush=True)


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): "" if v is None else str(v) for k, v in row.items()} for row in csv.DictReader(f)]


def _iter_csv_rows(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            yield {str(k): "" if v is None else str(v) for k, v in row.items()}


def _ensure_domain_scoped_identity_items(phase0_dir: Path) -> Optional[Path]:
    src = phase0_dir / "identity_items.csv"
    if not src.is_file():
        return None

    shard_dir = phase0_dir / "identity_items_by_domain"
    shard_dir.mkdir(parents=True, exist_ok=True)
    sentinel = shard_dir / ".complete"

    try:
        if sentinel.is_file():
            stored = sentinel.read_text(encoding="utf-8").strip()
            if stored == str(src.stat().st_mtime):
                return shard_dir
    except OSError:
        pass

    for old in shard_dir.glob("*.csv"):
        old.unlink(missing_ok=True)

    handles: Dict[str, Any] = {}
    writers: Dict[str, csv.DictWriter] = {}
    try:
        with src.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            if not fieldnames:
                return shard_dir
            for row in reader:
                domain = str(row.get("domain", "")).strip()
                if not domain:
                    continue
                if domain not in writers:
                    fp = (shard_dir / f"{domain}.csv").open("w", encoding="utf-8", newline="")
                    handles[domain] = fp
                    w = csv.DictWriter(fp, fieldnames=fieldnames)
                    w.writeheader()
                    writers[domain] = w
                writers[domain].writerow({k: row.get(k, "") for k in fieldnames})
    finally:
        for fp in handles.values():
            fp.close()

    sentinel.write_text(str(src.stat().st_mtime), encoding="utf-8")
    return shard_dir


def _validate_line_pattern_synthetic_norm_hash(phase0_dir: Path) -> None:
    records_csv = phase0_dir / "records.csv"
    items_csv = phase0_dir / "identity_items.csv"
    if not records_csv.is_file() or not items_csv.is_file():
        raise SystemExit("flatten/enrichment stage did not produce records.csv and identity_items.csv before apply")
    line_pattern_pks: List[str] = []
    for r in _iter_csv_rows(records_csv):
        if str(r.get("domain", "")) == "line_patterns":
            line_pattern_pks.append(r.get("record_pk", ""))
    if not line_pattern_pks:
        return
    pks_with_norm: set = set()
    for r in _iter_csv_rows(items_csv):
        key = str(r.get("item_key", "") or r.get("k", ""))
        if str(r.get("domain", "")) == "line_patterns" and key == "line_pattern.segments_norm_hash":
            pks_with_norm.add(str(r.get("record_pk", "")))
    missing = [pk for pk in line_pattern_pks if pk not in pks_with_norm]
    if missing:
        sample = ",".join(missing[:10])
        more = "" if len(missing) <= 10 else f" (+{len(missing)-10} more)"
        raise SystemExit(
            "flatten/enrichment stage did not produce synthetic norm hashes before apply: "
            f"missing line_pattern.segments_norm_hash for {len(missing)} line_patterns records. "
            f"sample_record_pks={sample}{more}"
        )


def _emit_join_policy_diagnostics(rows: List[Dict[str, str]], diagnostics_dir: Path, domains: Optional[List[str]] = None) -> List[Dict[str, str]]:
    import csv

    dom_filter = set(domains or [])
    problems: List[Dict[str, str]] = []
    for r in rows:
        dom = str(r.get("domain", "")).strip()
        if dom_filter and dom not in dom_filter:
            continue
        schema = str(r.get("join_key_schema", "")).strip()
        status = str(r.get("join_key_status", "")).strip()
        if schema == "sig_hash_as_join_key.v1" or status != "ok":
            problems.append(
                {
                    "domain": dom,
                    "file_id": str(r.get("file_id", "")),
                    "record_pk": str(r.get("record_pk", "")),
                    "join_key_schema": schema,
                    "join_key_status": status,
                    "reason": "bootstrap_schema" if schema == "sig_hash_as_join_key.v1" else "non_ok_status",
                }
            )
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    out_csv = diagnostics_dir / "join_policy_gate_diagnostics.csv"
    fields = ["domain", "file_id", "record_pk", "join_key_schema", "join_key_status", "reason"]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in sorted(problems, key=lambda x: (x["domain"], x["file_id"], x["record_pk"])):
            w.writerow(row)
    return problems


def _detect_surfaces(exports_dir: Path) -> Dict[str, int]:
    names = [p.name for p in exports_dir.iterdir() if p.is_file() and p.name.lower().endswith(".json")]
    details = sum(1 for n in names if n.lower().endswith(".details.json"))
    index = sum(1 for n in names if n.lower().endswith(".index.json"))
    legacy = sum(1 for n in names if n.lower().endswith(".legacy.json"))
    fingerprint = sum(1 for n in names if n.lower().endswith("__fingerprint.json"))
    plain = len(names) - details - index - legacy - fingerprint
    return {
        "details": details,
        "index": index,
        "legacy": legacy,
        "fingerprint_json": fingerprint,
        "plain_json": plain,
        "total_json": len(names),
    }


def _merge_index_details(index_fp: Dict[str, Any], details_fp: Dict[str, Any]) -> Dict[str, Any]:
    """Merge index (metadata) and details (domain payloads) into a single fingerprint object."""
    merged = {**index_fp}
    for key, value in details_fp.items():
        # Domain payloads don't start with underscore; index metadata does
        if not key.startswith("_") and key not in merged:
            merged[key] = value
    return merged


def _pick_sample_file(exports_dir: Path) -> Tuple[Optional[Path], Optional[Path]]:
    """Pick sample files for domain inference.

    Priority order:
      1. *__fingerprint.json monolithic exports
      2. *.details.json / *.index.json split exports
      3. other non-legacy *.json files
      4. *.legacy.json files

    Returns (index_path, details_path) tuple. Both may be None if no files found.
    For split exports, returns both index and details paths.
    For monolithic, plain, or legacy exports, returns (path, None).
    """
    fingerprints = sorted(exports_dir.glob("*__fingerprint.json"))
    if fingerprints:
        return (fingerprints[0], None)

    details = sorted(exports_dir.glob("*.details.json"))
    index = sorted(exports_dir.glob("*.index.json"))

    if index and details:
        # Split export: return first matching pair
        index_by_stem = {p.stem.lower().replace('.index', ''): p for p in index}
        details_by_stem = {p.stem.lower().replace('.details', ''): p for p in details}
        for stem in sorted(index_by_stem.keys()):
            if stem in details_by_stem:
                return (index_by_stem[stem], details_by_stem[stem])
        # Fallback: return first index even without matching details
        return (index[0], details_by_stem.get(index[0].stem.lower().replace('.index', '')))

    if index:
        return (index[0], None)

    if details:
        return (None, details[0])

    plain = sorted([p for p in exports_dir.glob("*.json") if not (p.name.lower().endswith(".legacy.json") or p.name.lower().endswith("__fingerprint.json"))])
    if plain:
        return (plain[0], None)

    legacy = sorted(exports_dir.glob("*.legacy.json"))
    if legacy:
        return (legacy[0], None)

    return (None, None)


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"JSON root must be object: {path}")
    return data


def _infer_domains(exports_dir: Path) -> List[str]:
    """Infer domain names from sample export files.

    Handles split exports by merging index + details for reliable domain discovery.
    """
    index_path, details_path = _pick_sample_file(exports_dir)

    if index_path is None and details_path is None:
        return []

    # Load and potentially merge files
    fp: Dict[str, Any] = {}
    if index_path and details_path:
        # Split export: merge index + details
        sys.stderr.write("[INFO run_extract_all] Found split exports. Merging index + details for domain inference.\n")
        index_fp = _read_json(index_path)
        details_fp = _read_json(details_path)
        fp = _merge_index_details(index_fp, details_fp)
    elif index_path:
        fp = _read_json(index_path)
    elif details_path:
        fp = _read_json(details_path)

    # Try contract first (most reliable)
    c = fp.get("_contract")
    if isinstance(c, dict):
        doms = c.get("domains")
        if isinstance(doms, dict):
            return sorted([str(k) for k in doms.keys()])

    # Try _domains (back-compat surface)
    d = fp.get("_domains")
    if isinstance(d, dict):
        return sorted([str(k) for k in d.keys()])

    # Fallback: scan top-level keys for domain-like payloads
    out: List[str] = []
    for k, v in fp.items():
        if not isinstance(k, str) or k.startswith("_"):
            continue
        if isinstance(v, dict) and (("records" in v) or ("status" in v) or ("domain_version" in v)):
            out.append(k)
    return sorted(out)


def _parse_stage_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [s.strip().lower() for s in str(raw).split(',') if s.strip()]


def _warn_deprecated_alias(flag: str, replacement: str) -> None:
    sys.stderr.write(f"[WARN extract_all] Deprecated alias: use {replacement} instead of {flag}.\n")


def _enforce_policy_gate(rows: List[Dict[str, str]], diagnostics_dir: Path, domains: Optional[List[str]], allow_sig_hash_join_key: bool) -> None:
    problems = _emit_join_policy_diagnostics(rows, diagnostics_dir, domains)
    if problems and not allow_sig_hash_join_key:
        raise SystemExit(
            "Join-policy gate failed: identity-mode join keys detected (join_key_schema=sig_hash_as_join_key.v1 or join_key_status!=ok). "
            "Re-run with --stages flatten,discover,apply,split (or include authority/patterns with apply), "
            "or use --allow-sig-hash-join-key for degraded exploratory analysis. "
            f"Diagnostics: {diagnostics_dir / 'join_policy_gate_diagnostics.csv'}"
        )
    if problems and allow_sig_hash_join_key:
        sys.stderr.write("\n" + "!" * 80 + "\n")
        sys.stderr.write("[WARN extract_all] --allow-sig-hash-join-key enabled; proceeding with DEGRADED identity-mode clustering (not for governance conclusions).\n")
        sys.stderr.write(f"[WARN extract_all] Diagnostics: {diagnostics_dir / 'join_policy_gate_diagnostics.csv'}\n")
        sys.stderr.write("!" * 80 + "\n\n")


def main() -> None:
    stage_names = ["flatten", "sig_hash", "discover", "apply", "placeholders", "authority", "patterns", "split", "flat_tables"]
    ap = argparse.ArgumentParser(
        description=(
            "Pipeline orchestrator with explicit stages: flatten (T0), sig_hash (T0.5), discover (T1), apply (T2), split, authority, patterns. "
            "Default stages are flatten,sig_hash,discover."
        ),
        epilog=(
            "Examples:\n"
            "  default (draft prep): --stages flatten,discover\n"
            "  governance prep:      --stages sig_hash,flatten,discover\n"
            "  operational commit:  --stages flatten,discover,apply\n"
            "  placeholder prep:    --stages flatten,discover,apply,placeholders\n"
            "  analysis after apply: --stages flatten,discover,apply,placeholders,split,authority,patterns\n"
            "  degraded exploratory analysis (not governance-grade): add --allow-sig-hash-join-key\n"
            "  matrix reference: docs/extract_stage_matrix.md"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ap.add_argument("exports_dir", help="Folder containing fingerprint exports (*__fingerprint.json, or legacy *.details.json / *.index.json).")
    ap.add_argument("--out-root", required=True, help="Output root folder.")
    ap.add_argument("--seed", default=None, help="Path to a seed fingerprint JSON. When provided, emits a seed-comparison sidecar for the seed-baseline BI dashboard (project drift vs template). Not part of standard segment runs.")
    ap.add_argument("--domains", default=None, help="Comma list of domains; if omitted, infer from exports.")
    ap.add_argument("--stages", default="flatten,sig_hash,discover", help="Comma-separated stages to run. Default: flatten,sig_hash,discover.")
    ap.add_argument("--skip-stages", default="", help="Comma-separated stages to skip from --stages.")
    ap.add_argument("--join-policy", default=None, help="Policy JSON path used by apply stage.")
    ap.add_argument("--sig-hash-policy", default=None, help="Policy JSON path used by sig_hash stage.")
    ap.add_argument("--skip-sig-hash-missing-policy", action="store_true", help="Skip sig_hash stage if no policy file is found.")
    ap.add_argument("--allow-sig-hash-join-key", action="store_true", help="Allow degraded identity-mode join keys (sig_hash_as_join_key.v1) for exploratory analysis.")
    ap.add_argument("--split-domains", nargs="?", const="__ALL__", default=None, help="Domains for split stage; optional CSV. If no value, run all discovered domains.")
    ap.add_argument(
        "--flat-tables-emit",
        default="layer_stacks",
        help="Comma-separated emit types for the flat_tables stage. Used primarily for compound type layer stack export (layer_stacks, layer_stack_rows). Default is layer_stacks.",
    )
    ap.add_argument(
        "--filter-export-run-ids",
        default=None,
        help="Path to a text file with one export_run_id per line. "
             "Filters meta_rows and record_rows to the specified population "
             "before running authority/patterns stages. "
             "flatten/apply/placeholders/split stages are unaffected."
    )
    ap.add_argument(
        "--records-dir",
        default=None,
        help="Path to directory containing records.csv and file_metadata.csv. "
             "Overrides the default {out-root}/results/records/ for authority/patterns stages. "
             "Use when running per-segment analysis where records live at corpus level."
    )
    ap.add_argument(
        "--label-synth-dir",
        default=None,
        help="Path to a label_synthesis/ directory to use as the read source for label "
             "population, LLM cache, and curator annotations. Overrides the default "
             "{out-root}/results/label_synthesis/ for the read path only — analysis outputs "
             "still write to {out-root}/results/. Use when running per-segment analysis so "
             "that corpus-level LLM improvements are picked up without rebuilding per segment."
    )
    args = ap.parse_args()

    allow_sig_hash_join_key = args.allow_sig_hash_join_key
    require_join_policy = True
    label_synth_dir = Path(args.label_synth_dir).resolve() if args.label_synth_dir else None

    selected_stages = _parse_stage_csv(args.stages) or ["flatten", "sig_hash", "discover"]

    skipped = set(_parse_stage_csv(args.skip_stages))
    for st in selected_stages + list(skipped):
        if st not in stage_names:
            raise SystemExit(f"Unknown stage: {st}. Valid stages: {','.join(stage_names)}")
    selected_stages = [s for s in stage_names if s in selected_stages and s not in skipped]
    if "apply" in selected_stages and "flatten" not in selected_stages:
        selected_stages = ["flatten"] + selected_stages
        report_note = "auto_inserted_flatten_for_apply"
    else:
        report_note = None
    if "apply" in selected_stages and "sig_hash" not in selected_stages:
        insert_at = selected_stages.index("apply")
        selected_stages.insert(insert_at, "sig_hash")
        report_note = (report_note + "|auto_inserted_sig_hash_for_apply") if report_note else "auto_inserted_sig_hash_for_apply"

    plan_msg = " -> ".join([s if s in selected_stages else f"({s} skipped)" for s in stage_names])
    if require_join_policy and any(s in selected_stages for s in ("split", "authority", "patterns")) and "apply" not in selected_stages:
        plan_msg += " -> (analysis gated: requires policy join keys; include apply stage)"
    print(f"Plan: {plan_msg}")

    exports_dir = Path(args.exports_dir).resolve()
    out_root = Path(args.out_root).resolve()
    v21_root = out_root / "results"
    v21_phase0_dir = v21_root / "records"
    effective_phase0_dir = v21_phase0_dir
    v21_analysis_dir = v21_root / "analysis"
    v21_split_root = v21_root / "split_analysis"
    flat_tables_dir = v21_root / "flat_tables"

    _ensure_dir(out_root)
    surfaces = _detect_surfaces(exports_dir)

    if args.domains and str(args.domains).strip():
        domains = [d.strip() for d in str(args.domains).split(",") if d.strip()]
    else:
        domains = _infer_domains(exports_dir)
    active_domains = [d for d in domains if d not in SUPPRESSED_DOWNSTREAM_DOMAINS]
    suppressed_domains = sorted(set(domains) - set(active_domains))
    if suppressed_domains:
        sys.stderr.write(
            f"[INFO extract_all] suppressed_downstream_domains={','.join(suppressed_domains)}\n"
        )
    if not domains and any(s in selected_stages for s in ("patterns",)):
        raise SystemExit("No domains inferred; provide --domains.")

    env = os.environ.copy()
    report: Dict[str, Any] = {"tool": "tools/run_extract_all.py", "exports_dir": str(exports_dir), "out_root": str(out_root), "surfaces": surfaces, "domains": domains, "active_domains": active_domains, "selected_stages": selected_stages, "commands": [], "notes": []}
    if report_note:
        report["notes"].append(report_note)
    meta_rows: List[Dict[str, str]] = []
    record_rows: List[Dict[str, str]] = []

    if "flatten" in selected_stages:
        print("[extract_all] Stage flatten (T0): emitting flatten outputs...", flush=True)
        _ensure_dir(v21_phase0_dir)
        report["commands"].append({"stage": "flatten", "out": str(v21_phase0_dir)})
        file_count, record_count = emit_records(exports_dir, v21_phase0_dir, file_id_mode="basename")
        print(f"[extract_all] Stage flatten complete: rows={record_count} files={file_count} out={v21_phase0_dir}", flush=True)
        items_csv = v21_phase0_dir / "identity_items.csv"
        stats = _append_line_pattern_synthetic_norm_hash(items_csv)
        print(
            f"[extract_all] line_patterns segments_norm_hash: "
            f"total={stats['total']} ok={stats['ok']} missing={stats['missing']}",
            flush=True,
        )
    if "sig_hash" in selected_stages:
        _records_csv = effective_phase0_dir / "records.csv"
        _items_csv = effective_phase0_dir / "identity_items.csv"
        if not _records_csv.is_file() or not _items_csv.is_file():
            raise SystemExit(
                "sig_hash stage requires records.csv and identity_items.csv to exist. "
                "Run the flatten stage first, or include flatten in --stages."
            )
        sig_pol = _resolve_sig_hash_policy_path(args.sig_hash_policy, out_root)
        if sig_pol is None:
            if args.skip_sig_hash_missing_policy:
                sys.stderr.write(
                    "[WARN extract_all] sig_hash stage skipped: no policy file found. "
                    "sig_hash and join_hash will be empty for all records.\n"
                )
                report["notes"].append("sig_hash stage skipped: no policy found")
            else:
                raise SystemExit("sig_hash stage requested but no policy file found. Use --sig-hash-policy or --skip-sig-hash-missing-policy.")
        else:
            print(f"[extract_all] Stage sig_hash (T0.5): applying policy {sig_pol.name} ...", flush=True)
            diag = _apply_sig_hash_to_phase0(effective_phase0_dir, sig_pol, active_domains or domains)
            diag_dir = v21_root / "diagnostics"
            _ensure_dir(diag_dir)
            diag_path = diag_dir / "sig_hash_policy_diagnostics.json"
            diag_path.write_text(json.dumps(diag, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(
                f"[extract_all] Stage sig_hash complete: "
                f"processed={diag['records_processed']} hashed={diag['records_hashed']} "
                f"blocked={diag['records_blocked']} degraded={diag['records_degraded']} "
                f"basis_items={diag.get('sig_basis_items_written', 0)} "
                f"domains_without_policy={len(diag['domains_without_policy'])}",
                flush=True,
            )
            report["commands"].append({"stage": "sig_hash", "policy": str(sig_pol), "out": str(effective_phase0_dir), "diagnostics": str(diag_path)})

    if "discover" in selected_stages:
        print("[extract_all] Stage discover (T1): exploring join/sig hash policy candidates...", flush=True)
        cmd_discover = [
            sys.executable,
            "tools/discover_hash_policy.py",
            "--phase0-dir",
            str(effective_phase0_dir),
            "--discovery-target",
            "both",
            "--search-modes",
            "greedy,pareto",
            "--policy-modes",
            "discover,validate,harsh",
        ]
        if args.domains and str(args.domains).strip():
            cmd_discover += ["--domains", str(args.domains)]
        report["commands"].append({"stage": "discover", "cmd": cmd_discover})
        _run(cmd_discover, env=env)

    if "apply" in selected_stages:
        print("[extract_all] Stage apply (T2): applying join policy to flatten outputs...", flush=True)
        items_csv = effective_phase0_dir / "identity_items.csv"
        stats = _append_line_pattern_synthetic_norm_hash(items_csv)
        print(
            f"[extract_all] line_patterns segments_norm_hash (pre-apply): "
            f"total={stats['total']} ok={stats['ok']} missing={stats['missing']}",
            flush=True,
        )
        _validate_line_pattern_synthetic_norm_hash(effective_phase0_dir)
        print(f"[apply] using enriched records dir: {effective_phase0_dir}", flush=True)
        policy_path = Path(args.join_policy).resolve() if args.join_policy else (v21_root / "policies" / "domain_join_key_policies.v21.json").resolve()
        cmd_apply = [sys.executable, "tools/apply_join_policy.py", "--phase0-dir", str(effective_phase0_dir), "--join-policy", str(policy_path)]
        report["commands"].append({"stage": "apply", "cmd": cmd_apply})
        _run(cmd_apply, env=env)

    if "placeholders" in selected_stages:
        print("[extract_all] Stage placeholders (T2b): generating placeholder exclusion CSVs...", flush=True)
        cmd_ph = [sys.executable, "tools/bundle_analysis/placeholder_exclusions.py", "--phase0-dir", str(v21_phase0_dir), "--policies-dir", "policies", "--out-dir", str(v21_root / "placeholder_exclusions"), "--file-metadata-path", str(v21_phase0_dir / "file_metadata.csv")]
        report["commands"].append({"stage": "placeholders", "cmd": cmd_ph})
        try:
            _run(cmd_ph, env=env)
        except Exception as e:
            sys.stderr.write("[WARN extract_all] placeholders stage failed; continuing: {}\n".format(e))

    if "authority" in selected_stages or "patterns" in selected_stages:
        records_source_dir = Path(args.records_dir).resolve() if args.records_dir else v21_phase0_dir
        phase0_records_csv = records_source_dir / "records.csv"
        if phase0_records_csv.is_file():
            # Always reload from disk here so analyze uses post-apply join_hash values,
            # not in-memory rows captured before join policy application.
            record_rows = _read_csv_rows(phase0_records_csv)
        if (records_source_dir / "file_metadata.csv").is_file():
            meta_rows = _read_csv_rows(records_source_dir / "file_metadata.csv")
        # Snapshot pre-filter rows so split domain auto-discovery (which runs after
        # this block) uses the full post-reload population regardless of filter.
        _pre_filter_record_rows = record_rows

        if args.filter_export_run_ids:
            _filter_path = Path(args.filter_export_run_ids)
            if not _filter_path.is_file():
                raise SystemExit(f"--filter-export-run-ids file not found: {_filter_path}")
            _allowed = {
                line.strip() for line in _filter_path.read_text(encoding="utf-8-sig").splitlines()
                if line.strip()
            }
            meta_rows = [r for r in meta_rows if r.get("export_run_id", "").strip() in _allowed]
            record_rows = [r for r in record_rows if r.get("export_run_id", "").strip() in _allowed]
            print(
                f"[extract_all] export_run_id filter applied: "
                f"meta_rows={len(meta_rows)} record_rows={len(record_rows)}",
                flush=True,
            )
            if not meta_rows or not record_rows:
                _sample_allowed = sorted(_allowed)[:5]
                _meta_ids = sorted({r.get("export_run_id", "").strip() for r in _read_csv_rows(records_source_dir / "file_metadata.csv")})[:5] if (records_source_dir / "file_metadata.csv").is_file() else []
                sys.stderr.write(
                    f"[WARN extract_all] filter left meta_rows={len(meta_rows)} record_rows={len(record_rows)} — "
                    f"emit_analysis will be skipped and pattern_presence_file.csv will NOT be written.\n"
                    f"[WARN extract_all] records_source_dir={records_source_dir}\n"
                    f"[WARN extract_all] filter_file={_filter_path} (first 5 IDs: {_sample_allowed})\n"
                    f"[WARN extract_all] file_metadata.csv first 5 export_run_ids: {_meta_ids}\n"
                )

        if require_join_policy and phase0_records_csv.is_file():
            _enforce_policy_gate(record_rows, v21_root / "diagnostics", active_domains, allow_sig_hash_join_key)

        if meta_rows and record_rows:
            shard_dir = _ensure_domain_scoped_identity_items(v21_phase0_dir)
            if shard_dir is not None:
                report["notes"].append(f"identity_items_shards={shard_dir}")

            # Ensure modal label population artifacts exist for the active v2.1 emit path.
            cmd_label_pop = [
                sys.executable,
                "tools/label_synthesis/build_label_population.py",
                "--out-root",
                str(out_root),
            ]
            if args.records_dir:
                cmd_label_pop += ["--records-dir", str(records_source_dir)]
            report["commands"].append({"stage": "analyze", "cmd": cmd_label_pop})
            _run(cmd_label_pop, env=env)

            _ensure_dir(v21_analysis_dir)
            seed_export_run_id = ""
            seed_path = Path(args.seed).resolve() if args.seed else None
            if seed_path is not None:
                candidate_ids = sorted(
                    {
                        str(r.get("export_run_id", "")).strip()
                        for r in meta_rows
                        if str(r.get("file_id", "")).strip() == seed_path.name
                    }
                )
                if len(candidate_ids) != 1:
                    raise ValueError(
                        f"Expected exactly one export_run_id for seed file {seed_path.name!r}; found {candidate_ids}"
                    )
                seed_export_run_id = candidate_ids[0]

            if seed_export_run_id:
                full_seed_dir = v21_analysis_dir / "_seed_full"
                _ensure_dir(full_seed_dir)
                emit_analysis(
                    meta_rows,
                    record_rows,
                    full_seed_dir,
                    phase0_dir=v21_phase0_dir,
                    results_v21_dir=v21_root,
                    label_synth_dir=label_synth_dir,
                )
                corpus_meta_rows = [r for r in meta_rows if str(r.get("export_run_id", "")).strip() != seed_export_run_id]
                corpus_record_rows = [r for r in record_rows if str(r.get("export_run_id", "")).strip() != seed_export_run_id]
                analysis_run_id = emit_analysis(
                    corpus_meta_rows,
                    corpus_record_rows,
                    v21_analysis_dir,
                    phase0_dir=v21_phase0_dir,
                    results_v21_dir=v21_root,
                    label_synth_dir=label_synth_dir,
                )

                corpus_domain_patterns = read_csv_rows(v21_analysis_dir / "domain_patterns.csv")
                full_domain_patterns = read_csv_rows(full_seed_dir / "domain_patterns.csv")
                full_presence = read_csv_rows(full_seed_dir / "pattern_presence_file.csv")
                seed_pattern_keys = {
                    (str(r.get("domain", "")).strip(), str(r.get("pattern_id", "")).strip())
                    for r in full_presence
                    if str(r.get("export_run_id", "")).strip() == seed_export_run_id
                    and str(r.get("domain", "")).strip()
                    and str(r.get("pattern_id", "")).strip()
                }
                merged_domain_patterns: List[Dict[str, str]] = []
                existing_keys: set[Tuple[str, str]] = set()
                for row in corpus_domain_patterns:
                    key = (str(row.get("domain", "")).strip(), str(row.get("pattern_id", "")).strip())
                    existing_keys.add(key)
                    new_row = dict(row)
                    new_row["is_seed"] = "true" if key in seed_pattern_keys else "false"
                    merged_domain_patterns.append(new_row)
                for row in full_domain_patterns:
                    key = (str(row.get("domain", "")).strip(), str(row.get("pattern_id", "")).strip())
                    if key in existing_keys or key not in seed_pattern_keys:
                        continue
                    new_row = dict(row)
                    new_row["analysis_run_id"] = analysis_run_id
                    new_row["is_seed"] = "true"
                    merged_domain_patterns.append(new_row)

                merged_domain_patterns.sort(
                    key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("pattern_id", ""))
                )
                if merged_domain_patterns:
                    fieldnames = list(merged_domain_patterns[0].keys())
                    if "is_seed" not in fieldnames:
                        fieldnames.append("is_seed")
                    atomic_write_csv(v21_analysis_dir / "domain_patterns.csv", fieldnames, merged_domain_patterns)

                schema_version = read_csv_rows(v21_analysis_dir / "corpus_manifest.csv")[0].get("schema_version", "")
                seed_sidecar_rows = [
                    {
                        "domain": dom,
                        "pattern_id": pid,
                        "is_seed": "true",
                        "seed_file_stem": seed_path.stem if seed_path is not None else "",
                    }
                    for dom, pid in sorted(seed_pattern_keys)
                ]
                sidecar_path = write_sidecar(v21_analysis_dir, seed_export_run_id, seed_sidecar_rows, schema_version)
                print(f"[extract] Seed reference bundle written to {sidecar_path}")
            else:
                analysis_run_id = emit_analysis(
                    meta_rows,
                    record_rows,
                    v21_analysis_dir,
                    phase0_dir=v21_phase0_dir,
                    results_v21_dir=v21_root,
                    label_synth_dir=label_synth_dir,
                )
                domain_patterns = read_csv_rows(v21_analysis_dir / "domain_patterns.csv")
                for row in domain_patterns:
                    row["is_seed"] = "false"
                if domain_patterns:
                    fieldnames = list(domain_patterns[0].keys())
                    if "is_seed" not in fieldnames:
                        fieldnames.append("is_seed")
                    atomic_write_csv(v21_analysis_dir / "domain_patterns.csv", fieldnames, domain_patterns)
            report["notes"].append(f"analysis_run_id={analysis_run_id}")
            emit_element_dominance(v21_analysis_dir)
            report["notes"].append("element_dominance: emitted")
        record_rows = _pre_filter_record_rows

    split_domains: List[str] = []
    if "split" in selected_stages:
        if args.split_domains is None or str(args.split_domains) == "__ALL__":
            split_domains = sorted({str(r.get("domain", "")).strip() for r in (record_rows or []) if str(r.get("domain", "")).strip() and str(r.get("domain", "")).strip() not in SUPPRESSED_DOWNSTREAM_DOMAINS}, key=lambda s: s.lower())
            if not split_domains:
                split_domains = [d for d in _discover_domains_from_exports(exports_dir) if d not in SUPPRESSED_DOWNSTREAM_DOMAINS]
        else:
            split_domains = [d.strip() for d in str(args.split_domains).split(",") if d.strip() and d.strip() not in SUPPRESSED_DOWNSTREAM_DOMAINS]

    if split_domains:
        print(f"[extract_all] Stage split: running split detection for {len(split_domains)} domain(s)...", flush=True)
        _ensure_dir(v21_split_root)
        phase0_records_csv = v21_phase0_dir / "records.csv"
        use_phase0_dir = phase0_records_csv.is_file()
        if use_phase0_dir and require_join_policy:
            _enforce_policy_gate(_read_csv_rows(phase0_records_csv), v21_root / "diagnostics", split_domains, allow_sig_hash_join_key)
        for split_domain in split_domains:
            cmd_split = [sys.executable, "tools/run_split_detection_all.py", str(exports_dir), "--domain", split_domain, "--out-root", str(v21_split_root / split_domain), "--mode", "allpairs", *(["--phase0-dir", str(v21_phase0_dir)] if use_phase0_dir else []), *(["--allow-sig-hash-join-key"] if allow_sig_hash_join_key else [])]
            report["commands"].append({"stage": "split", "domain": split_domain, "cmd": cmd_split})
            _run(cmd_split, env=env)

    if "flat_tables" in selected_stages:
        print("[extract_all] Stage flat_tables: writing flat CSV tables...", flush=True)
        _ensure_dir(flat_tables_dir)
        cmd_flat = [
            sys.executable,
            "tools/export_to_flat_tables.py",
            "--root_dir", str(exports_dir),
            "--out_dir", str(flat_tables_dir),
            "--file_id_mode", "basename",
            "--emit", str(args.flat_tables_emit),
        ]
        if args.domains and str(args.domains).strip():
            cmd_flat += ["--domains", str(args.domains)]
        report["commands"].append({"stage": "flat_tables", "cmd": cmd_flat})
        _run(cmd_flat, env=env)
        print(f"[extract_all] Stage flat_tables complete: out={flat_tables_dir}", flush=True)

    report_path = out_root / "extract_all.report.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)
    print(f"Wrote: {report_path}")


if __name__ == "__main__":
    main()
