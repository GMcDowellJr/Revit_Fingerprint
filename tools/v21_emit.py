#!/usr/bin/env python3
from __future__ import annotations

import base64
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from label_synthesis.label_resolver import (
    find_near_duplicate_merges,
    load_annotations,
    load_label_population,
    load_llm_cache,
    resolve_pattern_label,
)

SCHEMA_VERSION = "2.1.0"
STANDARD_PRESENCE_MIN = 0.75
DOMINANT_SHARE_MIN = 0.50
MIN_RECORDS_FOR_DOMAIN = 50
MIN_FILES_FOR_DOMAIN = 3
UNKNOWN_RATE_MAX = 0.20

# See docs/CENTRAL_PATH_NORM_RULE.md for normalization contract.
_VOLATILE_SEGMENTS = {
    "documents",
    "desktop",
    "downloads",
    "appdata",
    "local",
    "roaming",
    "autodesk",
    "revit",
    "cache",
}


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iter_export_files(exports_dir: Path) -> List[Tuple[str, Path, Optional[Path]]]:
    files = [p for p in exports_dir.glob("*.json") if p.is_file() and not p.name.lower().endswith(".legacy.json")]
    index_by_base: Dict[str, Path] = {}
    details_by_base: Dict[str, Path] = {}
    fingerprint: List[Tuple[str, Path, Optional[Path]]] = []
    plain: List[Tuple[str, Path, Optional[Path]]] = []
    for p in files:
        lower = p.name.lower()
        if lower.endswith("__fingerprint.json"):
            fingerprint.append((p.name, p, None))
        elif lower.endswith(".index.json"):
            base = lower[:-len(".index.json")]
            index_by_base[base] = p
        elif lower.endswith(".details.json"):
            base = lower[:-len(".details.json")]
            details_by_base[base] = p
        else:
            plain.append((p.name, p, None))

    split_pairs: List[Tuple[str, Path, Optional[Path]]] = []
    for base in sorted(set(index_by_base) | set(details_by_base)):
        idx = index_by_base.get(base)
        det = details_by_base.get(base)
        if idx is not None:
            split_pairs.append((idx.name, idx, det))
        elif det is not None:
            split_pairs.append((det.name, det, None))

    sys.stderr.write(
        "[INFO v21_emit] export surfaces: "
        f"fingerprint={len(fingerprint)} split_pairs={len(split_pairs)} plain={len(plain)}\n"
    )

    merged: List[Tuple[str, Path, Optional[Path]]] = []
    merged.extend(sorted(fingerprint, key=lambda t: t[0].lower()))
    merged.extend(split_pairs)
    merged.extend(sorted(plain, key=lambda t: t[0].lower()))
    return merged


def _read_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        d = json.load(f)
    if not isinstance(d, dict):
        raise TypeError(f"JSON root must be object: {p}")
    return d


def _merge_index_details(index_fp: Dict[str, Any], details_fp: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(index_fp)
    for key, value in details_fp.items():
        if not key.startswith("_") and key not in merged:
            merged[key] = value
    return merged


def _iter_domains(d: Dict[str, Any]) -> List[str]:
    c = d.get("_contract")
    if isinstance(c, dict):
        doms = c.get("domains")
        if isinstance(doms, dict):
            return sorted([str(k) for k in doms.keys()])
    out: List[str] = []
    for k, v in d.items():
        if isinstance(k, str) and not k.startswith("_") and isinstance(v, dict) and isinstance(v.get("records"), list):
            out.append(k)
    return sorted(out)


def _file_id(path: Path, mode: str) -> str:
    if mode == "basename":
        return path.name
    if mode == "stem":
        return path.stem
    return str(path.resolve())


def _get_tool_version() -> str:
    env_v = os.environ.get("FINGERPRINT_TOOL_VERSION", "").strip()
    if env_v:
        return env_v if re.match(r"^\d+\.\d+\.\d+([+-].+)?$", env_v) else f"0.0.0+{env_v}"
    base = "0.0.0"
    try:
        gitsha = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
        if gitsha:
            return f"{base}+{gitsha}"
    except Exception:
        pass
    return f"{base}+nogit"


def _identity_metadata(data: Dict[str, Any]) -> Dict[str, str]:
    identity = data.get("identity") if isinstance(data.get("identity"), dict) else {}
    contract = data.get("_contract") if isinstance(data.get("_contract"), dict) else {}
    contract_ident = contract.get("identity") if isinstance(contract.get("identity"), dict) else {}
    phase2 = identity.get("phase2") if isinstance(identity.get("phase2"), dict) else {}
    lineage_items = phase2.get("lineage_items") if isinstance(phase2.get("lineage_items"), dict) else {}

    central_path = _safe_str(
        lineage_items.get("central_path")
        or contract_ident.get("central_path")
        or identity.get("central_path")
        or data.get("central_path")
    )
    return {
        "project_label": _safe_str(identity.get("project_title") or contract_ident.get("project_title")),
        "model_label": _safe_str(
            lineage_items.get("filename")
            or identity.get("filename")
            or identity.get("model_title")
            or contract_ident.get("model_title")
        ),
        "central_path": central_path,
        "central_path_norm": _safe_str(lineage_items.get("central_path_norm") or _norm_central_path(central_path)),
        "lineage_hash": _safe_str(phase2.get("lineage_hash") or data.get("lineage_hash") or data.get("_lineage_hash")),
        "revit_version_number": _safe_str(identity.get("revit_version_number") or contract_ident.get("revit_version_number")),
        "revit_version_name": _safe_str(identity.get("revit_version_name") or contract_ident.get("revit_version_name")),
        "revit_build": _safe_str(identity.get("revit_build") or contract_ident.get("revit_build")),
        "is_workshared": _safe_str(identity.get("is_workshared") if "is_workshared" in identity else contract_ident.get("is_workshared")),
    }


def _norm_central_path(path: str) -> str:
    s = (path or "").strip().replace("\\", "/")
    s = re.sub(r"/+", "/", s).lower()
    s = re.sub(r"^[a-z]:/", "/", s)
    s = re.sub(r"/users/[^/]+/", "/users/<user>/", s)
    parts = [p for p in s.split("/") if p]
    cleaned: List[str] = []
    for p in parts:
        if p.startswith("onedrive"):
            continue
        if p in _VOLATILE_SEGMENTS:
            continue
        if cleaned and cleaned[-1] == p:
            continue
        cleaned.append(p)
    out = "/" + "/".join(cleaned) if cleaned else ""
    return out.rstrip("/")


def _b32_sha1_16(text: str) -> str:
    digest = hashlib.sha1(text.encode("utf-8")).digest()
    token = base64.b32encode(digest).decode("ascii").lower().rstrip("=")
    return token[:16]


def _stable_pattern_id(
    domain: str,
    join_key_schema: str,
    join_hash: str,
    taken: set[str],
) -> str:
    raw = f"{domain}|{join_key_schema}|{join_hash}"
    digest = hashlib.sha1(raw.encode("utf-8")).digest()
    token = base64.b32encode(digest).decode("ascii").lower().rstrip("=")
    for n in range(16, len(token) + 1):
        candidate = f"pat_{token[:n]}"
        if candidate not in taken:
            taken.add(candidate)
            return candidate
    candidate = f"pat_{token}"
    taken.add(candidate)
    return candidate


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def _sort_rows(rows: List[Dict[str, str]], keys: List[str]) -> List[Dict[str, str]]:
    return sorted(rows, key=lambda r: tuple(r.get(k, "") for k in keys))


def _load_identity_items_by_record(phase0_dir: Optional[Path], domain: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
    if phase0_dir is None:
        return {}

    csv_path: Optional[Path] = None
    if domain:
        scoped = phase0_dir / "phase0_identity_items_by_domain" / f"{domain}.csv"
        if scoped.is_file():
            csv_path = scoped

    if csv_path is None:
        fallback = phase0_dir / "phase0_identity_items.csv"
        if not fallback.is_file():
            return {}
        csv_path = fallback

    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if domain and _safe_str(row.get("domain")) and _safe_str(row.get("domain")) != domain:
                continue
            record_pk = _safe_str(row.get("record_pk"))
            if not record_pk:
                continue
            out[record_pk].append({
                "k": _safe_str(row.get("item_key")),
                "v": _safe_str(row.get("item_value")),
                "q": _safe_str(row.get("item_value_type")),
                "role": _safe_str(row.get("item_role")),
            })
    return out


def _load_label_resolution_inputs(results_v21_dir: Optional[Path], domain: str) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str], Dict[str, Any]]:
    if results_v21_dir is None:
        return {}, {}, {}

    label_synth_dir = results_v21_dir / "label_synthesis"
    analysis_v21_dir = results_v21_dir / "analysis_v21"

    population_candidates = [
        label_synth_dir / f"{domain}.joinhash_label_population.csv",
        analysis_v21_dir / f"{domain}.joinhash_label_population.csv",
        analysis_v21_dir / "label_population" / f"{domain}.joinhash_label_population.csv",
    ]
    pop_path = next((p for p in population_candidates if p.is_file()), None)
    label_pop = load_label_population(str(pop_path), domain) if pop_path else {}

    annotation_candidates = [
        label_synth_dir / f"{domain}.pattern_annotations.csv",
        label_synth_dir / "pattern_annotations.csv",
        analysis_v21_dir / "pattern_annotations.csv",
    ]
    anno_path = next((p for p in annotation_candidates if p.is_file()), None)
    annotations = load_annotations(str(anno_path)) if anno_path else {}

    llm_cache_candidates = [
        label_synth_dir / f"{domain}.llm_name_cache.json",
        label_synth_dir / "llm_name_cache.json",
        analysis_v21_dir / f"{domain}.llm_name_cache.json",
        analysis_v21_dir / "llm_name_cache.json",
    ]
    llm_path = next((p for p in llm_cache_candidates if p.is_file()), None)
    llm_cache = load_llm_cache(str(llm_path)) if llm_path else {}
    return label_pop, annotations, llm_cache


def emit_phase0_v21(exports_dir: Path, out_dir: Path, file_id_mode: str = "basename") -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    exported_utc = _utc_now_iso()
    tool_version = _get_tool_version()
    meta_rows: List[Dict[str, str]] = []
    record_rows: List[Dict[str, str]] = []
    item_rows: List[Dict[str, str]] = []
    label_rows: List[Dict[str, str]] = []
    reason_rows: List[Dict[str, str]] = []

    for _, primary, secondary in _iter_export_files(exports_dir):
        data = _read_json(primary)
        if secondary is not None:
            data = _merge_index_details(data, _read_json(secondary))
        export_run_id = _file_id(primary, file_id_mode)
        file_id = export_run_id

        contract = data.get("_contract") if isinstance(data.get("_contract"), dict) else {}
        ident = contract.get("identity") if isinstance(contract.get("identity"), dict) else {}
        identity_meta = _identity_metadata(data)
        meta_rows.append({
            "schema_version": SCHEMA_VERSION,
            "export_run_id": export_run_id,
            "file_id": file_id,
            "project_id": _safe_str(ident.get("project_id") or ident.get("project_title")),
            "model_id": _safe_str(ident.get("model_id") or ident.get("model_title")),
            "project_label": identity_meta["project_label"],
            "model_label": identity_meta["model_label"],
            "central_path": identity_meta["central_path"],
            "central_path_norm": identity_meta["central_path_norm"],
            "lineage_hash": identity_meta["lineage_hash"],
            "revit_version_number": identity_meta["revit_version_number"],
            "revit_version_name": identity_meta["revit_version_name"],
            "revit_build": identity_meta["revit_build"],
            "is_workshared": identity_meta["is_workshared"],
            "tool_version": tool_version,
            "exported_utc": exported_utc,
        })

        for domain in _iter_domains(data):
            payload = data.get(domain)
            recs = payload.get("records") if isinstance(payload, dict) else None
            if not isinstance(recs, list):
                continue
            for i, rec in enumerate(recs):
                if not isinstance(rec, dict):
                    continue
                record_ordinal = f"{i:06d}"
                record_pk = f"{file_id}|{domain}|{record_ordinal}"
                record_id = _safe_str(rec.get("record_id") or rec.get("id") or rec.get("name"))
                # Day-1 identity-mode flatten join regime:
                # - keep sig_hash as-is
                # - set join_hash = sig_hash
                # - set join_key_schema = sig_hash_as_join_key.v1
                sig_hash_v = _safe_str(rec.get("sig_hash") or (rec.get("identity_basis", {}) or {}).get("sig_hash"))
                row = {
                    "schema_version": SCHEMA_VERSION,
                    "export_run_id": export_run_id,
                    "file_id": file_id,
                    "domain": domain,
                    "record_pk": record_pk,
                    "record_id": record_id,
                    "record_ordinal": record_ordinal,
                    "status": _safe_str(rec.get("status")),
                    "identity_quality": _safe_str(rec.get("identity_quality")),
                    "sig_hash": sig_hash_v,
                    "join_hash": sig_hash_v,
                    "join_key_schema": "sig_hash_as_join_key.v1",
                    "join_key_status": "bootstrap",
                    "join_key_policy_id": "",
                    "join_key_policy_version": "",
                    "label_display": _safe_str((rec.get("label") or {}).get("display")),
                    "label_quality": _safe_str((rec.get("label") or {}).get("quality")),
                    "label_provenance": _safe_str((rec.get("label") or {}).get("provenance")),
                }
                record_rows.append(row)

                for reason in rec.get("status_reasons") if isinstance(rec.get("status_reasons"), list) else []:
                    if isinstance(reason, str) and reason:
                        reason_rows.append({
                            "schema_version": SCHEMA_VERSION,
                            "export_run_id": export_run_id,
                            "domain": domain,
                            "record_pk": record_pk,
                            "reason_code": reason,
                            "reason_detail": "",
                        })

                items = (rec.get("identity_basis") or {}).get("items") if isinstance(rec.get("identity_basis"), dict) else None
                if isinstance(items, list):
                    for it in items:
                        if not isinstance(it, dict):
                            continue
                        item_rows.append({
                            "schema_version": SCHEMA_VERSION,
                            "export_run_id": export_run_id,
                            "domain": domain,
                            "record_pk": record_pk,
                            "item_key": _safe_str(it.get("k")),
                            "item_value": _safe_str(it.get("v")),
                            "item_value_type": _safe_str(it.get("q")),
                            "item_role": "identity_basis",
                        })

                comps = (rec.get("label") or {}).get("components") if isinstance(rec.get("label"), dict) else None
                if isinstance(comps, dict):
                    for order, key in enumerate(sorted(comps.keys(), key=str)):
                        val = comps.get(key)
                        if not isinstance(val, (str, int, float, bool)) and val is not None:
                            val = json.dumps(val, ensure_ascii=False, sort_keys=True)
                        label_rows.append({
                            "schema_version": SCHEMA_VERSION,
                            "export_run_id": export_run_id,
                            "domain": domain,
                            "record_pk": record_pk,
                            "component_key": _safe_str(key),
                            "component_value": _safe_str(val),
                            "component_order": str(order),
                        })

    _write_csv(out_dir / "file_metadata.csv", [
        "schema_version", "export_run_id", "file_id", "project_id", "model_id",
        "project_label", "model_label", "central_path", "central_path_norm",
        "lineage_hash", "revit_version_number", "revit_version_name", "revit_build",
        "is_workshared", "tool_version", "exported_utc",
    ], _sort_rows(meta_rows, ["export_run_id"]))

    _write_csv(out_dir / "phase0_records.csv", [
        "schema_version", "export_run_id", "file_id", "domain", "record_pk", "record_id", "record_ordinal",
        "status", "identity_quality", "sig_hash", "join_hash", "join_key_schema",
        "join_key_status", "join_key_policy_id", "join_key_policy_version",
        "label_display", "label_quality", "label_provenance",
    ], _sort_rows(record_rows, ["export_run_id", "domain", "record_pk"]))

    _write_csv(out_dir / "phase0_identity_items.csv", [
        "schema_version", "export_run_id", "domain", "record_pk", "item_key", "item_value",
        "item_value_type", "item_role",
    ], _sort_rows(item_rows, ["export_run_id", "domain", "record_pk", "item_key", "item_value"]))

    _write_csv(out_dir / "phase0_label_components.csv", [
        "schema_version", "export_run_id", "domain", "record_pk", "component_key", "component_value", "component_order",
    ], _sort_rows(label_rows, ["export_run_id", "domain", "record_pk", "component_order", "component_key"]))

    _write_csv(out_dir / "phase0_status_reasons.csv", [
        "schema_version", "export_run_id", "domain", "record_pk", "reason_code", "reason_detail",
    ], _sort_rows(reason_rows, ["export_run_id", "domain", "record_pk", "reason_code"]))
    return meta_rows, record_rows


def emit_analysis_v21(
    meta_rows: List[Dict[str, str]],
    records: List[Dict[str, str]],
    out_dir: Path,
    *,
    phase0_dir: Optional[Path] = None,
    results_v21_dir: Optional[Path] = None,
) -> str:
    exports = sorted({r["export_run_id"] for r in meta_rows})
    domains = sorted({r["domain"] for r in records})
    executed_utc = _utc_now_iso()
    scope_src = "|".join(exports)
    analysis_scope_hash = hashlib.sha1(scope_src.encode("utf-8")).hexdigest()
    analysis_run_id = f"ana_{analysis_scope_hash[:12]}"

    _write_csv(out_dir / "analysis_manifest.csv", [
        "schema_version", "analysis_run_id", "analysis_scope_hash", "export_run_count", "domain_count",
        "tool_version", "policy_baseline_version", "policy_pareto_version",
        "join_key_policy_version", "pattern_promotion_policy_version", "authority_metric_version", "executed_utc",
        "is_incremental_update", "notes",
    ], [{
        "schema_version": SCHEMA_VERSION,
        "analysis_run_id": analysis_run_id,
        "analysis_scope_hash": analysis_scope_hash,
        "export_run_count": str(len(exports)),
        "domain_count": str(len(domains)),
        "tool_version": _get_tool_version(),
        "policy_baseline_version": "0.0.0",
        "policy_pareto_version": "0.0.0",
        "join_key_policy_version": "0.0.0",
        "pattern_promotion_policy_version": "0.0.0",
        "authority_metric_version": "0.0.0",
        "executed_utc": executed_utc,
        "is_incremental_update": "0",
        "notes": (
            "defaults: STANDARD_PRESENCE_MIN=0.75; DOMINANT_SHARE_MIN=0.50; "
            "MIN_RECORDS_FOR_DOMAIN=50; MIN_FILES_FOR_DOMAIN=3; UNKNOWN_RATE_MAX=0.20"
        ),
    }])

    membership_rows = [{
        "schema_version": SCHEMA_VERSION,
        "analysis_run_id": analysis_run_id,
        "export_run_id": ex,
        "membership_role": "included",
    } for ex in exports]
    _write_csv(out_dir / "analysis_export_membership.csv", [
        "schema_version", "analysis_run_id", "export_run_id", "membership_role",
    ], _sort_rows(membership_rows, ["analysis_run_id", "export_run_id"]))

    files_total = len(exports)
    by_dom_cluster: Dict[Tuple[str, str, str], List[Dict[str, str]]] = defaultdict(list)
    for r in records:
        jh = r.get("join_hash", "")
        if not jh:
            continue
        key = (r["domain"], r.get("join_key_schema", ""), jh)
        by_dom_cluster[key].append(r)

    domain_metrics: List[Dict[str, str]] = []
    domain_patterns: List[Dict[str, str]] = []
    rec_membership: List[Dict[str, str]] = []
    authority_rows: List[Dict[str, str]] = []
    presence_rows: List[Dict[str, str]] = []
    diag_rows: List[Dict[str, str]] = []

    dom_clusters: Dict[str, List[Tuple[Tuple[str, str, str], List[Dict[str, str]]]]] = defaultdict(list)
    for k, v in by_dom_cluster.items():
        dom_clusters[k[0]].append((k, v))

    records_by_domain: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for r in records:
        records_by_domain[r["domain"]].append(r)
    pattern_id_by_cluster: Dict[Tuple[str, str, str], str] = {}
    for dom in domains:
        print(f"[v21_emit] domain={dom} (start)", flush=True)
        identity_items_by_record = _load_identity_items_by_record(phase0_dir, dom)
        cluster_items = dom_clusters.get(dom, [])
        domain_records = records_by_domain.get(dom, [])
        domain_files_present = len({r["export_run_id"] for r in domain_records})
        label_population_by_hash, annotations, llm_cache = _load_label_resolution_inputs(results_v21_dir, dom)
        pattern_ids_taken: set[str] = set()
        cluster_rows: List[Dict[str, Any]] = []
        for (_, schema, join_hash), rows in sorted(cluster_items, key=lambda kv: (kv[0][1], kv[0][2])):
            # v2.1 default source-of-truth: phase0 join_hash/join_key_schema emitted in export JSON records.
            pid = _stable_pattern_id(dom, schema, join_hash, pattern_ids_taken)
            files_present = len({r["export_run_id"] for r in rows})
            cluster_rows.append({
                "schema": schema,
                "join_hash": join_hash,
                "rows": rows,
                "pid": pid,
                "files_present": files_present,
                "records_count": len(rows),
                "identity_items": identity_items_by_record.get(rows[0].get("record_pk", ""), []),
            })
            pattern_id_by_cluster[(dom, schema, join_hash)] = pid

        sorted_clusters = sorted(
            cluster_rows,
            key=lambda c: (-c["files_present"], -c["records_count"], c["pid"]),
        )
        n = len(sorted_clusters)
        total_dom_records = sum(int(c["records_count"]) for c in sorted_clusters)
        near_dup_merge_map = find_near_duplicate_merges(sorted_clusters)
        resolved_labels: Dict[str, Tuple[str, str]] = {}

        for rank, cluster in enumerate(sorted_clusters, start=1):
            schema = str(cluster["schema"])
            join_hash = str(cluster["join_hash"])
            rows = list(cluster["rows"])
            files_present = int(cluster["files_present"])
            cluster_id = f"{dom}|{schema}|{join_hash}"
            presence_pct = (files_present / files_total) if files_total else 0.0
            coverage_pct = (len(rows) / total_dom_records) if total_dom_records else 0.0
            cluster_size = len(rows)
            domain_metrics.append({
                "schema_version": SCHEMA_VERSION,
                "analysis_run_id": analysis_run_id,
                "domain": dom,
                "group_type": "CORPUS",
                "group_id": "CORPUS",
                "join_key_schema": schema,
                "join_hash": join_hash,
                "cluster_id": cluster_id,
                "cluster_size": str(cluster_size),
                "files_present": str(files_present),
                "files_total": str(files_total),
                "presence_pct": f"{presence_pct:.6f}",
                "coverage_pct": f"{coverage_pct:.6f}",
                "collision_pct": "0.000000",
                "stability_pct": f"{presence_pct:.6f}",
            })

            # See docs/PATTERN_ID_AND_LABEL_RULES.md for stable pattern identity/label.
            pid = str(cluster["pid"])
            generic_label = f"{schema} — Variant {rank} of {n}"
            near_dup_target_label: Optional[str] = None
            near_dup_target_hash = near_dup_merge_map.get(join_hash)
            if near_dup_target_hash:
                near_dup_target_label = resolved_labels.get(near_dup_target_hash, ("", ""))[0] or None
            resolved_label, resolved_source = resolve_pattern_label(
                domain=dom,
                join_hash=join_hash,
                join_key_schema=schema,
                pattern_rank=rank,
                pattern_count=n,
                identity_items=cluster.get("identity_items") or [],
                label_population=label_population_by_hash.get(join_hash) or [],
                annotations=annotations,
                llm_cache=llm_cache,
                pattern_id=pid,
                near_dup_target_label=near_dup_target_label,
            )
            resolved_labels[join_hash] = (resolved_label, resolved_source)
            domain_patterns.append({
                "schema_version": SCHEMA_VERSION,
                "analysis_run_id": analysis_run_id,
                "domain": dom,
                "pattern_id": pid,
                # Back-compat: keep legacy generic label in pattern_label so existing
                # Power BI transforms that parse "Variant X of N" continue to work.
                "pattern_label": generic_label,
                "pattern_label_human": resolved_label,
                "pattern_label_source": resolved_source,
                "pattern_label_fallback": generic_label,
                "source_cluster_id": cluster_id,
                "pattern_size_records": str(cluster_size),
                "pattern_size_files": str(files_present),
                "pattern_rank": str(rank),
                "is_candidate_standard": "true" if presence_pct >= STANDARD_PRESENCE_MIN else "",
                "notes": "",
            })

            for r in rows:
                rec_membership.append({
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": analysis_run_id,
                    "export_run_id": r["export_run_id"],
                    "domain": dom,
                    "record_pk": r["record_pk"],
                    "pattern_id": pid,
                    "membership_confidence": "1.000000",
                    "membership_reason_code": "join_hash_exact",
                })

            shares = [int(c["records_count"]) / total_dom_records for c in sorted_clusters] if total_dom_records else []
            hhi = sum(s * s for s in shares)
            eff = (1.0 / hhi) if hhi > 0 else 0.0
            authority_rows.append({
                "schema_version": SCHEMA_VERSION,
                "analysis_run_id": analysis_run_id,
                "domain": dom,
                "pattern_id": pid,
                "join_key_schema": schema,
                "files_present": str(files_present),
                "files_total": str(files_total),
                "presence_pct": f"{presence_pct:.6f}",
                "hhi": f"{hhi:.6f}",
                "effective_cluster_count": f"{eff:.6f}",
                "authority_score": f"{presence_pct:.6f}",
                "confidence_tier": "high" if presence_pct >= STANDARD_PRESENCE_MIN else "medium",
            })

        for export_run_id in exports:
            dom_records = [r for r in domain_records if r["export_run_id"] == export_run_id]
            total = len(dom_records)
            per_pat = defaultdict(int)
            unknown = 0
            for r in dom_records:
                jh = r.get("join_hash", "")
                if not jh:
                    unknown += 1
                    continue
                schema = r.get("join_key_schema", "")
                pid = pattern_id_by_cluster.get((dom, schema, jh))
                if not pid:
                    continue
                per_pat[pid] += 1
            dominant_pid = ""
            dominant_share = 0.0
            if per_pat and total > 0:
                dominant_pid, dominant_count = sorted(per_pat.items(), key=lambda kv: (-kv[1], kv[0]))[0]
                dominant_share = dominant_count / total
            for pid, cnt in sorted(per_pat.items()):
                share = cnt / total if total else 0.0
                presence_rows.append({
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": analysis_run_id,
                    "export_run_id": export_run_id,
                    "domain": dom,
                    "pattern_id": pid,
                    "pattern_share_pct": f"{share:.6f}",
                    "is_dominant_pattern": "true" if pid == dominant_pid else "false",
                    "deviation_score": f"{max(0.0, dominant_share - share):.6f}",
                    "classification": "STANDARD" if share >= STANDARD_PRESENCE_MIN else "VARIANT",
                })
            if unknown > 0:
                presence_rows.append({
                    "schema_version": SCHEMA_VERSION,
                    "analysis_run_id": analysis_run_id,
                    "export_run_id": export_run_id,
                    "domain": dom,
                    "pattern_id": "",
                    "pattern_share_pct": f"{(unknown / total) if total else 0.0:.6f}",
                    "is_dominant_pattern": "false",
                    "deviation_score": "0.000000",
                    "classification": "UNKNOWN",
                })

        unknown_domain = len([r for r in domain_records if not r.get("join_hash")])
        total_domain = len(domain_records)
        shares = [int(c["records_count"]) / total_domain for c in sorted_clusters] if total_domain else []
        dominant = max(shares) if shares else 0.0
        entropy = -sum((s * (0.0 if s <= 0 else __import__('math').log(s, 2))) for s in shares) if shares else 0.0
        unknown_rate = (unknown_domain / total_domain) if total_domain else 0.0
        rec_grain = "DOMAIN_OK"
        if total_domain < MIN_RECORDS_FOR_DOMAIN or domain_files_present < MIN_FILES_FOR_DOMAIN:
            rec_grain = "INSUFFICIENT_EVIDENCE"
        elif unknown_rate > UNKNOWN_RATE_MAX:
            rec_grain = "KEY_REVISION_REQUIRED"
        elif dominant < DOMINANT_SHARE_MIN:
            rec_grain = "PATTERN_REQUIRED"
        diag_rows.append({
            "schema_version": SCHEMA_VERSION,
            "analysis_run_id": analysis_run_id,
            "domain": dom,
            "pattern_count": str(len(sorted_clusters)),
            "dominant_pattern_share_pct": f"{dominant:.6f}",
            "entropy_index": f"{entropy:.6f}",
            "mixture_flag": "true" if dominant < DOMINANT_SHARE_MIN else "false",
            "unknown_rate_pct": f"{unknown_rate:.6f}",
            "recommended_analysis_grain": rec_grain,
        })
        print(
            f"[v21_emit] domain={dom} (done) clusters={len(sorted_clusters)} records={len(domain_records)}",
            flush=True,
        )

    # Unknown join_hash rows still get membership rows with blank pattern_id.
    for r in records:
        if r.get("join_hash"):
            continue
        rec_membership.append({
            "schema_version": SCHEMA_VERSION,
            "analysis_run_id": analysis_run_id,
            "export_run_id": r["export_run_id"],
            "domain": r["domain"],
            "record_pk": r["record_pk"],
            "pattern_id": "",
            "membership_confidence": "0.000000",
            "membership_reason_code": "missing_join_hash",
        })

    _write_csv(out_dir / "phase1_domain_metrics.csv", [
        "schema_version", "analysis_run_id", "domain", "group_type", "group_id",
        "join_key_schema", "join_hash", "cluster_id", "cluster_size",
        "files_present", "files_total", "presence_pct", "coverage_pct", "collision_pct", "stability_pct",
    ], _sort_rows(domain_metrics, ["domain", "join_key_schema", "join_hash"]))

    _write_csv(out_dir / "domain_patterns.csv", [
        # Keep legacy first 11 columns in the original order for Power BI queries
        # that pin Csv.Document([Columns=11]) and/or type-steps against that shape.
        "schema_version", "analysis_run_id", "domain", "pattern_id", "pattern_label",
        "source_cluster_id", "pattern_size_records", "pattern_size_files", "pattern_rank",
        "is_candidate_standard", "notes",
        # v2.1 human-readable/audit extensions are appended for compatibility.
        "pattern_label_human", "pattern_label_source", "pattern_label_fallback",
    ], _sort_rows(domain_patterns, ["analysis_run_id", "domain", "pattern_id"]))

    _write_csv(out_dir / "record_pattern_membership.csv", [
        "schema_version", "analysis_run_id", "export_run_id", "domain", "record_pk",
        "pattern_id", "membership_confidence", "membership_reason_code",
    ], _sort_rows(rec_membership, ["analysis_run_id", "export_run_id", "domain", "record_pk"]))

    _write_csv(out_dir / "phase2_authority_pattern.csv", [
        "schema_version", "analysis_run_id", "domain", "pattern_id", "join_key_schema",
        "files_present", "files_total", "presence_pct", "hhi", "effective_cluster_count",
        "authority_score", "confidence_tier",
    ], _sort_rows(authority_rows, ["analysis_run_id", "domain", "pattern_id"]))

    _write_csv(out_dir / "pattern_presence_file.csv", [
        "schema_version", "analysis_run_id", "export_run_id", "domain", "pattern_id",
        "pattern_share_pct", "is_dominant_pattern", "deviation_score", "classification",
    ], _sort_rows(presence_rows, ["analysis_run_id", "export_run_id", "domain", "pattern_id"]))

    _write_csv(out_dir / "domain_pattern_diagnostics.csv", [
        "schema_version", "analysis_run_id", "domain", "pattern_count", "dominant_pattern_share_pct",
        "entropy_index", "mixture_flag", "unknown_rate_pct", "recommended_analysis_grain",
    ], _sort_rows(diag_rows, ["analysis_run_id", "domain"]))

    return analysis_run_id
