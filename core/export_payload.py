# -*- coding: utf-8 -*-
"""Build pre-v1 fingerprint export payloads from legacy runner surfaces."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
import platform
import re
from typing import Any, Dict, List, Optional, Tuple

_ALLOWED_T = {"s", "i", "f", "b", "json"}


def get_export_mode() -> str:
    raw = str(os.getenv("REVIT_FINGERPRINT_EXPORT_MODE", "lean") or "lean").strip().lower()
    return "audit" if raw == "audit" else "lean"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _coalesce(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        s = str(value).strip()
        if s:
            return s
    return None


def _to_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None


def _infer_t(v: Any) -> str:
    if isinstance(v, bool):
        return "b"
    if isinstance(v, int) and not isinstance(v, bool):
        return "i"
    if isinstance(v, float):
        return "f"
    if isinstance(v, (dict, list, tuple)):
        return "json"
    return "s"


def _map_q(q: Any) -> str:
    s = str(q or "").strip().lower()
    if s == "ok":
        return "ok"
    if s in {"missing", "unreadable", "unsupported", "unsupported.not_applicable", "unsupported.not_implemented", "warn"}:
        return "warn"
    return "unknown"


def _typed_item(k: str, v: Any, q: Any, t: Any = None) -> Dict[str, Any]:
    tt = str(t or "").strip().lower() or _infer_t(v)
    if tt not in _ALLOWED_T:
        tt = "s"
    return {
        "k": str(k).strip(),
        "t": tt,
        "v": v,
        "q": _map_q(q),
    }


def _extract_definition_items(rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    identity_basis = rec.get("identity_basis") if isinstance(rec.get("identity_basis"), dict) else {}
    items = identity_basis.get("items") if isinstance(identity_basis.get("items"), list) else []
    out: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        k = str(it.get("k", "")).strip()
        if not k:
            continue
        item = _typed_item(k, it.get("v"), it.get("q"), it.get("t"))
        item["u"] = "def"
        out.append(item)
    return sorted(out, key=lambda x: x["k"])


def _extract_phase2_unknown_items(rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    p2 = rec.get("phase2") if isinstance(rec.get("phase2"), dict) else {}
    unknown = p2.get("unknown_items") if isinstance(p2.get("unknown_items"), list) else []
    return [it for it in unknown if isinstance(it, dict)]


def _is_provenance_key(k: str) -> bool:
    k = str(k or "").strip().lower()
    return bool(
        k.endswith(".uid")
        or k.endswith(".elem_id")
        or k.endswith(".id.int")
        or k.endswith(".doc_unique_id")
        or "source_unique_id" in k
        or "source_element_id" in k
        or re.search(r"(^|[._])element_id$", k)
        or "doc_unique_id" in k
    )


def _is_label_key(k: str) -> bool:
    k = str(k or "").strip().lower()
    return k.endswith(".name") or k.endswith(".label") or k in {"name", "label", "display_name", "project_title"}


def _provenance_field_for_key(k: str) -> str:
    lk = str(k or "").strip().lower()
    if lk.endswith(".doc_unique_id") or "doc_unique_id" in lk:
        return "doc_unique_id"
    if lk.endswith(".uid") or "source_unique_id" in lk:
        return "element_unique_id"
    if lk.endswith(".elem_id") or lk.endswith(".id.int") or "source_element_id" in lk or re.search(r"(^|[._])element_id$", lk):
        return "element_id"
    return re.sub(r"[^a-z0-9_]+", "_", lk).strip("_") or "source_key"


def _normalize_blocked(detail: str) -> str:
    d = str(detail or "").strip().lower()
    if "not_applicable" in d or d.startswith("unsupported.not_applicable"):
        return "not_applicable"
    if "policy" in d and "omitted" in d:
        return "policy_omitted"
    if any(x in d for x in ("missing", "parse", "exception", "failed")):
        return "error"
    return "unavailable"


def _cleanup_unclassified(
    unclassified: List[Dict[str, Any]],
    *,
    definition_keys: set,
    provenance_source: Dict[str, Any],
    label_display: str,
    label_meta: Dict[str, Any],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    alt_names: List[str] = list(label_meta.get("alt_names") or [])

    for it in unclassified:
        k = str(it.get("k", "")).strip()
        if not k or k in definition_keys:
            continue
        v = it.get("v")

        if _is_provenance_key(k):
            field = _provenance_field_for_key(k)
            if field not in provenance_source:
                provenance_source[field] = _to_int(v) if field == "element_id" else (str(v) if v is not None else None)
            continue

        if _is_label_key(k):
            val = str(v).strip() if v is not None else ""
            if not val:
                continue
            if val == label_display:
                continue
            alt_names.append(val)
            continue

        out.append(it)

    if alt_names:
        label_meta["alt_names"] = sorted(set(alt_names))
    return sorted(out, key=lambda x: x["k"])


def _classify_unknown_items(
    unknown_items: List[Dict[str, Any]],
    definition_keys: set,
    display_seed: str,
) -> Tuple[Dict[str, Any], Optional[str], Dict[str, Any], List[Dict[str, Any]]]:
    source: Dict[str, Any] = {}
    label_display = display_seed
    label_meta: Dict[str, Any] = {}
    unclassified: List[Dict[str, Any]] = []

    for it in unknown_items:
        k = str(it.get("k", "")).strip()
        if not k or k in definition_keys:
            continue
        v = it.get("v")
        if _is_provenance_key(k):
            field = _provenance_field_for_key(k)
            if field == "element_id":
                iv = _to_int(v)
                source[field] = iv if iv is not None else v
            else:
                source[field] = str(v) if v is not None else None
            continue

        if _is_label_key(k):
            val = str(v).strip() if v is not None else ""
            if val:
                if not label_display:
                    label_display = val
                else:
                    label_meta[k] = val
            continue

        unclassified.append(_typed_item(k, v, it.get("q"), it.get("t")))

    unclassified = _cleanup_unclassified(
        unclassified,
        definition_keys=definition_keys,
        provenance_source=source,
        label_display=label_display or "",
        label_meta=label_meta,
    )

    return source, label_display, label_meta, unclassified


def _build_record(domain_name: str, rec: Dict[str, Any], export_mode: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    definition_items = _extract_definition_items(rec)
    definition_keys = {it["k"] for it in definition_items}
    unknown_phase2 = _extract_phase2_unknown_items(rec)

    join_key = rec.get("join_key") if isinstance(rec.get("join_key"), dict) else {}
    label = rec.get("label") if isinstance(rec.get("label"), dict) else {}
    display = str(label.get("display") or label.get("text") or rec.get("record_id") or "").strip()

    provenance_source, display, label_meta, unclassified_items = _classify_unknown_items(
        unknown_phase2,
        definition_keys,
        display,
    )

    sig_hash = rec.get("sig_hash")
    join_hash = join_key.get("join_hash")
    status = str(rec.get("status") or "")
    status_reasons = [str(x) for x in list(rec.get("status_reasons") or [])]

    blocked_details: List[str] = []
    if status == "blocked":
        blocked_details.extend(status_reasons or ["blocked"])
    if not sig_hash:
        blocked_details.append("missing_sig_hash")
    if not join_hash:
        blocked_details.append("missing_join_hash")

    if blocked_details:
        detail = sorted(set(blocked_details))[0]
        blocked_item = {
            "label": display,
            "class": _normalize_blocked(detail),
            "detail": detail,
            "reasons": status_reasons,
        }
        audit_blocked = None
        if export_mode == "audit":
            audit_blocked = {
                "label": display,
                "class": _normalize_blocked(detail),
                "detail": detail,
                "reasons": status_reasons,
                "record_id": rec.get("record_id"),
            }
        return None, blocked_item, audit_blocked

    id_block = {
        "sig_hash": sig_hash,
        "join_hash": join_hash,
    }
    if rec.get("record_id") is not None:
        id_block["record_id"] = rec.get("record_id")

    out_label: Dict[str, Any] = {"display": display}
    is_system = label.get("is_system")
    if isinstance(is_system, bool):
        out_label["is_system"] = is_system
    if label_meta:
        out_label["meta"] = {k: label_meta[k] for k in sorted(label_meta.keys())}

    out: Dict[str, Any] = {
        "schema": {"id": "fingerprint.record", "version": "0.4.0"},
        "domain": domain_name,
        "id": id_block,
        "label": out_label,
        "definition": {"items": definition_items},
        "provenance": {"source": provenance_source},
        "diagnostics": {
            "warnings": [],
            "status": status or "ok",
            "status_reasons": status_reasons,
        },
    }
    if unclassified_items:
        out["diagnostics"]["unclassified_items"] = unclassified_items

    if export_mode == "audit":
        sig_basis = rec.get("sig_basis") if isinstance(rec.get("sig_basis"), dict) else {}
        out["audit"] = {
            "sig_basis": {
                "keys_used": sorted({str(x) for x in list(sig_basis.get("keys_used") or []) if str(x).strip()}),
            },
            "join_basis": {
                "keys_used": sorted({str(x) for x in list(join_key.get("keys_used") or []) if str(x).strip()}),
            },
        }

    return out, None, None


def _default_policy_registry_path() -> str:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(repo_root, "policies", "domain_join_key_policies.json")


def _load_policy_registry(policy_registry_path: Optional[str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    path = policy_registry_path or _default_policy_registry_path()
    policy_ref = {
        "policy_id": "domain_join_key_policies",
        "policy_version": "0.0.0+unknown",
        "policy_hash": "unknown",
        "source": "unknown",
    }

    if not path:
        return {}, policy_ref

    try:
        with open(path, "rb") as f:
            raw = f.read()
        policy_ref["policy_hash"] = hashlib.sha256(raw).hexdigest()
        norm = path.replace("\\", "/").lower()
        policy_ref["source"] = "in_repo" if norm.endswith("/policies/domain_join_key_policies.json") else "external"

        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            return {}, policy_ref
        policy_ref["policy_version"] = str(payload.get("schema_version") or "0.0.0+unknown")
        domains = payload.get("domains") if isinstance(payload.get("domains"), dict) else {}
        return domains, policy_ref
    except Exception:
        return {}, policy_ref


def build_export_payload(
    *,
    legacy_payload: Dict[str, Any],
    tool_version: Optional[str],
    tool_git_sha: Optional[str],
    host_app_version: Optional[str],
    thinrunner_meta: Optional[Dict[str, Any]] = None,
    policy_registry_path: Optional[str] = None,
) -> Dict[str, Any]:
    export_mode = get_export_mode()
    contract_domains = legacy_payload.get("_contract", {}).get("domains", {}) if isinstance(legacy_payload.get("_contract"), dict) else {}
    domains_expected = sorted([str(k) for k in contract_domains.keys()])

    tr_meta = thinrunner_meta if isinstance(thinrunner_meta, dict) else {}
    tr_exporter = tr_meta.get("exporter") if isinstance(tr_meta.get("exporter"), dict) else {}
    tr_host = tr_meta.get("host") if isinstance(tr_meta.get("host"), dict) else {}

    policy_domains, policy_ref = _load_policy_registry(policy_registry_path)

    domains_out: Dict[str, Any] = {}
    for domain_name in domains_expected:
        payload = legacy_payload.get(domain_name) if isinstance(legacy_payload.get(domain_name), dict) else {}
        records_raw = payload.get("records") if isinstance(payload.get("records"), list) else []

        records: List[Dict[str, Any]] = []
        blocked_records: List[Dict[str, Any]] = []
        audit_blocked_records: List[Dict[str, Any]] = []
        for raw in records_raw:
            if not isinstance(raw, dict):
                continue
            transformed, blocked_item, audit_blocked = _build_record(domain_name, raw, export_mode)
            if transformed is not None:
                records.append(transformed)
            elif blocked_item is not None:
                blocked_records.append(blocked_item)
                if audit_blocked is not None:
                    audit_blocked_records.append(audit_blocked)

        raw_count = payload.get("raw_count") if isinstance(payload, dict) else None
        if raw_count is None:
            raw_count = payload.get("count") if isinstance(payload, dict) else None
        if raw_count is None:
            raw_count = len(records)

        env = contract_domains.get(domain_name) if isinstance(contract_domains.get(domain_name), dict) else {}
        warnings = list(env.get("warnings") or [])

        summary = {
            "raw_count": int(raw_count),
            "exported_count": len(records),
            "blocked_count": len(blocked_records),
        }
        assert summary["exported_count"] == len(records)

        diag: Dict[str, Any] = {
            "warnings": warnings,
            "block_reasons": list(env.get("block_reasons") or []),
        }
        if blocked_records:
            diag["blocked_records"] = blocked_records
        if export_mode == "audit" and audit_blocked_records:
            diag["audit_blocked_records"] = audit_blocked_records

        domains_out[domain_name] = {
            "schema": {"id": "fingerprint.domain", "version": "0.2.0"},
            "summary": summary,
            "diag": diag,
            "records": records,
        }

    domain_policies: Dict[str, Any] = {}
    for domain_name in sorted(domains_out.keys()):
        pol = policy_domains.get(domain_name)
        if not isinstance(pol, dict):
            domain_policies[domain_name] = {"missing_policy": True}
            domains_out[domain_name]["diag"]["warnings"].append("missing_policy")
            continue
        domain_policies[domain_name] = {
            "join_key_schema": pol.get("join_key_schema"),
            "hash_alg": pol.get("hash_alg"),
            "required_keys": list(pol.get("required_items") or []),
            "optional_keys": list(pol.get("optional_items") or []),
            "explicitly_excluded_keys": list(pol.get("explicitly_excluded_items") or []),
            "shape_gating": pol.get("shape_gating") if isinstance(pol.get("shape_gating"), dict) else None,
        }

    exporter_name = _coalesce(
        tr_exporter.get("name"),
        "revit_fingerprint",
    )
    exporter_version = _coalesce(
        tr_exporter.get("version"),
        tool_version,
        "0.0.0+unknown",
    )
    exporter_git_sha = _coalesce(
        tr_exporter.get("git_sha"),
        tool_git_sha,
        "unknown",
    )
    host_python = _coalesce(
        tr_host.get("python"),
        "{} {}".format(platform.python_implementation(), platform.python_version()),
    )
    host_app = _coalesce(tr_host.get("app"), "Revit")
    host_app_ver = _coalesce(tr_host.get("app_version"), host_app_version, "unknown")

    return {
        "contract": {
            "schema": {"id": "fingerprint.export", "version": "0.3.0"},
            "domain_schema": {"id": "fingerprint.domain", "version": "0.2.0"},
            "record_schema": {"id": "fingerprint.record", "version": "0.4.0"},
            "typing_schema": {"id": "fingerprint.typing", "version": "0.1.0"},
            "hashing_schema": {"id": "fingerprint.hashing", "version": "0.2.1"},
            "typing": {"t_enum": ["s", "i", "f", "b", "json"]},
            "hashing": {
                "sig_hash": {"alg": "md5", "encoding": "utf8", "canonical": "k|t|v sorted by k"},
                "join_hash": {"alg": "md5", "encoding": "utf8", "canonical": "domain_policy_current"},
            },
        },
        "manifest": {
            "schema": {"id": "fingerprint.manifest", "version": "0.2.0"},
            "domains_expected": domains_expected,
            "policy_ref": policy_ref,
            "domain_policies": domain_policies,
            "record_requirements": {
                "required_paths": ["domain", "id.sig_hash", "id.join_hash", "label.display", "definition.items"],
            },
        },
        "meta": {
            "schema": {"id": "fingerprint.meta", "version": "0.2.0"},
            "run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
            "exported_at_utc": _utc_now_iso(),
            "export": {"export_mode": export_mode},
            "tools": {
                "exporter": {
                    "name": exporter_name,
                    "version": exporter_version,
                    "git_sha": exporter_git_sha,
                }
            },
            "host": {
                "app": host_app,
                "app_version": host_app_ver,
                "python": host_python,
            },
        },
        "notes": {
            "schema": {"id": "fingerprint.notes", "version": "0.1.0"},
            "operator": list(legacy_payload.get("_notes") or []),
            "known_issues": [],
        },
        "domains": domains_out,
    }
