# -*- coding: utf-8 -*-
"""Build pre-v1 fingerprint export payloads from legacy runner surfaces."""

from __future__ import annotations

from datetime import datetime, timezone
import os
import re
from typing import Any, Dict, List, Optional, Tuple

_ALLOWED_T = {"s", "i", "f", "b", "json"}


def get_export_mode() -> str:
    raw = str(os.getenv("REVIT_FINGERPRINT_EXPORT_MODE", "lean") or "lean").strip().lower()
    return "audit" if raw == "audit" else "lean"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
        t = str(it.get("t", "")).strip().lower() or _infer_t(it.get("v"))
        if t not in _ALLOWED_T:
            t = "s"
        out.append({
            "k": k,
            "t": t,
            "v": it.get("v"),
            "q": _map_q(it.get("q")),
        })
    return sorted(out, key=lambda x: x["k"])


def _extract_phase2_unknown_items(rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    p2 = rec.get("phase2") if isinstance(rec.get("phase2"), dict) else {}
    unknown = p2.get("unknown_items") if isinstance(p2.get("unknown_items"), list) else []
    out: List[Dict[str, Any]] = []
    for it in unknown:
        if isinstance(it, dict):
            out.append(it)
    return out


def _split_provenance_unknown(unknown_items: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    source: Dict[str, Any] = {}
    remaining: List[Dict[str, Any]] = []
    for it in unknown_items:
        k = str(it.get("k", "")).lower()
        v = it.get("v")
        if any(token in k for token in ("doc_unique_id", "source_doc_unique_id")):
            source["doc_unique_id"] = str(v) if v is not None else None
            continue
        if any(token in k for token in ("element_unique_id", "source_unique_id")):
            source["element_unique_id"] = str(v) if v is not None else None
            continue
        if re.search(r"(^|[._])element_id$", k) or "source_element_id" in k:
            iv = _to_int(v)
            source["element_id"] = iv if iv is not None else v
            continue
        remaining.append({
            "k": str(it.get("k", "")),
            "t": str(it.get("t", "")).strip().lower() if str(it.get("t", "")).strip().lower() in _ALLOWED_T else _infer_t(v),
            "v": v,
            "q": _map_q(it.get("q")),
        })
    remaining = sorted([x for x in remaining if x.get("k")], key=lambda x: x["k"])
    return source, remaining


def _build_record(domain_name: str, rec: Dict[str, Any], export_mode: str) -> Dict[str, Any]:
    definition_items = _extract_definition_items(rec)
    unknown_phase2 = _extract_phase2_unknown_items(rec)
    provenance_source, unknown_items = _split_provenance_unknown(unknown_phase2)

    join_key = rec.get("join_key") if isinstance(rec.get("join_key"), dict) else {}
    id_block = {
        "sig_hash": rec.get("sig_hash"),
        "join_hash": join_key.get("join_hash"),
    }
    if rec.get("record_id") is not None:
        id_block["record_id"] = rec.get("record_id")

    label = rec.get("label") if isinstance(rec.get("label"), dict) else {}
    display = label.get("display") or label.get("text") or str(rec.get("record_id", ""))

    out: Dict[str, Any] = {
        "schema": {"id": "fingerprint.record", "version": "0.4.0"},
        "domain": domain_name,
        "id": id_block,
        "label": {"display": display},
        "definition": {"items": definition_items},
        "provenance": {"source": provenance_source},
        "diagnostics": {
            "unknown_items": unknown_items,
            "warnings": [],
            "status": rec.get("status"),
            "status_reasons": list(rec.get("status_reasons") or []),
        },
    }

    is_system = label.get("is_system")
    if isinstance(is_system, bool):
        out["label"]["is_system"] = is_system

    if export_mode == "audit":
        sig_basis = rec.get("sig_basis") if isinstance(rec.get("sig_basis"), dict) else {}
        out["audit"] = {
            "sig_basis": {
                "keys_used": list(sig_basis.get("keys_used") or []),
            },
            "join_basis": {
                "schema": join_key.get("schema"),
                "keys_used": list(join_key.get("keys_used") or []),
                "selectors": list(join_key.get("selectors") or []),
            },
        }

    return out


def build_export_payload(*, legacy_payload: Dict[str, Any], tool_version: Optional[str], tool_git_sha: Optional[str], host_app_version: Optional[str]) -> Dict[str, Any]:
    export_mode = get_export_mode()
    contract_domains = legacy_payload.get("_contract", {}).get("domains", {}) if isinstance(legacy_payload.get("_contract"), dict) else {}
    domains_expected = sorted([str(k) for k in contract_domains.keys()])

    domain_policies_raw = legacy_payload.get("_join_key_policies") if isinstance(legacy_payload.get("_join_key_policies"), dict) else {}
    domain_policies: Dict[str, Any] = {}
    for domain_name in sorted(domain_policies_raw.keys()):
        pol = domain_policies_raw.get(domain_name)
        if not isinstance(pol, dict):
            continue
        domain_policies[domain_name] = {
            "join_policy_id": pol.get("policy_id") or f"{domain_name}.join_key",
            "join_policy_version": pol.get("version") or "0.1.0",
            "required_keys": list(pol.get("required") or []),
            "optional_keys": list(pol.get("optional") or []),
        }

    domains_out: Dict[str, Any] = {}
    for domain_name in domains_expected:
        payload = legacy_payload.get(domain_name) if isinstance(legacy_payload.get(domain_name), dict) else {}
        records_raw = payload.get("records") if isinstance(payload.get("records"), list) else []
        records = [_build_record(domain_name, r, export_mode) for r in records_raw if isinstance(r, dict)]
        raw_count = payload.get("raw_count") if isinstance(payload, dict) else None
        if raw_count is None:
            raw_count = payload.get("count") if isinstance(payload, dict) else None
        if raw_count is None:
            raw_count = len(records)
        blocked_count = sum(1 for r in records if r.get("diagnostics", {}).get("status") == "blocked")

        env = contract_domains.get(domain_name) if isinstance(contract_domains.get(domain_name), dict) else {}
        domains_out[domain_name] = {
            "schema": {"id": "fingerprint.domain", "version": "0.2.0"},
            "summary": {
                "raw_count": int(raw_count),
                "exported_count": len(records),
                "blocked_count": blocked_count,
            },
            "diag": {
                "warnings": [],
                "block_reasons": list(env.get("block_reasons") or []),
            },
            "records": records,
        }

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
                    "name": "revit_fingerprint",
                    "version": tool_version,
                    "git_sha": tool_git_sha,
                }
            },
            "host": {
                "app": "Revit",
                "app_version": host_app_version,
                "python": "CPython",
            },
        },
        "notes": {
            "schema": {"id": "fingerprint.notes", "version": "0.1.0"},
            "operator": list(legacy_payload.get("_notes") or []),
            "known_issues": [],
        },
        "domains": domains_out,
    }
