from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def detect_export_schema(data: Dict[str, Any]) -> str:
    if not isinstance(data, dict):
        return "unknown"
    if isinstance(data.get("contract"), dict) and isinstance(data.get("domains"), dict):
        return "new"
    if isinstance(data.get("_contract"), dict) or isinstance(data.get("_domains"), dict):
        return "old"
    return "unknown"


def get_top_contract(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    schema = detect_export_schema(data)
    if schema == "new":
        c = data.get("contract")
    else:
        c = data.get("_contract")
    return c if isinstance(c, dict) else None


def get_top_manifest(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    m = data.get("manifest")
    return m if isinstance(m, dict) else None


def iter_domains(data: Dict[str, Any]) -> List[str]:
    if not isinstance(data, dict):
        return []

    schema = detect_export_schema(data)
    if schema == "new":
        doms = data.get("domains")
        if isinstance(doms, dict):
            return [k for k in doms.keys() if isinstance(k, str)]
        return []

    if schema == "old":
        dm = data.get("_domains")
        if isinstance(dm, dict):
            return [k for k in dm.keys() if isinstance(k, str)]

    inferred: List[str] = []
    for k, v in data.items():
        if not isinstance(k, str) or k.startswith("_"):
            continue
        if isinstance(v, dict) and isinstance(v.get("records"), list):
            inferred.append(k)
    return inferred


def get_domain_records(data: Dict[str, Any], domain: str) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []

    schema = detect_export_schema(data)
    payload: Any = None

    if schema == "new":
        doms = data.get("domains")
        if isinstance(doms, dict):
            payload = doms.get(domain)
    else:
        payload = data.get(domain)
        if not isinstance(payload, dict):
            dm = data.get("_domains")
            if isinstance(dm, dict):
                payload = dm.get(domain)

    if not isinstance(payload, dict):
        return []

    recs = payload.get("records")
    if not isinstance(recs, list):
        return []
    return [r for r in recs if isinstance(r, dict)]


def get_definition_items(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(record, dict):
        return []

    definition = record.get("definition")
    if isinstance(definition, dict) and isinstance(definition.get("items"), list):
        return [i for i in definition["items"] if isinstance(i, dict)]

    for key in ("features", "identity_basis", "sig_basis"):
        section = record.get(key)
        if isinstance(section, dict) and isinstance(section.get("items"), list):
            return [i for i in section["items"] if isinstance(i, dict)]

    return []


def build_item_map(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    duplicate_count = 0
    duplicate_keys: List[str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        k = it.get("k")
        if not isinstance(k, str) or not k:
            continue
        if k in out:
            duplicate_count += 1
            duplicate_keys.append(k)
        out[k] = {"k": k, "q": it.get("q"), "t": it.get("t"), "v": it.get("v")}

    out["__duplicate_count__"] = {"v": duplicate_count}
    if duplicate_keys:
        out["__duplicate_keys__"] = {"v": duplicate_keys}
    return out


def qv_from_item(item: Optional[Dict[str, Any]]) -> Tuple[Any, Any]:
    if isinstance(item, dict):
        return item.get("q", ""), item.get("v", "")
    return "missing", ""


def get_id_sig_hash(record: Dict[str, Any]) -> Optional[str]:
    if not isinstance(record, dict):
        return None
    rid = record.get("id")
    if isinstance(rid, dict):
        sig = rid.get("sig_hash")
        if isinstance(sig, str) and sig:
            return sig
    sig = record.get("sig_hash")
    if isinstance(sig, str) and sig:
        return sig
    signature = record.get("signature")
    if isinstance(signature, dict):
        sig2 = signature.get("sig_hash")
        if isinstance(sig2, str) and sig2:
            return sig2
    return None


def get_id_join_hash(record: Dict[str, Any]) -> Optional[str]:
    if not isinstance(record, dict):
        return None
    rid = record.get("id")
    if isinstance(rid, dict):
        jh = rid.get("join_hash")
        if isinstance(jh, str) and jh:
            return jh
    jk = record.get("join_key")
    if isinstance(jk, dict):
        jh2 = jk.get("join_hash")
        if isinstance(jh2, str) and jh2:
            return jh2
    jh3 = record.get("join_hash")
    if isinstance(jh3, str) and jh3:
        return jh3
    return None


def get_record_label(record: Dict[str, Any]) -> Optional[str]:
    if not isinstance(record, dict):
        return None
    label = record.get("label")
    if isinstance(label, str) and label:
        return label
    diagnostics = record.get("diagnostics")
    if isinstance(diagnostics, dict):
        d_label = diagnostics.get("label")
        if isinstance(d_label, str) and d_label:
            return d_label
    return None
