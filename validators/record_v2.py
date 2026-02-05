# validators/record_v2.py
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


_SIG_HASH_RE = re.compile(r"^[0-9a-f]{32}$")
_STATUS_SET = {"ok", "degraded", "blocked"}
_ITEM_Q_SET = {
    "ok",
    "missing",
    "unreadable",
    "unsupported",
    "unsupported.not_applicable",
    "unsupported.not_implemented",
}
_IDENTITY_QUALITY_SET = {
    "complete",
    "incomplete_missing",
    "incomplete_unreadable",
    "incomplete_unsupported",
    "none_blocked",
}


def load_json_file(path: str) -> Dict[str, Any]:
    """Small helper for tests; keep IO out of core exporter if you want."""
    import json

    with open(path, "r") as f:
        return json.load(f)


def validate_record_v2(record: Dict[str, Any], registry: Dict[str, Any]) -> List[str]:
    """
    Validate a single record against record.v2 core invariants plus the per-domain key registry.

    Returns:
        [] if valid
        ["violation.code:detail", ...] if invalid

    Notes:
        - This validator is intentionally strict and deterministic.
        - It does not attempt to "fix" records.
        - It treats unknown domains and missing registry entries as hard violations.
    """
    violations: List[str] = []

    # ---- Core required fields (minimal) ----
    if record.get("schema_version") != registry.get("record_schema_version", "record.v2"):
        violations.append("schema.version.invalid")

    domain = record.get("domain")
    if not isinstance(domain, str) or not domain:
        violations.append("domain.missing_or_invalid")
        return violations

    domains = registry.get("domains", {})
    if domain not in domains:
        violations.append(f"domain.unknown:{domain}")
        return violations

    record_id = record.get("record_id")
    if not isinstance(record_id, str) or not record_id:
        violations.append("record_id.missing_or_invalid")

    record_id_alg = record.get("record_id_alg")
    if not isinstance(record_id_alg, str) or not record_id_alg:
        violations.append("record_id_alg.missing_or_invalid")

    record_id_scope = record.get("record_id_scope")
    if record_id_scope != "file_local":
        violations.append("record_id_scope.invalid")

    status = record.get("status")
    if status not in _STATUS_SET:
        violations.append("status.invalid")

    reasons = record.get("status_reasons")
    if not isinstance(reasons, list):
        violations.append("status_reasons.missing_or_invalid")
    else:
        for r in reasons:
            if not isinstance(r, str) or not r:
                violations.append("status_reasons.entry.invalid")
                break
            # machine-readable token, not free text (kept permissive but bounded)
            if not re.match(r"^[a-z0-9_.:-]+$", r):
                violations.append(f"status_reasons.entry.bad_format:{r}")
                break

    sig_hash = record.get("sig_hash", None)
    if sig_hash is not None:
        if not isinstance(sig_hash, str) or not _SIG_HASH_RE.match(sig_hash):
            violations.append("sig_hash.invalid_format")

    identity_quality = record.get("identity_quality")
    if identity_quality not in _IDENTITY_QUALITY_SET:
        violations.append("identity_quality.invalid")

    # status/sig_hash consistency
    if status == "blocked":
        if sig_hash is not None:
            violations.append("status.blocked.sig_hash_present")
        if identity_quality != "none_blocked":
            violations.append("status.blocked.identity_quality_not_none_blocked")
    elif status in {"ok", "degraded"}:
        if sig_hash is None:
            violations.append("status.not_blocked.sig_hash_missing")

    # label structure
    label = record.get("label")
    if not isinstance(label, dict):
        violations.append("label.missing_or_invalid")
    else:
        if "display" not in label or not isinstance(label.get("display"), str):
            violations.append("label.display.missing_or_invalid")
        if label.get("quality") not in {
            "human",
            "system",
            "placeholder_missing",
            "placeholder_unreadable",
            "placeholder_unsupported",
        }:
            violations.append("label.quality.invalid")
        if label.get("provenance") not in {
            "revit.Name",
            "revit.FamilyName+Name",
            "revit.ViewName",
            "revit.BuiltInEnum",
            "revit.SpecTypeId",
            "computed.path",
            "none",
        }:
            violations.append("label.provenance.invalid")
        if "components" not in label or not isinstance(label.get("components"), dict):
            violations.append("label.components.missing_or_invalid")

        # label placeholder => must not be ok
        if label.get("quality", "").startswith("placeholder_") and status == "ok":
            violations.append("label.placeholder_but_status_ok")

    # identity_basis + items
    identity_basis = record.get("identity_basis")
    if not isinstance(identity_basis, dict):
        violations.append("identity_basis.missing_or_invalid")
        return violations  # cannot proceed safely

    if not isinstance(identity_basis.get("hash_alg"), str) or not identity_basis.get("hash_alg"):
        violations.append("identity_basis.hash_alg.missing_or_invalid")

    if identity_basis.get("item_schema") != registry.get("identity_item_schema", "identity_items.v1"):
        violations.append("identity_basis.item_schema.invalid")

    items = identity_basis.get("items")
    if not isinstance(items, list) or not items:
        violations.append("identity_basis.items.missing_or_invalid")
        return violations

    # ---- Per-domain enforcement ----
    domain_spec = domains[domain]
    allowed_keys = set(domain_spec.get("allowed_keys", []))
    allowed_prefixes = list(domain_spec.get("allowed_key_prefixes", []))
    indexed_rules = domain_spec.get("indexed_key_rules", {}) or {}
    banned_substrings = list(registry.get("banned_identity_value_substrings", []))
    required_keys = list(domain_spec.get("required_keys", []))
    minima = domain_spec.get("minima", {}) or {}
    block_if_any_required_not_ok = bool(minima.get("block_if_any_required_not_ok", False))

    # 1) item structure + k uniqueness + sorting
    ks: List[str] = []
    item_by_k: Dict[str, Dict[str, Any]] = {}

    for idx, it in enumerate(items):
        if not isinstance(it, dict):
            violations.append(f"identity.item.invalid_type:{idx}")
            continue

        k = it.get("k")
        v = it.get("v")
        q = it.get("q")

        if not isinstance(k, str) or not k:
            violations.append(f"identity.item.k.invalid:{idx}")
            continue

        if q not in _ITEM_Q_SET:
            violations.append(f"identity.item.q.invalid:{k}")

        if v is not None and not isinstance(v, str):
            violations.append(f"identity.item.v.invalid_type:{k}")

        # banned substrings in v
        if isinstance(v, str):
            for b in banned_substrings:
                if b in v:
                    violations.append(f"identity.item.v.banned_substring:{k}:{b}")

        if k in item_by_k:
            violations.append(f"identity.item.k.duplicate:{k}")
        else:
            item_by_k[k] = it
            ks.append(k)

    if ks != sorted(ks):
        violations.append("identity.items.not_sorted_by_k")

    # 2) allowed key enforcement
    for k in ks:
        if k in allowed_keys:
            continue
        if _is_allowed_indexed_key(k, allowed_prefixes, indexed_rules):
            continue
        violations.append(f"identity.key.not_allowed:{k}")

    # 3) required keys presence
    for rk in required_keys:
        if rk not in item_by_k:
            violations.append(f"identity.required_key.missing:{rk}")

    # 4) minima: required q must be ok if configured
    required_qs: List[str] = []
    for rk in required_keys:
        it = item_by_k.get(rk)
        if not it:
            continue
        required_qs.append(it.get("q", "missing"))

    if block_if_any_required_not_ok:
        bad_required = [rk for rk in required_keys if (item_by_k.get(rk) and item_by_k[rk].get("q") != "ok")]
        if bad_required:
            if status != "blocked":
                violations.append(f"minima.required_not_ok_but_status_not_blocked:{','.join(bad_required)}")

    # 5) identity_quality derivation check (required keys only)
    computed_quality = _compute_identity_quality(status, required_qs, registry)
    if computed_quality != identity_quality:
        violations.append(f"identity_quality.mismatch:computed={computed_quality}:got={identity_quality}")

    # 6) sig_hash recomputation check (only when not blocked)
    if status != "blocked":
        hash_alg = identity_basis.get("hash_alg")
        if isinstance(hash_alg, str) and hash_alg:
            # Prefer explicit sig basis selectors when present.
            # Fallback to full identity item set for legacy records.
            sig_basis = record.get("sig_basis") if isinstance(record.get("sig_basis"), dict) else {}
            keys_used = sig_basis.get("keys_used")
            if isinstance(keys_used, list) and all(isinstance(k, str) for k in keys_used):
                keyset = set(keys_used)
                basis_items = [it for it in items if isinstance(it, dict) and it.get("k") in keyset]
                preimage = serialize_identity_items(basis_items)
            else:
                preimage = serialize_identity_items(items)

            recomputed = _hash_preimage(preimage, hash_alg)
            if isinstance(sig_hash, str) and recomputed != sig_hash:
                violations.append("sig_hash.mismatch")
        else:
            violations.append("sig_hash.unverifiable_missing_hash_alg")

    return violations


def validate_records_v2(
    records: List[Dict[str, Any]],
    registry: Dict[str, Any],
) -> List[Tuple[str, str]]:
    """
    Validate many records.
    Returns a list of (record_id, violation_code) tuples.
    """
    out: List[Tuple[str, str]] = []
    seen: Dict[Tuple[str, str, str], int] = {}
    for rec in records:
        rid = rec.get("record_id", "<no_record_id>")
        domain = rec.get("domain", "<no_domain>")
        file_id = rec.get("file_id")
        if not isinstance(file_id, str) or not file_id:
            out.append((rid, "file_id.missing_or_invalid"))
            file_id = "<no_file_id>"
        key = (str(file_id), str(domain), str(rid))
        seen[key] = seen.get(key, 0) + 1
        for v in validate_record_v2(rec, registry):
            out.append((rid, v))

    for (file_id, domain, rid), count in seen.items():
        if count > 1:
            out.append((rid, f"record_id.duplicate:{file_id}:{domain}"))
    return out


def serialize_identity_items(items: List[Dict[str, Any]]) -> List[str]:
    """
    Authoritative preimage serialization for record.v2.

    Input MUST already be sorted by k; validator enforces this.

    Format:
      k=<k>|q=<q>|v=<v_or_empty>
    """
    preimage: List[str] = []
    for it in items:
        k = it.get("k", "")
        q = it.get("q", "")
        v = it.get("v", None)
        v_or_empty = "" if v is None else v
        preimage.append(f"k={k}|q={q}|v={v_or_empty}")
    return preimage


def _compute_identity_quality(status: Any, required_qs: List[str], registry: Dict[str, Any]) -> str:
    if status == "blocked":
        return "none_blocked"
    # dominance over required keys only
    if any(q == "unreadable" for q in required_qs):
        return "incomplete_unreadable"
    if any(q == "unsupported" for q in required_qs):
        return "incomplete_unsupported"
    if any(q == "missing" for q in required_qs):
        return "incomplete_missing"
    return "complete"


def _is_allowed_indexed_key(k: str, allowed_prefixes: List[str], indexed_rules: Dict[str, Any]) -> bool:
    """
    Very small matcher:
    - must start with one of the allowed prefixes (e.g. "vf.rule[")
    - must match a known indexed pattern family in indexed_rules via a normalization:
        "vf.rule[012].op" -> "vf.rule[i].op"
    """
    if not any(k.startswith(p) for p in allowed_prefixes):
        return False

    norm = _normalize_indexed_key(k)
    return bool(indexed_rules.get(norm, False))


def _normalize_indexed_key(k: str) -> str:
    # replace any [digits] with [i]
    return re.sub(r"\[\d+\]", "[i]", k)


def _hash_preimage(preimage_strings: List[str], hash_alg: str) -> str:
    """
    Hash helper for tests/validation.

    Supported hash_alg:
      - "md5_utf8_join_pipe"
    """
    if hash_alg != "md5_utf8_join_pipe":
        raise ValueError(f"Unsupported hash_alg: {hash_alg}")

    import hashlib

    joined = "|".join(preimage_strings).encode("utf-8")
    return hashlib.md5(joined).hexdigest()
