from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Sequence, Tuple


def _norm(v: Any) -> str:
    return "" if v is None else str(v).strip()


def build_identity_index(identity_items: Sequence[Dict[str, str]]) -> Dict[str, Dict[str, Tuple[str, str]]]:
    """record_pk -> item_key -> (q, v), deterministic tie-break by (q,v)."""
    out: Dict[str, Dict[str, Tuple[str, str]]] = {}
    grouped: Dict[Tuple[str, str], List[Tuple[str, str]]] = defaultdict(list)
    for row in identity_items:
        record_pk = _norm(row.get("record_pk"))
        k = _norm(row.get("item_key") or row.get("k"))
        if not record_pk or not k:
            continue
        q = _norm(row.get("item_value_type") or row.get("q"))
        v = _norm(row.get("item_value") or row.get("v"))

        # Keep q-only rows (blank v) so required-key presence semantics match runtime
        # join_key building, which treats key presence independent of value completeness.
        if not v and not q:
            continue

        grouped[(record_pk, k)].append((q, v))

    for (record_pk, k), vals in sorted(grouped.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        chosen = sorted(vals, key=lambda t: (t[0], t[1]))[0]
        out.setdefault(record_pk, {})[k] = chosen
    return out


def _listish(v: Any) -> List[str]:
    if not isinstance(v, list):
        return []
    return [str(x).strip() for x in v if str(x).strip()]




def _lookup_shape_cfg(shape_requirements: Dict[str, Any], shape_value: str) -> Tuple[str, Any]:
    """Return (matched_key, cfg) with tolerant bool-string matching."""
    if not isinstance(shape_requirements, dict):
        return "", None

    key = str(shape_value).strip()
    if not key:
        return "", None

    if key in shape_requirements:
        return key, shape_requirements.get(key)

    lowered = key.lower()
    for cand in (lowered, lowered.capitalize(), lowered.upper()):
        if cand in shape_requirements:
            return cand, shape_requirements.get(cand)

    if lowered in ("true", "false"):
        py_bool = "True" if lowered == "true" else "False"
        if py_bool in shape_requirements:
            return py_bool, shape_requirements.get(py_bool)

    return "", None

def normalize_policy_block(policy_block: Dict[str, Any] | None, fallback_selected_fields: Sequence[str] | None = None) -> Dict[str, Any]:
    policy_block = policy_block or {}
    fallback_selected = [str(x).strip() for x in (fallback_selected_fields or []) if str(x).strip()]
    selected_fields = _listish(policy_block.get("selected_fields")) or fallback_selected
    required_fields = _listish(policy_block.get("required_fields")) or _listish(policy_block.get("required_items")) or list(selected_fields)
    optional_items = _listish(policy_block.get("optional_items"))
    explicitly_excluded_items = _listish(policy_block.get("explicitly_excluded_items"))

    gates = dict(policy_block.get("gates") or {}) if isinstance(policy_block.get("gates"), dict) else {}
    legacy_shape = policy_block.get("shape_gating") if isinstance(policy_block.get("shape_gating"), dict) else {}
    if legacy_shape:
        if not gates.get("discriminator_key") and legacy_shape.get("discriminator_key"):
            gates["discriminator_key"] = legacy_shape.get("discriminator_key")
        if not isinstance(gates.get("shape_requirements"), dict) and isinstance(legacy_shape.get("shape_requirements"), dict):
            gates["shape_requirements"] = legacy_shape.get("shape_requirements")
        if not gates.get("default_shape_behavior") and legacy_shape.get("default_shape_behavior"):
            gates["default_shape_behavior"] = legacy_shape.get("default_shape_behavior")

    return {
        "selected_fields": selected_fields,
        "required_fields": required_fields,
        "optional_items": optional_items,
        "explicitly_excluded_items": explicitly_excluded_items,
        "gates": gates,
    }


def build_candidate_join_key_with_details(
    identity_items_by_record: Dict[str, Dict[str, Tuple[str, str]]],
    record_pk: str,
    selected_fields: Sequence[str],
    gates: Dict[str, Any] | None = None,
) -> Tuple[str, List[Dict[str, str]], str, Dict[str, Any]]:
    """Returns (status, selected_items, reason, details)."""
    gates = gates or {}
    row_items = identity_items_by_record.get(record_pk, {})

    base_required = [str(f) for f in (gates.get("required_fields") or selected_fields) if str(f).strip()]
    disc_key = str(gates.get("discriminator_key") or "").strip()
    shape_value = ""
    shape_matched = False
    additional_required: List[str] = []
    additional_optional: List[str] = []
    if disc_key and row_items.get(disc_key):
        shape_value = row_items[disc_key][1]
        shape_requirements = gates.get("shape_requirements") if isinstance(gates.get("shape_requirements"), dict) else {}
        _shape_key, shape_cfg = _lookup_shape_cfg(shape_requirements, shape_value)
        if isinstance(shape_cfg, dict):
            shape_matched = True
            additional_required = [str(f).strip() for f in (shape_cfg.get("additional_required") or []) if str(f).strip()]
            # Informational only: additional_optional does not participate in required/
            # selected/hash composition here (discovery's candidate-key model, unlike
            # core/join_key_builder.py's production build_join_key_from_policy(), only
            # ever searches/scores over required fields). Surfaced in `details` purely
            # so summarize_shape_gate_usage() can report it -- previously invisible to
            # any audit even though core/join_key_builder.py already honors it.
            additional_optional = [str(f).strip() for f in (shape_cfg.get("additional_optional") or []) if str(f).strip()]

    required = sorted(set(base_required + additional_required), key=lambda s: s.lower())
    selected: List[Dict[str, str]] = []
    missing: List[str] = []
    for field in required:
        qv = row_items.get(field)
        if not qv:
            missing.append(field)
            continue
        q, v = qv
        selected.append({"k": field, "q": q, "v": v})

    # Domains configured with zero required_items (e.g. units_doc, worksets_doc --
    # single synthetic document-level summary records; policy notes: "all fields
    # optional... nothing blocks it") have no required fields to select from here.
    # Previously that always fell through to selected=[] -> status="blocked",
    # contradicting the policy's own documented intent. When the caller supplies
    # optional_fields via gates AND zero required fields were configured (not
    # "required fields configured but missing on this record" -- that still
    # returns "missing_required" below, unchanged), fall back to whichever
    # optional fields are actually present on the row. Gated behind an explicit
    # gates["optional_fields"] key so discovery/scoring callers (greedy.py,
    # pareto_joinkey_search.py, discover_hash_policy.py's score_candidate path),
    # which never pass this key, keep their exact current candidate-scoring
    # semantics -- this only activates for callers that opt in (apply_join_policy.py).
    optional_fallback_used = False
    optional_selected: List[str] = []
    if not required and not selected:
        optional_fields = [str(f) for f in (gates.get("optional_fields") or []) if str(f).strip()]
        for field in optional_fields:
            qv = row_items.get(field)
            if not qv:
                continue
            q, v = qv
            selected.append({"k": field, "q": q, "v": v})
            optional_selected.append(field)
        optional_fallback_used = bool(optional_fields)

    details = {
        "effective_required_fields": required,
        "missing_required_fields": sorted(missing, key=str.lower),
        "effective_optional_fields": sorted(set(additional_optional) | set(optional_selected), key=str.lower),
        "discriminator_key": disc_key,
        "discriminator_value": shape_value,
        "shape_matched": shape_matched,
        "optional_fallback_used": optional_fallback_used,
    }
    if missing:
        return ("missing_required", selected, ",".join(details["missing_required_fields"]), details)
    if not selected:
        return ("blocked", selected, "no_selected_fields", details)
    return ("ok", sorted(selected, key=lambda it: it["k"]), "", details)


def build_candidate_join_key(
    identity_items_by_record: Dict[str, Dict[str, Tuple[str, str]]],
    record_pk: str,
    selected_fields: Sequence[str],
    gates: Dict[str, Any] | None = None,
) -> Tuple[str, List[Dict[str, str]], str]:
    """Returns (status, selected_items, reason)."""
    status, selected, reason, _details = build_candidate_join_key_with_details(identity_items_by_record, record_pk, selected_fields, gates)
    return status, selected, reason



def summarize_shape_gate_usage(
    records: Sequence[Dict[str, str]],
    identity_items_by_record: Dict[str, Dict[str, Tuple[str, str]]],
    selected_fields: Sequence[str],
    gates: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Audit effective join-key fields for every observed shape-gate value."""
    gates = gates or {}
    discriminator_key = _norm(gates.get("discriminator_key"))
    shape_requirements = gates.get("shape_requirements") if isinstance(gates.get("shape_requirements"), dict) else {}
    out: Dict[str, Any] = {
        "enabled": bool(discriminator_key),
        "discriminator_key": discriminator_key,
        "default_shape_behavior": _norm(gates.get("default_shape_behavior")) or "common_only",
        "configured_shape_values": sorted((str(k) for k in shape_requirements), key=str.lower),
        "records_total": len(records),
        "records_missing_discriminator": 0,
        "records_missing_required": 0,
        "matched_records": 0,
        "unmatched_records": 0,
        "shapes": [],
    }
    if not discriminator_key:
        return out
    by_shape: Dict[str, Dict[str, Any]] = {}
    for row in sorted(records, key=lambda r: (_norm(r.get("record_pk")), _norm(r.get("file_id")))):
        record_pk = _norm(row.get("record_pk"))
        row_items = identity_items_by_record.get(record_pk, {})
        discriminator = row_items.get(discriminator_key)
        shape_value = discriminator[1] if discriminator else ""
        if not shape_value:
            out["records_missing_discriminator"] += 1
            shape_value = "**missing_discriminator**"
        status, _items, _reason, details = build_candidate_join_key_with_details(
            identity_items_by_record, record_pk, selected_fields, gates
        )
        matched = bool(details.get("shape_matched"))
        out["matched_records" if matched else "unmatched_records"] += 1
        if status == "missing_required":
            out["records_missing_required"] += 1
        bucket = by_shape.setdefault(shape_value, {
            "shape_value": shape_value,
            "shape_matched": matched,
            "records": 0,
            "records_missing_required": 0,
            "effective_required_fields": set(),
            "missing_required_fields": set(),
            "effective_optional_fields": set(),
        })
        bucket["records"] += 1
        bucket["shape_matched"] = bucket["shape_matched"] or matched
        if status == "missing_required":
            bucket["records_missing_required"] += 1
        bucket["effective_required_fields"].update(str(v) for v in details.get("effective_required_fields", []) if str(v))
        bucket["missing_required_fields"].update(str(v) for v in details.get("missing_required_fields", []) if str(v))
        bucket["effective_optional_fields"].update(str(v) for v in details.get("effective_optional_fields", []) if str(v))
    for shape_value in sorted(by_shape, key=str.lower):
        bucket = by_shape[shape_value]
        out["shapes"].append({
            "shape_value": bucket["shape_value"],
            "shape_matched": bool(bucket["shape_matched"]),
            "records": int(bucket["records"]),
            "records_missing_required": int(bucket["records_missing_required"]),
            "effective_required_fields": sorted(bucket["effective_required_fields"], key=str.lower),
            "missing_required_fields": sorted(bucket["missing_required_fields"], key=str.lower),
            "effective_optional_fields": sorted(bucket["effective_optional_fields"], key=str.lower),
        })
    return out

def score_candidate(
    records: Sequence[Dict[str, str]],
    identity_items_by_record: Dict[str, Dict[str, Tuple[str, str]]],
    selected_fields: Sequence[str],
    cfg: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    cfg = cfg or {}
    total = len(records)
    by_join: Dict[str, List[str]] = defaultdict(list)
    by_sig: Dict[str, set[str]] = defaultdict(set)
    covered = 0
    failures: Dict[str, int] = defaultdict(int)

    for row in sorted(records, key=lambda r: (_norm(r.get("record_pk")), _norm(r.get("file_id")))):
        record_pk = _norm(row.get("record_pk"))
        status, selected, reason = build_candidate_join_key(identity_items_by_record, record_pk, selected_fields, cfg.get("gates"))
        if status != "ok":
            failures[status if not reason else f"{status}:{reason}"] += 1
            continue
        key_text = "\n".join(f"k={it['k']}|q={it['q']}|v={it['v']}" for it in selected)
        by_join[key_text].append(_norm(row.get("sig_hash")))
        by_sig[_norm(row.get("sig_hash"))].add(key_text)
        covered += 1

    colliding_records = 0
    for sigs in by_join.values():
        if len(set(sigs)) > 1:
            colliding_records += len(sigs)

    frag_records = 0
    for join_keys in by_sig.values():
        if len(join_keys) > 1:
            frag_records += 1

    shares = []
    for sigs in by_join.values():
        shares.append(len(sigs) / covered if covered else 0.0)
    hhi = sum(s * s for s in shares) if shares else 0.0
    eff = (1.0 / hhi) if hhi > 0 else 0.0

    return {
        "selected_fields": list(selected_fields),
        "records_total": total,
        "records_covered": covered,
        "coverage": (covered / total) if total else 0.0,
        "collision_records": colliding_records,
        "collision_rate": (colliding_records / covered) if covered else 1.0,
        "fragmented_sig_count": frag_records,
        "fragmentation_rate": (frag_records / len(by_sig)) if by_sig else 0.0,
        "join_group_count": len(by_join),
        "hhi": hhi,
        "effective_cluster_count": eff,
        "failures": dict(sorted(failures.items(), key=lambda kv: kv[0])),
    }
