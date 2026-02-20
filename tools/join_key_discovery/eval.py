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
        if not v:
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
    if disc_key and row_items.get(disc_key):
        shape_value = row_items[disc_key][1]
        shape_requirements = gates.get("shape_requirements") if isinstance(gates.get("shape_requirements"), dict) else {}
        shape_cfg = shape_requirements.get(shape_value)
        if isinstance(shape_cfg, dict):
            shape_matched = True
            additional_required = [str(f).strip() for f in (shape_cfg.get("additional_required") or []) if str(f).strip()]

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

    details = {
        "effective_required_fields": required,
        "missing_required_fields": sorted(missing, key=str.lower),
        "discriminator_key": disc_key,
        "discriminator_value": shape_value,
        "shape_matched": shape_matched,
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
