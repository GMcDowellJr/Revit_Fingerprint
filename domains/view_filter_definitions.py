# -*- coding: utf-8 -*-
"""domains/view_filter_definitions.py

record.v2 domain: view_filter_definitions

Definitions-only extraction for Revit ParameterFilterElement.

Key contract constraints:
  - No sentinel literals in identity values (enforced by make_identity_item).
  - Parameter references:
      * builtin: stable builtin identifier token (bip:{int})
      * shared: GUID token
      * project/positive ids: do NOT hash int id; represent as v=null + q=unreadable
        unless resolvable to a stable id (shared GUID).

This domain must remain independent of view templates and their application stacks.
"""

from __future__ import annotations

import json

from typing import Any, Dict, List, Optional, Tuple

from Autodesk.Revit import DB as RDB

from core.collect import purge_lookup, collect_instances
from core.hashing import make_hash, safe_str
from core.record_v2 import (
    ITEM_Q_MISSING,
    ITEM_Q_OK,
    ITEM_Q_UNREADABLE,
    STATUS_BLOCKED,
    STATUS_DEGRADED,
    STATUS_OK,
    build_record_v2,
    canonical_structural_fields,
    canonicalize_int,
    canonicalize_str,
    finalize_record_ids_for_domain,
    make_identity_item,
    make_record_id_from_element,
    make_record_id_structural,
    serialize_identity_items,
)

from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
)

from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

def _logic_root_token(elem_filter) -> Tuple[Optional[str], str]:
    """Return (v, q) for vf.logic_root."""
    if elem_filter is None:
        # Selection filter (no ElementFilter) is a valid definition.
        return "selection", ITEM_Q_OK
    try:
        t = elem_filter.GetType()
        # Prefer FullName to reduce ambiguity.
        tn = getattr(t, "FullName", None) or getattr(t, "Name", None)
        return canonicalize_str(tn)
    except Exception:
        return None, ITEM_Q_UNREADABLE


def _param_ref_from_param_id(doc, param_id: Any) -> Tuple[Tuple[Optional[str], str], Tuple[Optional[str], str], List[str]]:
    """Return ((kind_v, kind_q), (id_v, id_q), status_reasons_additions)."""
    reasons: List[str] = []

    if param_id is None:
        return (None, ITEM_Q_MISSING), (None, ITEM_Q_MISSING), ["param_ref.id_missing"]

    try:
        pid = param_id
        if isinstance(pid, ElementId):
            pid_int = int(pid.IntegerValue)
        else:
            pid_int = int(getattr(pid, "IntegerValue", pid))
    except Exception:
        return (None, ITEM_Q_UNREADABLE), (None, ITEM_Q_UNREADABLE), ["param_ref.id_unreadable"]

    if pid_int < 0:
        # BuiltInParameter (stable negative int).
        return ("builtin", ITEM_Q_OK), (f"bip:{pid_int}", ITEM_Q_OK), reasons

    if pid_int == 0:
        return (None, ITEM_Q_MISSING), (None, ITEM_Q_MISSING), ["param_ref.id_missing"]

    # Positive id: must not hash raw int. Try resolve to SharedParameterElement GUID.
    kind = ("project", ITEM_Q_OK)
    try:
        elem = doc.GetElement(ElementId(pid_int))
    except Exception:
        elem = None

    if elem is not None:
        try:
            if isinstance(elem, SharedParameterElement):
                try:
                    g = getattr(elem, "GuidValue", None)
                except Exception:
                    g = None
                gv, gq = canonicalize_str(g)
                if gv is not None and gq == ITEM_Q_OK:
                    return ("shared", ITEM_Q_OK), (gv, ITEM_Q_OK), reasons
        except Exception:
            pass

    # Unresolvable to stable id
    reasons.append("param_ref.positive_id_unresolvable")
    return kind, (None, ITEM_Q_UNREADABLE), reasons


def _value_token_from_rule(doc, rule) -> Tuple[Optional[str], str, Optional[str]]:
    """Return (value_v, value_q, kind_v) where kind_v is a stable rule kind token."""

    # Try to discriminate by common rule attributes.
    # Revit rule classes differ across versions; keep this defensive.

    # StringRule: RuleString
    try:
        if hasattr(rule, "RuleString"):
            v, q = canonicalize_str(getattr(rule, "RuleString", None))
            return v, q, "string"
    except Exception:
        return None, ITEM_Q_UNREADABLE, "string"

    # IntegerRule: RuleValue
    try:
        if hasattr(rule, "RuleValue"):
            rv = getattr(rule, "RuleValue", None)
            # Could be int, double, ElementId, etc.
            if isinstance(rv, ElementId):
                iv = int(rv.IntegerValue)
                if iv < 0:
                    return f"eid:{iv}", ITEM_Q_OK, "element_id"
                # Positive element ids are not stable without resolving to uid.
                # Contract: do not resolve to UID; treat as unreadable for identity.
                return None, ITEM_Q_UNREADABLE, "element_id"

            # Prefer int if it is integral
            iv, iq = canonicalize_int(rv)
            if iq == ITEM_Q_OK:
                return iv, iq, "int"

            # Fallback: attempt float-like string canonicalization
            try:
                fv = float(rv)
                return ("{:.9f}".format(fv), ITEM_Q_OK, "float")
            except Exception:
                return None, ITEM_Q_UNREADABLE, "unknown"
    except Exception:
        return None, ITEM_Q_UNREADABLE, "unknown"

    # Unknown rule kind
    return None, ITEM_Q_UNREADABLE, "unknown"


def _op_token_from_rule(rule) -> Tuple[Optional[str], str]:
    """Return (op_v, op_q) for vf.rule[i].op."""
    try:
        ev = rule.GetEvaluator() if hasattr(rule, "GetEvaluator") else None
        if ev is None:
            return None, ITEM_Q_MISSING
        t = ev.GetType()
        tn = getattr(t, "FullName", None) or getattr(t, "Name", None)
        return canonicalize_str(tn)
    except Exception:
        return None, ITEM_Q_UNREADABLE


def _walk_rules(elem_filter, out_rules: List[Dict[str, Any]], doc, rule_prefix: str = "") -> Tuple[bool, Optional[str]]:
    """Depth-first traversal accumulating parameter rules.

    Returns:
      (ok, reason)
    """
    if elem_filter is None:
        return True, None

    def _append_rule(rule_obj, prefix: str) -> Tuple[bool, Optional[str]]:
        """Append a rule, unwrapping FilterInverseRule wrappers into NOT.-prefixed inner rule."""
        try:
            if FilterInverseRule is not None and isinstance(rule_obj, FilterInverseRule):
                inner_rule = rule_obj.GetInnerRule() if hasattr(rule_obj, "GetInnerRule") else None
                if inner_rule is None:
                    return False, "filter_tree.rules_unreadable"
                return _append_rule(inner_rule, "{}NOT.".format(prefix))
            out_rules.append({"rule": rule_obj, "prefix": prefix})
            return True, None
        except Exception:
            return False, "filter_tree.rules_unreadable"

    # Logical nodes
    try:
        if isinstance(elem_filter, LogicalAndFilter) or isinstance(elem_filter, LogicalOrFilter):
            kids = list(elem_filter.GetFilters() or []) if hasattr(elem_filter, "GetFilters") else []
            for k in kids:
                ok, reason = _walk_rules(k, out_rules, doc, rule_prefix=rule_prefix)
                if not ok:
                    return False, reason
            return True, None
    except Exception:
        return False, "filter_tree.logical_unreadable"

    # Inverse wrapper: NOT(inner_rule)
    try:
        if FilterInverseRule is not None and isinstance(elem_filter, FilterInverseRule):
            inner_rule = elem_filter.GetInnerRule() if hasattr(elem_filter, "GetInnerRule") else None
            if inner_rule is None:
                return False, "filter_tree.rules_unreadable"
            return _append_rule(inner_rule, "{}NOT.".format(rule_prefix))
    except Exception:
        return False, "filter_tree.rules_unreadable"

    # Leaf: ElementParameterFilter rules
    try:
        if isinstance(elem_filter, ElementParameterFilter):
            rules = list(elem_filter.GetRules() or []) if hasattr(elem_filter, "GetRules") else []
            for r in rules:
                ok, reason = _append_rule(r, rule_prefix)
                if not ok:
                    return False, reason
            return True, None
    except Exception:
        return False, "filter_tree.rules_unreadable"

    # Unknown leaf type
    return False, "filter_tree.leaf_unsupported"


def extract(doc, ctx=None):
    """Extract record.v2 view filter definitions."""

    result: Dict[str, Any] = {
        "count": 0,
        "raw_count": 0,
        "records": [],
                "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    try:
        col = list(
            collect_instances(
                doc,
                of_class=ParameterFilterElement,
                require_unique_id=False,  # uid_or_namekey handles fallback
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="view_filter_definitions:ParameterFilterElement:instances",
            )
        )
    except Exception as e:
        result["debug_v2_blocked"] = True
        result["debug_v2_block_reasons"] = {"collect_failed": safe_str(str(e))}
        return result

    result["raw_count"] = len(col)

    record_specs: List[Dict[str, Any]] = []
    v2_sig_hashes: List[str] = []
    v2_block_reasons: Dict[str, Any] = {}
    uid_to_sig_hash: Dict[str, str] = {}

    for f in col:
        try:
            name_raw = getattr(f, "Name", None)
        except Exception:
            name_raw = None
        name_v, _name_q = canonicalize_str(name_raw)
        label_display = "View Filter Definition ({})".format(name_v or "(unnamed)")
        label = {
            "display": label_display,
            "quality": ("human" if name_v is not None else "placeholder_missing"),
            "provenance": "revit.Name",
            "components": {"name": safe_str(name_v or "")},
        }

        identity_items: List[Dict[str, Any]] = []
        status_reasons: List[str] = []

        # UID is metadata only (must not participate in identity/sig_hash).
        try:
            uid_raw = getattr(f, "UniqueId", None)
        except Exception:
            uid_raw = None
        uid_v, uid_q = canonicalize_str(uid_raw)

        # vf.categories (negative ids only; positive ids are unstable)
        cats_v = None
        cats_q = ITEM_Q_MISSING
        try:
            cat_ids = list(f.GetCategories() or [])
            neg = []
            pos_seen = False
            for cid in cat_ids:
                try:
                    iv = int(getattr(cid, "IntegerValue", cid))
                    if iv < 0:
                        neg.append(str(iv))
                    elif iv > 0:
                        pos_seen = True
                except Exception:
                    pos_seen = True
            neg_sorted = sorted(set([x for x in neg if x]))
            if pos_seen:
                cats_v, cats_q = (None, ITEM_Q_UNREADABLE)
            else:
                cats_v, cats_q = (
                    (",".join(neg_sorted) if neg_sorted else None),
                    ITEM_Q_OK if neg_sorted else ITEM_Q_MISSING,
                )
        except Exception:
            cats_v, cats_q = (None, ITEM_Q_UNREADABLE)

        identity_items.append(make_identity_item("vf.categories", cats_v, cats_q))

        # Element filter (tree)
        try:
            elem_filter = f.GetElementFilter() if hasattr(f, "GetElementFilter") else None
        except Exception:
            elem_filter = None

        # Required: vf.logic_root
        logic_v, logic_q = _logic_root_token(elem_filter)
        identity_items.append(make_identity_item("vf.logic_root", logic_v, logic_q))

        # Rules
        rules: List[Dict[str, Any]] = []
        structural_rules: List[Dict[str, Any]] = []
        ok_tree, tree_reason = _walk_rules(elem_filter, rules, doc)
        if not ok_tree and tree_reason:
            status_reasons.append(tree_reason)

        # Required: vf.rule_count
        if ok_tree:
            rc_v, rc_q = canonicalize_int(len(rules))
        else:
            rc_v, rc_q = (None, ITEM_Q_UNREADABLE)
        identity_items.append(make_identity_item("vf.rule_count", rc_v, rc_q))

        # Indexed rule keys
        for idx, rr in enumerate(rules):
            r = rr.get("rule")
            rule_prefix = safe_str(rr.get("prefix") or "")
            idx3 = "{:03d}".format(idx)

            val_v, val_q, kind_v = _value_token_from_rule(doc, r)
            kind_cv, kind_cq = canonicalize_str(kind_v)
            identity_items.append(make_identity_item(f"vf.rule[{idx3}].kind", kind_cv, kind_cq))
            identity_items.append(make_identity_item(f"vf.rule[{idx3}].value", val_v, val_q))

            op_v, op_q = _op_token_from_rule(r)
            identity_items.append(make_identity_item(f"vf.rule[{idx3}].op", op_v, op_q))
            identity_items.append(make_identity_item(f"vf.rule[{idx3}].prefix", rule_prefix or None, ITEM_Q_OK))

            pid = None
            # Revit rule classes differ by version; prefer method if available.
            try:
                if hasattr(r, "GetRuleParameter"):
                    pid = r.GetRuleParameter()
                elif hasattr(r, "ParameterId"):
                    pid = getattr(r, "ParameterId", None)
            except Exception:
                pid = None

            (pk_v, pk_q), (pi_v, pi_q), add_reasons = _param_ref_from_param_id(doc, pid)
            identity_items.append(make_identity_item(f"vf.rule[{idx3}].param_ref.kind", pk_v, pk_q))
            identity_items.append(make_identity_item(f"vf.rule[{idx3}].param_ref.id", pi_v, pi_q))
            status_reasons.extend(add_reasons)
            structural_rules.append(
                {
                    "kind": safe_str(kind_cv or ""),
                    "value": val_v,
                    "value_q": safe_str(val_q),
                    "op": safe_str(op_v or ""),
                    "param_kind": safe_str(pk_v or ""),
                    "param_id": safe_str(pi_v or ""),
                    "param_id_q": safe_str(pi_q),
                    "prefix": rule_prefix,
                }
            )

            # Derived rule atom: bind op + param_ref.id + value (+ kind) into a single definition unit.
            # Canonical form is JSON to avoid delimiter/escaping ambiguities.
            if (
                kind_cq == ITEM_Q_OK
                and op_q == ITEM_Q_OK
                and pi_q == ITEM_Q_OK
                and val_q == ITEM_Q_OK
                and kind_cv is not None
                and op_v is not None
                and pi_v is not None
            ):
                try:
                    sig_obj = {"kind": kind_cv, "op": op_v, "param_ref.id": pi_v, "value": val_v}
                    sig_v = json.dumps(sig_obj, separators=(",", ":"), ensure_ascii=True, sort_keys=True)
                    if rule_prefix:
                        sig_v = "{}{}".format(rule_prefix, sig_v)
                    sig_q = ITEM_Q_OK
                except Exception:
                    sig_v, sig_q = (None, ITEM_Q_UNREADABLE)
            else:
                # If the rule components are not all OK, the combined signature is not safely defined.
                # Choose MISSING only if all are missing, otherwise UNREADABLE.
                if (op_q == ITEM_Q_MISSING and pi_q == ITEM_Q_MISSING and val_q == ITEM_Q_MISSING):
                    sig_v, sig_q = (None, ITEM_Q_MISSING)
                else:
                    sig_v, sig_q = (None, ITEM_Q_UNREADABLE)

            identity_items.append(make_identity_item(f"vf.rule[{idx3}].sig", sig_v, sig_q))

        # Canonical evidence superset for this pilot is identity_basis.items.
        # Selectors (join_key.keys_used, phase2.semantic_keys, sig_basis.keys_used)
        # define hashed/semantic subsets without duplicating k/q/v evidence.
        semantic_keys = sorted({it.get("k") for it in identity_items if isinstance(it.get("k"), str)})
        semantic_key_set = set(semantic_keys)
        semantic_basis_items = sorted(
            [it for it in identity_items if it.get("k") in semantic_key_set],
            key=lambda it: str(it.get("k", "")),
        )

        # Backward-compat pointer retained for current join-key policy (vf.def_hash required).
        # Future direction is selector-based joins directly from canonical evidence.
        if semantic_basis_items:
            def_hash_v = make_hash(serialize_identity_items(semantic_basis_items))
            def_hash_q = ITEM_Q_OK
        else:
            def_hash_v = None
            def_hash_q = ITEM_Q_MISSING
        identity_items.append(make_identity_item("vf.def_hash", def_hash_v, def_hash_q))

        items_sorted = sorted(identity_items, key=lambda it: str(it.get("k", "")))

        # Minima: block if any required key q != ok
        required_keys = ["vf.logic_root", "vf.rule_count"]

        item_by_k = {it.get("k"): it for it in items_sorted}
        required_qs = [safe_str(item_by_k.get(rk, {}).get("q", ITEM_Q_MISSING)) for rk in required_keys]
        blocked = any(q != ITEM_Q_OK for q in required_qs)

        any_incomplete = False
        for it in items_sorted:
            q = it.get("q")
            if q != ITEM_Q_OK:
                any_incomplete = True
                k = it.get("k")
                status_reasons.append("identity.incomplete:{}:{}".format(q, k))

        if blocked:
            status = STATUS_BLOCKED
            status_reasons = sorted(set(status_reasons)) or ["minima.required_not_ok"]
            sig_hash = None
        else:
            status = STATUS_DEGRADED if any_incomplete else STATUS_OK
            # sig_hash is semantic-basis derived (not join-key-material derived).
            sig_hash = make_hash(serialize_identity_items(semantic_basis_items))
            v2_sig_hashes.append(sig_hash)

        record_id = None
        record_id_alg = None
        record_id_base = None
        record_id_sort_key = None

        rid_info = make_record_id_from_element(f)
        if rid_info:
            record_id, record_id_alg = rid_info
        else:
            identity_preimage = serialize_identity_items(items_sorted)
            structural_fields = {
                "identity_preimage": identity_preimage,
                "structural_rules": structural_rules,
            }
            record_id_base, record_id_alg, _canon = make_record_id_structural(structural_fields)
            record_id = record_id_base
            record_id_sort_key = canonical_structural_fields(
                {
                    "identity_preimage": identity_preimage,
                    "status": status,
                    "status_reasons": sorted(set(status_reasons)),
                }
            )

        # Downstream mapping (UniqueId only)
        if uid_v is not None and uid_q == ITEM_Q_OK and sig_hash:
            uid_to_sig_hash[uid_v] = sig_hash

        # -----------------------------
        # Phase 2 (empirical, additive)
        # -----------------------------

        # Join key must be built from the canonical evidence superset via policy selectors.
        pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "view_filter_definitions")

        # Unknown: element-backed id may vary across files.
        try:
            _eid = int(getattr(f.Id, "IntegerValue", 0))
            _eid_v, _eid_q = canonicalize_int(_eid)
        except Exception:
            _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)

        # Traceability fields (metadata only — never in hash/sig/join)
        try:
            _trace_eid_raw = getattr(getattr(f, "Id", None), "IntegerValue", None)
            _trace_eid_v, _trace_eid_q = canonicalize_int(_trace_eid_raw)
        except Exception:
            _trace_eid_v, _trace_eid_q = (None, ITEM_Q_UNREADABLE)
        try:
            _trace_uid_raw = getattr(f, "UniqueId", None)
            _trace_uid_v, _trace_uid_q = canonicalize_str(_trace_uid_raw)
        except Exception:
            _trace_uid_v, _trace_uid_q = (None, ITEM_Q_UNREADABLE)

        p2_unknown = phase2_sorted_items([
            {"k": "vf.elem_id", "q": _eid_q, "v": _eid_v},
            {"k": "vf.source_element_id", "q": _trace_eid_q, "v": _trace_eid_v},
            {"k": "vf.source_unique_id", "q": _trace_uid_q, "v": _trace_uid_v},
        ])

        record_specs.append(
            {
                "domain": "view_filter_definitions",
                "record_id": record_id,
                "record_id_alg": record_id_alg,
                "record_id_base": record_id_base,
                "record_id_sort_key": record_id_sort_key,
                "status": status,
                "status_reasons": sorted(set(status_reasons)),
                "sig_hash": sig_hash,
                "identity_items": items_sorted,
                "required_qs": required_qs,
                "label": label,
                "pf": f,
                "phase2_payload": {
                    "semantic_keys": semantic_keys,
                    "p2_unknown": p2_unknown,
                    "join_key_policy": pol,
                },
            }
        )

    finalize_record_ids_for_domain(record_specs)

    v2_records: List[Dict[str, Any]] = []
    for spec in record_specs:
        rec = build_record_v2(
            domain=spec["domain"],
            record_id=spec["record_id"],
            record_id_alg=spec["record_id_alg"],
            status=spec["status"],
            status_reasons=spec["status_reasons"],
            sig_hash=spec["sig_hash"],
            identity_items=spec["identity_items"],
            required_qs=spec["required_qs"],
            label=spec["label"],
        )
        _ip, _ip_q = purge_lookup(
            getattr(getattr(spec.get("pf"), "Id", None), "IntegerValue", None),
            ctx,
        )
        rec["is_purgeable"] = _ip
        rec["is_purgeable_q"] = _ip_q
        if rec["status"] == STATUS_BLOCKED:
            v2_block_reasons[f"record_blocked:{rec['record_id']}"] = True
        pol = spec["phase2_payload"]["join_key_policy"]
        semantic_keys = spec["phase2_payload"]["semantic_keys"]
        p2_unknown = spec["phase2_payload"]["p2_unknown"]
        rec["join_key"], _missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=spec["identity_items"],
            include_optional_items=False,
            emit_keys_used=True,
            hash_optional_items=False,
            emit_items=False,
            emit_selectors=True,
        )

        rec["phase2"] = {
            "schema": "phase2.view_filter_definitions.v1",
            "grouping_basis": "phase2.hypothesis",
            # Selector-based semantic basis; canonical evidence lives in identity_basis.items.
                        "cosmetic_items": phase2_sorted_items([]),
            "unknown_items": p2_unknown,
        }
        rec["sig_basis"] = {
            "schema": "view_filter_definitions.sig_basis.v1",
            "keys_used": semantic_keys,
        }

        v2_records.append(rec)

    result["records"] = sorted(v2_records, key=lambda r: safe_str(r.get("record_id", "")))
    result["count"] = len(result["records"])

    if v2_block_reasons:
        # Any blocked record blocks the domain hash (contract invariant).
        result["hash_v2"] = None
        result["debug_v2_blocked"] = True
        result["debug_v2_block_reasons"] = v2_block_reasons
    else:
        # Empty domain is valid: emit deterministic empty hash and stay unblocked.
        result["hash_v2"] = make_hash(sorted(v2_sig_hashes))
        result["debug_v2_blocked"] = False
        result["debug_v2_block_reasons"] = {}

    if ctx is not None:
        ctx["view_filter_uid_to_sig_hash_v2"] = uid_to_sig_hash if result.get("hash_v2") is not None else {}

    return result
ElementId = RDB.ElementId
ElementParameterFilter = RDB.ElementParameterFilter
LogicalAndFilter = RDB.LogicalAndFilter
LogicalOrFilter = RDB.LogicalOrFilter
ParameterFilterElement = RDB.ParameterFilterElement
SharedParameterElement = RDB.SharedParameterElement
FilterInverseRule = getattr(RDB, "FilterInverseRule", None)
