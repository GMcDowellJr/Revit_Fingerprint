# -*- coding: utf-8 -*-
"""domains/view_filter_applications_view_templates.py

record.v2 domain: view_filter_applications_view_templates

Extract the ordered view filter application stacks from View Templates.

Hard constraints:
  - No sentinel literals in identity values.
  - Applications reference view_filter_definitions records by sig_hash.
  - Missing/unresolvable dependencies surface via q + dependency.* reasons.
  - Minima (per registry): block if required keys are not ok.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from Autodesk.Revit.DB import ElementId, View, ViewSchedule

from core.collect import collect_instances
from core.hashing import make_hash, safe_str
from core.record_v2 import (
    ITEM_Q_MISSING,
    ITEM_Q_OK,
    ITEM_Q_UNSUPPORTED,
    ITEM_Q_UNREADABLE,
    STATUS_BLOCKED,
    STATUS_DEGRADED,
    STATUS_OK,
    build_record_v2,
    canonical_structural_fields,
    canonicalize_bool,
    canonicalize_int,
    canonicalize_str,
    finalize_record_ids_for_domain,
    make_identity_item,
    make_record_id_from_element,
    make_record_id_structural,
    serialize_identity_items,
)

from core.phase2 import (
    phase2_join_hash,
    phase2_sorted_items,
)


from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy


def _is_schedule_view(view) -> bool:
    # Most reliable: schedule views/templates are ViewSchedule instances.
    try:
        if isinstance(view, ViewSchedule):
            return True
    except Exception:
        pass

    # Fallback: tolerate enum stringify variations
    try:
        vt = getattr(view, "ViewType", None)
        s = safe_str(vt)
        return s == "Schedule" or s.endswith(".Schedule") or (".Schedule" in s)
    except Exception:
        return False


def _semantic_keys_from_identity_items(identity_items: List[Dict[str, Any]]) -> List[str]:
    """Declare semantic sig basis as selectors over canonical identity evidence."""
    keys = sorted(
        {
            safe_str(it.get("k", ""))
            for it in (identity_items or [])
            if isinstance(it.get("k"), str)
            # Keep join-key material separate from semantic signature basis.
            and safe_str(it.get("k", "")) != "vfa.stack_def_hash"
        }
    )
    return [k for k in keys if k]


def extract(doc, ctx=None):
    """Extract record.v2 view filter application stacks for view templates."""

    diag = None
    if isinstance(ctx, dict):
        diag = ctx.get("diag")

    result: Dict[str, Any] = {
        "count": 0,
        "raw_count": 0,
        "records": [],
        "hash": None,  # legacy hash intentionally not defined
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    def_map = {}
    if isinstance(ctx, dict):
        def_map = ctx.get("view_filter_uid_to_sig_hash_v2", {}) or {}

    diag = None
    if isinstance(ctx, dict):
        diag = ctx.get("diag")

    try:
        col = list(
            collect_instances(
                doc,
                of_class=View,
                require_unique_id=False,
                where=lambda v: bool(getattr(v, "IsTemplate", False)),
                where_key="IsTemplate==True",
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="view_filter_applications_view_templates:View:templates",
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

    for v in col:
        try:
            name_raw = getattr(v, "Name", None)
        except Exception:
            name_raw = None

        name_v, _name_q = canonicalize_str(name_raw)
        label_display = "View Template Filter Stack ({})".format(name_v or "(unnamed)")
        label = {
            "display": label_display,
            "quality": ("human" if name_v is not None else "placeholder_missing"),
            "provenance": "revit.ViewName",
            "components": {"name": safe_str(name_v or "")},
        }

        identity_items: List[Dict[str, Any]] = []
        status_reasons: List[str] = []

        # Required: vfa.template_uid_or_namekey
        try:
            uid_raw = getattr(v, "UniqueId", None)
        except Exception:
            uid_raw = None
        uid_v, uid_q = canonicalize_str(uid_raw)
        if uid_v is None:
            uid_v, uid_q = canonicalize_str(name_raw)
        identity_items.append(make_identity_item("vfa.template_uid_or_namekey", uid_v, uid_q))

        # Filter stack
        filter_ids: Optional[List[ElementId]] = None
        stack_q = ITEM_Q_OK

        if _is_schedule_view(v):
            # Schedules/templates do not have view filters. Treat as empty + OK.
            filter_ids = []
            stack_q = ITEM_Q_OK
        else:
            try:
                if hasattr(v, "GetFilters"):
                    filter_ids = list(v.GetFilters() or [])
                    stack_q = ITEM_Q_OK
                else:
                    filter_ids = []
                    stack_q = ITEM_Q_UNSUPPORTED
                    status_reasons.append("filter_stack.unsupported:missing_api")
            except Exception:
                # Some view/template types throw here even when API is reachable.
                # Treat as unsupported rather than unreadable so downstream can degrade if policy allows.
                filter_ids = []
                stack_q = ITEM_Q_UNSUPPORTED
                status_reasons.append("filter_stack.unsupported:exception")

        # Normalize stack order to match dialog display: sort by filter name (then id) deterministically.
        if filter_ids is not None and len(filter_ids) > 1:
            decorated = []
            for _fid in filter_ids:
                _nm = ""
                try:
                    _fe = doc.GetElement(_fid)
                    _nm_raw = getattr(_fe, "Name", None) if _fe is not None else None
                    _nm_v, _ = canonicalize_str(_nm_raw)
                    _nm = safe_str(_nm_v or "")
                except Exception:
                    _nm = ""
                try:
                    _iid = int(getattr(_fid, "IntegerValue", 0))
                except Exception:
                    _iid = 0
                decorated.append((_nm.lower(), _iid, _fid))
            decorated.sort(key=lambda t: (t[0], t[1]))
            filter_ids = [t[2] for t in decorated]

        # Required: vfa.filter_stack_count
        if filter_ids is not None:
            sc_v, _ = canonicalize_int(len(filter_ids))
            sc_q = stack_q
        else:
            sc_v, sc_q = (None, ITEM_Q_UNREADABLE)
            status_reasons.append("filter_stack.unreadable")
        identity_items.append(make_identity_item("vfa.filter_stack_count", sc_v, sc_q))

        if filter_ids is not None and stack_q != ITEM_Q_UNREADABLE:
            for i, fid in enumerate(filter_ids):
                idx3 = "{:03d}".format(i)

                f_uid_v, f_uid_q = (None, ITEM_Q_UNREADABLE)
                try:
                    fe = doc.GetElement(fid)
                    f_uid_v, f_uid_q = canonicalize_str(getattr(fe, "UniqueId", None) if fe is not None else None)
                except Exception:
                    f_uid_v, f_uid_q = (None, ITEM_Q_UNREADABLE)

                sig_v, sig_q = (None, ITEM_Q_UNREADABLE)
                if f_uid_v is None or f_uid_q != ITEM_Q_OK:
                    status_reasons.append("dependency.unreadable:view_filter_definitions")
                else:
                    mapped = def_map.get(f_uid_v)
                    if mapped:
                        sig_v, sig_q = (safe_str(mapped), ITEM_Q_OK)
                    else:
                        status_reasons.append("dependency.unresolved:view_filter_definitions")

                identity_items.append(make_identity_item(f"vfa.stack[{idx3}].filter_sig_hash", sig_v, sig_q))

                vis_v, vis_q = (None, ITEM_Q_UNREADABLE)
                try:
                    if hasattr(v, "GetFilterVisibility"):
                        vb = bool(v.GetFilterVisibility(fid))
                        vis_v, vis_q = canonicalize_bool(vb)
                    else:
                        vis_v, vis_q = (None, ITEM_Q_UNSUPPORTED)
                except Exception:
                    vis_v, vis_q = (None, ITEM_Q_UNREADABLE)

                identity_items.append(make_identity_item(f"vfa.stack[{idx3}].visible", vis_v, vis_q))

                en_v, en_q = (None, ITEM_Q_UNREADABLE)
                try:
                    if hasattr(v, "GetIsFilterEnabled"):
                        eb = bool(v.GetIsFilterEnabled(fid))
                        en_v, en_q = canonicalize_bool(eb)
                    else:
                        en_v, en_q = (None, ITEM_Q_UNSUPPORTED)
                        status_reasons.append("filter_enabled.unsupported:missing_api")
                except Exception:
                    en_v, en_q = (None, ITEM_Q_UNREADABLE)
                    status_reasons.append("filter_enabled.unreadable:exception")

                identity_items.append(make_identity_item(f"vfa.stack[{idx3}].enabled", en_v, en_q))

        items_sorted = sorted(identity_items, key=lambda it: str(it.get("k", "")))

        required_keys = ["vfa.template_uid_or_namekey", "vfa.filter_stack_count"]
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

        record_id = None
        record_id_alg = None
        record_id_base = None
        record_id_sort_key = None

        rid_info = make_record_id_from_element(v)
        if rid_info:
            record_id, record_id_alg = rid_info
        else:
            structural_fields = {
                "template_name": safe_str(name_v or ""),
                "identity_preimage": serialize_identity_items(items_sorted),
            }
            record_id_base, record_id_alg, _canon = make_record_id_structural(structural_fields)
            record_id = record_id_base
            record_id_sort_key = canonical_structural_fields(
                {
                    "label": label_display,
                    "structural": structural_fields,
                }
            )

        # -----------------------------
        # Phase 2 (empirical, additive)
        # -----------------------------

        # Phase-2 item partitions (hypotheses only; no inference).
        # Contract for structured domains:
        #   - semantic_items contains ONLY the derived bundle hash pointer
        #   - leaf members (vfa.stack[...].*) are excluded from semantic_items
        stack_items = []
        for it in (items_sorted or []):
            k = safe_str(it.get("k", ""))
            if k.startswith("vfa.stack["):
                stack_items.append(it)

        stack_items_sorted = phase2_sorted_items(stack_items)
        # NOTE: empty stack is a valid definition state; emit a deterministic empty def_hash.
        stack_def_hash = phase2_join_hash(stack_items_sorted) if stack_items_sorted is not None else None

        # Canonical evidence superset is identity_basis.items.
        # Keep derived stack hash as canonical evidence once, then select subsets via keys_used/semantic_keys.
        if stack_def_hash is not None:
            identity_items.append(make_identity_item("vfa.stack_def_hash", stack_def_hash, ITEM_Q_OK))
            items_sorted = sorted(identity_items, key=lambda it: str(it.get("k", "")))

        semantic_keys = _semantic_keys_from_identity_items(items_sorted)
        semantic_key_set = set(semantic_keys)
        semantic_items_for_sig = [it for it in items_sorted if it.get("k") in semantic_key_set]

        if blocked:
            status = STATUS_BLOCKED
            status_reasons = sorted(set(status_reasons)) or ["minima.required_not_ok"]
            sig_hash = None
        else:
            status = STATUS_DEGRADED if any_incomplete else STATUS_OK
            sig_hash = make_hash(serialize_identity_items(semantic_items_for_sig))
            v2_sig_hashes.append(sig_hash)

        # Build record-level join_key from canonical identity evidence via policy selectors.
        pol = get_domain_join_key_policy(
            (ctx or {}).get("join_key_policies"),
            "view_filter_applications_view_templates",
        )

        # Unknown: template element id may vary across files.
        try:
            _eid = int(getattr(v.Id, "IntegerValue", 0))
            _eid_v, _eid_q = canonicalize_int(_eid)
        except Exception:
            _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)

        p2_unknown = phase2_sorted_items([
            {"k": "vfa.template_elem_id", "q": _eid_q, "v": _eid_v},
        ])

        record_specs.append(
            {
                "domain": "view_filter_applications_view_templates",
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
                "phase2_payload": {
                    "p2_unknown": p2_unknown,
                    "semantic_keys": semantic_keys,
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

        pol = spec["phase2_payload"]["join_key_policy"]
        rec["join_key"], _missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=spec["identity_items"],
            include_optional_items=False,
            emit_keys_used=True,
            hash_optional_items=False,
        )

        semantic_keys = spec["phase2_payload"].get("semantic_keys", [])
        p2_unknown = spec["phase2_payload"]["p2_unknown"]

        rec["phase2"] = {
            "schema": "phase2.view_filter_applications_view_templates.v1",
            "grouping_basis": "phase2.hypothesis",
            # Selector-only: semantic basis is declared by keys, evidence remains canonical in identity_basis.items.
            "semantic_keys": semantic_keys,
            "cosmetic_items": phase2_sorted_items([]),
            "coordination_items": phase2_sorted_items([]),
            "unknown_items": p2_unknown,
        }
        rec["sig_basis"] = {
            "schema": "view_filter_applications_view_templates.sig_basis.v1",
            "keys_used": semantic_keys,
        }

        if rec["status"] == STATUS_BLOCKED:
            v2_block_reasons[f"record_blocked:{rec['record_id']}"] = True

        v2_records.append(rec)

    result["records"] = sorted(v2_records, key=lambda r: safe_str(r.get("record_id", "")))
    result["count"] = len(result["records"])

    if v2_sig_hashes and not v2_block_reasons:
        result["hash_v2"] = make_hash(sorted(v2_sig_hashes))
        result["debug_v2_blocked"] = False
        result["debug_v2_block_reasons"] = {}
    else:
        result["hash_v2"] = None
        result["debug_v2_blocked"] = True
        result["debug_v2_block_reasons"] = v2_block_reasons or {"no_nonblocked_records": True}

    return result
