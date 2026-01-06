# -*- coding: utf-8 -*-
"""
View Filters domain extractor.

Fingerprints view filter definitions including:
- Filter rules (parameter-based conditions)
- Categories the filter applies to
- Filter type (rule-based vs selection)

This is a GLOBAL domain - filters are defined once and referenced
by views and view templates.

Per-record identity: UniqueId (element-backed)
Ordering: rules are order-sensitive (preserved), categories are sorted
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import (
    canon_str,
    canon_num,
    canon_bool,
    canon_id,
    sig_val,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)


try:
    from Autodesk.Revit.DB import FilteredElementCollector, ParameterFilterElement
except ImportError:
    FilteredElementCollector = None
    ParameterFilterElement = None

try:
    from Autodesk.Revit.DB import (
        ElementParameterFilter,
        ElementLogicalFilter,
        LogicalAndFilter,
        LogicalOrFilter,
        FilterRule,
        FilterStringRule,
        FilterIntegerRule,
        FilterDoubleRule,
        FilterElementIdRule,
    )
except Exception:
    ElementParameterFilter = None
    ElementLogicalFilter = None
    LogicalAndFilter = None
    LogicalOrFilter = None
    FilterRule = None
    FilterStringRule = None
    FilterIntegerRule = None
    FilterDoubleRule = None
    FilterElementIdRule = None


def _rule_token(rule):
    """
    Convert a Revit FilterRule into a stable, comparable token string.
    Goal: represent parameter + operator/evaluator + value.
    """
    if rule is None:
        return f"rule={S_MISSING}"

    parts = []
    try:
        parts.append("type={}".format(rule.GetType().FullName))
    except Exception:
        parts.append(f"type={S_MISSING}")

    # Parameter id:
    # - negative ids (BuiltInParameter-style) are allowed
    # - positive ElementIds must NOT enter hash surfaces
    try:
        pid = rule.GetRuleParameter() if hasattr(rule, "GetRuleParameter") else None
        pid_int = getattr(pid, "IntegerValue", pid) if pid is not None else None
        try:
            pid_int = int(pid_int) if pid_int is not None else None
        except Exception:
            pid_int = None

        if pid_int is None:
            parts.append(f"param={S_MISSING}")
        elif pid_int < 0:
            parts.append("param_id={}".format(safe_str(pid_int)))
        else:
            parts.append(f"param={S_NOT_APPLICABLE}")
    except Exception:
        parts.append(f"param={S_UNREADABLE}")

    # Try to capture evaluator/operator identity + value
    # String rules
    try:
        if FilterStringRule is not None and isinstance(rule, FilterStringRule):
            try:
                ev = rule.GetEvaluator() if hasattr(rule, "GetEvaluator") else None
                parts.append("op={}".format(ev.GetType().FullName if ev else S_MISSING))
            except Exception:
                parts.append(f"op={S_UNREADABLE}")
            try:
                parts.append("val={}".format(sig_val(canon_str(getattr(rule, "RuleString", None)))))
            except Exception:
                parts.append(f"val={S_UNREADABLE}")
            return "|".join(parts)
    except Exception:
        pass

    # Integer rules
    try:
        if FilterIntegerRule is not None and isinstance(rule, FilterIntegerRule):
            try:
                ev = rule.GetEvaluator() if hasattr(rule, "GetEvaluator") else None
                parts.append("op={}".format(ev.GetType().FullName if ev else S_MISSING))
            except Exception:
                parts.append(f"op={S_UNREADABLE}")
            try:
                parts.append("val={}".format(sig_val(safe_str(getattr(rule, "RuleValue", None)))))
            except Exception:
                parts.append(f"val={S_UNREADABLE}")
            return "|".join(parts)
    except Exception:
        pass

    # Double rules
    try:
        if FilterDoubleRule is not None and isinstance(rule, FilterDoubleRule):
            try:
                ev = rule.GetEvaluator() if hasattr(rule, "GetEvaluator") else None
                parts.append("op={}".format(ev.GetType().FullName if ev else S_MISSING))
            except Exception:
                parts.append(f"op={S_UNREADABLE}")
            try:
                parts.append("val={}".format(sig_val(safe_str(getattr(rule, "RuleValue", None)))))
            except Exception:
                parts.append(f"val={S_UNREADABLE}")
            return "|".join(parts)
    except Exception:
        pass

    # ElementId rules
    try:
        if FilterElementIdRule is not None and isinstance(rule, FilterElementIdRule):
            try:
                ev = rule.GetEvaluator() if hasattr(rule, "GetEvaluator") else None
                parts.append("op={}".format(ev.GetType().FullName if ev else S_MISSING))
            except Exception:
                parts.append(f"op={S_UNREADABLE}")
            try:
                v = getattr(rule, "RuleValue", None)
                v_int = getattr(v, "IntegerValue", v)
                try:
                    v_int = int(v_int) if v_int is not None else None
                except Exception:
                    v_int = None

                if v_int is None:
                    parts.append("val={S_MISSING}")
                elif v_int < 0:
                    parts.append("val_id={}".format(safe_str(v_int)))
                else:
                    parts.append(f"val={S_NOT_APPLICABLE}")
            except Exception:
                parts.append(f"val={S_UNREADABLE}")
    except Exception:
        pass

    # Fallback: best-effort string, but still include type + param_id above
    try:
        parts.append("raw={}".format(sig_val(safe_str(rule))))
    except Exception:
        parts.append("raw={S_UNREADABLE}")
    return "|".join(parts)


def _walk_elem_filter(elem_filter, out_tokens):
    """
    Walk ElementFilter trees (AND/OR) and collect rule tokens in a stable order.
    Rules are appended in traversal order.
    """
    if elem_filter is None:
        out_tokens.append("filter={S_MISSING}")
        return

    # Logical filters (AND/OR): recurse into children
    try:
        if ElementLogicalFilter is not None and isinstance(elem_filter, ElementLogicalFilter):
            out_tokens.append("logic={}".format(elem_filter.GetType().FullName))
            kids = list(elem_filter.GetFilters()) if hasattr(elem_filter, "GetFilters") else []
            out_tokens.append("child_count={}".format(len(kids)))
            for k in kids:
                _walk_elem_filter(k, out_tokens)
            return
    except Exception:
        pass

    # Explicit logical types (some environments don’t share ElementLogicalFilter cleanly)
    try:
        if LogicalAndFilter is not None and isinstance(elem_filter, LogicalAndFilter):
            out_tokens.append("logic=LogicalAndFilter")
            kids = list(elem_filter.GetFilters()) if hasattr(elem_filter, "GetFilters") else []
            out_tokens.append("child_count={}".format(len(kids)))
            for k in kids:
                _walk_elem_filter(k, out_tokens)
            return
    except Exception:
        pass

    try:
        if LogicalOrFilter is not None and isinstance(elem_filter, LogicalOrFilter):
            out_tokens.append("logic=LogicalOrFilter")
            kids = list(elem_filter.GetFilters()) if hasattr(elem_filter, "GetFilters") else []
            out_tokens.append("child_count={}".format(len(kids)))
            for k in kids:
                _walk_elem_filter(k, out_tokens)
            return
    except Exception:
        pass

    # Parameter filter: extract real FilterRule objects
    try:
        if ElementParameterFilter is not None and isinstance(elem_filter, ElementParameterFilter):
            out_tokens.append("leaf={}".format(elem_filter.GetType().FullName))
            rules = list(elem_filter.GetRules()) if hasattr(elem_filter, "GetRules") else []
            out_tokens.append("rule_count={}".format(len(rules)))
            for i, r in enumerate(rules):
                idx = "{:03d}".format(i)
                out_tokens.append("rule[{}]={}".format(idx, sig_val(_rule_token(r))))
            return
    except Exception:
        pass

    # Unknown leaf filter type
    try:
        out_tokens.append("leaf={}".format(elem_filter.GetType().FullName))
    except Exception:
        out_tokens.append("leaf={S_MISSING}")

def _rule_token_v2(rule, doc):
    """
    Contract semantic tokenization:
    - No unreadables in semantic.
    - No ToString()/raw fallbacks in semantic.
    - No positive ElementIds in semantic.
    - Positive parameter ids are allowed ONLY if resolvable to a Shared Parameter GUID.
    Returns: (token:str, ok:bool, reason:str|None)
    """
    if rule is None:
        return None, False, "rule_none"

    parts = []

    # Rule type identity (API type name; not user-editable)
    try:
        parts.append("type={}".format(rule.GetType().FullName))
    except Exception:
        return None, False, "type_unreadable"

    # Parameter id (must be negative BuiltInParameter id OR shared-parameter GUID)
    try:
        pid = rule.GetRuleParameter() if hasattr(rule, "GetRuleParameter") else None
        pid_int = getattr(pid, "IntegerValue", pid) if pid is not None else None
        pid_int = int(pid_int) if pid_int is not None else None
    except Exception:
        return None, False, "param_unreadable"

    if pid_int is None:
        return None, False, "param_none"

    if pid_int >= 0:
        # Contract: no guids/unstable ids in semantic surfaces.
        # Positive parameter ids are unstable across projects; block v2.
        return None, False, "param_positive_id"
    else:
        # Negative ids represent BuiltInParameter-style identifiers (stable enum surface).
        parts.append("param_id={}".format(pid_int))

    # Evaluator/operator identity (API type name; not user-editable)
    try:
        ev = rule.GetEvaluator() if hasattr(rule, "GetEvaluator") else None
        ev_name = ev.GetType().FullName if ev else None
        if not ev_name:
            return None, False, "op_none"
        parts.append("op={}".format(ev_name))
    except Exception:
        return None, False, "op_unreadable"

    # Rule value tokenization
    try:
        if FilterStringRule is not None and isinstance(rule, FilterStringRule):
            v = getattr(rule, "RuleString", None)
            if v is None:
                return None, False, "val_none"
            # String values are user-editable but semantically relevant; canon via safe_str + sig_val
            parts.append("val_s={}".format(sig_val(canon_str(v))))
            return "|".join(parts), True, None

        if FilterIntegerRule is not None and isinstance(rule, FilterIntegerRule):
            v = getattr(rule, "RuleValue", None)
            if v is None:
                return None, False, "val_none"
            parts.append("val_i={}".format(sig_val(safe_str(v))))
            return "|".join(parts), True, None

        if FilterDoubleRule is not None and isinstance(rule, FilterDoubleRule):
            v = getattr(rule, "RuleValue", None)
            if v is None:
                return None, False, "val_none"
            parts.append("val_d={}".format(sig_val(safe_str(v))))
            return "|".join(parts), True, None

        if FilterElementIdRule is not None and isinstance(rule, FilterElementIdRule):
            v = getattr(rule, "RuleValue", None)
            v_int = getattr(v, "IntegerValue", v)
            v_int = int(v_int) if v_int is not None else None
            if v_int is None:
                return None, False, "val_none"
            if v_int >= 0:
                return None, False, "val_positive_id"
            parts.append("val_id={}".format(v_int))
            return "|".join(parts), True, None

        return None, False, "rule_type_unsupported"
    except Exception:
        return None, False, "val_unreadable"

def _walk_elem_filter_v2(elem_filter, out_tokens, doc):
    """
    Contract semantic walk:
    - No unreadables/sentinels in semantic.
    Returns: (ok:bool, reason:str|None)
    """
    if elem_filter is None:
        return False, "elem_filter_none"

    # Logical filters
    try:
        if LogicalAndFilter is not None and isinstance(elem_filter, LogicalAndFilter):
            out_tokens.append("logic=LogicalAndFilter")
            kids = list(elem_filter.GetFilters()) if hasattr(elem_filter, "GetFilters") else []
            out_tokens.append("child_count={}".format(len(kids)))
            for k in kids:
                ok, reason = _walk_elem_filter_v2(k, out_tokens, doc)
                if not ok:
                    return False, reason
            return True, None
    except Exception:
        return False, "and_unreadable"

    try:
        if LogicalOrFilter is not None and isinstance(elem_filter, LogicalOrFilter):
            out_tokens.append("logic=LogicalOrFilter")
            kids = list(elem_filter.GetFilters()) if hasattr(elem_filter, "GetFilters") else []
            out_tokens.append("child_count={}".format(len(kids)))
            for k in kids:
                ok, reason = _walk_elem_filter_v2(k, out_tokens, doc)
                if not ok:
                    return False, reason
            return True, None
    except Exception:
        return False, "or_unreadable"

    # Parameter filter leaf: rules
    try:
        if ElementParameterFilter is not None and isinstance(elem_filter, ElementParameterFilter):
            out_tokens.append("leaf=ElementParameterFilter")
            rules = list(elem_filter.GetRules()) if hasattr(elem_filter, "GetRules") else []
            out_tokens.append("rule_count={}".format(len(rules)))

            for i, r in enumerate(rules):
                tok, ok, reason = _rule_token_v2(r, doc)
                if not ok:
                    return False, reason
                out_tokens.append("rule[{}]={}".format("{:03d}".format(i), tok))
            return True, None
    except Exception:
        return False, "leaf_unreadable"

    # Unknown leaf type
    try:
        tname = elem_filter.GetType().FullName
        out_tokens.append("leaf_unknown={}".format(tname))
    except Exception:
        pass
    return False, "leaf_unknown"

def extract(doc, ctx=None):
    """
    Extract View Filters fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (will be populated with filter_uid -> def_hash mapping)

    Returns:
        Dictionary with count, hash, signature_hashes, records,
        record_rows, and debug counters
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "names": [],
        "records": [],
        "signature_hashes": [],
        "hash": None,

        # debug counters
        "debug_missing_name": 0,
        "debug_fail_read": 0,
        "debug_kept": 0,
        # v2 (contract semantic) surfaces - additive only
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_v2_blocked": 0,
        "debug_v2_block_reasons": {},        
    }

    try:
        col = list(FilteredElementCollector(doc).OfClass(ParameterFilterElement))
    except Exception as e:
        return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []
    uid_to_hash = {}  # For context population
    per_hashes_v2 = []
    uid_to_hash_v2 = {}  # For downstream v2 mapping (only when record v2 is ok)

    for f in col:
        # Name is metadata only
        name = canon_str(getattr(f, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            name = S_MISSING
        names.append(name)

        uid = None
        try:
            uid = canon_str(getattr(f, "UniqueId", None))
        except Exception as e:
            uid = None

        # Build filter signature
        # Build v2 (contract semantic) signature in parallel (no names, no unreadables)
        sig_v2 = []
        v2_ok = True
        v2_reason = None
        
        sig = []

        # Filter type (rule-based vs selection-based)
        try:
            # Check if this is a selection filter
            is_selection = hasattr(f, "GetElementFilter") and f.GetElementFilter() is None
            sig.append("is_selection={}".format(sig_val(is_selection)))
        except Exception as e:
            sig.append("is_selection={S_MISSING}")

        # Categories the filter applies to
        try:
            cat_ids = list(f.GetCategories())
            cat_names = []
            cat_ints = []
            for cid in cat_ids:
                try:
                    cat_ints.append(safe_str(getattr(cid, "IntegerValue", cid)))
                except Exception as e:
                    pass
                try:
                    cat = doc.Settings.Categories.get_Item(cid)
                    cat_name = canon_str(cat.Name) if cat else None
                    if cat_name:
                        cat_names.append(cat_name)
                except Exception as e:
                    pass

            # Prefer names when resolvable; otherwise fall back to ids deterministically
            cat_names_sorted = sorted(set([c for c in cat_names if c]))
            if cat_names_sorted:
                sig.append("categories={}".format(sig_val(",".join(cat_names_sorted))))
            else:
                # Contract: only negative ids allowed in hash surfaces
                neg_ids = []
                for cid in cat_ids:
                    try:
                        iv = getattr(cid, "IntegerValue", cid)
                        iv = int(iv)
                        if iv < 0:
                            neg_ids.append(str(iv))
                    except Exception as e:
                        pass

                neg_ids_sorted = sorted(set([x for x in neg_ids if x]))
                sig.append("categories_ids={}".format(sig_val(",".join(neg_ids_sorted) if neg_ids_sorted else S_MISSING)))
        except Exception as e:
            sig.append("categories={S_MISSING}")
            
        # v2 categories: negative category ids only (no names)
        if v2_ok:
            try:
                cat_ids_v2 = list(f.GetCategories())
                neg_ids = []
                for cid in cat_ids_v2:
                    iv = getattr(cid, "IntegerValue", cid)
                    iv = int(iv)
                    if iv < 0:
                        neg_ids.append(str(iv))
                    else:
                        # Positive ids are not allowed in semantic surfaces
                        v2_ok = False
                        v2_reason = "category_positive_id"
                        break
                if v2_ok:
                    neg_ids_sorted = sorted(set([x for x in neg_ids if x]))
                    if not neg_ids_sorted:
                        v2_ok = False
                        v2_reason = "category_none"
                    else:
                        sig_v2.append("categories_ids={}".format(sig_val(",".join(neg_ids_sorted))))
            except Exception:
                v2_ok = False
                v2_reason = "category_unreadable"

        # Filter rules (order-sensitive by traversal order; preserves AND/OR structure)
        try:
            elem_filter = f.GetElementFilter()
            if elem_filter is None:
                sig.append("filter_tree={S_MISSING}")
            else:
                tokens = []
                _walk_elem_filter(elem_filter, tokens)
                sig.append("filter_tree_count={}".format(len(tokens)))
                for i, t in enumerate(tokens):
                    idx = "{:03d}".format(i)
                    sig.append("ft[{}]={}".format(idx, sig_val(t)))
        except Exception as e:
            sig.append(f"filter_tree={S_UNREADABLE}")
            
        # v2 filter tree: strict walk, block on unreadable/unallowed
        if v2_ok:
            try:
                elem_filter_v2 = f.GetElementFilter()
                tokens_v2 = []
                ok, reason = _walk_elem_filter_v2(elem_filter_v2, tokens_v2, doc)
                if not ok:
                    v2_ok = False
                    v2_reason = reason
                else:
                    sig_v2.append("filter_tree_count={}".format(len(tokens_v2)))
                    for i, t in enumerate(tokens_v2):
                        idx = "{:03d}".format(i)
                        sig_v2.append("ft[{}]={}".format(idx, sig_val(t)))
            except Exception:
                v2_ok = False
                v2_reason = "filter_tree_unreadable"

        # Hash the definition (rules are NOT sorted - order matters)
        def_hash = make_hash(sig)
        def_hash_v2 = None
        if v2_ok:
            def_hash_v2 = make_hash(sig_v2)
            per_hashes_v2.append(def_hash_v2)
            if uid:
                uid_to_hash_v2[uid] = def_hash_v2
        else:
            info["debug_v2_blocked"] += 1
            if v2_reason:
                info["debug_v2_block_reasons"][v2_reason] = info["debug_v2_block_reasons"].get(v2_reason, 0) + 1

        rec = {
            "id": safe_str(f.Id.IntegerValue),
            "uid": uid or "",
            "name": name,
            "def_hash": def_hash,
            "def_signature": sig  # Include for explainability
        }

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

        # Populate context mapping
        if uid:
            uid_to_hash[uid] = def_hash

    # Populate context for downstream domains (views, templates)
    if ctx is not None:
        ctx["filter_uid_to_hash"] = uid_to_hash
        # NOTE: filter_uid_to_hash_v2 is populated after domain-level v2 outcome is known.

    info["names"] = sorted(set(names))
    info["count"] = len(info["names"])
    info["records"] = sorted(records, key=lambda r: (r.get("name",""), r.get("id","")))
    info["signature_hashes"] = sorted(per_hashes)
    info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else None
    info["signature_hashes_v2"] = sorted(per_hashes_v2)
    if info["debug_v2_blocked"] > 0:
        info["hash_v2"] = None
    else:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"]) if info["signature_hashes_v2"] else None
        
    # Only publish v2 mapping when the DOMAIN v2 hash is valid.
    # Contract: no partial v2 coverage; downstream must block if view_filters v2 is blocked.
    if ctx is not None:
        ctx["filter_uid_to_hash_v2"] = uid_to_hash_v2 if info["hash_v2"] is not None else {}

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("uid", "")),
            "sig_hash":   safe_str(r.get("def_hash", "")),
            "name":       safe_str(r.get("name", "")),
        } for r in recs]
    except Exception as e:
        info["record_rows"] = []

    return info
