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

import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
core_dir = os.path.join(parent_dir, 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

from core.hashing import make_hash, safe_str
from core.canon import canon_str, sig_val

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
        return "rule=<None>"

    parts = []
    try:
        parts.append("type={}".format(rule.GetType().FullName))
    except Exception:
        parts.append("type=<Unknown>")

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
            parts.append("param=<None>")
        elif pid_int < 0:
            parts.append("param_id={}".format(safe_str(pid_int)))
        else:
            parts.append("param=<PositiveId>")
    except Exception:
        parts.append("param=<Unreadable>")

    # Try to capture evaluator/operator identity + value
    # String rules
    try:
        if FilterStringRule is not None and isinstance(rule, FilterStringRule):
            try:
                ev = rule.GetEvaluator() if hasattr(rule, "GetEvaluator") else None
                parts.append("op={}".format(ev.GetType().FullName if ev else "<None>"))
            except Exception:
                parts.append("op=<Unreadable>")
            try:
                parts.append("val={}".format(sig_val(canon_str(getattr(rule, "RuleString", None)))))
            except Exception:
                parts.append("val=<Unreadable>")
            return "|".join(parts)
    except Exception:
        pass

    # Integer rules
    try:
        if FilterIntegerRule is not None and isinstance(rule, FilterIntegerRule):
            try:
                ev = rule.GetEvaluator() if hasattr(rule, "GetEvaluator") else None
                parts.append("op={}".format(ev.GetType().FullName if ev else "<None>"))
            except Exception:
                parts.append("op=<Unreadable>")
            try:
                parts.append("val={}".format(sig_val(safe_str(getattr(rule, "RuleValue", None)))))
            except Exception:
                parts.append("val=<Unreadable>")
            return "|".join(parts)
    except Exception:
        pass

    # Double rules
    try:
        if FilterDoubleRule is not None and isinstance(rule, FilterDoubleRule):
            try:
                ev = rule.GetEvaluator() if hasattr(rule, "GetEvaluator") else None
                parts.append("op={}".format(ev.GetType().FullName if ev else "<None>"))
            except Exception:
                parts.append("op=<Unreadable>")
            try:
                parts.append("val={}".format(sig_val(safe_str(getattr(rule, "RuleValue", None)))))
            except Exception:
                parts.append("val=<Unreadable>")
            return "|".join(parts)
    except Exception:
        pass

    # ElementId rules
    try:
        if FilterElementIdRule is not None and isinstance(rule, FilterElementIdRule):
            try:
                ev = rule.GetEvaluator() if hasattr(rule, "GetEvaluator") else None
                parts.append("op={}".format(ev.GetType().FullName if ev else "<None>"))
            except Exception:
                parts.append("op=<Unreadable>")
            try:
                v = getattr(rule, "RuleValue", None)
                v_int = getattr(v, "IntegerValue", v)
                try:
                    v_int = int(v_int) if v_int is not None else None
                except Exception:
                    v_int = None

                if v_int is None:
                    parts.append("val=<None>")
                elif v_int < 0:
                    parts.append("val_id={}".format(safe_str(v_int)))
                else:
                    parts.append("val=<PositiveId>")
            except Exception:
                parts.append("val=<Unreadable>")
    except Exception:
        pass

    # Fallback: best-effort string, but still include type + param_id above
    try:
        parts.append("raw={}".format(sig_val(safe_str(rule))))
    except Exception:
        parts.append("raw=<Unreadable>")
    return "|".join(parts)


def _walk_elem_filter(elem_filter, out_tokens):
    """
    Walk ElementFilter trees (AND/OR) and collect rule tokens in a stable order.
    Rules are appended in traversal order.
    """
    if elem_filter is None:
        out_tokens.append("filter=<None>")
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
        out_tokens.append("leaf=<Unknown>")

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
    }

    try:
        col = list(FilteredElementCollector(doc).OfClass(ParameterFilterElement))
    except:
        return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []
    uid_to_hash = {}  # For context population

    for f in col:
        # Name is metadata only
        name = canon_str(getattr(f, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            name = "<unnamed>"
        names.append(name)

        uid = None
        try:
            uid = canon_str(getattr(f, "UniqueId", None))
        except:
            uid = None

        # Build filter signature
        sig = []

        # Filter type (rule-based vs selection-based)
        try:
            # Check if this is a selection filter
            is_selection = hasattr(f, "GetElementFilter") and f.GetElementFilter() is None
            sig.append("is_selection={}".format(sig_val(is_selection)))
        except:
            sig.append("is_selection=<None>")

        # Categories the filter applies to
        try:
            cat_ids = list(f.GetCategories())
            cat_names = []
            cat_ints = []
            for cid in cat_ids:
                try:
                    cat_ints.append(safe_str(getattr(cid, "IntegerValue", cid)))
                except:
                    pass
                try:
                    cat = doc.Settings.Categories.get_Item(cid)
                    cat_name = canon_str(cat.Name) if cat else None
                    if cat_name:
                        cat_names.append(cat_name)
                except:
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
                    except:
                        pass

                neg_ids_sorted = sorted(set([x for x in neg_ids if x]))
                sig.append("categories_ids={}".format(sig_val(",".join(neg_ids_sorted) if neg_ids_sorted else "<None>")))
        except:
            sig.append("categories=<None>")

        # Filter rules (order-sensitive by traversal order; preserves AND/OR structure)
        try:
            elem_filter = f.GetElementFilter()
            if elem_filter is None:
                sig.append("filter_tree=<None>")
            else:
                tokens = []
                _walk_elem_filter(elem_filter, tokens)
                sig.append("filter_tree_count={}".format(len(tokens)))
                for i, t in enumerate(tokens):
                    idx = "{:03d}".format(i)
                    sig.append("ft[{}]={}".format(idx, sig_val(t)))
        except:
            sig.append("filter_tree=<Unreadable>")

        # Hash the definition (rules are NOT sorted - order matters)
        def_hash = make_hash(sig)

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

    info["names"] = sorted(set(names))
    info["count"] = len(info["names"])
    info["records"] = sorted(records, key=lambda r: (r.get("name",""), r.get("id","")))
    info["signature_hashes"] = sorted(per_hashes)
    info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else None

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("uid", "")),
            "sig_hash":   safe_str(r.get("def_hash", "")),
            "name":       safe_str(r.get("name", "")),
        } for r in recs]
    except:
        info["record_rows"] = []

    return info
