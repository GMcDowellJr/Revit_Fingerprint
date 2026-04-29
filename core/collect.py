# -*- coding: utf-8 -*-
"""
core/collect.py

Shared collection helpers for domains.

Goals (PR5):
- Centralize include/exclude rules (no domain-local drift)
- Provide per-run caching keyed by query intent
- Emit explicit counters for observability

Non-goals:
- Changing contract envelope semantics (runner merges counters)
- Swallowing collector failures (errors should surface to caller)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

try:
    from Autodesk.Revit.DB import FilteredElementCollector, ElementId, BuiltInParameter
except Exception:
    FilteredElementCollector = None
    ElementId = None
    BuiltInParameter = None


CacheKey = Union[str, Tuple[Any, ...]]


@dataclass
class CollectCtx:
    """
    Per-run collection context.

    - collector_cache maps semantic query keys to a list of ElementId integer values.
    - counters are stable names used for PR5 acceptance verification.
    - timing holds an optional TimingCollector reference for API call instrumentation.
    """
    collector_cache: Dict[CacheKey, List[int]] = field(default_factory=dict)
    counters: Dict[str, int] = field(default_factory=dict)
    timing: Any = None  # Optional TimingCollector reference

    def inc(self, key: str, n: int = 1) -> None:
        try:
            self.counters[str(key)] = int(self.counters.get(str(key), 0)) + int(n)
        except Exception:
            # If counters are corrupted, fail loudly: caller should not assume observability.
            raise


def _is_invalid_element_id(elem_id: Any) -> bool:
    """
    Returns True if elem_id is missing/invalid.

    Handles:
    - None
    - Revit ElementId.InvalidElementId (when available)
    - ElementId with IntegerValue < 0 (defensive)
    """
    if elem_id is None:
        return True

    try:
        if ElementId is not None and hasattr(ElementId, "InvalidElementId"):
            if elem_id == ElementId.InvalidElementId:
                return True
    except Exception:
        # Do not assume valid if we cannot evaluate.
        return True

    try:
        iv = getattr(elem_id, "IntegerValue", None)
        if iv is None:
            return True
        if int(iv) < 0:
            return True
    except Exception:
        return True

    return False


def _safe_unique_id(elem: Any) -> Optional[str]:
    try:
        uid = getattr(elem, "UniqueId", None)
        if uid is None:
            return None
        uid = str(uid)
        return uid if uid.strip() else None
    except Exception:
        return None


def _make_query_key(
    *,
    kind: str,
    of_class: Any,
    of_category: Any,
    where_key: Optional[CacheKey],
    require_unique_id: bool,
) -> Tuple[Any, ...]:
    """
    Cache key must be based on semantic query intent, not object reprs that can drift.
    """
    cls_name = None
    try:
        cls_name = getattr(of_class, "__name__", None) if of_class is not None else None
    except Exception:
        cls_name = None

    cat_int = None
    try:
        # BuiltInCategory is an enum in Revit; int(...) should be stable.
        if of_category is not None:
            cat_int = int(of_category)
    except Exception:
        # If category is not int-able, treat as None (caller should pass where_key if needed).
        cat_int = None

    return (str(kind), cls_name, cat_int, bool(require_unique_id), where_key)


def _require_revit_api() -> None:
    if FilteredElementCollector is None:
        raise RuntimeError("Revit API not reachable: FilteredElementCollector import failed")


def _collect_id_ints_uncached(
    doc: Any,
    *,
    kind: str,
    of_class: Any = None,
    of_category: Any = None,
    where: Optional[Callable[[Any], bool]] = None,
    require_unique_id: bool = False,
    cctx: Optional[CollectCtx] = None,
) -> List[int]:
    """
    Execute a collection query without caching and return ElementId.IntegerValue list.
    """
    _require_revit_api()
    if doc is None:
        raise ValueError("doc is None")

    if cctx is not None:
        cctx.inc("collect.calls_total", 1)

    # Timing: instrument FilteredElementCollector creation + execution
    _tc = getattr(cctx, "timing", None) if cctx is not None else None
    if _tc is not None:
        try:
            _tc.start_timer("api:filter_elements")
        except Exception:
            pass

    fec = FilteredElementCollector(doc)

    if of_class is not None:
        fec = fec.OfClass(of_class)
    if of_category is not None:
        fec = fec.OfCategory(of_category)

    if kind == "types":
        fec = fec.WhereElementIsElementType()
    elif kind == "instances":
        fec = fec.WhereElementIsNotElementType()
    else:
        if _tc is not None:
            try:
                _tc.end_timer("api:filter_elements")
            except Exception:
                pass
        raise ValueError("Unknown collect kind: {}".format(kind))

    # ToElements() exists; iterating fec also works. Keep it explicit.
    elems = list(fec.ToElements())

    if _tc is not None:
        try:
            _tc.end_timer("api:filter_elements")
        except Exception:
            pass

    out: List[int] = []
    for e in elems:
        if e is None:
            if cctx is not None:
                cctx.inc("collect.excluded.null_element", 1)
            continue

        try:
            eid = getattr(e, "Id", None)
        except Exception:
            eid = None

        if _is_invalid_element_id(eid):
            if cctx is not None:
                cctx.inc("collect.excluded.invalid_id", 1)
            continue

        if require_unique_id:
            uid = _safe_unique_id(e)
            if uid is None:
                if cctx is not None:
                    cctx.inc("collect.excluded.missing_unique_id", 1)
                continue

        if where is not None:
            ok = False
            try:
                ok = bool(where(e))
            except Exception:
                # Predicate failure is treated as exclusion (explicit), not silent success.
                ok = False
                if cctx is not None:
                    cctx.inc("collect.excluded.where_exception", 1)
            if not ok:
                if cctx is not None:
                    cctx.inc("collect.excluded.where_false", 1)
                continue

        try:
            out.append(int(eid.IntegerValue))
        except Exception:
            if cctx is not None:
                cctx.inc("collect.excluded.invalid_id", 1)
            continue

    if cctx is not None:
        cctx.inc("collect.returned_ids_total", len(out))

    return out


def collect_id_ints(
    doc: Any,
    *,
    kind: str,
    of_class: Any = None,
    of_category: Any = None,
    where: Optional[Callable[[Any], bool]] = None,
    where_key: Optional[CacheKey] = None,
    require_unique_id: bool = False,
    cctx: Optional[CollectCtx] = None,
    cache_key: Optional[CacheKey] = None,
) -> List[int]:
    """
    Cached collector entry point returning ElementId.IntegerValue list.

    Caching rules:
    - If cctx is None -> no caching
    - If where is provided, caller SHOULD provide where_key or cache_key; otherwise cache is bypassed.
    - cache_key overrides all computed key parts.
    """
    if cctx is None:
        return _collect_id_ints_uncached(
            doc,
            kind=kind,
            of_class=of_class,
            of_category=of_category,
            where=where,
            require_unique_id=require_unique_id,
            cctx=None,
        )

    # Choose key
    if cache_key is not None:
        key = cache_key
    else:
        if where is not None and where_key is None:
            # Cannot safely cache unkeyed predicates.
            cctx.inc("collect.cache_bypass.unkeyed_predicate", 1)
            return _collect_id_ints_uncached(
                doc,
                kind=kind,
                of_class=of_class,
                of_category=of_category,
                where=where,
                require_unique_id=require_unique_id,
                cctx=cctx,
            )
        key = _make_query_key(
            kind=kind,
            of_class=of_class,
            of_category=of_category,
            where_key=where_key,
            require_unique_id=require_unique_id,
        )

    # Cache lookup
    if key in cctx.collector_cache:
        cctx.inc("collect.cache_hit", 1)
        return list(cctx.collector_cache[key])

    cctx.inc("collect.cache_miss", 1)
    ids = _collect_id_ints_uncached(
        doc,
        kind=kind,
        of_class=of_class,
        of_category=of_category,
        where=where,
        require_unique_id=require_unique_id,
        cctx=cctx,
    )
    cctx.collector_cache[key] = list(ids)
    return ids


def _get_element(doc: Any, id_int: int) -> Any:
    if doc is None:
        raise ValueError("doc is None")
    try:
        if ElementId is not None:
            return doc.GetElement(ElementId(int(id_int)))
    except Exception:
        pass
    # Fall back: some hosts accept int directly (defensive).
    return doc.GetElement(int(id_int))


def collect_elements(
    doc: Any,
    *,
    kind: str,
    of_class: Any = None,
    of_category: Any = None,
    where: Optional[Callable[[Any], bool]] = None,
    where_key: Optional[CacheKey] = None,
    require_unique_id: bool = False,
    cctx: Optional[CollectCtx] = None,
    cache_key: Optional[CacheKey] = None,
) -> List[Any]:
    """
    Cached collector returning live elements (resolved from cached id_int list).
    """
    ids = collect_id_ints(
        doc,
        kind=kind,
        of_class=of_class,
        of_category=of_category,
        where=where,
        where_key=where_key,
        require_unique_id=require_unique_id,
        cctx=cctx,
        cache_key=cache_key,
    )

    out: List[Any] = []
    for id_int in ids:
        e = _get_element(doc, id_int)
        if e is None:
            if cctx is not None:
                cctx.inc("collect.excluded.null_element", 1)
            continue
        out.append(e)
    return out


def collect_types(
    doc: Any,
    *,
    of_class: Any = None,
    of_category: Any = None,
    where: Optional[Callable[[Any], bool]] = None,
    where_key: Optional[CacheKey] = None,
    require_unique_id: bool = False,
    cctx: Optional[CollectCtx] = None,
    cache_key: Optional[CacheKey] = None,
) -> List[Any]:
    return collect_elements(
        doc,
        kind="types",
        of_class=of_class,
        of_category=of_category,
        where=where,
        where_key=where_key,
        require_unique_id=require_unique_id,
        cctx=cctx,
        cache_key=cache_key,
    )


def collect_instances(
    doc: Any,
    *,
    of_class: Any = None,
    of_category: Any = None,
    where: Optional[Callable[[Any], bool]] = None,
    where_key: Optional[CacheKey] = None,
    require_unique_id: bool = False,
    cctx: Optional[CollectCtx] = None,
    cache_key: Optional[CacheKey] = None,
) -> List[Any]:
    return collect_elements(
        doc,
        kind="instances",
        of_class=of_class,
        of_category=of_category,
        where=where,
        where_key=where_key,
        require_unique_id=require_unique_id,
        cctx=cctx,
        cache_key=cache_key,
    )




def build_purgeable_id_set(doc: Any, ctx: Optional[dict] = None):
    """
    Builds a frozenset of ElementId.IntegerValue (int) for all elements
    currently listed as purgeable by Document.GetUnusedElements().

    Requires Revit 2024+ (API Since: 2024).
    For pre-2024, the equivalent approach is:
      - Get PerformanceAdviser.GetPerformanceAdviser()
      - Find the rule with GUID 'e8c63650-70b7-435a-9010-ec97660c1bda'
      - Call ExecuteRules(doc, [ruleId])
      - Call failureMessages[0].GetFailingElements() to get purgeable IDs

    Caches results to ctx:
      ctx["_purgeable_id_set"]   -> frozenset[int] | None
      ctx["_purgeable_id_set_q"] -> "ok" | "unreadable"

    Returns (frozenset_or_none, q_string).
    """
    _CACHE_KEY = "_purgeable_id_set"
    _CACHE_Q_KEY = "_purgeable_id_set_q"

    if ctx is not None and _CACHE_KEY in ctx:
        return ctx[_CACHE_KEY], ctx.get(_CACHE_Q_KEY, "unreadable")

    try:
        if doc is None:
            raise ValueError("doc is None")

        try:
            import System.Collections.Generic as _scg
            category_set = _scg.HashSet[ElementId]()
        except Exception:
            category_set = set()
        unused = doc.GetUnusedElements(category_set)

        id_set = frozenset(
            int(eid.IntegerValue)
            for eid in unused
            if eid is not None and not _is_invalid_element_id(eid)
        )

        if ctx is not None:
            ctx[_CACHE_KEY] = id_set
            ctx[_CACHE_Q_KEY] = "ok"
        return id_set, "ok"

    except Exception:
        if ctx is not None:
            ctx[_CACHE_KEY] = None
            ctx[_CACHE_Q_KEY] = "unreadable"
        return None, "unreadable"


def purge_lookup(element_id_int: Any, ctx: Optional[dict]):
    purgeable_set = (ctx or {}).get("_purgeable_id_set")
    purgeable_q = (ctx or {}).get("_purgeable_id_set_q", "unreadable")
    if purgeable_q == "unreadable" or purgeable_set is None:
        return None, "unreadable"
    if element_id_int is None:
        return None, "unreadable"
    try:
        return (int(element_id_int) in purgeable_set), "ok"
    except Exception:
        return None, "unreadable"


def build_subcategory_used_id_set(doc: Any, parent_cat_obj: Any, ctx: Optional[dict] = None):
    """Build/cache used subcategory ids for a given parent category."""
    try:
        parent_id_int = int(getattr(getattr(parent_cat_obj, "Id", None), "IntegerValue", None))
    except Exception:
        return None

    cache_key = "_obj_style_used_subcats:{}".format(parent_id_int)
    if ctx is not None and cache_key in ctx:
        return ctx[cache_key]

    try:
        try:
            parent_cat_eid = parent_cat_obj.Id
            if parent_cat_eid is None:
                if ctx is not None:
                    ctx[cache_key] = None
                return None
        except Exception:
            if ctx is not None:
                ctx[cache_key] = None
            return None

        try:
            pre_check = (
                FilteredElementCollector(doc)
                .OfCategoryId(parent_cat_eid)
                .WhereElementIsNotElementType()
                .GetElementCount()
            )
            if pre_check == 0:
                if ctx is not None:
                    ctx[cache_key] = frozenset()
                return frozenset()
        except Exception:
            pass

        used = set()
        try:
            _subcat_param = BuiltInParameter.ELEM_SUBCATEGORY_PARAM
        except Exception:
            _subcat_param = None
        try:
            instances = (
                FilteredElementCollector(doc)
                .OfCategoryId(parent_cat_eid)
                .WhereElementIsNotElementType()
                .ToElements()
            )
            for inst in instances:
                try:
                    p = inst.get_Parameter(_subcat_param) if _subcat_param is not None else None
                    if p is not None and p.HasValue:
                        val = p.AsElementId()
                        if val is not None:
                            iv = int(val.IntegerValue)
                            if iv > 0:
                                used.add(iv)
                except Exception:
                    continue
        except Exception:
            if ctx is not None:
                ctx[cache_key] = None
            return None

        result = frozenset(used)
        if ctx is not None:
            ctx[cache_key] = result
        return result
    except Exception:
        if ctx is not None:
            ctx[cache_key] = None
        return None

def is_type_purgeable(
    doc: Any,
    type_id: Any,
    bic: Any,
    *,
    cctx: Optional[CollectCtx] = None,
    cache_key: Optional[CacheKey] = None,
) -> Optional[bool]:
    """
    DEPRECATED. Use build_purgeable_id_set() + a set lookup instead.

    is_type_purgeable() performs a per-record FilteredElementCollector scan,
    which is O(instances) per call. build_purgeable_id_set() amortizes the
    cost to a single Document.GetUnusedElements() call per run, with O(1)
    per-record lookups thereafter.

    This function is retained for reference only. It will be removed once all
    call sites have been migrated to the new pattern.
    """
    try:
        _require_revit_api()
        if doc is None or type_id is None or bic is None:
            return None

        categories = bic if isinstance(bic, (list, tuple, set, frozenset)) else (bic,)
        for cat in categories:
            try:
                fec = FilteredElementCollector(doc).OfCategory(cat).WhereElementIsNotElementType()
            except Exception:
                return None

            for elem in fec:
                try:
                    getter = getattr(elem, "GetTypeId", None)
                    if getter is None:
                        continue
                    if getter() == type_id:
                        return False
                except Exception:
                    continue

        return True
    except Exception:
        return None
