# -*- coding: utf-8 -*-
"""
Browser Organization domain extractor.

Fingerprints the Project Browser's grouping/sorting configuration via a
single extractor, `extract_browser_organization()` -> domain=
"browser_organization": one record.v2 identity record per resolvable
`BrowserOrganization` element.

Per Step 0 (audit_results/audit_11_domain_extractor_delta_step0_findings.md
Sec. 4), Revit exposes THREE `BrowserOrganization` category entry points --
`BrowserOrganization.GetCurrentBrowserOrganizationForViews(doc)`,
`...ForSheets(doc)`, and `...ForSchedules(doc)` -- not just views/sheets.
All three that resolve non-null are collected, one record each, keyed by
which entry point produced them (`bo.category` in {"views", "sheets",
"schedules"}). `bo.category` is a synthesized loop discriminator (which
API call produced this org), not a Revit-read property, so it is always
`ITEM_Q_OK` whenever a record is built at all.

Fields (per tools/probes/probe_browser_organization.py, confirmed at Step 0):
- `bo.category` / `bo.sorting_order` / `bo.sorting_parameter_id` /
  `bo.filter_has_value`: semantic (drive `sig_hash`) -- these are the org's
  actual configured grouping/sorting/filtering behavior. `bo.filter_has_value`
  (`org.GetParameters("Filter")[0].HasValue`) is included so two otherwise-
  identical browser organizations that differ only in whether a browser
  filter is configured do not silently collapse to the same `sig_hash` --
  the probe confirmed this read succeeds for every probed organization (see
  audit_results/audit_11_domain_extractor_delta_step0_findings.md Sec. 4.3's
  `filter_param_has_value` correction), so it is captured even though the
  task's original field list omitted it.
- `bo.sorting_parameter_id` resolves to a human-readable parameter name via
  `dir()`/`getattr` `BuiltInParameter` introspection (same proven pattern as
  the probe's `bip_lookup` and `domains/worksets.py`'s `WorksetKind`
  discovery), not a hardcoded parameter-name list -- falls back to
  `doc.GetElement()` for positive (shared/project parameter) ids, and to the
  raw int as a string (fail-soft, matching `workset.kind`'s fallback
  pattern) only if neither resolves a name.
- `bo.family_name` (`refl.BrowserOrganization.FamilyName` in the probe,
  e.g. "Browser - Views"): cosmetic -- a Revit-internal display label, not
  behavior, excluded from `sig_hash` per the same naming-is-metadata rule
  every other domain follows.
- `bo.org_id` / `bo.unique_id`: unknown/traceability. `BrowserOrganization`
  derives from `Element` (unlike `Workset`), so `UniqueId` here is a real
  Element `UniqueId` string (no `Guid` coercion needed) -- still kept
  traceability-only rather than identity, since these ids are not expected
  to be stable/comparable across different documents.
- `bo.workset_id` / `bo.workset_name` / `bo.workset_unique_id`: coordination
  (cross-model, name-based resolution -- never in `sig_hash`, per the
  Phase-2 bucket rule for `coordination_items`). See `_resolve_workset_
  crosswalk()` below for the resolution design and why it needs one
  unavoidable `WorksetTable.GetWorkset()` call.

Fields deliberately dropped from the probe's inventory (see
`folder_items_walked_count` / `name_fallback_used_count` in the probe and in
audit_results/audit_11_domain_extractor_delta_step0_findings.md Sec. 4.3):
both are probe-only diagnostics of a capped, order-dependent `FolderItemInfo`
tree walk (bounded by the probe's own `max_items_per_level`/`max_tree_depth`
inputs) -- not a deterministic property of the `BrowserOrganization` element
itself, and therefore not eligible for a fingerprint that must be
"deterministic, stable across sessions, independent of element creation
order" (CLAUDE.md). Carrying a probe-run-parameter-dependent count into a
governed field would violate that invariant, so both are dropped rather than
replicated.

WorksetId crosswalk design (`_resolve_workset_crosswalk`):
`BrowserOrganization.WorksetId` is a distinct .NET type from `ElementId`
(both happen to expose `.IntegerValue`), and `Workset` does not derive from
`Element`, so `doc.GetElement()` can never resolve it -- only
`WorksetTable.GetWorkset()` can (see the probe's own `_resolve_workset`
docstring). That single, targeted id->name lookup is the minimal,
unavoidable translation of a foreign-key id into a name -- the same pattern
`domains/materials.py` already uses for its own `doc.GetElement()`-based
fill-pattern name resolution -- and is NOT a reimplementation of
`worksets.py`'s own discovery/classification/hashing logic (which sweeps
every `UserWorkset` and computes kind/editable/default/hash for each). The
resulting `bo.workset_unique_id` is deliberately NOT read off the live
`Workset` object directly (that would be an independently re-derived value,
which is what this area's task explicitly warns against); it is looked up
via `workset_name_to_unique_id`, a ctx map the runner builds from Area 3's
own already-computed `worksets` records (`runner/run_dynamo.py` -- worksets.py
itself is out of scope to modify for this area, and does not currently
export a ctx map of its own), so this field is a genuine cross-domain join
against Area 3's evidence rather than an independently re-derived value.

This is a GLOBAL domain family -- browser organization is defined once per
document (per category), like `worksets`/`worksets_doc`.

Per-record identity: `bo.category` (synthesized discriminator; `Browser
Organization` elements are not expected to be stable/comparable across
documents by `Id`/`UniqueId`, so those are traceability evidence only, same
rule D-004 applies to `Workset.UniqueId`).
Ordering: sorted by record_id (order-insensitive; at most 3 records, no
meaningful creation-order signal).
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    canonicalize_str,
    canonicalize_str_allow_empty,
    canonicalize_int,
    canonicalize_bool,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy
from core.phase2 import phase2_sorted_items

try:
    from Autodesk.Revit.DB import BrowserOrganization, BuiltInParameter, ElementId
except ImportError:
    BrowserOrganization = None
    BuiltInParameter = None
    ElementId = None


# Category discriminator -> BrowserOrganization static getter name. All 3 are
# real, distinct entry points per Step 0 (Sec. 4.3) -- schedules is not a
# probe artifact, it is `BrowserOrganization.GetCurrentBrowserOrganizationFor
# Schedules(doc)`, confirmed populated in PROBE_INVENTORY.csv alongside
# views/sheets.
_ORG_CATEGORIES = (
    ("views", "GetCurrentBrowserOrganizationForViews"),
    ("sheets", "GetCurrentBrowserOrganizationForSheets"),
    ("schedules", "GetCurrentBrowserOrganizationForSchedules"),
)

# Identity keys that drive sig_hash -- the org's actual configured
# grouping/sorting behavior. Deliberately excludes bo.family_name (cosmetic
# display label), bo.org_id/bo.unique_id (traceability, not expected to be
# stable/comparable across documents), and bo.workset_id/bo.workset_name/
# bo.workset_unique_id (coordination -- cross-model name-based resolution,
# per the Phase-2 bucket rule, never behavior).
BROWSER_ORGANIZATION_SEMANTIC_KEYS = (
    "bo.category",
    "bo.filter_has_value",
    "bo.sorting_order",
    "bo.sorting_parameter_id",
)

# bo.filter_has_value is deliberately NOT required: a read failure there
# (org.GetParameters("Filter") returning no "Filter" parameter, or the read
# itself throwing) still contributes a distinct, non-"ok" value to the
# sig_hash preimage above (never silently collapsed into the "no filter"
# state), but must not block the whole record over a single non-essential
# evidence field the way an unreadable bo.category/sorting_order/
# sorting_parameter_id would.
_BROWSER_ORGANIZATION_REQUIRED_KEYS = (
    "bo.category",
    "bo.sorting_order",
    "bo.sorting_parameter_id",
)


def _discover_bip_reverse_lookup():
    """Map negative BuiltInParameter int value -> member name.

    Same proven `dir()`/`getattr` introspection pattern as
    `tools/probes/probe_browser_organization.py`'s `bip_lookup` and
    `domains/worksets.py`'s `WorksetKind` discovery -- not a hardcoded
    parameter-name list, since `BuiltInParameter` has thousands of members
    and `SortingParameterId` can legitimately resolve to any of them.
    """
    lookup = {}
    if BuiltInParameter is None:
        return lookup
    try:
        names = dir(BuiltInParameter)
    except Exception:
        return lookup
    for n in names:
        if n.startswith("_"):
            continue
        try:
            attr = getattr(BuiltInParameter, n, None)
            if attr is None:
                continue
            iv = int(str(attr))
        except Exception:
            continue
        if iv < 0:
            lookup[iv] = n
    return lookup


def _resolve_sorting_parameter_name(doc, sp_int, bip_lookup):
    """Resolve a SortingParameterId int to a human-readable name.

    Negative values are BuiltInParameter ids (reverse-looked-up via
    bip_lookup); positive values are shared/project parameter element ids
    (resolved via doc.GetElement()). Returns None if neither resolves,
    letting the caller fall back to the raw int as a string.
    """
    if sp_int is None:
        return None
    if sp_int < 0:
        return bip_lookup.get(sp_int)
    if ElementId is None:
        return None
    try:
        elem = doc.GetElement(ElementId(sp_int))
    except Exception:
        elem = None
    if elem is None:
        return None
    try:
        name = elem.Name
    except Exception:
        return None
    return name if name else None


def _resolve_workset_crosswalk(doc, org, is_workshared, workset_name_to_unique_id):
    """Resolve BrowserOrganization.WorksetId to (id, name, unique_id) evidence.

    Returns three (v, q) pairs for bo.workset_id / bo.workset_name /
    bo.workset_unique_id. See the module docstring for the crosswalk design
    rationale (one unavoidable WorksetTable.GetWorkset() call for id->name,
    then a ctx-map join against Area 3's own computed records for the
    unique_id, rather than reading Workset.UniqueId off the live object).
    """
    try:
        ws_id_obj = org.WorksetId
    except Exception:
        unreadable = (None, ITEM_Q_UNREADABLE)
        return unreadable, unreadable, unreadable

    if ws_id_obj is None:
        missing = (None, ITEM_Q_MISSING)
        return missing, missing, missing

    try:
        ws_id_int = ws_id_obj.IntegerValue
    except Exception:
        ws_id_int = None

    if is_workshared is False:
        # Non-workshared document -- worksets don't meaningfully apply. A
        # negative ws_id_int here is the "Invalid"/unassigned sentinel, not
        # a real id -- keep it not_applicable too rather than reporting a
        # confirmed numeric "ok" value that doesn't actually mean anything.
        na = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
        if ws_id_int is not None and ws_id_int >= 0:
            id_pair = canonicalize_int(ws_id_int)
        else:
            id_pair = na
        return id_pair, na, na

    if ws_id_int is None or ws_id_int < 0:
        # Workshared document but no valid workset assignment resolved --
        # unexpected; keep as unreadable rather than silently treating a
        # confirmed non-applicable state the same as a genuine read failure.
        unreadable = (None, ITEM_Q_UNREADABLE)
        return unreadable, unreadable, unreadable

    id_pair = canonicalize_int(ws_id_int)

    ws = None
    try:
        wt_table = doc.GetWorksetTable()
        ws = wt_table.GetWorkset(ws_id_obj)
        ws_name = ws.Name if ws is not None else None
    except Exception:
        ws_name = None

    name_pair = canonicalize_str(ws_name)
    if name_pair[1] != ITEM_Q_OK:
        return id_pair, name_pair, (None, ITEM_Q_UNREADABLE)

    mapped_uid = (workset_name_to_unique_id or {}).get(ws_name)
    if mapped_uid:
        uid_pair = canonicalize_str(mapped_uid)
    else:
        # Not every WorksetId a BrowserOrganization can reference resolves
        # to a WorksetKind.UserWorkset -- worksets.py's "worksets" domain
        # (Area 3) is deliberately scoped to UserWorkset only (see its
        # module docstring), so a BrowserOrganization pinned to a
        # StandardWorkset (e.g. Revit's own system "Views, Browser
        # Organization"-style workset) will never appear in
        # workset_name_to_unique_id at all -- there is no Area 3 record to
        # join against for that case, not a join failure. Fall back to the
        # same live Workset object already fetched above (Workset.UniqueId
        # is a System.Guid, not a System.String, unlike Element.UniqueId --
        # must be safe_str()-coerced, same as domains/worksets.py's own
        # workset.unique_id handling) rather than reporting a spurious
        # "unreadable" for a legitimately out-of-Area-3-scope workset kind.
        try:
            uid_pair = canonicalize_str(safe_str(ws.UniqueId)) if ws is not None else (None, ITEM_Q_UNREADABLE)
        except Exception:
            uid_pair = (None, ITEM_Q_UNREADABLE)

    return id_pair, name_pair, uid_pair


def _build_record(category, org, doc, is_workshared, bip_lookup, workset_name_to_unique_id, ctx):
    category_v, category_q = canonicalize_str(category)

    try:
        so_raw = org.SortingOrder
        so_int = int(str(so_raw))
    except Exception:
        so_int = None
    so_v, so_q = canonicalize_int(so_int)

    try:
        sp = org.SortingParameterId
        sp_int = sp.IntegerValue if sp is not None else None
    except Exception:
        sp_int = None

    if sp_int is not None:
        sp_name = _resolve_sorting_parameter_name(doc, sp_int, bip_lookup)
        if sp_name:
            sp_v, sp_q = canonicalize_str(sp_name)
        else:
            # Fail-soft fallback: keep the raw int rather than dropping the
            # observation, matching workset.kind's fallback pattern in
            # domains/worksets.py.
            sp_v, sp_q = canonicalize_str(str(sp_int))
    else:
        sp_v, sp_q = (None, ITEM_Q_UNREADABLE)

    try:
        fam_raw = getattr(org, "FamilyName", None)
    except Exception:
        fam_raw = None
    fam_v, fam_q = canonicalize_str_allow_empty(fam_raw)

    try:
        filter_params = list(org.GetParameters("Filter") or [])
    except Exception:
        filter_params = None
    if filter_params is None:
        filter_v, filter_q = (None, ITEM_Q_UNREADABLE)
    elif not filter_params:
        # No "Filter" parameter found on this org -- confirmed present for
        # every probed organization at Step 0, so treat an absent parameter
        # as an unexpected read gap rather than a legitimate not-applicable
        # state (fail-soft: don't collapse "couldn't read it" into "N/A").
        filter_v, filter_q = (None, ITEM_Q_UNREADABLE)
    else:
        try:
            has_value = bool(filter_params[0].HasValue)
            filter_v, filter_q = canonicalize_bool(has_value)
        except Exception:
            filter_v, filter_q = (None, ITEM_Q_UNREADABLE)

    try:
        oid_int = org.Id.IntegerValue
    except Exception:
        oid_int = None
    oid_v, oid_q = canonicalize_int(oid_int)

    try:
        uid_raw = org.UniqueId
    except Exception:
        uid_raw = None
    uid_v, uid_q = canonicalize_str(uid_raw)

    ws_id_pair, ws_name_pair, ws_uid_pair = _resolve_workset_crosswalk(
        doc, org, is_workshared, workset_name_to_unique_id
    )

    identity_items = [
        make_identity_item("bo.category", category_v, category_q),
        make_identity_item("bo.sorting_order", so_v, so_q),
        make_identity_item("bo.sorting_parameter_id", sp_v, sp_q),
        make_identity_item("bo.filter_has_value", filter_v, filter_q),
        make_identity_item("bo.family_name", fam_v, fam_q),
        make_identity_item("bo.org_id", oid_v, oid_q),
        make_identity_item("bo.unique_id", uid_v, uid_q),
        make_identity_item("bo.workset_id", ws_id_pair[0], ws_id_pair[1]),
        make_identity_item("bo.workset_name", ws_name_pair[0], ws_name_pair[1]),
        make_identity_item("bo.workset_unique_id", ws_uid_pair[0], ws_uid_pair[1]),
    ]
    identity_items_sorted = sorted(identity_items, key=lambda it: it.get("k", ""))
    item_by_k = {it["k"]: it for it in identity_items_sorted}

    required_qs = [item_by_k[k]["q"] for k in _BROWSER_ORGANIZATION_REQUIRED_KEYS]
    blocked = any(q != ITEM_Q_OK for q in required_qs)

    status_reasons = []
    any_incomplete = False
    for it in identity_items_sorted:
        if it.get("q") != ITEM_Q_OK:
            any_incomplete = True
            status_reasons.append("identity.incomplete:{}:{}".format(it.get("q"), it.get("k")))

    status = STATUS_BLOCKED if blocked else (STATUS_DEGRADED if any_incomplete else STATUS_OK)

    record_id = "browser_organization:{}".format(category_v) if category_v else "browser_organization:unknown"

    sig_hash = None
    if not blocked:
        semantic_items = [it for it in identity_items_sorted if it.get("k") in BROWSER_ORGANIZATION_SEMANTIC_KEYS]
        sig_hash = make_hash(serialize_identity_items(semantic_items))

    label_quality = "placeholder_unreadable" if blocked else "system"
    display_label = safe_str(fam_v) if fam_v else "Browser Organization ({})".format(category_v or "unknown")

    rec = build_record_v2(
        domain="browser_organization",
        record_id=record_id,
        status=status,
        status_reasons=sorted(set(status_reasons)),
        sig_hash=sig_hash,
        identity_items=identity_items_sorted,
        required_qs=required_qs,
        label={
            "display": display_label,
            "quality": label_quality,
            "provenance": "revit.BrowserOrganization",
            "components": {"category": safe_str(category_v) if category_v else ""},
        },
    )
    rec["is_purgeable"] = None
    rec["is_purgeable_q"] = "unsupported_not_applicable"

    pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "browser_organization")
    rec["join_key"], _missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=identity_items_sorted,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )

    cosmetic_keys = {"bo.family_name"}
    coordination_keys = {"bo.workset_id", "bo.workset_name", "bo.workset_unique_id"}
    unknown_keys = {"bo.org_id", "bo.unique_id"}
    rec["phase2"] = {
        "schema": "phase2.browser_organization.v1",
        "grouping_basis": "phase2.hypothesis",
        "cosmetic_items": phase2_sorted_items(
            [dict(it) for it in identity_items_sorted if it.get("k") in cosmetic_keys]
        ),
        "coordination_items": phase2_sorted_items(
            [dict(it) for it in identity_items_sorted if it.get("k") in coordination_keys]
        ),
        "unknown_items": phase2_sorted_items(
            [dict(it) for it in identity_items_sorted if it.get("k") in unknown_keys]
        ),
    }
    rec["sig_basis"] = {
        "schema": "browser_organization.sig_basis.v1",
        "keys_used": list(BROWSER_ORGANIZATION_SEMANTIC_KEYS),
    }

    return rec


def extract_browser_organization(doc, ctx=None):
    """
    Extract per-BrowserOrganization identity records (domain=
    "browser_organization").

    Args:
        doc: Revit Document
        ctx: Context dictionary. Reads ctx["workset_name_to_unique_id"] (built
            by runner/run_dynamo.py from Area 3's own worksets records -- see
            the module docstring's WorksetId crosswalk section) for the
            bo.workset_unique_id join. Absent/empty is handled fail-soft
            (bo.workset_unique_id degrades to unreadable, non-blocking).

    Returns:
        Dictionary with count, hash_v2, records, record_rows.
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "records": [],
        "record_rows": [],

        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    if BrowserOrganization is None:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"BrowserOrganization_unavailable": True}
        return info

    bip_lookup = _discover_bip_reverse_lookup()
    workset_name_to_unique_id = (ctx or {}).get("workset_name_to_unique_id") or {}

    try:
        is_workshared = bool(getattr(doc, "IsWorkshared", False))
    except Exception:
        is_workshared = None

    orgs = []
    read_errors = []
    for category, method_name in _ORG_CATEGORIES:
        try:
            getter = getattr(BrowserOrganization, method_name, None)
            org = getter(doc) if getter is not None else None
        except Exception:
            org = None
            read_errors.append(category)
        if org is not None:
            orgs.append((category, org))

    info["raw_count"] = len(orgs)

    v2_records = []
    v2_sig_hashes = []
    v2_block_reasons = {}

    for category, org in orgs:
        rec = _build_record(category, org, doc, is_workshared, bip_lookup, workset_name_to_unique_id, ctx)
        v2_records.append(rec)
        if rec.get("sig_hash"):
            v2_sig_hashes.append(rec["sig_hash"])
        else:
            v2_block_reasons["record_blocked:{}".format(rec.get("record_id"))] = True

    info["records"] = sorted(v2_records, key=lambda r: safe_str(r.get("record_id", "")))
    info["count"] = len(v2_records)
    info["record_rows"] = [
        {
            "record_key": safe_str(r.get("record_id", "")),
            "sig_hash": r.get("sig_hash", None),
            "name": safe_str((r.get("label", {}) or {}).get("display", "")),
        }
        for r in info["records"]
    ]

    if read_errors or v2_block_reasons:
        # A category read failure or an individually-blocked record must
        # never be silently absorbed into a partial-success aggregate hash
        # just because other categories/records happened to succeed -- that
        # would make an incomplete extraction indistinguishable from a
        # complete one. Block the aggregate whenever either occurs, even if
        # v2_sig_hashes is non-empty.
        info["hash_v2"] = None
        info["debug_v2_blocked"] = True
        reasons = dict(v2_block_reasons)
        for cat in read_errors:
            reasons["category_unreadable:{}".format(cat)] = True
        info["debug_v2_block_reasons"] = reasons
    elif v2_sig_hashes:
        info["hash_v2"] = make_hash(sorted(v2_sig_hashes))
        info["debug_v2_blocked"] = False
        info["debug_v2_block_reasons"] = {}
    else:
        # Zero records, zero read errors -- legitimately empty population
        # (e.g. a family document, where the Project Browser concept
        # BrowserOrganization models does not apply) -- not a failure.
        info["hash_v2"] = None
        info["debug_v2_blocked"] = False
        info["debug_v2_block_reasons"] = {}

    return info
