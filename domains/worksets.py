# -*- coding: utf-8 -*-
"""
Worksets domain extractor.

Fingerprints the workset partition of a workshared document:

- One identity record per user-facing `Workset`
  (`FilteredWorksetCollector(doc).OfKind(WorksetKind.UserWorkset)`).
  `workset.name` / `workset.kind` / `workset.is_editable` /
  `workset.is_default_workset` are the deterministic, session-independent
  properties that drive `sig_hash`. `workset.owner` / `workset.is_active_workset`
  are live editing-session state (who currently has the workset checked out,
  which workset the current session happens to have active) and
  `workset.unique_id` is non-Element-backed identity (D-004 restricts
  `UniqueId` to element-backed entities; `Workset` is not one) -- all three
  are captured as identity evidence (required, block-if-unreadable) so a
  genuine API read failure still surfaces, but they are deliberately excluded
  from `sig_basis.keys_used` so the hash stays "stable across sessions" per
  CLAUDE.md's hash-semantics rules.
- One synthetic document-level record (`domain="worksets_doc"`,
  `record_id="worksets:_doc"`) summarizing `doc.IsWorkshared`, the active
  workset's name, and a population count per `WorksetKind` (all kinds, not
  just `UserWorkset`). Document-level fields are optional (never block the
  record) so a doc-level read hiccup can never take down the per-workset
  records -- this is why doc-level facts are emitted as a *separate* domain
  ("worksets_doc") rather than folded into the "worksets" per-record schema:
  record.v2's `required_keys` are enforced uniformly across every record of
  a domain, and the two record shapes have no required keys in common.

This is a GLOBAL domain -- worksets are defined once per document.

Per-record identity: `workset.name` (Workset is not Element-backed, so
UniqueId is traceability evidence only, per D-004 -- never the record key).
Ordering: sorted by record_id (order-insensitive; worksets carry no
meaningful creation-order signal analogous to phase sequence numbers).
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
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    canonicalize_str,
    canonicalize_str_allow_empty,
    canonicalize_bool,
    canonicalize_int,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy
from core.phase2 import phase2_sorted_items

try:
    from Autodesk.Revit.DB import FilteredWorksetCollector, WorksetKind
except ImportError:
    FilteredWorksetCollector = None
    WorksetKind = None


# Per-workset identity keys that drive sig_hash. Deliberately excludes
# workset.owner / workset.is_active_workset (live session state) and
# workset.unique_id (non-Element-backed, traceability only per D-004).
WORKSETS_SEMANTIC_KEYS = (
    "workset.is_default_workset",
    "workset.is_editable",
    "workset.kind",
    "workset.name",
)

_WORKSETS_REQUIRED_KEYS = (
    "workset.name",
    "workset.kind",
    "workset.is_editable",
    "workset.is_default_workset",
)

# Known WorksetKind members (confirmed against a live Revit 2025 session via
# tools/probes/probe_worksets.py's dir()/getattr enum introspection -- fixed
# here as an explicit list, matching every other domain's fixed field lists
# (e.g. units.py's specs_raw), rather than re-running introspection at
# extraction time for a set that isn't expected to grow.
_WORKSET_KIND_FIELD_SUFFIX = (
    ("UserWorkset", "user_workset"),
    ("StandardWorkset", "standard_workset"),
    ("ViewWorkset", "view_workset"),
    ("FamilyWorkset", "family_workset"),
    ("OtherWorkset", "other_workset"),
)

# CLR enum members to skip during dir()/getattr introspection (matches
# tools/probes/probe_worksets.py's _ENUM_INTROSPECT_SKIP).
_ENUM_INTROSPECT_SKIP = set([
    "value__", "GetType", "ToString", "Equals", "GetHashCode",
    "CompareTo", "GetTypeCode", "HasFlag",
])


def _discover_workset_kind_names():
    """Map WorksetKind int value -> member name (e.g. 0 -> "UserWorkset").

    dir()/getattr introspection, not System.Enum.GetNames(): the probe this
    extractor is based on documents System.Enum.GetNames() failing to
    resolve reliably in this Dynamo CPython3 host, with dir()/getattr as the
    proven-working substitute (tools/probes/probe_worksets.py, lines
    ~129-172). str() on a live WorksetKind enum *instance* returns its
    integer value as text in this host (not its symbolic name -- also
    documented in the probe), so per-workset reads below resolve the name
    through this int->name map rather than trusting str(ws.Kind) directly.
    """
    names = {}
    if WorksetKind is None:
        return names
    try:
        candidates = dir(WorksetKind)
    except Exception:
        return names
    for n in candidates:
        if n.startswith("_") or n in _ENUM_INTROSPECT_SKIP:
            continue
        try:
            attr = getattr(WorksetKind, n, None)
            if attr is None:
                continue
            iv = int(str(attr))
        except Exception:
            continue
        names[iv] = n
    return names


def _build_per_workset_record(ws, active_workset_id, kind_name_by_int, ctx):
    try:
        name_raw = ws.Name
    except Exception:
        name_raw = None
    name_v, name_q = canonicalize_str(name_raw)

    try:
        kind_raw = ws.Kind
        kind_int = int(str(kind_raw))
    except Exception:
        kind_int = None
    kind_name = kind_name_by_int.get(kind_int) if kind_int is not None else None
    if kind_name is not None:
        kind_v, kind_q = canonicalize_str(kind_name)
    elif kind_int is not None:
        # Resolved an int but couldn't map it to a member name -- keep the
        # raw integer rather than dropping the observation (fail-soft).
        kind_v, kind_q = canonicalize_str(str(kind_int))
    else:
        kind_v, kind_q = (None, ITEM_Q_UNREADABLE)

    try:
        editable_raw = bool(ws.IsEditable)
    except Exception:
        editable_raw = None
    editable_v, editable_q = canonicalize_bool(editable_raw)

    try:
        default_raw = bool(ws.IsDefaultWorkset)
    except Exception:
        default_raw = None
    default_v, default_q = canonicalize_bool(default_raw)

    try:
        owner_raw = ws.Owner
    except Exception:
        owner_raw = None
    owner_v, owner_q = canonicalize_str_allow_empty(owner_raw)

    try:
        # Workset.UniqueId is a System.Guid, not a System.String (unlike
        # Element.UniqueId) -- must be str()-coerced before it touches JSON
        # serialization, or the run breaks (see module docstring / probe).
        uid_str = safe_str(ws.UniqueId)
    except Exception:
        uid_str = None
    uid_v, uid_q = canonicalize_str(uid_str)

    try:
        ws_id = ws.Id
    except Exception:
        ws_id = None
    is_active = bool(active_workset_id is not None and ws_id is not None and ws_id == active_workset_id)
    active_v, active_q = canonicalize_bool(is_active)

    identity_items = [
        make_identity_item("workset.name", name_v, name_q),
        make_identity_item("workset.kind", kind_v, kind_q),
        make_identity_item("workset.is_editable", editable_v, editable_q),
        make_identity_item("workset.is_default_workset", default_v, default_q),
        make_identity_item("workset.owner", owner_v, owner_q),
        make_identity_item("workset.is_active_workset", active_v, active_q),
        make_identity_item("workset.unique_id", uid_v, uid_q),
    ]
    identity_items_sorted = sorted(identity_items, key=lambda it: it.get("k", ""))
    item_by_k = {it["k"]: it for it in identity_items_sorted}

    required_qs = [item_by_k[k]["q"] for k in _WORKSETS_REQUIRED_KEYS]
    blocked = any(q != ITEM_Q_OK for q in required_qs)

    status_reasons = []
    any_incomplete = False
    for it in identity_items_sorted:
        if it.get("q") != ITEM_Q_OK:
            any_incomplete = True
            status_reasons.append("identity.incomplete:{}:{}".format(it.get("q"), it.get("k")))

    status = STATUS_BLOCKED if blocked else (STATUS_DEGRADED if any_incomplete else STATUS_OK)

    record_id = "workset:{}".format(name_v) if name_v else "workset:id_{}".format(
        safe_str(getattr(ws_id, "IntegerValue", None)) if ws_id is not None else "unknown"
    )

    sig_hash = None
    if not blocked:
        semantic_items = [it for it in identity_items_sorted if it.get("k") in WORKSETS_SEMANTIC_KEYS]
        sig_hash = make_hash(serialize_identity_items(semantic_items))

    label_quality = "human"
    if blocked:
        label_quality = "placeholder_unreadable" if name_q == ITEM_Q_UNREADABLE else "placeholder_missing"

    rec = build_record_v2(
        domain="worksets",
        record_id=record_id,
        status=status,
        status_reasons=sorted(set(status_reasons)),
        sig_hash=sig_hash,
        identity_items=identity_items_sorted,
        required_qs=required_qs,
        label={
            "display": safe_str(name_v) if name_v else "(unnamed workset)",
            "quality": label_quality,
            "provenance": "revit.Workset.Name",
            "components": {"kind": safe_str(kind_v) if kind_v else ""},
        },
    )
    rec["is_purgeable"] = None
    rec["is_purgeable_q"] = "unsupported_not_applicable"

    pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "worksets")
    rec["join_key"], _missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=identity_items_sorted,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )

    cosmetic_keys = {"workset.owner"}
    unknown_keys = {"workset.is_active_workset", "workset.unique_id"}
    rec["phase2"] = {
        "schema": "phase2.worksets.v1",
        "grouping_basis": "phase2.hypothesis",
        "cosmetic_items": phase2_sorted_items(
            [dict(it) for it in identity_items_sorted if it.get("k") in cosmetic_keys]
        ),
        "coordination_items": phase2_sorted_items([]),
        "unknown_items": phase2_sorted_items(
            [dict(it) for it in identity_items_sorted if it.get("k") in unknown_keys]
        ),
    }
    rec["sig_basis"] = {
        "schema": "worksets.sig_basis.v1",
        "keys_used": list(WORKSETS_SEMANTIC_KEYS),
    }

    return rec


def _build_doc_level_record(doc, is_workshared, active_workset_name, kind_counts, ctx):
    doc_items = []

    is_workshared_v, is_workshared_q = canonicalize_bool(is_workshared)
    doc_items.append(make_identity_item("worksets_doc.is_workshared", is_workshared_v, is_workshared_q))

    if not is_workshared:
        awn_v, awn_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
    elif active_workset_name:
        awn_v, awn_q = canonicalize_str(active_workset_name)
    else:
        awn_v, awn_q = (None, ITEM_Q_UNREADABLE)
    doc_items.append(make_identity_item("worksets_doc.active_workset_name", awn_v, awn_q))

    for kind_name, suffix in _WORKSET_KIND_FIELD_SUFFIX:
        key = "worksets_doc.count_{}".format(suffix)
        if not is_workshared:
            cv, cq = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
        else:
            cnt = kind_counts.get(kind_name)
            if cnt is None:
                cv, cq = (None, ITEM_Q_UNREADABLE)
            else:
                cv, cq = canonicalize_int(cnt)
        doc_items.append(make_identity_item(key, cv, cq))

    doc_items_sorted = sorted(doc_items, key=lambda it: it.get("k", ""))

    status_reasons = []
    any_incomplete = False
    for it in doc_items_sorted:
        if it.get("q") != ITEM_Q_OK:
            any_incomplete = True
            status_reasons.append("identity.incomplete:{}:{}".format(it.get("q"), it.get("k")))

    # Document-level fields are optional (never block): a doc-level read
    # hiccup must not take down the per-workset records or leave the
    # summary record entirely absent.
    status = STATUS_DEGRADED if any_incomplete else STATUS_OK
    sig_hash = make_hash(serialize_identity_items(doc_items_sorted))

    rec = build_record_v2(
        domain="worksets_doc",
        record_id="worksets:_doc",
        status=status,
        status_reasons=sorted(set(status_reasons)),
        sig_hash=sig_hash,
        identity_items=doc_items_sorted,
        required_qs=[],
        label={
            "display": "Worksets (Document Summary)",
            "quality": "system",
            "provenance": "none",
            "components": {},
        },
    )
    rec["is_purgeable"] = None
    rec["is_purgeable_q"] = "unsupported_not_applicable"

    pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "worksets_doc")
    rec["join_key"], _missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=doc_items_sorted,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )

    rec["phase2"] = {
        "schema": "phase2.worksets_doc.v1",
        "grouping_basis": "phase2.hypothesis",
        "cosmetic_items": phase2_sorted_items([]),
        "coordination_items": phase2_sorted_items([]),
        "unknown_items": phase2_sorted_items([]),
    }
    rec["sig_basis"] = {
        "schema": "worksets_doc.sig_basis.v1",
        "keys_used": [it.get("k") for it in doc_items_sorted],
    }

    return rec


def extract_worksets(doc, ctx=None):
    """
    Extract Worksets fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for cross-domain reads; reserved for
            future consumers, e.g. Area 4's browser_organization resolving
            WorksetId through these records -- see CLAUDE.md's Out of scope
            note for this area)

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

    if FilteredWorksetCollector is None or WorksetKind is None:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"WorksetKind_unavailable": True}
        return info

    kind_name_by_int = _discover_workset_kind_names()

    try:
        is_workshared = bool(getattr(doc, "IsWorkshared", False))
    except Exception:
        is_workshared = False

    active_workset_id = None
    if is_workshared:
        try:
            wt = doc.GetWorksetTable()
            active_workset_id = wt.GetActiveWorksetId()
        except Exception:
            active_workset_id = None

    user_worksets = []
    if is_workshared:
        try:
            user_worksets = list(FilteredWorksetCollector(doc).OfKind(WorksetKind.UserWorkset))
        except Exception:
            user_worksets = []

    info["raw_count"] = len(user_worksets)

    kind_counts = {}
    if is_workshared:
        for kind_name, _suffix in _WORKSET_KIND_FIELD_SUFFIX:
            kind_attr = getattr(WorksetKind, kind_name, None)
            if kind_attr is None:
                kind_counts[kind_name] = None
                continue
            try:
                kind_counts[kind_name] = len(list(FilteredWorksetCollector(doc).OfKind(kind_attr)))
            except Exception:
                kind_counts[kind_name] = None

    active_workset_name = None
    if is_workshared and active_workset_id is not None:
        for ws in user_worksets:
            try:
                if ws.Id == active_workset_id:
                    active_workset_name = ws.Name
                    break
            except Exception:
                continue

    v2_records = []
    v2_sig_hashes = []
    v2_block_reasons = {}

    for ws in user_worksets:
        rec = _build_per_workset_record(ws, active_workset_id, kind_name_by_int, ctx)
        v2_records.append(rec)
        if rec.get("sig_hash"):
            v2_sig_hashes.append(rec["sig_hash"])
        else:
            v2_block_reasons["record_blocked:{}".format(rec.get("record_id"))] = True

    doc_rec = _build_doc_level_record(doc, is_workshared, active_workset_name, kind_counts, ctx)
    v2_records.append(doc_rec)
    if doc_rec.get("sig_hash"):
        v2_sig_hashes.append(doc_rec["sig_hash"])

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

    if v2_sig_hashes:
        info["hash_v2"] = make_hash(sorted(v2_sig_hashes))
        info["debug_v2_blocked"] = False
        info["debug_v2_block_reasons"] = {}
    else:
        info["hash_v2"] = None
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = v2_block_reasons or {"no_nonblocked_records": True}

    return info
