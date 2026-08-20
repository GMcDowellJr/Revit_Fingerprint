# -*- coding: utf-8 -*-
"""
Identity domain extractor.

Captures project metadata including:
- Project title
- Central path / file path
- Worksharing status
- Revit version information
- ProjectInformation fields (project_info.*): built-in project metadata
  (number/status/address/issue date/client/building/organization/IFC GUIDs)
  plus deployment-configured optional shared parameters, where present.

Despite the module summary above, this domain DOES compute a real sig_hash
(see identity_items / build_record_v2 below) from a subset of its captured
fields -- the "no hash" description is stale relative to the code and is not
updated here beyond this note, per this change's own scope boundary.
"""

import os
import sys
import uuid

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
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)

from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
    phase2_join_hash,
)
from core.record_v2 import (
    STATUS_BLOCKED,
    STATUS_DEGRADED,
    STATUS_OK,
    ITEM_Q_OK,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    build_record_v2,
    canonicalize_bool,
    canonicalize_str,
    make_identity_item,
    serialize_identity_items,
)
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy, compute_projection_status

try:
    from Autodesk.Revit.DB import WorksharingUtils, BuiltInParameter
except ImportError:
    WorksharingUtils = None
    BuiltInParameter = None

try:
    from System import Guid
except ImportError:
    Guid = None


# ---------------------------
# project_info.* field tables
# ---------------------------
#
# Built-ins: present on ProjectInformation for every Revit project regardless
# of template (confirmed via tools/probes/probe_identity.py's definition_origin
# classifier -- see audit_results/audit_11_domain_extractor_delta_step0_findings.md
# section 5.2). Read via BuiltInParameter so behavior is independent of Revit's
# display-language locale (LookupParameter-by-name is locale-sensitive). The
# three IFC GUID fields are ALSO confirmed built-ins (PR review follow-up):
# tools/archetype/bip_lookup.json (a generated BuiltInParameter id->name
# registry consumed elsewhere, e.g. domains/browser_organization.py) records
# IFC_PROJECT_GUID/IFC_BUILDING_GUID/IFC_SITE_GUID as real enum members, not
# custom/shared parameters -- moved here from the named-field table below.
_PROJECT_INFO_BUILTIN_FIELDS = (
    ("project_info.number", "PROJECT_NUMBER"),
    ("project_info.status", "PROJECT_STATUS"),
    ("project_info.address", "PROJECT_ADDRESS"),
    ("project_info.issue_date", "PROJECT_ISSUE_DATE"),
    ("project_info.client_name", "CLIENT_NAME"),
    ("project_info.building_name", "PROJECT_BUILDING_NAME"),
    ("project_info.organization_name", "PROJECT_ORGANIZATION_NAME"),
    ("project_info.organization_description", "PROJECT_ORGANIZATION_DESCRIPTION"),
    ("project_info.ifc_building_guid", "IFC_BUILDING_GUID"),
    ("project_info.ifc_project_guid", "IFC_PROJECT_GUID"),
    ("project_info.ifc_site_guid", "IFC_SITE_GUID"),
)

# Optional deployment fields are deliberately absent from checked-in defaults.
# A deployment may pass ``ctx["project_info_shared_parameters"]`` as a list of
# mappings with ``key``, ``name``, and optional ``guid`` members. Keeping this
# at the extraction boundary avoids embedding an organization's parameter
# names or identifiers in production source.
_PROJECT_INFO_SHARED_PARAMETER_CONFIG_KEY = "project_info_shared_parameters"


def _param_raw_str(p):
    """Best-effort string extraction from a Parameter, tolerant of storage type."""
    raw = p.AsString()
    if raw is None:
        raw = p.AsValueString()
    return raw


def _read_project_info_builtin_item(pi, key, bip_name):
    """Read a ProjectInformation field via BuiltInParameter enum.

    Built-ins are expected to exist on every project's ProjectInformation
    element; a missing Parameter object (not just an empty value) is treated
    as unreadable rather than missing, since that would indicate something
    unexpected about the document/API surface rather than a normal blank field.
    """
    if pi is None or BuiltInParameter is None:
        return make_identity_item(key, None, ITEM_Q_UNREADABLE)

    bip = getattr(BuiltInParameter, bip_name, None)
    if bip is None:
        return make_identity_item(key, None, ITEM_Q_UNREADABLE)

    try:
        p = pi.get_Parameter(bip)
    except Exception:
        return make_identity_item(key, None, ITEM_Q_UNREADABLE)

    if p is None:
        return make_identity_item(key, None, ITEM_Q_UNREADABLE)

    try:
        raw = _param_raw_str(p)
    except Exception:
        return make_identity_item(key, None, ITEM_Q_UNREADABLE)

    v, q = canonicalize_str(raw)
    return make_identity_item(key, v, q)


def _read_project_info_named_item(pi, key, param_name, guid_str=None):
    """Read a ProjectInformation field by display name (shared/custom
    parameters without a stable BuiltInParameter id) -- or, when guid_str is
    given by the shared parameter's GUID via
    Element.get_Parameter(Guid), which resolves the exact bound definition
    instead of matching by display name (LookupParameter can otherwise return
    an arbitrary same-named parameter if more than one exists in a project).

    Distinguishes "parameter definition not loaded on this document" (q=
    unsupported.not_applicable -- e.g. a configured deployment field on a
    project where its definition is not loaded) from "parameter present but unreadable" (q=unreadable) and from
    "parameter present with no value" (q=missing, via canonicalize_str).
    """
    if pi is None:
        return make_identity_item(key, None, ITEM_Q_UNREADABLE)

    if guid_str and Guid is None:
        raise RuntimeError("System.Guid is required for configured GUID lookup")
    if guid_str:
        try:
            p = pi.get_Parameter(Guid(guid_str))
        except Exception:
            return make_identity_item(key, None, ITEM_Q_UNREADABLE)
    else:
        try:
            p = pi.LookupParameter(param_name)
        except Exception:
            return make_identity_item(key, None, ITEM_Q_UNREADABLE)

    if p is None:
        return make_identity_item(key, None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)

    try:
        raw = _param_raw_str(p)
    except Exception:
        return make_identity_item(key, None, ITEM_Q_UNREADABLE)

    v, q = canonicalize_str(raw)
    return make_identity_item(key, v, q)


def validate_project_info_shared_parameters(fields, allowed_keys=None):
    """Validate and normalize deployment fields before extraction starts."""
    if fields is None:
        return []
    if not isinstance(fields, (list, tuple)):
        raise ValueError("project_info_shared_parameters must be a list")
    builtins = {key for key, _ in _PROJECT_INFO_BUILTIN_FIELDS} | {"project_info.name"}
    seen_keys, seen_guids, normalized = set(), {}, []
    for field in fields:
        if not isinstance(field, dict):
            raise ValueError("each configured project-information field must be a mapping")
        key = safe_str(field.get("key")).strip()
        name = safe_str(field.get("name")).strip()
        guid = safe_str(field.get("guid")).strip() or None
        if not key.startswith("project_info.") or not name:
            raise ValueError("configured project-information fields require project_info.* key and non-blank name")
        if key in builtins:
            raise ValueError("configured key collides with a built-in field: {}".format(key))
        if allowed_keys is not None and key not in allowed_keys:
            raise ValueError("configured project-information key is not contract-registered: {}".format(key))
        if key in seen_keys:
            raise ValueError("duplicate configured project-information key: {}".format(key))
        if guid:
            try:
                guid = str(uuid.UUID(guid))
            except (ValueError, AttributeError, TypeError):
                raise ValueError("malformed GUID for configured project-information field: {}".format(key))
            if guid in seen_guids and seen_guids[guid] != key:
                raise ValueError("configured GUID maps to conflicting keys")
            seen_guids[guid] = key
        seen_keys.add(key)
        normalized.append({"key": key, "name": name, "guid": guid})
    return normalized


def _configured_project_info_fields(ctx):
    fields = validate_project_info_shared_parameters((ctx or {}).get(_PROJECT_INFO_SHARED_PARAMETER_CONFIG_KEY, []))
    return [(f["key"], f["name"], f["guid"]) for f in fields]


def _extract_project_info_items(doc, ctx=None):
    """Build project_info.* identity items from doc.ProjectInformation.

    Returns a list of IdentityItem dicts (unsorted); callers merge into the
    domain's identity_items list.
    """
    try:
        pi = doc.ProjectInformation
    except Exception:
        pi = None

    items = []

    if pi is None:
        # ProjectInformation is a well-known singleton element; its absence is
        # a real gap, not a normal per-field condition -- mark every field
        # unreadable rather than silently omitting them.
        for key, _bip_name in _PROJECT_INFO_BUILTIN_FIELDS:
            items.append(make_identity_item(key, None, ITEM_Q_UNREADABLE))
        for key, _param_name, _guid in _configured_project_info_fields(ctx):
            items.append(make_identity_item(key, None, ITEM_Q_UNREADABLE))
        items.append(make_identity_item("project_info.name", None, ITEM_Q_UNREADABLE))
        return items

    # project_info.name: ProjectInformation.Name (matches the established
    # tools/probes/probe_identity.py mechanism/key), not LookupParameter("Project Name").
    try:
        name_v, name_q = canonicalize_str(pi.Name)
    except Exception:
        name_v, name_q = None, ITEM_Q_UNREADABLE
    items.append(make_identity_item("project_info.name", name_v, name_q))

    for key, bip_name in _PROJECT_INFO_BUILTIN_FIELDS:
        items.append(_read_project_info_builtin_item(pi, key, bip_name))

    for key, param_name, guid_str in _configured_project_info_fields(ctx):
        items.append(_read_project_info_named_item(pi, key, param_name, guid_str=guid_str))

    return items

def _phase2_build_lineage_items(info):
    """
    Phase-2 lineage signals for identity domain (heuristic, non-authoritative).

    Purpose:
    - Support file history / lineage hypotheses (moved/copied/renamed) without
      presenting a domain join key that could be mistaken for identity.
    """
    items = []

    # Central path (raw) and a normalized variant (best-effort; still heuristic).
    cp_raw = info.get("central_path", None)
    v, q = phase2_qv_from_legacy_sentinel_str(cp_raw, allow_empty=False)
    items.append({"k": "identity.central_path", "q": q, "v": v})

    cp_norm = safe_str(cp_raw).strip().replace("\\", "/").lower()
    v, q = phase2_qv_from_legacy_sentinel_str(cp_norm, allow_empty=False)
    items.append({"k": "identity.central_path_norm", "q": q, "v": v})

    # Filename (weak signal; helpful when paths move but names persist).
    fn = os.path.basename(safe_str(cp_raw).replace("\\", "/").strip())
    v, q = phase2_qv_from_legacy_sentinel_str(fn, allow_empty=False)
    items.append({"k": "identity.filename", "q": q, "v": v})

    # Workshared flag (context signal).
    v_raw = "true" if bool(info.get("is_workshared", False)) else "false"
    v, q = phase2_qv_from_legacy_sentinel_str(v_raw, allow_empty=False)
    items.append({"k": "identity.is_workshared", "q": q, "v": v})

    # Project title (very weak; include explicitly so it never “sneaks in” elsewhere).
    pt_raw = info.get("project_title", None)
    v, q = phase2_qv_from_legacy_sentinel_str(pt_raw, allow_empty=False)
    items.append({"k": "identity.project_title", "q": q, "v": v})

    return phase2_sorted_items(items)

def extract(doc, ctx=None):
    """
    Extract project identity metadata from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain)

    Returns:
        Dictionary with project metadata (no hash)
    """
    app = doc.Application
    info = {}

    info["project_title"] = safe_str(doc.Title)

    try:
        if doc.IsWorkshared:
            # Central path or model path
            try:
                mp = WorksharingUtils.GetModelPath(doc)
                info["central_path"] = safe_str(mp.CentralServerPath)
            except Exception as e:
                info["central_path"] = safe_str(doc.PathName)
        else:
            info["central_path"] = safe_str(doc.PathName)
    except Exception as e:
        info["central_path"] = safe_str(doc.PathName)

    info["is_workshared"] = bool(getattr(doc, "IsWorkshared", False))

    # Revit version/build
    info["revit_version_number"] = safe_str(app.VersionNumber)
    info["revit_version_name"]   = safe_str(app.VersionName)
    info["revit_build"]          = safe_str(app.VersionBuild)

    # ---------------------------
    # Phase-2 additive emission + record.v2 pilot (single-record domain)
    # ---------------------------

    lineage_items = _phase2_build_lineage_items(info)
    lineage_hash = phase2_join_hash(lineage_items)

    # Attribute hypotheses (Phase-2 only; no enforcement / no inference)
    semantic_items = []
    cosmetic_items = []
    unknown_items = []

    # semantic (hypothesis): worksharing + version/build metadata
    # NOTE: file-local identifiers (paths/title/filename) are intentionally excluded from
    # canonical identity evidence and remain only in label/lineage/debug surfaces.
    v, q = phase2_qv_from_legacy_sentinel_str(
        "true" if bool(info.get("is_workshared", False)) else "false",
        allow_empty=False,
    )
    semantic_items.append({"k": "identity.is_workshared", "q": q, "v": v})

    for k in ("revit_version_number", "revit_build"):
        v_raw = info.get(k, None)
        v, q = phase2_qv_from_legacy_sentinel_str(v_raw, allow_empty=False)
        semantic_items.append({"k": "identity.{}".format(k), "q": q, "v": v})

    # cosmetic (hypothesis): application/version/build metadata
    for k in ("revit_version_number", "revit_version_name", "revit_build"):
        v_raw = info.get(k, None)
        v, q = phase2_qv_from_legacy_sentinel_str(v_raw, allow_empty=False)
        cosmetic_items.append({"k": "identity.{}".format(k), "q": q, "v": v})

    # unknown (hypothesis): title may be stable or may change (keep explicit)
    v, q = phase2_qv_from_legacy_sentinel_str(info.get("project_title", None), allow_empty=False)
    unknown_items.append({"k": "identity.project_title", "q": q, "v": v})

    info["phase2"] = {
        "schema": "phase2.identity.v1",
        "grouping_basis": "phase2.hypothesis",
        "semantic_items": phase2_sorted_items(semantic_items),
        "cosmetic_items": phase2_sorted_items(cosmetic_items),
        "coordination_items": phase2_sorted_items([]),
        "unknown_items": phase2_sorted_items(unknown_items),

        # lineage (heuristic): explicit non-authoritative correlation surface
        "lineage_items": lineage_items,
        "lineage_hash": lineage_hash,
    }

    # Canonical evidence superset for this pilot is identity_basis.items (record.v2).
    # Selectors (join_key.keys_used, phase2.semantic_keys, sig_basis.keys_used) define
    # hashed/semantic subsets without duplicating k/q/v evidence.
    identity_items = []
    is_workshared_v, is_workshared_q = canonicalize_bool(info.get("is_workshared", False))
    identity_items.append(make_identity_item("identity.is_workshared", is_workshared_v, is_workshared_q))

    rvn_v, rvn_q = canonicalize_str(info.get("revit_version_number", None))
    identity_items.append(make_identity_item("identity.revit_version_number", rvn_v, rvn_q))

    rvname_v, rvname_q = canonicalize_str(info.get("revit_version_name", None))
    identity_items.append(make_identity_item("identity.revit_version_name", rvname_v, rvname_q))

    rb_v, rb_q = canonicalize_str(info.get("revit_build", None))
    identity_items.append(make_identity_item("identity.revit_build", rb_v, rb_q))

    # status/status_reasons are computed from the original core items only
    # (worksharing/version/build) -- see D-025. project_info.* fields are merged
    # in afterward: they fully participate in identity_basis.items and sig_hash,
    # but blank/not-applicable ProjectInfo fields (extremely common in practice --
    # e.g. a deployment field may be absent by design on another template) must
    # not flip this domain's record status to degraded on every ordinary export.
    status_reasons = []
    if any(it.get("q") != ITEM_Q_OK for it in identity_items):
        status_reasons = [
            "identity.incomplete:{}:{}".format(it.get("q"), it.get("k"))
            for it in identity_items
            if it.get("q") != ITEM_Q_OK
        ]

    status = STATUS_OK if not status_reasons else STATUS_DEGRADED

    identity_items = identity_items + _extract_project_info_items(doc, ctx=ctx)
    identity_items = sorted(identity_items, key=lambda it: safe_str(it.get("k", "")))

    sig_preimage = serialize_identity_items(identity_items)
    sig_hash = make_hash(sig_preimage) if status != STATUS_BLOCKED else None

    pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "identity")
    join_key, _missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=identity_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )

    # sig_basis.keys_used must describe what sig_hash actually hashes (all of
    # identity_items, per serialize_identity_items(identity_items) above) --
    # computed dynamically rather than hardcoded so it can't drift from the
    # real hash inputs as fields are added (this also fixes a pre-existing gap
    # where "identity.revit_version_name" was hashed but absent from this list).
    #
    # This is DELIBERATELY a separate selector from phase2.semantic_keys just
    # below (PR review follow-up): "what sig_hash hashes" and "what Phase-2
    # calls behavior-defining" are different questions. project_info.* items
    # are naming/label metadata (D-025's own framing) included in sig_hash as
    # an explicit, documented exception -- they must not also be reported as
    # Phase-2 "semantic" (behavior-defining) content, or consumers of
    # phase2.semantic_keys would see ordinary metadata edits (e.g. a project
    # address correction) as semantic/behavioral differences.
    sig_basis_keys_used = sorted(it["k"] for it in identity_items)

    # phase2.semantic_keys: unchanged by D-025 -- still just the pre-existing
    # worksharing/version/build behavioral core. "identity.revit_version_name"
    # stays excluded, as before (it lives in cosmetic_items below instead).
    semantic_keys = sorted(["identity.is_workshared", "identity.revit_version_number", "identity.revit_build"])
    info["phase2"].pop("semantic_items", None)
    info["phase2"]["semantic_keys"] = semantic_keys

    # Canonical Name Identity Projection (PR1): second, independent join_hash variant keyed
    # off this record's own label.display-backing item (identity.project_title). Unlike
    # is_workshared/revit_version_*/revit_build above, project_title is not a member of
    # identity_items today -- it lives only in phase2.unknown_items (file-local noise,
    # excluded from join-keys by Phase-2 bucket contract). This call therefore
    # uses a LOCAL widened items list (identity_items + one freshly-wrapped item) for the
    # name-key projection only; identity_basis.items/sig_hash/join_key above are computed
    # from the original, unwidened identity_items list and are unaffected.
    project_title_v, project_title_q = canonicalize_str(info.get("project_title", None))
    name_key_items = identity_items + [
        make_identity_item("identity.project_title", project_title_v, project_title_q)
    ]
    name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), "identity")
    name_key, name_key_missing = build_join_key_from_policy(
        domain_policy=name_key_pol,
        identity_items=name_key_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )
    name_key["status"] = compute_projection_status(name_key_pol, name_key_missing)

    rec_v2 = build_record_v2(
        domain="identity",
        record_id="document",
        status=status,
        status_reasons=sorted(set(status_reasons)),
        sig_hash=sig_hash,
        identity_items=identity_items,
        required_qs=[is_workshared_q, rvn_q, rb_q],
        label={
            "display": safe_str(info.get("project_title", "")),
            "quality": "human",
            "provenance": "revit.Document.Title",
        },
    )
    rec_v2["is_purgeable"] = None
    rec_v2["is_purgeable_q"] = "unsupported_not_applicable"
    rec_v2["join_key"] = join_key
    rec_v2["join_key_name_identity"] = name_key
    rec_v2["phase2"] = info["phase2"]
    rec_v2["sig_basis"] = {
        # v2 (D-025): sig_hash preimage now includes project_info.* content;
        # bumped so consumers comparing sig_hash across exports can tell a
        # pre-D-025 export apart from a post-D-025 one instead of reading an
        # ordinary schema-version difference as fingerprint drift.
        "schema": "identity.sig_basis.v2",
        "keys_used": sig_basis_keys_used,
    }

    # Back-compat conveniences while the ecosystem pivots to record.v2.
    info["records"] = [rec_v2]
    info["record_rows"] = [{"record_key": "document", "sig_hash": sig_hash, "name": safe_str(info.get("project_title", ""))}]
    info["signature_hashes_v2"] = [sig_hash] if sig_hash else []
    info["hash_v2"] = sig_hash
    info["join_key"] = join_key
    info["sig_hash"] = sig_hash

    return info
