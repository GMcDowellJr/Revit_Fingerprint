# -*- coding: utf-8 -*-
"""
Dynamo runner for Revit Fingerprint extraction.

This runner:
- Acquires the Revit document from Dynamo context
- Selects which domains to run (allowlist mechanism)
- Assembles final JSON output

Current implementation (M5): full modular architecture with behavioral view templates
"""

import clr
import json
import sys
import os
import time
import hashlib

_SCRIPT_START = time.perf_counter()

# --- ensure unsafe-location flags exist before use ---
def _looks_like_unc_path(p):
    try:
        s = str(p)
    except Exception:
        return False
    return s.startswith("\\\\")

def _is_probably_sync_path(p):
    try:
        s = os.path.abspath(str(p))
    except Exception:
        return False
    sl = s.lower()
    for m in ("\\onedrive\\", "\\sharepoint\\", "\\microsoft teams\\"):
        if m in sl:
            return True
    if "\\documents\\" in sl and ("- sharepoint" in sl or "sharepoint" in sl):
        return True
    return False

# runner/.. is the repo root
try:
    _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except Exception:
    _SCRIPT_DIR = os.getcwd()

_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)

_UNSAFE_REASONS = []
if _looks_like_unc_path(_REPO_ROOT):
    _UNSAFE_REASONS.append("repo_root_is_unc_path")
if _is_probably_sync_path(_REPO_ROOT):
    _UNSAFE_REASONS.append("repo_root_looks_like_sharepoint_onedrive_sync")

def _read_tool_version(repo_root):
    try:
        p = os.path.join(repo_root, "VERSION.txt")
        if not os.path.exists(p):
            return None
        with open(p, "r") as f:
            s = f.read().strip()
        return s if s else None
    except Exception:
        return None

_TOOL_VERSION = _read_tool_version(_REPO_ROOT)
# --- end unsafe-location flags ---

if _UNSAFE_REASONS:
    OUT = json.dumps(
        {
            "status": "blocked",
            "error": "Unsafe execution location. Install locally and run from there (not SharePoint/OneDrive/UNC).",
            "repo_root": _REPO_ROOT,
            "unsafe_reasons": _UNSAFE_REASONS,
            "_meta": {
                "runner": "M5",
                "runner_file": __file__,
                "tool_version": _TOOL_VERSION,
            },
        },
        indent=2,
        sort_keys=True,
    )
    raise SystemExit

# Add repo root to path for imports
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# Contract + dependency utilities (must be imported after sys.path adjustment)
from core import contracts
from core.collect import CollectCtx
from core.context import DocViewContext
from core.deps import Blocked, require_domain
from core import naming as fp_naming
from core.timing_collector import TimingCollector

# Revit/Dynamo plumbing
clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

# Import domain extractors
from domains import identity, units, line_patterns, line_styles
from domains import arrowheads, text_types
from domains import view_filter_definitions, view_filter_applications_view_templates
from domains import phases, phase_filters, phase_graphics
from domains import view_category_overrides
# Split domains: object_styles
from domains import object_styles
from domains import fill_patterns
from domains import dimension_types
from domains import view_templates
from core.manifest import build_manifest
from core.features import build_features
from core.join_key_policy import load_join_key_policies

# Domain selection configuration
# Set to None to run all domains, or provide a list of domain names to run specific domains
ENABLED_DOMAINS = None  # None = all domains

# Hash computation uses record.v2 identity_basis items (semantic mode).
# Legacy pipe-delimited hashing removed in PR #XXX.

_DOMAIN_VERSION = "1"

def _use_filename_stamp():
    """
    Returns True unless explicitly disabled.

    Accepts common Dynamo / shell representations for false.
    """
    try:
        v = os.environ.get("REVIT_FINGERPRINT_FILENAME_STAMP", "")
    except Exception:
        v = ""
    v = str(v).strip().lower()

    if not v:
        return True

    if v in ("0", "false", "no", "off", "n", "f"):
        return False

    # Handle "0.0" / "1.0" style values
    try:
        fv = float(v)
        if fv == 0.0:
            return False
        if fv == 1.0:
            return True
    except Exception:
        pass

    # Default: enabled
    return True

def _extract_v2_hash(payload):
    """
    Best-effort extraction of the contract semantic hash (v2) without changing legacy behavior.
    """
    try:
        if isinstance(payload, dict):
            # Primary: current domain contract surface
            if "hash_v2" in payload:
                return payload.get("hash_v2", None)

            # Fallback: future/alternate nesting (do not require domains to emit this)
            sv2 = payload.get("semantic_v2", None)
            if isinstance(sv2, dict) and "hash" in sv2:
                return sv2.get("hash", None)
    except Exception as e:
        pass
    return None


def _extract_legacy_quality(payload):
    q = {}
    try:
        if isinstance(payload, dict) and "count" in payload:
            q["count"] = payload.get("count", None)
        if isinstance(payload, dict) and "raw_count" in payload:
            q["raw_count"] = payload.get("raw_count", None)
    except Exception as e:
        pass
    return q

def _extract_v2_block_reasons(payload):
    """Best-effort extraction of v2 block reasons from a domain payload.

    Domains are allowed to evolve their internal debug surfaces; the runner
    lifts these into the authoritative contract diag.
    """
    if not isinstance(payload, dict):
        return {}

    # Current domains (PR6–PR8) typically emit one of these.
    for k in ("debug_v2_block_reasons", "v2_block_reasons", "semantic_v2_block_reasons"):
        try:
            v = payload.get(k, None)
        except Exception:
            v = None
        if isinstance(v, dict) and v:
            # Keep values stable: prefer ints/bools/strings only.
            out = {}
            for rk, rv in v.items():
                try:
                    key = str(rk)
                except Exception:
                    continue
                if rv is None:
                    out[key] = True
                elif isinstance(rv, (bool, int, float, str)):
                    out[key] = rv
                else:
                    out[key] = True
            return out

    # Some domains only expose a simple blocked flag.
    for k in ("debug_v2_blocked", "v2_blocked"):
        try:
            if bool(payload.get(k, False)) is True:
                return {"blocked": True}
        except Exception:
            pass

    return {}


def _looks_like_revit_unique_id(v):
    """Heuristic: detect Revit UniqueId strings."""
    try:
        s = str(v or "")
    except Exception:
        return False
    if not s or len(s) < 45:
        return False
    import re as _re
    return bool(_re.match(r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}-[0-9A-Fa-f]{8}$", s))

def _has_v2_surface(payload):
    """Return True if the domain payload appears to implement a v2 hash surface."""
    if not isinstance(payload, dict):
        return False
    try:
        if "hash_v2" in payload:
            return True
    except Exception:
        pass
    try:
        sv2 = payload.get("semantic_v2", None)
        if isinstance(sv2, dict) and ("hash" in sv2):
            return True
    except Exception:
        pass
    return False

def _domain_run(domain_name, fn, doc, ctx, contract_domains, run_diag, runner_notes, *, require_v2_hash=True):
    """Runs a domain extractor and records a contract envelope.

    Returns legacy_payload (or None on failure).
    """
    import traceback as _traceback

    domain_name = str(domain_name)

    # Timing instrumentation: wrap domain extraction
    _tc = ctx.get("_timing") if isinstance(ctx, dict) else None
    _timing_label = "domain:{}".format(domain_name)

    try:
        if _tc is not None:
            try:
                _tc.set_active_domain(domain_name)
                _tc.start_timer(_timing_label)
            except Exception:
                pass

        legacy = fn(doc, ctx)

        # Domains may optionally emit contract signals into their legacy payload.
        # Runner lifts these into the authoritative contract envelope and strips them from the legacy payload.
        domain_status = contracts.DOMAIN_STATUS_OK
        block_reasons = []
        domain_diag = {
            "api_reachable": True,
        }

        if isinstance(legacy, dict):
            try:
                _st = legacy.pop("_domain_status", None)
                if isinstance(_st, str) and _st in contracts.VALID_DOMAIN_STATUSES:
                    domain_status = _st
            except Exception:
                pass

            try:
                _br = legacy.pop("_domain_block_reasons", None)
                if isinstance(_br, list):
                    block_reasons = [str(x) for x in _br]
            except Exception:
                pass

            try:
                _dg = legacy.pop("_domain_diag", None)
                if isinstance(_dg, dict):
                    # Merge domain diag into base diag (domain wins on key collisions)
                    domain_diag.update(_dg)
            except Exception:
                pass

        hash_value = _extract_v2_hash(legacy)

        # Lift v2 diagnostics into the contract envelope.
        domain_diag["has_v2"] = bool(_has_v2_surface(legacy))
        try:
            recs = legacy.get("records", None) if isinstance(legacy, dict) else None
            if isinstance(recs, list):
                domain_diag["details_records_count"] = len(recs)
                v2_count = 0
                sample_items = None
                uid_like_values = 0
                uid_key_count = 0
                for r in recs[:3]:
                    if isinstance(r, dict) and r.get("schema_version", None) == "record.v2":
                        v2_count += 1
                        ib = r.get("identity_basis", {}) if isinstance(r.get("identity_basis", {}), dict) else {}
                        items = ib.get("items", []) if isinstance(ib.get("items", []), list) else []
                        if sample_items is None and items:
                            sample_items = items
                        for it in items:
                            if not isinstance(it, dict):
                                continue
                            k = str(it.get("k", ""))
                            if ("uid" in k) or k.endswith("_uid"):
                                uid_key_count += 1
                            if _looks_like_revit_unique_id(it.get("v", None)):
                                uid_like_values += 1
                domain_diag["records_v2_sample_count"] = v2_count
                if sample_items is not None:
                    domain_diag["v2_sample_identity_keys"] = [str(it.get("k", "")) for it in sample_items[:12]]
                domain_diag["v2_uid_key_count_in_sample"] = int(uid_key_count)
                domain_diag["v2_uid_like_values_in_sample"] = int(uid_like_values)
        except Exception:
            pass
        v2_reasons = _extract_v2_block_reasons(legacy)
        if v2_reasons:
            domain_diag["v2_block_reasons"] = v2_reasons

        # Lift count/raw_count into the contract diagnostics.
        quality = _extract_legacy_quality(legacy)
        if "count" in quality:
            domain_diag["count"] = quality["count"]
        if "raw_count" in quality:
            domain_diag["raw_count"] = quality["raw_count"]

        # Empty-population exemption: if the domain explicitly emits raw_count=0 and
        # hash_v2=None with debug_v2_blocked=False, that is a valid "no content" state —
        # not a hash failure.  Only block on no_semantic_hash when raw_count > 0 (i.e. the
        # domain had candidates but produced no hash, which is a genuine problem).
        _raw_count = quality.get("raw_count", None)
        _empty_population = (_raw_count is not None and _raw_count == 0
                             and not bool((legacy or {}).get("debug_v2_blocked", True)))

        if require_v2_hash and domain_status == contracts.DOMAIN_STATUS_OK and hash_value is None and not _empty_population:
            domain_status = contracts.DOMAIN_STATUS_BLOCKED
            if v2_reasons:
                block_reasons = sorted({str(k) for k in v2_reasons.keys()})
            else:
                block_reasons = ["no_semantic_hash"]
        elif domain_status == contracts.DOMAIN_STATUS_BLOCKED and not block_reasons:
            block_reasons = sorted({str(k) for k in v2_reasons.keys()}) if v2_reasons else ["blocked"]

        env = contracts.new_domain_envelope(
            domain=domain_name,
            domain_version=_DOMAIN_VERSION,
            status=domain_status,
            block_reasons=block_reasons,
            diag=domain_diag,
            records=None,
            hash_value=hash_value,
        )
        contract_domains[domain_name] = env

        # End timing on success
        if _tc is not None:
            try:
                _tc.end_timer(_timing_label)
                _tc.set_active_domain(None)
            except Exception:
                pass

        return legacy

    except Exception as e:
        # End timing on failure
        if _tc is not None:
            try:
                _tc.end_timer(_timing_label)
                _tc.set_active_domain(None)
            except Exception:
                pass

        # Hard fail: downstream must not infer success.
        contracts.add_bounded_error(
            run_diag,
            domain=domain_name,
            status=contracts.DOMAIN_STATUS_FAILED,
            code="domain_exception",
            message=str(e),
        )
        contract_domains[domain_name] = contracts.new_domain_envelope(
            domain=domain_name,
            domain_version=_DOMAIN_VERSION,
            status=contracts.DOMAIN_STATUS_FAILED,
            block_reasons=[],
            diag={
                "api_reachable": True,
                "error": str(e),
                "traceback": _traceback.format_exc(),
            },
            records=None,
            hash_value=None,
        )
        runner_notes.append("One or more domains failed; see _contract.run_diag and _contract.domains.*.diag")
        return None

def _enabled(domain_name):
    """
    Allowlist gate for domain execution.
    - ENABLED_DOMAINS = None  -> run all domains
    - ENABLED_DOMAINS = [...] -> run only listed domains (exact key match)
    """
    if ENABLED_DOMAINS is None:
        return True
    try:
        allowed = set(ENABLED_DOMAINS)
    except Exception as e:
        allowed = set()
    return domain_name in allowed

def get_doc():
    """Get current Revit document from Dynamo context."""
    return DocumentManager.Instance.CurrentDBDocument

def run_fingerprint(doc):
    """
    Execute fingerprint extraction on the given document.

    Args:
        doc: Revit Document

    Returns:
        Dictionary with all domain fingerprints
    """
    start_ts = time.time()

    # Context dictionary for cross-domain references
    # Populated by global domains, consumed by contextual domains
    ctx = {}
    ctx["debug_vg_details"] = False

    # Join-key policies (explicit ctx injection; no globals)
    policy_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "policies", "domain_join_key_policies.json")
    ctx["join_key_policies"] = load_join_key_policies(policy_path)

    # PR5: per-run collector cache + counters
    ctx["_collect"] = CollectCtx()

    # Timing instrumentation: create collector and wire into subsystems
    _timing = TimingCollector()
    ctx["_timing"] = _timing
    ctx["_collect"].timing = _timing

    # Wire timing into hashing module (module-level ref, never affects hash output)
    try:
        from core import hashing as _hashing_mod
        _hashing_mod._timing_collector = _timing
    except Exception:
        pass

    _timing.start_timer("total_extraction")

    # PR6: shared document + view context (domains can use for consistent view reads)
    ctx["_doc_view"] = DocViewContext(doc)

    # Assemble fingerprint by calling each domain extractor (legacy payloads)
    fingerprint = {}

    # Contract envelope (authoritative for statuses)
    contract_domains = {}
    run_diag = contracts.new_run_diag()
    runner_notes = []
    
    # Expose authoritative domain envelopes to extractors for dependency gating.
    # contract_domains is mutated as domains run; ctx sees the live dict.
    ctx["_domains"] = contract_domains

    # Metadata domains (no behavioral hash)
    if _enabled("identity"):
        legacy = _domain_run("identity", identity.extract, doc, ctx, contract_domains, run_diag, runner_notes, require_v2_hash=False)
        if legacy is not None:
            fingerprint["identity"] = legacy

    if _enabled("units"):
        legacy = _domain_run("units", units.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["units"] = legacy

    # Global style domains (locked semantics)
    # NOTE: line_patterns must run first to populate ctx mappings consumed by object_styles/line_styles.
    if _enabled("line_patterns"):
        legacy = _domain_run("line_patterns", line_patterns.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["line_patterns"] = legacy

    # object_styles split domains (model must run first to export baseline map to ctx)
    if _enabled("object_styles_model"):
        legacy = _domain_run("object_styles_model", object_styles.extract_model, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["object_styles_model"] = legacy

    if _enabled("object_styles_annotation"):
        legacy = _domain_run("object_styles_annotation", object_styles.extract_annotation, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["object_styles_annotation"] = legacy

    if _enabled("object_styles_analytical"):
        legacy = _domain_run("object_styles_analytical", object_styles.extract_analytical, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["object_styles_analytical"] = legacy

    if _enabled("object_styles_imported"):
        legacy = _domain_run("object_styles_imported", object_styles.extract_imported, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["object_styles_imported"] = legacy

    if _enabled("line_styles"):
        legacy = _domain_run("line_styles", line_styles.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["line_styles"] = legacy

    # fill_patterns split domains
    if _enabled("fill_patterns_drafting"):
        legacy = _domain_run("fill_patterns_drafting", fill_patterns.extract_drafting, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["fill_patterns_drafting"] = legacy

    if _enabled("fill_patterns_model"):
        legacy = _domain_run("fill_patterns_model", fill_patterns.extract_model, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["fill_patterns_model"] = legacy

    if _enabled("arrowheads"):
        legacy = _domain_run("arrowheads", arrowheads.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["arrowheads"] = legacy

    if _enabled("text_types"):
        legacy = _domain_run("text_types", text_types.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["text_types"] = legacy

    # dimension_types split domains
    if _enabled("dimension_types_linear"):
        legacy = _domain_run("dimension_types_linear", dimension_types.extract_linear, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["dimension_types_linear"] = legacy

    if _enabled("dimension_types_angular"):
        legacy = _domain_run("dimension_types_angular", dimension_types.extract_angular, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["dimension_types_angular"] = legacy

    if _enabled("dimension_types_radial"):
        legacy = _domain_run("dimension_types_radial", dimension_types.extract_radial, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["dimension_types_radial"] = legacy

    if _enabled("dimension_types_diameter"):
        legacy = _domain_run("dimension_types_diameter", dimension_types.extract_diameter, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["dimension_types_diameter"] = legacy

    if _enabled("dimension_types_spot_elevation"):
        legacy = _domain_run("dimension_types_spot_elevation", dimension_types.extract_spot_elevation, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["dimension_types_spot_elevation"] = legacy

    if _enabled("dimension_types_spot_coordinate"):
        legacy = _domain_run("dimension_types_spot_coordinate", dimension_types.extract_spot_coordinate, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["dimension_types_spot_coordinate"] = legacy

    if _enabled("dimension_types_spot_slope"):
        legacy = _domain_run("dimension_types_spot_slope", dimension_types.extract_spot_slope, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["dimension_types_spot_slope"] = legacy

    # New global domains (M4) - run before contextual domains
    # These populate ctx with mappings for views/templates to reference
    if _enabled("view_filter_definitions"):
        legacy = _domain_run(
            "view_filter_definitions",
            view_filter_definitions.extract,
            doc,
            ctx,
            contract_domains,
            run_diag,
            runner_notes,
        )
        if legacy is not None:
            fingerprint["view_filter_definitions"] = legacy

    if _enabled("phases"):
        legacy = _domain_run("phases", phases.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["phases"] = legacy

    if _enabled("phase_filters"):
        legacy = _domain_run("phase_filters", phase_filters.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["phase_filters"] = legacy

    # Phase graphics are not exposed via the Revit API (as of 2021–2025).
    # Domain intentionally disabled to avoid misleading fingerprints.
    # if _enabled("phase_graphics"):
    #     fingerprint["phase_graphics"] = phase_graphics.extract(doc, ctx)

    # Contract-only emission: phase graphics are known API-unreachable in supported versions.
    # Do not produce a semantic hash.
    if _enabled("phase_graphics"):
        contract_domains["phase_graphics"] = contracts.new_domain_envelope(
            domain="phase_graphics",
            domain_version=_DOMAIN_VERSION,
            status=contracts.DOMAIN_STATUS_UNSUPPORTED,
            block_reasons=["api_unreachable:phase_graphics"],
            diag={
                "api_reachable": False,
                "reason": "Phase graphics are not reachable via Revit API in supported versions.",
            },
            records=None,
            hash_value=None,
        )

    # Contextual domains (can reference global domains via ctx)
    if _enabled("view_filter_applications_view_templates"):
        legacy = _domain_run(
            "view_filter_applications_view_templates",
            view_filter_applications_view_templates.extract,
            doc,
            ctx,
            contract_domains,
            run_diag,
            runner_notes,
        )
        if legacy is not None:
            fingerprint["view_filter_applications_view_templates"] = legacy

    if _enabled("view_category_overrides"):
        # Hard dependencies: must run after object_styles_model + line_patterns.
        # VCO cannot produce meaningful output without the model baseline or
        # line pattern refs, so these remain hard requirements.
        # fill_patterns are soft — graphic_overrides.py degrades gracefully
        # when a fill pattern ref can't be resolved (q=missing on the field).
        try:
            require_domain(contract_domains, "object_styles_model")
            require_domain(contract_domains, "line_patterns")
            # Soft requirements for fill pattern partitions
            for _fp_dep in ["fill_patterns_drafting", "fill_patterns_model"]:
                if _fp_dep not in contract_domains:
                    runner_notes.append(
                        "view_category_overrides: {} not run; "
                        "fill pattern refs in overrides will degrade to q=missing".format(
                            _fp_dep)
                    )
            # Soft requirements — emit note if absent but do not block VCO
            # (these partitions are legitimately empty in most files)
            for _soft_dep in ["object_styles_annotation",
                              "object_styles_analytical",
                              "object_styles_imported"]:
                if _soft_dep not in contract_domains:
                    runner_notes.append(
                        "view_category_overrides: {} not run; "
                        "{} category overrides will have no baseline".format(
                            _soft_dep,
                            _soft_dep.replace("object_styles_", "")
                        )
                    )

            legacy = _domain_run(
                "view_category_overrides",
                view_category_overrides.extract,
                doc,
                ctx,
                contract_domains,
                run_diag,
                runner_notes,
            )
            if legacy is not None:
                fingerprint["view_category_overrides"] = legacy
        except Blocked as b:
            contract_domains["view_category_overrides"] = contracts.new_domain_envelope(
                domain="view_category_overrides",
                domain_version=_DOMAIN_VERSION,
                status=contracts.DOMAIN_STATUS_BLOCKED,
                block_reasons=list(b.reasons),
                diag={
                    "blocked_code": b.code,
                    "upstream": b.upstream,
                },
                records=None,
                hash_value=None,
            )
            contracts.add_bounded_error(
                run_diag,
                domain="view_category_overrides",
                status=contracts.DOMAIN_STATUS_BLOCKED,
                code=b.code,
                message=";".join(list(b.reasons)),
            )

    # view_templates split domains.
    # Non-schedule domains require both phase_filters and view_filter_definitions
    # (filter stack resolution). Schedules only require phase_filters — ViewSchedule
    # templates do not have view filter stacks.
    for _vt_domain, _vt_extractor in [
        ("view_templates_floor_structural_area_plans", view_templates.extract_floor_structural_area_plans),
        ("view_templates_ceiling_plans", view_templates.extract_ceiling_plans),
        ("view_templates_elevations_sections_detail", view_templates.extract_elevations_sections_detail),
        ("view_templates_renderings_drafting", view_templates.extract_renderings_drafting),
    ]:
        if not _enabled(_vt_domain):
            continue
        try:
            require_domain(contract_domains, "phase_filters")
            require_domain(contract_domains, "view_filter_definitions")
            legacy = _domain_run(_vt_domain, _vt_extractor, doc, ctx,
                                 contract_domains, run_diag, runner_notes)
            if legacy is not None:
                fingerprint[_vt_domain] = legacy
        except Blocked as b:
            contract_domains[_vt_domain] = contracts.new_domain_envelope(
                domain=_vt_domain,
                domain_version=_DOMAIN_VERSION,
                status=contracts.DOMAIN_STATUS_BLOCKED,
                block_reasons=list(b.reasons),
                diag={"blocked_code": b.code, "upstream": b.upstream},
                records=None,
                hash_value=None,
            )
            contracts.add_bounded_error(run_diag, domain=_vt_domain,
                status=contracts.DOMAIN_STATUS_BLOCKED, code=b.code,
                message=";".join(list(b.reasons)))

    # Schedules gate: only requires phase_filters
    if _enabled("view_templates_schedules"):
        try:
            require_domain(contract_domains, "phase_filters")
            legacy = _domain_run("view_templates_schedules",
                                 view_templates.extract_schedules, doc, ctx,
                                 contract_domains, run_diag, runner_notes)
            if legacy is not None:
                fingerprint["view_templates_schedules"] = legacy
        except Blocked as b:
            contract_domains["view_templates_schedules"] = contracts.new_domain_envelope(
                domain="view_templates_schedules",
                domain_version=_DOMAIN_VERSION,
                status=contracts.DOMAIN_STATUS_BLOCKED,
                block_reasons=list(b.reasons),
                diag={"blocked_code": b.code, "upstream": b.upstream},
                records=None,
                hash_value=None,
            )
            contracts.add_bounded_error(run_diag, domain="view_templates_schedules",
                status=contracts.DOMAIN_STATUS_BLOCKED, code=b.code,
                message=";".join(list(b.reasons)))

    # Routing completeness check: verify all view templates accounted for
    # across all 5 domains. Emits a runner note if any templates fell through.
    try:
        _vt_domains = [
            "view_templates.extract_floor_structural_area_plans",
            "view_templates.extract_ceiling_plans",
            "view_templates.extract_elevations_sections_detail",
            "view_templates.extract_renderings_drafting",
            "view_templates_schedules",
        ]
        _vt_total_kept = sum(
            fingerprint.get(d, {}).get("debug_kept", 0)
            for d in _vt_domains
        )
        _vt_raw = fingerprint.get(
            "view_templates.extract_floor_structural_area_plans", {}
        ).get("raw_count", 0)
        _vt_not_template = fingerprint.get(
            "view_templates.extract_floor_structural_area_plans", {}
        ).get("debug_not_template", 0)
        _vt_templates_total = (_vt_raw or 0) - (_vt_not_template or 0)
        _vt_unrouted = _vt_templates_total - _vt_total_kept
        if _vt_unrouted > 0:
            runner_notes.append(
                "view_templates: {} template(s) not routed to any domain "
                "(unrecognized viewtype)".format(_vt_unrouted)
            )
    except Exception:
        pass

    try:
        _vt_records = []
        for _dom in ["view_templates.extract_floor_structural_area_plans",
                     "view_templates.extract_ceiling_plans",
                     "view_templates.extract_elevations_sections_detail",
                     "view_templates.extract_renderings_drafting",
                     "view_templates_schedules"]:
            _vt_records.extend(fingerprint.get(_dom, {}).get("records", []))
        if _vt_records:
            fingerprint["_compat_view_templates"] = {
                "records": _vt_records,
                "count": len(_vt_records),
                "_is_compat_alias": True,
                "_source_domains": ["view_templates.extract_floor_structural_area_plans",
                                    "view_templates.extract_ceiling_plans",
                                    "view_templates.extract_elevations_sections_detail",
                                    "view_templates.extract_renderings_drafting",
                                    "view_templates_schedules"],
            }
    except Exception:
        pass

    # End total extraction timer
    try:
        _timing.end_timer("total_extraction")
    except Exception:
        pass

    # Clean up hashing module timing reference
    try:
        from core import hashing as _hashing_mod
        _hashing_mod._timing_collector = None
    except Exception:
        pass

    # PR5: merge collector counters into contract run_diag for acceptance verification
    try:
        _c = ctx.get("_collect")
        if _c is not None and hasattr(_c, "counters"):
            for _k, _v in dict(_c.counters).items():
                run_diag["counters"][str(_k)] = int(_v)
    except Exception:
        # Do not change run outcome if diagnostics merge fails.
        pass

    # Merge timing report into run_diag (timing does not affect hashes or stable surfaces)
    try:
        timing_report = _timing.get_report()
        if isinstance(timing_report, dict):
            run_diag["timings"] = timing_report
    except Exception:
        pass

    # Hash mode participates in stable surfaces; timing does not.

    # Authoritative contract (statuses live here; legacy payloads may still exist at top-level)
    run_status, run_diag = contracts.compute_run_status(contract_domains, base_run_diag=run_diag, treat_unsupported_as_degraded=False)
    fingerprint["_contract"] = contracts.new_run_envelope(
        schema_version=contracts.SCHEMA_VERSION,
        run_status=run_status,
        run_diag=run_diag,
        domains=contract_domains,
    )

    # Stable comparison + cohort-analysis surfaces
    # Must never throw (runner should remain usable even if these builders fail).
    try:
        fingerprint["_manifest"] = build_manifest(fingerprint)
    except Exception as e:
        contracts.add_bounded_error(
            run_diag,
            domain="_runner",
            status=contracts.DOMAIN_STATUS_DEGRADED,
            code="manifest_build_failed",
            message=str(e),
        )
        run_status2, run_diag2 = contracts.compute_run_status(contract_domains, base_run_diag=run_diag, treat_unsupported_as_degraded=False)
        fingerprint["_contract"] = contracts.new_run_envelope(
            schema_version=contracts.SCHEMA_VERSION,
            run_status=run_status2,
            run_diag=run_diag2,
            domains=contract_domains,
        )

    try:
        fingerprint["_features"] = build_features(fingerprint)
    except Exception as e:
        contracts.add_bounded_error(
            run_diag,
            domain="_runner",
            status=contracts.DOMAIN_STATUS_DEGRADED,
            code="features_build_failed",
            message=str(e),
        )
        run_status3, run_diag3 = contracts.compute_run_status(contract_domains, base_run_diag=run_diag, treat_unsupported_as_degraded=False)
        fingerprint["_contract"] = contracts.new_run_envelope(
            schema_version=contracts.SCHEMA_VERSION,
            run_status=run_status3,
            run_diag=run_diag3,
            domains=contract_domains,
        )

    # Back-compat: keep a pointer to domains map (same object shape as _contract.domains)
    fingerprint["_domains"] = contract_domains
    fingerprint["_notes"] = runner_notes

    return fingerprint


# Execute extraction (OUT protection)
try:
    doc = get_doc()
    fingerprint = run_fingerprint(doc)

    domains_emitted = sorted([k for k in fingerprint.keys() if not str(k).startswith("_")])

    if ENABLED_DOMAINS is None:
        domains_requested = "ALL"
    else:
        domains_requested = list(ENABLED_DOMAINS)

    fingerprint["_meta"] = {
        "repo_root": _REPO_ROOT,
        "tool_version": _TOOL_VERSION,
        "runner": "M5",
        "elapsed_seconds": fingerprint.pop("_elapsed_seconds", None),
        "elapsed_seconds_total": round(time.perf_counter() - _SCRIPT_START, 3),
        "domains_requested": domains_requested,
        "domains_emitted": domains_emitted,
    }

    # ------------------------------------------------------------
    # Output strategy:
    # - If IN[0] provides an output file path: write full JSON to disk here,
    #   then return a small summary JSON via OUT (keeps Revit/Dynamo responsive).
    # - If no path provided: preserve legacy behavior (OUT is the full JSON string).
    # ------------------------------------------------------------
    def _get_output_path_from_dynamo():
        # 1) Preferred: env var injected by thin runner (works across import boundary)
        try:
            p = os.getenv("REVIT_FINGERPRINT_OUTPUT_PATH", "")
            if p is not None:
                p = str(p).strip()
                if p:
                    return p
        except Exception as e:
            pass

        # 2) Fallback: direct IN[0] (only works if this module is executed as the Dynamo node)
        try:
            _in = IN
            if _in is not None and len(_in) > 0 and _in[0] is not None:
                p = str(_in[0]).strip()
                if p:
                    return p
        except Exception as e:
            pass

        # 3) Default: user temp directory (file named from RVT identity)
        try:
            import tempfile
            from datetime import datetime

            base = os.path.join(tempfile.gettempdir(), "Revit_Fingerprint")
            try:
                if not os.path.exists(base):
                    os.makedirs(base)
            except Exception:
                pass

            # Timestamp control (single source of truth)
            use_stamp = _use_filename_stamp()
            stamp = datetime.now().strftime("%Y%m%dT%H%M") if use_stamp else None

            fname = fp_naming.build_output_filename(
                doc,
                stamp=stamp,
                kind="fingerprint",
                ext="json",
                include_stamp=use_stamp,
            )
            return os.path.join(base, fname)

        except Exception:
            return None

    def _ensure_parent_dir(path):
        try:
            parent = os.path.dirname(path)
            if parent and not os.path.exists(parent):
                os.makedirs(parent)
        except Exception as e:
            pass

    def _write_json_to_disk(path, payload):
        """
        Writes JSON directly to disk to avoid returning multi-MB payloads through Dynamo.
        Returns (bytes_written, write_elapsed_seconds).
        """
        t0 = time.perf_counter()
        _ensure_parent_dir(path)
        # Keep formatting identical to legacy OUT behavior (indent=2, sort_keys=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        bytes_written = None
        try:
            bytes_written = os.path.getsize(path)
        except Exception as e:
            pass
        return bytes_written, round(time.perf_counter() - t0, 3)

    def _write_fingerprint(base_payload_path, fingerprint_payload):
        """Write one monolithic fingerprint JSON file."""
        import time as _time

        paths = {
            "payload": base_payload_path,
        }

        bytes_written = {}
        sha256 = {}
        errors = []

        t0 = _time.perf_counter()
        total_write_sec = 0.0

        def _try_write(kind, obj):
            nonlocal total_write_sec
            try:
                b, sec = _write_json_to_disk(paths[kind], obj)
                bytes_written[kind] = b
                total_write_sec += float(sec) if sec is not None else 0.0
                try:
                    sha256[kind] = _sha256_of_file(paths[kind])
                except Exception as e:
                    errors.append({"surface": kind, "code": "sha256_failed", "message": str(e)})
            except Exception as e:
                errors.append({"surface": kind, "code": "write_failed", "message": str(e)})

        _try_write("payload", fingerprint_payload)

        total_write_sec = round(_time.perf_counter() - t0, 3)

        return paths, bytes_written, sha256, total_write_sec, errors

    def _sha256_of_file(path, buf_size=1024 * 1024):
        """
        Compute SHA-256 of a file without loading it into memory.
        Returns hex digest string.
        """
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(buf_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    # Timings around the post-extraction phase
    t_extract_done = round(time.perf_counter() - _SCRIPT_START, 3)

    output_path = _get_output_path_from_dynamo()

    # If caller provided a directory, write a deterministically-named file into it.
    # This supports batch runs: set output path once to a folder and let the runner name files.
    try:
        if output_path:
            op = str(output_path).strip()
            if op:
                is_dir = False
                try:
                    if os.path.isdir(op):
                        is_dir = True
                except Exception:
                    is_dir = False

                # Heuristic: treat as directory if it ends with a path separator or has no ".json" suffix.
                # (We do NOT want to silently interpret arbitrary filenames as directories.)
                try:
                    if (op.endswith(os.sep) or op.endswith("/") or op.endswith("\\")) and (not os.path.exists(op) or os.path.isdir(op)):
                        is_dir = True
                except Exception:
                    pass

                if is_dir:
                    try:
                        if not os.path.exists(op):
                            os.makedirs(op)
                    except Exception:
                        # If we cannot create the directory, fall back to original op and let write fail explicitly.
                        pass

                    from datetime import datetime

                    # Timestamp control (single source of truth)
                    use_stamp = _use_filename_stamp()
                    stamp = datetime.now().strftime("%Y%m%dT%H%M") if use_stamp else None

                    fname = fp_naming.build_output_filename(
                        doc,
                        stamp=stamp,
                        kind="fingerprint",
                        ext="json",
                        include_stamp=use_stamp,
)
                    output_path = os.path.join(op, fname)

    except Exception:
        # Never crash the run due to naming; write will handle errors explicitly.
        pass

    # Escape hatch: force legacy behavior (return full JSON via OUT) when explicitly requested
    force_full_out = False
    try:
        force_full_out = str(os.getenv("REVIT_FINGERPRINT_FORCE_FULL_OUT", "")).strip() in ("1", "true", "True", "YES", "yes")
    except Exception as e:
        force_full_out = False

    if output_path and not force_full_out:
        paths, bytes_written, sha256, write_sec_total, write_errors = _write_fingerprint(output_path, fingerprint)

        t_total_done = round(time.perf_counter() - _SCRIPT_START, 3)

        status = "ok" if not write_errors else "degraded"

        summary = {
            "status": status,
            "output_paths": paths,
            "output_surfaces": ["payload"],
            "filename_stamp_enabled": _use_filename_stamp(),
            "filename_stamp_env": os.environ.get("REVIT_FINGERPRINT_FILENAME_STAMP", None),
            "bytes_written": bytes_written,
            "sha256": sha256,
            "write_errors": write_errors,
            "timings": {
                "extract_done_sec_from_start": t_extract_done,
                "json_write_sec_total": write_sec_total,
                "total_done_sec_from_start": t_total_done,
            },
            "_meta": fingerprint.get("_meta", {}),
        }

        OUT = json.dumps(summary, indent=2, sort_keys=True)

    else:
        # Legacy behavior: return full JSON through Dynamo (may hang on large payloads)
        OUT = json.dumps(fingerprint, indent=2, sort_keys=True)

except Exception as e:
    import traceback as _traceback

    err = {
        "error": str(e),
        "traceback": _traceback.format_exc(),
        "_meta": {
            "runner": "M5",
            "runner_file": __file__,
        },
    }

    # Keep OUT type consistent (JSON string) even on failure
    OUT = json.dumps(err, indent=2, sort_keys=True)
