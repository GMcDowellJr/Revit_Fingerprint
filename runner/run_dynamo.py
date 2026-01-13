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

# Add parent directory to path for imports
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Contract + dependency utilities (must be imported after sys.path adjustment)
from core import contracts
from core.collect import CollectCtx
from core.context import DocViewContext
from core.deps import Blocked, require_domain

# Revit/Dynamo plumbing
clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

# Import domain extractors
from domains import identity, units, object_styles, line_patterns, line_styles
from domains import fill_patterns, text_types, dimension_types
from domains import view_filters, phases, phase_filters, phase_graphics
from domains import view_templates

# Domain selection configuration
# Set to None to run all domains, or provide a list of domain names to run specific domains
ENABLED_DOMAINS = None  # None = all domains
HASH_MODE = os.getenv("REVIT_FINGERPRINT_HASH_MODE", "legacy").strip().lower()
if HASH_MODE not in ("legacy", "semantic"):
    HASH_MODE = "legacy"

_DOMAIN_VERSION = "1"

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

def _extract_legacy_hash(payload):
    """
    Best-effort extraction of the legacy semantic hash without changing behavior.
    """
    try:
        if isinstance(payload, dict) and "hash" in payload:
            return payload.get("hash", None)
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

def _domain_run(domain_name, fn, doc, ctx, contract_domains, run_diag, runner_notes):
    """Runs a domain extractor and records a contract envelope.

    Returns legacy_payload (or None on failure).
    """
    import traceback as _traceback

    domain_name = str(domain_name)

    try:
        legacy = fn(doc, ctx)

        # Domains may optionally emit contract signals into their legacy payload.
        # Runner lifts these into the authoritative contract envelope and strips them from the legacy payload.
        domain_status = contracts.DOMAIN_STATUS_OK
        block_reasons = []
        domain_diag = {
            "api_reachable": True,
            "hash_mode": HASH_MODE,
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

        # Select which hash is surfaced in the contract based on runner HASH_MODE.
        legacy_hash = _extract_legacy_hash(legacy)
        v2_hash = _extract_v2_hash(legacy)

        hash_value = legacy_hash
        if HASH_MODE == "semantic":
            hash_value = v2_hash

        # Lift v2 diagnostics into the contract envelope.
        domain_diag["has_v2"] = bool(_has_v2_surface(legacy))
        v2_reasons = _extract_v2_block_reasons(legacy)
        if v2_reasons:
            domain_diag["v2_block_reasons"] = v2_reasons

        # Semantic mode is an authoritative contract: missing v2 hash must BLOCK.
        if HASH_MODE == "semantic":
            if domain_status == contracts.DOMAIN_STATUS_OK and hash_value is None:
                domain_status = contracts.DOMAIN_STATUS_BLOCKED
                if v2_reasons:
                    block_reasons = sorted({str(k) for k in v2_reasons.keys()})
                else:
                    block_reasons = ["missing_v2_hash"]
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

        return legacy

    except Exception as e:
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

    # PR5: per-run collector cache + counters
    ctx["_collect"] = CollectCtx()

    # PR6: shared document + view context (domains can use for consistent view reads)
    ctx["_doc_view"] = DocViewContext(doc)

    # Assemble fingerprint by calling each domain extractor (legacy payloads)
    fingerprint = {}

    # Contract envelope (authoritative for statuses)
    contract_domains = {}
    run_diag = contracts.new_run_diag()
    runner_notes = []

    # Metadata domains (no behavioral hash)
    if _enabled("identity"):
        legacy = _domain_run("identity", identity.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["identity"] = legacy

    if _enabled("units"):
        legacy = _domain_run("units", units.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["units"] = legacy

    # Global style domains (locked semantics)
    # NOTE: line_patterns must run first to populate ctx mappings consumed by objectstyles/line_styles.
    if _enabled("line_patterns"):
        legacy = _domain_run("line_patterns", line_patterns.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["line_patterns"] = legacy

    if _enabled("objectstyles"):
        legacy = _domain_run("objectstyles", object_styles.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["objectstyles"] = legacy

    if _enabled("line_styles"):
        legacy = _domain_run("line_styles", line_styles.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["line_styles"] = legacy

    if _enabled("fill_patterns"):
        legacy = _domain_run("fill_patterns", fill_patterns.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["fill_patterns"] = legacy

    if _enabled("text_types"):
        legacy = _domain_run("text_types", text_types.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["text_types"] = legacy

    if _enabled("dimension_types"):
        legacy = _domain_run("dimension_types", dimension_types.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["dimension_types"] = legacy

    # New global domains (M4) - run before contextual domains
    # These populate ctx with mappings for views/templates to reference
    if _enabled("view_filters"):
        legacy = _domain_run("view_filters", view_filters.extract, doc, ctx, contract_domains, run_diag, runner_notes)
        if legacy is not None:
            fingerprint["view_filters"] = legacy

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
    if _enabled("view_templates"):
        # Hard dependencies: downstream must not run if upstream is missing or non-acceptable.
        try:
            require_domain(contract_domains, "view_filters")
            require_domain(contract_domains, "phase_filters")

            legacy = _domain_run("view_templates", view_templates.extract, doc, ctx, contract_domains, run_diag, runner_notes)
            if legacy is not None:
                fingerprint["view_templates"] = legacy
        except Blocked as b:
            contract_domains["view_templates"] = contracts.new_domain_envelope(
                domain="view_templates",
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
                domain="view_templates",
                status=contracts.DOMAIN_STATUS_BLOCKED,
                code=b.code,
                message=";".join(list(b.reasons)),
            )

    # PR5: merge collector counters into contract run_diag for acceptance verification
    try:
        _c = ctx.get("_collect")
        if _c is not None and hasattr(_c, "counters"):
            for _k, _v in dict(_c.counters).items():
                run_diag["counters"][str(_k)] = int(_v)
    except Exception:
        # Do not change run outcome if diagnostics merge fails.
        pass

    elapsed_seconds = round(time.time() - start_ts, 3)
    fingerprint["_elapsed_seconds"] = elapsed_seconds
    fingerprint["_hash_mode"] = HASH_MODE

    # Authoritative contract (statuses live here; legacy payloads may still exist at top-level)
    run_status, run_diag = contracts.compute_run_status(contract_domains, base_run_diag=run_diag)
    fingerprint["_contract"] = contracts.new_run_envelope(
        schema_version=contracts.SCHEMA_VERSION,
        run_status=run_status,
        run_diag=run_diag,
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

        # 3) Default: user temp directory
        try:
            import tempfile
            from datetime import datetime

            base = os.path.join(tempfile.gettempdir(), "Revit_Fingerprint")
            try:
                if not os.path.exists(base):
                    os.makedirs(base)
            except Exception as e:
                pass

            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            return os.path.join(base, "fingerprint_{0}.json".format(stamp))
        except Exception as e:
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

    # Escape hatch: force legacy behavior (return full JSON via OUT) when explicitly requested
    force_full_out = False
    try:
        force_full_out = str(os.getenv("REVIT_FINGERPRINT_FORCE_FULL_OUT", "")).strip() in ("1", "true", "True", "YES", "yes")
    except Exception as e:
        force_full_out = False

    if output_path and not force_full_out:
        bytes_written, write_sec = _write_json_to_disk(output_path, fingerprint)

        # Keep OUT a JSON string (type-stable for Dynamo graphs),
        # but small enough to avoid marshaling/preview stalls.
        sha256 = _sha256_of_file(output_path)
        t_total_done = round(time.perf_counter() - _SCRIPT_START, 3)

        summary = {
            "status": "ok",
            "output_path": output_path,
            "bytes_written": bytes_written,
            "sha256": sha256,
            "timings": {
                "extract_done_sec_from_start": t_extract_done,
                "json_write_sec": write_sec,
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
